"""Exercise durable delivery identity and retry ancestry through real ledgers."""

import json
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from threading import Barrier

import pytest

from LiuXin_alpha.jobs.api import (
    JobConcurrencyPolicy,
    JobDefinition,
    JobHandlerAPI,
    JobHandlerRegistry,
    JobLeaseLost,
    JobRepository,
    JobRequestConflict,
    JobResultPolicy,
    JobWorker,
)


def definition(**overrides):
    return replace(
        JobDefinition(
            job_kind="probe",
            job_name="Submission",
            payload_json='{"destination": "archive", "count": 1}',
            result_policy=JobResultPolicy.KEEP_ALL,
        ),
        **overrides,
    )


def finish(repository, run, *, succeeded=False):
    lease = repository.lease_next_run(worker_id="owner", lease_for_s=60)
    assert lease.job_run_id == run.job_run_id
    owner = {"worker_id": "owner", "lease_token": lease.lease_token}
    repository.mark_running(run.job_run_id, **owner)
    if succeeded:
        return repository.mark_succeeded(
            run.job_run_id, **owner, result_json='{"published": true}'
        )
    return repository.mark_failed(
        run.job_run_id, **owner, error_text="Failed after entry"
    )


def counts(repository):
    with closing(repository.connect()) as connection:
        return tuple(
            connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ("job_definitions", "job_runs", "job_run_events")
        )


def test_repeated_delivery_returns_the_same_completed_run(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    run = repository.submit_job(definition(), request_key="upload-1")
    finish(repository, run, succeeded=True)
    repeated = repository.submit_job(
        definition(payload_json=' { "count": 1, "destination": "archive" } '),
        request_key="upload-1",
    )
    assert repeated.job_run_id == run.job_run_id
    assert repeated.state.value == "succeeded"
    assert json.loads(repeated.result_json) == {"published": True}
    assert counts(repository) == (1, 1, 4)


def test_concurrent_duplicate_delivery_has_one_definition_run_and_event(tmp_path):
    path = tmp_path / "jobs.sqlite"
    repository = JobRepository(path)
    repositories = [JobRepository(path) for _ in range(8)]
    barrier = Barrier(len(repositories))

    def submit(repo):
        barrier.wait(timeout=10)
        return repo.submit_job(definition(), request_key="same-request").job_run_id

    with ThreadPoolExecutor(max_workers=8) as executor:
        identifiers = list(executor.map(submit, repositories))
    assert len(set(identifiers)) == 1
    assert counts(repository) == (1, 1, 1)


def test_changed_request_is_refused_without_creating_work(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    original = repository.submit_job(definition(), request_key="fixed")
    with pytest.raises(JobRequestConflict, match="request key"):
        repository.submit_job(
            definition(payload_json='{"destination": "other"}'), request_key="fixed"
        )
    assert counts(repository) == (1, 1, 1)
    assert repository.get_run(original.job_run_id).state.value == "queued"


@pytest.mark.parametrize(
    "field,value",
    [
        ("job_kind", "different"),
        ("job_name", "Renamed"),
        ("priority", 1),
        ("timeout_s", 10),
        ("schedule_json", '{"interval_seconds": 5}'),
        ("concurrency_policy", JobConcurrencyPolicy.ALLOW_PARALLEL),
        ("result_policy", JobResultPolicy.KEEP_LATEST_ONLY),
    ],
)
def test_delivery_key_covers_configuration_not_just_payload(tmp_path, field, value):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    repository.submit_job(definition(), request_key="fixed")
    with pytest.raises(JobRequestConflict):
        repository.submit_job(definition(**{field: value}), request_key="fixed")
    assert counts(repository) == (1, 1, 1)


@pytest.mark.parametrize("key", ["", "   ", "x" * 201, None, 123])
def test_submission_requires_a_bounded_explicit_key(tmp_path, key):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    with pytest.raises(ValueError, match="request key"):
        repository.submit_job(definition(), request_key=key)
    assert counts(repository) == (0, 0, 0)


@pytest.mark.parametrize("payload", ["not-json", '{"x": NaN}', '{"x": Infinity}'])
def test_invalid_request_json_is_refused_before_creating_work(tmp_path, payload):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    with pytest.raises(ValueError):
        repository.submit_job(definition(payload_json=payload), request_key="bad")
    assert counts(repository) == (0, 0, 0)


@pytest.mark.parametrize("failure", ["event", "request"])
def test_first_submission_rolls_back_all_partial_rows(tmp_path, monkeypatch, failure):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    method = "_append_event" if failure == "event" else "_remember_request"
    original = getattr(repository, method)

    def fail_after_insert(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("injected storage failure")

    with monkeypatch.context() as patch:
        patch.setattr(repository, method, fail_after_insert)
        with pytest.raises(RuntimeError, match="injected"):
            repository.submit_job(definition(), request_key="recoverable")
    assert counts(repository) == (0, 0, 0)
    # The failed request did not consume the caller's delivery key.
    accepted = repository.submit_job(definition(), request_key="recoverable")
    assert repository.get_run(accepted.job_run_id).state.value == "queued"


def test_duplicate_enqueue_does_not_repeat_replace_running_cancellation(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    saved = repository.create_definition(
        definition(concurrency_policy=JobConcurrencyPolicy.REPLACE_RUNNING)
    )
    old = repository.enqueue_run(job_definition_id=saved.job_definition_id)
    current = repository.enqueue_run(
        job_definition_id=saved.job_definition_id, request_key="replacement"
    )
    event_count = counts(repository)[2]
    assert (
        repository.enqueue_run(
            job_definition_id=saved.job_definition_id, request_key="replacement"
        )
        == current
    )
    assert repository.get_run(old.job_run_id).state.value == "cancelled"
    assert repository.get_run(current.job_run_id).state.value == "queued"
    assert counts(repository) == (1, 2, event_count)


def test_changed_definition_requires_a_new_enqueue_key(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    saved = repository.create_definition(definition())
    original = repository.enqueue_run(
        job_definition_id=saved.job_definition_id, request_key="fixed"
    )
    repository.request_cancel(original.job_run_id)
    repository.update_definition(replace(saved, payload_json='{"changed": true}'))
    with pytest.raises(JobRequestConflict):
        repository.enqueue_run(
            job_definition_id=saved.job_definition_id, request_key="fixed"
        )
    assert len(repository.list_runs()) == 1


@pytest.mark.parametrize("state", ["queued", "leased", "running", "succeeded"])
def test_active_or_successful_attempts_cannot_be_retried(tmp_path, state):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    run = repository.submit_job(definition(), request_key="first")
    if state == "succeeded":
        finish(repository, run, succeeded=True)
    elif state in {"leased", "running"}:
        lease = repository.lease_next_run(worker_id="owner", lease_for_s=60)
        if state == "running":
            repository.mark_running(
                run.job_run_id, worker_id="owner", lease_token=lease.lease_token
            )
    with pytest.raises(ValueError, match="Only"):
        repository.retry_run(
            run.job_run_id, request_key="retry", acknowledge_possible_effects=True
        )
    assert len(repository.list_runs()) == 1


def test_cancelled_unstarted_run_retries_without_effect_acknowledgement(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    original = repository.submit_job(definition(), request_key="original")
    repository.request_cancel(original.job_run_id)
    second = repository.retry_run(original.job_run_id, request_key="second")
    finish(repository, second)
    third = repository.retry_run(
        second.job_run_id, request_key="third", acknowledge_possible_effects=True
    )
    assert third.attempt_number == 3
    assert third.retry_of_job_run_id == second.job_run_id
    assert not third.cancel_requested
    event = repository.list_events(third.job_run_id)[0]
    assert json.loads(event.event_json) == {
        "attempt_number": 3,
        "retry_of_job_run_id": second.job_run_id,
        "possible_effects_acknowledged": True,
    }
    # Lost acknowledgement of retry two resolves that run, not its latest successor.
    assert (
        repository.retry_run(original.job_run_id, request_key="second").job_run_id
        == second.job_run_id
    )


def test_concurrent_retry_requests_cannot_fork_attempt_history(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    original = repository.submit_job(
        definition(concurrency_policy=JobConcurrencyPolicy.ALLOW_PARALLEL),
        request_key="original",
    )
    repository.request_cancel(original.job_run_id)
    barrier = Barrier(8)

    def retry(index):
        barrier.wait(timeout=10)
        try:
            return repository.retry_run(
                original.job_run_id, request_key=f"retry-{index}"
            )
        except ValueError as exc:
            assert "already has a retry" in str(exc)
            return None

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(retry, range(8)))
    assert sum(result is not None for result in results) == 1
    assert len(repository.list_runs()) == 2


def test_retry_request_failure_rolls_back_successor_and_can_be_repeated(
    tmp_path, monkeypatch
):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    original = repository.submit_job(definition(), request_key="original")
    repository.request_cancel(original.job_run_id)
    baseline = counts(repository)

    def fail(*args):
        raise RuntimeError("request mapping failed")

    with monkeypatch.context() as patch:
        patch.setattr(repository, "_remember_request", fail)
        with pytest.raises(RuntimeError, match="mapping"):
            repository.retry_run(original.job_run_id, request_key="retry")
    assert counts(repository) == baseline
    assert (
        repository.retry_run(original.job_run_id, request_key="retry").attempt_number
        == 2
    )


def test_result_pruning_keeps_request_identity(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    saved = repository.create_definition(
        definition(result_policy=JobResultPolicy.KEEP_LATEST_ONLY)
    )
    first = repository.enqueue_run(
        job_definition_id=saved.job_definition_id, request_key="first"
    )
    finish(repository, first, succeeded=True)
    second = repository.enqueue_run(
        job_definition_id=saved.job_definition_id, request_key="second"
    )
    finish(repository, second, succeeded=True)
    repeated = repository.enqueue_run(
        job_definition_id=saved.job_definition_id, request_key="first"
    )
    assert repeated.job_run_id == first.job_run_id
    assert repeated.state.value == "succeeded"
    assert repeated.result_json is None
    assert len(repository.list_runs()) == 2


def test_delivery_keys_share_one_namespace_across_operations(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    first = repository.submit_job(definition(), request_key="shared")
    repository.request_cancel(first.job_run_id)
    with pytest.raises(JobRequestConflict):
        repository.retry_run(first.job_run_id, request_key="shared")
    with pytest.raises(JobRequestConflict):
        repository.enqueue_run(
            job_definition_id=first.job_definition_id, request_key="shared"
        )
    assert len(repository.list_runs()) == 1


def test_legacy_upgrade_captures_label_once_and_does_not_invent_ancestry(tmp_path):
    path = tmp_path / "legacy.sqlite"
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.executescript(
            Path(__file__)
            .with_name("fixtures")
            .joinpath("legacy_schema.sql")
            .read_text()
        )
        connection.execute("""
            INSERT INTO job_definitions(job_kind, job_name, state, payload_json,
                concurrency_policy, result_policy, created_timestamp_ep_k, modified_timestamp_ep_k)
            VALUES ('probe', 'Original label', 'enabled', '{}', 'skip_if_running', 'keep_all', 1, 1)
        """)
        connection.execute("""
            INSERT INTO job_runs(job_definition_id, job_kind, trigger_kind, state, attempt_number)
            VALUES (1, 'probe', 'manual', 'cancelled', 4)
        """)
    repository = JobRepository(path)
    repository.update_definition(
        replace(repository.get_definition(1), job_name="Edited")
    )
    reopened = JobRepository(path)
    run = reopened.get_run(1)
    assert run.job_name == "Original label"
    assert run.attempt_number == 4
    assert run.retry_of_job_run_id is None
    assert repository.ledger_uuid == reopened.ledger_uuid
    retried = reopened.retry_run(1, request_key="retry-legacy")
    assert retried.attempt_number == 5
    assert retried.job_name == "Original label"


def test_retry_captures_original_inputs_and_records_ancestry(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    original = repository.submit_job(definition(), request_key="first")
    finish(repository, original)
    repository.update_definition(
        replace(
            repository.get_definition(original.job_definition_id),
            job_kind="changed",
            job_name="Renamed",
            payload_json='{"destination": "changed"}',
        )
    )
    with pytest.raises(ValueError, match="effects"):
        repository.retry_run(original.job_run_id, request_key="retry")
    retried = repository.retry_run(
        original.job_run_id, request_key="retry", acknowledge_possible_effects=True
    )
    assert retried.retry_of_job_run_id == original.job_run_id
    assert retried.attempt_number == 2
    assert retried.job_kind == original.job_kind
    assert retried.job_name == original.job_name
    assert retried.payload_json == original.payload_json
    assert retried.state.value == "queued"
    assert retried.result_json is None
    assert repository.get_run(original.job_run_id).state.value == "failed"
    assert (
        repository.retry_run(
            original.job_run_id, request_key="retry", acknowledge_possible_effects=True
        )
        == retried
    )


def test_fresh_process_resolves_ledger_and_submission_identity(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    run = repository.submit_job(definition(), request_key="durable-request")
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import json, sys
from LiuXin_alpha.jobs.api import JobDefinition, JobRepository, JobResultPolicy
repo = JobRepository(sys.argv[1])
run = repo.submit_job(JobDefinition(job_kind="probe", job_name="Submission",
    payload_json='{"destination": "archive", "count": 1}',
    result_policy=JobResultPolicy.KEEP_ALL), request_key="durable-request")
print(json.dumps([repo.ledger_uuid, run.job_run_id]))
""",
            str(repository.sqlite_path),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert json.loads(result.stdout) == [repository.ledger_uuid, run.job_run_id]
    assert (
        JobRepository(tmp_path / "other.sqlite").ledger_uuid != repository.ledger_uuid
    )


def test_enqueue_can_deduplicate_without_bypassing_concurrency(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    saved = repository.create_definition(
        definition(concurrency_policy=JobConcurrencyPolicy.QUEUE_ONE)
    )
    first = repository.enqueue_run(
        job_definition_id=saved.job_definition_id, request_key="scheduled-1"
    )
    assert (
        repository.enqueue_run(
            job_definition_id=saved.job_definition_id, request_key="scheduled-1"
        )
        == first
    )
    with pytest.raises(RuntimeError):
        repository.enqueue_run(
            job_definition_id=saved.job_definition_id, request_key="scheduled-2"
        )
    assert counts(repository) == (1, 1, 1)


def test_duplicate_delivery_does_not_run_publishing_handler_again(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    output = tmp_path / "publication.txt"

    class PublishingHandler(JobHandlerAPI):
        job_kind = "probe"

        def validate_payload(self, payload_json):
            pass

        def run(self, *, payload_json, run_context):
            with output.open("a") as stream:
                stream.write("published\n")
            return {"path": str(output)}

    handlers = JobHandlerRegistry()
    handlers.register(PublishingHandler())
    worker = JobWorker(repository=repository, handlers=handlers)
    first = repository.submit_job(definition(), request_key="delivery")
    assert worker.run_once()
    repeated = repository.submit_job(definition(), request_key="delivery")
    assert repeated.job_run_id == first.job_run_id
    assert not worker.run_once()
    assert output.read_text() == "published\n"


def test_recovered_interruption_requires_acknowledgement_and_fences_old_owner(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    first = repository.submit_job(definition(), request_key="original")
    lease = repository.lease_next_run(worker_id="old", lease_for_s=60)
    owner = {"worker_id": "old", "lease_token": lease.lease_token}
    repository.mark_running(first.job_run_id, **owner)
    with closing(repository.connect()) as connection, connection:
        connection.execute(
            "UPDATE job_runs SET lease_expires_timestamp_ep_k=1 WHERE job_run_id=?",
            (first.job_run_id,),
        )
    assert repository.recover_expired_runs() == [first.job_run_id]
    with pytest.raises(ValueError, match="effects"):
        repository.retry_run(first.job_run_id, request_key="retry")
    retry = repository.retry_run(
        first.job_run_id, request_key="retry", acknowledge_possible_effects=True
    )
    with pytest.raises(JobLeaseLost):
        repository.mark_succeeded(first.job_run_id, **owner, result_json="{}")
    assert repository.get_run(first.job_run_id).state.value == "aborted"
    assert repository.get_run(retry.job_run_id).state.value == "queued"
