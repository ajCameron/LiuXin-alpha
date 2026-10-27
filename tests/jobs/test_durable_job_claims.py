"""Prove durable ownership, recovery and receipts against actual SQLite ledgers."""

import json
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import asdict, replace
from pathlib import Path
from threading import Event

import pytest

from LiuXin_alpha.jobs.api import (
    JobConcurrencyPolicy,
    JobDefinition,
    JobHandlerAPI,
    JobHandlerRegistry,
    JobLeaseLost,
    JobProgressUpdate,
    JobRepository,
    JobResultPolicy,
    JobWorker,
)


def queued(repository, kind="probe"):
    definition = repository.create_definition(
        JobDefinition(job_kind=kind, job_name="Probe", payload_json="{}")
    )
    return repository.enqueue_run(job_definition_id=definition.job_definition_id)


def expire(repository, run_id):
    with closing(repository.connect()) as connection, connection:
        connection.execute(
            "UPDATE job_runs SET lease_expires_timestamp_ep_k=1 WHERE job_run_id=?",
            (run_id,),
        )


def test_stale_worker_cannot_start_reassigned_lease(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    run = queued(repository)
    old = repository.lease_next_run(worker_id="old", lease_for_s=60)
    expire(repository, run.job_run_id)
    repository.lease_next_run(worker_id="new", lease_for_s=60)
    with pytest.raises(RuntimeError, match="[Ll]ease"):
        repository.mark_running(
            run.job_run_id, worker_id="old", lease_token=old.lease_token
        )
    assert repository.get_run(run.job_run_id).worker_id == "new"


def test_expired_running_attempt_becomes_explicitly_interrupted(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    run = queued(repository)
    leased = repository.lease_next_run(worker_id="lost", lease_for_s=60)
    repository.mark_running(
        run.job_run_id, worker_id="lost", lease_token=leased.lease_token
    )
    expire(repository, run.job_run_id)
    # Recovery must survive process death, not depend on an in-memory manager.
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import json, sys
from LiuXin_alpha.jobs.repository import JobRepository
repo = JobRepository(sys.argv[1])
assert repo.lease_next_run(worker_id="replacement", lease_for_s=60) is None
print(json.dumps(repo.recover_expired_runs()))
""",
            str(repository.sqlite_path),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert json.loads(result.stdout) == []  # Leasing already performed recovery.
    recovered = repository.get_run(run.job_run_id)
    assert recovered.state.value == "aborted"
    assert recovered.error_text


def test_unknown_handler_is_a_durable_failure(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    run = queued(repository, kind="unavailable")
    worker = JobWorker(repository=repository, handlers=JobHandlerRegistry())
    assert worker.run_once() is True
    failed = repository.get_run(run.job_run_id)
    assert failed.state.value == "failed"
    assert "Unknown job kind" in failed.error_text


class CountingHandler(JobHandlerAPI):
    job_kind = "probe"

    def __init__(self):
        self.calls = 0

    def validate_payload(self, payload_json):
        pass

    def run(self, *, payload_json, run_context):
        self.calls += 1
        return {"calls": self.calls}


def test_cancelled_queue_never_enters_handler(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    run = queued(repository)
    repository.request_cancel(run.job_run_id)
    registry = JobHandlerRegistry()
    handler = CountingHandler()
    registry.register(handler)
    JobWorker(repository=repository, handlers=registry).run_once()
    assert handler.calls == 0
    assert repository.get_run(run.job_run_id).state.value == "cancelled"


def test_repository_closes_every_owned_connection(tmp_path):
    connections = []

    class ObservedRepository(JobRepository):
        def connect(self):
            connection = super().connect()
            connections.append(connection)
            return connection

    repository = ObservedRepository(tmp_path / "jobs.sqlite")
    run = queued(repository)
    repository.get_run(run.job_run_id)
    repository.list_events(run.job_run_id)
    for connection in connections:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            connection.execute("SELECT 1")


def claim(repository, worker="owner"):
    leased = repository.lease_next_run(worker_id=worker, lease_for_s=60)
    owner = {"worker_id": worker, "lease_token": leased.lease_token}
    return repository.mark_running(leased.job_run_id, **owner), owner


def test_parallel_workers_claim_each_run_once(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    definition = repository.create_definition(
        JobDefinition(
            job_kind="probe",
            job_name="Parallel",
            payload_json="{}",
            concurrency_policy=JobConcurrencyPolicy.ALLOW_PARALLEL,
        )
    )
    expected = {
        repository.enqueue_run(
            job_definition_id=definition.job_definition_id
        ).job_run_id
        for _ in range(8)
    }
    start = Event()

    def take(index):
        start.wait(5)
        independent = JobRepository(repository.sqlite_path)
        return independent.lease_next_run(worker_id=f"worker-{index}", lease_for_s=60)

    with ThreadPoolExecutor(max_workers=16) as pool:
        pending = [pool.submit(take, index) for index in range(16)]
        start.set()
        results = [future.result(timeout=10) for future in pending]
    taken = [row.job_run_id for row in results if row is not None]
    assert len(taken) == len(set(taken)) == 8
    assert set(taken) == expected


def test_parallel_enqueue_enforces_one_active_run(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    definition = repository.create_definition(
        JobDefinition(
            job_kind="probe",
            job_name="One",
            payload_json="{}",
            concurrency_policy=JobConcurrencyPolicy.QUEUE_ONE,
        )
    )
    start = Event()

    def submit():
        start.wait(5)
        try:
            return repository.enqueue_run(
                job_definition_id=definition.job_definition_id
            )
        except RuntimeError as error:
            assert "already has" in str(error)
            return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        pending = [pool.submit(submit) for _ in range(8)]
        start.set()
        results = [future.result(timeout=10) for future in pending]
    assert sum(row is not None for row in results) == 1
    assert len(repository.list_runs()) == 1


def test_reused_worker_name_does_not_reuse_claim_authority(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    queued(repository)
    old = repository.lease_next_run(worker_id="same", lease_for_s=60)
    stale = {"worker_id": "same", "lease_token": old.lease_token}
    expire(repository, old.job_run_id)
    run, owner = claim(repository, "same")
    assert owner["lease_token"] != stale["lease_token"]
    before = repository.get_run(run.job_run_id)
    events = repository.list_events(run.job_run_id)
    rejected = (
        lambda: repository.mark_running(run.job_run_id, **stale),
        lambda: repository.heartbeat(run.job_run_id, **stale),
        lambda: repository.update_progress(
            run.job_run_id, JobProgressUpdate(progress_current=99), **stale
        ),
        lambda: repository.append_log(run.job_run_id, message="stale", **stale),
        lambda: repository.mark_succeeded(run.job_run_id, result_json="{}", **stale),
        lambda: repository.mark_failed(run.job_run_id, error_text="stale", **stale),
        lambda: repository.mark_cancelled(run.job_run_id, **stale),
    )
    for operation in rejected:
        with pytest.raises(JobLeaseLost):
            operation()
        assert repository.get_run(run.job_run_id) == before
        assert repository.list_events(run.job_run_id) == events
    repository.mark_succeeded(run.job_run_id, result_json='{"owner": true}', **owner)
    with pytest.raises(JobLeaseLost):
        repository.heartbeat(run.job_run_id, **owner)
    assert repository.request_cancel(run.job_run_id) is False
    assert repository.get_run(run.job_run_id).state.value == "succeeded"


def test_expired_claim_cannot_revive_itself(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    queued(repository)
    run, owner = claim(repository)
    expire(repository, run.job_run_id)
    with pytest.raises(JobLeaseLost):
        repository.heartbeat(run.job_run_id, **owner)
    with pytest.raises(JobLeaseLost):
        repository.mark_succeeded(run.job_run_id, **owner)
    assert repository.recover_expired_runs() == [run.job_run_id]
    assert repository.recover_expired_runs() == []


@pytest.mark.parametrize("phase", ("enqueue", "lease", "progress", "finish"))
def test_state_and_event_rollback_together(tmp_path, monkeypatch, phase):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    definition = repository.create_definition(
        JobDefinition(job_kind="probe", job_name="Atomic", payload_json="{}")
    )
    if phase != "enqueue":
        repository.enqueue_run(job_definition_id=definition.job_definition_id)
    if phase in ("progress", "finish"):
        run, owner = claim(repository)

    def snapshot():
        return (
            repository.list_definitions(),
            repository.list_runs(),
            [repository.list_events(row.job_run_id) for row in repository.list_runs()],
        )

    before = snapshot()

    def fail_event(*args, **kwargs):
        raise RuntimeError("Injected event persistence failure")

    monkeypatch.setattr(repository, "_append_event", fail_event)
    with pytest.raises(RuntimeError, match="event persistence"):
        if phase == "enqueue":
            repository.enqueue_run(job_definition_id=definition.job_definition_id)
        elif phase == "lease":
            repository.lease_next_run(worker_id="owner", lease_for_s=60)
        elif phase == "progress":
            repository.update_progress(
                run.job_run_id, JobProgressUpdate(progress_current=1), **owner
            )
        else:
            repository.mark_succeeded(
                run.job_run_id, result_json='{"done": true}', **owner
            )
    assert snapshot() == before


def test_worker_uses_captured_kind_and_payload(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    run = queued(repository)
    definition = repository.get_definition(run.job_definition_id)
    repository.update_definition(
        replace(definition, job_kind="not-installed", payload_json='{"changed": true}')
    )

    class EchoHandler(CountingHandler):
        def run(self, *, payload_json, run_context):
            return json.loads(payload_json)

    registry = JobHandlerRegistry()
    registry.register(EchoHandler())
    assert JobWorker(repository=repository, handlers=registry).run_once()
    completed = repository.get_run(run.job_run_id)
    assert completed.state.value == "succeeded"
    assert json.loads(completed.result_json) == {}


def test_heartbeat_renews_long_handler_without_progress_callbacks(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    run = queued(repository)
    renewed, release = Event(), Event()
    original = repository.heartbeat
    renewals = []

    def heartbeat(*args, **kwargs):
        original(*args, **kwargs)
        renewals.append(True)
        if len(renewals) >= 4:
            renewed.set()

    repository.heartbeat = heartbeat

    class WaitingHandler(CountingHandler):
        def run(self, *, payload_json, run_context):
            assert release.wait(10)
            return {"finished": True}

    registry = JobHandlerRegistry()
    registry.register(WaitingHandler())
    worker = JobWorker(repository=repository, handlers=registry, lease_for_s=1)
    with ThreadPoolExecutor(max_workers=1) as pool:
        try:
            pending = pool.submit(worker.run_once)
            assert renewed.wait(8)
            assert repository.get_run(run.job_run_id).state.value == "running"
            release.set()
            assert pending.result(timeout=5)
        finally:
            release.set()
    assert repository.get_run(run.job_run_id).state.value == "succeeded"


@pytest.mark.parametrize(
    "policy", (JobResultPolicy.KEEP_FAILURES, JobResultPolicy.KEEP_LATEST_ONLY)
)
def test_result_retention_keeps_live_runs_and_history(tmp_path, policy):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    definition = repository.create_definition(
        JobDefinition(
            job_kind="probe",
            job_name="Retention",
            payload_json="{}",
            concurrency_policy=JobConcurrencyPolicy.ALLOW_PARALLEL,
            result_policy=policy,
        )
    )
    for _ in range(2):
        repository.enqueue_run(job_definition_id=definition.job_definition_id)
    first, first_owner = claim(repository, "first")
    second, second_owner = claim(repository, "second")
    repository.mark_succeeded(
        second.job_run_id, result_json='{"second": true}', **second_owner
    )
    assert repository.get_run(first.job_run_id).state.value == "running"
    repository.mark_succeeded(
        first.job_run_id, result_json='{"first": true}', **first_owner
    )
    assert repository.get_run(first.job_run_id).result_json is None
    assert repository.get_run(second.job_run_id).result_json == '{"second": true}'
    events = repository.list_events(first.job_run_id)
    assert events[-1].event_kind.value == "succeeded"
    first_page = repository.list_events(first.job_run_id, limit=2)
    assert first_page == events[:2]
    assert (
        repository.list_events(first.job_run_id, after_event_id=first_page[-1].event_id)
        == events[2:]
    )


def test_cancel_acknowledgement_preserves_completed_effect_receipt(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    run = queued(repository)
    output = tmp_path / "completed-effect"

    class PublishingHandler(CountingHandler):
        def run(self, *, payload_json, run_context):
            output.write_text("published")
            repository.request_cancel(run_context.job_run_id)
            return {"path": str(output), "published": True}

    registry = JobHandlerRegistry()
    registry.register(PublishingHandler())
    JobWorker(repository=repository, handlers=registry).run_once()
    completed = repository.get_run(run.job_run_id)
    assert completed.state.value == "cancelled"
    assert json.loads(completed.result_json)["published"] is True
    assert output.read_text() == "published"


def test_legacy_ledger_upgrade_captures_inputs_once(tmp_path):
    path = tmp_path / "legacy.sqlite"
    schema = Path(__file__).with_name("fixtures") / "legacy_schema.sql"
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.executescript(schema.read_text())
        connection.execute("""
            INSERT INTO job_definitions(job_kind, job_name, state, payload_json,
                concurrency_policy, result_policy, created_timestamp_ep_k,
                modified_timestamp_ep_k)
            VALUES ('probe', 'Legacy', 'enabled', '{"legacy": true}',
                'skip_if_running', 'keep_all', 1, 1)
        """)
        connection.execute("""
            INSERT INTO job_runs(job_definition_id, job_kind, trigger_kind, state)
            VALUES (1, 'probe', 'manual', 'queued')
        """)
    repository = JobRepository(path)
    migrated = repository.get_run(1)
    assert json.loads(migrated.payload_json) == {"legacy": True}
    definition = repository.get_definition(1)
    repository.update_definition(replace(definition, payload_json="{}"))
    reopened = JobRepository(path)
    assert asdict(reopened.get_run(1)) == asdict(migrated)
    run, owner = claim(reopened)
    assert run.lease_token
    reopened.mark_succeeded(run.job_run_id, result_json="{}", **owner)


def test_cancelling_leased_run_refuses_late_start(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    queued(repository)
    leased = repository.lease_next_run(worker_id="owner", lease_for_s=60)
    assert repository.request_cancel(leased.job_run_id)
    with pytest.raises(JobLeaseLost):
        repository.mark_running(
            leased.job_run_id, worker_id="owner", lease_token=leased.lease_token
        )
    assert repository.get_run(leased.job_run_id).state.value == "cancelled"


@pytest.mark.parametrize("failure", ("validation", "serialization"))
def test_worker_persists_pre_execution_and_result_failures(tmp_path, failure):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    run = queued(repository)

    class FailingHandler(CountingHandler):
        def validate_payload(self, payload_json):
            if failure == "validation":
                raise ValueError("Invalid payload")

        def run(self, *, payload_json, run_context):
            return {"not_json": object()}

    registry = JobHandlerRegistry()
    registry.register(FailingHandler())
    assert JobWorker(repository=repository, handlers=registry).run_once()
    failed = repository.get_run(run.job_run_id)
    assert failed.state.value == "failed"
    assert failed.error_text
    assert failed.result_json is None


@pytest.mark.parametrize("outcome", ("return", "raise"))
def test_late_worker_cannot_overwrite_recovery_receipt(tmp_path, outcome):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    run = queued(repository)
    started, release = Event(), Event()

    class LateHandler(CountingHandler):
        def run(self, *, payload_json, run_context):
            started.set()
            assert release.wait(8)
            if outcome == "raise":
                raise RuntimeError("Failure after losing the claim")
            return {"too_late": True}

    registry = JobHandlerRegistry()
    registry.register(LateHandler())
    worker = JobWorker(repository=repository, handlers=registry)
    with ThreadPoolExecutor(max_workers=1) as pool:
        try:
            pending = pool.submit(worker.run_once)
            assert started.wait(3)
            expire(repository, run.job_run_id)
            assert repository.recover_expired_runs() == [run.job_run_id]
            recovered = repository.get_run(run.job_run_id)
            events = repository.list_events(run.job_run_id)
            release.set()
            assert pending.result(timeout=3)
        finally:
            release.set()
    assert recovered.state.value == "aborted"
    assert repository.get_run(run.job_run_id) == recovered
    assert repository.list_events(run.job_run_id) == events


def test_worker_records_renewal_thread_start_failure(tmp_path, monkeypatch):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    run = queued(repository)
    registry = JobHandlerRegistry()
    registry.register(CountingHandler())

    def fail_start(thread):
        raise RuntimeError("Cannot allocate renewal thread")

    monkeypatch.setattr("threading.Thread.start", fail_start)
    assert JobWorker(repository=repository, handlers=registry).run_once()
    failed = repository.get_run(run.job_run_id)
    assert failed.state.value == "failed"
    assert "renewal thread" in failed.error_text


def test_handler_can_acknowledge_cancellation_at_checkpoint(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    run = queued(repository)

    class CooperativeHandler(CountingHandler):
        def run(self, *, payload_json, run_context):
            repository.request_cancel(run_context.job_run_id)
            run_context.raise_if_cancel_requested()
            raise AssertionError("Code after cancellation must not run")

    registry = JobHandlerRegistry()
    registry.register(CooperativeHandler())
    assert JobWorker(repository=repository, handlers=registry).run_once()
    cancelled = repository.get_run(run.job_run_id)
    assert cancelled.state.value == "cancelled"
    assert "checkpoint" in cancelled.error_text
