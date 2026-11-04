"""Verify bounded job-history retirement against catalogue delivery receipts."""

import json
from contextlib import closing

import pytest

from LiuXin_alpha.core import maintenance_history
from LiuXin_alpha.core.maintenance_jobs import MAINTENANCE_JOB_KIND
from LiuXin_alpha.databases.db_types.maintenance_types import MaintenanceMode
from LiuXin_alpha.jobs.models import JobDefinition
from tests.databases.test_maintenance_operator_controls import _publish, _setup
from tests.databases.test_maintenance_runtime import Projection
from tests.databases.test_maintenance_runtime import runtime_factory as runtime_factory


def _make(runtime_factory, tmp_path, monkeypatch):
    plugin = Projection()
    plugin.mode = MaintenanceMode.LAZY
    runtime, repository, worker, _ = _setup(runtime_factory, tmp_path, [plugin])
    # Make age tests independent of host wall-clock adjustments.
    monkeypatch.setattr(maintenance_history, "now_ms", lambda: 20 * 86400000)
    return plugin, runtime, repository, worker


def _age(repository, *, days_ago=10):
    with closing(repository.connect()) as connection, connection:
        connection.execute(
            "UPDATE job_runs SET finished_timestamp_ep_k=? WHERE finished_timestamp_ep_k IS NOT NULL",
            ((20 - days_ago) * 86400000,),
        )


def _prune(runtime, repository, **kwargs):
    return maintenance_history.prune_maintenance_history(
        runtime, repository, library_uuid="test-library", **kwargs
    )


def test_retention_age_and_paging_remove_retry_history_after_journal_pruning(
    runtime_factory, tmp_path, monkeypatch
):
    plugin, runtime, repository, worker = _make(runtime_factory, tmp_path, monkeypatch)
    initial = _publish(runtime, repository)["run_ids"][0]
    plugin.fail = True
    assert worker.run_once()
    plugin.fail = False
    _publish(runtime, repository)
    assert worker.run_once()
    last_run_id = max(run.job_run_id for run in repository.list_runs())
    journal_uuid = runtime.status()["journal_uuid"]
    ledger_uuid = repository.ledger_uuid
    _age(repository, days_ago=1)
    assert _prune(runtime, repository)["removed_runs"] == 0
    _age(repository)
    assert runtime.prune_completed() == 1
    first = _prune(runtime, repository, limit=1)
    assert first["removed_runs"] == 1 and first["has_more"]
    assert first["next_before_run_id"] == last_run_id
    second = _prune(
        runtime, repository, limit=1, before_run_id=first["next_before_run_id"]
    )
    assert second["removed_runs"] == 1 and not second["has_more"]
    assert second["removed_definitions"] == 1
    with closing(repository.connect()) as connection:
        for table in ("job_runs", "job_requests", "job_definitions", "job_run_events"):
            assert (
                connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
            )
    assert runtime.status()["journal_uuid"] == journal_uuid
    assert repository.ledger_uuid == ledger_uuid
    assert _publish(runtime, repository)["published"] == 0
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (2, 'after retirement')")
    assert _publish(runtime, repository)["run_ids"][0] > last_run_id > initial


def test_retention_protects_pending_foreign_malformed_and_unrelated_work(
    runtime_factory, tmp_path, monkeypatch
):
    plugin, runtime, repository, worker = _make(runtime_factory, tmp_path, monkeypatch)
    completed = _publish(runtime, repository)["run_ids"][0]
    assert worker.run_once()
    captured = json.loads(repository.get_run(completed).payload_json)
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (2, 'failed pending')")
    pending = _publish(runtime, repository)["run_ids"][0]
    plugin.fail = True
    assert worker.run_once()
    foreign = []
    for index, payload in enumerate(
        (
            {**captured, "journal_uuid": "other"},
            {**captured, "library_uuid": "other"},
            {"invalid": True},
        )
    ):
        run = repository.submit_job(
            JobDefinition(
                job_kind=MAINTENANCE_JOB_KIND,
                job_name="Foreign",
                payload_json=json.dumps(payload),
            ),
            request_key=f"foreign-{index}",
        )
        foreign.append(run.job_run_id)
        assert worker.run_once()
    audit = repository.submit_job(
        JobDefinition(job_kind="audit", job_name="Keep", payload_json="{}"),
        request_key="audit",
    )
    _age(repository)
    result = _prune(runtime, repository)
    assert result["removed_runs"] == 1
    assert {run.job_run_id for run in repository.list_runs()} == {
        pending,
        audit.job_run_id,
        *foreign,
    }
    assert runtime.status()["pending"][0]["job_run_id"] == pending


def test_retention_rechecks_active_successor_after_catalogue_acknowledgement(
    runtime_factory, tmp_path, monkeypatch
):
    _, runtime, repository, _ = _make(runtime_factory, tmp_path, monkeypatch)
    parent = _publish(runtime, repository)["run_ids"][0]
    lease = repository.lease_next_run(worker_id="review", lease_for_s=60)
    claim = dict(worker_id="review", lease_token=lease.lease_token)
    repository.mark_running(parent, **claim)
    obligation = runtime.status()["pending"][0]["obligation_id"]
    runtime.deliver(
        "projection",
        [obligation],
        job_run_id=parent,
        job_ledger_uuid=repository.ledger_uuid,
    )
    repository.mark_failed(
        parent, **claim, error_text="Receipt failure after catalogue commit"
    )
    _age(repository)
    original = maintenance_history.terminal_history_page

    def concurrent_retry(*args, **kwargs):
        page = original(*args, **kwargs)
        repository.retry_run(
            parent, request_key="concurrent", acknowledge_possible_effects=True
        )
        return page

    monkeypatch.setattr(maintenance_history, "terminal_history_page", concurrent_retry)
    assert _prune(runtime, repository)["removed_runs"] == 0
    assert len(repository.list_runs()) == 2


def test_history_deletion_failure_rolls_back_events_requests_and_definitions(
    runtime_factory, tmp_path, monkeypatch
):
    _, runtime, repository, worker = _make(runtime_factory, tmp_path, monkeypatch)
    run_id = _publish(runtime, repository)["run_ids"][0]
    assert worker.run_once()
    _age(repository)
    before = repository.list_events(run_id)
    with closing(repository.connect()) as connection, connection:
        connection.execute(
            "CREATE TRIGGER refuse_prune BEFORE DELETE ON job_runs BEGIN SELECT RAISE(ABORT, 'injected prune failure'); END"
        )
    with pytest.raises(Exception, match="injected prune failure"):
        _prune(runtime, repository)
    assert repository.list_events(run_id) == before
    assert len(repository.list_runs()) == 1
    assert _publish(runtime, repository)["published"] == 0


def test_crash_after_history_commit_is_safe_to_repeat(
    runtime_factory, tmp_path, monkeypatch
):
    _, runtime, repository, worker = _make(runtime_factory, tmp_path, monkeypatch)
    _publish(runtime, repository)
    assert worker.run_once()
    _age(repository)
    original = maintenance_history.prune_terminal_history

    def interrupted(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("interrupted after ledger commit")

    with monkeypatch.context() as patch:
        patch.setattr(maintenance_history, "prune_terminal_history", interrupted)
        with pytest.raises(RuntimeError, match="interrupted"):
            _prune(runtime, repository)
    assert _prune(runtime, repository)["removed_runs"] == 0
    assert runtime.status()["pending"] == []
    assert _publish(runtime, repository)["published"] == 0


def test_worker_late_receipt_after_retirement_is_fenced(
    runtime_factory, tmp_path, monkeypatch
):
    from LiuXin_alpha.core.maintenance_jobs import MaintenanceJobHandler
    from LiuXin_alpha.jobs.repository import JobLeaseLost

    _, runtime, repository, worker = _make(runtime_factory, tmp_path, monkeypatch)
    run_id = _publish(runtime, repository)["run_ids"][0]
    original = MaintenanceJobHandler.run
    claims = []

    def interrupted_receipt(self, *, payload_json, run_context):
        result = original(self, payload_json=payload_json, run_context=run_context)
        claim = dict(
            worker_id=run_context.worker_id, lease_token=run_context.lease_token
        )
        claims.append(claim)
        repository.mark_failed(
            run_id, **claim, error_text="Interrupted receipt after commit"
        )
        _age(repository)
        assert _prune(runtime, repository)["removed_runs"] == 1
        return result

    monkeypatch.setattr(MaintenanceJobHandler, "run", interrupted_receipt)
    assert worker.run_once()
    with pytest.raises(JobLeaseLost, match="retired"):
        repository.check_claim(run_id, **claims[0])
    assert runtime.status()["pending"] == []
    assert repository.list_runs() == []


@pytest.mark.parametrize(
    "kwargs",
    [
        {"limit": 0},
        {"limit": 501},
        {"older_than_days": -1},
        {"older_than_days": True},
        {"before_run_id": 0},
    ],
)
def test_retention_rejects_invalid_budgets(
    runtime_factory, tmp_path, monkeypatch, kwargs
):
    _, runtime, repository, _ = _make(runtime_factory, tmp_path, monkeypatch)
    with pytest.raises(ValueError):
        _prune(runtime, repository, **kwargs)
