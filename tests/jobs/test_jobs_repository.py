from __future__ import annotations

import json

from LiuXin_alpha.jobs.api import (
    JobConcurrencyPolicy,
    JobDefinition,
    JobEventKind,
    JobProgressUpdate,
    JobRepository,
    JobResultPolicy,
    JobRunState,
    JobScheduler,
    JobTriggerKind,
)


def test_job_repository_round_trip_and_run_lifecycle(tmp_path) -> None:
    repo = JobRepository(tmp_path / "jobs.sqlite")
    definition = repo.create_definition(
        JobDefinition(
            job_kind="example",
            job_name="Example Job",
            payload_json=json.dumps({"hello": "world"}),
            schedule_json=json.dumps({"interval_seconds": 60}),
            concurrency_policy=JobConcurrencyPolicy.ALLOW_PARALLEL,
            result_policy=JobResultPolicy.KEEP_ALL,
        )
    )
    assert definition.job_definition_id is not None

    queued = repo.enqueue_run(job_definition_id=int(definition.job_definition_id), trigger_kind=JobTriggerKind.MANUAL)
    assert queued.state is JobRunState.QUEUED

    leased = repo.lease_next_run(worker_id="worker-1", lease_for_s=30)
    assert leased is not None
    assert leased.job_run_id == queued.job_run_id
    assert leased.state is JobRunState.LEASED

    running = repo.mark_running(int(queued.job_run_id), worker_id="worker-1")
    assert running.state is JobRunState.RUNNING

    repo.update_progress(int(queued.job_run_id), JobProgressUpdate(progress_current=2, progress_total=5, progress_message="Doing work"))
    repo.heartbeat(int(queued.job_run_id), worker_id="worker-1", message="still alive")
    completed = repo.mark_succeeded(int(queued.job_run_id), result_json=json.dumps({"ok": True}))
    assert completed.state is JobRunState.SUCCEEDED

    refreshed = repo.get_run(int(queued.job_run_id))
    assert refreshed.result_json == json.dumps({"ok": True})
    events = repo.list_events(int(queued.job_run_id))
    assert any(evt.event_kind is JobEventKind.QUEUED for evt in events)
    assert any(evt.event_kind is JobEventKind.PROGRESS for evt in events)
    assert any(evt.event_kind is JobEventKind.SUCCEEDED for evt in events)

    due = repo.get_due_definitions_for_scheduling(now_timestamp_ep_k=(definition.created_timestamp_ep_k or 0) + 61000)
    assert any(one.job_definition_id == definition.job_definition_id for one in due)


def test_job_scheduler_materialises_due_runs(tmp_path) -> None:
    repo = JobRepository(tmp_path / "jobs.sqlite")
    definition = repo.create_definition(
        JobDefinition(
            job_kind="example",
            job_name="Scheduled Example",
            payload_json="{}",
            schedule_json=json.dumps({"interval_seconds": 1}),
        )
    )
    scheduler = JobScheduler(repo)
    queued = scheduler.tick(now_timestamp_ep_k=(definition.created_timestamp_ep_k or 0) + 1500)
    assert queued == 1
    runs = repo.list_runs(job_definition_id=int(definition.job_definition_id))
    assert len(runs) == 1
    assert runs[0].trigger_kind is JobTriggerKind.SCHEDULED


def test_concurrency_queue_one_blocks_duplicate_enqueue(tmp_path) -> None:
    repo = JobRepository(tmp_path / "jobs.sqlite")
    definition = repo.create_definition(
        JobDefinition(
            job_kind="example",
            job_name="Singleton",
            payload_json="{}",
            concurrency_policy=JobConcurrencyPolicy.QUEUE_ONE,
        )
    )
    repo.enqueue_run(job_definition_id=int(definition.job_definition_id))
    try:
        repo.enqueue_run(job_definition_id=int(definition.job_definition_id))
    except RuntimeError as exc:
        assert "queued/running" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError for duplicate enqueue")
