"""Prove an expired lease cannot overlap a surviving effectful executor."""

import threading

from LiuXin_alpha.jobs.handler_api import JobHandlerAPI, JobHandlerRegistry
from LiuXin_alpha.jobs.models import JobDefinition, JobRunState
from LiuXin_alpha.jobs.recovery import resume_interrupted
from LiuXin_alpha.jobs.repository import JobRepository
from LiuXin_alpha.jobs.worker import JobWorker


class Resumable(JobHandlerAPI):
    job_kind = "test.resumable"
    guarded_execution = True

    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.calls = 0

    def validate_payload(self, payload_json):
        pass

    def can_resume(self, payload_json):
        return True

    def run(self, *, payload_json, run_context):
        self.calls += 1
        self.entered.set()
        assert self.release.wait(10)
        return {"checkpoint": "complete"}


def test_recovery_waits_for_surviving_executor_and_reuses_successor(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    handler = Resumable()
    handlers = JobHandlerRegistry()
    handlers.register(handler)
    run = repository.submit_job(
        JobDefinition(job_kind=handler.job_kind, job_name="Resume", payload_json="{}"),
        request_key="test",
    )
    worker = JobWorker(repository=repository, handlers=handlers, lease_for_s=300)
    thread = threading.Thread(target=worker.run_once)
    thread.start()
    try:
        assert handler.entered.wait(5)
        with repository._connection(write=True) as connection:
            connection.execute(
                "UPDATE job_runs SET lease_expires_timestamp_ep_k=1 WHERE job_run_id=?",
                (run.job_run_id,),
            )
        assert repository.recover_expired_runs() == [run.job_run_id]
        assert resume_interrupted(repository, handlers)[0] == []
        assert len(repository.list_runs()) == 1
    finally:
        handler.release.set()
        thread.join(5)
    assert not thread.is_alive()
    assert repository.get_run(run.job_run_id).state is JobRunState.ABORTED
    resumed, _ = resume_interrupted(repository, handlers)
    assert len(resumed) == 1
    assert resume_interrupted(repository, handlers)[0] == []
    assert worker.run_once()
    assert repository.get_run(resumed[0]).state is JobRunState.SUCCEEDED
    assert handler.calls == 2


def test_unguarded_or_uncertain_handler_is_never_replayed(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    handler = Resumable()
    handler.guarded_execution = False
    handlers = JobHandlerRegistry()
    handlers.register(handler)
    run = repository.submit_job(
        JobDefinition(job_kind=handler.job_kind, job_name="Held", payload_json="{}"),
        request_key="test",
    )
    lease = repository.lease_next_run(worker_id="gone", lease_for_s=300)
    repository.mark_running(
        run.job_run_id, worker_id="gone", lease_token=lease.lease_token
    )
    with repository._connection(write=True) as connection:
        connection.execute("UPDATE job_runs SET lease_expires_timestamp_ep_k=1")
    repository.recover_expired_runs()
    assert resume_interrupted(repository, handlers)[0] == []
    assert repository.get_run(run.job_run_id).state is JobRunState.ABORTED


def test_cancellation_between_proof_and_retry_prevents_continuation(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    handler = Resumable()
    handlers = JobHandlerRegistry()
    handlers.register(handler)
    run = repository.submit_job(
        JobDefinition(job_kind=handler.job_kind, job_name="Race", payload_json="{}"),
        request_key="race",
    )
    lease = repository.lease_next_run(worker_id="gone", lease_for_s=300)
    repository.mark_running(
        run.job_run_id, worker_id="gone", lease_token=lease.lease_token
    )
    with repository._connection(write=True) as connection:
        connection.execute("UPDATE job_runs SET lease_expires_timestamp_ep_k=1")
    repository.recover_expired_runs()

    def proof(payload):
        assert repository.cancel_action(run.job_run_id)
        return True

    handler.can_resume = proof
    assert resume_interrupted(repository, handlers)[0] == []
    assert len(repository.list_runs()) == 1
    assert repository.get_run(run.job_run_id).recovery_state == "held"


def test_os_guard_survives_lease_expiry_and_releases_only_when_process_dies(tmp_path):
    import os
    import subprocess
    import sys
    import time
    from pathlib import Path

    repository = JobRepository(tmp_path / "jobs.sqlite")
    handler = Resumable()
    handler.release.set()
    handlers = JobHandlerRegistry()
    handlers.register(handler)
    run = repository.submit_job(
        JobDefinition(job_kind=handler.job_kind, job_name="Process", payload_json="{}"),
        request_key="process",
    )
    ready = tmp_path / "entered"
    worker = """
import sys, time
from pathlib import Path
from LiuXin_alpha.jobs.repository import JobRepository
from LiuXin_alpha.jobs.execution_guard import execution_guard
repo = JobRepository(sys.argv[1])
claim = repo.lease_next_run(worker_id="child", lease_for_s=300)
repo.mark_running(claim.job_run_id, worker_id="child", lease_token=claim.lease_token)
with execution_guard(repo, claim.job_run_id):
    Path(sys.argv[2]).touch()
    while True:
        time.sleep(1)
"""
    process = subprocess.Popen(
        [sys.executable, "-c", worker, str(repository.sqlite_path), str(ready)],
        env={
            **os.environ,
            "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
        },
    )
    try:
        deadline = time.monotonic() + 15
        while not ready.exists() and time.monotonic() < deadline:
            assert process.poll() is None
            time.sleep(0.02)
        assert ready.exists()
        with repository._connection(write=True) as connection:
            connection.execute("UPDATE job_runs SET lease_expires_timestamp_ep_k=1")
        repository.recover_expired_runs()
        assert resume_interrupted(repository, handlers)[0] == []
        assert repository.get_run(run.job_run_id).recovery_state == "waiting_executor"
    finally:
        process.kill()
        process.wait(timeout=10)
    resumed, _ = resume_interrupted(repository, handlers)
    assert len(resumed) == 1
    assert JobWorker(repository=repository, handlers=handlers).run_once()
    assert repository.latest_attempt(run.job_run_id).state is JobRunState.SUCCEEDED
