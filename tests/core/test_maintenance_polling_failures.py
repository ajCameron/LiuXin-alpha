"""Keep queued jobs runnable when maintenance publication or its diagnostic fails."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from LiuXin_alpha.core.durable_jobs import DurableJobService
from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services.maintenance import maintenance_run
from LiuXin_alpha.jobs.api import (
    JobDefinition,
    JobHandlerAPI,
    JobHandlerRegistry,
    JobRepository,
    JobWorker,
)
from tests.jobs.test_worker_error_diagnostics import BrokenText


@pytest.mark.parametrize(
    "error", [RuntimeError("publication unavailable"), BrokenText()]
)
def test_polling_publication_failure_does_not_stop_existing_delivery(
    tmp_path, monkeypatch, caplog, error
):
    class Handler(JobHandlerAPI):
        job_kind = "queued-before-publication-failed"

        def validate_payload(self, payload_json):
            pass

        def run(self, *, payload_json, run_context):
            return {"delivered": True}

    repository = JobRepository(tmp_path / "jobs.sqlite")
    run = repository.submit_job(
        JobDefinition(job_kind=Handler.job_kind, job_name="Queued", payload_json="{}"),
        request_key="queued",
    )
    registry = JobHandlerRegistry()
    registry.register(Handler())
    worker = JobWorker(repository=repository, handlers=registry)
    service = DurableJobService(
        path=repository.sqlite_path, library_uuid="library", open_library=Mock()
    )
    service._maintenance_polling = True
    monkeypatch.setattr(service, "publish_maintenance", Mock(side_effect=error))
    original = worker.run_once

    def run_once_then_stop():
        try:
            return original()
        finally:
            worker.stop()

    monkeypatch.setattr(worker, "run_once", run_once_then_stop)
    service._poll_work(worker)
    assert repository.get_run(run.job_run_id).state.value == "succeeded"
    assert "Maintenance outbox publication failed" in caplog.text


def test_null_drain_budget_is_rejected_before_starting_jobs():
    jobs = SimpleNamespace(drain_maintenance=Mock())
    runtime = SimpleNamespace(
        database=SimpleNamespace(driver=SimpleNamespace(_maintenance_runtime=object())),
        durable_jobs=jobs,
    )
    with pytest.raises(CoreDispatchError, match="max_batches"):
        maintenance_run(
            runtime, SimpleNamespace(payload={"action": "drain", "max_batches": None})
        )
    jobs.drain_maintenance.assert_not_called()
