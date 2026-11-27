"""Exercise the catalogue/ledger crash window with real independent repositories."""

import json
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from LiuXin_alpha.core import CoreRuntime
from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.errors import CoreError
from LiuXin_alpha.core.workflow_delivery import WorkflowHandler, publish
from LiuXin_alpha.databases import command_outbox
from LiuXin_alpha.jobs.handler_api import JobHandlerRegistry
from LiuXin_alpha.jobs.models import JobRunState
from LiuXin_alpha.jobs.worker import JobWorker
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)


def delivery_service(database, path):
    from LiuXin_alpha.core.durable_jobs import DurableJobService

    service = DurableJobService(
        path=path,
        library_uuid=str(database.uuid),
        open_library=lambda: nullcontext(
            SimpleNamespace(library=SimpleNamespace(db=database))
        ),
    )
    return service


def test_outbox_duplicate_delivery_after_lost_acknowledgement(
    metadata_db, tmp_path, monkeypatch
):
    service = delivery_service(metadata_db, tmp_path / "jobs.sqlite")
    with CoreRuntime(library=SimpleNamespace(database=metadata_db)) as runtime:
        runtime._durable_jobs = service
        command = CoreCommand("metadata.identify.start", {"title": "One request"})
        accepted = runtime.execute_command(command).result
        assert accepted["job_id"] is None and accepted["state"] == "accepted"
        assert runtime.execute_command(command).result == accepted
        with monkeypatch.context() as broken:
            broken.setattr(
                command_outbox,
                "acknowledge",
                lambda *args: (_ for _ in ()).throw(OSError("lost ack")),
            )
            with pytest.raises(OSError):
                publish(service)
        assert len(service.repository.list_runs()) == 1
        publish(service)
        assert len(service.repository.list_runs()) == 1
        outcome = runtime.query("commands.outcome", {"command_id": command.command_id})
        assert outcome["job_id"] == service._job_id(1)
        assert outcome["current_job_id"] == outcome["job_id"]
        assert outcome["effects"] == "pending"


def test_failed_acceptance_does_not_leave_queued_work(
    metadata_db, tmp_path, monkeypatch
):
    service = delivery_service(metadata_db, tmp_path / "jobs.sqlite")
    from LiuXin_alpha.databases import command_journal

    with CoreRuntime(library=SimpleNamespace(database=metadata_db)) as runtime:
        runtime._durable_jobs = service
        command = CoreCommand("metadata.identify.start", {"title": "Rollback"})
        with monkeypatch.context() as broken:
            broken.setattr(
                command_journal,
                "insert",
                lambda *args: (_ for _ in ()).throw(OSError("receipt write fails")),
            )
            with pytest.raises(CoreError):
                runtime.execute_command(command)
        assert publish(service) == []
        assert not service.repository.list_runs()
        assert (
            runtime.query("commands.outcome", {"command_id": command.command_id})[
                "state"
            ]
            == "not_found"
        )


def test_worker_preserves_domain_failure_receipt_and_captured_preferences(
    metadata_db, tmp_path, monkeypatch
):
    service = delivery_service(metadata_db, tmp_path / "jobs.sqlite")
    preferences = {"test_marker": "captured"}
    with CoreRuntime(
        library=SimpleNamespace(database=metadata_db), preferences=preferences
    ) as runtime:
        runtime._durable_jobs = service
        runtime.command("metadata.identify.start", {"title": "Incomplete"})
        preferences["test_marker"] = "changed"
        publish(service)
        from LiuXin_alpha.configuration import current_preferences
        from LiuXin_alpha.core import workflow_jobs

        def incomplete(**kwargs):
            assert current_preferences()["test_marker"] == "captured"
            return {
                "status": "failed",
                "completed": [1],
                "errors": ["one provider failed"],
            }

        monkeypatch.setattr(workflow_jobs, "run_metadata_identify_job", incomplete)
        handlers = JobHandlerRegistry()
        handlers.register(WorkflowHandler(str(metadata_db.uuid)))
        assert JobWorker(repository=service.repository, handlers=handlers).run_once()
        run = service.repository.get_run(1)
        assert run.state is JobRunState.FAILED
        assert json.loads(run.result_json)["completed"] == [1]
