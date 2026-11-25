"""Adapt captured application workflows to the existing durable jobs executor."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from LiuXin_alpha.configuration import CapturedPreferences, use_preferences
from LiuXin_alpha.core.wire import to_wire
from LiuXin_alpha.databases import command_outbox
from LiuXin_alpha.jobs.handler_api import (
    JobHandlerAPI,
    JobOutcomeFailure,
    JobRunContext,
)
from LiuXin_alpha.jobs.models import JobDefinition, JobResultPolicy

KIND = "core.workflow.v1"
FUNCTIONS = frozenset(
    (
        "run_ingest_disk_job",
        "run_application_operation_job",
        "run_sync_store_job",
        "run_ingest_remote_html_job",
        "run_ingest_web_job",
        "run_conversion_job",
        "run_squashfs_backup_job",
        "run_publish_open_squashfs_store_job",
        "run_publish_squashfs_files_job",
        "run_persisted_backup_job",
        "run_metadata_identify_job",
        "run_metadata_cover_job",
    )
)


class WorkflowHandler(JobHandlerAPI):
    """Execute only registered Core workflows; arbitrary imports are never accepted."""

    job_kind = KIND
    guarded_execution = True

    def execution_key(self, payload_json: str) -> str | None:
        payload = json.loads(payload_json)
        if payload["function"] == "run_persisted_backup_job":
            return f"backup:{self.library_uuid}:{payload['kwargs']['workflow_id']}"
        return None

    def can_resume(self, payload_json: str) -> bool:
        self.validate_payload(payload_json)
        payload = json.loads(payload_json)
        if payload["function"] == "run_persisted_backup_job":
            from LiuXin_alpha.core.workflow_jobs import _open_library_services
            from LiuXin_alpha.storage.backup import BackupWorkflowRepository
            from LiuXin_alpha.storage.backup.recovery import can_resume_staging

            kwargs = payload["kwargs"]
            with use_preferences(CapturedPreferences(**payload["preferences"])):
                with _open_library_services(
                    database_path=kwargs["database_path"],
                    db_type=kwargs["db_type"],
                    database_metadata=kwargs.get("database_metadata"),
                ) as services:
                    if str(services.library.db.uuid) != self.library_uuid:
                        return False
                    checkpoint = BackupWorkflowRepository(
                        services.library.db
                    ).load_checkpoint(kwargs["workflow_id"])
                    return can_resume_staging(checkpoint)
        return payload["function"] in {
            "run_ingest_disk_job",
            "run_metadata_identify_job",
            "run_metadata_cover_job",
        }

    def __init__(self, library_uuid: str):
        self.library_uuid = library_uuid

    def validate_payload(self, payload_json: str) -> None:
        payload = json.loads(payload_json)
        if payload.get("library_uuid") != self.library_uuid:
            raise ValueError("Workflow belongs to another library")
        if payload.get("function") not in FUNCTIONS or not isinstance(
            payload.get("kwargs"), dict
        ):
            raise ValueError("Unknown workflow or invalid captured arguments")
        if not isinstance(payload.get("preferences", {}).get("values"), dict):
            raise ValueError("Workflow requires captured preferences")

    def run(self, *, payload_json: str, run_context: JobRunContext) -> dict[str, Any]:
        self.validate_payload(payload_json)
        payload = json.loads(payload_json)
        from LiuXin_alpha.core import workflow_jobs
        from LiuXin_alpha.core.workflow_context import workflow_context
        from LiuXin_alpha.storage.operation_context import operation_scope

        preferences = CapturedPreferences(**payload["preferences"])
        run_context.raise_if_cancel_requested()
        with (
            use_preferences(preferences),
            workflow_context(run_context, self.library_uuid),
            operation_scope(run_context.raise_if_cancel_requested),
        ):
            result = to_wire(
                getattr(workflow_jobs, payload["function"])(**payload["kwargs"])
            )
        # Providers which use an event-like cancellation API return normally.
        # Acknowledgement belongs to the executor and must retain timeout state.
        if run_context.deadline_exceeded():
            from LiuXin_alpha.jobs.handler_api import JobDeadlineExceeded

            raise JobDeadlineExceeded(
                "Workflow returned after its execution deadline; inspect retained effects",
                result,
            )
        if run_context.is_cancel_requested():
            raise JobOutcomeFailure(
                "Cancellation acknowledged after workflow returned; inspect retained effects",
                result,
                cancelled=True,
            )
        state = result.get("state", result.get("status", ""))
        if isinstance(state, dict):
            state = state.get("status", "")
        state = str(state).lower()
        if (
            state in ("failed", "cancelled", "aborted")
            or result.get("ok") is False
            or result.get("errors")
            or result.get("exists") is False
        ):
            raise JobOutcomeFailure(
                "Workflow reported incomplete effects; inspect its receipt",
                result,
                cancelled=state == "cancelled",
            )
        return result


def publish(service: Any, *, limit: int = 32) -> list[str]:
    """Deliver committed intents idempotently, retaining undelivered work on failure."""
    opener = getattr(service, "_open_outbox_library", service._open_library)
    with opener() as services:
        database = services.library.db
        if str(database.uuid) != service.library_uuid:
            raise ValueError("Outbox belongs to a replaced catalogue")
        schema = getattr(database.driver, "schema", None)
        with database.macros.read_transaction() as connection:
            entries = command_outbox.pending(connection, schema, limit)
        delivered = []
        for command_id, intent in entries:
            payload = json.dumps(intent, sort_keys=True, allow_nan=False)
            handler = WorkflowHandler(service.library_uuid)
            handler.validate_payload(payload)
            run = service.repository.submit_job(
                JobDefinition(
                    job_kind=KIND,
                    job_name=intent["label"],
                    payload_json=payload,
                    timeout_s=intent.get("timeout_s"),
                    result_policy=JobResultPolicy.KEEP_ALL,
                ),
                request_key="core.command:"
                + hashlib.sha256(command_id.encode("utf-8")).hexdigest(),
            )
            job_id = service._job_id(run.job_run_id)
            with database.macros.transaction(serialized=True) as connection:
                command_outbox.acknowledge(connection, command_id, job_id, schema)
            delivered.append(job_id)
        return delivered
