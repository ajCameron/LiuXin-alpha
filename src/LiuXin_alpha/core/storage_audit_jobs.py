"""Run the shared audit orchestration through storage's verification owner.

The durable handler opens and closes its own LibraryServices graph. It records
per-replica receipts and checks cancellation between verifications; an in-flight
backend read cannot be preempted. Verification persists storage observations.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any

from LiuXin_alpha.configuration import use_preferences
from LiuXin_alpha.core.program_services.payloads import plain
from LiuXin_alpha.core.wire import to_wire
from LiuXin_alpha.jobs.handler_api import JobHandlerAPI, JobRunContext
from LiuXin_alpha.jobs.models import JobProgressUpdate

AUDIT_JOB_KIND = "core.storage.audit.v1"


def run_storage_audit(
    manager: Any,
    *,
    limit: int,
    offset: int,
    calculate_digests: bool,
    run_context: JobRunContext | None = None,
) -> dict[str, Any]:
    """Inspect the current page once, retaining completed receipts on cancellation."""
    records = sorted(
        (
            record
            for record in manager.iter_replica_records()
            if str(getattr(record.state, "value", record.state)) != "deleted"
        ),
        key=lambda record: int(record.replica_id),
    )
    selected = records[offset : offset + limit]
    results: list[dict[str, Any]] = []
    cancelled = False
    if run_context is not None:
        run_context.update_progress(
            JobProgressUpdate(
                progress_current=0,
                progress_total=len(selected),
                progress_unit="replicas",
                progress_message="Inspecting selected replicas",
            )
        )
    for record in selected:
        if run_context is not None and run_context.is_cancel_requested():
            cancelled = True
            break
        entry = _verify_replica(manager, record, calculate_digests=calculate_digests)
        results.append(entry)
        if run_context is not None:
            run_context.log(
                f"Inspected Replica {record.replica_id}",
                event_json=json.dumps(to_wire(entry), sort_keys=True, allow_nan=False),
            )
            run_context.update_progress(
                JobProgressUpdate(
                    progress_current=len(results),
                    progress_total=len(selected),
                    progress_unit="replicas",
                    progress_message=f"Inspected Replica {record.replica_id}",
                )
            )
    if run_context is not None:
        cancelled = cancelled or run_context.is_cancel_requested()
    healthy = sum(bool(item["healthy"]) for item in results)
    receipt = {
        "ok": not cancelled and healthy == len(results),
        "offset": offset,
        "limit": limit,
        "total_replicas": len(records),
        "checked": len(results),
        "healthy": healthy,
        "unhealthy": len(results) - healthy,
        "has_more": offset + len(results) < len(records),
        "results": results,
    }
    if run_context is not None:
        receipt.update(result_version=1, cancelled=cancelled)
    return receipt


def _verify_replica(
    manager: Any, record: Any, *, calculate_digests: bool
) -> dict[str, Any]:
    """Keep the synchronous API's per-replica error and report shapes."""
    entry: dict[str, Any] = {
        "replica_id": int(record.replica_id),
        "asset_id": int(record.digital_asset_id),
    }
    try:
        report = manager.verify_replica(
            record.replica_id, calculate_digests=calculate_digests
        )
    except Exception as error:
        entry.update(
            healthy=False,
            error=str(error) or type(error).__name__,
            error_type=type(error).__name__,
        )
    else:
        entry.update(healthy=bool(report.healthy), report=plain(report))
    return entry


class StorageAuditHandler(JobHandlerAPI):
    """Bind the persisted audit intent to one independently opened library."""

    job_kind = AUDIT_JOB_KIND

    def __init__(
        self,
        *,
        library_uuid: str,
        open_library: Callable[[], AbstractContextManager[Any]],
    ) -> None:
        self.library_uuid = library_uuid
        self.open_library = open_library

    def validate_payload(self, payload_json: str) -> None:
        payload = json.loads(payload_json)
        if payload.get("library_uuid") != self.library_uuid:
            raise ValueError("Audit belongs to a different library")
        for field, minimum, maximum in (("limit", 1, 10000), ("offset", 0, None)):
            value = payload.get(field)
            if (
                type(value) is not int
                or value < minimum
                or (maximum is not None and value > maximum)
            ):
                raise ValueError(f"Invalid audit {field}")
        if type(payload.get("calculate_digests")) is not bool:
            raise ValueError("calculate_digests must be a bool")

    def run(self, *, payload_json: str, run_context: JobRunContext) -> dict[str, Any]:
        payload = json.loads(payload_json)
        with self.open_library() as services:
            if str(services.library.db.uuid) != self.library_uuid:
                raise ValueError("Library identity changed before audit execution")
            with use_preferences(services.core_services.preferences):
                return to_wire(
                    run_storage_audit(
                        services.library.storage,
                        limit=payload["limit"],
                        offset=payload["offset"],
                        calculate_digests=payload["calculate_digests"],
                        run_context=run_context,
                    )
                )
