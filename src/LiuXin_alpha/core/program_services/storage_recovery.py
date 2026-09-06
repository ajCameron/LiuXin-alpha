"""Core-owned storage recovery operations and wire translation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services.payloads import (
    _optional_int,
    _payload,
    _required_text,
    plain,
)

if TYPE_CHECKING:
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


def storage_recovery_list(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    payload = _payload(query)
    state_filter = str(payload.get("state") or "").strip().casefold()
    limit = _optional_int(payload, "limit", default=100, minimum=1)
    offset = _optional_int(payload, "offset", default=0, minimum=0)
    assert limit is not None and offset is not None
    manager = runtime.library.storage
    records = [dict(value) for value in manager.list_ingest_operations()]
    if state_filter:
        records = [
            value
            for value in records
            if str(value.get("state") or "").casefold() == state_filter
        ]
    selected = records[offset : offset + min(limit, 10_000)]
    return {
        "operations": [plain(value) for value in selected],
        "total": len(records),
        "offset": offset,
        "limit": min(limit, 10_000),
        "complete": offset + len(selected) >= len(records),
    }


def storage_recovery_recover_pending(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    operation_raw = payload.get("operation_id")
    try:
        operation_id = None if operation_raw in (None, "") else UUID(str(operation_raw))
    except ValueError as error:
        raise CoreDispatchError("`operation_id` must be a UUID.") from error
    manager = runtime.library.storage
    issues = manager.recover_pending_ingests(operation_id)
    return {
        "ok": not issues,
        "operation_id": operation_id,
        "issues": list(issues),
        "operations": plain(manager.list_ingest_operations()),
    }


def storage_recovery_retry_ingest(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    try:
        operation_id = UUID(_required_text(payload, "operation_id"))
    except ValueError as error:
        raise CoreDispatchError("`operation_id` must be a UUID.") from error
    result = runtime.library.storage.retry_ingest_operation(operation_id)
    return {
        "ok": True,
        "operation_id": operation_id,
        "result": plain(result),
    }
