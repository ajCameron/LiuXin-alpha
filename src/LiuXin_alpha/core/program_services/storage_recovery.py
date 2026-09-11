"""
Expose durable ingest-operation inspection, pending recovery, and explicit retry through Core.

Recovery and retry run synchronously in the storage manager. The adapters report
manager outcomes without adding a transaction around recovery and subsequent
listing/projection; reporting can therefore fail after state has changed.
"""

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
    """
    Materialize ingest operations, optionally filter their state, and return a bounded page.

    Requested state is stripped and casefolded, but record state is only casefolded.
    Manager order is preserved. Limit defaults to 100, must be positive, and is
    capped at 10,000; offset defaults to zero and must be nonnegative. Explicit
    None reaches non-None assertions. complete indicates that the page reaches
    the filtered end, unlike the query-coverage flag on structured row queries.

    Example:
        >>> from types import SimpleNamespace
        >>> from unittest.mock import Mock
        >>> manager = Mock()
        >>> manager.list_ingest_operations.return_value = [{"state": "pending"}]
        >>> runtime = SimpleNamespace(library=SimpleNamespace(storage=manager))
        >>> storage_recovery_list(runtime, SimpleNamespace(payload={"state": "PENDING"}))["total"]
        1


    :param runtime: Runtime whose storage manager lists durable ingest-operation mappings.
    :param query: Query with optional state filter and limit/offset page bounds.
    :return: Plain-projected operations, filtered total, effective bounds, and end-of-list completeness.
    """
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
    """
    Recover one UUID-selected pending ingest, or all pending ingests when no selector is supplied.

    None/empty operation_id becomes None; nonempty values are stringified without
    stripping before UUID parsing. ok tests the returned issues collection's
    truthiness before materializing it. The final operation listing is unfiltered
    and occurs after recovery, so it can fail after successful recovery work.

    Example:
        >>> receipt = storage_recovery_recover_pending(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime providing synchronous storage recovery and operation listing.
    :param command: Command with optional operation_id; no separate confirmation is consumed.
    :return: ok, parsed UUID-or-None selector, materialized issues, and all projected operations.
    :raises CoreDispatchError: If UUID construction raises ValueError for the supplied selector.
    """
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
    """
    Retry one required UUID-selected ingest synchronously and project the manager's result.

    Required text is stripped before UUID parsing. ok=True means the retry call
    returned; this adapter does not independently inspect the result for success.
    Manager eligibility errors propagate without a second recovery attempt.

    Example:
        >>> receipt = storage_recovery_retry_ingest(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime whose storage manager implements retry_ingest_operation.
    :param command: Command with required operation_id text parseable as a UUID.
    :return: ok=True, parsed operation UUID, and plain-projected retry result.
    :raises CoreDispatchError: If operation_id is missing/blank or UUID parsing raises ValueError.
    """
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
