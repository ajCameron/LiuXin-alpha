"""
Expose replica/Asset verification, paged audits, and bounded reload/verification reconciliation through Core.

Verification can read backend bytes and update observations through the storage
manager. Adapters distinguish caught per-operation failures from later projection
errors, and do not add transactions or rollback. Reconciliation does not execute
placement, deletion, or ingest-retry actions.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services.payloads import (
    _optional_int,
    _payload,
    _required_int,
    plain,
)

if TYPE_CHECKING:
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


def storage_reconcile_plan(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    """
    Partition current recovery suggestions into reload/verification actions and explicitly deferred operations.

    Classification uses action names only, retaining manager order without executing
    them. refresh_stores is truth-tested and may request fresh backend observations.
    The advertised safe scope is a declaration, not validation of each action payload.

    Example:
        >>> plan = storage_reconcile_plan(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime whose storage manager supplies operational status and recovery actions.
    :param query: Query with optional refresh_stores=False.
    :return: Status health/projection and automatic/deferred action lists, with the fixed apply-scope explanation.
    """
    payload = _payload(query)
    status = runtime.library.storage.get_operational_status(
        refresh_stores=bool(payload.get("refresh_stores", False))
    )
    automatic = []
    deferred = []
    for action in status.recovery_actions:
        rendered = plain(action)
        if action.action in {"reload_stores", "verify_replica"}:
            automatic.append(rendered)
        else:
            deferred.append(rendered)
    return {
        "healthy": bool(status.healthy),
        "status": plain(status),
        "automatic_actions": automatic,
        "deferred_actions": deferred,
        "safe_apply_scope": (
            "Store reload and bounded Replica verification only; placement, "
            "deletion, and ingest retry remain explicit."
        ),
    }


def storage_replica_verify(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    """
    Verify one replica through the manager and expose the report's health flag.

    calculate_digests defaults True and is truth-tested. The manager owns backend
    reads and observation updates; no adapter byte/time cap or independent health
    readback is added. Verification and subsequent projection failures propagate.

    Example:
        >>> receipt = storage_replica_verify(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime supplying storage.verify_replica.
    :param command: Command with required integer-convertible replica_id and optional calculate_digests.
    :return: Replica ID, truth-tested report.healthy, and the projected verification report.
    """
    payload = _payload(command)
    replica_id = _required_int(payload, "replica_id")
    report = runtime.library.storage.verify_replica(
        replica_id,
        calculate_digests=bool(payload.get("calculate_digests", True)),
    )
    return {
        "replica_id": replica_id,
        "healthy": bool(report.healthy),
        "report": plain(report),
    }


def storage_asset_verify(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    """
    Verify an Asset using optional replica selection and report readability as the top-level healthy flag.

    replica_ids is None or a non-string Sequence of str/int/float values excluding
    bool. int conversion can truncate fractions; TypeError/ValueError are wrapped,
    but overflow remains visible. Duplicates and an explicit empty list are retained.
    all_replicas is forwarded only when present, truth-tested even for None, leaving
    the manager's default untouched when omitted. Readability need not mean every
    replica is healthy.

    Example:
        >>> receipt = storage_asset_verify(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime providing storage.verify_digital_asset and its replica-selection policy.
    :param command: Command with asset_id and optional replica_ids/all_replicas.
    :return: Asset ID, healthy derived from report.readable, and the projected detailed report.
    :raises CoreDispatchError: If Asset ID or replica-selection validation fails.
    """
    payload = _payload(command)
    asset_id = _required_int(payload, "asset_id")
    raw_ids = payload.get("replica_ids")
    replica_ids = None
    if raw_ids is not None:
        if not isinstance(raw_ids, Sequence) or isinstance(raw_ids, (str, bytes)):
            raise CoreDispatchError("`replica_ids` must be an array or null.")
        replica_ids = []
        for value in raw_ids:
            if isinstance(value, bool) or not isinstance(value, (str, int, float)):
                raise CoreDispatchError("`replica_ids` must contain integers.")
            try:
                replica_ids.append(int(value))
            except (TypeError, ValueError) as error:
                raise CoreDispatchError(
                    "`replica_ids` must contain integers."
                ) from error
    verify_options: dict[str, Any] = {"replica_ids": replica_ids}
    if "all_replicas" in payload:
        verify_options["all_replicas"] = bool(payload["all_replicas"])
    report = runtime.library.storage.verify_digital_asset(
        asset_id,
        **verify_options,
    )
    return {
        "asset_id": asset_id,
        "healthy": bool(report.readable),
        "report": plain(report),
    }


def storage_audit(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    """
    Materialize non-deleted replicas, sort by numeric ID, and verify one bounded page with per-call error receipts.

    State filtering compares exact textual values with deleted. Limit defaults to
    100, must be positive, and is capped at 10,000; offset defaults to zero and must
    be nonnegative. Explicit None bounds reach assertions. Verification errors are
    recorded and processing continues, but record conversion or successful-report
    projection errors can still abort after earlier verifications have changed state.
    ok assesses this page only and is True for an empty page.

    Example:
        >>> audit = storage_audit(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime whose storage manager enumerates and verifies replicas.
    :param command: Command with optional limit, offset, and truth-tested calculate_digests=True.
    :return: Page results, selected health/error counts, full non-deleted count, effective bounds, and has_more.
    """
    payload = _payload(command)
    limit = _optional_int(payload, "limit", default=100, minimum=1)
    offset = _optional_int(payload, "offset", default=0, minimum=0)
    assert limit is not None and offset is not None
    limit = min(limit, 10000)
    manager = runtime.library.storage
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
    for record in selected:
        try:
            report = manager.verify_replica(
                record.replica_id,
                calculate_digests=bool(payload.get("calculate_digests", True)),
            )
        except Exception as error:
            results.append(
                {
                    "replica_id": int(record.replica_id),
                    "asset_id": int(record.digital_asset_id),
                    "healthy": False,
                    "error": str(error) or type(error).__name__,
                    "error_type": type(error).__name__,
                }
            )
        else:
            results.append(
                {
                    "replica_id": int(record.replica_id),
                    "asset_id": int(record.digital_asset_id),
                    "healthy": bool(report.healthy),
                    "report": plain(report),
                }
            )
    healthy = sum(bool(item["healthy"]) for item in results)
    return {
        "ok": healthy == len(results),
        "offset": offset,
        "limit": limit,
        "total_replicas": len(records),
        "checked": len(results),
        "healthy": healthy,
        "unhealthy": len(results) - healthy,
        "has_more": offset + len(results) < len(records),
        "results": results,
    }


def storage_reconcile_apply(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    """
    Always attempt one Store reload, then verify a bounded set of candidate replicas and report later operational health.

    max_actions defaults to 100, is positive, and is capped at 10,000; the reload
    receipt consumes one action even on failure. Candidates are selected after
    reload by present/unverified/missing/unavailable/corrupt state text and sorted
    by numeric ID. Ordinary reload/verification failures become receipts; successful
    result projection, enumeration, and before/after status errors still propagate.

    Final ok uses after.healthy, not whether every action succeeded or all candidates
    fit. No supplied plan is executed and no placement/deletion/ingest retry occurs.

    Example:
        >>> receipt = storage_reconcile_apply(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime supplying manager reload, replica enumeration/verification, and status snapshots.
    :param command: Command with optional max_actions and truth-tested include_offline=False for reload.
    :return: Before/after status, action receipts, truncation flag, later-health ok, and the fixed deferred-scope explanation.
    """
    payload = _payload(command)
    max_actions = _optional_int(payload, "max_actions", default=100, minimum=1)
    assert max_actions is not None
    max_actions = min(max_actions, 10000)
    manager = runtime.library.storage
    before = manager.get_operational_status(refresh_stores=False)
    receipts: list[dict[str, Any]] = []
    try:
        reload_report = manager.reload_stores(
            include_offline=bool(payload.get("include_offline", False)),
            replace_existing=True,
        )
    except Exception as error:
        receipts.append(
            {
                "action": "reload_stores",
                "ok": False,
                "error": str(error) or type(error).__name__,
            }
        )
    else:
        receipts.append(
            {"action": "reload_stores", "ok": True, "report": plain(reload_report)}
        )
    candidates = sorted(
        (
            record
            for record in manager.iter_replica_records()
            if str(getattr(record.state, "value", record.state))
            in {"present", "unverified", "missing", "unavailable", "corrupt"}
        ),
        key=lambda record: int(record.replica_id),
    )
    remaining = max(0, max_actions - len(receipts))
    for record in candidates[:remaining]:
        try:
            report = manager.verify_replica(record.replica_id)
        except Exception as error:
            receipts.append(
                {
                    "action": "verify_replica",
                    "replica_id": int(record.replica_id),
                    "ok": False,
                    "error": str(error) or type(error).__name__,
                }
            )
        else:
            receipts.append(
                {
                    "action": "verify_replica",
                    "replica_id": int(record.replica_id),
                    "ok": bool(report.healthy),
                    "report": plain(report),
                }
            )
    after = manager.get_operational_status(refresh_stores=False)
    return {
        "ok": bool(after.healthy),
        "before": plain(before),
        "after": plain(after),
        "actions": receipts,
        "actions_truncated": len(candidates) > remaining,
        "deferred": (
            "Replica placement, deletion, and ingest retry require their "
            "dedicated explicit commands."
        ),
    }
