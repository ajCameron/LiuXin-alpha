"""Core-owned storage integrity operations and wire translation."""

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
