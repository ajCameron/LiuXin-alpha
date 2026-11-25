"""Expose verified restoration and reviewed exact recreation through domain owners."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.payloads import integer_value
from LiuXin_alpha.core.program_services.store_resolution import _store
from LiuXin_alpha.core.wire import to_wire
from LiuXin_alpha.core.workflow_context import checkpoint


def _positive(value: Any, name: str) -> int:
    number = integer_value(value, name)
    if number < 1:
        raise CoreDispatchError(f"{name} must be positive", code="invalid_payload")
    return number


def plan(runtime: Any, request: Any) -> dict[str, Any]:
    """Retain the selected recipes and current availability in a review fingerprint."""
    asset_id = _positive(request.payload.get("asset_id"), "asset_id")
    selected = runtime.library.storage.plan_digital_asset_recreation(asset_id)
    facts = to_wire(selected)
    fingerprint = hashlib.sha256(
        json.dumps(facts, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return {
        "plan": facts,
        "expected_state": fingerprint,
        "can_recreate_exactly": selected.can_recreate_exactly,
    }


def _destination(runtime: Any, payload: dict[str, Any]) -> Any:
    reference = payload.get("store")
    return (
        None
        if reference is None
        else _store(runtime, reference).configuration.store_uuid
    )


def restore(runtime: Any, command: Any) -> dict[str, Any]:
    """Stream and verify a new active copy, retaining the original Asset identity."""
    from LiuXin_alpha.storage.workflows.recreation import restore_asset

    payload = command.payload
    asset_id = _positive(payload.get("asset_id"), "asset_id")
    source = payload.get("source_replica_id")
    if source is not None:
        source = _positive(source, "source_replica_id")
    if payload.get("metadata"):
        raise CoreDispatchError(
            "Restoring bytes preserves Asset metadata; edit metadata separately",
            code="invalid_payload",
        )
    replica = restore_asset(
        runtime.library.storage,
        asset_id,
        destination=_destination(runtime, payload),
        source_replica_id=source,
        checkpoint=checkpoint,
    )
    return {"ok": True, "asset_id": asset_id, "replica": to_wire(replica)}


def recreate(runtime: Any, command: Any) -> dict[str, Any]:
    """Require the reviewed plan before invoking pinned privileged recipe execution."""
    from LiuXin_alpha.storage.workflows.recreation import recreate_asset

    current = plan(runtime, command)
    if command.payload.get("expected_state") != current["expected_state"]:
        raise CoreDispatchError(
            "Recreation plan changed; inspect a fresh plan", code="recreation_conflict"
        )
    result = recreate_asset(
        runtime.library.storage,
        command.payload["asset_id"],
        destination=_destination(runtime, command.payload),
        max_steps=_positive(command.payload.get("max_steps", 32), "max_steps"),
        max_bytes=_positive(command.payload.get("max_bytes", 1024**4), "max_bytes"),
        checkpoint=checkpoint,
    )
    return {"ok": True, **result}
