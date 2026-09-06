"""Core envelope adapters for typed evacuation planning and execution."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services.evacuation_execution import execute_evacuation
from LiuXin_alpha.core.program_services.evacuation_models import (
    EvacuationLimits,
    EvacuationPlan,
)
from LiuXin_alpha.core.program_services.evacuation_planning import build_evacuation_plan
from LiuXin_alpha.core.program_services.payloads import _optional_int as optional_int
from LiuXin_alpha.core.program_services.payloads import _payload as payload
from LiuXin_alpha.core.program_services.store_resolution import _store

if TYPE_CHECKING:
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


def _plan(
    runtime: CoreRuntime,
    *,
    store_reference: Any,
    destination_reference: Any | None,
    max_assets: int,
) -> EvacuationPlan:
    source_ref = _store(runtime, store_reference).store_ref
    destination_ref = (
        None
        if destination_reference in (None, "")
        else _store(runtime, destination_reference).store_ref
    )
    return build_evacuation_plan(
        runtime.library.storage,
        source_ref=source_ref,
        destination_ref=destination_ref,
        max_assets=max_assets,
    )


def _storage_store_evacuation_plan_payload(
    runtime: CoreRuntime,
    *,
    store_reference: Any,
    destination_reference: Any | None,
    max_assets: int,
) -> dict[str, object]:
    """Retain the historical plan helper's wire result for compatibility."""
    return _plan(
        runtime,
        store_reference=store_reference,
        destination_reference=destination_reference,
        max_assets=max_assets,
    ).to_wire()


def storage_store_evacuate_plan(
    runtime: CoreRuntime, query: CoreQuery
) -> dict[str, object]:
    values = payload(query)
    reference = values.get("store")
    if reference in (None, ""):
        raise CoreDispatchError("`store` is required.")
    max_assets = optional_int(values, "max_assets", default=100, minimum=1)
    assert max_assets is not None
    return _storage_store_evacuation_plan_payload(
        runtime,
        store_reference=reference,
        destination_reference=values.get("destination_store"),
        max_assets=min(max_assets, 10_000),
    )


def storage_store_evacuate_apply(
    runtime: CoreRuntime, command: CoreCommand
) -> dict[str, object]:
    values = payload(command)
    reference = values.get("store")
    if reference in (None, ""):
        raise CoreDispatchError("`store` is required.")
    max_assets = optional_int(values, "max_assets", default=100, minimum=1)
    max_actions = optional_int(values, "max_actions", default=1000, minimum=1)
    max_transfer_bytes = optional_int(
        values, "max_transfer_bytes", default=1024**4, minimum=1
    )
    assert (
        max_assets is not None
        and max_actions is not None
        and max_transfer_bytes is not None
    )
    max_assets = min(max_assets, 10_000)
    before = _plan(
        runtime,
        store_reference=reference,
        destination_reference=values.get("destination_store"),
        max_assets=max_assets,
    )
    keep_source_bytes = bool(values.get("keep_source_bytes", False))
    execution = execute_evacuation(
        runtime.library.storage,
        before,
        EvacuationLimits(min(max_actions, 100_000), max_transfer_bytes),
        keep_source_bytes=keep_source_bytes,
    )
    after = _plan(
        runtime,
        store_reference=reference,
        destination_reference=values.get("destination_store"),
        max_assets=max_assets,
    )
    return {
        "ok": not execution.failures
        and not execution.truncated
        and after.replicas_planned == 0,
        "before": before.to_wire(),
        "after": after.to_wire(),
        "actions": execution.actions,
        "actions_applied": len(execution.actions),
        "actions_failed": execution.failures,
        "actions_truncated": execution.truncated,
        "transferred_bytes": execution.transferred_bytes,
        "source_bytes_retained": execution.source_bytes_retained,
    }
