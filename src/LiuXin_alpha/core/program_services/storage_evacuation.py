"""
Adapt Core evacuation requests into live Store resolution, typed plans, bounded execution, and wire receipts.

Planning reads manager state without copying/removing bytes. Apply builds a fresh
plan, executes it synchronously, and builds another plan afterward; it does not
consume a caller-supplied plan receipt or wrap the sequence in a transaction.
Lookup, validation, planning, or post-write reporting errors can propagate to the
outer Core handler boundary.
"""

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
    """
    Resolve source and optional destination as live Stores, then build a typed evacuation snapshot.

    No bound normalization occurs here. Destination None or the empty string
    permits automatic selection; other references pass through the live resolver.

    Example:
        >>> plan = _plan(runtime, store_reference="source", destination_reference=None, max_assets=100)  # doctest: +SKIP


    :param runtime: Runtime whose library storage manager and database support Store reference resolution.
    :param store_reference: Source UUID, numeric store-row identity, or exact live Store name.
    :param destination_reference: Optional live Store reference; None/empty text leaves selection unconstrained.
    :param max_assets: Slice bound forwarded unchanged to the typed planner.
    :return: Unreserved typed plan from current manager observations, without transfer or removal.
    """
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
    """
    Retain the historical helper entry point by rendering a freshly built typed plan.

    This is presentation data, not persisted or executable workflow state.

    Example:
        >>> receipt = _storage_store_evacuation_plan_payload(runtime, store_reference="source", destination_reference=None, max_assets=100)  # doctest: +SKIP


    :param runtime: Runtime supplying Store resolution and the storage manager used for planning.
    :param store_reference: Source Store reference accepted by the live resolver.
    :param destination_reference: Optional single destination reference; None/empty text allows automatic selection.
    :param max_assets: Asset selection bound passed through without clamping or validation here.
    :return: Newly rendered Core plan dictionary with counts, entries, shortfalls, and transfer estimates.
    """
    return _plan(
        runtime,
        store_reference=store_reference,
        destination_reference=destination_reference,
        max_assets=max_assets,
    ).to_wire()


def storage_store_evacuate_plan(
    runtime: CoreRuntime, query: CoreQuery
) -> dict[str, object]:
    """
    Validate a plan query's source and Asset bound, then return a fresh evacuation plan receipt.

    max_assets defaults to 100, must convert to an integer >= 1, and is capped at
    10,000. Booleans are rejected; other int-convertible values may be coerced.
    Explicit null survives the optional parser and fails the non-null assertion.

    Example:
        >>> receipt = storage_store_evacuate_plan(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime with library storage and Store-reference lookup services.
    :param query: Query carrying store, optional destination_store, and optional max_assets.
    :return: Wire plan snapshot; complete/blocked fields describe selection and shortfall, not execution success.
    :raises CoreDispatchError: If store is absent/empty, a non-null bound is invalid, or Store resolution/planning rejects it.
    :raises AssertionError: If max_assets is explicitly None while assertions are enabled.
    """
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
    """
    Replan, execute bounded evacuation, and replan again to produce a synchronous attempt receipt.

    Defaults are 100 Assets, 1,000 action receipts, and 1 TiB of estimated transfer
    bytes. Bounds reject booleans and converted values below one; Asset/action counts
    are capped at 10,000/100,000. keep_source_bytes uses ordinary truth conversion.
    No rollback occurs if execution or the final read/report step fails.

    Example:
        >>> receipt = storage_store_evacuate_apply(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime providing Store resolution and the storage manager used for the entire attempt.
    :param command: Command with store, optional destination_store, max_assets, max_actions, max_transfer_bytes, and keep_source_bytes.
    :return: Before/after plans and execution receipts; actions_applied counts all receipts, and ok requires no failures/truncation plus zero after-plan source claims.
    :raises CoreDispatchError: If required source, non-null bounds, or Store/planning constraints are invalid.
    :raises AssertionError: If any explicit bound is None while assertions are enabled.
    """
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
