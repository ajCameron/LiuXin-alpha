"""Deterministic evacuation planning against storage contracts, without writes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from uuid import UUID

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services.evacuation_models import (
    EvacuationEntry,
    EvacuationPlan,
)
from LiuXin_alpha.core.program_services.storage_placement import (
    PlacementPolicy,
    accepts_configuration,
    placement_capacity,
    policy_for_mode,
    respects_separation,
    verified_outside_source,
)
from LiuXin_alpha.storage import api


@dataclass(frozen=True)
class _PlacementContext:
    manager: api.StorageManagerAPI
    source_ref: UUID
    destination_ref: UUID | None
    default_ref: UUID | None
    configurations: Mapping[UUID, api.StoreConfiguration]


def _available(manager: api.StorageManagerAPI, store_ref: UUID) -> bool:
    try:
        return manager.get_store(store_ref).status().available
    except Exception:
        # An unavailable backend is a rejected destination, not a failed plan.
        return False


def _destinations(
    context: _PlacementContext,
    *,
    mode: api.ReplicaMode,
    policy: PlacementPolicy | None,
    occupied: set[UUID],
    existing: Sequence[api.StoreConfiguration],
    needed: int,
) -> tuple[UUID, ...]:
    candidates = sorted(
        context.configurations.values(),
        key=lambda configuration: (
            configuration.store_uuid
            != (
                context.destination_ref
                if context.destination_ref is not None
                else context.default_ref
            ),
            configuration.store_name,
            str(configuration.store_uuid),
        ),
    )
    destinations: list[UUID] = []
    selected = list(existing)
    for configuration in candidates:
        candidate_ref = configuration.store_uuid
        if candidate_ref == context.source_ref or candidate_ref in occupied:
            continue
        if (
            context.destination_ref is not None
            and candidate_ref != context.destination_ref
        ):
            continue
        if configuration.read_only or mode not in configuration.supported_replica_modes:
            continue
        if not accepts_configuration(configuration, mode, policy):
            continue
        if not respects_separation(configuration, selected, policy):
            continue
        if not _available(context.manager, candidate_ref):
            continue
        destinations.append(candidate_ref)
        selected.append(configuration)
        occupied.add(candidate_ref)
        if len(destinations) >= needed:
            break
    return tuple(destinations)


def _entry(
    context: _PlacementContext,
    *,
    asset: api.DigitalAssetRecord,
    mode: api.ReplicaMode,
    sources: Sequence[api.ReplicaRecord],
    policy: PlacementPolicy | None,
    occupied: set[UUID],
) -> EvacuationEntry:
    target_count = max(1, 1 if policy is None else int(policy.effective_target_copies))
    outside = verified_outside_source(
        context.manager,
        asset_id=asset.digital_asset_id,
        mode=mode,
        source_ref=context.source_ref,
        configurations=context.configurations,
        policy=policy,
    )
    existing = [context.configurations[record.location.store_ref] for record in outside]
    needed = max(0, target_count - placement_capacity(existing, policy))
    destinations = _destinations(
        context,
        mode=mode,
        policy=policy,
        occupied=occupied,
        existing=existing,
        needed=needed,
    )
    return EvacuationEntry(
        asset_id=asset.digital_asset_id,
        source_replica_ids=tuple(record.replica_id for record in sources),
        source_mode=sources[0].mode,
        target_mode=mode,
        target_copies=target_count,
        verified_outside_source=len(outside),
        destination_store_refs=destinations,
        shortfall=max(0, needed - len(destinations)),
        estimated_transfer_bytes=len(destinations) * int(asset.size_bytes),
    )


def _asset_entries(
    context: _PlacementContext,
    asset_id: api.DigitalAssetID,
    sources: Sequence[api.ReplicaRecord],
) -> tuple[EvacuationEntry, ...]:
    asset = context.manager.get_digital_asset_record(asset_id)
    policies = context.manager.resolve_effective_policies(asset_id)
    by_mode: dict[api.ReplicaMode, list[api.ReplicaRecord]] = {}
    for record in sources:
        mode = (
            policies.replication.mode
            if record.mode
            in {
                api.ReplicaMode.UNMANAGED,
                api.ReplicaMode.CACHE,
                api.ReplicaMode.TRANSIENT,
            }
            else record.mode
        )
        by_mode.setdefault(mode, []).append(record)
    occupied = {
        record.location.store_ref
        for record in context.manager.iter_replica_records(digital_asset_id=asset_id)
        if record.state is not api.ReplicaState.DELETED
    }
    return tuple(
        _entry(
            context,
            asset=asset,
            mode=mode,
            sources=records,
            policy=policy_for_mode(policies, mode),
            occupied=occupied,
        )
        for mode, records in sorted(by_mode.items(), key=lambda item: item[0].value)
    )


def build_evacuation_plan(
    manager: api.StorageManagerAPI,
    *,
    source_ref: UUID,
    destination_ref: UUID | None,
    max_assets: int,
) -> EvacuationPlan:
    """Plan bounded, policy-aware replacement of the source Store's live claims."""
    if destination_ref == source_ref:
        raise CoreDispatchError(
            "Evacuation destination must differ from the source Store."
        )
    live_records = sorted(
        (
            record
            for record in manager.iter_replica_records(store_ref=source_ref)
            if record.state is not api.ReplicaState.DELETED
        ),
        key=lambda record: (int(record.digital_asset_id), int(record.replica_id)),
    )
    grouped: dict[api.DigitalAssetID, list[api.ReplicaRecord]] = {}
    for record in live_records:
        grouped.setdefault(record.digital_asset_id, []).append(record)
    selected = sorted(grouped)[:max_assets]
    try:
        default_ref = manager.get_default_store_ref()
    except Exception:
        default_ref = None
    context = _PlacementContext(
        manager,
        source_ref,
        destination_ref,
        default_ref,
        {
            configuration.store_uuid: configuration
            for configuration in manager.iter_store_configurations()
        },
    )
    entries = tuple(
        entry
        for asset_id in selected
        for entry in _asset_entries(context, asset_id, grouped[asset_id])
    )
    source = manager.get_store_configuration(source_ref)
    return EvacuationPlan(
        source=source,
        source_is_default=source_ref == default_ref,
        destination_ref=destination_ref,
        assets_available=len(grouped),
        assets_planned=len(selected),
        max_assets=max_assets,
        entries=entries,
        deletes_source_bytes=not source.read_only
        and any(
            record.mode is not api.ReplicaMode.UNMANAGED for record in live_records
        ),
    )
