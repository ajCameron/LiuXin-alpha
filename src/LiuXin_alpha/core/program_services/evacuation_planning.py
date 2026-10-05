"""
Plan bounded source-store evacuation using current claims, policies, and destination status reads.

The planner performs no replication or removal and reserves no capacity. Its
greedy ordering is deterministic for fixed observations, but manager reads are
not an atomic snapshot. Existing VERIFIED claims are trusted as observations;
their bytes are not freshly verified by planning.
"""

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
    """
    Share manager access, source/destination identities, and a configuration snapshot across plan entries.

    destination_ref optionally confines selection to one Store; otherwise default_ref
    only influences ordering. The frozen record does not freeze its referenced manager
    or mapping, and it does not cache live status reads.

    Example:
        >>> context = _PlacementContext(manager, UUID(int=1), None, None, {})  # doctest: +SKIP
    """

    manager: api.StorageManagerAPI
    source_ref: UUID
    destination_ref: UUID | None
    default_ref: UUID | None
    configurations: Mapping[UUID, api.StoreConfiguration]


def _available(manager: api.StorageManagerAPI, store_ref: UUID) -> bool:
    """
    Read the destination's current availability flag, treating any ordinary lookup/status error as unavailable.

    Writability, free capacity, and status age are not checked here.

    Example:
        >>> from unittest.mock import Mock
        >>> manager = Mock()
        >>> manager.get_store.side_effect = OSError("offline")
        >>> _available(manager, UUID(int=2))
        False


    :param manager: Storage manager resolving the candidate Store and its status.
    :param store_ref: Candidate Store UUID to probe through the manager.
    :return: Status.available on success, or False when lookup/status access raises Exception.
    """
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
    """
    Greedily select eligible destinations and add their UUIDs to the caller's occupied set.

    Explicit destination or default Store sorts first, followed by name and UUID.
    Reject source/occupied Stores, disallowed modes/tags/separation, read-only
    configurations, and unavailable status. Status.writable and free bytes are
    not consulted. The count check occurs after appending, so needed <= 0 can
    still select one eligible destination instead of returning an empty tuple.

    Example:
        >>> from unittest.mock import Mock
        >>> candidate = api.StoreConfiguration(UUID(int=2), "destination", "filesystem", "file:///destination")
        >>> manager = Mock()
        >>> manager.get_store.return_value.status.return_value.available = True
        >>> context = _PlacementContext(manager, UUID(int=1), None, None, {candidate.store_uuid: candidate})
        >>> occupied = set()
        >>> _destinations(context, mode=api.ReplicaMode.ACTIVE, policy=None, occupied=occupied, existing=(), needed=1) == (candidate.store_uuid,)
        True
        >>> candidate.store_uuid in occupied
        True
        >>> len(_destinations(context, mode=api.ReplicaMode.ACTIVE, policy=None, occupied=set(), existing=(), needed=0))
        1


    :param context: Shared source, preferred/confined destination, manager, and configuration bindings.
    :param mode: Target Replica mode candidates must support.
    :param policy: Optional tag/separation policy; None disables those additional constraints.
    :param occupied: Mutable Store UUID set; selected destinations are inserted before returning.
    :param existing: Existing eligible configurations used as the initial separation context; not mutated.
    :param needed: Requested destination count, checked only after each successful selection.
    :return: Selected UUIDs in greedy order, possibly fewer than requested; no Store capacity is reserved.
    """
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
    """
    Build one Asset/mode entry from verified outside capacity and greedily selected replacement destinations.

    Target capacity is at least one, using the policy's effective target when
    present. Outside claim count and policy-aware capacity are distinct. Destination
    selection is called even when no extra capacity is needed and can over-plan
    one copy in that case. Transfer estimates use whole-Asset bytes per destination.

    Example:
        >>> entry = _entry(context, asset=asset, mode=api.ReplicaMode.ACTIVE, sources=records, policy=None, occupied=occupied)  # doctest: +SKIP


    :param context: Planning bindings and Store configuration snapshot.
    :param asset: Digital Asset record supplying identity and payload size in bytes.
    :param mode: Replacement mode whose outside claims and policy capacity are evaluated.
    :param sources: Nonempty group of source Replica records; the first supplies source_mode.
    :param policy: Governing replication/backup policy, or None for a one-copy target.
    :param occupied: Store UUIDs already holding live claims or selected by earlier entries; extended in place.
    :return: Typed entry with ordered source IDs, destinations, shortfall, and estimated transfer bytes.
    """
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
    """
    Group source claims by replacement mode and plan each group in enum-value order.

    Unmanaged, cache, and transient claims map to the current replication mode;
    other modes are retained. Every non-DELETED claim for the Asset occupies its
    Store regardless of availability. One shared occupied set also prevents
    different mode entries from selecting the same new Store during this call.

    Example:
        >>> entries = _asset_entries(context, api.DigitalAssetID(7), records)  # doctest: +SKIP


    :param context: Shared manager and configuration bindings for the source-store plan.
    :param asset_id: Digital Asset whose record, policies, and live claims are read afresh.
    :param sources: Source-store claims to group, preserving input order within each target mode.
    :return: One entry per replacement-mode group; an empty source sequence yields an empty tuple.
    """
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
    """
    Plan the first max_assets source Assets in numeric ID order without transferring or removing bytes.

    All non-DELETED source claims are loaded and grouped before applying the Asset
    slice. Default-Store lookup errors merely remove its preference; other manager
    errors propagate unless destination status handling suppresses them. The byte-
    deletion flag considers all live source claims, including unselected Assets.

    Example:
        >>> plan = build_evacuation_plan(manager, source_ref=UUID(int=1), destination_ref=None, max_assets=100)  # doctest: +SKIP


    :param manager: Storage manager supplying claims, records, policies, configurations, and live status.
    :param source_ref: Store UUID whose live Replica claims are candidates for evacuation.
    :param destination_ref: Optional single permitted destination UUID; None permits any eligible Store.
    :param max_assets: Slice bound applied directly; zero selects none and negatives follow Python slicing.
    :return: Typed, unreserved plan snapshot with total/selected counts and replacement-mode entries.
    :raises CoreDispatchError: If destination_ref equals source_ref.
    """
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
