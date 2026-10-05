"""
Execute planned replacements and recheck observed outside capacity before each entry's source removals.

Individual replication/removal failures become receipts, while lookup, projection,
and other unhandled errors can abort after partial work. There is no transaction,
rollback, or lock spanning the plan. Replacement eligibility is reread using current
policies, but the required target count remains the entry's planning-time value.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from uuid import UUID

from LiuXin_alpha.core.program_services.evacuation_models import (
    EvacuationEntry,
    EvacuationLimits,
    EvacuationPlan,
)
from LiuXin_alpha.core.program_services.payloads import plain
from LiuXin_alpha.core.program_services.storage_placement import (
    placement_capacity,
    policy_for_mode,
    verified_outside_source,
)
from LiuXin_alpha.storage import api


@dataclass
class EvacuationExecution:
    """
    Accumulate action receipts, counted payload bytes, and stopping/retention flags for one attempt.

    Each instance owns its action list. transferred_bytes counts Asset sizes after
    successful placements, not measured network traffic. source_bytes_retained
    records the operator/read-only policy and does not enumerate per-claim exceptions
    such as unmanaged bytes or sources retained after a failed replacement.

    Example:
        >>> result = EvacuationExecution(actions=[{"ok": True}, {"ok": False}])
        >>> result.failures, result.transferred_bytes
        (1, 0)
    """

    actions: list[dict[str, object]] = field(default_factory=list[dict[str, object]])
    transferred_bytes: int = 0
    truncated: bool = False
    source_bytes_retained: bool = False

    @property
    def failures(self) -> int:
        """
        Count falsey ok values in the currently accumulated action receipts.

        Example:
            >>> EvacuationExecution(actions=[{"ok": False}, {"ok": True}]).failures
            1


        :return: Number of receipts whose ok value is falsey, including explicit source-retention receipts.
        :raises KeyError: If a manually supplied receipt lacks the required ok key.
        """
        return sum(not receipt["ok"] for receipt in self.actions)


def _place_replacements(
    manager: api.StorageManagerAPI,
    entry: EvacuationEntry,
    execution: EvacuationExecution,
) -> bool:
    """
    Try every planned destination using the first source claim with a readable-state candidate.

    All source records are fetched first. VERIFIED, PRESENT, and UNVERIFIED qualify;
    no fallback source is tried after a copy failure. Each copy requests verification.
    Copy Exceptions become failed receipts and do not stop later destinations, but
    source lookup, successful-result projection, or byte-accounting errors propagate.

    Example:
        >>> copied = _place_replacements(manager, entry, execution)  # doctest: +SKIP


    :param manager: Storage manager fetching source claims and executing verified replication requests.
    :param entry: Planned Asset, ordered source IDs, target mode, and destination UUIDs.
    :param execution: Mutable accumulator receiving receipts and successful-copy Asset-size accounting.
    :return: True if no attempted placement raised, including when the destination tuple is empty.
    """
    records = [
        manager.get_replica_record(replica_id)
        for replica_id in entry.source_replica_ids
    ]
    source = next(
        (
            record
            for record in records
            if record.state
            in {
                api.ReplicaState.VERIFIED,
                api.ReplicaState.PRESENT,
                api.ReplicaState.UNVERIFIED,
            }
        ),
        None,
    )
    succeeded = True
    for destination in entry.destination_store_refs:
        receipt: dict[str, object] = {
            "action": "replicate_digital_asset",
            "digital_asset_id": int(entry.asset_id),
            "source_replica_id": None if source is None else int(source.replica_id),
            "destination_store_ref": str(destination),
            "mode": entry.target_mode.value,
        }
        try:
            if source is None:
                raise api.NoReadableReplica(
                    "source Store has no readable Replica for evacuation."
                )
            replica = manager.replicate_digital_asset(
                entry.asset_id,
                destination_store_ref=destination,
                source_replica_id=source.replica_id,
                mode=entry.target_mode,
                verify=True,
            )
        except Exception as error:
            receipt.update(ok=False, error=str(error) or type(error).__name__)
            succeeded = False
        else:
            receipt.update(ok=True, result=plain(replica))
            execution.transferred_bytes += manager.get_digital_asset_record(
                entry.asset_id
            ).size_bytes
        execution.actions.append(receipt)
    return succeeded


def _replacement_capacity(
    manager: api.StorageManagerAPI,
    entry: EvacuationEntry,
    source_ref: UUID,
) -> int:
    """
    Recompute outside placement capacity using current policies, configurations, and VERIFIED claim observations.

    This does not read replacement bytes or probe current Store availability.
    The caller compares this capacity to the entry's older target_copies, not a
    freshly read target count. Manager and capacity-calculation errors propagate.

    Example:
        >>> capacity = _replacement_capacity(manager, entry, UUID(int=1))  # doctest: +SKIP


    :param manager: Storage manager providing current policy, topology, and Replica observations.
    :param entry: Asset identity and target mode to evaluate.
    :param source_ref: Source Store UUID excluded from replacement capacity.
    :return: Eligible outside capacity under the currently resolved mode policy.
    """
    # Never authorize deletion from the earlier plan: policies, observations,
    # and topology may all have changed while replacement bytes were written.
    policies = manager.resolve_effective_policies(entry.asset_id)
    policy = policy_for_mode(policies, entry.target_mode)
    configurations = {
        configuration.store_uuid: configuration
        for configuration in manager.iter_store_configurations()
    }
    outside = verified_outside_source(
        manager,
        asset_id=entry.asset_id,
        mode=entry.target_mode,
        source_ref=source_ref,
        configurations=configurations,
        policy=policy,
    )
    return placement_capacity(
        (configurations[record.location.store_ref] for record in outside), policy
    )


def _remove_sources(
    manager: api.StorageManagerAPI,
    entry: EvacuationEntry,
    execution: EvacuationExecution,
    *,
    retain_bytes: bool,
) -> None:
    """
    Remove each source claim with a tombstone, optionally deleting its bytes, and record individual outcomes.

    Unmanaged claims always retain bytes. Removal Exceptions become receipts and
    later claims are still attempted; record lookup or successful-result projection
    errors propagate. This helper does not independently recheck replacement capacity.

    Example:
        >>> _remove_sources(manager, entry, execution, retain_bytes=True)  # doctest: +SKIP


    :param manager: Storage manager fetching each current claim and performing removal.
    :param entry: Ordered source Replica IDs and Asset identity for action receipts.
    :param execution: Mutable accumulator to which one receipt per completed attempt is appended.
    :param retain_bytes: If true, forbid byte deletion for every claim regardless of its mode.
    :return: None after all lookups/removal attempts finish; partial removals are not rolled back on failure.
    """
    for replica_id in entry.source_replica_ids:
        record = manager.get_replica_record(replica_id)
        delete_bytes = not retain_bytes and record.mode is not api.ReplicaMode.UNMANAGED
        receipt: dict[str, object] = {
            "action": "remove_source_replica",
            "replica_id": int(replica_id),
            "digital_asset_id": int(entry.asset_id),
            "delete_bytes": delete_bytes,
        }
        try:
            result = manager.remove_replica(
                record.replica_id, delete_bytes=delete_bytes, retain_tombstone=True
            )
        except Exception as error:
            receipt.update(ok=False, error=str(error) or type(error).__name__)
        else:
            receipt.update(ok=True, result=plain(result))
        execution.actions.append(receipt)


def execute_evacuation(
    manager: api.StorageManagerAPI,
    plan: EvacuationPlan,
    limits: EvacuationLimits,
    *,
    keep_source_bytes: bool,
) -> EvacuationExecution:
    """
    Process entries in plan order: budget, place replacements, recheck capacity, then remove source claims.

    Positive shortfalls emit failed receipts without placement. Budget exhaustion
    stops the loop; failed placement/capacity retains that entry's sources and
    proceeds to the next entry. Capacity is recalculated even after a copy failure,
    once per entry before removals. Unexpected errors can escape after prior writes.

    Example:
        >>> result = execute_evacuation(manager, plan, EvacuationLimits(1000, 1024**4), keep_source_bytes=True)  # doctest: +SKIP


    :param manager: Storage manager used for live reads, verified placement, and claim/byte removal.
    :param plan: Trusted typed plan; its target counts and destination choices are not replanned here.
    :param limits: Entry-level action/estimated-byte budgets checked against accumulated receipts and counted bytes.
    :param keep_source_bytes: Preserve source bytes while removing claims; current source read_only also forces retention.
    :return: Attempt accumulator, not proof the source is empty; source read_only is read once before the entry loop.
    """
    execution = EvacuationExecution()
    source = manager.get_store_configuration(plan.source.store_uuid)
    execution.source_bytes_retained = keep_source_bytes or source.read_only
    for entry in plan.entries:
        if len(execution.actions) >= limits.max_actions:
            execution.truncated = True
            break
        if entry.shortfall > 0:
            execution.actions.append(
                {
                    "action": "evacuate_asset",
                    "digital_asset_id": int(entry.asset_id),
                    "ok": False,
                    "error": "no safe destination satisfies the evacuation plan",
                    "entry": entry.to_wire(),
                }
            )
            continue
        if not limits.permits(
            entry,
            actions=len(execution.actions),
            transferred=execution.transferred_bytes,
        ):
            execution.truncated = True
            break
        placed = _place_replacements(manager, entry, execution)
        capacity = _replacement_capacity(manager, entry, source.store_uuid)
        if not placed or capacity < entry.target_copies:
            execution.actions.append(
                {
                    "action": "retain_source_replicas",
                    "digital_asset_id": int(entry.asset_id),
                    "ok": False,
                    "error": "replacement copies were not verified to the required target; source claims and bytes were retained",
                }
            )
            continue
        _remove_sources(
            manager,
            entry,
            execution,
            retain_bytes=execution.source_bytes_retained,
        )
    return execution
