"""Bounded evacuation execution with a fresh safety check before source removal."""

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
    """Action receipts and actual transfer accounting for one bounded attempt."""

    actions: list[dict[str, object]] = field(default_factory=list[dict[str, object]])
    transferred_bytes: int = 0
    truncated: bool = False
    source_bytes_retained: bool = False

    @property
    def failures(self) -> int:
        return sum(not receipt["ok"] for receipt in self.actions)


def _place_replacements(
    manager: api.StorageManagerAPI,
    entry: EvacuationEntry,
    execution: EvacuationExecution,
) -> bool:
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
    """Replace, recheck, then remove; retain sources on any placement failure."""
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
