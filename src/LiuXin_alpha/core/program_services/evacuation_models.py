"""
Carry typed evacuation snapshots, render Core plan receipts, and check entry-level execution budgets.

These frozen records do not validate their fields or reserve destination capacity.
Planning supplies observations and estimates; execution must recheck live replacement
safety before source removal. Wire dictionaries are presentation receipts rather
than accepted executable workflow state.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from LiuXin_alpha.storage import api


@dataclass(frozen=True)
class EvacuationEntry:
    """
    Describe replacement destinations and source claims for one Digital Asset/target-mode group.

    source_mode records the first grouped source's mode, not necessarily every
    source's mode. target_copies is required placement capacity; verified_outside_source
    counts existing verified claims and is not a failure-domain-aware capacity.
    shortfall counts missing planned destinations. estimated_transfer_bytes sums
    the planned new copies' payload sizes, excluding protocol overhead or retries.

    Example:
        >>> entry = EvacuationEntry(
        ...     asset_id=api.DigitalAssetID(7), source_replica_ids=(api.ReplicaID(9),),
        ...     source_mode=api.ReplicaMode.UNMANAGED, target_mode=api.ReplicaMode.ACTIVE,
        ...     target_copies=1, verified_outside_source=0,
        ...     destination_store_refs=(UUID(int=2),), shortfall=0, estimated_transfer_bytes=4,
        ... )
        >>> entry.to_wire()["digital_asset_id"]
        7
    """

    asset_id: api.DigitalAssetID
    source_replica_ids: tuple[api.ReplicaID, ...]
    source_mode: api.ReplicaMode
    target_mode: api.ReplicaMode
    target_copies: int
    verified_outside_source: int
    destination_store_refs: tuple[UUID, ...]
    shortfall: int
    estimated_transfer_bytes: int

    def to_wire(self) -> dict[str, object]:
        """
        Render this entry with integer IDs, enum values, text UUIDs, and fresh list containers.

        Scalar counts and estimates are passed through without range checks.

        Example:
            >>> entry = EvacuationEntry(
            ...     api.DigitalAssetID(7), (), api.ReplicaMode.ACTIVE, api.ReplicaMode.ACTIVE,
            ...     1, 1, (), 0, 0,
            ... )
            >>> entry.to_wire()["destination_store_refs"]
            []


        :return: New dictionary containing the public plan-entry fields; no storage operations occur.
        """
        return {
            "digital_asset_id": int(self.asset_id),
            "source_replica_ids": [int(value) for value in self.source_replica_ids],
            "source_mode": self.source_mode.value,
            "target_mode": self.target_mode.value,
            "target_copies": self.target_copies,
            "verified_outside_source": self.verified_outside_source,
            "destination_store_refs": [
                str(value) for value in self.destination_store_refs
            ],
            "shortfall": self.shortfall,
            "estimated_transfer_bytes": self.estimated_transfer_bytes,
        }


@dataclass(frozen=True)
class EvacuationPlan:
    """
    Hold a source-store evacuation snapshot bounded by the selected Digital Asset count.

    assets_available counts source Assets with live claims; assets_planned counts
    selected Assets, each of which may contribute multiple mode entries. source
    retains its planning-time configuration, and destination_ref optionally confines
    destination selection. deletes_source_bytes describes potential byte deletion
    from that snapshot, not the operator's later keep-bytes choice or an execution result.

    Example:
        >>> source = api.StoreConfiguration(UUID(int=1), "source", "filesystem", "file:///source")
        >>> plan = EvacuationPlan(source, False, None, 0, 0, 10, (), False)
        >>> plan.to_wire()["complete"], plan.to_wire()["blocked"]
        (True, False)
    """

    source: api.StoreConfiguration
    source_is_default: bool
    destination_ref: UUID | None
    assets_available: int
    assets_planned: int
    max_assets: int
    entries: tuple[EvacuationEntry, ...]
    deletes_source_bytes: bool

    @property
    def replicas_planned(self) -> int:
        """
        Count source Replica references across entries, not replacement destinations or distinct IDs.

        Repeated references in manually constructed entries are counted repeatedly.

        Example:
            >>> source = api.StoreConfiguration(UUID(int=1), "source", "filesystem", "file:///source")
            >>> EvacuationPlan(source, False, None, 0, 0, 10, (), False).replicas_planned
            0


        :return: Sum of the lengths of all entry source_replica_ids tuples.
        """
        return sum(len(entry.source_replica_ids) for entry in self.entries)

    def to_wire(self) -> dict[str, object]:
        """
        Render plan identity, selection counts, entries, positive shortfalls, and transfer estimates.

        complete compares only planned/available Asset counts; a complete plan can
        still be blocked. blocked_entries shares dictionary objects with entries
        within this result. No manager state is refreshed or replacement safety proven.

        Example:
            >>> source = api.StoreConfiguration(UUID(int=1), "source", "filesystem", "file:///source")
            >>> receipt = EvacuationPlan(source, False, None, 3, 0, 0, (), False).to_wire()
            >>> receipt["complete"], receipt["estimated_transfer_bytes"]
            (False, 0)


        :return: New Core receipt with text store references, aggregate counts, and newly rendered entry dictionaries.
        """
        entries = [entry.to_wire() for entry in self.entries]
        blocked = [
            value
            for entry, value in zip(self.entries, entries, strict=True)
            if entry.shortfall > 0
        ]
        return {
            "source_store_ref": str(self.source.store_uuid),
            "source_store_name": self.source.store_name,
            "source_is_default": self.source_is_default,
            "destination_store_ref": None
            if self.destination_ref is None
            else str(self.destination_ref),
            "assets_available": self.assets_available,
            "assets_planned": self.assets_planned,
            "replicas_planned": self.replicas_planned,
            "complete": self.assets_planned == self.assets_available,
            "max_assets": self.max_assets,
            "entries": entries,
            "blocked_entries": blocked,
            "blocked": bool(blocked),
            "estimated_transfer_bytes": sum(
                entry.estimated_transfer_bytes for entry in self.entries
            ),
            "source_read_only": bool(self.source.read_only),
            "deletes_source_bytes_on_apply": self.deletes_source_bytes,
        }


@dataclass(frozen=True)
class EvacuationLimits:
    """
    Carry inclusive action-count and estimated-transfer-byte limits for an evacuation attempt.

    Values are not range-validated here. The permits calculation is a pre-entry
    budget check, not a reservation or a bound on every later failure receipt.

    Example:
        >>> EvacuationLimits(max_actions=2, max_transfer_bytes=4).max_transfer_bytes
        4
    """

    max_actions: int
    max_transfer_bytes: int

    def permits(
        self, entry: EvacuationEntry, *, actions: int, transferred: int
    ) -> bool:
        """
        Check whether current usage plus a whole entry's estimated work fits both inclusive limits.

        One action is budgeted per destination and per source Replica reference.
        The supplied counts and byte estimate are trusted, not validated or updated;
        shortfalls, availability, policy compliance, and deletion safety are not checked.

        Example:
            >>> entry = EvacuationEntry(
            ...     api.DigitalAssetID(7), (api.ReplicaID(9),), api.ReplicaMode.ACTIVE,
            ...     api.ReplicaMode.ACTIVE, 1, 0, (UUID(int=2),), 0, 4,
            ... )
            >>> limits = EvacuationLimits(2, 4)
            >>> limits.permits(entry, actions=0, transferred=0)
            True
            >>> limits.permits(entry, actions=1, transferred=0)
            False


        :param entry: Planned replacements/removals whose counts and transfer estimate are added to current usage.
        :param actions: Number of action receipts already accumulated by the execution attempt.
        :param transferred: Payload bytes already counted as transferred by the execution attempt.
        :return: True exactly when both projected totals are at or below their respective limits.
        """
        required_actions = len(entry.destination_store_refs) + len(
            entry.source_replica_ids
        )
        return (
            actions + required_actions <= self.max_actions
            and transferred + entry.estimated_transfer_bytes <= self.max_transfer_bytes
        )
