"""Typed plans and execution limits shared by the evacuation workflow."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from LiuXin_alpha.storage import api


@dataclass(frozen=True)
class EvacuationEntry:
    """Replacement and removal work for one Asset and target Replica mode."""

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
        """Retain the public plan receipt without using it as workflow state."""
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
    """A bounded snapshot; apply must recheck replacement safety before removal."""

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
        return sum(len(entry.source_replica_ids) for entry in self.entries)

    def to_wire(self) -> dict[str, object]:
        """Render the stable Core plan shape."""
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
    """Operator bounds checked before starting the next Asset/mode entry."""

    max_actions: int
    max_transfer_bytes: int

    def permits(
        self, entry: EvacuationEntry, *, actions: int, transferred: int
    ) -> bool:
        required_actions = len(entry.destination_store_refs) + len(
            entry.source_replica_ids
        )
        return (
            actions + required_actions <= self.max_actions
            and transferred + entry.estimated_transfer_bytes <= self.max_transfer_bytes
        )
