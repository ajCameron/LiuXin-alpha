"""
Containers and row helpers for managed digital assets and storage state.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Optional, Any

from LiuXin_alpha.databases.row import FixedTableStorageRow

if TYPE_CHECKING:
    from LiuXin_alpha.storage.storage_types import (
        AssetReplicaID,
        CompositeDigitalAssetID,
        CompositeDigitalAssetItemLinkID,
        CompositeDigitalAssetMemberLinkID,
        DigitalAssetID,
        DigitalAssetItemLinkID,
    )
    from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI


class DigitalAssetRow(FixedTableStorageRow):
    """One atomic, byte-bearing managed digital asset row."""

    TABLE_NAME = "digital_assets"
    ID_COLUMN = "digital_asset_id"

    @property
    def digital_asset_id(self) -> Optional["DigitalAssetID"]:
        return self[self.ID_COLUMN]

    @digital_asset_id.setter
    def digital_asset_id(self, value: Optional["DigitalAssetID"]) -> None:
        self.primary_id = value


class CompositeDigitalAssetRow(FixedTableStorageRow):
    """One logical multipart assembly of atomic digital assets."""

    TABLE_NAME = "composite_digital_assets"
    ID_COLUMN = "composite_digital_asset_id"

    @property
    def composite_digital_asset_id(self) -> Optional["CompositeDigitalAssetID"]:
        return self[self.ID_COLUMN]

    @composite_digital_asset_id.setter
    def composite_digital_asset_id(self, value: Optional["CompositeDigitalAssetID"]) -> None:
        self.primary_id = value


class AssetReplicaRow(FixedTableStorageRow):
    """One physical copy of one managed digital asset on one store."""

    TABLE_NAME = "asset_replicas"
    ID_COLUMN = "asset_replica_id"

    @property
    def asset_replica_id(self) -> Optional["AssetReplicaID"]:
        return self[self.ID_COLUMN]

    @asset_replica_id.setter
    def asset_replica_id(self, value: Optional["AssetReplicaID"]) -> None:
        self.primary_id = value

    def validate(self) -> None:
        storage_key = self.row_dict.get("asset_replica_storage_key")
        if storage_key is not None and storage_key == "":
            raise ValueError("asset_replica_storage_key may not be an empty string.")

        mode = self.row_dict.get("asset_replica_mode")
        if mode is not None and mode not in {"active", "backup", "archive"}:
            raise ValueError("asset_replica_mode must be one of: active, backup, archive.")


class DigitalAssetItemLinkRow(FixedTableStorageRow):
    """One semantic link from an item to one atomic digital asset."""

    TABLE_NAME = "digital_asset_item_links"
    ID_COLUMN = "digital_asset_item_link_id"
    LINK_PREFIX = "digital_asset_item_link"

    @property
    def digital_asset_item_link_id(self) -> Optional["DigitalAssetItemLinkID"]:
        return self[self.ID_COLUMN]

    @digital_asset_item_link_id.setter
    def digital_asset_item_link_id(self, value: Optional["DigitalAssetItemLinkID"]) -> None:
        self.primary_id = value

    def validate(self) -> None:
        priority = self.row_dict.get(f"{self.LINK_PREFIX}_priority")
        if priority is not None and priority < 0:
            raise ValueError(f"{self.LINK_PREFIX}_priority must be >= 0.")


class CompositeDigitalAssetItemLinkRow(FixedTableStorageRow):
    """One semantic link from an item to one composite digital asset."""

    TABLE_NAME = "composite_digital_asset_item_links"
    ID_COLUMN = "composite_digital_asset_item_link_id"
    LINK_PREFIX = "composite_digital_asset_item_link"

    @property
    def composite_digital_asset_item_link_id(self) -> Optional["CompositeDigitalAssetItemLinkID"]:
        return self[self.ID_COLUMN]

    @composite_digital_asset_item_link_id.setter
    def composite_digital_asset_item_link_id(self, value: Optional["CompositeDigitalAssetItemLinkID"]) -> None:
        self.primary_id = value

    def validate(self) -> None:
        priority = self.row_dict.get(f"{self.LINK_PREFIX}_priority")
        if priority is not None and priority < 0:
            raise ValueError(f"{self.LINK_PREFIX}_priority must be >= 0.")


class CompositeDigitalAssetMemberLinkRow(FixedTableStorageRow):
    """Ordered membership link from one composite digital asset to one atomic digital asset."""

    TABLE_NAME = "composite_digital_asset_digital_asset_links"
    ID_COLUMN = "composite_digital_asset_digital_asset_link_id"
    LINK_PREFIX = "composite_digital_asset_digital_asset_link"

    @property
    def composite_digital_asset_member_link_id(self) -> Optional["CompositeDigitalAssetMemberLinkID"]:
        return self[self.ID_COLUMN]

    @composite_digital_asset_member_link_id.setter
    def composite_digital_asset_member_link_id(self, value: Optional["CompositeDigitalAssetMemberLinkID"]) -> None:
        self.primary_id = value

    def validate(self) -> None:
        composite_id = self.row_dict.get(f"{self.LINK_PREFIX}_composite_digital_asset_id")
        member_id = self.row_dict.get(f"{self.LINK_PREFIX}_digital_asset_id")
        if composite_id is not None and member_id is not None and composite_id == member_id:
            raise ValueError("A composite digital asset cannot directly include itself.")

        sequence_number = self.row_dict.get(f"{self.LINK_PREFIX}_sequence_number")
        if sequence_number is not None and sequence_number < 0:
            raise ValueError(f"{self.LINK_PREFIX}_sequence_number must be >= 0.")

        is_required = self.row_dict.get(f"{self.LINK_PREFIX}_is_required")
        if is_required is not None and is_required not in {0, 1, True, False}:
            raise ValueError(f"{self.LINK_PREFIX}_is_required must be a boolean-ish 0/1 value.")


DigitalAssetRecord = DigitalAssetRow
CompositeDigitalAssetRecord = CompositeDigitalAssetRow
AssetReplicaRecord = AssetReplicaRow
DigitalAssetItemLinkRecord = DigitalAssetItemLinkRow
CompositeDigitalAssetItemLinkRecord = CompositeDigitalAssetItemLinkRow
CompositeDigitalAssetMemberLinkRecord = CompositeDigitalAssetMemberLinkRow


@dataclasses.dataclass(slots=True)
class DigitalAssetReplicationCluster:
    """Informational container describing the physical replicas of one digital asset."""

    digital_asset_id: "DigitalAssetID"
    asset_replica_locs: dict["AssetReplicaID", "StoreLocationMixinAPI"]
    digital_asset_hash: str | None = None

    @property
    def replication_level(self) -> int:
        return len(self.asset_replica_locs)


__all__ = [
    "AssetReplicaRecord",
    "AssetReplicaRow",
    "CompositeDigitalAssetItemLinkRecord",
    "CompositeDigitalAssetItemLinkRow",
    "CompositeDigitalAssetMemberLinkRecord",
    "CompositeDigitalAssetMemberLinkRow",
    "CompositeDigitalAssetRecord",
    "CompositeDigitalAssetRow",
    "DigitalAssetItemLinkRecord",
    "DigitalAssetItemLinkRow",
    "DigitalAssetRecord",
    "DigitalAssetReplicationCluster",
    "DigitalAssetRow",
]
