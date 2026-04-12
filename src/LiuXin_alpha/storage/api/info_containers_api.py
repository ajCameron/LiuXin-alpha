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
        DigitalAssetCompositionID,
        DigitalAssetID,
        ItemDigitalAssetLinkID,
    )
    from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI


class DigitalAssetRow(FixedTableStorageRow):
    """
    One managed digital asset row.

    A digital asset is a digital object tracked by the system.
    (We will, eventually, be tracking physical assets. Problem for later though).
    """

    TABLE_NAME = "digital_assets"
    ID_COLUMN = "digital_asset_id"

    ATOMIC: bool

    def __init__(
            self,
            database: "DatabaseAPI",
            row_dict: Optional[dict[str, Any]] = None,
            read_only: bool = False) -> None:
        super().__init__(database=database, row_dict=row_dict, read_only=read_only)

        # If an Asset is atomic, it is composed of a single file.
        self.ATOMIC = True

    @property
    def digital_asset_id(self) -> Optional["DigitalAssetID"]:
        """
        Rowid for the digital asset we're tracking.

        :return:
        """
        return self[self.ID_COLUMN]

    @digital_asset_id.setter
    def digital_asset_id(self, value: Optional["DigitalAssetID"]) -> None:
        self.primary_id = value

    def validate(self) -> None:
        """
        Check the row is valid before write.

        :return:
        """
        kind = self.row_dict.get("digital_asset_kind")
        if kind is not None and kind not in {"atomic", "composite"}:
            raise ValueError("digital_asset_kind must be either 'atomic' or 'composite'.")


class AssetReplicaRow(FixedTableStorageRow):
    """
    One physical copy of one managed digital asset on one store.

    Every DigitalAsset should have multiple AssetReplicas.
    Possibly of different types.
    """

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


class ItemDigitalAssetLinkRow(FixedTableStorageRow):
    """
    One semantic link from an item to a managed digital asset.


    """

    # We are pegged to links of this type.
    TABLE_NAME = "items__digital_assets__links"

    ID_COLUMN = "item_digital_asset_link_id"

    @property
    def item_digital_asset_link_id(self) -> Optional["ItemDigitalAssetLinkID"]:
        """
        RowID for the LINK ROW.

        :return:
        """
        return self[self.ID_COLUMN]

    @item_digital_asset_link_id.setter
    def item_digital_asset_link_id(self, value: Optional["ItemDigitalAssetLinkID"]) -> None:
        self.primary_id = value

    def validate(self) -> None:
        priority = self.row_dict.get("link_priority")
        if priority is not None and priority < 0:
            raise ValueError("link_priority must be >= 0.")


class DigitalAssetCompositionMemberRow(FixedTableStorageRow):
    """Ordered membership edge inside one composite digital asset."""

    TABLE_NAME = "digital_asset_compositions"
    ID_COLUMN = "digital_asset_composition_id"

    @property
    def digital_asset_composition_id(self) -> Optional["DigitalAssetCompositionID"]:
        return self[self.ID_COLUMN]

    @digital_asset_composition_id.setter
    def digital_asset_composition_id(self, value: Optional["DigitalAssetCompositionID"]) -> None:
        self.primary_id = value

    def validate(self) -> None:
        parent_id = self.row_dict.get("digital_asset_composition_parent_asset_id")
        member_id = self.row_dict.get("digital_asset_composition_member_asset_id")
        if parent_id is not None and member_id is not None and parent_id == member_id:
            raise ValueError("Composite digital assets cannot directly contain themselves.")

        sequence_number = self.row_dict.get("digital_asset_composition_sequence_number")
        if sequence_number is not None and sequence_number < 0:
            raise ValueError("digital_asset_composition_sequence_number must be >= 0.")


# Backwards-compatible API names while the storage layer is being migrated.
DigitalAssetRecord = DigitalAssetRow
AssetReplicaRecord = AssetReplicaRow
ItemDigitalAssetLinkRecord = ItemDigitalAssetLinkRow
DigitalAssetCompositionMemberRecord = DigitalAssetCompositionMemberRow


@dataclasses.dataclass(slots=True)
class DigitalAssetReplicationCluster:
    """
    Informational container describing the physical replicas of one digital asset.

    Every digital asset is supposed to be linked to (at least one) replication cluster.
    CompositeAssets will be linked to more than one.
    """

    digital_asset_id: "DigitalAssetID"
    asset_replica_locs: dict["AssetReplicaID", "StoreLocationMixinAPI"]
    digital_asset_hash: str | None = None

    @property
    def replication_level(self) -> int:
        return len(self.asset_replica_locs)


__all__ = [
    "AssetReplicaRecord",
    "AssetReplicaRow",
    "DigitalAssetCompositionMemberRecord",
    "DigitalAssetCompositionMemberRow",
    "DigitalAssetRecord",
    "DigitalAssetReplicationCluster",
    "DigitalAssetRow",
    "ItemDigitalAssetLinkRecord",
    "ItemDigitalAssetLinkRow",
]
