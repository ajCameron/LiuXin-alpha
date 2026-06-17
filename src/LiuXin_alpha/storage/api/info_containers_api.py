"""
Containers and row helpers for managed digital assets and storage state.

The row heirachy for this part of the database is as follows.

Item
 - The bottom end of the WEMI stack - actual items of whatever form

These link to

DigitalAssets
 - Things we're actually keeping

CompositeDigitalAssets
 - Made of DigitalAssets

Either composite or digital assets can link to items.
They are both first class objects in this sense.
However, Composite assets CANNOT link, directly, to AssetReplicas

ONLY DigitalAssets link to AssetReplicas.

"""

from __future__ import annotations

import dataclasses
from enum import StrEnum
from typing import TYPE_CHECKING, Optional, Any, Iterable

from LiuXin_alpha.databases.row import FixedTableStorageRow
from LiuXin_alpha.utils.logging.api import EventLogAPI

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
    from LiuXin_alpha.storage.api.policy_apis import ReplicationPolicy
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
    from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI


class DigitalAssetRow(FixedTableStorageRow):
    """
    One atomic, byte-bearing managed digital asset row.
    """

    TABLE_NAME = "digital_assets"
    ID_COLUMN = "digital_asset_id"

    @property
    def digital_asset_id(self) -> Optional["DigitalAssetID"]:
        """
        Get the digital asset ID.

        :return:
        """
        return self[self.ID_COLUMN]

    @digital_asset_id.setter
    def digital_asset_id(self, value: Optional["DigitalAssetID"]) -> None:
        """
        Set the digitial asset ID.

        :param value:
        :return:
        """
        self.primary_id = value


class DigitalAsset:
    """
    Represents a digital asset on the system.

    Can be used to query the replica information, and what this asset is linked to.
    """
    # Underlying row for the class
    digital_asset_row: "DigitalAssetRow"

    asset_replicas: list["AssetReplica"]

    asset_hash: str

    asset_items: list[int]

    replication_policy: "ReplicationPolicy"

    replication_cluster: "DigitalAssetReplicationCluster"

    def read(self, digital_asset_id: int, db: "DatabaseAPI") -> bool:
        """
        Read details of this asset off the database.

        :param digital_asset_id:
        :param db:
        :return:
        """

    def add_replica(self, new_replica: "AssetReplica") -> None:
        """
        Add a replica of the DigitalAsset to the asset.

        :param new_replica:
        :return:
        """

    def update_replication_policy(self, new_replication_policy: "ReplicationPolicy") -> None:
        """
        Write a new replication policy out to the asset.

        :param new_replication_policy:
        :return:
        """

    def replication_policy_satisfied(self) -> bool:
        """
        Check to see if we've satisfied the replication policy.

        :return:
        """


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


# Note: Keeping the row, and a representation of the thing itself, fundamentally different classes.
class CompositeDigitalAsset:
    """
    A composite digital asset on the system.

    Composite assets are composed of DigitalAssets.
    They can be linked to items, but are also linked to DigitalAssets.
    """
    # Underlying row for the class
    composite_digital_asset_row: "CompositeDigitalAssetRow"

    digital_assets: list["DigitalAsset"]
    protected: bool

    assets_hash: set[str]

    asset_items: list[int]

    def read(self, digital_asset_id: int, db: "DatabaseAPI") -> bool:
        """
        Read details of this asset off the database.

        :param digital_asset_id:
        :param db:
        :return:
        """

    def replication_policy_satisfied(self) -> bool:
        """
        Check to see if we've satisfied the replication policy.

        :return:
        """


class AssetReplicaRow(FixedTableStorageRow):
    """
    The row for physical copy of one managed digital asset on one store.
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
        """
        Check the row.

        :return:
        """
        storage_key = self.row_dict.get("asset_replica_storage_key")
        if storage_key is not None and storage_key == "":
            raise ValueError("asset_replica_storage_key may not be an empty string.")

        mode = self.row_dict.get("asset_replica_mode")
        if mode is not None and mode not in {"active", "backup", "archive"}:
            raise ValueError("asset_replica_mode must be one of: active, backup, archive.")


class AssetReplica:
    """
    Represents a replica of the asset on the system.

    Here, at last, the rubber meets the road.
    An actual file, which exists!
    Hopefully even in multiple places.
    """
    asset_replica_row: "AssetReplicaRow"

    hash: str

    digital_asset_ids: set[int]
    digital_asset_rows: Iterable["DigitalAssetRow"]

    item_digital_asset_link_ids: Iterable[int]
    item_digital_asset_link_rows: Iterable["DigitalAssetItemLinkRow"]

    def get_file(self) -> "StoreLocationMixinAPI":
        """
        Return the underlying file of this AssetReplica.

        :return:
        """

    def set_file(self, file: "StoreLocationMixinAPI") -> None:
        """
        Set the underlying file of this AssetReplica.

        :param file:
        :return:
        """

    def check_file(self) -> bool:
        """
        Go and check that the file actually exists and is readable/hash valid.

        :return:
        """




class DigitalAssetItemLinkRow(FixedTableStorageRow):
    """
    One semantic link from an item to one atomic digital asset.
    """

    TABLE_NAME = "digital_asset_item_links"
    ID_COLUMN = "digital_asset_item_link_id"
    LINK_PREFIX = "digital_asset_item_link"

    @property
    def digital_asset_item_link_id(self) -> Optional["DigitalAssetItemLinkID"]:
        """
        Get the id for the link row.

        :return:
        """
        return self[self.ID_COLUMN]

    @digital_asset_item_link_id.setter
    def digital_asset_item_link_id(self, value: Optional["DigitalAssetItemLinkID"]) -> None:
        """
        Set the id of the link row.

        :param value:
        :return:
        """
        # Todo: Error if someone tries this after it's been set once.
        self.primary_id = value

    def validate(self) -> None:
        """
        Check the row before write.

        :return:
        """
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
        """
        ID of the link row linking the composite asset and an item.

        :return:
        """
        return self[self.ID_COLUMN]

    @composite_digital_asset_item_link_id.setter
    def composite_digital_asset_item_link_id(self, value: Optional["CompositeDigitalAssetItemLinkID"]) -> None:
        """
        Set the ID of the link row linkling the composite asset and an item.

        :param value:
        :return:
        """
        # Todo: Should, usually, error
        self.primary_id = value

    def validate(self) -> None:
        """
        Check the row before write.

        :return:
        """
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
        """
        Get the id for this link row.

        :return:
        """
        return self[self.ID_COLUMN]

    @composite_digital_asset_member_link_id.setter
    def composite_digital_asset_member_link_id(self, value: Optional["CompositeDigitalAssetMemberLinkID"]) -> None:
        """
        Set the id for this row - should, mostly, be impossible.

        :param value:
        :return:
        """
        self.primary_id = value

    def validate(self) -> None:
        """
        Check the row before write.

        :return:
        """
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


@dataclasses.dataclass(slots=True)
class DigitalAssetReplicationCluster:
    """
    Informational container describing the physical replicas of one digital asset.
    """

    digital_asset_id: "DigitalAssetID"
    asset_replica_locs: dict["AssetReplicaID", "StoreLocationMixinAPI"]

    # The replication policy for the cluster
    replication_policy: ReplicationPolicy

    digital_asset_hash: str | None = None

    protected: bool = False

    @property
    def replication_level(self) -> int:
        """
        Checks the total replication level.

        :return:
        """
        return len(self.asset_replica_locs)


__all__ = [
    "AssetReplicaRow",
    "AssetReplicaRow",
    "CompositeDigitalAssetItemLinkRow",
    "CompositeDigitalAssetItemLinkRow",
    "CompositeDigitalAssetMemberLinkRow",
    "CompositeDigitalAssetMemberLinkRow",
    "CompositeDigitalAsset",
    "CompositeDigitalAssetRow",
    "CompositeDigitalAssetRow",
    "DigitalAssetItemLinkRow",
    "DigitalAssetItemLinkRow",
    "DigitalAssetRow",
    "DigitalAssetReplicationCluster",
    "DigitalAssetRow",
    "ReplicationPolicy",
    "StoreOperationalRole",
]




class StoreOperationalRole(StrEnum):
    """Broad operator-intent role for a configured store."""

    LIVE = "live"
    MIXED = "mixed"
    BACKUP = "backup"
    ARCHIVE = "archive"
    CACHE = "cache"


@dataclasses.dataclass(slots=True)
class StoreSpec:
    """Declarative description of one configured store."""

    store_id: Optional[int]
    store_uuid: Optional[str]
    store_name: str
    store_kind: str
    store_url: str

    store_access_protocol: Optional[str] = None
    store_root_uri: Optional[str] = None

    store_failure_domain: Optional[str] = None
    store_region: Optional[str] = None
    store_tags: tuple[str, ...] = ()

    store_default_replication_policy_id: Optional[int] = None
    store_default_backup_policy_id: Optional[int] = None

    store_supports_active_replica_mode: bool = True
    store_supports_backup_replica_mode: bool = True
    store_supports_archive_replica_mode: bool = True

    store_operational_role: str | None = None

    store_is_read_only: bool = False
    store_supports_folders: bool = True
    store_policy_json: Optional[str] = None
    store_scratch: Optional[str] = None


@dataclasses.dataclass(slots=True)
class StoreCheckStatus:
    """Outcome of store self-check probes."""

    store_marker_file: bool = False
    read: bool = False
    write: bool = False
    update: bool = False
    sundry: bool = False

    @property
    def all_ok(self) -> bool:
        return self.store_marker_file and self.read and self.update and self.write and self.sundry


@dataclasses.dataclass(slots=True)
class StoreStatus:
    """Snapshot status for a store."""

    name: str
    uuid: Optional[str]
    url: str

    file_count: Optional[int] = None
    store_free_space: Optional[int] = None

    check_status: StoreCheckStatus = dataclasses.field(default_factory=StoreCheckStatus)
    checked: bool = False
    good: bool | str = True

    event_log: Optional[EventLogAPI] = None
    details: dict[str, Any] = dataclasses.field(default_factory=dict)

    @property
    def online(self) -> bool:
        return bool(self.checked or self.check_status.read or self.check_status.write)
