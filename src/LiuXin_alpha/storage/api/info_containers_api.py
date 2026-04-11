"""
Containers and row helpers for managed digital assets and storage state.
"""

from __future__ import annotations

import dataclasses

from typing import TYPE_CHECKING, Any, ClassVar, Optional

from LiuXin_alpha.databases import Row

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import DatabaseAPI
    from LiuXin_alpha.storage.storage_types import (
        AssetReplicaID,
        BackupPolicyID,
        DigitalAssetCompositionID,
        DigitalAssetID,
        ItemDigitalAssetLinkID,
        ItemID,
        ReplicationPolicyID,
        StoreID,
    )
    from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI


class FixedTableStorageRow(Row):
    """
    Small storage-facing ``Row`` specialisation with a fixed backing table.

    The generic ``Row`` class infers its table from the available columns. That is
    useful generally, but a little awkward for API surface objects that are meant
    to represent exactly one table. This helper pins the table early, offers more
    convenient constructors, and provides a validation hook for subclasses.
    """

    TABLE_NAME: ClassVar[Optional[str]] = None
    ID_COLUMN: ClassVar[Optional[str]] = None

    def __init__(self, database: "DatabaseAPI", row_dict: Optional[dict[str, Any]] = None, read_only: bool = False) -> None:
        super().__init__(database=database, row_dict=row_dict, read_only=read_only)

        table_name = self.TABLE_NAME
        if table_name is None:
            raise TypeError(f"{self.__class__.__name__} must define TABLE_NAME.")

        current_table = getattr(self, "table", None)
        if current_table is None:
            self._bind_fixed_table_metadata(table_name)
        elif current_table != table_name:
            raise ValueError(
                f"{self.__class__.__name__} expected table '{table_name}' but row_dict maps to '{current_table}'."
            )

        self.validate()

    def _bind_fixed_table_metadata(self, table_name: str) -> None:
        """Populate cached row metadata for an explicitly fixed table."""
        object.__setattr__(self, "_table", table_name)
        object.__setattr__(self, "allowed_tables", self.db.driver_wrapper.get_allowed_tables_snapshot())
        object.__setattr__(self, "self_linkable", bool(self.db.driver_wrapper.check_for_intralink_table(table_name)))
        object.__setattr__(self, "linkable_tables", self.db.driver_wrapper.get_interlinked_tables(table_name))
        object.__setattr__(self, "allowed_columns", self.db.get_column_headings(table_name))

        id_column = self.ID_COLUMN or self.db.driver_wrapper.get_id_column(table_name)
        object.__setattr__(self, "row_id", self.int_row_dict.get(id_column))

    @classmethod
    def blank(cls, database: "DatabaseAPI", *, read_only: bool = False) -> "FixedTableStorageRow":
        """Return a blank row for this fixed table."""
        row = cls(database=database, row_dict=None, read_only=read_only)
        row.load_blank_row(table=cls.TABLE_NAME)
        return row

    @classmethod
    def from_row_id(
        cls,
        database: "DatabaseAPI",
        row_id: int,
        *,
        read_only: bool = False,
    ) -> "FixedTableStorageRow":
        """Load one row by id from this fixed table."""
        row = cls(database=database, row_dict=None, read_only=read_only)
        row.load_row_from_id(row_id=row_id, table=cls.TABLE_NAME)
        return row

    @classmethod
    def from_idless_row_dict(
        cls,
        database: "DatabaseAPI",
        row_dict: dict[str, Any],
        *,
        table: Optional[str] = None,
        read_only: bool = False,
        reload_from_db: bool = True,
    ) -> "FixedTableStorageRow":
        """Insert one row for this fixed table and return it as the subclass."""
        if table is not None and table != cls.TABLE_NAME:
            raise ValueError(f"{cls.__name__} only supports table '{cls.TABLE_NAME}', not '{table}'.")

        draft = cls(database=database, row_dict=row_dict, read_only=read_only)
        draft.validate()

        return super().from_idless_row_dict(
            database=database,
            row_dict=row_dict,
            table=cls.TABLE_NAME,
            read_only=read_only,
            reload_from_db=reload_from_db,
        )

    @property
    def primary_id(self) -> Optional[int]:
        """Return the fixed table's primary id column, if present."""
        id_column = self.ID_COLUMN or self.db.driver_wrapper.get_id_column(self.TABLE_NAME)
        return self.row_dict.get(id_column)

    @primary_id.setter
    def primary_id(self, value: Optional[int]) -> None:
        id_column = self.ID_COLUMN or self.db.driver_wrapper.get_id_column(self.TABLE_NAME)
        self[id_column] = value
        object.__setattr__(self, "row_id", value)

    def sync(self) -> None:
        """Validate before syncing the row back to the database."""
        self.validate()
        super().sync()

    def validate(self) -> None:
        """Hook for subclasses to add light table-specific validation."""
        return None


class DigitalAssetRow(FixedTableStorageRow):
    """One managed digital asset row."""

    TABLE_NAME = "digital_assets"
    ID_COLUMN = "digital_asset_id"

    @property
    def digital_asset_id(self) -> Optional["DigitalAssetID"]:
        return self[self.ID_COLUMN]

    @digital_asset_id.setter
    def digital_asset_id(self, value: Optional["DigitalAssetID"]) -> None:
        self.primary_id = value

    def validate(self) -> None:
        kind = self.row_dict.get("digital_asset_kind")
        if kind is not None and kind not in {"atomic", "composite"}:
            raise ValueError("digital_asset_kind must be either 'atomic' or 'composite'.")


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


class ItemDigitalAssetLinkRow(FixedTableStorageRow):
    """One semantic link from an item to a managed digital asset."""

    TABLE_NAME = "items__digital_assets__links"
    ID_COLUMN = "item_digital_asset_link_id"

    @property
    def item_digital_asset_link_id(self) -> Optional["ItemDigitalAssetLinkID"]:
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
        parent_id = self.row_dict.get("parent_digital_asset_id")
        member_id = self.row_dict.get("member_digital_asset_id")
        if parent_id is not None and member_id is not None and parent_id == member_id:
            raise ValueError("Composite digital assets cannot directly contain themselves.")

        sequence_number = self.row_dict.get("sequence_number")
        if sequence_number is not None and sequence_number < 0:
            raise ValueError("sequence_number must be >= 0.")


# Backwards-compatible API names while the storage layer is being migrated.
DigitalAssetRecord = DigitalAssetRow
AssetReplicaRecord = AssetReplicaRow
ItemDigitalAssetLinkRecord = ItemDigitalAssetLinkRow
DigitalAssetCompositionMemberRecord = DigitalAssetCompositionMemberRow


@dataclasses.dataclass(slots=True)
class DigitalAssetReplicationCluster:
    """
    Informational container describing nominally identical managed digital assets.

    This is intentionally more granular than most callers should need. It remains
    useful for diagnostics and low-level reconciliation work.
    """

    digital_asset_locs: dict["DigitalAssetID", "StoreLocationMixinAPI"]
    replication_level: int
    digital_asset_hash: str

    @property
    def digital_asset_ids(self) -> set["DigitalAssetID"]:
        """Return the managed digital asset ids in the cluster."""
        return set(self.digital_asset_locs.keys())


__all__ = [
    "AssetReplicaRecord",
    "AssetReplicaRow",
    "DigitalAssetCompositionMemberRecord",
    "DigitalAssetCompositionMemberRow",
    "DigitalAssetRecord",
    "DigitalAssetReplicationCluster",
    "DigitalAssetRow",
    "FixedTableStorageRow",
    "ItemDigitalAssetLinkRecord",
    "ItemDigitalAssetLinkRow",
]
