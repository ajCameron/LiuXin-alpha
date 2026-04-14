"""Atomic digital asset access/update methods for the storage manager."""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from LiuXin_alpha.metadata.api import MetadataContainerAPI
    from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI
    from LiuXin_alpha.storage.api.info_containers_api import AssetReplicaRow, DigitalAssetRow
    from LiuXin_alpha.storage.storage_types import DigitalAssetID, ItemID, StoreID


class DigitalAssetsManagerAPI(abc.ABC):
    """CRUD and payload access for atomic managed digital assets."""

    @abc.abstractmethod
    def create_digital_asset(self, digital_asset: "DigitalAssetRow") -> "DigitalAssetRow":
        ...

    @abc.abstractmethod
    def get_digital_asset(self, digital_asset_id: "DigitalAssetID") -> "DigitalAssetRow":
        ...

    @abc.abstractmethod
    def update_digital_asset(self, digital_asset: "DigitalAssetRow") -> "DigitalAssetRow":
        ...

    @abc.abstractmethod
    def delete_digital_asset(self, digital_asset_id: "DigitalAssetID") -> bool:
        ...

    @abc.abstractmethod
    def iter_digital_assets(self) -> Iterator["DigitalAssetRow"]:
        ...

    @abc.abstractmethod
    def materialize_digital_asset(
        self,
        digital_asset_id: "DigitalAssetID",
        file_bytes: bytes,
        *,
        preferred_store_id: Optional["StoreID"] = None,
        metadata: Optional["MetadataContainerAPI"] = None,
    ) -> "AssetReplicaRow":
        """Write bytes for one managed digital asset to a concrete store as a replica."""

    @abc.abstractmethod
    def open_digital_asset(
        self,
        digital_asset_id: "DigitalAssetID",
        *,
        preferred_store_id: Optional["StoreID"] = None,
    ) -> "StoreLocationMixinAPI":
        ...

    @abc.abstractmethod
    def open_item_primary_asset(
        self,
        item_id: "ItemID",
        *,
        preferred_store_id: Optional["StoreID"] = None,
    ) -> "StoreLocationMixinAPI":
        ...
