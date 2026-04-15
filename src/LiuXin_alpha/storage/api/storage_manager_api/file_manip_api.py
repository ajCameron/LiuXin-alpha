"""Atomic digital asset access/update methods for the storage manager."""

from __future__ import annotations

import abc
import pathlib
from collections.abc import Iterator
from typing import TYPE_CHECKING, Optional, Union


if TYPE_CHECKING:
    from LiuXin_alpha.metadata.api import MetadataContainerAPI
    from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI
    from LiuXin_alpha.storage.api.info_containers_api import (
        AssetReplicaRow,
        DigitalAssetRow,
        CompositeDigitalAsset,
        CompositeDigitalAssetID,
        ReplicationPolicy)
    from LiuXin_alpha.storage.storage_types import DigitalAssetID, ItemID, StoreID


class DigitalAssetsFileManagerAPI(abc.ABC):
    """CRUD and payload access for atomic managed digital assets."""

    def locate_digital_asset(
        self,
        digital_asset_id: "DigitalAssetID",
        *,
        preferred_store_id: Optional["StoreID"] = None,
    ) -> "StoreLocationMixinAPI":
        """Preferred name for resolving one digital asset to a concrete Location."""
        return self.open_digital_asset(digital_asset_id, preferred_store_id=preferred_store_id)

    def locate_item_primary_asset(
        self,
        item_id: "ItemID",
        *,
        preferred_store_id: Optional["StoreID"] = None,
    ) -> "StoreLocationMixinAPI":
        """Preferred name for resolving the primary asset for one item."""
        return self.open_item_primary_asset(item_id, preferred_store_id=preferred_store_id)

    @abc.abstractmethod
    def materialize_composite_digital_asset(
            self, composite_digital_asset_id: "CompositeDigitalAssetID",
            *,
            override_path: Optional[pathlib.Path] = None) -> pathlib.Path:
        """
        We want to get a composite digital asset out of the system - this method does that.

        :param composite_digital_asset_id:
        :param override_path:
        :return:
        """

    @abc.abstractmethod
    def create_digital_asset_from_file(
            self,
            item_file: Union[pathlib.Path, str, bytes],
            *,
            replication_policy: "ReplicationPolicy"
            ) -> "DigitalAssetID":
        """
        Create a digital asset directly from a file.

        :param item_id:
        :param item_file:
        :param replication_policy:
        :return:
        """

    @abc.abstractmethod
    def create_digital_asset_from_row(self, digital_asset: "DigitalAssetRow") -> "DigitalAssetRow":
        """
        Write a prepared digital asset row out to the database.

        :param digital_asset:
        :return:
        """

    @abc.abstractmethod
    def get_digital_asset(self, digital_asset_id: "DigitalAssetID") -> "DigitalAssetRow":
        """
        Retrieve a digital asset row out from the database by id.

        :param digital_asset_id:
        :return:
        """

    @abc.abstractmethod
    def update_digital_asset(self, digital_asset: "DigitalAssetRow") -> "DigitalAssetRow":
        """
        Update a digital asset from a row.

        :param digital_asset:
        :return:
        """

    @abc.abstractmethod
    def delete_digital_asset(self, digital_asset_id: "DigitalAssetID") -> bool:
        """
        Delete a digital asset from the database.

        :param digital_asset_id:
        :return:
        """

    @abc.abstractmethod
    def iter_digital_assets(self) -> Iterator["DigitalAssetRow"]:
        """
        Iter over all of the digital assets on the system.

        :return:
        """

    @abc.abstractmethod
    def materialize_digital_asset(
        self,
        digital_asset_id: "DigitalAssetID",
        file_bytes: bytes,
        *,
        preferred_store_id: Optional["StoreID"] = None,
        metadata: Optional["MetadataContainerAPI"] = None,
    ) -> "AssetReplicaRow":
        """
        Write bytes for one managed digital asset to a concrete store as a replica.

        :param digital_asset_id:
        :param file_bytes:
        :param preferred_store_id:
        :param metadata:
        :return:
        """

    @abc.abstractmethod
    def open_digital_asset(
        self,
        digital_asset_id: "DigitalAssetID",
        *,
        preferred_store_id: Optional["StoreID"] = None,
    ) -> "StoreLocationMixinAPI":
        """
        Open a file linked to a digital asset.

        Unless a preferred store id is provided, will just pull the primary.
        (Falling back to other stores if needed).
        :param digital_asset_id:
        :param preferred_store_id:
        :return:
        """

    @abc.abstractmethod
    def open_item_primary_asset(
        self,
        item_id: "ItemID",
        *,
        preferred_store_id: Optional["StoreID"] = None,
    ) -> "StoreLocationMixinAPI":
        """
        Open and return the primary file for the primary asset of the item.

        :param item_id:
        :param preferred_store_id:
        :return:
        """
