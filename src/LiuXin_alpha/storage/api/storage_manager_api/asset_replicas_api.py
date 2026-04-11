"""Replica access/update methods for the storage manager."""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.info_containers_api import AssetReplicaRecord
    from LiuXin_alpha.storage.storage_types import AssetReplicaID, DigitalAssetID, StoreID


class AssetReplicasManagerAPI(abc.ABC):
    """CRUD-ish access to physical asset replicas."""

    @abc.abstractmethod
    def create_asset_replica(self, asset_replica: "AssetReplicaRecord") -> "AssetReplicaRecord":
        ...

    @abc.abstractmethod
    def get_asset_replica(self, asset_replica_id: "AssetReplicaID") -> "AssetReplicaRecord":
        ...

    @abc.abstractmethod
    def update_asset_replica(self, asset_replica: "AssetReplicaRecord") -> "AssetReplicaRecord":
        ...

    @abc.abstractmethod
    def delete_asset_replica(self, asset_replica_id: "AssetReplicaID") -> bool:
        ...

    @abc.abstractmethod
    def iter_asset_replicas(self) -> Iterator["AssetReplicaRecord"]:
        ...

    @abc.abstractmethod
    def iter_digital_asset_replicas(self, digital_asset_id: "DigitalAssetID") -> Iterator["AssetReplicaRecord"]:
        ...

    @abc.abstractmethod
    def iter_store_replicas(self, store_id: "StoreID") -> Iterator["AssetReplicaRecord"]:
        ...
