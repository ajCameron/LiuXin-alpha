"""Storage API contracts for managed digital assets."""

from __future__ import annotations

import abc
from collections.abc import Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.asset_replica_api import AssetReplicaIdentityAPI
    from LiuXin_alpha.storage.storage_types import ItemID


class DigitalAssetIdentityAPI(abc.ABC):
    """Represents one managed digital asset in the storage graph."""


class DigitalAssetMetadataAPI(abc.ABC):
    """Storage-facing metadata bundle for a digital asset."""

    @abc.abstractmethod
    def add_asset_replica(self, new_asset_replica: "AssetReplicaIdentityAPI") -> None:
        """Add a concrete replica to this asset."""

    @abc.abstractmethod
    def remove_asset_replica(self, removed_asset_replica: "AssetReplicaIdentityAPI") -> None:
        """Remove a concrete replica from this asset."""

    @abc.abstractmethod
    def replication_status(self) -> bool:
        """Return whether this asset's replication strategy is satisfied."""

    @abc.abstractmethod
    def backup_status(self) -> bool:
        """Return whether this asset's backup strategy is satisfied."""

    @property
    @abc.abstractmethod
    def item_ids(self) -> Iterable["ItemID"]:
        """Return item ids that use this asset."""


__all__ = ["DigitalAssetIdentityAPI", "DigitalAssetMetadataAPI"]
