"""Storage API contracts for managed digital assets.

Examples:
    Code that only needs identity can accept the narrow contract::

        def enqueue(asset: DigitalAssetIdentityAPI) -> None:
            work_queue.append(asset)
"""

from __future__ import annotations

import abc
from collections.abc import Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.asset_replica_api import AssetReplicaIdentityAPI
    from LiuXin_alpha.storage.storage_types import ItemID


class DigitalAssetIdentityAPI(abc.ABC):
    """Represents one managed digital asset in the storage graph.

    Examples:
        Keep graph code independent of database row implementations::

            def visit(asset: DigitalAssetIdentityAPI) -> None:
                visited.add(asset)
    """


class DigitalAssetMetadataAPI(abc.ABC):
    """Storage-facing metadata bundle for a digital asset.

    Examples:
        Inspect policy health through a concrete metadata object::

            healthy = asset.replication_status() and asset.backup_status()
    """

    @abc.abstractmethod
    def add_asset_replica(self, new_asset_replica: "AssetReplicaIdentityAPI") -> None:
        """Add a concrete replica to this asset.

        Examples:
            Attach a newly created physical copy::

                asset.add_asset_replica(replica)
        """

    @abc.abstractmethod
    def remove_asset_replica(self, removed_asset_replica: "AssetReplicaIdentityAPI") -> None:
        """Remove a concrete replica from this asset.

        Examples:
            Detach a replica that has been deleted from its store::

                asset.remove_asset_replica(replica)
        """

    @abc.abstractmethod
    def replication_status(self) -> bool:
        """Return whether this asset's replication strategy is satisfied.

        Examples:
            Queue remediation only when the strategy is unmet::

                if not asset.replication_status():
                    replication_queue.append(asset)
        """

    @abc.abstractmethod
    def backup_status(self) -> bool:
        """Return whether this asset's backup strategy is satisfied.

        Examples:
            Find assets that still need a cold backup::

                needs_backup = not asset.backup_status()
        """

    @property
    @abc.abstractmethod
    def item_ids(self) -> Iterable["ItemID"]:
        """Return item ids that use this asset.

        Examples:
            Materialise the related ids for display::

                related_items = list(asset.item_ids)
        """


__all__ = ["DigitalAssetIdentityAPI", "DigitalAssetMetadataAPI"]
