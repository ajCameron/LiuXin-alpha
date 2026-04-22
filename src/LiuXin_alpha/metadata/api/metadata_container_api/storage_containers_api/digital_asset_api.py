
"""
Digital Assets are the actual files which are tracked by the system.
"""

from __future__ import annotations

import abc

from typing import TYPE_CHECKING, Iterator, List, Optional, Tuple, Union, Iterable

if TYPE_CHECKING:

    from LiuXin_alpha.metadata.api.metadata_container_api.storage_containers_api.asset_replica_api import (
        AssetReplicaIdentityAPI)
    from LiuXin_alpha.metadata.metadata_types import ItemID


class DigitalAssetIdentityAPI(abc.ABC):
    """
    Represents a single digital asset on the system.
    """
    # DIGITAL ASSET ROW ACCESS METHODS HERE



class DigitalAssetMetadataContainerAPI(abc.ABC):
    """
    Container for a digital metadata container.
    """
    @abc.abstractmethod
    def add_asset_replica(self, new_asset_replica: "AssetReplicaIdentityAPI") -> None:
        """
        Add an asset replica to the system.

        :param new_asset_replica:
        :return:
        """

    @abc.abstractmethod
    def remove_asset_replica(self, removed_asset_replica: "AssetReplicaIdentityAPI") -> None:
        """
        Remove an asset replica from the system.

        :param removed_asset_replica:
        :return:
        """

    @abc.abstractmethod
    def replication_status(self) -> bool:
        """
        Is the replication strategy for this asset complete?

        :return:
        """

    @abc.abstractmethod
    def backup_status(self) -> bool:
        """
        Is the backup status for the
        :return:
        """

    @abc.abstractmethod
    @property
    def item_ids(self) -> Iterable["ItemID"]:
        """
        Return all the item ids this asset is used in.

        :return:
        """






