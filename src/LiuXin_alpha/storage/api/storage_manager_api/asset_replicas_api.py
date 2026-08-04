"""Replica access/update methods for the storage manager.

Examples:
    List every replica known to a concrete manager::

        replicas = list(manager.iter_asset_replicas())
"""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.info_containers_api import AssetReplicaRow
    from LiuXin_alpha.storage.storage_types import AssetReplicaID, DigitalAssetID, StoreID


class AssetReplicasManagerAPI(abc.ABC):
    """
    CRUD-ish access to physical asset replicas.

    At this point, we're actually interacting with real (ish) files in real (ish) stores.

    Examples:
        Concrete storage managers expose this contract directly::

            replicas = list(manager.iter_store_replicas(store_id=3))
    """

    @abc.abstractmethod
    def create_asset_replica(self, asset_replica: "AssetReplicaRow") -> "AssetReplicaRow":
        """
        Create an asset replica from an instructional asset_replica row.

        :param asset_replica:
        :return:

        Examples:
            Persist an instructional row and retain the assigned id::

                replica = manager.create_asset_replica(replica_row)
        """

    @abc.abstractmethod
    def get_asset_replica(self, asset_replica_id: "AssetReplicaID") -> "AssetReplicaRow":
        """
        Retrieve the asset replica row from the given id.

        :param asset_replica_id:
        :return:

        Examples:
            Load replica ``17``::

                replica = manager.get_asset_replica(17)
        """

    @abc.abstractmethod
    def update_asset_replica(self, asset_replica: "AssetReplicaRow") -> "AssetReplicaRow":
        """
        Write an asset replica row out to the database.

        :param asset_replica:
        :return:

        Examples:
            Save a changed status on an existing row::

                replica["asset_replica_integrity_status"] = "verified"
                replica = manager.update_asset_replica(replica)
        """

    @abc.abstractmethod
    def delete_asset_replica(self, asset_replica_id: "AssetReplicaID") -> bool:
        """
        Delete an asset replica row from the database.

        :param asset_replica_id:
        :return:

        Examples:
            Remove a stale replica record::

                removed = manager.delete_asset_replica(17)
        """

    @abc.abstractmethod
    def iter_asset_replicas(self) -> Iterator["AssetReplicaRow"]:
        """
        Iterate over all the asset replica rows in the database.

        :return:

        Examples:
            Materialise the iterator when a snapshot is needed::

                replicas = list(manager.iter_asset_replicas())
        """

    @abc.abstractmethod
    def iter_digital_asset_replicas(self, digital_asset_id: "DigitalAssetID") -> Iterator["AssetReplicaRow"]:
        """
        Iter over all the asset replica rows for a given digital_asset.

        :param digital_asset_id:
        :return:

        Examples:
            Inspect every physical copy of asset ``42``::

                replicas = list(manager.iter_digital_asset_replicas(42))
        """

    @abc.abstractmethod
    def iter_store_replicas(self, store_id: "StoreID") -> Iterator["AssetReplicaRow"]:
        """
        Iterate over all the replica rows in the database for the given store.

        :param store_id:
        :return:

        Examples:
            Inspect the replicas held by store ``3``::

                replicas = list(manager.iter_store_replicas(3))
        """
