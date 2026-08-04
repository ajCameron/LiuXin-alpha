
"""API for legacy database integration of a store.

Examples:
    Resolve a replica row through a database-aware store::

        replica = store.get_replica(17)
"""

from __future__ import annotations

import abc

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
    from LiuXin_alpha.storage.api.info_containers_api import FixedTableStorageRow
    from LiuXin_alpha.storage.api.info_containers_api import AssetReplica


class StoreDBAPI(abc.ABC):
    """
    DB interfaces for the store.

    Examples:
        Retrieve the configured store row::

            store_row = store.get_store_row()
    """
    @abc.abstractmethod
    def get_store_row(self) -> "FixedTableStorageRow":
        """
        Get the row for the store.

        :param store_id:
        :return:

        Examples:
            Read the row associated with this store::

                row = store.get_store_row()
        """

    @abc.abstractmethod
    def get_replica(self, replica_id: int) -> "AssetReplica":
        """
        Get a specific replica from the store by its db id.

        Raises WrongStoreError if the replica is not in this store.
        :param replica_id:
        :return:

        Examples:
            Load replica ``17`` from its owning store::

                replica = store.get_replica(17)
        """
    @abc.abstractmethod
    def check_replica(self, replica_id: int) -> bool:
        """
        Check to see if the given replica exists in the store.

        :param replica_id:
        :return:

        Examples:
            Verify that replica ``17`` still exists::

                present = store.check_replica(17)
        """
