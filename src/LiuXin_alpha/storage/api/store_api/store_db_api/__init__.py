
"""
API for the database integration of the store.
"""

from __future__ import annotations

import abc

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI
    from LiuXin_alpha.storage.api.info_containers_api import FixedTableStorageRow
    from LiuXin_alpha.storage.api.info_containers_api import AssetReplica


class StoreDBAPI(abc.ABC):
    """
    DB interfaces for the store.
    """
    @abc.abstractmethod
    def get_store_row(self) -> "FixedTableStorageRow":
        """
        Get the row for the store.

        :param store_id:
        :return:
        """

    @abc.abstractmethod
    def get_replica(self, replica_id: int) -> "AssetReplica":
        """
        Get a specific replica from the store by its db id.

        Raises WrongStoreError if the replica is not in this store.
        :param replica_id:
        :return:
        """
    @abc.abstractmethod
    def check_replica(self, replica_id: int) -> bool:
        """
        Check to see if the given replica exists in the store.

        :param replica_id:
        :return:
        """
