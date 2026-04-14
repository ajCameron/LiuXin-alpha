from __future__ import annotations

import abc

from typing import TYPE_CHECKING, Iterator

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api import StoreAPI, StoreSpec
    from LiuXin_alpha.storage.storage_types import StoreID


class StoresManagerAPI(abc.ABC):
    """API for the storage manager component which handles stores."""

    @abc.abstractmethod
    def get_store_spec_from_db(self, store_id: "StoreID") -> "StoreSpec":
        """
        Load the storage spec from a row on the database.

        :param store_id:
        :return:
        """

    @abc.abstractmethod
    def create_store(self, new_store_spec: "StoreSpec") -> "StoreAPI":
        """
        Create and return a store.
        
        :param new_store_spec: 
        :return: 
        """

    @abc.abstractmethod
    def add_store(self, new_store: "StoreAPI") -> bool:
        """
        Add a store to the internal cache.
        
        :param new_store: 
        :return: 
        """

    @abc.abstractmethod
    def remove_store(self, store_id: "StoreID", *, delete_from_db: bool = False) -> bool:
        """
        Remove a store from the internal cache.

        :param store_id:
        :param delete_from_db:
        :return:
        """

    @abc.abstractmethod
    def get_store(self, store_identifier: "StoreID") -> "StoreAPI":
        """
        Get a store from the internal cache.

        :param store_identifier:
        :return:
        """

    @abc.abstractmethod
    def iter_stores(self) -> Iterator["StoreAPI"]:
        """
        Iterate over all available stores.

        :return:
        """
