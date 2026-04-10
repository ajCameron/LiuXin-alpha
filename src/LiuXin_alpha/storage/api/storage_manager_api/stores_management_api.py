
from __future__ import annotations

import abc

from typing import TYPE_CHECKING, Iterator

if TYPE_CHECKING:

    from LiuXin_alpha.storage.api.store_api import StoreAPI, StoreSpec
    from LiuXin_alpha.storage.storage_types import StoreID



class StoresManagerAPI(abc.ABC):
    """
    API for the storage manager component which handles stores.
    """

    # --------------------
    # - STORE CRUD METHODS

    @abc.abstractmethod
    def get_store_spec_from_db(self, store_id: "StoreID") -> "StoreSpec":
        """
        Reads and returns a store spec from the database.

        :param store_id:
        :return:
        """

    @abc.abstractmethod
    def create_store(self, new_store_spec: "StoreSpec") -> "StoreAPI":
        """
        Create (and register) a new store.

        :param new_store_spec: Spec for the new store we're creating
        :return:
        """

    # Todo: Callbacks for when a new store is added? Event bus takes care of all that?
    @abc.abstractmethod
    def add_store(
        self,
        new_store: "StoreAPI") -> bool:
        """
        Register a store with the manager.

        :param new_store:
        :return:
        """

    @abc.abstractmethod
    def remove_store(
        self,
        store_id: "StoreID",
        *,
        delete_from_db: bool = False) -> bool:
        """
        Remove one store by id/name/url/uuid.

        :param store_id: The identifier of the store to remove.
        :param delete_from_db: Not only remove the store from this manager, but also delete it from the db.

        :return:
        """

    @abc.abstractmethod
    def get_store(
            self,
            store_identifier: "StoreID") -> "StoreAPI":
        """
        Resolve one store by UUID/name/url/id.

        :param store_identifier:
        :return:
        """

    @abc.abstractmethod
    def iter_stores(self) -> Iterator["StoreAPI"]:
        """
        Iterate over all available stores.

        :return:
        """

