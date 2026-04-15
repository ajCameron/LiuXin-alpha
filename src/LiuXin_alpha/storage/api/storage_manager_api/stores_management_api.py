from __future__ import annotations

import abc

from typing import TYPE_CHECKING, Iterator

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api import StoreAPI, StoreSpec
    from LiuXin_alpha.storage.api.store_container_api import StoreContainerAPI
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


    # ------------------------------------------------------------------
    # Preferred managed-store/container naming
    # ------------------------------------------------------------------

    def load_store_container(self, new_store_spec: "StoreSpec") -> "StoreContainerAPI":
        """Create/load one managed store container from a store spec."""
        maybe_store = self.create_store(new_store_spec)
        from LiuXin_alpha.storage.store_container import StoreContainer

        if isinstance(maybe_store, StoreContainer):
            return maybe_store
        return StoreContainer.from_plugin(maybe_store, db=getattr(self, "db", None), store_id=new_store_spec.store_id)

    def load_store_container_from_db(self, store_id: "StoreID") -> "StoreContainerAPI":
        """Load a managed store container from the database-backed spec."""
        return self.load_store_container(self.get_store_spec_from_db(store_id))

    def register_store_container(self, new_store_container: "StoreContainerAPI") -> bool:
        """Register one managed store container with the manager."""
        return bool(self.add_store(new_store_container.plugin))

    def unregister_store_container(self, store_id: "StoreID", *, delete_from_db: bool = False) -> bool:
        """Unregister one managed store container from the manager."""
        return self.remove_store(store_id, delete_from_db=delete_from_db)

    def get_store_container(self, store_identifier: "StoreID") -> "StoreContainerAPI":
        """Return the managed store container for one configured store."""
        from LiuXin_alpha.storage.store_container import StoreContainer

        maybe_store = self.get_store(store_identifier)
        if isinstance(maybe_store, StoreContainer):
            return maybe_store
        store_id: int | None
        try:
            store_id = int(store_identifier)
        except Exception:
            store_id = None
        return StoreContainer.from_plugin(maybe_store, db=getattr(self, "db", None), store_id=store_id)

    def iter_store_containers(self) -> Iterator["StoreContainerAPI"]:
        """Iterate over managed store containers."""
        from LiuXin_alpha.storage.store_container import StoreContainer

        for store in self.iter_stores():
            if isinstance(store, StoreContainer):
                yield store
                continue
            yield StoreContainer.from_plugin(store, db=getattr(self, "db", None))
