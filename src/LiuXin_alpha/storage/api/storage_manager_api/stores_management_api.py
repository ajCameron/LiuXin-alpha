from __future__ import annotations

import abc

from typing import TYPE_CHECKING, Iterator

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.storage_api import StoreAPI, StoreSpec
    from LiuXin_alpha.storage.storage_types import StoreID


class StoresManagerAPI(abc.ABC):
    """API for the storage manager component which handles stores."""

    @abc.abstractmethod
    def get_store_spec_from_db(self, store_id: "StoreID") -> "StoreSpec":
        ...

    @abc.abstractmethod
    def create_store(self, new_store_spec: "StoreSpec") -> "StoreAPI":
        ...

    @abc.abstractmethod
    def add_store(self, new_store: "StoreAPI") -> bool:
        ...

    @abc.abstractmethod
    def remove_store(self, store_id: "StoreID", *, delete_from_db: bool = False) -> bool:
        ...

    @abc.abstractmethod
    def get_store(self, store_identifier: "StoreID") -> "StoreAPI":
        ...

    @abc.abstractmethod
    def iter_stores(self) -> Iterator["StoreAPI"]:
        ...
