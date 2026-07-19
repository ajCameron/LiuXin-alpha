"""Store-container orchestration methods for the storage manager."""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.info_containers_api import StoreSpec
    from LiuXin_alpha.storage.api.store_container_api import StoreContainerAPI
    from LiuXin_alpha.storage.api.store_plugin_api import StorePluginAPI
    from LiuXin_alpha.storage.storage_types import StoreID, StoreRef


class StoresManagerAPI(abc.ABC):
    """Orchestration API for configured store containers.

    The manager owns many containers. Each container owns one plugin. The
    manager should not bypass containers and talk raw-backend state into its own
    registry structures.
    """

    @abc.abstractmethod
    def get_store_spec_from_db(self, store_id: "StoreID") -> "StoreSpec":
        ...

    @abc.abstractmethod
    def create_store_plugin(self, store_spec: "StoreSpec") -> "StorePluginAPI":
        ...

    @abc.abstractmethod
    def build_store_container(self, store_spec: "StoreSpec") -> "StoreContainerAPI":
        ...

    @abc.abstractmethod
    def register_store_container(self, store_container: "StoreContainerAPI") -> bool:
        ...

    @abc.abstractmethod
    def unregister_store_container(self, store_ref: "StoreRef", *, delete_from_db: bool = False) -> bool:
        ...

    @abc.abstractmethod
    def get_store_container(self, store_ref: "StoreRef") -> "StoreContainerAPI":
        ...

    @abc.abstractmethod
    def iter_store_containers(self) -> Iterator["StoreContainerAPI"]:
        ...

    def load_store_container_from_db(self, store_id: "StoreID") -> "StoreContainerAPI":
        return self.build_store_container(self.get_store_spec_from_db(store_id))

    def get_store_plugin(self, store_ref: "StoreRef") -> "StorePluginAPI":
        return self.get_store_container(store_ref).plugin

    def get_store(self, store_ref: "StoreRef") -> "StorePluginAPI":
        return self.get_store_plugin(store_ref)

    def iter_store_plugins(self) -> Iterator["StorePluginAPI"]:
        for store_container in self.iter_store_containers():
            yield store_container.plugin

    @abc.abstractmethod
    def bind_store_id(self, store_id: "StoreID", store_ref: "StoreRef") -> None:
        ...

    @abc.abstractmethod
    def set_default_store(self, store_ref: "StoreRef") -> None:
        ...

    @abc.abstractmethod
    def get_default_store_container(self) -> "StoreContainerAPI":
        ...
