"""Store-container orchestration methods for the storage manager.

Examples:
    Inspect the configured stores without reaching into manager internals::

        names = [container.store_name for container in manager.iter_store_containers()]
"""

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

    Examples:
        Resolve a registered store by name, id, or UUID::

            container = manager.get_store_container("main")
    """

    @abc.abstractmethod
    def get_store_spec_from_db(self, store_id: "StoreID") -> "StoreSpec":
        """Load a configured store specification from the bound database.

        Examples:
            Read store ``3`` without instantiating its plugin::

                spec = manager.get_store_spec_from_db(3)
        """
        ...

    @abc.abstractmethod
    def create_store_plugin(self, store_spec: "StoreSpec") -> "StorePluginAPI":
        """Instantiate the backend plugin described by a store spec.

        Examples:
            Build the physical backend for an existing specification::

                plugin = manager.create_store_plugin(spec)
        """
        ...

    @abc.abstractmethod
    def build_store_container(self, store_spec: "StoreSpec") -> "StoreContainerAPI":
        """Wrap a specification and plugin in a managed container.

        Examples:
            Build, then register, a configured store::

                container = manager.build_store_container(spec)
        """
        ...

    @abc.abstractmethod
    def register_store_container(self, store_container: "StoreContainerAPI") -> bool:
        """Register a container with this manager.

        Examples:
            Register a newly built container::

                added = manager.register_store_container(container)
        """
        ...

    @abc.abstractmethod
    def unregister_store_container(self, store_ref: "StoreRef", *, delete_from_db: bool = False) -> bool:
        """Remove a container from the registry and optionally its database row.

        Examples:
            Detach a store while retaining its durable configuration::

                removed = manager.unregister_store_container("archive")
        """
        ...

    @abc.abstractmethod
    def get_store_container(self, store_ref: "StoreRef") -> "StoreContainerAPI":
        """Resolve a container by store id, UUID, name, or accepted reference.

        Examples:
            Select the container named ``main``::

                container = manager.get_store_container("main")
        """
        ...

    @abc.abstractmethod
    def iter_store_containers(self) -> Iterator["StoreContainerAPI"]:
        """Iterate over registered store containers.

        Examples:
            Probe every configured store::

                statuses = [container.probe() for container in manager.iter_store_containers()]
        """
        ...

    def load_store_container_from_db(self, store_id: "StoreID") -> "StoreContainerAPI":
        """Build a container from a database-backed store specification.

        Examples:
            Load store ``3`` before registering it::

                container = manager.load_store_container_from_db(3)
        """
        return self.build_store_container(self.get_store_spec_from_db(store_id))

    def get_store_plugin(self, store_ref: "StoreRef") -> "StorePluginAPI":
        """Return the raw plugin owned by a registered container.

        Examples:
            Access backend capabilities for a selected store::

                plugin = manager.get_store_plugin("main")
        """
        return self.get_store_container(store_ref).plugin

    def get_store(self, store_ref: "StoreRef") -> "StorePluginAPI":
        """Compatibility alias for :meth:`get_store_plugin`.

        Examples:
            Resolve the default application's named store::

                plugin = manager.get_store("main")
        """
        return self.get_store_plugin(store_ref)

    def iter_store_plugins(self) -> Iterator["StorePluginAPI"]:
        """Iterate over the plugins owned by registered containers.

        Examples:
            Read each backend's plugin kind::

                kinds = [plugin.plugin_kind for plugin in manager.iter_store_plugins()]
        """
        for store_container in self.iter_store_containers():
            yield store_container.plugin

    @abc.abstractmethod
    def bind_store_id(self, store_id: "StoreID", store_ref: "StoreRef") -> None:
        """Bind a durable database id to an already registered store.

        Examples:
            Associate an inserted row with the in-memory ``main`` store::

                manager.bind_store_id(3, "main")
        """
        ...

    @abc.abstractmethod
    def set_default_store(self, store_ref: "StoreRef") -> None:
        """Choose the store used when no preference is supplied.

        Examples:
            Route subsequent writes to ``main`` by default::

                manager.set_default_store("main")
        """
        ...

    @abc.abstractmethod
    def get_default_store_container(self) -> "StoreContainerAPI":
        """Return the currently selected default container.

        Examples:
            Inspect the default store's health::

                status = manager.get_default_store_container().status()
        """
        ...
