"""
Configured-store administration facade.
"""

from __future__ import annotations

import abc

from collections.abc import Iterator
from typing import TYPE_CHECKING

from LiuXin_alpha.storage.api.models import Location, StoreRef, StoreStatus
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    StorageBootstrapReport, StoreSpec, TopologyRelation,
)

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.store_api import StoreAPI


class StoreAdministrationAPI(abc.ABC):
    """Configure, inspect, start, and stop the manager's backend stores.

    Example:
        >>> def choose_primary(manager: StoreAdministrationAPI) -> None:
        ...     manager.set_default_store(UUID(int=1))
    """

    @abc.abstractmethod
    def create_store(
        self, spec: StoreSpec, *, startup: bool = True,
    ) -> StoreSpec:
        """
        Register a store specification and optionally start its backend.

        Example:
            >>> created = manager.create_store(spec, startup=True)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def update_store(self, store_ref: StoreRef, spec: StoreSpec) -> StoreSpec:
        """Replace durable configuration for one existing store.

        Example:
            >>> updated = manager.update_store(UUID(int=1), spec)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def remove_store(
        self,
        store_ref: StoreRef,
        *,
        forget_configuration: bool = False,
    ) -> bool:
        """Stop and unregister a Store, optionally forgetting configuration.

        Example:
            >>> removed = manager.remove_store(  # doctest: +SKIP
            ...     retired_uuid, forget_configuration=True,
            ... )
        """
        ...

    @abc.abstractmethod
    def get_store_spec(self, store_ref: StoreRef) -> StoreSpec:
        """Return durable configuration for one store.

        Example:
            >>> spec = manager.get_store_spec(UUID(int=1))  # doctest: +SKIP
        """
        ...

    def compare_location_hosts(
        self,
        source: Location,
        destination: Location,
    ) -> TopologyRelation:
        """Compare the declared host computers for two Locations.

        The Store UUIDs on the Locations are resolved through durable Store
        specifications. Missing host metadata produces ``UNKNOWN``, never a
        false claim that the Stores are on different computers.

        Example:
            >>> relation = manager.compare_location_hosts(  # doctest: +SKIP
            ...     source_location, destination_location,
            ... )
            >>> relation is TopologyRelation.SAME  # doctest: +SKIP
            True
        """

        source_host = self.get_store_spec(source.store_ref).store_host_uuid
        destination_host = self.get_store_spec(
            destination.store_ref
        ).store_host_uuid
        if source_host is None or destination_host is None:
            return TopologyRelation.UNKNOWN
        if source_host == destination_host:
            return TopologyRelation.SAME
        return TopologyRelation.DIFFERENT

    def compare_location_devices(
        self,
        source: Location,
        destination: Location,
    ) -> TopologyRelation:
        """Compare the declared physical devices for two Locations.

        This can distinguish two Stores on one disk from two Stores merely
        attached to the same host. Unknown device metadata remains explicit.

        Example:
            >>> relation = manager.compare_location_devices(  # doctest: +SKIP
            ...     source_location, destination_location,
            ... )
        """

        source_device = self.get_store_spec(source.store_ref).store_device_uuid
        destination_device = self.get_store_spec(
            destination.store_ref
        ).store_device_uuid
        if source_device is None or destination_device is None:
            return TopologyRelation.UNKNOWN
        if source_device == destination_device:
            return TopologyRelation.SAME
        return TopologyRelation.DIFFERENT

    @abc.abstractmethod
    def iter_store_specs(self) -> Iterator[StoreSpec]:
        """Iterate over configured store specifications.

        Example:
            >>> specs = list(manager.iter_store_specs())  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def get_store(self, store_ref: StoreRef) -> StoreAPI:
        """Return the live configured-store facade for one store UUID.

        Example:
            >>> store = manager.get_store(UUID(int=1))  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def iter_stores(self) -> Iterator[StoreAPI]:
        """Iterate over live configured-store facades managed by this manager.

        Example:
            >>> stores = list(manager.iter_stores())  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def iter_store_statuses(self, *, refresh: bool = False) -> Iterator[StoreStatus]:
        """Yield status for every configured store.

        Example:
            >>> statuses = list(manager.iter_store_statuses())  # doctest: +SKIP
        """

        for spec in self.iter_store_specs():
            if refresh:
                yield self.get_store(spec.store_uuid).status(refresh=True)
            else:
                yield self.get_store(spec.store_uuid).status()

    @abc.abstractmethod
    def reload_stores(
        self, *, include_offline: bool = False, replace_existing: bool = True,
    ) -> StorageBootstrapReport:
        """Rebuild the runtime store registry from durable configuration.

        Example:
            >>> report = manager.reload_stores(  # doctest: +SKIP
            ...     include_offline=True, replace_existing=True,
            ... )
        """
        ...

    @abc.abstractmethod
    def set_default_store(self, store_ref: StoreRef) -> None:
        """Choose the store used when higher-level placement has no preference.

        Example:
            >>> manager.set_default_store(UUID(int=1))  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def get_default_store_ref(self) -> StoreRef:
        """Return the configured default store UUID.

        Example:
            >>> store_ref = manager.get_default_store_ref()  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def close(self) -> None:
        """Release resources held by every started store backend.

        Example:
            >>> manager.close()  # doctest: +SKIP
        """
        ...


__all__ = ["StoreAdministrationAPI"]
