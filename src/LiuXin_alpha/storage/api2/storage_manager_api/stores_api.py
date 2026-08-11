"""
Configured-store administration facade.
"""

from __future__ import annotations

import abc

from collections.abc import Iterator
from typing import TYPE_CHECKING

from LiuXin_alpha.storage.api2.models import StoreRef, StoreStatus
from LiuXin_alpha.storage.api2.storage_manager_api.models import (
    StorageBootstrapReport, StoreSpec,
)

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api2.store_api import StoreAPI


class StoreAdministrationAPI(abc.ABC):
    """Configure, inspect, start, and stop the manager's backend stores.

    Example:
        >>> def choose_primary(manager: StoreAdministrationAPI) -> None:
        ...     manager.set_default_store("primary")
    """

    @abc.abstractmethod
    def create_store(
        self, spec: StoreSpec, *, persist: bool = True, startup: bool = True,
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
            >>> updated = manager.update_store("primary", spec)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def remove_store(self, store_ref: StoreRef, *, delete_from_db: bool = False) -> bool:
        """Stop and unregister a store, optionally deleting its configuration.

        Example:
            >>> removed = manager.remove_store(  # doctest: +SKIP
            ...     "retired", delete_from_db=True,
            ... )
        """
        ...

    @abc.abstractmethod
    def get_store_spec(self, store_ref: StoreRef) -> StoreSpec:
        """Return durable configuration for one store.

        Example:
            >>> spec = manager.get_store_spec("primary")  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def iter_store_specs(self) -> Iterator[StoreSpec]:
        """Iterate over configured store specifications.

        Example:
            >>> specs = list(manager.iter_store_specs())  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def get_store(self, store_ref: StoreRef) -> StoreAPI:
        """Return the live configured-store facade for one store reference.

        Example:
            >>> store = manager.get_store("primary")  # doctest: +SKIP
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
    def get_store_status(self, store_ref: StoreRef, *, refresh: bool = False) -> StoreStatus:
        """Return cached or freshly probed status for one store.

        Example:
            >>> status = manager.get_store_status(  # doctest: +SKIP
            ...     "primary", refresh=True,
            ... )
        """
        ...

    def iter_store_statuses(self, *, refresh: bool = False) -> Iterator[StoreStatus]:
        """Yield status for every configured store.

        Example:
            >>> statuses = list(manager.iter_store_statuses())  # doctest: +SKIP
        """

        for spec in self.iter_store_specs():
            ref: StoreRef = spec.store_id if spec.store_id is not None else spec.store_name
            yield self.get_store_status(ref, refresh=refresh)

    @abc.abstractmethod
    def reload_stores(
        self, *, include_offline: bool = False, clear_existing: bool = True,
    ) -> StorageBootstrapReport:
        """Rebuild the runtime store registry from durable configuration.

        Example:
            >>> report = manager.reload_stores(  # doctest: +SKIP
            ...     include_offline=True, clear_existing=True,
            ... )
        """
        ...

    @abc.abstractmethod
    def set_default_store(self, store_ref: StoreRef) -> None:
        """Choose the store used when higher-level placement has no preference.

        Example:
            >>> manager.set_default_store("primary")  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def get_default_store_ref(self) -> StoreRef:
        """Return the configured default store reference.

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
