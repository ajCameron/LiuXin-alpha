"""Configured-store API above backend-specific driver mechanics.

``StoreAPI`` represents exactly one configured destination or source.  It owns
identity, lifecycle, transactional byte access, and store-local convenience
operations.  Cross-store routing and storage policy stay in
``StorageManagerAPI``; backend mechanics sit below this package in
``StoreDriverAPI``.
"""

from __future__ import annotations

import abc

from types import TracebackType

from LiuXin_alpha.storage.api2.models import Location
from LiuXin_alpha.storage.api2.store_api.file_api import (
    DigestingStore,
    FileStore,
    NativeCopyStore,
    NativeMoveStore,
    StoreFileAPI,
    WriteSession,
)
from LiuXin_alpha.storage.api2.store_api.identity_api import StoreIdentityAPI, StoreSpecAPI
from LiuXin_alpha.storage.api2.store_api.lifecycle_api import StoreLifecycleAPI


class StoreAPI(
    StoreIdentityAPI,
    StoreLifecycleAPI,
    StoreFileAPI,
    abc.ABC,
):
    """Complete facade for one configured store.

    Concrete stores enforce that every ``Location`` belongs to ``store_ref``
    and implement the small transactional primitives by delegating physical
    operations to a backend-specific ``StoreDriverAPI`` without exposing that
    driver to the manager.

    Example:
        >>> def read_object(store: StoreAPI, key: str) -> bytes:
        ...     location = store.require_location(Location(store.store_ref, key))
        ...     return store.read_bytes(location)
    """

    def __enter__(self) -> StoreAPI:
        """Enter the configured-store lifetime and return this store.

        Example:
            >>> entered = store.__enter__()  # doctest: +SKIP
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the configured store when leaving its context.

        Example:
            >>> store.__exit__(None, None, None)  # doctest: +SKIP
        """
        self.close()


from LiuXin_alpha.storage.api2.store_api.driver_backed_api import DriverBackedStoreAPI


__all__ = [
    "DigestingStore",
    "DriverBackedStoreAPI",
    "FileStore",
    "NativeCopyStore",
    "NativeMoveStore",
    "StoreAPI",
    "StoreFileAPI",
    "StoreIdentityAPI",
    "StoreLifecycleAPI",
    "StoreSpecAPI",
    "WriteSession",
]
