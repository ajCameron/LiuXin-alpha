"""
Reusable lowest-level storage-driver contracts.

``StorageDriverAPI`` is the small readable/lifecycle/addressing core. Listing,
staged writes, deletion, allocation, hierarchy, and native accelerators are
separate structural protocols, so an HTTP import source or immutable archive
can expose only what it genuinely supports. Drivers know bytes and their own
address space, not Stores, assets, replicas, import policy, or bibliographic
records.
"""

from __future__ import annotations

import abc

from types import TracebackType
from typing import Generic

from LiuXin_alpha.storage.api.storage_driver_api.accelerators_api import (
    NativeCopyStorageDriverAPI,
    NativeDigestStorageDriverAPI,
    NativeMoveStorageDriverAPI,
)
from LiuXin_alpha.storage.api.storage_driver_api.lifecycle_api import (
    StorageDriverLifecycleAPI,
)
from LiuXin_alpha.storage.api.storage_driver_api.models import (
    DriverCapabilities,
    DriverConcurrency,
    DriverFileInfo,
    DriverObjectAddress,
    DriverObjectAddressChecker,
    DriverObjectAddressInput,
    DriverObjectAddressT,
    DriverObjectEntry,
    DriverObjectHints,
    DriverStatus,
    ScopedDriverObjectAddressChecker,
)
from LiuXin_alpha.storage.api.storage_driver_api.object_address_api import (
    StorageDriverObjectAddressAPI,
)
from LiuXin_alpha.storage.api.storage_driver_api.optional_api import (
    DeletableStorageDriverAPI,
    DriverWriteSession,
    EnumerableStorageDriverAPI,
    HierarchicalStorageDriverAPI,
    ObjectAddressAllocatorStorageDriverAPI,
    WritableStorageDriverAPI,
)
from LiuXin_alpha.storage.api.storage_driver_api.readable_api import (
    ReadableStorageDriverAPI,
)


class StorageDriverAPI(
    StorageDriverObjectAddressAPI[DriverObjectAddressT],
    StorageDriverLifecycleAPI,
    ReadableStorageDriverAPI[DriverObjectAddressT],
    Generic[DriverObjectAddressT],
    abc.ABC,
):
    """
    Complete reusable core for one configured storage endpoint.

    The generic address subtype prevents crossing backend technologies at type
    check time; the injected checker prevents crossing configured instances at
    runtime. Optional protocols are detected separately and corroborated by
    ``DriverCapabilities``.

    Example:
        >>> address = driver.parse_object_address("incoming/book.epub")  # doctest: +SKIP
        >>> payload = driver.read_bytes(address)  # doctest: +SKIP
    """

    def __enter__(self) -> StorageDriverAPI[DriverObjectAddressT]:
        """
        Idempotently start the driver and return it for the managed lifetime.

        Example:
            >>> entered = driver.__enter__()  # doctest: +SKIP
        """
        _ = self.startup()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the driver when leaving its context.

        Example:
            >>> driver.__exit__(None, None, None)  # doctest: +SKIP
        """
        self.close()


__all__ = [
    "DeletableStorageDriverAPI",
    "DriverCapabilities",
    "DriverConcurrency",
    "DriverFileInfo",
    "DriverObjectAddress",
    "DriverObjectAddressChecker",
    "DriverObjectAddressInput",
    "DriverObjectAddressT",
    "DriverObjectEntry",
    "DriverObjectHints",
    "DriverStatus",
    "DriverWriteSession",
    "EnumerableStorageDriverAPI",
    "HierarchicalStorageDriverAPI",
    "NativeCopyStorageDriverAPI",
    "NativeDigestStorageDriverAPI",
    "NativeMoveStorageDriverAPI",
    "ObjectAddressAllocatorStorageDriverAPI",
    "ReadableStorageDriverAPI",
    "ScopedDriverObjectAddressChecker",
    "StorageDriverAPI",
    "StorageDriverLifecycleAPI",
    "StorageDriverObjectAddressAPI",
    "WritableStorageDriverAPI",
]
