"""
Compose the raw readable driver core and export independent optional capabilities.

StorageDriverAPI combines address parsing/checking, lifecycle, read conveniences,
and generic file helpers for one endpoint. Writable/listing/native operations are
separate protocols corroborated by capability declarations. Drivers operate on
their own address spaces; Store identity, Asset/Replica policy, and catalogue
orchestration belong above this boundary.

Example:
    >>> payload = driver.read_file("incoming/book.epub")  # doctest: +SKIP
"""

from __future__ import annotations

import abc

from types import TracebackType
from typing import Generic

from LiuXin_alpha.storage.api.store_driver_api.convenience_api import (
    DriverFileIdentifier,
    DriverNativeMetadata,
    StorageDriverConvenienceAPI,
    StorageDriverSource,
)
from LiuXin_alpha.storage.api.store_driver_api.accelerators_api import (
    NativeCopyStorageDriverAPI,
    NativeDigestStorageDriverAPI,
    NativeMoveStorageDriverAPI,
)
from LiuXin_alpha.storage.api.store_driver_api.lifecycle_api import (
    StorageDriverLifecycleAPI,
)
from LiuXin_alpha.storage.api.store_driver_api.models import (
    DriverCapabilities,
    DriverConcurrencyCapabilities,
    DriverObjectInfo,
    DriverObjectAddress,
    DriverObjectAddressCheckerAPI,
    DriverObjectAddressInput,
    DriverObjectAddressT,
    DriverInventoryEntry,
    DriverInventoryPage,
    DriverObjectHints,
    DriverStatus,
    ScopedDriverObjectAddressChecker,
)
from LiuXin_alpha.storage.api.store_driver_api.object_address_api import (
    StorageDriverObjectAddressAPI,
)
from LiuXin_alpha.storage.api.store_driver_api.optional_api import (
    DeletableStorageDriverAPI,
    DriverWriteSessionAPI,
    EnumerableStorageDriverAPI,
    PagedEnumerableStorageDriverAPI,
    HierarchicalStorageDriverAPI,
    ObjectAddressAllocatorStorageDriverAPI,
    StorageDriverCharacteristicsAPI,
    WritableStorageDriverAPI,
)
from LiuXin_alpha.storage.api.store_driver_api.readable_api import (
    ReadableStorageDriverAPI,
)


class StorageDriverAPI(
    StorageDriverConvenienceAPI[DriverObjectAddressT],
    StorageDriverObjectAddressAPI[DriverObjectAddressT],
    StorageDriverLifecycleAPI,
    ReadableStorageDriverAPI[DriverObjectAddressT],
    Generic[DriverObjectAddressT],
    abc.ABC,
):
    """
    Complete reusable core for one configured storage endpoint.

    The generic address subtype prevents crossing backend technologies at type check time; the
    injected checker prevents crossing configured instances at runtime. Optional protocols are
    detected separately and corroborated by ``DriverCapabilities``. Backend-native failures are
    translated to the shared ``StorageError`` hierarchy with a credential-safe message naming the
    backend, operation, target, and reason; exception chaining retains the diagnostic cause.

    The inherited mixins do not automatically translate arbitrary backend exceptions or redact
    messages. Concrete implementations are responsible for meeting that failure contract.

    Example:
        >>> payload = driver.read_file("incoming/book.epub")  # doctest: +SKIP
    """

    def __enter__(self) -> StorageDriverAPI[DriverObjectAddressT]:
        """
        Idempotently start the driver and return it for the managed lifetime.

        Discard the returned status and do not separately require available=True. Idempotent
        resource behavior is the concrete startup contract.

        Example:
            >>> entered = driver.__enter__()  # doctest: +SKIP


        :return: This driver after startup returns; startup errors propagate.
        """
        _ = self.startup()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Close the driver when leaving its context.

        A close failure propagates and can replace an exception already leaving the body.

        Example:
            >>> driver.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type: Exception class from the context body, or None; ignored by this implementation.
        :param exc: Body exception instance, or None; ignored.
        :param traceback: Body traceback, or None; ignored.
        :return: None after close, so body exceptions are not suppressed.
        """
        self.close()


__all__ = [
    "DeletableStorageDriverAPI",
    "DriverCapabilities",
    "DriverConcurrencyCapabilities",
    "DriverFileIdentifier",
    "DriverNativeMetadata",
    "DriverObjectInfo",
    "DriverObjectAddress",
    "DriverObjectAddressCheckerAPI",
    "DriverObjectAddressInput",
    "DriverObjectAddressT",
    "DriverInventoryEntry",
    "DriverInventoryPage",
    "DriverObjectHints",
    "DriverStatus",
    "DriverWriteSessionAPI",
    "EnumerableStorageDriverAPI",
    "PagedEnumerableStorageDriverAPI",
    "HierarchicalStorageDriverAPI",
    "NativeCopyStorageDriverAPI",
    "NativeDigestStorageDriverAPI",
    "NativeMoveStorageDriverAPI",
    "ObjectAddressAllocatorStorageDriverAPI",
    "ReadableStorageDriverAPI",
    "ScopedDriverObjectAddressChecker",
    "StorageDriverAPI",
    "StorageDriverCharacteristicsAPI",
    "StorageDriverConvenienceAPI",
    "StorageDriverSource",
    "StorageDriverLifecycleAPI",
    "StorageDriverObjectAddressAPI",
    "WritableStorageDriverAPI",
]
