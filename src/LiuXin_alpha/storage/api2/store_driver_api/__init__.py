"""Lowest-level storage-driver contracts.

``StoreDriverAPI`` implements mechanics for one backend technology and endpoint.
It receives opaque driver keys rather than routed ``Location`` objects and has
no knowledge of store ids, database rows, assets, replicas, or policy.

Useful legacy behavior carried forward:

* ``root_path`` / ``location`` / ``locate`` become explicit URI and key APIs;
* ``self_test`` becomes ``probe``;
* existence, file size, whole-byte reads/writes, and inventory are conveniences
  over the transactional primitives;
* in-backend copy and digest operations are capability-gated accelerators; and
* legacy implicit placement becomes explicit ``allocate_key``.

Database hooks, bibliographic placement metadata, path-like mutation, append,
and unchecked in-place update are intentionally not driver responsibilities.
Replacement uses a staged write with ``WriteMode.REPLACE``.
"""

from __future__ import annotations

import abc

from types import TracebackType

from LiuXin_alpha.storage.api2.store_driver_api.accelerators_api import (
    NativeCopyStoreDriverAPI,
    NativeDigestStoreDriverAPI,
    NativeMoveStoreDriverAPI,
)
from LiuXin_alpha.storage.api2.store_driver_api.file_api import (
    DEFAULT_DRIVER_CHUNK_SIZE,
    DriverWriteSession,
    StoreDriverFileAPI,
)
from LiuXin_alpha.storage.api2.store_driver_api.key_api import StoreDriverKeyAPI
from LiuXin_alpha.storage.api2.store_driver_api.lifecycle_api import (
    StoreDriverLifecycleAPI,
)
from LiuXin_alpha.storage.api2.store_driver_api.models import (
    DriverFileInfo,
    DriverKey,
    DriverKeyInput,
)


class StoreDriverAPI(
    StoreDriverKeyAPI,
    StoreDriverLifecycleAPI,
    StoreDriverFileAPI,
    abc.ABC,
):
    """Complete facade for the backend mechanics of one configured store.

    A configured ``StoreAPI`` privately owns a driver instance, translates
    ``Location`` to ``DriverKey``, and translates ``DriverFileInfo`` back to
    routed ``FileInfo``.  Managers should never call drivers directly.

    Example:
        >>> def read_object(driver: StoreDriverAPI, identifier: str) -> bytes:
        ...     return driver.read_bytes(driver.resolve_key(identifier))
    """

    def __enter__(self) -> StoreDriverAPI:
        """Enter the driver lifetime and return this driver.

        Example:
            >>> entered = driver.__enter__()  # doctest: +SKIP
        """
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
    "DEFAULT_DRIVER_CHUNK_SIZE",
    "DriverFileInfo",
    "DriverKey",
    "DriverKeyInput",
    "DriverWriteSession",
    "NativeCopyStoreDriverAPI",
    "NativeDigestStoreDriverAPI",
    "NativeMoveStoreDriverAPI",
    "StoreDriverAPI",
    "StoreDriverFileAPI",
    "StoreDriverKeyAPI",
    "StoreDriverLifecycleAPI",
]
