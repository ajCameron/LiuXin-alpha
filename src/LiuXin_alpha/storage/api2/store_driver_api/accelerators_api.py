"""Optional capability-gated store-driver accelerators."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from LiuXin_alpha.storage.api2.models import Digest, WriteMode
from LiuXin_alpha.storage.api2.store_driver_api.models import DriverFileInfo, DriverKey


@runtime_checkable
class NativeCopyStoreDriverAPI(Protocol):
    """Optional backend-local copy operation.

    Callers must also check ``capabilities.native_copy``; structural presence
    alone does not advertise that an implementation is usable.

    Example:
        >>> info = driver.native_copy(source, destination)  # doctest: +SKIP
    """

    def native_copy(
        self,
        source: DriverKey,
        destination: DriverKey,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> DriverFileInfo:
        """Copy without streaming bytes through the configured store.

        Example:
            >>> info = driver.native_copy(source, destination)  # doctest: +SKIP
        """
        ...


@runtime_checkable
class NativeMoveStoreDriverAPI(Protocol):
    """Optional backend-local move operation.

    Example:
        >>> info = driver.native_move(source, destination)  # doctest: +SKIP
    """

    def native_move(
        self,
        source: DriverKey,
        destination: DriverKey,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> DriverFileInfo:
        """Move within the backend using explicit collision behavior.

        Example:
            >>> info = driver.native_move(source, destination)  # doctest: +SKIP
        """
        ...


@runtime_checkable
class NativeDigestStoreDriverAPI(Protocol):
    """Optional authoritative or server-side digest operation.

    Example:
        >>> digest = driver.native_compute_digest(key, "sha256")  # doctest: +SKIP
    """

    def native_compute_digest(
        self,
        key: DriverKey,
        algorithm: str = "sha256",
    ) -> Digest:
        """Compute a digest without a generic client-side read.

        Example:
            >>> digest = driver.native_compute_digest(key)  # doctest: +SKIP
        """
        ...


__all__ = [
    "NativeCopyStoreDriverAPI",
    "NativeDigestStoreDriverAPI",
    "NativeMoveStoreDriverAPI",
]
