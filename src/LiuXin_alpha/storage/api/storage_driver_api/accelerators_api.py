"""Optional capability-gated accelerators for reusable storage drivers."""

from __future__ import annotations

from typing import Protocol, TypeVar, runtime_checkable

from LiuXin_alpha.storage.api.models import Digest, WriteMode
from LiuXin_alpha.storage.api.storage_driver_api.models import (
    DriverFileInfo,
    DriverObjectAddress,
    DriverObjectAddressT,
)


_DriverObjectAddressContraT = TypeVar(
    "_DriverObjectAddressContraT",
    bound=DriverObjectAddress,
    contravariant=True,
)


@runtime_checkable
class NativeCopyStorageDriverAPI(Protocol[DriverObjectAddressT]):
    """Optional native copy between addresses in one driver instance.

    Example:
        >>> info = driver.native_copy(source, destination)  # doctest: +SKIP
    """

    def native_copy(
        self,
        source: DriverObjectAddressT,
        destination: DriverObjectAddressT,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> DriverFileInfo[DriverObjectAddressT]:
        """Copy internally using explicit collision behaviour.

        The returned address must equal ``destination``. Success makes the
        complete destination readable. Failure must not expose a partial object
        that appears successfully published.

        Example:
            >>> info = driver.native_copy(source, destination)  # doctest: +SKIP
        """
        ...


@runtime_checkable
class NativeMoveStorageDriverAPI(Protocol[DriverObjectAddressT]):
    """Optional native move between addresses in one driver instance.

    Example:
        >>> info = driver.native_move(source, destination)  # doctest: +SKIP
    """

    def native_move(
        self,
        source: DriverObjectAddressT,
        destination: DriverObjectAddressT,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        if_source_version: str | None = None,
    ) -> DriverFileInfo[DriverObjectAddressT]:
        """Move internally using explicit collision and race protection.

        ``if_source_version`` protects the exact source previously observed by
        ``stat`` when supplied. Success returns metadata whose address equals
        ``destination``, makes that complete destination readable, and removes
        the intended source. Failure must leave at least one complete copy and
        must not expose a successful-looking partial destination.

        Example:
            >>> info = driver.native_move(source, destination)  # doctest: +SKIP
        """
        ...


@runtime_checkable
class NativeDigestStorageDriverAPI(Protocol[_DriverObjectAddressContraT]):
    """Optional authoritative or server-side digest operation.

    Example:
        >>> digest = driver.native_compute_digest(address)  # doctest: +SKIP
    """

    def native_compute_digest(
        self,
        object_address: _DriverObjectAddressContraT,
        algorithm: str = "sha256",
    ) -> Digest:
        """Compute a digest without a generic client-side read.

        Example:
            >>> digest = driver.native_compute_digest(address, "sha256")  # doctest: +SKIP
        """
        ...


__all__ = [
    "NativeCopyStorageDriverAPI",
    "NativeDigestStorageDriverAPI",
    "NativeMoveStorageDriverAPI",
]
