"""
Specify optional native copy, move, and digest operations within one driver.

These runtime-checkable protocols describe required accelerator semantics;
implementations and matching capability flags provide actual support. Protocol
membership alone does not verify complete publication, source-version protection,
or digest correctness. Cross-driver orchestration belongs to transfer helpers.

Example:
    >>> info = driver.native_copy(source, destination)  # doctest: +SKIP
"""

from __future__ import annotations

from typing import Protocol, TypeVar, runtime_checkable

from LiuXin_alpha.storage.api.models import Digest, WriteMode
from LiuXin_alpha.storage.api.store_driver_api.models import (
    DriverObjectInfo,
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
    """
    Optional native copy between addresses in one driver instance.

    Example:
        >>> info = driver.native_copy(source, destination)  # doctest: +SKIP
    """

    def native_copy(
        self,
        source: DriverObjectAddressT,
        destination: DriverObjectAddressT,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> DriverObjectInfo[DriverObjectAddressT]:
        """
        Copy internally using explicit collision behaviour.

        The returned address must equal ``destination``. Success makes the complete destination
        readable. Failure must not expose a partial object that appears successfully published.

        Example:
            >>> info = driver.native_copy(source, destination)  # doctest: +SKIP


        :param source: Owned concrete address whose complete bytes are copied.
        :param destination: Owned canonical target address that the result must identify.
        :param mode: Explicit destination collision policy; CREATE_ONLY by default.
        :return: Destination DriverObjectInfo after complete publication; failures must not expose a successful-looking partial object.
        """
        ...


@runtime_checkable
class NativeMoveStorageDriverAPI(Protocol[DriverObjectAddressT]):
    """
    Optional native move between addresses in one driver instance.

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
    ) -> DriverObjectInfo[DriverObjectAddressT]:
        """
        Move internally using explicit collision and race protection.

        ``if_source_version`` protects the exact source previously observed by ``stat`` when
        supplied. Success returns metadata whose address equals ``destination``, makes that complete
        destination readable, and removes the intended source. Failure must leave at least one
        complete copy and must not expose a successful-looking partial destination.

        Example:
            >>> info = driver.native_move(source, destination)  # doctest: +SKIP


        :param source: Owned source address to remove only under the stated complete-copy guarantee.
        :param destination: Owned canonical publication target identified by the returned metadata.
        :param mode: Explicit destination collision policy; CREATE_ONLY by default.
        :param if_source_version: Optional opaque stat token protecting the exact source version being moved.
        :return: Destination metadata after complete publication and removal of the intended source; failure must preserve a complete copy.
        """
        ...


@runtime_checkable
class NativeDigestStorageDriverAPI(Protocol[_DriverObjectAddressContraT]):
    """
    Optional authoritative or server-side digest operation.

    Example:
        >>> digest = driver.native_compute_digest(address)  # doctest: +SKIP
    """

    def native_compute_digest(
        self,
        object_address: _DriverObjectAddressContraT,
        algorithm: str = "sha256",
    ) -> Digest:
        """
        Compute a digest without a generic client-side read.

        Example:
            >>> digest = driver.native_compute_digest(address, "sha256")  # doctest: +SKIP


        :param object_address: Owned address whose content is digested.
        :param algorithm: Requested digest algorithm name, defaulting to sha256.
        :return: Authoritative content Digest under the native implementation contract, without a generic client-side read.
        """
        ...


__all__ = [
    "NativeCopyStorageDriverAPI",
    "NativeDigestStorageDriverAPI",
    "NativeMoveStorageDriverAPI",
]
