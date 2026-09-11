"""
Define the mandatory readable driver core and reusable byte/digest conveniences.

Concrete stat/read methods own backend I/O, range and version enforcement, and
typed error translation. Shared helpers check address/result relationships, hide
only genuine not-found results where documented, and manage streams they open.
Native digest capability is corroborated by its protocol before delegation.

Example:
    >>> payload = driver.read_bytes(address, length=16)  # doctest: +SKIP
"""

from __future__ import annotations

import abc
import hashlib

from typing import BinaryIO, Generic, cast

from LiuXin_alpha.storage.api.errors import (
    StorageIntegrityError,
    StorageNotFound,
    StorageUnsupportedOperation,
)
from LiuXin_alpha.storage.api.models import Digest
from LiuXin_alpha.storage.api.store_driver_api.accelerators_api import (
    NativeDigestStorageDriverAPI,
)
from LiuXin_alpha.storage.api.store_driver_api.models import (
    DriverCapabilities,
    DriverObjectInfo,
    DriverObjectAddressT,
)
from LiuXin_alpha.storage.utils.constants import DEFAULT_STORAGE_CHUNK_SIZE


class ReadableStorageDriverAPI(Generic[DriverObjectAddressT], abc.ABC):
    """
    Small mandatory core for addressing and reading concrete objects.

    A read-only, non-enumerable source can implement this API honestly. Write, delete, listing,
    allocation, and hierarchical joining are independent protocols rather than abstract methods that
    every driver must pretend to support.

    Example:
        >>> header = driver.read_bytes(address, length=16)  # doctest: +SKIP
    """

    @abc.abstractmethod
    def check_object_address(
        self,
        object_address: DriverObjectAddressT,
    ) -> DriverObjectAddressT:
        """
        Validate an address before any backend I/O.

        Example:
            >>> checked = driver.check_object_address(address)  # doctest: +SKIP


        :param object_address: Typed object address expected to belong to this configured driver.
        :return: Validated address under the concrete ownership/type checker contract.
        """
        ...

    @abc.abstractmethod
    def require_canonical_object_address(
        self,
        object_address: DriverObjectAddressT,
    ) -> DriverObjectAddressT:
        """
        Validate ownership and canonical serialization.

        Example:
            >>> checked = driver.require_canonical_object_address(address)  # doctest: +SKIP


        :param object_address: Typed object address expected to belong to this configured driver.
        :return: Owned canonical address or a typed validation failure.
        """
        ...

    @property
    @abc.abstractmethod
    def capabilities(self) -> DriverCapabilities:
        """
        Describe mechanics this raw driver inherently supports.

        Example:
            >>> driver.capabilities.range_reads  # doctest: +SKIP
            True


        :return: DriverCapabilities describing supported mechanics, not current online/writable state.
        """
        ...

    @abc.abstractmethod
    def stat(
        self,
        object_address: DriverObjectAddressT,
    ) -> DriverObjectInfo[DriverObjectAddressT]:
        """
        Describe one object or raise ``StorageNotFound``.

        Connection, permission, and authentication errors must remain visible. The returned
        ``object_address`` must equal the checked requested address. A driver may populate
        ``digest`` only when ``capabilities.stat_digest_authoritative`` is true; such a digest is
        authoritative for the object version described by this result.

        Example:
            >>> info = driver.stat(address)  # doctest: +SKIP


        :param object_address: Typed object address expected to belong to this configured driver.
        :return: Version-specific DriverObjectInfo for the requested address; genuine absence raises StorageNotFound.
        """
        ...

    @abc.abstractmethod
    def open_read(
        self,
        object_address: DriverObjectAddressT,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Open a context-managed, binary, read-only object stream.

        The returned stream need not be seekable. Closing it must release all resources. Negative
        ranges are invalid. A non-default range must either be honoured exactly (returning at most
        ``length`` bytes) or raise ``StorageUnsupportedOperation``; it must never be silently
        ignored. ``if_version`` pins the stream to the opaque version returned by ``stat`` and
        requires ``capabilities.conditional_read``. A stale token raises
        ``StoragePreconditionFailed`` before any mismatched bytes are returned.

        Example:
            >>> with driver.open_read(address, offset=10, length=20) as source:  # doctest: +SKIP
            ...     payload = source.read()


        :param object_address: Typed object address expected to belong to this configured driver.
        :param offset: Nonnegative byte offset from the start of the object.
        :param length: Maximum bytes to return, or None to read through the end.
        :param if_version: Optional opaque stat version token requiring conditional-read support.
        :return: Caller-owned context-manageable binary stream; closing it releases backend resources.
        """
        ...

    def try_stat(
        self,
        object_address: DriverObjectAddressT,
    ) -> DriverObjectInfo[DriverObjectAddressT] | None:
        """
        Return ``None`` only for genuine absence.

        The initial check_object_address call is outside the not-found guard. This helper does not
        flatten permission, authentication, or integrity failures into absence.

        Example:
            >>> driver.try_stat(missing) is None  # doctest: +SKIP
            True


        :param object_address: Typed object address expected to belong to this configured driver.
        :return: Validated current metadata, or None only for StorageNotFound caught around stat/result validation.
        """
        checked = self.check_object_address(object_address)
        try:
            return self.require_object_info(checked, self.stat(checked))
        except StorageNotFound:
            return None

    def require_object_info(
        self,
        expected_address: DriverObjectAddressT,
        info: DriverObjectInfo[DriverObjectAddressT],
    ) -> DriverObjectInfo[DriverObjectAddressT]:
        """
        Require returned metadata to describe the requested object.

        Driver adapters and reusable callers should apply this to results from raw ``stat``, commit,
        and native operations before trusting them.

        Canonicalize-check both expected and reported addresses, then enforce the
        stat_digest_authoritative flag for any supplied digest. This does not read bytes, compare
        versions, or prove that reported size/digest values are accurate.

        Example:
            >>> info = driver.require_object_info(address, driver.stat(address))  # doctest: +SKIP


        :param expected_address: Requested address checked for canonical round-trip equality.
        :param info: Returned metadata whose address and digest-advertisement rule are checked.
        :return: Original info object after checks; a mismatched address or unadvertised digest raises StorageIntegrityError.
        """
        expected = self.require_canonical_object_address(expected_address)
        actual = self.require_canonical_object_address(info.object_address)
        if actual != expected:
            raise StorageIntegrityError(
                "driver returned metadata for another object address."
            )
        if (
            info.digest is not None
            and not self.capabilities.stat_digest_authoritative
        ):
            raise StorageIntegrityError(
                "driver returned a stat digest without advertising "
                + "stat_digest_authoritative."
            )
        return info

    def exists(self, object_address: DriverObjectAddressT) -> bool:
        """
        Test existence without concealing backend failures.

        Example:
            >>> driver.exists(address)  # doctest: +SKIP
            True


        :param object_address: Typed object address expected to belong to this configured driver.
        :return: Whether try_stat returns metadata; other errors propagate.
        """
        return self.try_stat(object_address) is not None

    def file_size(self, object_address: DriverObjectAddressT) -> int | None:
        """
        Return an authoritative byte size, or ``None`` when unknown.

        Example:
            >>> driver.file_size(address)  # doctest: +SKIP
            42


        :param object_address: Typed object address expected to belong to this configured driver.
        :return: Validated stat size in bytes, preserving None for unknown size; absence still raises.
        """
        checked = self.check_object_address(object_address)
        return self.require_object_info(checked, self.stat(checked)).size

    def get(
        self,
        object_address: DriverObjectAddressT,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Return ``open_read`` using familiar retrieval vocabulary.

        Check the address first and omit the if_version keyword when it is None, preserving
        compatibility with simple unversioned readers. Range/version validation is delegated to
        open_read.

        Example:
            >>> source = driver.get(address, length=20)  # doctest: +SKIP


        :param object_address: Typed object address expected to belong to this configured driver.
        :param offset: Nonnegative byte offset from the start of the object.
        :param length: Maximum bytes to return, or None to read through the end.
        :param if_version: Optional opaque stat version token requiring conditional-read support.
        :return: Open read stream owned by the caller; all backend errors propagate.
        """
        checked = self.check_object_address(object_address)
        if if_version is None:
            return self.open_read(checked, offset=offset, length=length)
        return self.open_read(
            checked, offset=offset, length=length, if_version=if_version
        )

    def read_bytes(
        self,
        object_address: DriverObjectAddressT,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> bytes:
        """
        Read one small object or range fully into memory.

        The helper materializes all returned bytes and imposes no independent memory ceiling or
        range-length check. It closes the stream even when reading fails; a close failure can mask
        that failure.

        Example:
            >>> driver.read_bytes(address, length=4)  # doctest: +SKIP
            b'book'


        :param object_address: Typed object address expected to belong to this configured driver.
        :param offset: Nonnegative byte offset from the start of the object.
        :param length: Maximum bytes to return, or None to read through the end.
        :param if_version: Optional opaque stat version token requiring conditional-read support.
        :return: Complete bytes read from the selected stream, after closing it; non-bytes results raise TypeError.
        """
        checked = self.check_object_address(object_address)
        reader = (
            self.open_read(checked, offset=offset, length=length)
            if if_version is None
            else self.open_read(
                checked,
                offset=offset,
                length=length,
                if_version=if_version,
            )
        )
        with reader as source:
            payload = source.read()
        if not isinstance(payload, bytes):
            raise TypeError("driver read stream must return bytes.")
        return payload

    def compute_digest(
        self,
        object_address: DriverObjectAddressT,
        algorithm: str = "sha256",
        *,
        chunk_size: int = DEFAULT_STORAGE_CHUNK_SIZE,
    ) -> Digest:
        """
        Use an authoritative native digest or a streaming fallback.

        The native path requires both its capability flag and structural protocol, then trusts the
        returned Digest without rechecking its algorithm/content. The fallback opens an unversioned
        stream, treating a falsey chunk as EOF before requiring nonempty chunks to be bytes. It does
        not pin a source version during hashing.

        Example:
            >>> digest = driver.compute_digest(address, "sha256")  # doctest: +SKIP


        :param object_address: Typed object address expected to belong to this configured driver.
        :param algorithm: Algorithm selector passed to native_compute_digest or hashlib.new.
        :param chunk_size: Positive read chunk size in bytes; validated even for native digest delegation.
        :return: Native result unchanged or a Digest built from streamed bytes; there is no fallback after a native failure.
        """
        object_address = self.check_object_address(object_address)
        if chunk_size < 1:
            raise ValueError("chunk_size must be at least one byte.")
        if self.capabilities.native_digest:
            if not isinstance(self, NativeDigestStorageDriverAPI):
                raise StorageUnsupportedOperation(
                    "driver advertises native_digest but does not "
                    + "implement native_compute_digest()."
                )
            native_driver = cast(
                NativeDigestStorageDriverAPI[DriverObjectAddressT], self
            )
            return native_driver.native_compute_digest(
                object_address, algorithm
            )
        try:
            digest = hashlib.new(algorithm)
        except ValueError as exc:
            raise StorageUnsupportedOperation(
                f"digest algorithm is not supported: {algorithm!r}"
            ) from exc
        with self.open_read(object_address) as source:
            while True:
                chunk = source.read(chunk_size)
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    raise TypeError("driver read stream must return bytes.")
                digest.update(chunk)
        return Digest(algorithm=algorithm, value=digest.hexdigest())


__all__ = ["ReadableStorageDriverAPI"]
