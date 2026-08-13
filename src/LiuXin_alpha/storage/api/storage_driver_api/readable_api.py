"""Minimal readable core and safe read-only conveniences for drivers."""

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
from LiuXin_alpha.storage.api.storage_driver_api.accelerators_api import (
    NativeDigestStorageDriverAPI,
)
from LiuXin_alpha.storage.api.storage_driver_api.models import (
    DriverCapabilities,
    DriverFileInfo,
    DriverObjectAddressT,
)
from LiuXin_alpha.storage.utils.constants import DEFAULT_STORAGE_CHUNK_SIZE


class ReadableStorageDriverAPI(Generic[DriverObjectAddressT], abc.ABC):
    """
    Small mandatory core for addressing and reading concrete objects.

    A read-only, non-enumerable source can implement this API honestly. Write,
    delete, listing, allocation, and hierarchical joining are independent
    protocols rather than abstract methods that every driver must pretend to
    support.

    Example:
        >>> header = driver.read_bytes(address, length=16)  # doctest: +SKIP
    """

    @abc.abstractmethod
    def check_object_address(
        self,
        object_address: DriverObjectAddressT,
    ) -> DriverObjectAddressT:
        """Validate an address before any backend I/O.

        Example:
            >>> checked = driver.check_object_address(address)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def require_canonical_object_address(
        self,
        object_address: DriverObjectAddressT,
    ) -> DriverObjectAddressT:
        """Validate ownership and canonical serialization.

        Example:
            >>> checked = driver.require_canonical_object_address(address)  # doctest: +SKIP
        """
        ...

    @property
    @abc.abstractmethod
    def capabilities(self) -> DriverCapabilities:
        """Describe mechanics this raw driver inherently supports.

        Example:
            >>> driver.capabilities.range_reads  # doctest: +SKIP
            True
        """
        ...

    @abc.abstractmethod
    def stat(
        self,
        object_address: DriverObjectAddressT,
    ) -> DriverFileInfo[DriverObjectAddressT]:
        """Describe one object or raise ``StorageNotFound``.

        Connection, permission, and authentication errors must remain visible.
        The returned ``object_address`` must equal the checked requested
        address. A driver may populate ``digest`` only when
        ``capabilities.stat_digest_authoritative`` is true; such a digest is
        authoritative for the object version described by this result.

        Example:
            >>> info = driver.stat(address)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def open_read(
        self,
        object_address: DriverObjectAddressT,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO:
        """
        Open a context-managed, binary, read-only object stream.

        The returned stream need not be seekable. Closing it must release all
        resources. Negative ranges are invalid. A non-default range must either
        be honoured exactly (returning at most ``length`` bytes) or raise
        ``StorageUnsupportedOperation``; it must never be silently ignored.

        Example:
            >>> with driver.open_read(address, offset=10, length=20) as source:  # doctest: +SKIP
            ...     payload = source.read()
        """
        ...

    def try_stat(
        self,
        object_address: DriverObjectAddressT,
    ) -> DriverFileInfo[DriverObjectAddressT] | None:
        """
        Return ``None`` only for genuine absence.

        Example:
            >>> driver.try_stat(missing) is None  # doctest: +SKIP
            True

        :param object_address:
        :return:
        """
        checked = self.check_object_address(object_address)
        try:
            return self.require_file_info(checked, self.stat(checked))
        except StorageNotFound:
            return None

    def require_file_info(
        self,
        expected_address: DriverObjectAddressT,
        info: DriverFileInfo[DriverObjectAddressT],
    ) -> DriverFileInfo[DriverObjectAddressT]:
        """Require returned metadata to describe the requested object.

        Driver adapters and reusable callers should apply this to results from
        raw ``stat``, commit, and native operations before trusting them.

        Example:
            >>> info = driver.require_file_info(address, driver.stat(address))  # doctest: +SKIP
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

        :param object_address:
        :return:
        """
        return self.try_stat(object_address) is not None

    def file_size(self, object_address: DriverObjectAddressT) -> int | None:
        """Return an authoritative byte size, or ``None`` when unknown.

        Example:
            >>> driver.file_size(address)  # doctest: +SKIP
            42
        """
        checked = self.check_object_address(object_address)
        return self.require_file_info(checked, self.stat(checked)).size

    def get(
        self,
        object_address: DriverObjectAddressT,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO:
        """Return ``open_read`` using familiar retrieval vocabulary.

        Example:
            >>> source = driver.get(address, length=20)  # doctest: +SKIP
        """
        return self.open_read(
            self.check_object_address(object_address),
            offset=offset,
            length=length,
        )

    def read_bytes(
        self,
        object_address: DriverObjectAddressT,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> bytes:
        """Read one small object or range fully into memory.

        Example:
            >>> driver.read_bytes(address, length=4)  # doctest: +SKIP
            b'book'
        """
        with self.open_read(
            self.check_object_address(object_address),
            offset=offset,
            length=length,
        ) as source:
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
        """Use an authoritative native digest or a streaming fallback.

        Example:
            >>> digest = driver.compute_digest(address, "sha256")  # doctest: +SKIP
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
