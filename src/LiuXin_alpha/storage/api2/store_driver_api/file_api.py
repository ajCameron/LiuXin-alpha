"""Transactional driver-local byte operations.

The mandatory surface deliberately excludes append and in-place update.
Replacement is an explicit staged write using ``WriteMode.REPLACE``.  This
retains the useful legacy read/write/stat/delete/inventory behavior without
requiring immutable, remote, or archive drivers to pretend they are mutable
filesystems.
"""

from __future__ import annotations

import abc
import hashlib
import io

from collections.abc import Iterator
from types import TracebackType
from typing import BinaryIO, Protocol, runtime_checkable

from LiuXin_alpha.storage.api2.errors import (
    StoreError,
    StoreNotFound,
    StoreUnsupportedOperation,
)
from LiuXin_alpha.storage.api2.models import Digest, StoreCapabilities, WriteMode
from LiuXin_alpha.storage.api2.store_driver_api.accelerators_api import (
    NativeCopyStoreDriverAPI,
    NativeDigestStoreDriverAPI,
    NativeMoveStoreDriverAPI,
)
from LiuXin_alpha.storage.api2.store_driver_api.models import DriverFileInfo, DriverKey


DEFAULT_DRIVER_CHUNK_SIZE = 1024 * 1024


@runtime_checkable
class DriverWriteSession(Protocol):
    """One staged driver write that publishes only on successful commit.

    The same atomicity, verification, and idempotent-abort guarantees as the
    configured-store ``WriteSession`` apply, but commit returns driver-local
    metadata rather than a routed ``FileInfo``.

    Example:
        >>> def publish(session: DriverWriteSession, data: bytes) -> DriverFileInfo:
        ...     with session:
        ...         session.write(data)
        ...         return session.commit()
    """

    def write(self, data: bytes) -> int:
        """Append bytes to private staged state and return the count accepted.

        Example:
            >>> accepted = session.write(b"payload")  # doctest: +SKIP
        """
        ...

    def commit(self) -> DriverFileInfo:
        """Verify expectations and publish one complete driver object.

        Example:
            >>> info = session.commit()  # doctest: +SKIP
        """
        ...

    def abort(self) -> None:
        """Discard staged state; repeated calls must be safe.

        Example:
            >>> session.abort()  # doctest: +SKIP
            >>> session.abort()  # doctest: +SKIP
        """
        ...

    def __enter__(self) -> DriverWriteSession:
        """Enter the staged-write lifetime and return this session.

        Example:
            >>> entered = session.__enter__()  # doctest: +SKIP
        """
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Abort unless the session has already committed.

        Example:
            >>> session.__exit__(None, None, None)  # doctest: +SKIP
        """
        ...


def _write_all(session: DriverWriteSession, data: bytes) -> None:
    """Write all bytes even when a driver accepts partial chunks.

    Example:
        >>> _write_all(session, b"complete payload")  # doctest: +SKIP
    """
    view = memoryview(data)
    written = 0
    while written < len(view):
        accepted = session.write(view[written:].tobytes())
        if accepted <= 0:
            raise StoreError("driver write session accepted no bytes and made no progress.")
        if accepted > len(view) - written:
            raise StoreError(
                "driver write session reported accepting more bytes than supplied."
            )
        written += accepted


class StoreDriverFileAPI(abc.ABC):
    """Mandatory driver primitives plus safe driver-local conveniences.

    Example:
        >>> def read_header(driver: StoreDriverFileAPI, key: DriverKey) -> bytes:
        ...     return driver.read_bytes(key, length=16)
    """

    @property
    @abc.abstractmethod
    def capabilities(self) -> StoreCapabilities:
        """Describe operations the backend driver inherently supports.

        Example:
            >>> atomic = driver.capabilities.atomic_publish  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def stat(self, key: DriverKey) -> DriverFileInfo:
        """Describe one concrete object or raise ``StoreNotFound``.

        Example:
            >>> info = driver.stat(DriverKey("objects/42"))  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def open_read(
        self,
        key: DriverKey,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO:
        """Open a binary read-only stream, optionally restricted to a range.

        Example:
            >>> source = driver.open_read(  # doctest: +SKIP
            ...     DriverKey("objects/42"), offset=10, length=20,
            ... )
        """
        ...

    @abc.abstractmethod
    def begin_write(
        self,
        key: DriverKey,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        metadata: tuple[tuple[str, str], ...] = (),
    ) -> DriverWriteSession:
        """Begin a private staged write at an explicit driver key.

        ``metadata`` is restricted to backend-native string pairs.  It must not
        contain bibliographic records, replica policy, or manager state.

        Example:
            >>> session = driver.begin_write(  # doctest: +SKIP
            ...     DriverKey("objects/42"), expected_size=4,
            ...     metadata=(("content-type", "application/epub+zip"),),
            ... )
        """
        ...

    @abc.abstractmethod
    def delete(
        self,
        key: DriverKey,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """Delete one object with optional idempotence and race protection.

        Example:
            >>> driver.delete(key, if_version="v3")  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def iter_keys(self, *, prefix: DriverKey | None = None) -> Iterator[DriverKey]:
        """Enumerate concrete object keys only.

        Enumeration completeness is declared by ``capabilities.enumeration``.

        Example:
            >>> keys = list(driver.iter_keys())  # doctest: +SKIP
        """
        ...

    def try_stat(self, key: DriverKey) -> DriverFileInfo | None:
        """Return ``None`` only when the driver reports genuine absence.

        Example:
            >>> driver.try_stat(DriverKey("missing")) is None  # doctest: +SKIP
            True
        """
        try:
            return self.stat(key)
        except StoreNotFound:
            return None

    def exists(self, key: DriverKey) -> bool:
        """Test existence without concealing backend failures.

        Example:
            >>> driver.exists(DriverKey("objects/42"))  # doctest: +SKIP
            True
        """
        return self.try_stat(key) is not None

    def file_size(self, key: DriverKey) -> int:
        """Return one object's authoritative byte size.

        Example:
            >>> size = driver.file_size(DriverKey("objects/42"))  # doctest: +SKIP
        """
        return self.stat(key).size

    def get(
        self,
        key: DriverKey,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO:
        """Familiar alias for ``open_read``.

        Example:
            >>> source = driver.get(key, length=20)  # doctest: +SKIP
        """
        return self.open_read(key, offset=offset, length=length)

    def read_bytes(
        self,
        key: DriverKey,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> bytes:
        """Read one object or range fully into memory.

        Example:
            >>> driver.read_bytes(key, length=4)  # doctest: +SKIP
            b'book'
        """
        with self.open_read(key, offset=offset, length=length) as source:
            return source.read()

    def put(
        self,
        key: DriverKey,
        source: BinaryIO,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        metadata: tuple[tuple[str, str], ...] = (),
        chunk_size: int = DEFAULT_DRIVER_CHUNK_SIZE,
    ) -> DriverFileInfo:
        """Stream through a staged driver write and commit after verification.

        Example:
            >>> import io
            >>> info = driver.put(  # doctest: +SKIP
            ...     key, io.BytesIO(b"book"), expected_size=4,
            ... )
        """
        if chunk_size < 1:
            raise ValueError("chunk_size must be at least one byte.")
        if expected_size is not None and expected_size < 0:
            raise ValueError("expected_size must not be negative.")

        session = self.begin_write(
            key,
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
            metadata=metadata,
        )
        with session:
            while True:
                chunk = source.read(chunk_size)
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    raise TypeError("source must be a binary stream returning bytes.")
                _write_all(session, chunk)
            return session.commit()

    def write_bytes(
        self,
        key: DriverKey,
        data: bytes,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_digest: Digest | None = None,
        metadata: tuple[tuple[str, str], ...] = (),
    ) -> DriverFileInfo:
        """Write a small in-memory payload with an exact size expectation.

        Example:
            >>> info = driver.write_bytes(key, b"book")  # doctest: +SKIP
        """
        return self.put(
            key,
            io.BytesIO(data),
            mode=mode,
            expected_size=len(data),
            expected_digest=expected_digest,
            metadata=metadata,
        )

    def iter_infos(self, *, prefix: DriverKey | None = None) -> Iterator[DriverFileInfo]:
        """Enumerate concrete keys and describe each object.

        Example:
            >>> infos = list(driver.iter_infos())  # doctest: +SKIP
        """
        for key in self.iter_keys(prefix=prefix):
            yield self.stat(key)

    def compute_digest(
        self,
        key: DriverKey,
        algorithm: str = "sha256",
        *,
        chunk_size: int = DEFAULT_DRIVER_CHUNK_SIZE,
    ) -> Digest:
        """Use an authoritative native digest or a streaming fallback.

        Example:
            >>> digest = driver.compute_digest(key, "sha256")  # doctest: +SKIP
        """
        if chunk_size < 1:
            raise ValueError("chunk_size must be at least one byte.")
        if self.capabilities.authoritative_digest:
            if not isinstance(self, NativeDigestStoreDriverAPI):
                raise StoreUnsupportedOperation(
                    "driver advertises authoritative_digest but does not implement "
                    "native_compute_digest()."
                )
            return self.native_compute_digest(key, algorithm)

        try:
            digest = hashlib.new(algorithm)
        except ValueError as exc:
            raise StoreUnsupportedOperation(
                f"digest algorithm is not supported: {algorithm!r}"
            ) from exc

        with self.open_read(key) as source:
            while True:
                chunk = source.read(chunk_size)
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    raise TypeError("driver read stream must return bytes.")
                digest.update(chunk)
        return Digest(algorithm=algorithm, value=digest.hexdigest())

    def copy(
        self,
        source: DriverKey,
        destination: DriverKey,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> DriverFileInfo:
        """Use native copy when advertised, otherwise stream and verify.

        Example:
            >>> info = driver.copy(source, destination)  # doctest: +SKIP
        """
        if self.capabilities.native_copy:
            if not isinstance(self, NativeCopyStoreDriverAPI):
                raise StoreUnsupportedOperation(
                    "driver advertises native_copy but does not implement native_copy()."
                )
            return self.native_copy(source, destination, mode=mode)

        source_info = self.stat(source)
        with self.open_read(source) as source_stream:
            return self.put(
                destination,
                source_stream,
                mode=mode,
                expected_size=source_info.size,
                expected_digest=source_info.digest,
                metadata=source_info.metadata,
            )

    def move(
        self,
        source: DriverKey,
        destination: DriverKey,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> DriverFileInfo:
        """Use native move, or verified copy and conditional deletion.

        Example:
            >>> info = driver.move(source, destination)  # doctest: +SKIP
        """
        if self.capabilities.native_move:
            if not isinstance(self, NativeMoveStoreDriverAPI):
                raise StoreUnsupportedOperation(
                    "driver advertises native_move but does not implement native_move()."
                )
            return self.native_move(source, destination, mode=mode)

        source_info = self.stat(source)
        result = self.copy(source, destination, mode=mode)
        self.delete(source, if_version=source_info.version)
        return result


__all__ = [
    "DEFAULT_DRIVER_CHUNK_SIZE",
    "DriverWriteSession",
    "StoreDriverFileAPI",
]
