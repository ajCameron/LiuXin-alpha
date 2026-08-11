"""Transactional configured-store file protocols.

The primitive contract is deliberately small.  It describes concrete byte
storage only; assets, replicas, placement policy, repair, reconciliation, and
database transactions belong to higher layers.
"""

from __future__ import annotations

import abc
import hashlib

from collections.abc import Iterator
from types import TracebackType
from typing import BinaryIO, Protocol, runtime_checkable

from LiuXin_alpha.storage.api2.errors import StoreUnsupportedOperation
from LiuXin_alpha.storage.api2.models import (
    Digest,
    FileInfo,
    Location,
    StoreCapabilities,
    StoreStatus,
    WriteMode,
)


@runtime_checkable
class WriteSession(Protocol):
    """One staged write whose final Location changes only at commit.

    Required guarantees:

    * the final Location stays absent or unchanged until ``commit()`` begins;
    * ``commit()`` checks expected size and digest before publication;
    * successful commit publishes one complete, readable object;
    * failed commit never leaves a successful-looking partial object;
    * leaving the context without committing aborts the session; and
    * ``abort()`` and its cleanup are idempotent.

    When ``atomic_publish`` is true, commit changes visibility in one atomic
    step.  A store that cannot guarantee that advertises ``atomic_publish`` as
    false; callers may then choose a safer destination or recovery policy.

    Example:
        >>> def publish(session: WriteSession, data: bytes) -> FileInfo:
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

    def commit(self) -> FileInfo:
        """Verify expectations and publish the completed object.

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

    def __enter__(self) -> WriteSession:
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
        """Abort unless this session has already committed.

        Example:
            >>> session.__exit__(None, None, None)  # doctest: +SKIP
        """
        ...


@runtime_checkable
class FileStore(Protocol):
    """Structural view of the mandatory operations on one configured store.

    Generic code should depend on this protocol rather than backend-specific
    driver, path, or connection types.  It intentionally says nothing about
    how the configured store delegates work to ``StoreDriverAPI``.

    Example:
        >>> def read_all(store: FileStore, location: Location) -> bytes:
        ...     with store.open_read(location) as source:
        ...         return source.read()
    """

    @property
    @abc.abstractmethod
    def capabilities(self) -> StoreCapabilities:
        """Describe operations the backend can inherently perform.

        Example:
            >>> supports_ranges = store.capabilities.range_reads  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def stat(self, location: Location) -> FileInfo:
        """Describe one object or raise ``StoreNotFound``.

        Connection, permission, and backend errors must remain visible rather
        than being converted into a false not-found result.

        Example:
            >>> info = store.stat(Location("primary", "objects/42"))  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def open_read(
        self,
        location: Location,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO:
        """Open a binary, read-only stream, optionally restricted to a range.

        Example:
            >>> source = store.open_read(  # doctest: +SKIP
            ...     Location("primary", "objects/42"), offset=10, length=20,
            ... )
        """
        ...

    @abc.abstractmethod
    def begin_write(
        self,
        location: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
    ) -> WriteSession:
        """Start a private staged write without changing the final Location.

        ``CREATE_ONLY`` is the safe default.  Immutable or content-addressed
        stores may treat publication of identical bytes at an existing key as
        idempotent success, but different bytes must raise an integrity or
        already-exists error rather than overwrite the object.

        Example:
            >>> session = store.begin_write(  # doctest: +SKIP
            ...     Location("primary", "objects/42"),
            ...     mode=WriteMode.CREATE_ONLY, expected_size=4,
            ... )
        """
        ...

    @abc.abstractmethod
    def delete(
        self,
        location: Location,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """Delete an object with optional idempotence and race protection.

        Example:
            >>> store.delete(  # doctest: +SKIP
            ...     Location("primary", "objects/42"), if_version="v3",
            ... )
        """
        ...

    @abc.abstractmethod
    def iter_locations(
        self,
        *,
        prefix: Location | None = None,
    ) -> Iterator[Location]:
        """Enumerate concrete files only; completeness is a capability.

        Example:
            >>> locations = list(store.iter_locations())  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def status(self, *, refresh: bool = False) -> StoreStatus:
        """Return the configured store's availability and capacity state.

        Example:
            >>> online = store.status(refresh=True).available  # doctest: +SKIP
        """
        ...


class StoreFileAPI(FileStore, abc.ABC):
    """Nominal configured-store byte API with safe convenience operations.

    Example:
        >>> def read_header(store: StoreFileAPI, location: Location) -> bytes:
        ...     return store.read_bytes(location, length=16)
    """

    def try_stat(self, location: Location) -> FileInfo | None:
        """Return ``None`` only when the store reports genuine absence.

        Example:
            >>> store.try_stat(Location("primary", "missing")) is None  # doctest: +SKIP
            True
        """
        from LiuXin_alpha.storage.api2.convenience import try_stat

        return try_stat(self, location)

    def exists(self, location: Location) -> bool:
        """Test existence without masking permission or availability errors.

        Example:
            >>> store.exists(Location("primary", "objects/42"))  # doctest: +SKIP
            True
        """
        return self.try_stat(location) is not None

    def file_size(self, location: Location) -> int:
        """Return one object's authoritative byte size.

        Example:
            >>> size = store.file_size(Location("primary", "objects/42"))  # doctest: +SKIP
        """
        return self.stat(location).size

    def get(
        self,
        location: Location,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO:
        """Familiar alias for ``open_read``.

        Example:
            >>> source = store.get(location, offset=10, length=20)  # doctest: +SKIP
        """
        return self.open_read(location, offset=offset, length=length)

    def read_bytes(
        self,
        location: Location,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> bytes:
        """Read one object or range fully into memory.

        Example:
            >>> store.read_bytes(location, length=4)  # doctest: +SKIP
            b'book'
        """
        with self.open_read(location, offset=offset, length=length) as source:
            return source.read()

    def put(
        self,
        location: Location,
        source: BinaryIO,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        chunk_size: int = 1024 * 1024,
    ) -> FileInfo:
        """Stream, verify, and transactionally publish one object.

        Example:
            >>> import io
            >>> info = store.put(  # doctest: +SKIP
            ...     location, io.BytesIO(b"book"), expected_size=4,
            ... )
        """
        from LiuXin_alpha.storage.api2.convenience import put

        return put(
            self,
            location,
            source,
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
            chunk_size=chunk_size,
        )

    def write_bytes(
        self,
        location: Location,
        data: bytes,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_digest: Digest | None = None,
    ) -> FileInfo:
        """Write a small in-memory payload with an exact size expectation.

        Example:
            >>> info = store.write_bytes(location, b"book")  # doctest: +SKIP
        """
        from LiuXin_alpha.storage.api2.convenience import write_bytes

        return write_bytes(
            self,
            location,
            data,
            mode=mode,
            expected_digest=expected_digest,
        )

    def iter_infos(self, *, prefix: Location | None = None) -> Iterator[FileInfo]:
        """Enumerate concrete locations and describe each object.

        Example:
            >>> infos = list(store.iter_infos())  # doctest: +SKIP
        """
        for location in self.iter_locations(prefix=prefix):
            yield self.stat(location)

    def compute_digest(
        self,
        location: Location,
        algorithm: str = "sha256",
        *,
        chunk_size: int = 1024 * 1024,
    ) -> Digest:
        """Compute an object digest by streaming through the configured store.

        A concrete store may override this method to expose an authoritative
        driver-side digest.

        Example:
            >>> digest = store.compute_digest(location, "sha256")  # doctest: +SKIP
        """
        if chunk_size < 1:
            raise ValueError("chunk_size must be at least one byte.")
        try:
            digest = hashlib.new(algorithm)
        except ValueError as exc:
            raise StoreUnsupportedOperation(
                f"digest algorithm is not supported: {algorithm!r}"
            ) from exc

        with self.open_read(location) as source:
            while True:
                chunk = source.read(chunk_size)
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    raise TypeError("store read stream must return bytes.")
                digest.update(chunk)
        return Digest(algorithm=algorithm, value=digest.hexdigest())

    def copy(
        self,
        source: Location,
        destination: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> FileInfo:
        """Copy by verified streaming unless a concrete store overrides it.

        Example:
            >>> info = store.copy(source, destination)  # doctest: +SKIP
        """
        source_info = self.stat(source)
        with self.open_read(source) as source_stream:
            return self.put(
                destination,
                source_stream,
                mode=mode,
                expected_size=source_info.size,
                expected_digest=source_info.digest,
            )

    def move(
        self,
        source: Location,
        destination: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> FileInfo:
        """Perform a verified copy followed by conditional source deletion.

        A concrete store may override this method with a safe native move.

        Example:
            >>> info = store.move(source, destination)  # doctest: +SKIP
        """
        source_info = self.stat(source)
        result = self.copy(source, destination, mode=mode)
        self.delete(source, if_version=source_info.version)
        return result


@runtime_checkable
class NativeCopyStore(FileStore, Protocol):
    """Optional backend-native copy acceleration.

    Example:
        >>> def clone(store: NativeCopyStore, source: Location, target: Location):
        ...     return store.copy(source, target, mode=WriteMode.CREATE_ONLY)
    """

    def copy(
        self,
        source: Location,
        destination: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> FileInfo:
        """Copy entirely within the backend without client-side streaming.

        Example:
            >>> info = store.copy(source, destination)  # doctest: +SKIP
        """
        ...


@runtime_checkable
class NativeMoveStore(FileStore, Protocol):
    """Optional backend-native move acceleration.

    Example:
        >>> def relocate(store: NativeMoveStore, source: Location, target: Location):
        ...     return store.move(source, target, mode=WriteMode.CREATE_ONLY)
    """

    def move(
        self,
        source: Location,
        destination: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> FileInfo:
        """Move entirely within the backend with explicit collision behavior.

        Example:
            >>> info = store.move(source, destination)  # doctest: +SKIP
        """
        ...


@runtime_checkable
class DigestingStore(FileStore, Protocol):
    """Optional authoritative or server-side digest acceleration.

    Example:
        >>> def sha256(store: DigestingStore, location: Location) -> Digest:
        ...     return store.compute_digest(location, "sha256")
    """

    def compute_digest(
        self,
        location: Location,
        algorithm: str = "sha256",
    ) -> Digest:
        """Compute a digest without requiring a generic client-side read.

        Example:
            >>> digest = store.compute_digest(location, "sha256")  # doctest: +SKIP
        """
        ...


__all__ = [
    "DigestingStore",
    "FileStore",
    "NativeCopyStore",
    "NativeMoveStore",
    "StoreFileAPI",
    "WriteSession",
]
