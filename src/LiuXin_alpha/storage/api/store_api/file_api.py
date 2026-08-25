"""
Transactional configured-store file protocols.

The primitive contract is deliberately small.  It describes concrete byte
storage only; assets, replicas, placement policy, repair, reconciliation, and
database transactions belong to higher layers.

Every public Store operation addresses objects with ``Location``. Driver-local
``DriverObjectAddress`` values are confined to ``StorageDriverAPI`` and private
translation inside a driver-backed Store.
"""

from __future__ import annotations

import abc
import hashlib
import io

from collections.abc import Iterator
from types import TracebackType
from typing import BinaryIO, Protocol, runtime_checkable
from LiuXin_alpha.storage.api.errors import (
    StoreError,
    StoreNotFound,
    StoreUnsupportedOperation,
)
from LiuXin_alpha.storage.api.models import (
    Digest,
    FileInfo,
    Location,
    StoreCapabilities,
    StoreInventoryEntry,
    StoreInventoryPage,
    StoreStatus,
    WriteMode,
)
from LiuXin_alpha.storage.api.placement_hints_api import StoragePlacementHints


@runtime_checkable
class WriteSessionAPI(Protocol):
    """
    One staged write whose final Location changes only at commit.

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
        >>> def publish(session: WriteSessionAPI, data: bytes) -> FileInfo:
        ...     with session:
        ...         session.write(data)
        ...         return session.commit()
    """

    def write(self, data: bytes) -> int:
        """
        Append bytes to private staged state and return the count accepted.

        Example:
            >>> accepted = session.write(b"payload")  # doctest: +SKIP


        :param data:
        :return:
        """
        ...

    def commit(self) -> FileInfo:
        """
        Verify expectations and publish the completed object.

        Example:
            >>> info = session.commit()  # doctest: +SKIP


        :return:
        """
        ...

    def abort(self) -> None:
        """
        Discard staged state; repeated calls must be safe.

        Example:
            >>> session.abort()  # doctest: +SKIP
            >>> session.abort()  # doctest: +SKIP


        :return:
        """
        ...

    def __enter__(self) -> WriteSessionAPI:
        """
        Enter the staged-write lifetime and return this session.

        Example:
            >>> entered = session.__enter__()  # doctest: +SKIP


        :return:
        """
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Abort unless this session has already committed.

        Example:
            >>> session.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type:
        :param exc:
        :param traceback:
        :return:
        """
        ...


@runtime_checkable
class StoreCoreAPI(Protocol):
    """
    Structural view of the mandatory operations on one configured store.

    Generic code should depend on this protocol rather than backend-specific
    driver, path, or connection types.  It intentionally says nothing about
    how the configured store delegates work to ``StorageDriverAPI``.

    Example:
        >>> def read_all(store: StoreCoreAPI, location: Location) -> bytes:
        ...     with store.open_read(location) as source:
        ...         return source.read()
    """

    @property
    @abc.abstractmethod
    def capabilities(  # pyright: ignore[reportInvalidAbstractMethod]
        self,
    ) -> "StoreCapabilities":
        """
        Describe operations the backend can inherently perform.

        Example:
            >>> supports_ranges = store.capabilities.range_reads  # doctest: +SKIP


        :return:
        """
        ...

    @abc.abstractmethod
    def stat(  # pyright: ignore[reportInvalidAbstractMethod]
        self,
        location: Location,
    ) -> FileInfo:
        """
        Get one object's stats or raise ``StoreNotFound``.

        Connection, permission, and backend errors must remain visible rather
        than being converted into a false not-found result.

        Example:
            >>> info = store.stat(Location(UUID(int=1), "objects/42"))  # doctest: +SKIP


        :param location:
        :return:
        """
        ...

    @abc.abstractmethod
    def open_read(  # pyright: ignore[reportInvalidAbstractMethod]
        self,
        location: Location,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Open a binary, read-only stream, optionally restricted to a range.

        ``if_version`` pins the stream to a version returned by ``stat`` when
        ``capabilities.conditional_read`` is true. A stale token raises
        ``StorePreconditionFailed`` before mismatched bytes are returned.

        Example:
            >>> source = store.open_read(  # doctest: +SKIP
            ...     Location(UUID(int=1), "objects/42"), offset=10, length=20,
            ... )


        :param location:
        :param offset:
        :param length:
        :param if_version:
        :return:
        """
        ...

    @abc.abstractmethod
    def begin_write(  # pyright: ignore[reportInvalidAbstractMethod]
        self,
        location: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        placement_hints: StoragePlacementHints | None = None,
    ) -> "WriteSessionAPI":
        """
        Start a private staged write without changing the final Location.

        ``CREATE_ONLY`` is the safe default.  Immutable or content-addressed
        stores may treat publication of identical bytes at an existing key as
        idempotent success, but different bytes must raise an integrity or
        already-exists error rather than overwrite the object.
        Placement hints are advisory and Store-facing. Implementations that
        support rich layouts or indexes may consume them during commit;
        implementations that do not may ignore them.

        Example:
            >>> session = store.begin_write(  # doctest: +SKIP
            ...     Location(UUID(int=1), "objects/42"),
            ...     mode=WriteMode.CREATE_ONLY, expected_size=4,
            ... )


        :param location:
        :param mode:
        :param expected_size:
        :param expected_digest:
        :param placement_hints:
        :return:
        """
        ...

    @abc.abstractmethod
    def delete(  # pyright: ignore[reportInvalidAbstractMethod]
        self,
        location: Location,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Delete an object with optional idempotence and race protection.

        ``missing_ok`` suppresses only genuine absence. Passing
        ``if_version`` requires ``capabilities.conditional_delete`` and
        deletes only the exact version previously returned by ``stat``.
        Unsupported conditional deletion raises ``StoreUnsupportedOperation``;
        a stale token raises ``StorePreconditionFailed``. Availability,
        permission, and other failures remain visible.

        Example:
            >>> store.delete(  # doctest: +SKIP
            ...     Location(UUID(int=1), "objects/42"), if_version="v3",
            ... )


        :param location:
        :param missing_ok:
        :param if_version:
        :return:
        """
        ...

    @abc.abstractmethod
    def iter_locations(  # pyright: ignore[reportInvalidAbstractMethod]
        self,
        *,
        prefix: Location | None = None,
    ) -> Iterator[Location]:
        """
        Enumerate concrete files only; completeness is a capability.

        A non-``None`` prefix additionally requires
        ``capabilities.prefix_enumeration``. Implementations raise
        ``StoreUnsupportedOperation`` rather than ignoring an unsupported
        filter.

        Example:
            >>> locations = list(store.iter_locations())  # doctest: +SKIP


        :param prefix:
        :return:
        """
        ...

    @abc.abstractmethod
    def status(  # pyright: ignore[reportInvalidAbstractMethod]
        self,
        *,
        refresh: bool = False,
    ) -> StoreStatus:
        """
        Return the configured store's availability and capacity state.

        Example:
            >>> online = store.status(refresh=True).available  # doctest: +SKIP


        :param refresh:
        :return:
        """
        ...


class StoreFileAPI(StoreCoreAPI, abc.ABC):
    """
    Nominal configured-store byte API with safe convenience operations.

    Example:
        >>> def read_header(store: StoreFileAPI, location: Location) -> bytes:
        ...     return store.read_bytes(location, length=16)
    """

    def try_stat(self, location: Location) -> FileInfo | None:
        """
        Return ``None`` only when the store reports genuine absence.

        Example:
            >>> store.try_stat(Location(UUID(int=1), "missing")) is None  # doctest: +SKIP
            True


        :param location:
        :return:
        """
        try:
            return self.stat(location)
        except StoreNotFound:
            return None

    def exists(self, location: Location) -> bool:
        """
        Test existence without masking permission or availability errors.

        Example:
            >>> store.exists(Location(UUID(int=1), "objects/42"))  # doctest: +SKIP
            True


        :param location:
        :return:
        """
        return self.try_stat(location) is not None

    def file_size(self, location: Location) -> int:
        """
        Return one object's authoritative byte size.

        Example:
            >>> size = store.file_size(Location(UUID(int=1), "objects/42"))  # doctest: +SKIP


        :param location:
        :return:
        """
        return self.stat(location).size

    def get(
        self,
        location: Location,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Familiar alias for ``open_read``.

        Example:
            >>> source = store.get(location, offset=10, length=20)  # doctest: +SKIP


        :param location:
        :param offset:
        :param length:
        :param if_version:
        :return:
        """
        if if_version is None:
            return self.open_read(location, offset=offset, length=length)
        return self.open_read(
            location, offset=offset, length=length, if_version=if_version
        )

    def read_bytes(
        self,
        location: Location,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> bytes:
        """
        Read one object or range fully into memory.

        Example:
            >>> store.read_bytes(location, length=4)  # doctest: +SKIP
            b'book'


        :param location:
        :param offset:
        :param length:
        :param if_version:
        :return:
        """
        reader = (
            self.open_read(location, offset=offset, length=length)
            if if_version is None
            else self.open_read(
                location,
                offset=offset,
                length=length,
                if_version=if_version,
            )
        )
        with reader as source:
            return source.read()

    def put(
        self,
        location: Location,
        source: BinaryIO,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        placement_hints: StoragePlacementHints | None = None,
        chunk_size: int = 1024 * 1024,
    ) -> FileInfo:
        """
        Stream, verify, and transactionally publish one object.

        Example:
            >>> import io
            >>> info = store.put(  # doctest: +SKIP
            ...     location, io.BytesIO(b"book"), expected_size=4,
            ... )


        :param location:
        :param source:
        :param mode:
        :param expected_size:
        :param expected_digest:
        :param placement_hints:
        :param chunk_size:
        :return:
        """
        if chunk_size < 1:
            raise ValueError("chunk_size must be at least one byte.")
        if expected_size is not None and expected_size < 0:
            raise ValueError("expected_size must not be negative.")

        session = (
            self.begin_write(
                location,
                mode=mode,
                expected_size=expected_size,
                expected_digest=expected_digest,
            )
            if placement_hints is None or not self.capabilities.placement_hints
            else self.begin_write(
                location,
                mode=mode,
                expected_size=expected_size,
                expected_digest=expected_digest,
                placement_hints=placement_hints,
            )
        )
        with session:
            while True:
                chunk = source.read(chunk_size)
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    raise TypeError("source must be a binary stream returning bytes.")
                view = memoryview(chunk)
                written = 0
                while written < len(view):
                    accepted = session.write(view[written:].tobytes())
                    if accepted <= 0:
                        raise StoreError(
                            "write session accepted no bytes and made no progress."
                        )
                    if accepted > len(view) - written:
                        raise StoreError(
                            "write session accepted more bytes than supplied."
                        )
                    written += accepted
            return session.commit()

    def write_bytes(
        self,
        location: Location,
        data: bytes,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_digest: Digest | None = None,
        placement_hints: StoragePlacementHints | None = None,
    ) -> FileInfo:
        """
        Write a small in-memory payload with an exact size expectation.

        Example:
            >>> info = store.write_bytes(location, b"book")  # doctest: +SKIP


        :param location:
        :param data:
        :param mode:
        :param expected_digest:
        :param placement_hints:
        :return:
        """
        return self.put(
            location,
            io.BytesIO(data),
            mode=mode,
            expected_size=len(data),
            expected_digest=expected_digest,
            placement_hints=placement_hints,
        )

    def iter_file_infos(
        self,
        *,
        prefix: Location | None = None,
    ) -> Iterator[FileInfo]:
        """
        Enumerate concrete locations and describe each object.

        Example:
            >>> infos = list(store.iter_file_infos())  # doctest: +SKIP


        :param prefix:
        :return:
        """
        for location in self.iter_locations(prefix=prefix):
            yield self.stat(location)

    def iter_inventory_entries(
        self,
        *,
        prefix: Location | None = None,
    ) -> Iterator[StoreInventoryEntry]:
        """
        Enumerate discovery entries, including objects with unknown sizes.

        The default derives entries from authoritative ``FileInfo`` values.
        Driver-backed Stores override this to preserve optional-size inventory
        without forcing a ``stat`` call.

        Example:
            >>> entries = list(store.iter_inventory_entries())  # doctest: +SKIP

        :param prefix:
        :return:
        """

        for info in self.iter_file_infos(prefix=prefix):
            yield info.as_inventory_entry()

    def inventory_page(
        self,
        *,
        prefix: Location | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        snapshot_token: str | None = None,
    ) -> StoreInventoryPage:
        """
        Return one resumable inventory page when inherently supported.

        Example:
            >>> page = store.inventory_page(limit=500)  # doctest: +SKIP

        :param prefix:
        :param cursor:
        :param limit:
        :param snapshot_token:
        :return:
        """

        _ = prefix, cursor, limit, snapshot_token
        raise StoreUnsupportedOperation(
            f"{type(self).__name__} does not support resumable inventory pages."
        )

    def compute_digest(
        self,
        location: Location,
        algorithm: str = "sha256",
        *,
        chunk_size: int = 1024 * 1024,
    ) -> Digest:
        """
        Compute an object digest by streaming through the configured store.

        A concrete store may override this method to expose an authoritative
        driver-side digest.

        Example:
            >>> digest = store.compute_digest(location, "sha256")  # doctest: +SKIP


        :param location:
        :param algorithm:
        :param chunk_size:
        :return:
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
        """
        Copy by verified streaming unless a concrete store overrides it.

        Example:
            >>> info = store.copy(source, destination)  # doctest: +SKIP


        :param source:
        :param destination:
        :param mode:
        :return:
        """
        source_info = self.stat(source)
        reader = (
            self.open_read(source, if_version=source_info.version)
            if self.capabilities.conditional_read
            and source_info.version is not None
            else self.open_read(source)
        )
        with reader as source_stream:
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
        """
        Perform a verified copy followed by conditional source deletion.

        A concrete store may override this method with a safe native move. The
        generic fallback refuses to publish the destination unless the source
        advertises conditional deletion and ``stat`` returns a version token.

        Example:
            >>> info = store.move(source, destination)  # doctest: +SKIP


        :param source:
        :param destination:
        :param mode:
        :return:
        """
        source_info = self.stat(source)
        if not self.capabilities.conditional_delete:
            raise StoreUnsupportedOperation(
                "safe fallback move requires conditional deletion."
            )
        if source_info.version is None:
            raise StoreUnsupportedOperation(
                "safe fallback move requires a source version for "
                + "conditional deletion."
            )
        result = self.copy(source, destination, mode=mode)
        self.delete(source, if_version=source_info.version)
        return result


@runtime_checkable
class NativeImportStoreAPI(StoreCoreAPI, Protocol):
    """Optional cross-Store native object-transfer acceleration.

    Example:
        >>> destination.can_import_from(source)  # doctest: +SKIP
        True
    """

    def can_import_from(self, source: StoreCoreAPI) -> bool:
        """Return whether this destination can natively read the source.

        Example:
            >>> supported = destination.can_import_from(source)  # doctest: +SKIP

        :param source: Configured source Store.
        :return: Whether a native import may be attempted.
        """
        ...

    def import_from(
        self,
        source: StoreCoreAPI,
        source_location: Location,
        destination: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int,
        expected_digest: Digest,
        placement_hints: StoragePlacementHints | None = None,
    ) -> FileInfo:
        """Transfer and verify an object without client-side byte streaming.

        Example:
            >>> info = destination.import_from(  # doctest: +SKIP
            ...     source, source_location, target,
            ...     expected_size=4, expected_digest=digest,
            ... )

        :param source: Configured source Store.
        :param source_location: Source-owned object location.
        :param destination: Destination-owned object location.
        :param mode: Publication mode.
        :param expected_size: Exact authoritative source size.
        :param expected_digest: Authoritative digest verified after transfer.
        :param placement_hints: Advisory destination metadata.
        :return: Committed destination information.
        """
        ...


@runtime_checkable
class NativeCopyStoreAPI(StoreCoreAPI, Protocol):
    """
    Optional backend-native copy acceleration.

    Example:
        >>> def clone(store: NativeCopyStoreAPI, source: Location, target: Location):
        ...     return store.copy(source, target, mode=WriteMode.CREATE_ONLY)
    """

    def copy(
        self,
        source: Location,
        destination: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> FileInfo:
        """
        Copy entirely within the backend without client-side streaming.

        Example:
            >>> info = store.copy(source, destination)  # doctest: +SKIP


        :param source:
        :param destination:
        :param mode:
        :return:
        """
        ...


@runtime_checkable
class NativeMoveStoreAPI(StoreCoreAPI, Protocol):
    """
    Optional backend-native move acceleration.

    Example:
        >>> def relocate(store: NativeMoveStoreAPI, source: Location, target: Location):
        ...     return store.move(source, target, mode=WriteMode.CREATE_ONLY)
    """

    def move(
        self,
        source: Location,
        destination: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> FileInfo:
        """
        Move entirely within the backend with explicit collision behavior.

        Example:
            >>> info = store.move(source, destination)  # doctest: +SKIP


        :param source:
        :param destination:
        :param mode:
        :return:
        """
        ...


@runtime_checkable
class DigestingStoreAPI(StoreCoreAPI, Protocol):
    """
    Optional authoritative or server-side digest acceleration.

    Example:
        >>> def sha256(store: DigestingStoreAPI, location: Location) -> Digest:
        ...     return store.compute_digest(location, "sha256")
    """

    def compute_digest(
        self,
        location: Location,
        algorithm: str = "sha256",
    ) -> Digest:
        """
        Compute a digest without requiring a generic client-side read.

        Example:
            >>> digest = store.compute_digest(location, "sha256")  # doctest: +SKIP


        :param location:
        :param algorithm:
        :return:
        """
        ...


__all__ = [
    "DigestingStoreAPI",
    "StoreCoreAPI",
    "NativeImportStoreAPI",
    "NativeCopyStoreAPI",
    "NativeMoveStoreAPI",
    "StoreFileAPI",
    "WriteSessionAPI",
]
