
"""
Define routed Store file primitives, staged publication, and optional accelerators.

Every public target is a Location owned by one configured Store. Raw driver
addresses stay below this boundary; Asset/Replica identity, cross-Store placement,
repair, and database transactions belong to higher layers. Protocol declarations
state required behavior without supplying backend enforcement.

StoreFileAPI composes the primitives into whole-buffer reads, staged writes,
inventory, hashing, and copy/move operations. Copies pin a read when supported
and a stat version exists; fallback moves additionally require conditional source
deletion. A later delete or metadata failure can follow successful publication.

Example:
    >>> info = store.write_bytes(location, b"book")  # doctest: +SKIP
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


# Todo: Session should store the location it's writing to and store - so it can be passed into other functions
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

    When ``atomic_publish`` is true, commit changes visibility in one atomic step.  A store that
    cannot guarantee that advertises ``atomic_publish`` as false; callers may then choose a safer
    destination or recovery policy.

    Runtime protocol membership establishes method presence, not that a session satisfies these
    publication and cleanup guarantees.

    Example:
        >>> with store.begin_write(location, expected_size=len(payload)) as session:  # doctest: +SKIP
        ...     remaining = payload
        ...     while remaining:
        ...         accepted = session.write(remaining)
        ...         if not 0 < accepted <= len(remaining):
        ...             raise StoreError("invalid write progress")
        ...         remaining = remaining[accepted:]
        ...     info = session.commit()
    """

    def write(self, data: bytes) -> int:
        """
        Append bytes to private staged state and return the count accepted.

        Example:
            >>> accepted = session.write(b"payload")  # doctest: +SKIP


        :param data: Payload bytes appended to unpublished staging.
        :return: Number of bytes accepted; callers must handle partial acceptance.
        """
        ...

    def commit(self) -> FileInfo:
        """
        Verify expectations and publish the completed object.

        Example:
            >>> info = session.commit()  # doctest: +SKIP


        :return: FileInfo for the complete published destination after checking supplied size/digest expectations.
        """
        ...

    def abort(self) -> None:
        """
        Discard staged state; repeated calls must be safe.

        Example:
            >>> session.abort()  # doctest: +SKIP
            >>> session.abort()  # doctest: +SKIP


        :return: None after discarding unpublished staging; repeated cleanup must be safe.
        """
        ...

    def __enter__(self) -> WriteSessionAPI:
        """
        Enter the staged-write lifetime and return this session.

        Example:
            >>> entered = session.__enter__()  # doctest: +SKIP


        :return: This staged-write session under its managed lifetime.
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


        :param exc_type: Exception class from the context body, or None on normal exit.
        :param exc: Exception instance from the context body, or None.
        :param traceback: Body exception traceback, or None.
        :return: None after cleanup, leaving any body exception unsuppressed.
        """
        ...


# Todo: Why is this in the file api? Why is it called this?
@runtime_checkable
class StoreCoreAPI(Protocol):
    """
    Structural view of the mandatory operations on one configured store.

    Generic code should depend on this protocol rather than backend-specific driver, path, or
    connection types.  It intentionally says nothing about how the configured store delegates work
    to ``StorageDriverAPI``.

    Concrete implementations enforce Location ownership and typed backend failures. Runtime
    membership does not validate that implementation behavior matches these contracts.

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

        Configured wrappers may restrict backend claims according to read-only policy; current
        health belongs to status rather than this capability value.

        Example:
            >>> supports_ranges = store.capabilities.range_reads  # doctest: +SKIP

        :return: Declared Store operation support, distinct from current availability and capacity.
        """
        ...

    @abc.abstractmethod
    def stat(  # pyright: ignore[reportInvalidAbstractMethod]
        self,
        location: Location,
    ) -> FileInfo:
        """
        Get one object's stats or raise ``StoreNotFound``.

        Connection, permission, and backend errors must remain visible rather than being converted
        into a false not-found result.

        Example:
            >>> info = store.stat(Location(UUID(int=1), "objects/42"))  # doctest: +SKIP


        :param location: Routed object Location belonging to this configured Store.
        :return: Current FileInfo for the object; only genuine absence is StoreNotFound.
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
        ``capabilities.conditional_read`` is true. A stale token raises ``StorePreconditionFailed``
        before mismatched bytes are returned.

        Range and version preconditions are enforced by the concrete implementation; callers must
        not infer a stable snapshot from an unconditioned read.

        Example:
            >>> source = store.open_read(  # doctest: +SKIP
            ...     Location(UUID(int=1), "objects/42"), offset=10, length=20,
            ... )

        :param location: Routed object Location belonging to this configured Store.
        :param offset: Starting byte offset, zero by default.
        :param length: Optional byte count; None reads through the remaining object.
        :param if_version: Optional opaque stat version used as a read precondition when supported.

        :return: Binary read stream owned by the caller, who must close it.
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

        ``CREATE_ONLY`` is the safe default.  Immutable or content-addressed stores may treat
        publication of identical bytes at an existing key as idempotent success, but different bytes
        must raise an integrity or already-exists error rather than overwrite the object. Placement
        hints are advisory and Store-facing. Implementations that support rich layouts or indexes
        may consume them during commit; implementations that do not may ignore them.

        Example:
            >>> session = store.begin_write(  # doctest: +SKIP
            ...     Location(UUID(int=1), "objects/42"),
            ...     mode=WriteMode.CREATE_ONLY, expected_size=4,
            ... )


        :param location: Owned final destination, which remains absent or unchanged during staging.
        :param mode: Destination collision policy; CREATE_ONLY unless explicitly overridden.
        :param expected_size: Optional exact logical byte count checked by the staged session before publication.
        :param expected_digest: Optional expected content digest checked by the staged session before publication.
        :param placement_hints: Optional advisory placement metadata; unsupported Stores may ignore it.
        :return: Uncommitted write session whose context owns cleanup and whose commit publishes bytes.
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

        ``missing_ok`` suppresses only genuine absence. Passing ``if_version`` requires
        ``capabilities.conditional_delete`` and deletes only the exact version previously returned
        by ``stat``. Unsupported conditional deletion raises ``StoreUnsupportedOperation``; a stale
        token raises ``StorePreconditionFailed``. Availability, permission, and other failures
        remain visible.

        Example:
            >>> store.delete(  # doctest: +SKIP
            ...     Location(UUID(int=1), "objects/42"), if_version="v3",
            ... )


        :param location: Routed object Location belonging to this configured Store.
        :param missing_ok: Whether genuine absence is accepted as successful idempotent deletion.
        :param if_version: Optional observed object version that must still match at deletion.
        :return: None after deletion or permitted absence; unsupported/stale conditions and operational errors remain visible.
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

        A non-``None`` prefix additionally requires ``capabilities.prefix_enumeration``.
        Implementations raise ``StoreUnsupportedOperation`` rather than ignoring an unsupported
        filter.

        Example:
            >>> locations = list(store.iter_locations())  # doctest: +SKIP


        :param prefix: Optional owned prefix, requiring prefix_enumeration support.
        :return: Iterator of concrete file Locations under the declared enumeration completeness.
        """
        ...

    # Todo: Comment to explain this pyright ignores
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


        :param refresh: Whether to request a fresh endpoint check rather than cached state.
        :return: StoreStatus describing current operational availability, writability, and known capacity.
        """
        ...


class StoreFileAPI(StoreCoreAPI, abc.ABC):
    """
    Nominal configured-store byte API with safe convenience operations.

    These methods add no independent Location-ownership check or committed-result validation;
    concrete primitives retain those responsibilities. Iteration and optional accelerators remain
    subject to backend support.

    Example:
        >>> def read_header(store: StoreFileAPI, location: Location) -> bytes:
        ...     return store.read_bytes(location, length=16)
    """

    # Todo: try_* for everything - need to be consistent
    def try_stat(self, location: Location) -> FileInfo | None:
        """
        Return ``None`` only when the store reports genuine absence.

        The helper adds no separate preflight or error conversion beyond catching StoreNotFound.

        Example:
            >>> store.try_stat(Location(UUID(int=1), "missing")) is None  # doctest: +SKIP
            True


        :param location: Routed object Location belonging to this configured Store.
        :return: stat result unchanged, or None only when stat raises StoreNotFound.
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


        :param location: Routed object Location belonging to this configured Store.
        :return: Whether try_stat returns non-None; other stat failures propagate.
        """
        return self.try_stat(location) is not None

    def file_size(self, location: Location) -> int:
        """
        Return one object's authoritative byte size.

        Example:
            >>> size = store.file_size(Location(UUID(int=1), "objects/42"))  # doctest: +SKIP


        :param location: Routed object Location belonging to this configured Store.
        :return: size from a fresh stat result, measured in logical bytes.
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

        When if_version is None, omit the keyword entirely so readers without the optional keyword
        remain compatible.

        Example:
            >>> source = store.get(location, offset=10, length=20)  # doctest: +SKIP


        :param location: Routed object Location belonging to this configured Store.
        :param offset: Starting byte offset, zero by default.
        :param length: Optional byte count; None reads through the remaining object.
        :param if_version: Optional opaque stat version used as a read precondition when supported.
        :return: open_read stream owned and closed by the caller.
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

        The whole selected range is materialized without an independent memory cap. A None version
        omits the keyword when opening the stream; context cleanup runs when reading exits.

        Example:
            >>> store.read_bytes(location, length=4)  # doctest: +SKIP
            b'book'


        :param location: Routed object Location belonging to this configured Store.
        :param offset: Starting byte offset, zero by default.
        :param length: Optional byte count; None reads through the remaining object.
        :param if_version: Optional opaque stat version used as a read precondition when supported.
        :return: source.read() result after stream cleanup, without an additional bytes-type check.
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

        Validate the chunk size and any size expectation before beginning a session. Forward
        placement_hints only when supplied and advertised; otherwise omit the keyword. Falsey read
        results end transfer before type checking, while nonempty chunks must be bytes. Retry
        partial writes and reject nonpositive or excessive acceptance. Session context cleanup owns
        abandoned staging, and commit owns verification and publication.

        Example:
            >>> import io
            >>> info = store.put(  # doctest: +SKIP
            ...     location, io.BytesIO(b"book"), expected_size=4,
            ... )


        :param location: Owned final destination supplied to begin_write.
        :param source: Borrowed binary input read from its current position; this method does not close or rewind it.
        :param mode: Destination collision policy; CREATE_ONLY unless explicitly overridden.
        :param expected_size: Optional exact logical byte count checked by the staged session before publication.
        :param expected_digest: Optional expected content digest checked by the staged session before publication.
        :param placement_hints: Optional advisory placement metadata; unsupported Stores may ignore it.
        :param chunk_size: Positive maximum requested bytes per read; 1 MiB by default.
        :return: session.commit() result without a separate metadata or Location validation pass.
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

        This does not perform digest verification itself; the exact size and optional digest are
        passed to put and its staged session.

        Example:
            >>> info = store.write_bytes(location, b"book")  # doctest: +SKIP


        :param location: Owned final destination supplied to put.
        :param data: Small payload wrapped in BytesIO; len(data) supplies the exact size expectation.
        :param mode: Destination collision policy; CREATE_ONLY unless explicitly overridden.
        :param expected_digest: Optional expected content digest checked by the staged session before publication.
        :param placement_hints: Optional advisory placement metadata; unsupported Stores may ignore it.
        :return: Committed destination metadata returned by put.
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

        Enumeration and stat occur as the iterator advances. A later error propagates after any
        earlier results were yielded; the method adds no snapshot or duplicate filtering.

        Example:
            >>> infos = list(store.iter_file_infos())  # doctest: +SKIP


        :param prefix: Optional owned prefix forwarded to iter_locations.
        :return: Lazy sequence of stat results in enumeration order.
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

        The default derives entries from authoritative ``FileInfo`` values. Driver-backed Stores
        override this to preserve optional-size inventory without forcing a ``stat`` call.

        The default calls each FileInfo.as_inventory_entry and therefore retains the stat cost of
        iter_file_infos. It does not itself discover unknown-size objects.

        Example:
            >>> entries = list(store.iter_inventory_entries())  # doctest: +SKIP


        :param prefix: Optional owned prefix forwarded to iter_file_infos.
        :return: Lazy inventory-entry projections of the default per-object FileInfo results.
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

        This default ignores all supplied arguments and performs no inventory call or pagination
        validation.

        Example:
            >>> page = store.inventory_page(limit=500)  # doctest: +SKIP


        :param prefix: Optional owned prefix for a supporting implementation.
        :param cursor: Optional opaque continuation token from a preceding page.
        :param limit: Optional requested maximum entries, interpreted by the backend.
        :param snapshot_token: Optional backend snapshot identity used for continuation.
        :return: StoreInventoryPage in a supporting override; this default raises StoreUnsupportedOperation.
        """

        _ = prefix, cursor, limit, snapshot_token
        raise StoreUnsupportedOperation(
            f"{type(self).__name__} does not support resumable inventory pages."
        )

    # Todo: Once again, lotta code in this API
    def compute_digest(
        self,
        location: Location,
        algorithm: str = "sha256", # Todo: This should be an enum? Or a list of string values. Error message should include available algoriths.
        *,
        chunk_size: int = 1024 * 1024,
    ) -> Digest:
        """
        Compute an object digest by streaming through the configured store.

        A concrete store may override this method to expose an authoritative driver-side digest.

        Validate chunk size, translate hashlib ValueError to StoreUnsupportedOperation, and then
        open an unversioned stream. Falsey reads mean EOF; nonempty reads must be bytes. Stream
        cleanup is owned here. This default does not select a native protocol on its own.

        Example:
            >>> digest = store.compute_digest(location, "sha256")  # doctest: +SKIP


        :param location: Routed object Location belonging to this configured Store.
        :param algorithm: hashlib algorithm name; sha256 by default.
        :param chunk_size: Positive maximum bytes requested per source read; 1 MiB by default.
        :return: Normalized Digest of bytes observed through the opened stream.
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

    # Todo: Lotta code in this API
    def copy(
        self,
        source: Location,
        destination: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> FileInfo:
        """
        Copy by verified streaming unless a concrete store overrides it.

        Stat once, then pass if_version only when conditional_read is advertised and the observed
        version is non-None.
        Otherwise read unversioned.
        Destination verification uses the observed size/digest.
        The default does not forward placement hints or native metadata, and a close
        failure can occur after destination commit.

        Example:
            >>> info = store.copy(source, destination)  # doctest: +SKIP


        :param source: Owned source Location whose content is copied.
        :param destination: Owned destination Location subject to the requested collision policy.
        :param mode: Destination collision policy; CREATE_ONLY unless explicitly overridden.
        :return: Destination metadata returned by put after source-stream cleanup.
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

        A concrete store may override this method with a safe native move. The generic fallback
        refuses to publish the destination unless the source advertises conditional deletion and
        ``stat`` returns a version token.

        Stat first and reject missing conditional-delete support or a missing version before
        copying. Then call self.copy, which may be overridden and may observe the source again, and
        delete using the first stat token. Deletion failure is not rolled back and can leave both
        copies present.

        Example:
            >>> info = store.move(source, destination)  # doctest: +SKIP


        :param source: Owned source Location to copy and conditionally delete.
        :param destination: Owned destination Location subject to the requested collision policy.
        :param mode: Destination collision policy; CREATE_ONLY unless explicitly overridden.
        :return: Copy result after conditional source deletion succeeds.
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
    """
    Optional cross-Store native object-transfer acceleration.

    This is destination-side cross-Store acceleration. A structural match alone does not establish
    compatibility with a particular source; callers use can_import_from before requesting a
    transfer.

    Example:
        >>> destination.can_import_from(source)  # doctest: +SKIP
        True
    """

    def can_import_from(self, source: StoreCoreAPI) -> bool:
        """
        Return whether this destination can natively read the source.

        Example:
            >>> supported = destination.can_import_from(source)  # doctest: +SKIP


        :param source: Configured source Store to test for native import compatibility.
        :return: Whether this destination can attempt native import; this is not a transfer or success receipt.
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
        """
        Transfer and verify an object without client-side byte streaming.

        Example:
            >>> info = destination.import_from(  # doctest: +SKIP
            ...     source, source_location, target,
            ...     expected_size=4, expected_digest=digest,
            ... )


        :param source: Configured source Store supplying the object.
        :param source_location: Source-owned Location of the complete object.
        :param destination: Destination-owned Location where the transferred object is published.
        :param mode: Destination collision policy; CREATE_ONLY unless explicitly overridden.
        :param expected_size: Required exact authoritative source byte count to verify.
        :param expected_digest: Required authoritative source digest verified after transfer.
        :param placement_hints: Optional advisory destination placement metadata.
        :return: Committed destination FileInfo after the native transfer and requested verification.
        """
        ...


@runtime_checkable
class NativeCopyStoreAPI(StoreCoreAPI, Protocol):
    """
    Optional backend-native copy acceleration.

    Method presence alone does not distinguish native acceleration from an inherited streaming
    fallback; callers also consult native_copy capability.

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


        :param source: Owned source Location whose content is copied.
        :param destination: Owned destination Location subject to the requested collision policy.
        :param mode: Destination collision policy; CREATE_ONLY unless explicitly overridden.
        :return: Metadata for the complete destination copied within the backend.
        """
        ...


@runtime_checkable
class NativeMoveStoreAPI(StoreCoreAPI, Protocol):
    """
    Optional backend-native move acceleration.

    Method presence alone does not prove backend-native execution; callers also consult native_move
    capability.

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


        :param source: Owned Location to relocate within the backend.
        :param destination: Owned destination Location subject to the requested collision policy.
        :param mode: Destination collision policy; CREATE_ONLY unless explicitly overridden.
        :return: Metadata for the complete relocated destination.
        """
        ...


@runtime_checkable
class DigestingStoreAPI(StoreCoreAPI, Protocol):
    """
    Optional authoritative or server-side digest acceleration.

    A StoreFileAPI instance already has a generic compute_digest method. Runtime protocol membership
    must therefore be combined with native_digest capability when choosing acceleration.

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


        :param location: Routed object Location belonging to this configured Store.
        :param algorithm: Requested digest algorithm, sha256 by default.
        :return: Authoritative or server-computed Digest for the object.
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
