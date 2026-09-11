"""
Compose configured-Store primitives into reads, staged writes, hashing, and moves.

Helpers rely on concrete Stores for Location ownership, capabilities, and staged
publication guarantees. They do not independently validate returned metadata.
Generic copies read without a version pin and ask the destination session to
check observed size/digest. Generic moves require conditional source deletion
before copying, but a later delete failure can leave both objects present.

Streams opened by read/copy helpers are closed by those helpers; put borrows its
input and owns only the staged session. Chunk limits bound individual reads,
not the memory used by read_bytes or backend-specific staging.

Example:
    >>> info = write_bytes(store, location, b"book")  # doctest: +SKIP
"""

from __future__ import annotations

import hashlib
import io

from collections.abc import Iterator
from typing import BinaryIO
from uuid import UUID

from LiuXin_alpha.storage.api.errors import (
    StoreError,
    StoreNotFound,
    StoreUnsupportedOperation,
)
from LiuXin_alpha.storage.api.models import Digest, FileInfo, Location, WriteMode
from LiuXin_alpha.storage.api.store_api.file_api import (
    DigestingStoreAPI,
    StoreCoreAPI,
    NativeCopyStoreAPI,
    NativeMoveStoreAPI,
    WriteSessionAPI,
)
from LiuXin_alpha.storage.utils.constants import DEFAULT_STORAGE_CHUNK_SIZE


DEFAULT_COPY_CHUNK_SIZE = DEFAULT_STORAGE_CHUNK_SIZE


def try_stat(store: StoreCoreAPI, location: Location) -> FileInfo | None:
    """
    Return ``None`` only when the backend reports genuine absence.

    No existence preflight, ownership check, or result-address validation is added here. Permission,
    availability, and other non-not-found failures propagate.

    Example:
        >>> try_stat(store, Location(UUID(int=1), "missing")) is None  # doctest: +SKIP
        True


    :param store: Configured Store primitive implementation; ownership and backend policy are enforced there.
    :param location: Routed Location passed to this Store without additional validation in the helper.
    :return: Store metadata unchanged, or None only when stat raises StoreNotFound.
    """
    try:
        return store.stat(location)
    except StoreNotFound:
        return None


def exists(store: StoreCoreAPI, location: Location) -> bool:
    """
    Return existence without concealing permission or availability errors.

    Example:
        >>> exists(store, Location(UUID(int=1), "objects/42"))  # doctest: +SKIP
        True


    :param store: Configured Store primitive implementation; ownership and backend policy are enforced there.
    :param location: Routed Location passed to this Store without additional validation in the helper.
    :return: Whether try_stat returns a non-None result; other exceptions remain visible.
    """
    return try_stat(store, location) is not None


def get(
    store: StoreCoreAPI,
    location: Location,
    *,
    offset: int = 0,
    length: int | None = None,
    if_version: str | None = None,
) -> BinaryIO:
    """
    Familiar alias for the primitive ``open_read`` operation.

    Omitting if_version when None preserves compatibility with readers that do not accept that
    optional keyword. Range and version validation remain with open_read.

    Example:
        >>> stream = get(store, Location(UUID(int=1), "objects/42"))  # doctest: +SKIP


    :param store: Configured Store primitive implementation; ownership and backend policy are enforced there.
    :param location: Routed Location passed to this Store without additional validation in the helper.
    :param offset: Starting byte offset delegated to open_read; zero by default.
    :param length: Optional range length in bytes; None asks for the remaining object.
    :param if_version: Optional opaque version precondition; None omits the keyword entirely.
    :return: Read stream owned by the caller, who must close it.
    """
    if if_version is None:
        return store.open_read(location, offset=offset, length=length)
    return store.open_read(
        location, offset=offset, length=length, if_version=if_version
    )


def read_bytes(
    store: StoreCoreAPI,
    location: Location,
    *,
    offset: int = 0,
    length: int | None = None,
    if_version: str | None = None,
) -> bytes:
    """
    Read a complete object or requested range into memory.

    The selected range is fully materialized with no separate memory cap. Context cleanup occurs
    even when read raises, subject to the concrete stream context manager.

    Example:
        >>> read_bytes(store, Location(UUID(int=1), "objects/42"), length=4)  # doctest: +SKIP
        b'book'


    :param store: Configured Store primitive implementation; ownership and backend policy are enforced there.
    :param location: Routed Location passed to this Store without additional validation in the helper.
    :param offset: Starting byte offset forwarded through get.
    :param length: Optional byte range length; None reads the remaining object.
    :param if_version: Optional version token forwarded through get when supplied.
    :return: source.read() result after stream cleanup; the helper does not independently check its bytes type.
    """
    with get(
        store,
        location,
        offset=offset,
        length=length,
        if_version=if_version,
    ) as source:
        return source.read()


def _write_all(session: WriteSessionAPI, data: bytes) -> None:
    """
    Write all bytes even when a session accepts only partial chunks.

    An empty payload causes no write. This helper neither commits nor aborts the session.

    Example:
        >>> _write_all(session, b"complete payload")  # doctest: +SKIP


    :param session: Unfinished staged session accepting payload chunks, potentially only partly.
    :param data: Bytes whose unaccepted suffix is retried until exhausted.
    :return: None after accepting every byte; nonpositive or excessive counts raise StoreError.
    """

    view = memoryview(data)
    written = 0
    while written < len(view):
        accepted = session.write(view[written:].tobytes())
        if accepted <= 0:
            raise StoreError("write session accepted no bytes and made no progress.")
        if accepted > len(view) - written:
            raise StoreError("write session reported accepting more bytes than supplied.")
        written += accepted


def put(
    store: StoreCoreAPI,
    location: Location,
    source: BinaryIO,
    *,
    mode: WriteMode = WriteMode.CREATE_ONLY,
    expected_size: int | None = None,
    expected_digest: Digest | None = None,
    chunk_size: int = DEFAULT_COPY_CHUNK_SIZE,
) -> FileInfo:
    """
    Stream through a staged write and publish only after verification.

    Validate chunk size and a supplied expected size before creating the session. Falsey read
    results end the loop before type checking; each nonempty result must be bytes. The session
    context owns cleanup on failure, and its commit owns size/digest verification and publication.
    No retry or generic rollback is added.

    Example:
        >>> import io
        >>> info = put(  # doctest: +SKIP
        ...     store, Location(UUID(int=1), "objects/42"), io.BytesIO(b"book"),
        ...     expected_size=4,
        ... )


    :param store: Configured Store primitive implementation; ownership and backend policy are enforced there.
    :param location: Routed Location passed to this Store without additional validation in the helper.
    :param source: Borrowed binary input consumed from its current position, without rewinding or closing it.
    :param mode: Explicit destination collision policy, defaulting to CREATE_ONLY.
    :param expected_size: Optional nonnegative exact logical byte count for the session to verify before publication.
    :param expected_digest: Optional expected content digest for the session to verify before publication.
    :param chunk_size: Positive maximum bytes requested per input read; defaults to the shared 1 MiB chunk size.
    :return: Result of session.commit(), without an additional Location or metadata validation pass.
    """
    if chunk_size < 1:
        raise ValueError("chunk_size must be at least one byte.")
    if expected_size is not None and expected_size < 0:
        raise ValueError("expected_size must not be negative.")

    session = store.begin_write(
        location,
        mode=mode,
        expected_size=expected_size,
        expected_digest=expected_digest,
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
    store: StoreCoreAPI,
    location: Location,
    data: bytes,
    *,
    mode: WriteMode = WriteMode.CREATE_ONLY,
    expected_digest: Digest | None = None,
) -> FileInfo:
    """
    Small-payload wrapper over ``put`` with an exact size expectation.

    This wrapper supplies the actual buffer length rather than accepting a separate size claim. It
    remains intended for payloads that fit in memory.

    Example:
        >>> info = write_bytes(  # doctest: +SKIP
        ...     store, Location(UUID(int=1), "objects/42"), b"book",
        ... )


    :param store: Configured Store primitive implementation; ownership and backend policy are enforced there.
    :param location: Routed Location passed to this Store without additional validation in the helper.
    :param data: Small in-memory payload wrapped in BytesIO with len(data) as its exact byte expectation.
    :param mode: Explicit destination collision policy, defaulting to CREATE_ONLY.
    :param expected_digest: Optional expected content digest passed to the staged session.
    :return: Committed FileInfo returned through put.
    """
    return put(
        store,
        location,
        io.BytesIO(data),
        mode=mode,
        expected_size=len(data),
        expected_digest=expected_digest,
    )


def iter_file_infos(
    store: StoreCoreAPI,
    *,
    prefix: Location | None = None,
) -> Iterator[FileInfo]:
    """
    Describe enumerated files without suppressing per-object stat errors.

    Enumeration and per-object stat occur only as the iterator is advanced. Earlier results remain
    consumed if a later stat fails; no deduplication or stable snapshot is added.

    Example:
        >>> list(iter_file_infos(store))  # doctest: +SKIP
        [FileInfo(...)]


    :param store: Configured Store primitive implementation; ownership and backend policy are enforced there.
    :param prefix: Optional routed prefix forwarded to the Store enumerator.
    :return: Lazy iterator of stat results in enumeration order; enumeration/stat failures propagate.
    """
    for location in store.iter_locations(prefix=prefix):
        yield store.stat(location)


def compute_digest(
    store: StoreCoreAPI,
    location: Location,
    algorithm: str = "sha256",
    *,
    chunk_size: int = DEFAULT_COPY_CHUNK_SIZE,
) -> Digest:
    """
    Use native digesting when advertised, otherwise stream and hash.

    Native hashing requires both the capability and runtime protocol. A missing protocol falls back
    to streaming, but an exception from an invoked native method does not. Generic hashing opens an
    unversioned stream, treats a falsey chunk as EOF, and requires every nonempty chunk to be bytes.
    Unsupported hashlib names become StoreUnsupportedOperation.

    Example:
        >>> digest = compute_digest(  # doctest: +SKIP
        ...     store, Location(UUID(int=1), "objects/42"), "sha256",
        ... )


    :param store: Configured Store primitive implementation; ownership and backend policy are enforced there.
    :param location: Routed Location passed to this Store without additional validation in the helper.
    :param algorithm: Digest algorithm sent unchanged to a native implementation or hashlib.new; sha256 by default.
    :param chunk_size: Positive requested read size for generic hashing; validated even when native hashing is used.
    :return: Native digest result unchanged, or a normalized Digest of the streamed bytes.
    """
    if chunk_size < 1:
        raise ValueError("chunk_size must be at least one byte.")
    if store.capabilities.native_digest and isinstance(store, DigestingStoreAPI):
        return store.compute_digest(location, algorithm)

    try:
        digest = hashlib.new(algorithm)
    except ValueError as exc:
        raise StoreUnsupportedOperation(
            f"digest algorithm is not supported: {algorithm!r}"
        ) from exc

    with store.open_read(location) as source:
        while True:
            chunk = source.read(chunk_size)
            if not chunk:
                break
            if not isinstance(chunk, bytes):
                raise TypeError("store read stream must return bytes.")
            digest.update(chunk)
    return Digest(algorithm=algorithm, value=digest.hexdigest())


def _copy_fallback(
    store: StoreCoreAPI,
    source: Location,
    destination: Location,
    *,
    mode: WriteMode,
    source_info: FileInfo,
) -> FileInfo:
    """
    Copy by staged streaming using prior source observations.

    The read is not pinned to source_info.version, so observations and bytes can come from different
    moments. The destination session is responsible for rejecting size/digest mismatches before
    publication. Native metadata is not separately copied by this helper.

    Example:
        >>> result = _copy_fallback(  # doctest: +SKIP
        ...     store, source, destination, mode=WriteMode.CREATE_ONLY,
        ...     source_info=store.stat(source),
        ... )


    :param store: Configured Store primitive implementation; ownership and backend policy are enforced there.
    :param source: Source Location in this configured Store.
    :param destination: Destination Location in the same Store; collision handling belongs to its write implementation.
    :param mode: Required destination collision policy selected by the calling operation.
    :param source_info: Prior source observations supplying expected size/digest; its version is not used for the read.
    :return: Committed destination metadata from put after the source stream is closed.
    """

    with store.open_read(source) as source_stream:
        return put(
            store,
            destination,
            source_stream,
            mode=mode,
            expected_size=source_info.size,
            expected_digest=source_info.digest,
        )


def copy(
    store: StoreCoreAPI,
    source: Location,
    destination: Location,
    *,
    mode: WriteMode = WriteMode.CREATE_ONLY,
) -> FileInfo:
    """
    Use native copy when available, otherwise read, stage, verify, commit.

    An advertised native-copy capability without its protocol raises before any fallback. Otherwise
    the generic branch stats the source once and uses its size/digest expectations with staged
    streaming.

    Example:
        >>> result = copy(store, source, destination)  # doctest: +SKIP


    :param store: Configured Store primitive implementation; ownership and backend policy are enforced there.
    :param source: Source Location in this configured Store.
    :param destination: Destination Location in the same Store; collision handling belongs to its write implementation.
    :param mode: Explicit destination collision policy, defaulting to CREATE_ONLY.
    :return: Native copy result or committed destination metadata from the streaming fallback.
    """
    if store.capabilities.native_copy:
        if not isinstance(store, NativeCopyStoreAPI):
            raise StoreUnsupportedOperation(
                "store advertises native_copy but does not implement copy()."
            )
        return store.copy(source, destination, mode=mode)

    source_info = store.stat(source)
    return _copy_fallback(
        store,
        source,
        destination,
        mode=mode,
        source_info=source_info,
    )


def move(
    store: StoreCoreAPI,
    source: Location,
    destination: Location,
    *,
    mode: WriteMode = WriteMode.CREATE_ONLY,
) -> FileInfo:
    """
    Use native move, or verified copy followed by conditional deletion.

    The fallback is deliberately unavailable unless the source Store both advertises conditional
    deletion and returns a version token. This check is made before destination publication.

    The generic branch stats first, then requires conditional_delete and a non-None source version
    before opening the source or publishing a destination. It calls the streaming fallback directly,
    even if native copy is advertised, then deletes with the observed token. The read itself is
    unversioned. Copy success followed by deletion failure is not rolled back;
    source-equals-destination and other ownership policies remain with the Store. An advertised
    native move without its protocol is an error, not a fallback request.

    Example:
        >>> result = move(store, source, destination)  # doctest: +SKIP


    :param store: Configured Store primitive implementation; ownership and backend policy are enforced there.
    :param source: Source Location in this configured Store.
    :param destination: Destination Location in the same Store; collision handling belongs to its write implementation.
    :param mode: Explicit destination collision policy, defaulting to CREATE_ONLY.
    :return: Native move result, or copied destination metadata after conditional source deletion succeeds.
    """
    if store.capabilities.native_move:
        if not isinstance(store, NativeMoveStoreAPI):
            raise StoreUnsupportedOperation(
                "store advertises native_move but does not implement move()."
            )
        return store.move(source, destination, mode=mode)

    source_info = store.stat(source)
    if not store.capabilities.conditional_delete:
        raise StoreUnsupportedOperation(
            "safe fallback move requires conditional deletion."
        )
    if source_info.version is None:
        raise StoreUnsupportedOperation(
            "safe fallback move requires a source version for conditional "
            + "deletion."
        )
    result = _copy_fallback(
        store,
        source,
        destination,
        mode=mode,
        source_info=source_info,
    )
    store.delete(source, if_version=source_info.version)
    return result


__all__ = [
    "DEFAULT_COPY_CHUNK_SIZE",
    "compute_digest",
    "copy",
    "exists",
    "get",
    "iter_file_infos",
    "move",
    "put",
    "read_bytes",
    "try_stat",
    "write_bytes",
]
