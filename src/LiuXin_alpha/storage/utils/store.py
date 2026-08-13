"""Safe convenience operations over configured Store primitives."""

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
from LiuXin_alpha.storage.api.store_api import (
    DigestingStore,
    FileStore,
    NativeCopyStore,
    NativeMoveStore,
    WriteSession,
)
from LiuXin_alpha.storage.utils.constants import DEFAULT_STORAGE_CHUNK_SIZE


DEFAULT_COPY_CHUNK_SIZE = DEFAULT_STORAGE_CHUNK_SIZE


def try_stat(store: FileStore, location: Location) -> FileInfo | None:
    """Return ``None`` only when the backend reports genuine absence.

    Example:
        >>> try_stat(store, Location(UUID(int=1), "missing")) is None  # doctest: +SKIP
        True
    """
    try:
        return store.stat(location)
    except StoreNotFound:
        return None


def exists(store: FileStore, location: Location) -> bool:
    """Return existence without concealing permission or availability errors.

    Example:
        >>> exists(store, Location(UUID(int=1), "objects/42"))  # doctest: +SKIP
        True
    """
    return try_stat(store, location) is not None


def get(
    store: FileStore,
    location: Location,
    *,
    offset: int = 0,
    length: int | None = None,
) -> BinaryIO:
    """Familiar alias for the primitive ``open_read`` operation.

    Example:
        >>> stream = get(store, Location(UUID(int=1), "objects/42"))  # doctest: +SKIP
    """
    return store.open_read(location, offset=offset, length=length)


def read_bytes(
    store: FileStore,
    location: Location,
    *,
    offset: int = 0,
    length: int | None = None,
) -> bytes:
    """Read a complete object or requested range into memory.

    Example:
        >>> read_bytes(store, Location(UUID(int=1), "objects/42"), length=4)  # doctest: +SKIP
        b'book'
    """
    with get(store, location, offset=offset, length=length) as source:
        return source.read()


def _write_all(session: WriteSession, data: bytes) -> None:
    """Write all bytes even when a session accepts only partial chunks.

    Example:
        >>> _write_all(session, b"complete payload")  # doctest: +SKIP
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
    store: FileStore,
    location: Location,
    source: BinaryIO,
    *,
    mode: WriteMode = WriteMode.CREATE_ONLY,
    expected_size: int | None = None,
    expected_digest: Digest | None = None,
    chunk_size: int = DEFAULT_COPY_CHUNK_SIZE,
) -> FileInfo:
    """Stream through a staged write and publish only after verification.

    Example:
        >>> import io
        >>> info = put(  # doctest: +SKIP
        ...     store, Location(UUID(int=1), "objects/42"), io.BytesIO(b"book"),
        ...     expected_size=4,
        ... )
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
    store: FileStore,
    location: Location,
    data: bytes,
    *,
    mode: WriteMode = WriteMode.CREATE_ONLY,
    expected_digest: Digest | None = None,
) -> FileInfo:
    """Small-payload wrapper over ``put`` with an exact size expectation.

    Example:
        >>> info = write_bytes(  # doctest: +SKIP
        ...     store, Location(UUID(int=1), "objects/42"), b"book",
        ... )
    """
    return put(
        store,
        location,
        io.BytesIO(data),
        mode=mode,
        expected_size=len(data),
        expected_digest=expected_digest,
    )


def iter_infos(
    store: FileStore,
    *,
    prefix: Location | None = None,
) -> Iterator[FileInfo]:
    """Describe enumerated files without suppressing per-object stat errors.

    Example:
        >>> list(iter_infos(store))  # doctest: +SKIP
        [FileInfo(...)]
    """
    for location in store.iter_locations(prefix=prefix):
        yield store.stat(location)


def compute_digest(
    store: FileStore,
    location: Location,
    algorithm: str = "sha256",
    *,
    chunk_size: int = DEFAULT_COPY_CHUNK_SIZE,
) -> Digest:
    """Use native digesting when advertised, otherwise stream and hash.

    Example:
        >>> digest = compute_digest(  # doctest: +SKIP
        ...     store, Location(UUID(int=1), "objects/42"), "sha256",
        ... )
    """
    if chunk_size < 1:
        raise ValueError("chunk_size must be at least one byte.")
    if store.capabilities.native_digest and isinstance(store, DigestingStore):
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
    store: FileStore,
    source: Location,
    destination: Location,
    *,
    mode: WriteMode,
    source_info: FileInfo,
) -> FileInfo:
    """Copy by verified streaming when native copy is unavailable.

    Example:
        >>> result = _copy_fallback(  # doctest: +SKIP
        ...     store, source, destination, mode=WriteMode.CREATE_ONLY,
        ...     source_info=store.stat(source),
        ... )
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
    store: FileStore,
    source: Location,
    destination: Location,
    *,
    mode: WriteMode = WriteMode.CREATE_ONLY,
) -> FileInfo:
    """Use native copy when available, otherwise read, stage, verify, commit.

    Example:
        >>> result = copy(store, source, destination)  # doctest: +SKIP
    """
    if store.capabilities.native_copy:
        if not isinstance(store, NativeCopyStore):
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
    store: FileStore,
    source: Location,
    destination: Location,
    *,
    mode: WriteMode = WriteMode.CREATE_ONLY,
) -> FileInfo:
    """Use native move, or verified copy followed by conditional deletion.

    The fallback is deliberately unavailable unless the source Store both
    advertises conditional deletion and returns a version token. This check is
    made before destination publication.

    Example:
        >>> result = move(store, source, destination)  # doctest: +SKIP
    """
    if store.capabilities.native_move:
        if not isinstance(store, NativeMoveStore):
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
    "iter_infos",
    "move",
    "put",
    "read_bytes",
    "try_stat",
    "write_bytes",
]
