"""
Transfer, enumerate, and materialize raw driver objects without Store policy.

Helpers corroborate capabilities with optional protocols and validate canonical
result addresses. Generic copies use source stat size/digest expectations with
staged destination writes; fallback moves additionally require version-protected
source deletion. Native acceleration relies on its declared complete-copy contract.
Validation after commit can fail after bytes were published; no generic rollback
spans source and destination endpoints.

Example:
    >>> result = transfer_between_drivers(source_driver, source, destination_driver, destination)  # doctest: +SKIP
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import tempfile

from collections.abc import Generator, Iterator
from pathlib import Path
from typing import BinaryIO, TypeVar, cast

from LiuXin_alpha.storage.api.errors import (
    StorageError,
    StorageIntegrityError,
    StorageUnsupportedOperation,
)
from LiuXin_alpha.storage.api.models import (
    Digest,
    EnumerationCompleteness,
    WriteMode,
)
from LiuXin_alpha.storage.api.store_driver_api.accelerators_api import (
    NativeCopyStorageDriverAPI,
    NativeMoveStorageDriverAPI,
)
from LiuXin_alpha.storage.api.store_driver_api.models import (
    DriverObjectInfo,
    DriverObjectAddress,
    DriverInventoryEntry,
)
from LiuXin_alpha.storage.api.store_driver_api.optional_api import (
    DeletableStorageDriverAPI,
    DriverWriteSessionAPI,
    EnumerableStorageDriverAPI,
    WritableStorageDriverAPI,
)
from LiuXin_alpha.storage.api.store_driver_api.readable_api import (
    ReadableStorageDriverAPI,
)
from LiuXin_alpha.storage.utils.constants import DEFAULT_STORAGE_CHUNK_SIZE


_SourceAddressT = TypeVar("_SourceAddressT", bound=DriverObjectAddress)
_DestinationAddressT = TypeVar(
    "_DestinationAddressT", bound=DriverObjectAddress
)


def write_all(
    session: DriverWriteSessionAPI[_DestinationAddressT],
    data: bytes,
) -> None:
    """
    Write all bytes even when a staged session accepts partial chunks.

    Retry partial acceptance without committing or aborting. Zero/negative progress or a count above
    the supplied remainder raises; an empty payload causes no session.write call.

    Example:
        >>> write_all(session, b"complete payload")  # doctest: +SKIP


    :param session: Borrowed unfinished staged-write session.
    :param data: Bytes whose unaccepted suffix is retried until exhausted.
    :return: None after every byte is accepted; impossible counts raise StorageError.
    """
    view = memoryview(data)
    written = 0
    while written < len(view):
        accepted = session.write(view[written:].tobytes())
        if accepted <= 0:
            raise StorageError(
                "driver write session accepted no bytes and made no progress."
            )
        if accepted > len(view) - written:
            raise StorageError(
                "driver write session reported accepting more bytes than supplied."
            )
        written += accepted


def put_object(
    driver: ReadableStorageDriverAPI[_DestinationAddressT],
    object_address: _DestinationAddressT,
    source: BinaryIO,
    *,
    mode: WriteMode = WriteMode.CREATE_ONLY,
    expected_size: int | None = None,
    expected_digest: Digest | None = None,
    metadata: tuple[tuple[str, str], ...] = (),
    chunk_size: int = DEFAULT_STORAGE_CHUNK_SIZE,
) -> DriverObjectInfo[_DestinationAddressT]:
    """
    Stream bytes through a driver's optional staged-write protocol.

    Validate chunk/size bounds and destination capability before beginning the session. Treat falsey
    reads as EOF before requiring nonempty chunks to be bytes. The session context handles
    abandonment; the helper borrows the source and adds no version pin. Returned metadata validation
    occurs after commit and cannot undo publication.

    Example:
        >>> import io
        >>> info = put_object(  # doctest: +SKIP
        ...     driver, address, io.BytesIO(b"book"), expected_size=4,
        ... )


    :param driver: Readable driver corroborated with the optional writable protocol.
    :param object_address: Concrete owned address required to round-trip canonically.
    :param source: Borrowed binary stream consumed from its current position; not closed here.
    :param mode: Requested collision policy used for capability checks and begin_write.
    :param expected_size: Optional expected byte length checked by staged commit.
    :param expected_digest: Optional expected content digest checked by staged commit.
    :param metadata: Native pairs requiring write_metadata support when nonempty.
    :param chunk_size: Positive number of bytes requested per source read.
    :return: Committed metadata after canonical destination/digest-capability validation.
    """
    if chunk_size < 1:
        raise ValueError("chunk_size must be at least one byte.")
    if expected_size is not None and expected_size < 0:
        raise ValueError("expected_size must not be negative.")
    checked = driver.require_canonical_object_address(object_address)
    writable = _require_writable_driver(driver, mode)
    if metadata and not driver.capabilities.write_metadata:
        raise StorageUnsupportedOperation(
            f"{type(driver).__name__} does not support write metadata."
        )
    session = writable.begin_write(
        checked,
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
            write_all(session, chunk)
        return _require_result_address(
            driver,
            checked,
            session.commit(),
            operation="write commit",
        )


def write_object_bytes(
    driver: ReadableStorageDriverAPI[_DestinationAddressT],
    object_address: _DestinationAddressT,
    data: bytes,
    *,
    mode: WriteMode = WriteMode.CREATE_ONLY,
    expected_digest: Digest | None = None,
    metadata: tuple[tuple[str, str], ...] = (),
) -> DriverObjectInfo[_DestinationAddressT]:
    """
    Write a small in-memory payload with an exact size expectation.

    Example:
        >>> info = write_object_bytes(driver, address, b"book")  # doctest: +SKIP


    :param driver: Destination driver supporting the selected staged-write mode.
    :param object_address: Concrete owned address required to round-trip canonically.
    :param data: In-memory bytes wrapped by BytesIO.
    :param mode: Destination collision policy.
    :param expected_digest: Optional commit-time integrity expectation.
    :param metadata: Native metadata forwarded to put_object.
    :return: Validated committed metadata with len(data) used as the exact size expectation.
    """
    import io

    return put_object(
        driver,
        object_address,
        io.BytesIO(data),
        mode=mode,
        expected_size=len(data),
        expected_digest=expected_digest,
        metadata=metadata,
    )


def iter_object_addresses(
    driver: ReadableStorageDriverAPI[_SourceAddressT],
    *,
    prefix: _SourceAddressT | None = None,
) -> Iterator[_SourceAddressT]:
    """
    Yield addresses from the optional rich inventory protocol.

    The generator checks capability/protocol/prefix support on iteration, tracks every yielded
    address in a growing set, and rejects duplicates after any earlier yields. It does not validate
    size or digest hints and does not strengthen the backend completeness/snapshot declaration.

    Example:
        >>> addresses = list(iter_object_addresses(driver))  # doctest: +SKIP


    :param driver: Readable driver requiring advertised enumeration and its structural protocol.
    :param prefix: Optional canonical owned prefix requiring prefix_enumeration support.
    :return: Lazy iterator of canonical unique addresses; errors can arise after earlier addresses were yielded.
    """
    if driver.capabilities.enumeration is EnumerationCompleteness.UNAVAILABLE:
        raise StorageUnsupportedOperation(
            f"{type(driver).__name__} does not provide object enumeration."
        )
    if not isinstance(driver, EnumerableStorageDriverAPI):
        raise StorageUnsupportedOperation(
            "driver advertises enumeration but does not implement "
            + "iter_inventory()."
        )
    enumerable = cast(EnumerableStorageDriverAPI[_SourceAddressT], driver)
    if prefix is not None and not driver.capabilities.prefix_enumeration:
        raise StorageUnsupportedOperation(
            f"{type(driver).__name__} does not support prefix enumeration."
        )
    checked_prefix = (
        None
        if prefix is None
        else driver.require_canonical_object_address(prefix)
    )
    seen: set[DriverObjectAddress] = set()
    for entry in enumerable.iter_inventory(prefix=checked_prefix):
        address = driver.require_canonical_object_address(
            entry.object_address
        )
        if address in seen:
            raise StorageIntegrityError(
                "driver enumeration returned a duplicate object address."
            )
        seen.add(address)
        yield address


def transfer_between_drivers(
    source_driver: ReadableStorageDriverAPI[_SourceAddressT],
    source_address: _SourceAddressT,
    destination_driver: ReadableStorageDriverAPI[_DestinationAddressT],
    destination_address: _DestinationAddressT,
    *,
    mode: WriteMode = WriteMode.CREATE_ONLY,
    destination_metadata: tuple[tuple[str, str], ...] = (),
    chunk_size: int = DEFAULT_STORAGE_CHUNK_SIZE,
) -> DriverObjectInfo[_DestinationAddressT]:
    """
    Copy one verified object between arbitrary reusable drivers.

    A same-instance native copy is used only when advertised. Otherwise bytes flow through
    ``open_read`` and a staged destination write with expected size and any authoritative source
    digest. Backend-native source metadata is not copied implicitly; callers may provide
    deliberately translated ``destination_metadata``.

    Corroborate native_copy when the two driver objects are identical, then validate its result; do
    not fall back after native failure. This native call does not receive destination_metadata or
    chunk_size. The generic path stats once and reads without an if_version token, so consistency
    relies on any known size/digest checks rather than a pinned snapshot.

    Example:
        >>> result = transfer_between_drivers(  # doctest: +SKIP
        ...     source_driver, source, destination_driver, destination,
        ... )


    :param source_driver: Readable driver owning the source address.
    :param source_address: Source identity checked within source_driver scope.
    :param destination_driver: Driver owning the destination and supporting the requested write mode.
    :param destination_address: Destination identity checked within destination_driver scope.
    :param mode: Explicit WriteMode collision policy, defaulting to CREATE_ONLY.
    :param destination_metadata: Explicit native pairs forwarded only on the generic streamed path.
    :param chunk_size: Positive generic-transfer read chunk size in bytes; unused by native acceleration.
    :return: Destination metadata validated after native copy or generic staged publication.
    """
    source_address = source_driver.require_canonical_object_address(
        source_address
    )
    destination_address = destination_driver.require_canonical_object_address(
        destination_address
    )
    _ = _require_writable_driver(destination_driver, mode)
    if (
        source_driver is destination_driver
        and source_driver.capabilities.native_copy
    ):
        if not isinstance(source_driver, NativeCopyStorageDriverAPI):
            raise StorageUnsupportedOperation(
                "driver advertises native_copy but does not implement "
                + "native_copy()."
            )
        native = cast(
            NativeCopyStorageDriverAPI[_DestinationAddressT], source_driver
        )
        return _require_result_address(
            destination_driver,
            destination_address,
            native.native_copy(
                cast(_DestinationAddressT, source_address),
                destination_address,
                mode=mode,
            ),
            operation="native copy",
        )
    source_info = _require_result_address(
        source_driver,
        source_address,
        source_driver.stat(source_address),
        operation="stat",
    )
    with source_driver.open_read(source_address) as source:
        return put_object(
            destination_driver,
            destination_address,
            source,
            mode=mode,
            expected_size=source_info.size,
            expected_digest=source_info.digest,
            metadata=destination_metadata,
            chunk_size=chunk_size,
        )


def move_between_drivers(
    source_driver: ReadableStorageDriverAPI[_SourceAddressT],
    source_address: _SourceAddressT,
    destination_driver: ReadableStorageDriverAPI[_DestinationAddressT],
    destination_address: _DestinationAddressT,
    *,
    mode: WriteMode = WriteMode.CREATE_ONLY,
    chunk_size: int = DEFAULT_STORAGE_CHUNK_SIZE,
) -> DriverObjectInfo[_DestinationAddressT]:
    """
    Perform verified transfer followed by conditional source deletion.

    Stat the source before selecting native or fallback behavior. The native path forwards any
    observed source version and trusts its own write/collision enforcement. The fallback refuses
    deletion without both conditional_delete support and a non-None source version, then transfers
    and deletes using that original token. Deletion failure can leave both complete copies; the
    destination is not rolled back.

    Example:
        >>> result = move_between_drivers(  # doctest: +SKIP
        ...     source_driver, source, destination_driver, destination,
        ... )


    :param source_driver: Readable driver owning the source address.
    :param source_address: Source identity checked within source_driver scope.
    :param destination_driver: Driver owning the destination and supporting the requested write mode.
    :param destination_address: Destination identity checked within destination_driver scope.
    :param mode: Explicit WriteMode collision policy, defaulting to CREATE_ONLY.
    :param chunk_size: Positive generic-transfer read chunk size in bytes; unused by native acceleration.
    :return: Validated destination metadata after native move or successful conditional source deletion.
    """
    source_address = source_driver.require_canonical_object_address(
        source_address
    )
    destination_address = destination_driver.require_canonical_object_address(
        destination_address
    )
    source_info = _require_result_address(
        source_driver,
        source_address,
        source_driver.stat(source_address),
        operation="stat",
    )
    if (
        source_driver is destination_driver
        and source_driver.capabilities.native_move
    ):
        if not isinstance(source_driver, NativeMoveStorageDriverAPI):
            raise StorageUnsupportedOperation(
                "driver advertises native_move but does not implement "
                + "native_move()."
            )
        native = cast(
            NativeMoveStorageDriverAPI[_DestinationAddressT], source_driver
        )
        return _require_result_address(
            destination_driver,
            destination_address,
            native.native_move(
                cast(_DestinationAddressT, source_address),
                destination_address,
                mode=mode,
                if_source_version=source_info.version,
            ),
            operation="native move",
        )
    if not source_driver.capabilities.delete or not isinstance(
        source_driver, DeletableStorageDriverAPI
    ):
        raise StorageUnsupportedOperation(
            f"{type(source_driver).__name__} does not support deletion."
        )
    if not source_driver.capabilities.conditional_delete:
        raise StorageUnsupportedOperation(
            "safe fallback move requires conditional deletion."
        )
    if source_info.version is None:
        raise StorageUnsupportedOperation(
            "safe fallback move requires a source version for conditional "
            + "deletion."
        )
    result = transfer_between_drivers(
        source_driver,
        source_address,
        destination_driver,
        destination_address,
        mode=mode,
        chunk_size=chunk_size,
    )
    deletable = cast(
        DeletableStorageDriverAPI[_SourceAddressT], source_driver
    )
    deletable.delete(source_address, if_version=source_info.version)
    return result


@contextlib.contextmanager
def materialize_object(
    driver: ReadableStorageDriverAPI[_SourceAddressT],
    object_address: _SourceAddressT,
    *,
    entry: DriverInventoryEntry[_SourceAddressT] | None = None,
    suggested_filename: str | None = None,
    chunk_size: int = DEFAULT_STORAGE_CHUNK_SIZE,
) -> Generator[Path, None, None]:
    """
    Yield a verified temporary local file and remove it on context exit.

    This is the narrow adapter for importers or metadata readers that still require a filesystem
    path. The temporary path is never an object address and carries no placement or bibliographic
    policy.

    Use fresh stat size/digest for checks, not the inventory entry observations. Choose a suffix
    from the explicit name, then entry hints, then stat hints; the remaining temporary name is
    generated. Reads are unversioned, and no total byte ceiling is imposed. Cleanup suppresses only
    FileNotFoundError. Digest-constructor failure occurs after temporary creation but before the
    cleanup try, so that early failure is outside this cleanup guarantee.

    Example:
        >>> with materialize_object(driver, address) as local_path:  # doctest: +SKIP
        ...     metadata = legacy_reader(local_path)


    :param driver: Readable driver supplying fresh stat metadata and an unversioned read stream.
    :param object_address: Concrete owned address required to round-trip canonically.
    :param entry: Optional inventory entry required to identify the same address; used for naming hints only.
    :param suggested_filename: Optional name hint whose local Path suffix overrides entry/stat hints.
    :param chunk_size: Positive read chunk size in bytes.
    :return: Context manager yielding a verified temporary Path and attempting deletion when its managed body exits.
    """
    if chunk_size < 1:
        raise ValueError("chunk_size must be at least one byte.")
    object_address = driver.require_canonical_object_address(object_address)
    if entry is not None:
        _ = driver.require_canonical_object_address(entry.object_address)
        if entry.object_address != object_address:
            raise ValueError("entry does not describe object_address.")
    info = driver.stat(object_address)
    info = _require_result_address(
        driver,
        object_address,
        info,
        operation="stat",
    )
    filename = suggested_filename
    if filename is None and entry is not None:
        filename = entry.hints.suggested_filename
    if filename is None:
        filename = info.hints.suggested_filename
    suffix = "" if filename is None else Path(filename).suffix
    temporary = tempfile.NamedTemporaryFile(
        mode="wb", suffix=suffix, prefix="liuxin-storage-", delete=False
    )
    path = Path(temporary.name)
    observed_size = 0
    expected_digest = info.digest
    digest = (
        None
        if expected_digest is None
        else hashlib.new(expected_digest.algorithm)
    )
    try:
        with temporary, driver.open_read(object_address) as source:
            while True:
                chunk = source.read(chunk_size)
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    raise TypeError("driver read stream must return bytes.")
                _ = temporary.write(chunk)
                observed_size += len(chunk)
                if digest is not None:
                    digest.update(chunk)
        if info.size is not None and observed_size != info.size:
            raise StorageIntegrityError(
                f"materialized size {observed_size} does not match {info.size}."
            )
        if (
            digest is not None
            and expected_digest is not None
            and digest.hexdigest() != expected_digest.value
        ):
            raise StorageIntegrityError(
                f"materialized {expected_digest.algorithm} digest does not match."
            )
        yield path
    finally:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass


def _require_writable_driver(
    driver: ReadableStorageDriverAPI[_DestinationAddressT],
    mode: WriteMode,
) -> WritableStorageDriverAPI[_DestinationAddressT]:
    """
    Validate capability flags and structural staged-write support.

    This checks static mechanics and protocol shape, not dynamic status. It performs no mode
    normalization, address validation, connection, or publication.

    Example:
        >>> writable = _require_writable_driver(driver, WriteMode.CREATE_ONLY)  # doctest: +SKIP


    :param driver: Driver whose capability flags and begin_write protocol are checked.
    :param mode: WriteMode selecting create, replace, or both capabilities for UPSERT.
    :return: Same driver cast to the writable protocol; unsupported capability/shape raises StorageUnsupportedOperation.
    """
    capabilities = driver.capabilities
    supported = {
        WriteMode.CREATE_ONLY: capabilities.create,
        WriteMode.REPLACE: capabilities.replace,
        WriteMode.UPSERT: capabilities.create and capabilities.replace,
    }[mode]
    if not supported:
        raise StorageUnsupportedOperation(
            f"{type(driver).__name__} does not support {mode.value} writes."
        )
    if not isinstance(driver, WritableStorageDriverAPI):
        raise StorageUnsupportedOperation(
            "driver advertises writes but does not implement begin_write()."
        )
    return cast(WritableStorageDriverAPI[_DestinationAddressT], driver)


def _require_result_address(
    driver: ReadableStorageDriverAPI[_DestinationAddressT],
    expected_address: _DestinationAddressT,
    info: DriverObjectInfo[_DestinationAddressT],
    *,
    operation: str,
) -> DriverObjectInfo[_DestinationAddressT]:
    """
    Require driver metadata to describe exactly the requested object.

    Every StorageIntegrityError from require_object_info is relabeled as another-address metadata,
    including an unadvertised stat digest; the original cause remains chained. Other error types
    propagate unchanged.

    Example:
        >>> checked = _require_result_address(  # doctest: +SKIP
        ...     driver, address, info, operation="stat",
        ... )


    :param driver: Driver enforcing metadata address/digest-advertisement checks.
    :param expected_address: Requested canonical address that returned metadata must describe.
    :param info: Operation result to validate.
    :param operation: Operation label inserted into the replacement integrity-error message.
    :return: Original validated metadata; StorageIntegrityError is re-raised with operation context and chained cause.
    """
    try:
        return driver.require_object_info(expected_address, info)
    except StorageIntegrityError as exc:
        raise StorageIntegrityError(
            f"driver {operation} returned metadata for another address."
        ) from exc


__all__ = [
    "iter_object_addresses",
    "materialize_object",
    "move_between_drivers",
    "put_object",
    "transfer_between_drivers",
    "write_all",
    "write_object_bytes",
]
