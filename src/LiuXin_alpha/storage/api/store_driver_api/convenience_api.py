"""
Adapt familiar file inputs and identifiers to one raw driver's core protocols.

Typed addresses, persisted strings, and returned metadata resolve through the
driver parser. Bytes, borrowed streams, and local paths use staged publication;
omitting a target requires both allocation capability and its structural protocol.
Metadata stays native key/value text rather than catalogue or placement policy.

Read helpers close only streams they open. Stream writes borrow the caller input;
local-file writes own their opened handle. Destination allocation can precede
metadata/mode validation, so a rejected request need not be side-effect-free.

Example:
    >>> info = driver.store_bytes(b"book", object_address="incoming/book.epub")  # doctest: +SKIP
"""

from __future__ import annotations

import os

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import BinaryIO, Generic, TypeAlias, TypeVar, cast

from LiuXin_alpha.storage.api.errors import (
    StorageIntegrityError,
    StorageUnsupportedOperation,
)
from LiuXin_alpha.storage.api.models import Digest, WriteMode
from LiuXin_alpha.storage.api.store_driver_api.models import (
    DriverObjectAddress,
    DriverObjectAddressInput,
    DriverObjectAddressT,
    DriverObjectHints,
    DriverObjectInfo,
)
from LiuXin_alpha.storage.api.store_driver_api.object_address_api import (
    StorageDriverObjectAddressAPI,
)
from LiuXin_alpha.storage.api.store_driver_api.optional_api import (
    DeletableStorageDriverAPI,
    ObjectAddressAllocatorStorageDriverAPI,
)
from LiuXin_alpha.storage.api.store_driver_api.readable_api import (
    ReadableStorageDriverAPI,
)
from LiuXin_alpha.storage.utils.streams import bytes_stream


StorageDriverSource: TypeAlias = (
    bytes | bytearray | memoryview | BinaryIO | str | os.PathLike[str]
)
DriverNativeMetadata: TypeAlias = (
    Mapping[str, str] | Iterable[tuple[str, str]]
)
_DriverFileAddressT = TypeVar(
    "_DriverFileAddressT",
    bound=DriverObjectAddress,
)
DriverFileIdentifier: TypeAlias = (
    DriverObjectAddressInput[_DriverFileAddressT]
    | DriverObjectInfo[_DriverFileAddressT]
)


class StorageDriverConvenienceBase(Generic[DriverObjectAddressT]):
    """
    Familiar file operations layered over optional driver protocols.

    An explicit address may be supplied as its typed value or persisted string. Omitting it asks a
    driver advertising object-address allocation to choose one. Native string metadata stays
    Store-neutral and backend-facing.

    Example:
        >>> info = driver.store_bytes(  # doctest: +SKIP
        ...     b"book", name="book.epub",
        ...     metadata={"content-type": "application/epub+zip"},
        ... )
    """

    def open_file(
        self,
        identifier: DriverFileIdentifier[DriverObjectAddressT],
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Open a driver object as a read-only binary stream.

        This method never opens the object for mutation and accepts no write mode. Use ``store()``,
        ``store_stream()``, or a supported ``begin_write()`` session for staged, commit-based
        writes. Close the returned stream, preferably by using it as a context manager.

        A content-addressed driver may use a hash as its persisted string, but this method does not
        invent reverse digest lookup for other drivers.

        A supplied DriverObjectInfo contributes only its address; its version is not automatically
        pinned. Runtime casts do not check the mixin host: concrete composition must supply the
        readable and address APIs.

        Example:
            >>> with driver.open_file(  # doctest: +SKIP
            ...     "objects/sha256/abcd",
            ... ) as source:
            ...     payload = source.read()


        :param identifier: Typed address, persisted relative string, or DriverObjectInfo contributing only its address.
        :param offset: Nonnegative byte offset forwarded to the driver.
        :param length: Optional maximum byte count; None requests the rest of the object.
        :param if_version: Optional opaque version requirement forwarded to the read operation.
        :return: Caller-owned read-only binary stream returned by reader.get; close it to release resources.
        """

        reader = cast(
            ReadableStorageDriverAPI[DriverObjectAddressT],
            cast(object, self),
        )
        address_api = cast(
            StorageDriverObjectAddressAPI[DriverObjectAddressT],
            cast(object, self),
        )
        return reader.get(
            _driver_file_address(address_api, identifier),
            offset=offset,
            length=length,
            if_version=if_version,
        )

    def get_file(
        self,
        identifier: DriverFileIdentifier[DriverObjectAddressT],
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Return the read-only ``open_file`` stream using familiar vocabulary.

        Example:
            >>> with driver.get_file("objects/42") as source:  # doctest: +SKIP
            ...     payload = source.read()


        :param identifier: Typed address, persisted relative string, or DriverObjectInfo contributing only its address.
        :param offset: Nonnegative byte offset forwarded to the driver.
        :param length: Optional maximum byte count; None requests the rest of the object.
        :param if_version: Optional opaque version requirement forwarded to the read operation.
        :return: The open_file stream, with ownership and errors unchanged.
        """

        return self.open_file(
            identifier,
            offset=offset,
            length=length,
            if_version=if_version,
        )

    def read_file(
        self,
        identifier: DriverFileIdentifier[DriverObjectAddressT],
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> bytes:
        """
        Read an object by typed address, persisted string, or returned info.

        Materialize the whole selected stream without an independent memory cap or bytes-type check
        here. The underlying driver owns range/version and binary-stream enforcement.

        Example:
            >>> driver.read_file("objects/42", length=4)  # doctest: +SKIP
            b'book'


        :param identifier: Typed address, persisted relative string, or DriverObjectInfo contributing only its address.
        :param offset: Nonnegative byte offset forwarded to the driver.
        :param length: Optional maximum byte count; None requests the rest of the object.
        :param if_version: Optional opaque version requirement forwarded to the read operation.
        :return: Result of source.read() after closing the stream; the binary-stream contract supplies its bytes type.
        """

        with self.open_file(
            identifier,
            offset=offset,
            length=length,
            if_version=if_version,
        ) as source:
            return source.read()

    def stat_file(
        self,
        identifier: DriverFileIdentifier[DriverObjectAddressT],
    ) -> DriverObjectInfo[DriverObjectAddressT]:
        """
        Return current object information from any ordinary identifier.

        A supplied ``DriverObjectInfo`` contributes its address; fresh information is still
        requested from the driver.

        Example:
            >>> current = driver.stat_file(stored)  # doctest: +SKIP


        :param identifier: Typed address, persisted relative string, or DriverObjectInfo contributing only its address.
        :return: Fresh stat result after canonical address/digest-capability validation; supplied metadata is not reused as current state.
        """

        reader = cast(
            ReadableStorageDriverAPI[DriverObjectAddressT],
            cast(object, self),
        )
        address_api = cast(
            StorageDriverObjectAddressAPI[DriverObjectAddressT],
            cast(object, self),
        )
        address = _driver_file_address(address_api, identifier)
        return reader.require_object_info(address, reader.stat(address))

    def file_exists(
        self,
        identifier: DriverFileIdentifier[DriverObjectAddressT],
    ) -> bool:
        """
        Test whether a driver object exists from any ordinary identifier.

        Identifier parsing occurs before the readable exists helper, and parsing/ownership failures
        remain visible.

        Example:
            >>> driver.file_exists("objects/42")  # doctest: +SKIP
            True


        :param identifier: Typed address, persisted relative string, or DriverObjectInfo contributing only its address.
        :return: Whether the readable driver finds valid metadata, hiding only its documented not-found result.
        """

        reader = cast(
            ReadableStorageDriverAPI[DriverObjectAddressT],
            cast(object, self),
        )
        address_api = cast(
            StorageDriverObjectAddressAPI[DriverObjectAddressT],
            cast(object, self),
        )
        return reader.exists(_driver_file_address(address_api, identifier))

    def delete_file(
        self,
        identifier: DriverFileIdentifier[DriverObjectAddressT],
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Delete a driver object from a string, typed address, or returned info.

        Drivers without advertised deletion support raise ``StorageUnsupportedOperation``.

        The wrapper checks delete plus protocol shape before parsing the identifier, then forwards
        if_version. The concrete deleter owns conditional capability/version enforcement.

        Example:
            >>> driver.delete_file(stored, missing_ok=True)  # doctest: +SKIP


        :param identifier: Typed address, persisted relative string, or DriverObjectInfo contributing only its address.
        :param missing_ok: Whether the concrete deleter may suppress genuine absence.
        :param if_version: Optional opaque version requirement forwarded to deletion.
        :return: None after delegated deletion; missing capability/protocol support raises StorageUnsupportedOperation.
        """

        reader = cast(
            ReadableStorageDriverAPI[DriverObjectAddressT],
            cast(object, self),
        )
        if (
            not reader.capabilities.delete
            or not isinstance(reader, DeletableStorageDriverAPI)
        ):
            raise StorageUnsupportedOperation(
                f"{type(reader).__name__} does not support deletion."
            )
        address_api = cast(
            StorageDriverObjectAddressAPI[DriverObjectAddressT],
            cast(object, self),
        )
        deleter = cast(
            DeletableStorageDriverAPI[DriverObjectAddressT],
            reader,
        )
        deleter.delete(
            _driver_file_address(address_api, identifier),
            missing_ok=missing_ok,
            if_version=if_version,
        )

    def store(
        self,
        source: StorageDriverSource,
        *,
        object_address: (
            DriverObjectAddressInput[DriverObjectAddressT] | None
        ) = None,
        name: str | None = None,
        metadata: DriverNativeMetadata = (),
        write_mode: WriteMode | str | None = None,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        mode: WriteMode | str | None = None,
    ) -> DriverObjectInfo[DriverObjectAddressT]:
        """
        Store bytes, a binary stream, or a local file through one driver.

        Convert bytes/bytearray/memoryview to bytes and compare any size expectation before
        dispatch. Strings and path-like values select local-file handling. Other inputs only require
        a read attribute here; stream result validation occurs during transfer.

        Example:
            >>> info = driver.store(  # doctest: +SKIP
            ...     b"cover", name="cover.jpg",
            ... )


        :param source: Bytes-like payload, local path, or caller-owned object exposing read.
        :param object_address: Explicit typed/persisted target, or None to request advertised backend allocation.
        :param name: Optional allocation name hint; ignored when an explicit address is supplied.
        :param metadata: Native key/value mapping or pair iterable, materialized and checked for unique keys.
        :param write_mode: Preferred collision mode enum/string; None uses mode or CREATE_ONLY.
        :param expected_size: Optional expected logical byte length to check before publication.
        :param expected_digest: Optional expected content digest to check before publication.
        :param mode: Compatibility collision-mode alias; supplying both mode names raises TypeError.
        :return: Committed destination metadata from the selected bytes/file/stream path.
        """

        if isinstance(source, (bytes, bytearray, memoryview)):
            data = bytes(source)
            if expected_size is not None and expected_size != len(data):
                raise StorageIntegrityError(
                    f"expected {expected_size} bytes, received {len(data)}."
                )
            return self.store_bytes(
                data,
                object_address=object_address,
                name=name,
                metadata=metadata,
                write_mode=write_mode,
                expected_digest=expected_digest,
                mode=mode,
            )
        if isinstance(source, (str, os.PathLike)):
            return self.store_file(
                source,
                object_address=object_address,
                name=name,
                metadata=metadata,
                write_mode=write_mode,
                expected_size=expected_size,
                expected_digest=expected_digest,
                mode=mode,
            )
        if not hasattr(source, "read"):
            raise TypeError(
                "source must be bytes, a binary stream, or a local path."
            )
        return self.store_stream(
            source,
            object_address=object_address,
            name=name,
            metadata=metadata,
            write_mode=write_mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
            mode=mode,
        )

    def store_bytes(
        self,
        data: bytes,
        *,
        object_address: (
            DriverObjectAddressInput[DriverObjectAddressT] | None
        ) = None,
        name: str | None = None,
        metadata: DriverNativeMetadata = (),
        write_mode: WriteMode | str | None = None,
        expected_digest: Digest | None = None,
        mode: WriteMode | str | None = None,
    ) -> DriverObjectInfo[DriverObjectAddressT]:
        """
        Store a small payload without constructing a driver address first.

        The buffer is fully in memory. This delegates to store_stream with an exact len(data)
        expectation; any supplied name affects allocation only.

        Example:
            >>> info = driver.store_bytes(  # doctest: +SKIP
            ...     b"book", object_address="incoming/book.epub",
            ... )


        :param data: Small in-memory payload wrapped by BytesIO; its length becomes the exact byte expectation.
        :param object_address: Explicit typed/persisted target, or None to request advertised backend allocation.
        :param name: Optional allocation name hint; ignored when an explicit address is supplied.
        :param metadata: Native key/value mapping or pair iterable, materialized and checked for unique keys.
        :param write_mode: Preferred collision mode enum/string; None uses mode or CREATE_ONLY.
        :param expected_digest: Optional expected content digest to check before publication.
        :param mode: Compatibility collision-mode alias; supplying both mode names raises TypeError.
        :return: Committed destination metadata after streaming the in-memory buffer.
        """

        return self.store_stream(
            bytes_stream(data),
            object_address=object_address,
            name=name,
            metadata=metadata,
            write_mode=write_mode,
            expected_size=len(data),
            expected_digest=expected_digest,
            mode=mode,
        )

    def store_stream(
        self,
        source: BinaryIO,
        *,
        object_address: (
            DriverObjectAddressInput[DriverObjectAddressT] | None
        ) = None,
        name: str | None = None,
        metadata: DriverNativeMetadata = (),
        write_mode: WriteMode | str | None = None,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        mode: WriteMode | str | None = None,
    ) -> DriverObjectInfo[DriverObjectAddressT]:
        """
        Stream bytes to an explicit or driver-allocated object address.

        Resolve or allocate the destination before validating metadata or selecting the write mode.
        Then put_object checks staged-write support, streams with partial-write handling, and
        validates committed metadata. This helper does not rewind or close the source, and
        post-commit validation errors can follow publication.

        Example:
            >>> info = driver.store_stream(  # doctest: +SKIP
            ...     source, expected_size=4, name="book.epub",
            ... )


        :param source: Borrowed binary stream consumed from its current position; it is not closed by this helper.
        :param object_address: Explicit typed/persisted target, or None to request advertised backend allocation.
        :param name: Optional allocation name hint; ignored when an explicit address is supplied.
        :param metadata: Native key/value mapping or pair iterable, materialized and checked for unique keys.
        :param write_mode: Preferred collision mode enum/string; None uses mode or CREATE_ONLY.
        :param expected_size: Optional expected logical byte length to check before publication.
        :param expected_digest: Optional expected content digest to check before publication.
        :param mode: Compatibility collision-mode alias; supplying both mode names raises TypeError.
        :return: Committed metadata validated by put_object against the chosen canonical destination.
        """

        reader = cast(
            ReadableStorageDriverAPI[DriverObjectAddressT],
            cast(object, self),
        )
        address_api = cast(
            StorageDriverObjectAddressAPI[DriverObjectAddressT],
            cast(object, self),
        )
        destination = _driver_object_address(
            address_api,
            reader,
            object_address,
            name=name,
            expected_size=expected_size,
            expected_digest=expected_digest,
        )
        native_metadata = _native_metadata(metadata)

        from LiuXin_alpha.storage.utils.driver import put_object

        return cast(
            DriverObjectInfo[DriverObjectAddressT],
            put_object(
                reader,
                destination,
                source,
                mode=_write_mode_argument(write_mode, mode),
                expected_size=expected_size,
                expected_digest=expected_digest,
                metadata=native_metadata,
            ),
        )

    def store_file(
        self,
        path: str | os.PathLike[str],
        *,
        object_address: (
            DriverObjectAddressInput[DriverObjectAddressT] | None
        ) = None,
        name: str | None = None,
        metadata: DriverNativeMetadata = (),
        write_mode: WriteMode | str | None = None,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        mode: WriteMode | str | None = None,
    ) -> DriverObjectInfo[DriverObjectAddressT]:
        """
        Store one local file and use its filename as the allocation hint.

        Stat the path first and compare a supplied size before opening it. Use the observed size as
        the transfer expectation; stat/open/read are separate observations and the file is not
        locked against changes.

        Example:
            >>> info = driver.store_file(  # doctest: +SKIP
            ...     "/incoming/book.epub",
            ... )


        :param path: Local path opened with Path directly, without expanduser or resolve.
        :param object_address: Explicit typed/persisted target, or None to request advertised backend allocation.
        :param name: Allocation hint; None uses the local basename, while explicit text is retained.
        :param metadata: Native key/value mapping or pair iterable, materialized and checked for unique keys.
        :param write_mode: Preferred collision mode enum/string; None uses mode or CREATE_ONLY.
        :param expected_size: Optional expected logical byte length to check before publication.
        :param expected_digest: Optional expected content digest to check before publication.
        :param mode: Compatibility collision-mode alias; supplying both mode names raises TypeError.
        :return: Committed destination metadata after closing the local source handle.
        """

        source_path = Path(path)
        observed_size = source_path.stat().st_size
        if expected_size is not None and expected_size != observed_size:
            raise StorageIntegrityError(
                f"expected {expected_size} bytes, found {observed_size}."
            )
        with source_path.open("rb") as source:
            return self.store_stream(
                source,
                object_address=object_address,
                name=source_path.name if name is None else name,
                metadata=metadata,
                write_mode=write_mode,
                expected_size=observed_size,
                expected_digest=expected_digest,
                mode=mode,
            )


class StorageDriverConvenienceAPI(
    StorageDriverConvenienceBase[DriverObjectAddressT],
    Generic[DriverObjectAddressT],
):
    """
    Preserve the original public convenience-mixin name over its implementation base.

    New composition code may name ``StorageDriverConvenienceBase`` to make the implementation role
    explicit. Existing subclasses and imports keep the API name without behavioral change.
    """


def _native_metadata(
    metadata: DriverNativeMetadata,
) -> tuple[tuple[str, str], ...]:
    """
    Normalize native metadata mappings or pairs and validate their keys.

    Mapping iteration order is retained; pair iterables are consumed once. Validation rejects
    duplicate keys but does not validate string types, blank keys, or backend metadata support.

    Example:
        >>> _native_metadata({"content-type": "text/plain"})
        (('content-type', 'text/plain'),)


    :param metadata: Mapping or ordered iterable of native key/value pairs.
    :return: Materialized tuple after duplicate-key checks; values are not coerced to strings.
    """

    if isinstance(metadata, Mapping):
        metadata_mapping = cast(Mapping[str, str], metadata)
        normalized = tuple(metadata_mapping.items())
    else:
        normalized = tuple(metadata)
    return DriverObjectHints(metadata=normalized).metadata


def _driver_file_address(
    address_api: StorageDriverObjectAddressAPI[DriverObjectAddressT],
    identifier: DriverFileIdentifier[DriverObjectAddressT],
) -> DriverObjectAddressT:
    """
    Resolve an ordinary driver-file identifier to a checked address.

    The parser owns validation. The helper adds no stat, URI resolution, or reverse lookup by
    digest.

    Example:
        >>> address = _driver_file_address(driver, stored)  # doctest: +SKIP


    :param address_api: Driver address parser responsible for scope and canonical form.
    :param identifier: Typed address, persisted relative string, or DriverObjectInfo contributing only its address.
    :return: Parsed address; a DriverObjectInfo contributes only object_address, not a pinned version.
    """

    if isinstance(identifier, DriverObjectInfo):
        return address_api.parse_object_address(identifier.object_address)
    return address_api.parse_object_address(identifier)


def _driver_object_address(
    address_api: StorageDriverObjectAddressAPI[DriverObjectAddressT],
    reader: ReadableStorageDriverAPI[DriverObjectAddressT],
    object_address: DriverObjectAddressInput[DriverObjectAddressT] | None,
    *,
    name: str | None,
    expected_size: int | None,
    expected_digest: Digest | None,
) -> DriverObjectAddressT:
    """
    Parse an ordinary address or ask an advertised allocator for one.

    An explicit address bypasses capability inspection and ignores allocation hints. Otherwise call
    the advertised allocator and require its result to round-trip canonically. No byte publication
    or rollback of allocator effects occurs here.

    Example:
        >>> address = _driver_object_address(  # doctest: +SKIP
        ...     driver, driver, "incoming/book.epub", name=None,
        ...     expected_size=4, expected_digest=None,
        ... )


    :param address_api: Parser/canonical checker for the same configured driver.
    :param reader: Driver whose capability and allocator protocol corroborate allocation support.
    :param object_address: Explicit typed/persisted target, or None to request advertised backend allocation.
    :param name: Optional name hint used only when allocating.
    :param expected_size: Optional expected logical byte length to check before publication.
    :param expected_digest: Optional expected content digest to check before publication.
    :return: Parsed explicit address or canonical-checked allocator result; absent support raises StorageUnsupportedOperation.
    """

    if object_address is not None:
        return address_api.parse_object_address(object_address)
    if (
        not reader.capabilities.object_address_allocation
        or not isinstance(reader, ObjectAddressAllocatorStorageDriverAPI)
    ):
        raise StorageUnsupportedOperation(
            f"{type(reader).__name__} does not allocate object addresses; "
            + "supply object_address explicitly."
        )
    allocator = cast(
        ObjectAddressAllocatorStorageDriverAPI[DriverObjectAddressT],
        reader,
    )
    return address_api.require_canonical_object_address(
        allocator.allocate_object_address(
            expected_size=expected_size,
            expected_digest=expected_digest,
            name_hint=name,
        )
    )


def _write_mode(mode: WriteMode | str) -> WriteMode:
    """
    Normalize a write mode enum or its string value.

    Example:
        >>> _write_mode("create_only") is WriteMode.CREATE_ONLY
        True


    :param mode: WriteMode instance or exact enum value string.
    :return: Existing enum unchanged or WriteMode conversion result; invalid values propagate ValueError/TypeError.
    """

    return mode if isinstance(mode, WriteMode) else WriteMode(mode)


def _write_mode_argument(
    write_mode: WriteMode | str | None,
    mode: WriteMode | str | None,
) -> WriteMode:
    """
    Select the clear write-mode name while retaining the former alias.

    Even equal values supplied under both names are rejected. Strings are not stripped or
    case-normalized.

    Example:
        >>> _write_mode_argument("replace", None) is WriteMode.REPLACE
        True


    :param write_mode: Preferred collision mode enum/string; None uses mode or CREATE_ONLY.
    :param mode: Compatibility collision-mode alias; supplying both mode names raises TypeError.
    :return: Selected normalized WriteMode, defaulting to CREATE_ONLY; dual non-None inputs raise TypeError.
    """

    if write_mode is not None and mode is not None:
        raise TypeError("use write_mode or mode, not both.")
    selected = write_mode if write_mode is not None else mode
    return WriteMode.CREATE_ONLY if selected is None else _write_mode(selected)


__all__ = [
    "DriverFileIdentifier",
    "DriverNativeMetadata",
    "StorageDriverConvenienceBase",
    "StorageDriverConvenienceAPI",
    "StorageDriverSource",
]
