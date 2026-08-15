"""Low-cognitive-overhead file operations for one reusable driver."""

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


class StorageDriverConvenienceAPI(Generic[DriverObjectAddressT]):
    """
    Familiar file operations layered over optional driver protocols.

    An explicit address may be supplied as its typed value or persisted string.
    Omitting it asks a driver advertising object-address allocation to choose
    one. Native string metadata stays Store-neutral and backend-facing.

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
    ) -> BinaryIO:
        """
        Open a driver object as a read-only binary stream.

        This method never opens the object for mutation and accepts no write
        mode. Use ``store()``, ``store_stream()``, or a supported
        ``begin_write()`` session for staged, commit-based writes. Close the
        returned stream, preferably by using it as a context manager.

        A content-addressed driver may use a hash as its persisted string, but
        this method does not invent reverse digest lookup for other drivers.

        Example:
            >>> with driver.open_file(  # doctest: +SKIP
            ...     "objects/sha256/abcd",
            ... ) as source:
            ...     payload = source.read()


        :param identifier:
        :param offset:
        :param length:
        :return:
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
        )

    def get_file(
        self,
        identifier: DriverFileIdentifier[DriverObjectAddressT],
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO:
        """
        Return the read-only ``open_file`` stream using familiar vocabulary.

        Example:
            >>> with driver.get_file("objects/42") as source:  # doctest: +SKIP
            ...     payload = source.read()


        :param identifier:
        :param offset:
        :param length:
        :return:
        """

        return self.open_file(identifier, offset=offset, length=length)

    def read_file(
        self,
        identifier: DriverFileIdentifier[DriverObjectAddressT],
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> bytes:
        """
        Read an object by typed address, persisted string, or returned info.

        Example:
            >>> driver.read_file("objects/42", length=4)  # doctest: +SKIP
            b'book'


        :param identifier:
        :param offset:
        :param length:
        :return:
        """

        with self.open_file(
            identifier,
            offset=offset,
            length=length,
        ) as source:
            return source.read()

    def stat_file(
        self,
        identifier: DriverFileIdentifier[DriverObjectAddressT],
    ) -> DriverObjectInfo[DriverObjectAddressT]:
        """
        Return current object information from any ordinary identifier.

        A supplied ``DriverObjectInfo`` contributes its address; fresh
        information is still requested from the driver.

        Example:
            >>> current = driver.stat_file(stored)  # doctest: +SKIP


        :param identifier:
        :return:
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

        Example:
            >>> driver.file_exists("objects/42")  # doctest: +SKIP
            True


        :param identifier:
        :return:
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

        Drivers without advertised deletion support raise
        ``StorageUnsupportedOperation``.

        Example:
            >>> driver.delete_file(stored, missing_ok=True)  # doctest: +SKIP


        :param identifier:
        :param missing_ok:
        :param if_version:
        :return:
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

        Example:
            >>> info = driver.store(  # doctest: +SKIP
            ...     b"cover", name="cover.jpg",
            ... )


        :param source:
        :param object_address:
        :param name:
        :param metadata:
        :param write_mode:
        :param expected_size:
        :param expected_digest:
        :param mode: Backward-compatible alias for ``write_mode``.
        :return:
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

        Example:
            >>> info = driver.store_bytes(  # doctest: +SKIP
            ...     b"book", object_address="incoming/book.epub",
            ... )


        :param data:
        :param object_address:
        :param name:
        :param metadata:
        :param write_mode:
        :param expected_digest:
        :param mode: Backward-compatible alias for ``write_mode``.
        :return:
        """

        return self.store_stream(
            _bytes_stream(data),
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

        Example:
            >>> info = driver.store_stream(  # doctest: +SKIP
            ...     source, expected_size=4, name="book.epub",
            ... )


        :param source:
        :param object_address:
        :param name:
        :param metadata:
        :param write_mode:
        :param expected_size:
        :param expected_digest:
        :param mode: Backward-compatible alias for ``write_mode``.
        :return:
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

        return put_object(
            reader,
            destination,
            source,
            mode=_write_mode_argument(write_mode, mode),
            expected_size=expected_size,
            expected_digest=expected_digest,
            metadata=native_metadata,
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

        Example:
            >>> info = driver.store_file(  # doctest: +SKIP
            ...     "/incoming/book.epub",
            ... )


        :param path:
        :param object_address:
        :param name:
        :param metadata:
        :param write_mode:
        :param expected_size:
        :param expected_digest:
        :param mode: Backward-compatible alias for ``write_mode``.
        :return:
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


def _bytes_stream(data: bytes) -> BinaryIO:
    """
    Wrap an in-memory payload as a binary stream.

    Example:
        >>> _bytes_stream(b"book").read()
        b'book'
    """

    import io

    return io.BytesIO(data)


def _native_metadata(
    metadata: DriverNativeMetadata,
) -> tuple[tuple[str, str], ...]:
    """
    Normalize native metadata mappings or pairs and validate their keys.

    Example:
        >>> _native_metadata({"content-type": "text/plain"})
        (('content-type', 'text/plain'),)
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

    Example:
        >>> address = _driver_file_address(driver, stored)  # doctest: +SKIP
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

    Example:
        >>> address = _driver_object_address(  # doctest: +SKIP
        ...     driver, driver, "incoming/book.epub", name=None,
        ...     expected_size=4, expected_digest=None,
        ... )
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
    """

    return mode if isinstance(mode, WriteMode) else WriteMode(mode)


def _write_mode_argument(
    write_mode: WriteMode | str | None,
    mode: WriteMode | str | None,
) -> WriteMode:
    """
    Select the clear write-mode name while retaining the former alias.

    Example:
        >>> _write_mode_argument("replace", None) is WriteMode.REPLACE
        True
    """

    if write_mode is not None and mode is not None:
        raise TypeError("use write_mode or mode, not both.")
    selected = write_mode if write_mode is not None else mode
    return WriteMode.CREATE_ONLY if selected is None else _write_mode(selected)


__all__ = [
    "DriverFileIdentifier",
    "DriverNativeMetadata",
    "StorageDriverConvenienceAPI",
    "StorageDriverSource",
]
