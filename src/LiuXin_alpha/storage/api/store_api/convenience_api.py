"""Low-cognitive-overhead file operations for one configured Store."""

from __future__ import annotations

import os

from pathlib import Path
from typing import BinaryIO, TypeAlias, cast

from LiuXin_alpha.storage.api.errors import (
    StoreIntegrityError,
    StoreUnsupportedOperation,
)
from LiuXin_alpha.storage.api.models import Digest, FileInfo, Location, WriteMode
from LiuXin_alpha.storage.api.placement_hints_api import (
    StorageHintSource,
    StoragePlacementHints,
    derive_storage_hints,
)
from LiuXin_alpha.storage.api.store_api.file_api import StoreFileAPI
from LiuXin_alpha.storage.api.store_api.identity_api import StoreIdentityAPI


StoreSource: TypeAlias = (
    bytes | bytearray | memoryview | BinaryIO | str | os.PathLike[str]
)
StoreFileIdentifier: TypeAlias = str | Location | FileInfo


class StoreConvenienceAPI:
    """
    Familiar file operations layered over a configured Store's exact API.

    This mixin owns no state. An omitted Location asks the Store allocator to
    choose one; a string is parsed by the Store; and a Location remains
    available when the caller needs an exact destination.

    Example:
        >>> info = store.store_bytes(  # doctest: +SKIP
        ...     b"book", name="book.epub", metadata=item_metadata,
        ... )
    """

    def open_file(
        self,
        identifier: StoreFileIdentifier,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO:
        """
        Open a Store file as a read-only binary stream.

        This method never opens the destination for mutation and accepts no
        write mode. Use ``store()``, ``store_stream()``, or ``begin_write()``
        for staged, commit-based writes. Close the returned stream, preferably
        by using it as a context manager.

        Store keys may themselves be hashes in a content-addressed Store, but
        a generic Store does not claim a digest reverse index.

        Example:
            >>> with store.open_file("objects/sha256/abcd") as source:  # doctest: +SKIP
            ...     payload = source.read()


        :param identifier:
        :param offset:
        :param length:
        :return:
        """

        identity = cast(StoreIdentityAPI, cast(object, self))
        return cast(StoreFileAPI, cast(object, self)).get(
            _store_file_location(identity, identifier),
            offset=offset,
            length=length,
        )

    def get_file(
        self,
        identifier: StoreFileIdentifier,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO:
        """
        Return the read-only ``open_file`` stream using familiar vocabulary.

        Example:
            >>> with store.get_file("objects/42") as source:  # doctest: +SKIP
            ...     payload = source.read()


        :param identifier:
        :param offset:
        :param length:
        :return:
        """

        return self.open_file(identifier, offset=offset, length=length)

    def read_file(
        self,
        identifier: StoreFileIdentifier,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> bytes:
        """
        Read a Store file by opaque key, Location, or returned FileInfo.

        Example:
            >>> store.read_file("objects/42", length=4)  # doctest: +SKIP
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

    def stat_file(self, identifier: StoreFileIdentifier) -> FileInfo:
        """
        Return current information using a key, Location, or prior FileInfo.

        A supplied ``FileInfo`` identifies the object; this method still asks
        the Store for fresh information rather than returning a stale value.

        Example:
            >>> current = store.stat_file(stored)  # doctest: +SKIP


        :param identifier:
        :return:
        """

        identity = cast(StoreIdentityAPI, cast(object, self))
        return cast(StoreFileAPI, cast(object, self)).stat(
            _store_file_location(identity, identifier)
        )

    def file_exists(self, identifier: StoreFileIdentifier) -> bool:
        """
        Test whether a Store file exists using any ordinary identifier.

        Example:
            >>> store.file_exists("objects/42")  # doctest: +SKIP
            True


        :param identifier:
        :return:
        """

        identity = cast(StoreIdentityAPI, cast(object, self))
        return cast(StoreFileAPI, cast(object, self)).exists(
            _store_file_location(identity, identifier)
        )

    def delete_file(
        self,
        identifier: StoreFileIdentifier,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Delete a Store file using a key, Location, or returned FileInfo.

        Example:
            >>> store.delete_file(stored, missing_ok=True)  # doctest: +SKIP


        :param identifier:
        :param missing_ok:
        :param if_version:
        :return:
        """

        identity = cast(StoreIdentityAPI, cast(object, self))
        cast(StoreFileAPI, cast(object, self)).delete(
            _store_file_location(identity, identifier),
            missing_ok=missing_ok,
            if_version=if_version,
        )

    def store(
        self,
        source: StoreSource,
        *,
        location: str | Location | None = None,
        name: str | None = None,
        metadata: StorageHintSource | None = None,
        write_mode: WriteMode | str | None = None,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        mode: WriteMode | str | None = None,
    ) -> FileInfo:
        """
        Store bytes, a binary stream, or a local file at one Store.

        Example:
            >>> info = store.store(  # doctest: +SKIP
            ...     b"cover", name="cover.jpg",
            ...     metadata={"title": "Permutation City"},
            ... )


        :param source:
        :param location:
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
                raise StoreIntegrityError(
                    f"expected {expected_size} bytes, received {len(data)}."
                )
            return self.store_bytes(
                data,
                location=location,
                name=name,
                metadata=metadata,
                write_mode=write_mode,
                expected_digest=expected_digest,
                mode=mode,
            )
        if isinstance(source, (str, os.PathLike)):
            return self.store_file(
                source,
                location=location,
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
            location=location,
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
        location: str | Location | None = None,
        name: str | None = None,
        metadata: StorageHintSource | None = None,
        write_mode: WriteMode | str | None = None,
        expected_digest: Digest | None = None,
        mode: WriteMode | str | None = None,
    ) -> FileInfo:
        """
        Store a small payload without constructing a Location first.

        Example:
            >>> info = store.store_bytes(  # doctest: +SKIP
            ...     b"book", location="incoming/book.epub",
            ... )


        :param data:
        :param location:
        :param name:
        :param metadata:
        :param write_mode:
        :param expected_digest:
        :param mode: Backward-compatible alias for ``write_mode``.
        :return:
        """

        return self.store_stream(
            _bytes_stream(data),
            location=location,
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
        location: str | Location | None = None,
        name: str | None = None,
        metadata: StorageHintSource | None = None,
        write_mode: WriteMode | str | None = None,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        mode: WriteMode | str | None = None,
    ) -> FileInfo:
        """
        Stream bytes to an explicit or Store-allocated Location.

        Example:
            >>> info = store.store_stream(  # doctest: +SKIP
            ...     source, expected_size=4, name="book.epub",
            ... )


        :param source:
        :param location:
        :param name:
        :param metadata:
        :param write_mode:
        :param expected_size:
        :param expected_digest:
        :param mode: Backward-compatible alias for ``write_mode``.
        :return:
        """

        hints = _placement_hints(metadata)
        identity = cast(StoreIdentityAPI, cast(object, self))
        file_store = cast(StoreFileAPI, cast(object, self))
        destination = _store_location(
            identity,
            location,
            name=name,
            expected_size=expected_size,
            expected_digest=expected_digest,
            placement_hints=(
                hints if file_store.capabilities.placement_hints else None
            ),
        )
        return file_store.put(
            destination,
            source,
            mode=_write_mode_argument(write_mode, mode),
            expected_size=expected_size,
            expected_digest=expected_digest,
            placement_hints=hints,
        )

    def store_file(
        self,
        path: str | os.PathLike[str],
        *,
        location: str | Location | None = None,
        name: str | None = None,
        metadata: StorageHintSource | None = None,
        write_mode: WriteMode | str | None = None,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        mode: WriteMode | str | None = None,
    ) -> FileInfo:
        """
        Store one local file and use its filename as the default name hint.

        Example:
            >>> info = store.store_file(  # doctest: +SKIP
            ...     "/incoming/book.epub",
            ... )


        :param path:
        :param location:
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
            raise StoreIntegrityError(
                f"expected {expected_size} bytes, found {observed_size}."
            )
        with source_path.open("rb") as source:
            return self.store_stream(
                source,
                location=location,
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


def _placement_hints(
    metadata: StorageHintSource | None,
) -> StoragePlacementHints | None:
    """
    Project optional library metadata into Store placement hints.

    Example:
        >>> _placement_hints({"title": "Book"})["title"]
        'Book'
    """

    return None if metadata is None else derive_storage_hints(metadata)


def _store_file_location(
    store: StoreIdentityAPI,
    identifier: StoreFileIdentifier,
) -> Location:
    """
    Resolve an ordinary Store-file identifier to its checked Location.

    Example:
        >>> location = _store_file_location(store, stored)  # doctest: +SKIP
    """

    if isinstance(identifier, FileInfo):
        return store.locate(identifier.location)
    return store.locate(identifier)


def _store_location(
    store: StoreIdentityAPI,
    location: str | Location | None,
    *,
    name: str | None,
    expected_size: int | None,
    expected_digest: Digest | None,
    placement_hints: StoragePlacementHints | None,
) -> Location:
    """
    Resolve an ordinary key or ask the Store to allocate a destination.

    Example:
        >>> destination = _store_location(  # doctest: +SKIP
        ...     store, "incoming/book.epub", name=None,
        ...     expected_size=4, expected_digest=None,
        ...     placement_hints=None,
        ... )
    """

    if location is not None:
        return store.locate(location)
    try:
        if placement_hints is None:
            return store.allocate_location(
                expected_size=expected_size,
                expected_digest=expected_digest,
                name_hint=name,
            )
        return store.allocate_location(
            expected_size=expected_size,
            expected_digest=expected_digest,
            name_hint=name,
            placement_hints=placement_hints,
        )
    except StoreUnsupportedOperation as error:
        raise StoreUnsupportedOperation(
            f"{store.configuration.store_name!r} does not allocate Locations; "
            + "supply location explicitly."
        ) from error


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
    "StoreConvenienceAPI",
    "StoreFileIdentifier",
    "StoreSource",
]
