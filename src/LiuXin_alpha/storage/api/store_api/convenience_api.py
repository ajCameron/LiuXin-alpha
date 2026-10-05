"""
Adapt ordinary file identifiers and payloads to one configured Store's exact API.

Read/delete identifiers resolve through Store identity; a prior FileInfo supplies
only its Location, not cached metadata or an implicit version condition. Writes
accept bytes-like values, borrowed streams, or owned local-file reads. Optional
library metadata becomes placement advice, not catalogue persistence or mandatory
backend-native metadata.

Destination allocation precedes write-mode validation and may have implementation
effects even when a later argument is rejected. The composed Store owns read-only,
capability, Location, verification, and publication enforcement.

Example:
    >>> info = store.store_bytes(b"book", location="incoming/book.epub")  # doctest: +SKIP
"""

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


# Todo: split convenience down and include the subclasses in the submodules as appropriate
class StoreConvenienceAPI:
    """
    Familiar file operations layered over a configured Store's exact API.

    This mixin owns no state. An omitted Location asks the Store allocator to choose one; a string
    is parsed by the Store; and a Location remains available when the caller needs an exact
    destination.

    The runtime casts used by this mixin do not validate its host. Composition must provide
    StoreIdentityAPI and StoreFileAPI behavior.

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
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Open a Store file as a read-only binary stream.

        This method never opens the destination for mutation and accepts no write mode. Use
        ``store()``, ``store_stream()``, or ``begin_write()`` for staged, commit-based writes. Close
        the returned stream, preferably by using it as a context manager.

        Store keys may themselves be hashes in a content-addressed Store, but a generic Store does
        not claim a digest reverse index.

        Resolve the identifier before delegating to get. No implicit URI parsing, digest lookup, or
        version pinning is added for a FileInfo input.

        Example:
            >>> with store.open_file("objects/sha256/abcd") as source:  # doctest: +SKIP
            ...     payload = source.read()


        :param identifier: Opaque key, routed Location, or prior FileInfo used only to identify an object.
        :param offset: Starting byte offset forwarded to the Store reader.
        :param length: Optional byte count; None reads the remaining object.
        :param if_version: Optional explicit version precondition; a FileInfo identifier does not supply one automatically.
        :return: Binary read stream owned by the caller, who must close it.
        """

        identity = cast(StoreIdentityAPI, cast(object, self))
        return cast(StoreFileAPI, cast(object, self)).get(
            _store_file_location(identity, identifier),
            offset=offset,
            length=length,
            if_version=if_version,
        )

    def get_file(
        self,
        identifier: StoreFileIdentifier,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Return the read-only ``open_file`` stream using familiar vocabulary.

        Example:
            >>> with store.get_file("objects/42") as source:  # doctest: +SKIP
            ...     payload = source.read()


        :param identifier: Opaque key, routed Location, or prior FileInfo used only to identify an object.
        :param offset: Starting byte offset forwarded to the Store reader.
        :param length: Optional byte count; None reads the remaining object.
        :param if_version: Optional explicit version precondition; a FileInfo identifier does not supply one automatically.
        :return: The open_file result unchanged, with the same caller-owned lifetime.
        """

        return self.open_file(
            identifier,
            offset=offset,
            length=length,
            if_version=if_version,
        )

    def read_file(
        self,
        identifier: StoreFileIdentifier,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> bytes:
        """
        Read a Store file by opaque key, Location, or returned FileInfo.

        Read the entire selected range without an independent memory cap. Context cleanup owns the
        opened stream, including when read raises.

        Example:
            >>> store.read_file("objects/42", length=4)  # doctest: +SKIP
            b'book'


        :param identifier: Opaque key, routed Location, or prior FileInfo used only to identify an object.
        :param offset: Starting byte offset forwarded to the Store reader.
        :param length: Optional byte count; None reads the remaining object.
        :param if_version: Optional explicit version precondition; a FileInfo identifier does not supply one automatically.
        :return: The complete source.read() result after cleanup; no independent bytes-type check is performed.
        """

        with self.open_file(
            identifier,
            offset=offset,
            length=length,
            if_version=if_version,
        ) as source:
            return source.read()

    def stat_file(self, identifier: StoreFileIdentifier) -> FileInfo:
        """
        Return current information using a key, Location, or prior FileInfo.

        A supplied ``FileInfo`` identifies the object; this method still asks the Store for fresh
        information rather than returning a stale value.

        Example:
            >>> current = store.stat_file(stored)  # doctest: +SKIP


        :param identifier: Opaque key, routed Location, or prior FileInfo used only to identify an object.
        :return: Fresh stat metadata for the resolved Location.
        """

        identity = cast(StoreIdentityAPI, cast(object, self))
        return cast(StoreFileAPI, cast(object, self)).stat(
            _store_file_location(identity, identifier)
        )

    def file_exists(self, identifier: StoreFileIdentifier) -> bool:
        """
        Test whether a Store file exists using any ordinary identifier.

        Identifier resolution occurs before exists, so parsing and ownership errors remain visible.

        Example:
            >>> store.file_exists("objects/42")  # doctest: +SKIP
            True


        :param identifier: Opaque key, routed Location, or prior FileInfo used only to identify an object.
        :return: Whether the resolved Store object exists; only the underlying not-found path is suppressed.
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

        The wrapper neither infers a version from FileInfo nor performs its own capability check;
        those preconditions belong to the concrete delete operation.

        Example:
            >>> store.delete_file(stored, missing_ok=True)  # doctest: +SKIP


        :param identifier: Opaque key, routed Location, or prior FileInfo used only to identify an object.
        :param missing_ok: Whether the underlying delete may accept genuine absence.
        :param if_version: Optional explicit version condition forwarded to deletion.
        :return: None after delegated deletion; ownership, policy, and operational failures propagate.
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

        Bytes, bytearray, and memoryview are converted to bytes; a supplied size mismatch fails
        before Store dispatch. Strings and path-like objects select local-file reads. Other values
        only need a read attribute at this point; actual stream results are checked later by put.

        Example:
            >>> info = store.store(  # doctest: +SKIP
            ...     b"cover", name="cover.jpg",
            ...     metadata={"title": "Permutation City"},
            ... )


        :param source: Bytes-like payload, local path, or caller-owned object with a read attribute.
        :param location: Explicit key/Location, or None to request Store allocation.
        :param name: Optional name hint for allocation; an explicit destination does not use it.
        :param metadata: Optional library metadata projected into advisory Store placement hints.
        :param write_mode: WriteMode or exact enum-value string; defaults to CREATE_ONLY when both mode names are None.
        :param expected_size: Optional exact source byte count checked before dispatch or by the staged session.
        :param expected_digest: Optional expected content digest verified by the staged session.
        :param mode: Compatibility alias for write_mode; supplying both non-None names raises TypeError.
        :return: Committed FileInfo from the selected bytes, local-file, or stream path.
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


        :param data: Small in-memory payload wrapped in BytesIO with len(data) as the size expectation.
        :param location: Explicit key/Location, or None to request Store allocation.
        :param name: Optional name hint for allocation; an explicit destination does not use it.
        :param metadata: Optional library metadata projected into advisory Store placement hints.
        :param write_mode: WriteMode or exact enum-value string; defaults to CREATE_ONLY when both mode names are None.
        :param expected_digest: Optional expected content digest verified by the staged session.
        :param mode: Compatibility alias for write_mode; supplying both non-None names raises TypeError.
        :return: Committed metadata from store_stream.
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

        Project metadata before checking placement-hint capability or resolving the destination.
        Allocation receives hints only when advertised, while put always receives the projected
        value and its default implementation gates begin_write accordingly. Select write mode after
        resolution/allocation, so a later mode error does not undo allocator effects.

        Example:
            >>> info = store.store_stream(  # doctest: +SKIP
            ...     source, expected_size=4, name="book.epub",
            ... )


        :param source: Borrowed binary input consumed at its current position without closing or rewinding it.
        :param location: Explicit key/Location, or None to request Store allocation.
        :param name: Optional name hint for allocation; an explicit destination does not use it.
        :param metadata: Optional library metadata projected into advisory Store placement hints.
        :param write_mode: WriteMode or exact enum-value string; defaults to CREATE_ONLY when both mode names are None.
        :param expected_size: Optional exact source byte count checked before dispatch or by the staged session.
        :param expected_digest: Optional expected content digest verified by the staged session.
        :param mode: Compatibility alias for write_mode; supplying both non-None names raises TypeError.
        :return: Result of the Store put operation at the explicit or allocated destination.
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

        Observe size before opening and use that observation as the transfer expectation. Stat,
        open, and reading are separate operations; the helper does not lock the source against
        changes. A read/close failure can be reported after destination publication.

        Example:
            >>> info = store.store_file(  # doctest: +SKIP
            ...     "/incoming/book.epub",
            ... )


        :param path: Local path opened directly with Path, without expanduser or resolve.
        :param location: Explicit key/Location, or None to request Store allocation.
        :param name: Optional allocation name; None uses the source basename.
        :param metadata: Optional library metadata projected into advisory Store placement hints.
        :param write_mode: WriteMode or exact enum-value string; defaults to CREATE_ONLY when both mode names are None.
        :param expected_size: Optional byte count compared with the initial path stat before opening.
        :param expected_digest: Optional expected content digest verified by the staged session.
        :param mode: Compatibility alias for write_mode; supplying both non-None names raises TypeError.
        :return: Committed metadata after closing the owned local source handle.
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


# Todo: These should not be in the API...
def _bytes_stream(data: bytes) -> BinaryIO:
    """
    Wrap an in-memory payload as a binary stream.

    Example:
        >>> _bytes_stream(b"book").read()
        b'book'


    :param data: In-memory payload accepted by io.BytesIO.
    :return: New BytesIO positioned at the beginning of the payload.
    """

    import io

    return io.BytesIO(data)


def _placement_hints(
    metadata: StorageHintSource | None,
) -> StoragePlacementHints | None:
    """
    Project optional library metadata into Store placement hints.

    The projector may return None for unsupported sources or failed optional providers. Exceptions
    it does not handle propagate even when a later Store capability check would discard the hints.
    This helper does not write metadata to a catalogue.

    Example:
        >>> _placement_hints({"title": "Book"})["title"]
        'Book'


    :param metadata: Optional metadata/hint source accepted by derive_storage_hints.
    :return: None when absent, otherwise the metadata projection returned by derive_storage_hints.
    """

    return None if metadata is None else derive_storage_hints(metadata)


def _store_file_location(
    store: StoreIdentityAPI,
    identifier: StoreFileIdentifier,
) -> Location:
    """
    Resolve an ordinary Store-file identifier to its checked Location.

    The helper does not reuse FileInfo size/digest/version or perform stat; it resolves identity
    only.

    Example:
        >>> location = _store_file_location(store, stored)  # doctest: +SKIP


    :param store: Identity facade responsible for checking Location ownership and parsing keys.
    :param identifier: Opaque key, routed Location, or prior FileInfo used only to identify an object.
    :return: Location obtained from store.locate, using only FileInfo.location for metadata inputs.
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

    An explicit destination bypasses allocation and ignores name, expected size/digest, and
    placement hints here. With no destination, omit the placement_hints keyword when None for older
    allocator signatures. Catch only StoreUnsupportedOperation from allocation; other failures
    propagate, and no additional validation of a returned Location is performed here.

    Example:
        >>> destination = _store_location(  # doctest: +SKIP
        ...     store, "incoming/book.epub", name=None,
        ...     expected_size=4, expected_digest=None,
        ...     placement_hints=None,
        ... )


    :param store: Configured Store identity/allocation facade.
    :param location: Explicit key/Location, or None to request Store allocation.
    :param name: Optional name hint for allocation; an explicit destination does not use it.
    :param expected_size: Optional byte-size hint forwarded only when allocating a destination.
    :param expected_digest: Optional content-digest hint forwarded only when allocating a destination.
    :param placement_hints: Optional already-projected advice forwarded only on the allocation branch.
    :return: Resolved explicit Location or allocator result; unsupported allocation is re-raised with configured-name context.
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

    Text is not stripped or case-normalized before enum conversion.

    Example:
        >>> _write_mode("create_only") is WriteMode.CREATE_ONLY
        True


    :param mode: WriteMode instance or exact string value accepted by the enum.
    :return: Existing enum unchanged or WriteMode conversion result; invalid input errors propagate.
    """

    return mode if isinstance(mode, WriteMode) else WriteMode(mode)


def _write_mode_argument(
    write_mode: WriteMode | str | None,
    mode: WriteMode | str | None,
) -> WriteMode:
    """
    Select the clear write-mode name while retaining the former alias.

    Both names being non-None is an error even when their values are equal.

    Example:
        >>> _write_mode_argument("replace", None) is WriteMode.REPLACE
        True


    :param write_mode: WriteMode or exact enum-value string; defaults to CREATE_ONLY when both mode names are None.
    :param mode: Compatibility alias for write_mode; supplying both non-None names raises TypeError.
    :return: Selected normalized policy, defaulting to CREATE_ONLY when both inputs are None.
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
