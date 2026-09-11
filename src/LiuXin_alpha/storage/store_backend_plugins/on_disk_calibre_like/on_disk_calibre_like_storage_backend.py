"""
Place managed local files from bibliographic hints and update optional file rows.

Preferred keys, title/author layouts, and digest fallback determine unreserved
Store locations. File publication precedes the second metadata conversion and
database update, so errors there may leave committed bytes and partial row state.
"""

from __future__ import annotations

import dataclasses
import hashlib
import os
import time

from collections.abc import Mapping
from pathlib import Path
from typing import Any
from uuid import UUID

from LiuXin_alpha.storage.api import (
    Digest,
    FileInfo,
    StoragePlacementHints,
    derive_storage_hints,
)
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive import (
    OnDiskExistingManagedStorageBackend,
)


class OnDiskCalibreLikeStorageBackend(OnDiskExistingManagedStorageBackend):
    """
    Place local files from bibliographic hints and optionally update a database row after
    publication.

    A preferred key takes priority; title/author hints otherwise form readable folders and
    filenames, with digest or inherited allocation as fallback. Allocation does not reserve a
    pathname or resolve collisions.

    Filesystem writes retain the managed Store's transaction and ownership rules. Optional database
    updates are a subsequent operation: conversion, row lookup, assignment, or synchronization can
    fail after file bytes have committed, without a rollback across those boundaries.

    Example:
        >>> store = OnDiskCalibreLikeStorageBackend("library")  # doctest: +SKIP
        >>> info = store.store_bytes(b"book", metadata={"title": "A Book", "authors": ["Writer"], "format": "epub"})  # doctest: +SKIP
        >>> info.location.key  # doctest: +SKIP
        'Writer/A Book/A Book - Writer.epub'
    """

    store_kind = "on_disk_calibre_like"

    def __init__(
        self,
        url: str | os.PathLike[str],
        name: str | None = None,
        uuid: str | UUID | None = None,
        *,
        database: Any = None,
        store_id: int | None = None,
    ) -> None:
        """
        Configure the managed filesystem Store, then retain the optional database and row Store ID.

        Database shape and store_id are not validated. Construction does not probe/create the root,
        query the database, or synchronize file rows.

        Example:
            >>> store = OnDiskCalibreLikeStorageBackend("library", database=db, store_id=7)  # doctest: +SKIP


        :param url: Filesystem root path or supported local file URI.
        :param name: Optional display name; None or empty text uses inherited root-name fallback.
        :param uuid: UUID or UUID string; None generates a Store identity.
        :param database: Optional object consulted for get_row_from_id after successful writes.
        :param store_id: Value written to file_store_id on matching database rows, including None.
        :return: None after inherited configuration and retention of both database references.
        """
        super().__init__(url, name=name, uuid=uuid)
        self._database = database
        self._store_id = store_id

    @property
    def database(self) -> Any:
        """
        Expose the optional database object currently retained for post-write row updates.

        Example:
            >>> store.database is db  # doctest: +SKIP
            True


        :return: Stored database reference, or None; no connection or capability check occurs.
        """
        return self._database

    def set_database(self, database: Any) -> None:
        """
        Replace the retained database reference without validating, closing, or synchronizing either
        object.

        Example:
            >>> store.set_database(None)  # doctest: +SKIP


        :param database: Object to consult on future post-write updates, or None to disable them.
        :return: None after assignment; existing files and rows are unchanged by this setter.
        """
        self._database = database

    @property
    def store_id(self) -> int | None:
        """
        Expose the value configured for file_store_id in future database updates.

        Example:
            >>> store.store_id  # doctest: +SKIP
            7


        :return: Stored optional integer value as supplied; no lookup validates it.
        """
        return self._store_id

    def set_store_id(self, store_id: int | None) -> None:
        """
        Replace the value used for file_store_id without updating existing database rows.

        Example:
            >>> store.set_store_id(8)  # doctest: +SKIP


        :param store_id: New database-side Store ID, or None; the setter performs no runtime validation.
        :return: None after retaining the value.
        """
        self._store_id = store_id

    @property
    def capabilities(self):
        """
        Copy the inherited filesystem capabilities and advertise placement-hint support.

        Example:
            >>> store.capabilities.placement_hints  # doctest: +SKIP
            True


        :return: Dataclass replacement with placement_hints=True; all other capability fields are inherited.
        """
        return dataclasses.replace(super().capabilities, placement_hints=True)

    def allocate_location(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        name_hint: str | None = None,
        placement_hints: StoragePlacementHints | None = None,
    ):
        """
        Select a preferred key, hinted author/title layout, digest fallback, or inherited
        allocation.

        A nonempty preferred_storage_key is located directly. Without a nonempty title, a digest
        produces .liuxin/managed_drive/<first-five>/<value>; without a digest, the superclass
        receives size/digest/name hints and uses its ordinary allocation policy. The digest fallback
        omits the algorithm and differs from the superclass's .liuxin-managed prefix.

        With a title, sanitized author text and title plus the first integer identifier form
        directories. An optional preferred stem or title/author combination forms the filename, and
        format/name hints choose the extension. Components are cleaned but not truncated or
        Unicode-normalized. Location parsing still applies downstream key rules; no branch reserves
        the result or checks destination existence.

        Example:
            >>> store.allocate_location(placement_hints={"title": "Dune", "authors": ["Frank Herbert"], "work_id": 9, "format": "EPUB"}).key  # doctest: +SKIP
            'Frank Herbert/Dune (9)/Dune - Frank Herbert.epub'


        :param expected_size: Optional byte-size hint forwarded only to inherited fallback allocation.
        :param expected_digest: Optional digest used for no-title placement or forwarded to inherited allocation.
        :param name_hint: Optional filename used for extension fallback or inherited allocation.
        :param placement_hints: Mapping or placement-hint object supplying preferred key, title, authors, IDs, stem, and format; None selects fallback placement.
        :return: Owned Location selected by the first applicable rule, without a file write.
        """
        hints = _hint_mapping(placement_hints)
        preferred_key = _text(hints.get("preferred_storage_key"))
        if preferred_key:
            return self.locate(preferred_key)
        if not hints or not _text(hints.get("title")):
            if expected_digest is not None:
                return self.locate(
                    ".liuxin/managed_drive/"
                    f"{expected_digest.value[:5]}/{expected_digest.value}"
                )
            return super().allocate_location(
                expected_size=expected_size,
                expected_digest=expected_digest,
                name_hint=name_hint,
            )

        title = _component(_text(hints.get("title")) or "Untitled", "Untitled")
        authors = _authors(hints)
        author_text = _component(" & ".join(authors), "Unknown")
        identifier = _identifier(hints)
        folder_title = title if identifier is None else f"{title} ({identifier})"
        stem = _component(
            _text(hints.get("preferred_filename_stem"))
            or f"{title} - {author_text}",
            title,
        )
        extension = _extension(hints, name_hint)
        return self.location(
            author_text,
            _component(folder_title, title),
            f"{stem}.{extension}",
        )

    def store_bytes(
        self,
        data: bytes,
        *,
        expected_digest: Digest | None = None,
        **kwargs,
    ) -> FileInfo:
        """
        Compute SHA-256 when no digest is supplied and delegate the byte write with that
        expectation.

        The digest is computed even for an explicit destination. Inherited convenience dispatch
        reaches this class's store_stream, including its later optional database update.

        Example:
            >>> info = store.store_bytes(b"book", location="books/a.epub")  # doctest: +SKIP


        :param data: Payload bytes used for optional hashing and the inherited write.
        :param expected_digest: Digest to check during writing; None computes SHA-256 from data.
        :param kwargs: Arguments forwarded to Store.store_bytes, such as location, name, metadata, write_mode, or mode.
        :return: FileInfo after the delegated write and post-write update return; later metadata/database errors may follow publication.
        """
        digest = expected_digest or Digest(
            "sha256",
            hashlib.sha256(data).hexdigest(),
        )
        return super().store_bytes(data, expected_digest=digest, **kwargs)

    def store_stream(self, source, *, metadata=None, **kwargs) -> FileInfo:
        """
        Write through the inherited Store, then derive hints again and update an optional database
        row.

        The source is borrowed at its current position. The first inherited hint conversion supports
        placement before writing; the second happens after the inherited write returns, even when no
        database is configured. The shared helper suppresses Exceptions from calling a storage_hints
        provider, yielding no hints; attribute lookup and other uncaught conversion failures can
        still propagate. Such failures or row-update errors can follow committed bytes. No rollback
        joins file publication to database synchronization.

        Example:
            >>> info = store.store_stream(source, location="books/a.epub", metadata={"file_id": 11})  # doctest: +SKIP


        :param source: Borrowed readable binary stream; this method does not close or rewind it.
        :param metadata: Optional hint source accepted by derive_storage_hints and consulted again after writing.
        :param kwargs: Arguments forwarded to Store.store_stream, including location, name, expected_size, expected_digest, write_mode, or mode.
        :return: Underlying committed FileInfo after subsequent hint conversion and database update complete.
        """
        info = super().store_stream(source, metadata=metadata, **kwargs)
        hints = derive_storage_hints(metadata) if metadata is not None else None
        self._update_database(info, hints)
        return info

    def _update_database(
        self,
        info: FileInfo,
        placement_hints: StoragePlacementHints | None,
    ) -> None:
        """
        Find an optional file row and update its storage key, local path, name, Store ID, and
        timestamp.

        No database, noninteger/bool file ID, missing callable getter, or absent row returns without
        updates. The top-level file_id takes priority; extra.file_id is considered only when the
        top-level value is None. Integer IDs are not checked for positivity.

        Row lookup uses get_row_from_id("files", id). Each mapping assignment may fall back to an
        existing attribute on KeyError, TypeError, or AttributeError; unsupported fields without an
        attribute are skipped. Other assignment, lookup, fallback-attribute, and sync errors
        propagate. Callable row.sync runs after assignments, even if some were skipped. Partial row
        changes and already published files are not rolled back.

        Example:
            >>> store._update_database(info, {"file_id": 11})  # doctest: +SKIP


        :param info: Committed file facts; its key is trusted when constructing the local path.
        :param placement_hints: Mapping or hint object containing file_id directly or within extra, or None.
        :return: None after a skipped update or completed assignments/sync; field timestamp uses the current integer Unix time.
        """
        if self._database is None:
            return
        hints = _hint_mapping(placement_hints)
        extra = hints.get("extra")
        file_id = hints.get("file_id")
        if file_id is None and isinstance(extra, Mapping):
            file_id = extra.get("file_id")
        if isinstance(file_id, bool) or not isinstance(file_id, int):
            return
        getter = getattr(self._database, "get_row_from_id", None)
        if not callable(getter):
            return
        row = getter("files", file_id)
        if row is None:
            return
        absolute = self.root_path.joinpath(*info.location.key.split("/"))
        values = {
            "file_storage_key": info.location.key,
            "file_url": str(absolute),
            "file_name": absolute.name,
            "file_store_id": self._store_id,
            "file_modified_timestamp_ep_k": int(time.time()),
        }
        for key, value in values.items():
            try:
                row[key] = value
            except (KeyError, TypeError, AttributeError):
                if hasattr(row, key):
                    setattr(row, key, value)
        sync = getattr(row, "sync", None)
        if callable(sync):
            sync()


def _hint_mapping(hints: StoragePlacementHints | None) -> Mapping[str, Any]:
    """
    Return a mapping view or call to_mapping on a supplied hint object.

    Mappings are returned by identity. This helper does not copy or validate the result of
    to_mapping; errors from missing/noncallable converters propagate.

    Example:
        >>> values = {"title": "Book"}
        >>> _hint_mapping(values) is values
        True


    :param hints: Mapping, placement-hint object, or None.
    :return: The original mapping, converter result, or a new empty dictionary for None.
    """
    if hints is None:
        return {}
    if isinstance(hints, Mapping):
        return hints
    return hints.to_mapping()


def _authors(hints: Mapping[str, Any]) -> tuple[str, ...]:
    """
    Select primary_agents when present, otherwise authors, and project author text.

    A string is returned unchanged as one author, including empty or whitespace-only text.
    List/tuple entries are stringified and stripped through _text, with empty/None results dropped.
    Unsupported values or an empty filtered sequence use Unknown. A present unusable primary_agents
    value prevents fallback to authors.

    Example:
        >>> _authors({"primary_agents": None, "authors": ["Writer"]})
        ('Unknown',)
        >>> _authors({"authors": [" Writer ", None]})
        ('Writer',)


    :param hints: Borrowed mapping supplying author-related fields.
    :return: Tuple of selected author strings, using a one-item Unknown fallback when required.
    """
    raw = hints.get("primary_agents", hints.get("authors", ()))
    if isinstance(raw, str):
        return (raw,)
    if isinstance(raw, (list, tuple)):
        values = tuple(_text(value) for value in raw)
        return tuple(value for value in values if value) or ("Unknown",)
    return ("Unknown",)


def _identifier(hints: Mapping[str, Any]) -> int | None:
    """
    Choose the first nonboolean integer from prioritized top-level IDs, then selected extra IDs.

    Top-level order is work_id, book_id, item_id, manifestation_id, file_id. Extra uses work_id,
    book_id, item_id, file_id, omitting manifestation_id. Zero and negative integers are accepted;
    invalid values are skipped in favor of later keys.

    Example:
        >>> _identifier({"work_id": True, "book_id": -2, "item_id": 4})
        -2


    :param hints: Mapping with optional top-level identifiers and an extra mapping.
    :return: First accepted integer, or None when no supported identifier is found.
    """
    for key in ("work_id", "book_id", "item_id", "manifestation_id", "file_id"):
        value = hints.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    extra = hints.get("extra")
    if isinstance(extra, Mapping):
        for key in ("work_id", "book_id", "item_id", "file_id"):
            value = extra.get(key)
            if isinstance(value, int) and not isinstance(value, bool):
                return value
    return None


def _extension(hints: Mapping[str, Any], name_hint: str | None) -> str:
    """
    Choose and sanitize the first nonempty format hint, then the platform filename suffix, else bin.

    Format priority is file_formats, format_detail, format, file_extension. Only the first item of a
    list/tuple is considered. Selected text loses one leading dot, is lowercased, and passes through
    component cleaning; a result collapsing to bin stops selection rather than trying later hints.
    No codec or format registry validates the result.

    Example:
        >>> _extension({"file_formats": ["", "EPUB"], "format": "PDF"}, "book.txt")
        'pdf'


    :param hints: Mapping supplying prioritized format/extension values.
    :param name_hint: Optional pathname whose final suffix is the fallback when no format text is selected.
    :return: Sanitized lowercase extension component, or bin.
    """
    candidates = (
        hints.get("file_formats"),
        hints.get("format_detail"),
        hints.get("format"),
        hints.get("file_extension"),
    )
    for candidate in candidates:
        if isinstance(candidate, (list, tuple)):
            candidate = candidate[0] if candidate else None
        value = _text(candidate)
        if value:
            return _component(value.removeprefix(".").lower(), "bin")
    if name_hint and Path(name_hint).suffix:
        return _component(Path(name_hint).suffix[1:].lower(), "bin")
    return "bin"


def _text(value: Any) -> str | None:
    """
    Stringify and strip non-None values without Unicode normalization or domain validation.

    Example:
        >>> _text(False)
        'False'
        >>> _text("   ") is None
        True


    :param value: Arbitrary hint value; None bypasses string conversion.
    :return: Nonempty stripped string, or None for None/empty text; conversion errors propagate.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _component(value: str, fallback: str) -> str:
    """
    Replace forbidden filename characters, collapse whitespace, and strip surrounding spaces and
    dots.

    Forbidden characters are angle brackets, colon, double quote, slash, backslash, pipe, question
    mark, star, and NUL. Other non-whitespace controls and Unicode spelling are retained. There is
    no length/reserved-name check; an empty cleaned result returns fallback unchanged without
    cleaning it again.

    Example:
        >>> _component("  A/B : Book. ", "Untitled")
        'A_B _ Book'
        >>> _component(" . ", "Untitled")
        'Untitled'


    :param value: Component text to clean; expected to be a string.
    :param fallback: Value returned unchanged if cleaning removes all content.
    :return: Cleaned nonempty component or the caller's fallback.
    """
    forbidden = '<>:"/\\|?*\x00'
    cleaned = "".join(
        "_" if character in forbidden else character for character in value
    )
    cleaned = " ".join(cleaned.split()).strip(" .")
    return cleaned or fallback


__all__ = ["OnDiskCalibreLikeStorageBackend"]
