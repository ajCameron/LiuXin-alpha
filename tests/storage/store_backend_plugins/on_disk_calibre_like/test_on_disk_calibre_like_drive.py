"""
Exercise hint-driven local placement and post-write database-row projection.

Files are committed to real temporary roots. Small dictionary/database doubles
record row lookup, projected values, and sync calls without claiming persistence
or transaction behavior from a live database backend.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.store_backend_plugins.on_disk_calibre_like import (
    OnDiskCalibreLikeStorageBackend,
)
from tests.fixtures.storage_unicode import (
    UNICODE_AUTHORS,
    UNICODE_PAYLOAD,
    UNICODE_TITLE,
)


class _DummyFileRow(dict):
    """
    Provide mutable row fields and a sync-call counter without a persistent backend.

    Example:
        >>> row = _DummyFileRow(file_storage_key=None)
        >>> row.sync_calls
        0
    """
    def __init__(self, **kwargs):
        """
        Initialize dictionary fields from keyword arguments and reset the independent
        synchronization counter.

        Example:
            >>> _DummyFileRow(file_storage_key="books/a")["file_storage_key"]
            'books/a'


        :param kwargs: Initial row field/value pairs passed to dict.__init__.
        :return: None after storing fields and setting sync_calls to zero.
        """
        super().__init__(**kwargs)
        self.sync_calls = 0

    def sync(self) -> None:
        """
        Record one requested synchronization without persisting or validating row fields.

        Example:
            >>> row = _DummyFileRow()
            >>> row.sync()
            >>> row.sync_calls
            1


        :return: None after incrementing sync_calls.
        """
        self.sync_calls += 1


class _DummyDb:
    """
    Retain an ID-to-row mapping and record requested lookups for database projection tests.

    Example:
        >>> row = _DummyFileRow()
        >>> db = _DummyDb({11: row})
        >>> db.get_row_from_id("files", 11) is row
        True
    """
    def __init__(self, rows_by_id: dict[int, _DummyFileRow]):
        """
        Retain the supplied row dictionary by reference and start an empty lookup log.

        Example:
            >>> _DummyDb({}).calls
            []


        :param rows_by_id: Mutable row mapping borrowed by identity for later lookups.
        :return: None after initializing rows_by_id and calls.
        """
        self.rows_by_id = rows_by_id
        self.calls = []

    def get_row_from_id(self, table: str, row_id: int):
        """
        Append the requested table and ID to the log, then retrieve solely by ID.

        The table name is recorded but does not select a separate mapping.

        Example:
            >>> db = _DummyDb({})
            >>> db.get_row_from_id("files", 11) is None
            True
            >>> db.calls
            [('files', 11)]


        :param table: Requested table spelling retained in the call log.
        :param row_id: Integer key used to retrieve the stored fake row.
        :return: Mapped row object or None when the ID is absent.
        """
        self.calls.append((table, row_id))
        return self.rows_by_id.get(row_id)


class _HintsOnlyMetadata:
    """
    Expose a retained WorkStorageHints value only through the storage_hints adapter method.

    Example:
        >>> hints = api.WorkStorageHints(title="Book")
        >>> _HintsOnlyMetadata(hints).storage_hints() is hints
        True
    """
    def __init__(self, hints: api.WorkStorageHints):
        """
        Retain the supplied hints without copying or exposing their fields directly on this wrapper.

        Example:
            >>> metadata = _HintsOnlyMetadata(api.WorkStorageHints(title="Book"))


        :param hints: WorkStorageHints object returned by the adapter method.
        :return: None after retaining the hints reference.
        """
        self._hints = hints

    def storage_hints(self) -> api.WorkStorageHints:
        """
        Return the same retained hints object on every call.

        Example:
            >>> _HintsOnlyMetadata(api.WorkStorageHints(title="Book")).storage_hints().title
            'Book'


        :return: Stored WorkStorageHints by identity, without additional normalization.
        """
        return self._hints


def test_calibre_like_unicode_metadata_drives_lossless_rich_layout(
    tmp_path: Path,
) -> None:
    """
    Use the shared Unicode title/authors to verify the exact generated key, filename hint,
    inventory, and stored bytes for those fixture values.

    Example:
        >>> test_calibre_like_unicode_metadata_drives_lossless_rich_layout(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskCalibreLikeStorageBackend(tmp_path)
    author_text = " & ".join(UNICODE_AUTHORS)

    info = store.store_bytes(
        UNICODE_PAYLOAD,
        metadata={
            "title": UNICODE_TITLE,
            "authors": UNICODE_AUTHORS,
            "work_id": 314,
            "format": "EPUB",
        },
    )
    expected = (
        f"{author_text}/{UNICODE_TITLE} (314)/"
        f"{UNICODE_TITLE} - {author_text}.epub"
    )

    assert info.location.key == expected
    assert store.stat_file(info).hints.suggested_filename == expected.rsplit("/", 1)[-1]
    assert [location.key for location in store.iter_locations()] == [expected]
    assert store.read_file(info) == UNICODE_PAYLOAD


def test_calibre_like_layout_from_basic_metadata(tmp_path: Path) -> None:
    """
    Map title, one author, work ID, and uppercase format into the expected author/title-ID/filename
    layout and verify the payload.

    Example:
        >>> test_calibre_like_layout_from_basic_metadata(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskCalibreLikeStorageBackend(tmp_path)
    info = store.store_bytes(
        b"abc",
        metadata={
            "title": "Dune",
            "authors": ["Frank Herbert"],
            "work_id": 9,
            "format": "EPUB",
        },
    )
    assert info.location.key == (
        "Frank Herbert/Dune (9)/Dune - Frank Herbert.epub"
    )
    assert store.read_file(info) == b"abc"


def test_calibre_like_layout_uses_author_combo_folder(tmp_path: Path) -> None:
    """
    Combine two authors with an ampersand and use the book ID and PDF extension in the expected
    generated path.

    Example:
        >>> test_calibre_like_layout_uses_author_combo_folder(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskCalibreLikeStorageBackend(tmp_path)
    info = store.store_bytes(
        b"abc",
        metadata={
            "title": "Good Omens",
            "authors": ["Neil Gaiman", "Terry Pratchett"],
            "book_id": 17,
            "file_extension": "pdf",
        },
    )
    assert info.location.key == (
        "Neil Gaiman & Terry Pratchett/Good Omens (17)/"
        "Good Omens - Neil Gaiman & Terry Pratchett.pdf"
    )


def test_calibre_like_collision_requires_explicit_replacement(tmp_path: Path) -> None:
    """
    Reject a repeated implicit write at the same hinted path, then explicitly replace that location
    and verify new bytes.

    Example:
        >>> test_calibre_like_collision_requires_explicit_replacement(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskCalibreLikeStorageBackend(tmp_path)
    metadata = {
        "title": "Dune",
        "authors": ["Frank Herbert"],
        "work_id": 9,
        "format": "epub",
    }
    first = store.store_bytes(b"one", metadata=metadata)
    with pytest.raises(api.StoreAlreadyExists):
        store.store_bytes(b"one", metadata=metadata)
    replacement = store.store_bytes(
        b"two",
        location=first.location,
        metadata=metadata,
        write_mode="replace",
    )
    assert store.read_file(replacement) == b"two"


def test_calibre_like_updates_database_file_row(tmp_path: Path) -> None:
    """
    Commit a real file and verify its key, absolute path, filename, Store ID, and integer timestamp
    are projected to the fake row with one lookup and sync.

    Example:
        >>> test_calibre_like_updates_database_file_row(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    row = _DummyFileRow(file_storage_key=None)
    database = _DummyDb({11: row})
    store = OnDiskCalibreLikeStorageBackend(
        tmp_path,
        database=database,
        store_id=77,
    )
    info = store.store_bytes(
        b"payload",
        metadata={
            "title": "Children of Dune",
            "authors": ["Frank Herbert"],
            "file_id": 11,
            "file_extension": "epub",
        },
    )

    assert row["file_storage_key"] == info.location.key
    assert row["file_url"] == str(tmp_path / Path(info.location.key))
    assert row["file_name"] == "Children of Dune - Frank Herbert.epub"
    assert row["file_store_id"] == 77
    assert isinstance(row["file_modified_timestamp_ep_k"], int)
    assert row.sync_calls == 1
    assert database.calls == [("files", 11)]


def test_calibre_like_uses_storage_hints_when_direct_fields_absent(
    tmp_path: Path,
) -> None:
    """
    Supply a metadata object exposing only storage_hints and verify work-based MOBI placement plus
    row lookup using extra.file_id.

    Example:
        >>> test_calibre_like_uses_storage_hints_when_direct_fields_absent(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    row = _DummyFileRow(file_storage_key=None)
    database = _DummyDb({8: row})
    store = OnDiskCalibreLikeStorageBackend(tmp_path, database=database)
    metadata = _HintsOnlyMetadata(
        api.WorkStorageHints(
            work_id=5,
            title="Permutation City",
            primary_agents=("Greg Egan",),
            file_formats=("MOBI",),
            extra={"file_id": 8},
        )
    )
    info = store.store_bytes(b"hints", metadata=metadata)
    assert info.location.key == (
        "Greg Egan/Permutation City (5)/Permutation City - Greg Egan.mobi"
    )
    assert row["file_storage_key"] == info.location.key


def test_calibre_like_uses_item_storage_hints_when_available(
    tmp_path: Path,
) -> None:
    """
    Use ItemStorageHints to verify work-ID priority over item ID and the supplied EPUB filename
    stem.

    Example:
        >>> test_calibre_like_uses_item_storage_hints_when_available(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskCalibreLikeStorageBackend(tmp_path)
    hints = api.ItemStorageHints(
        item_id=44,
        work_id=5,
        title="Permutation City",
        primary_agents=("Greg Egan",),
        file_formats=("EPUB",),
        preferred_filename_stem="Permutation City - Greg Egan",
    )
    info = store.store_bytes(b"hints", metadata=hints)
    assert info.location.key == (
        "Greg Egan/Permutation City (5)/Permutation City - Greg Egan.epub"
    )


def test_calibre_like_without_metadata_uses_managed_hash_layout(
    tmp_path: Path,
) -> None:
    """
    Write without metadata and verify the .liuxin/managed_drive five-character-bucket SHA-256
    fallback key.

    Example:
        >>> test_calibre_like_without_metadata_uses_managed_hash_layout(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskCalibreLikeStorageBackend(tmp_path)
    info = store.store_bytes(b"abc")
    digest = __import__("hashlib").sha256(b"abc").hexdigest()
    assert info.location.key == (
        f".liuxin/managed_drive/{digest[:5]}/{digest}"
    )
