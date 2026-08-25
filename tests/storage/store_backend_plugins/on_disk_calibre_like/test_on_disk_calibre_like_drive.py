"""Rich-placement and transactional tests for the Calibre-like Store."""

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
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.sync_calls = 0

    def sync(self) -> None:
        self.sync_calls += 1


class _DummyDb:
    def __init__(self, rows_by_id: dict[int, _DummyFileRow]):
        self.rows_by_id = rows_by_id
        self.calls = []

    def get_row_from_id(self, table: str, row_id: int):
        self.calls.append((table, row_id))
        return self.rows_by_id.get(row_id)


class _HintsOnlyMetadata:
    def __init__(self, hints: api.WorkStorageHints):
        self._hints = hints

    def storage_hints(self) -> api.WorkStorageHints:
        return self._hints


def test_calibre_like_unicode_metadata_drives_lossless_rich_layout(
    tmp_path: Path,
) -> None:
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
    store = OnDiskCalibreLikeStorageBackend(tmp_path)
    info = store.store_bytes(b"abc")
    digest = __import__("hashlib").sha256(b"abc").hexdigest()
    assert info.location.key == (
        f".liuxin/managed_drive/{digest[:5]}/{digest}"
    )
