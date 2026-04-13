from __future__ import annotations

import pathlib

from LiuXin_alpha.metadata.api import ItemStorageHints, WorkStorageHints
from LiuXin_alpha.storage.store_backend_plugins.on_disk_calibre_like import (
    OnDiskCalibreLikeStorageBackend,
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
    def __init__(self, hints: WorkStorageHints):
        self._hints = hints

    def storage_hints(self) -> WorkStorageHints:
        return self._hints


def test_calibre_like_layout_from_basic_metadata(tmp_path) -> None:
    store = OnDiskCalibreLikeStorageBackend(url=str(tmp_path))
    file_obj = store.add_file(
        b"abc",
        metadata={
            "title": "Dune",
            "authors": ["Frank Herbert"],
            "work_id": 9,
            "format": "EPUB",
        },
    )

    expected = (tmp_path / "Frank Herbert" / "Dune (9)" / "Dune - Frank Herbert.epub").resolve()
    assert pathlib.Path(file_obj.file_url) == expected
    assert expected.exists() is True


def test_calibre_like_layout_uses_author_combo_folder(tmp_path) -> None:
    store = OnDiskCalibreLikeStorageBackend(url=str(tmp_path))
    file_obj = store.add_file(
        b"abc",
        metadata={
            "title": "Good Omens",
            "authors": ["Neil Gaiman", "Terry Pratchett"],
            "book_id": 17,
            "file_extension": "pdf",
        },
    )

    expected = (
        tmp_path
        / "Neil Gaiman & Terry Pratchett"
        / "Good Omens (17)"
        / "Good Omens - Neil Gaiman & Terry Pratchett.pdf"
    ).resolve()
    assert pathlib.Path(file_obj.file_url) == expected


def test_calibre_like_collision_keeps_existing_and_suffixes_new(tmp_path) -> None:
    store = OnDiskCalibreLikeStorageBackend(url=str(tmp_path))
    metadata = {
        "title": "Dune",
        "authors": ["Frank Herbert"],
        "work_id": 9,
        "format": "epub",
    }

    file_one = store.add_file(b"one", metadata=metadata)
    file_same = store.add_file(b"one", metadata=metadata)
    file_two = store.add_file(b"two", metadata=metadata)

    assert file_one.file_url == file_same.file_url
    assert file_two.file_url != file_one.file_url
    assert pathlib.Path(file_two.file_url).name.endswith(" (2).epub")


def test_calibre_like_updates_database_file_row(tmp_path) -> None:
    row = _DummyFileRow(file_storage_key=None)
    db = _DummyDb(rows_by_id={11: row})
    store = OnDiskCalibreLikeStorageBackend(url=str(tmp_path), database=db, store_id=77)

    file_obj = store.add_file(
        b"payload",
        metadata={
            "title": "Children of Dune",
            "authors": ["Frank Herbert"],
            "file_id": 11,
            "file_extension": "epub",
        },
    )

    expected = (
        tmp_path
        / "Frank Herbert"
        / "Children of Dune (11)"
        / "Children of Dune - Frank Herbert.epub"
    ).resolve()
    assert pathlib.Path(file_obj.file_url) == expected

    assert row["file_storage_key"] == "Frank Herbert/Children of Dune (11)/Children of Dune - Frank Herbert.epub"
    assert row["file_url"] == str(expected)
    assert row["file_name"] == "Children of Dune - Frank Herbert.epub"
    assert row["file_store_id"] == 77
    assert isinstance(row["file_modified_timestamp_ep_k"], int)
    assert row.sync_calls == 1
    assert ("files", 11) in db.calls


def test_calibre_like_uses_storage_hints_when_direct_fields_absent(tmp_path) -> None:
    row = _DummyFileRow(file_storage_key=None)
    db = _DummyDb(rows_by_id={8: row})
    store = OnDiskCalibreLikeStorageBackend(url=str(tmp_path), database=db)

    metadata = _HintsOnlyMetadata(
        WorkStorageHints(
            work_id=5,
            title="Permutation City",
            primary_agents=("Greg Egan",),
            file_formats=("MOBI",),
            extra={"file_id": 8},
        )
    )

    file_obj = store.add_file(b"hints", metadata=metadata)
    expected = (
        tmp_path
        / "Greg Egan"
        / "Permutation City (5)"
        / "Permutation City - Greg Egan.mobi"
    ).resolve()

    assert pathlib.Path(file_obj.file_url) == expected
    assert row["file_storage_key"] == "Greg Egan/Permutation City (5)/Permutation City - Greg Egan.mobi"


def test_calibre_like_uses_item_storage_hints_when_available(tmp_path) -> None:
    store = OnDiskCalibreLikeStorageBackend(url=str(tmp_path))

    class _ItemHintsOnlyMetadata:
        def storage_hints(self) -> ItemStorageHints:
            return ItemStorageHints(
                item_id=44,
                work_id=5,
                title="Permutation City",
                primary_agents=("Greg Egan",),
                file_formats=("EPUB",),
                preferred_filename_stem="Permutation City - Greg Egan",
            )

    file_obj = store.add_file(b"hints", metadata=_ItemHintsOnlyMetadata())
    expected = (
        tmp_path
        / "Greg Egan"
        / "Permutation City (5)"
        / "Permutation City - Greg Egan.epub"
    ).resolve()

    assert pathlib.Path(file_obj.file_url) == expected
