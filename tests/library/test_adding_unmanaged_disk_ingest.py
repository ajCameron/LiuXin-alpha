from __future__ import annotations

from pathlib import Path

import pytest

from LiuXin_alpha.storage.reconcile import (
    register_existing_disk_as_unmanaged_store,
    register_existing_disk_with_database_path,
)


def _write_file(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def test_register_existing_disk_creates_store_and_files(db, tmp_path: Path) -> None:
    disk_root = tmp_path / "unmanaged_disk"
    _write_file(disk_root / "Book One.epub", b"epub-data")
    _write_file(disk_root / "nested" / "BOOK_TWO.MOBI", b"mobi-data")
    _write_file(disk_root / "docs" / "readme.txt", b"text-data")
    _write_file(disk_root / "images" / "cover.jpg", b"not-an-ebook")

    report = register_existing_disk_as_unmanaged_store(db, disk_root)

    assert not report.errors
    assert report.ebook_candidates == 3
    assert report.inserted_files == 3
    assert report.updated_files == 0
    assert report.unchanged_files == 0
    assert report.skipped_non_ebook_files == 1
    assert report.store_row_id > 0

    store_row = db.get_row_from_id("stores", report.store_row_id)
    assert store_row is not None
    assert store_row["store_root_uri"] == str(disk_root.resolve())
    assert store_row["store_is_read_only"] == 1

    file_rows = db.search("files", "file_store_id", report.store_row_id)
    assert len(file_rows) == 3

    storage_keys = {row["file_storage_key"] for row in file_rows}
    assert storage_keys == {"Book One.epub", "nested/BOOK_TWO.MOBI", "docs/readme.txt"}

    extensions = {row["file_extension"] for row in file_rows}
    assert extensions == {"epub", "mobi", "txt"}

    for row in file_rows:
        assert row["file_hash_sha256"]
        assert row["file_size_bytes"] > 0

    if "file_store_links" in set(db.get_tables()):
        link_rows = db.search("file_store_links", "file_store_link_store_id", report.store_row_id)
        assert len(link_rows) == 3


def test_register_existing_disk_is_idempotent_and_updates_changed_files(db, tmp_path: Path) -> None:
    disk_root = tmp_path / "unmanaged_disk"
    _write_file(disk_root / "book.epub", b"first-version")
    _write_file(disk_root / "notes.txt", b"first-notes")

    first = register_existing_disk_as_unmanaged_store(db, disk_root)
    assert first.inserted_files == 2

    second = register_existing_disk_as_unmanaged_store(db, disk_root)
    assert second.inserted_files == 0
    assert second.updated_files == 0
    assert second.unchanged_files == 2

    _write_file(disk_root / "book.epub", b"second-version-with-new-size")
    third = register_existing_disk_as_unmanaged_store(db, disk_root)
    assert third.inserted_files == 0
    assert third.updated_files >= 1

    rows = db.search("files", "file_store_id", third.store_row_id)
    by_key = {row["file_storage_key"]: row for row in rows}
    assert by_key["book.epub"]["file_size_bytes"] == len(b"second-version-with-new-size")


def test_register_existing_disk_with_database_path_helper(
    provision_test_database, driver_spec, tmp_path: Path
) -> None:
    provisioned = provision_test_database("test_db_13")
    disk_root = tmp_path / "helper_disk"
    _write_file(disk_root / "one.epub", b"payload")

    report = register_existing_disk_with_database_path(
        database_path=provisioned.db_path,
        disk_path=disk_root,
        db_type=driver_spec.db_type,
        compute_hash=False,
    )

    assert report.inserted_files == 1
    assert report.errors == []

    from LiuXin_alpha.databases.database import Database

    with Database(
        metadata={"database_path": str(provisioned.db_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
    ) as reopened:
        files = reopened.search("files", "file_store_id", report.store_row_id)
        assert len(files) == 1
        assert files[0]["file_storage_key"] == "one.epub"


def test_legacy_library_wrapper_re_exports_canonical_api() -> None:
    from LiuXin_alpha.library import unmanaged_disk_ingest as legacy_ingest
    from LiuXin_alpha.storage import reconcile

    assert (
        legacy_ingest.register_existing_disk_as_unmanaged_store
        is reconcile.register_existing_disk_as_unmanaged_store
    )
    assert (
        legacy_ingest.register_existing_disk_with_database_path
        is reconcile.register_existing_disk_with_database_path
    )


def test_register_existing_disk_refreshes_db_storage_manager(db, tmp_path: Path) -> None:
    disk_root = tmp_path / "unmanaged_refresh"
    _write_file(disk_root / "book.epub", b"payload")

    report = register_existing_disk_as_unmanaged_store(
        db,
        disk_root,
        store_name="refresh_store",
    )

    assert db.storage is not None
    store = db.storage.get_store("refresh_store")
    assert store.url == str(disk_root.resolve())

    got = db.storage.retrieve_file(metadata={"file_storage_key": "book.epub", "file_store_id": report.store_row_id})
    assert got.as_bytes() == b"payload"


def test_register_existing_disk_can_skip_storage_manager_refresh(db, tmp_path: Path) -> None:
    disk_root = tmp_path / "unmanaged_no_refresh"
    _write_file(disk_root / "book.epub", b"payload")

    register_existing_disk_as_unmanaged_store(
        db,
        disk_root,
        store_name="no_refresh_store",
        refresh_storage_manager=False,
    )

    assert db.storage is not None
    with pytest.raises(KeyError):
        db.storage.get_store("no_refresh_store")
