"""
Verify unmanaged local registration against real temporary bytes and catalogue rows.

Cases cover suffix selection, repeated upserts, database reopening, compatibility
export identity, and manager bootstrap visibility. Legacy hash fields are checked
for presence only; they are not asserted to contain a standard SHA-256 digest.

Example:
    >>> test_register_existing_disk_creates_store_and_files(db, tmp_path)  # doctest: +SKIP
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

import pytest

from LiuXin_alpha.storage.api import StoreConfigurationNotFound
from LiuXin_alpha.storage.reconcile import (
    register_existing_disk_as_unmanaged_store,
    register_existing_disk_with_database_path,
)
from tests.support._surface_storage_tables import ensure_surface_asset_tables


def _write_file(path: Path, payload: bytes) -> None:
    """
    Create parent directories and write the requested fixture bytes, replacing any existing file.

    Example:
        >>> _write_file(path, b"book")  # doctest: +SKIP


    :param path: Temporary destination Path.
    :param payload: Exact bytes written by Path.write_bytes.
    :return: None after writing; filesystem errors propagate.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def test_register_existing_disk_creates_store_and_files(db, tmp_path: Path) -> None:
    """
    Register real local files selected by suffix into the provisioned catalogue and skip the JPEG.

    Assert resolved Store metadata, three keys/extensions, positive sizes, and nonempty legacy hash
    values, plus links when supported. The hash assertion does not establish SHA-256 correctness or
    ebook-format validity.

    Example:
        >>> test_register_existing_disk_creates_store_and_files(db, tmp_path)  # doctest: +SKIP


    :param db: Provisioned catalogue fixture receiving actual legacy Store/file/link writes.
    :param tmp_path: Pytest temporary directory for real local source bytes and any explicitly created catalogue/archive.
    :return: None after the stated regression assertions pass.
    """
    ensure_surface_asset_tables(db)
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
    """
    Repeat registration without duplicate rows, then rewrite one source and observe its new size.

    Use real temporary bytes and catalogue operations; the unchanged pass asserts zero updates
    despite fresh volatile timestamps.

    Example:
        >>> test_register_existing_disk_is_idempotent_and_updates_changed_files(db, tmp_path)  # doctest: +SKIP


    :param db: Provisioned catalogue fixture receiving actual legacy Store/file/link writes.
    :param tmp_path: Pytest temporary directory for real local source bytes and any explicitly created catalogue/archive.
    :return: None after the stated regression assertions pass.
    """
    ensure_surface_asset_tables(db)
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
    """
    Open an existing provisioned catalogue through the path wrapper and verify persistence after
    reopening it.

    Register one real file with hashing disabled, then query the stored key in a new Database
    context within the same process.

    Example:
        >>> test_register_existing_disk_with_database_path_helper(provision_test_database, driver_spec, tmp_path)  # doctest: +SKIP


    :param provision_test_database: Fixture callable provisioning the named catalogue for reopen tests.
    :param driver_spec: Fixture selecting the catalogue database adapter.
    :param tmp_path: Pytest temporary directory for real local source bytes and any explicitly created catalogue/archive.
    :return: None after the stated regression assertions pass.
    """
    from LiuXin_alpha.databases.database import Database

    provisioned = provision_test_database("test_db_13")
    with Database(
        metadata={"database_path": str(provisioned.db_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
    ) as seeded:
        ensure_surface_asset_tables(seeded)
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

    with Database(
        metadata={"database_path": str(provisioned.db_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
    ) as reopened:
        files = reopened.search("files", "file_store_id", report.store_row_id)
        assert len(files) == 1
        assert files[0]["file_storage_key"] == "one.epub"




def test_register_existing_disk_refreshes_db_storage_manager(db, tmp_path: Path) -> None:
    """
    Bootstrap the database manager after local registration and read the original bytes through the
    resulting Store.

    Use the persisted Store UUID and real local backend to verify manager visibility, beyond
    legacy-row insertion alone.

    Example:
        >>> test_register_existing_disk_refreshes_db_storage_manager(db, tmp_path)  # doctest: +SKIP


    :param db: Provisioned catalogue fixture receiving actual legacy Store/file/link writes.
    :param tmp_path: Pytest temporary directory for real local source bytes and any explicitly created catalogue/archive.
    :return: None after the stated regression assertions pass.
    """
    ensure_surface_asset_tables(db)
    disk_root = tmp_path / "unmanaged_refresh"
    _write_file(disk_root / "book.epub", b"payload")

    report = register_existing_disk_as_unmanaged_store(
        db,
        disk_root,
        store_name="refresh_store",
    )

    assert db.storage is not None
    row = db.get_row_from_id("stores", report.store_row_id)
    assert row is not None
    store = db.storage.get_store(UUID(str(row["store_uuid"])))
    assert store.root_path == disk_root.resolve()

    location = store.locate("book.epub")
    assert db.storage.read_bytes(location) == b"payload"


def test_register_existing_disk_can_skip_storage_manager_refresh(db, tmp_path: Path) -> None:
    """
    Persist a local Store declaration while leaving it absent from the existing manager when refresh
    is disabled.

    Assert the row exists and UUID lookup raises StoreConfigurationNotFound; source bytes remain on
    disk.

    Example:
        >>> test_register_existing_disk_can_skip_storage_manager_refresh(db, tmp_path)  # doctest: +SKIP


    :param db: Provisioned catalogue fixture receiving actual legacy Store/file/link writes.
    :param tmp_path: Pytest temporary directory for real local source bytes and any explicitly created catalogue/archive.
    :return: None after the stated regression assertions pass.
    """
    ensure_surface_asset_tables(db)
    disk_root = tmp_path / "unmanaged_no_refresh"
    _write_file(disk_root / "book.epub", b"payload")

    register_existing_disk_as_unmanaged_store(
        db,
        disk_root,
        store_name="no_refresh_store",
        refresh_storage_manager=False,
    )

    assert db.storage is not None
    rows = db.search("stores", "store_name", "no_refresh_store")
    assert len(rows) == 1
    with pytest.raises(StoreConfigurationNotFound):
        db.storage.get_store(UUID(str(rows[0]["store_uuid"])))
