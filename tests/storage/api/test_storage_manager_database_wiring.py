from __future__ import annotations

import shutil
import subprocess

from pathlib import Path

import pytest

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row


def _insert_store_row(
    db: Database,
    *,
    name: str,
    kind: str,
    root_uri: str,
    access_protocol: str = "file",
    is_read_only: int = 0,
    online_status: str = "online",
) -> int:
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "store_name": name,
            "store_kind": kind,
            "store_access_protocol": access_protocol,
            "store_root_uri": root_uri,
            "store_is_read_only": int(is_read_only),
            "store_online_status": online_status,
        },
        table="stores",
    )
    return int(row["store_id"])


def test_database_bootstrap_storage_manager_loads_stores(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "storage_bootstrap.sqlite"
    managed_root = tmp_path / "managed"
    unmanaged_root = tmp_path / "unmanaged"
    managed_root.mkdir(parents=True, exist_ok=True)
    unmanaged_root.mkdir(parents=True, exist_ok=True)
    (managed_root / "manual.epub").write_bytes(b"managed-book")
    (unmanaged_root / "manual.epub").write_bytes(b"unmanaged-book")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        managed_id = _insert_store_row(
            db,
            name="managed",
            kind="on_disk_existing_managed_drive",
            root_uri=str(managed_root),
            is_read_only=0,
        )
        unmanaged_id = _insert_store_row(
            db,
            name="unmanaged",
            kind="on_disk_existing_unmanaged_drive",
            root_uri=str(unmanaged_root),
            is_read_only=1,
        )

        report = db.bootstrap_storage_manager(startup_on_add=False, clear_existing=True)
        assert report.discovered_rows == 2
        assert report.loaded_stores == 2
        assert report.skipped_rows == 0
        assert report.failed_rows == 0
        assert db.storage is not None

        assert db.storage.get_store("managed").uuid == "store-{}".format(managed_id)
        assert db.storage.get_store("unmanaged").uuid == "store-{}".format(unmanaged_id)

        got = db.storage.retrieve_file(metadata={"file_storage_key": "manual.epub", "file_store_id": managed_id})
        assert got.as_bytes() == b"managed-book"


def test_database_bootstrap_storage_manager_reports_unknown_kind(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "storage_unknown.sqlite"
    mystery_root = tmp_path / "mystery"
    mystery_root.mkdir(parents=True, exist_ok=True)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        _insert_store_row(
            db,
            name="mystery",
            kind="quantum_tape",
            root_uri=str(mystery_root),
            access_protocol="warp",
        )

        report = db.bootstrap_storage_manager(startup_on_add=False, clear_existing=True)

        assert report.discovered_rows == 1
        assert report.loaded_stores == 0
        assert report.skipped_rows == 1
        assert report.failed_rows == 0
        assert report.issues
        assert "unsupported store kind/protocol" in report.issues[0].reason


def test_database_auto_wires_storage_manager_on_startup(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "storage_autowire.sqlite"
    managed_root = tmp_path / "managed_autowire"
    managed_root.mkdir(parents=True, exist_ok=True)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        _insert_store_row(
            db,
            name="auto_managed",
            kind="on_disk_existing_managed_drive",
            root_uri=str(managed_root),
        )

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        storage_startup_on_add=False,
    ) as reopened:
        assert reopened.storage is not None
        assert reopened.storage_bootstrap_report is not None
        assert reopened.storage_bootstrap_report.loaded_stores == 1
        assert reopened.storage.get_store("auto_managed").url == str(managed_root)


def test_database_bootstrap_loads_single_file_sqlite_store(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "storage_single_file.sqlite"
    store_file = tmp_path / "blob_store.sqlite"

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        _insert_store_row(
            db,
            name="blob_store",
            kind="single_file_sqlite",
            root_uri=str(store_file),
            access_protocol="sqlite",
            is_read_only=0,
        )

        report = db.bootstrap_storage_manager(startup_on_add=False, clear_existing=True)
        assert report.discovered_rows == 1
        assert report.loaded_stores == 1
        assert report.failed_rows == 0
        assert report.skipped_rows == 0
        assert db.storage is not None

        file_obj = db.storage.add_file(b"blob payload", preferred_store="blob_store")
        got = db.storage.retrieve_file(file_url=file_obj.file_url, preferred_store="blob_store")
        assert got.as_bytes() == b"blob payload"


def test_database_bootstrap_loads_squashfs_readonly_store(driver_spec, tmp_path: Path) -> None:
    if shutil.which("mksquashfs") is None or shutil.which("unsquashfs") is None:
        pytest.skip("squashfs-tools not available in environment")

    db_path = tmp_path / "storage_squashfs.sqlite"
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True, exist_ok=True)
    (source_root / "books").mkdir(parents=True, exist_ok=True)
    (source_root / "books" / "archive_book.epub").write_bytes(b"ARCHIVE-EPUB")

    archive_path = tmp_path / "library.squashfs"
    subprocess.run(
        ["mksquashfs", str(source_root), str(archive_path), "-noappend", "-quiet"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(
            db,
            name="archive_store",
            kind="squashfs_readonly",
            root_uri=str(archive_path),
            access_protocol="squashfs",
            is_read_only=1,
        )

        report = db.bootstrap_storage_manager(startup_on_add=False, clear_existing=True)
        assert report.discovered_rows == 1
        assert report.loaded_stores == 1
        assert report.failed_rows == 0
        assert report.skipped_rows == 0

        got = db.storage.retrieve_file(
            metadata={"file_storage_key": "books/archive_book.epub", "file_store_id": store_id}
        )
        assert got.as_bytes() == b"ARCHIVE-EPUB"
