from __future__ import annotations

from pathlib import Path

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.library import Library


def _insert_store_row(
    db: Database,
    *,
    name: str,
    kind: str,
    root_uri: str,
    access_protocol: str = "file",
    is_read_only: int = 0,
) -> int:
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "store_name": name,
            "store_kind": kind,
            "store_access_protocol": access_protocol,
            "store_root_uri": root_uri,
            "store_is_read_only": int(is_read_only),
            "store_online_status": "online",
        },
        table="stores",
    )
    return int(row["store_id"])


def test_library_facade_add_and_retrieve_file(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "unified_library.sqlite"
    managed_root = tmp_path / "managed_store"
    managed_root.mkdir(parents=True, exist_ok=True)

    with Library(
        database_path=db_path,
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as lib:
        store_id = _insert_store_row(
            lib.db,
            name="managed",
            kind="on_disk_existing_managed_drive",
            root_uri=str(managed_root),
            is_read_only=0,
        )

        report = lib.refresh_storage(clear_existing=True)
        assert report.loaded_stores == 1
        assert lib.storage_bootstrap_report is report
        assert lib.get_store("managed").uuid == "store-{}".format(store_id)

        added = lib.add_file(b"facade-bytes", preferred_store="managed")
        got = lib.retrieve_file(file_url=added.file_url, preferred_store="managed")
        assert got.as_bytes() == b"facade-bytes"


def test_library_facade_register_unmanaged_disk(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "unified_import.sqlite"
    unmanaged_root = tmp_path / "unmanaged_root"
    unmanaged_root.mkdir(parents=True, exist_ok=True)
    (unmanaged_root / "book.epub").write_bytes(b"ebook")

    with Library(
        database_path=db_path,
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as lib:
        report = lib.register_unmanaged_disk(unmanaged_root, store_name="unmanaged")
        assert report.inserted_files == 1
        assert report.errors == []

        got = lib.retrieve_file(
            metadata={
                "file_storage_key": "book.epub",
                "file_store_id": report.store_row_id,
            }
        )
        assert got.as_bytes() == b"ebook"


def test_library_facade_can_wrap_existing_database_without_owning_close(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "unified_external.sqlite"
    db = Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
    )
    try:
        lib = Library(database=db)
        lib.close()
        assert getattr(db, "_driver", None) is not None
    finally:
        db.close()
