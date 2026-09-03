from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.library import Library
from LiuXin_alpha.storage.stores import FilesystemStore
from tests.support._surface_storage_tables import ensure_surface_asset_tables


def _insert_store_row(
    db: Database,
    *,
    name: str,
    kind: str,
    root_uri: str,
    access_protocol: str = "file",
    is_read_only: int = 0,
) -> UUID:
    store_ref = uuid4()
    Row.from_idless_row_dict(
        db,
        row_dict={
            "store_uuid": str(store_ref),
            "store_name": name,
            "store_kind": kind,
            "store_access_protocol": access_protocol,
            "store_root_uri": root_uri,
            "store_is_read_only": int(is_read_only),
            "store_online_status": "online",
        },
        table="stores",
    )
    return store_ref


def test_library_facade_stores_and_reads_assets_by_id_hash_and_location(
    driver_spec,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "unified_library.sqlite"
    managed_root = tmp_path / "managed_store"
    managed_root.mkdir(parents=True, exist_ok=True)

    with Library(
        database_path=db_path,
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as library:
        ensure_surface_asset_tables(library.db)
        store_ref = _insert_store_row(
            library.db,
            name="managed",
            kind="on_disk_existing_managed_drive",
            root_uri=str(managed_root),
        )

        report = library.refresh_storage(clear_existing=True, startup_on_add=True)
        assert report.loaded_stores == 1
        assert library.storage_bootstrap_report is report
        assert library.get_store(store_ref).store_ref == store_ref
        assert list(library.iter_stores()) == [library.get_store(store_ref)]

        asset = library.add_file(
            b"facade-bytes",
            store=store_ref,
            original_name="book.epub",
        )
        digest = next(value for value in asset.digests if value.algorithm == "sha256")
        location = library.locate_file(asset)

        assert library.read_file(asset.digital_asset_id) == b"facade-bytes"
        assert library.read_file(digest) == b"facade-bytes"
        assert library.read_location(location, offset=7) == b"bytes"
        with library.open_file(asset) as source:
            assert source.read() == b"facade-bytes"
            assert not source.writable()
        assert list(library.iter_files()) == [asset]


def test_library_facade_ingests_an_enumerable_store(
    driver_spec,
    tmp_path: Path,
) -> None:
    destination_root = tmp_path / "destination"
    destination_root.mkdir()
    source = FilesystemStore(tmp_path / "source")
    source.store_bytes(b"imported", location="incoming/book.epub")

    with Library(
        database_path=tmp_path / "store-ingest.sqlite",
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as library:
        store_ref = _insert_store_row(
            library.db,
            name="destination",
            kind="on_disk_existing_managed_drive",
            root_uri=str(destination_root),
        )
        library.refresh_storage(clear_existing=True, startup_on_add=True)

        report = library.ingest_store(source, extensions={"epub"})

        assert report.ok and report.ingested_files == 1
        [item] = report.items
        assert item.result.location.store_ref == store_ref
        assert library.read_file(item.result.asset_record) == b"imported"
        assert item.result.asset_record.metadata.original_name == "book.epub"


def test_library_facade_registers_and_routes_unmanaged_disk(
    driver_spec,
    tmp_path: Path,
) -> None:
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
    ) as library:
        ensure_surface_asset_tables(library.db)
        report = library.register_unmanaged_disk(
            unmanaged_root,
            store_name="unmanaged",
        )

        assert report.inserted_files == 1
        assert report.errors == []
        stores = list(library.iter_stores())
        assert len(stores) == 1
        location = stores[0].locate("book.epub")
        assert library.read_location(location) == b"ebook"


def test_library_replica_deletion_is_explicit_and_tombstoned(
    driver_spec,
    tmp_path: Path,
) -> None:
    with Library(
        database_path=tmp_path / "delete.sqlite",
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
    ) as library:
        root = tmp_path / "delete-store"
        root.mkdir()
        store_ref = _insert_store_row(
            library.db,
            name="delete-store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(root),
        )
        library.refresh_storage(clear_existing=True, startup_on_add=True)
        asset = library.add_file(b"remove", store=store_ref)
        replica = next(
            library.storage.iter_replica_records(
                digital_asset_id=asset.digital_asset_id
            )
        )

        report = library.delete_file(replica)

        assert report.bytes_deleted
        assert report.tombstone_retained
        assert not library.storage.exists(replica.location)


def test_library_facade_can_wrap_existing_database_without_owning_close(
    driver_spec,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "unified_external.sqlite"
    db = Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
    )
    try:
        library = Library(database=db)
        library.close()
        assert getattr(db, "_driver", None) is not None
    finally:
        db.close()
