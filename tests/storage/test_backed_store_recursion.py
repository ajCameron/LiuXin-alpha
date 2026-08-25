"""End-to-end coverage for Store views backed by catalogued Assets."""

from __future__ import annotations

import io
import zipfile

from pathlib import Path

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.store_manager import StorageManager


def _zip_bytes(name: str, payload: bytes) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(name, payload)
    return output.getvalue()


def _open_database(path: Path, *, create: bool) -> Database:
    return Database(
        metadata={"database_path": str(path)},
        db_type="SQLite",
        create=create,
        backup=False,
        enable_storage_manager=False,
    )


def test_nested_backed_zip_store_materializes_and_survives_database_restart(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "catalogue.sqlite"
    source_root = tmp_path / "source"
    source_root.mkdir()
    inner_bytes = _zip_bytes("book.epub", b"nested book payload")
    (source_root / "outer.zip").write_bytes(
        _zip_bytes("packs/inner.zip", inner_bytes)
    )

    with _open_database(database_path, create=True) as database:
        manager = StorageManager(db=database, startup_on_add=True)
        source_configuration = manager.add_store(
            "source drive",
            "on_disk_existing_unmanaged_drive",
            source_root,
            protocol="file",
            modes=(api.ReplicaMode.UNMANAGED,),
            read_only=True,
        )
        cache_configuration = manager.add_filesystem_store(
            "container materialization",
            tmp_path / "materialized",
            modes=(api.ReplicaMode.CACHE,),
            operational_role="cache",
        )
        source = manager.get_store(source_configuration.store_uuid)
        outer_result = manager.adopt_location(
            source.locate("outer.zip"),
            metadata=api.DigitalAssetMetadata(original_name="outer.zip"),
            replica_mode=api.ReplicaMode.UNMANAGED,
            verify=True,
        )
        outer_configuration = manager.add_backed_store(
            "outer archive",
            "zip_readonly",
            outer_result.asset_record.digital_asset_id,
            source_replica_id=outer_result.replica_record.replica_id,
            protocol="zip",
        )
        outer = manager.get_store(outer_configuration.store_uuid)
        inner_result = manager.adopt_location(
            outer.locate("packs/inner.zip"),
            metadata=api.DigitalAssetMetadata(original_name="inner.zip"),
            replica_mode=api.ReplicaMode.ARCHIVE,
            verify=True,
        )
        inner_configuration = manager.add_backed_store(
            "inner archive",
            "zip_readonly",
            inner_result.asset_record.digital_asset_id,
            source_replica_id=inner_result.replica_record.replica_id,
            materialization_store_ref=cache_configuration.store_uuid,
            protocol="zip",
        )
        inner = manager.get_store(inner_configuration.store_uuid)

        assert inner.read_file("book.epub") == b"nested book payload"
        [cached] = tuple(
            manager.iter_replica_records(
                digital_asset_id=inner_result.asset_record.digital_asset_id,
                mode=api.ReplicaMode.CACHE,
            )
        )
        assert cached.location.store_ref == cache_configuration.store_uuid
        assert outer.configuration is outer_configuration
        assert inner.configuration is inner_configuration
        manager.close()

    with _open_database(database_path, create=False) as database:
        reloaded = StorageManager(db=database, startup_on_add=True)
        report = reloaded.load_from_database(startup=True)

        assert report.ok, report.issues
        restored_inner_configuration = reloaded.get_store_configuration(
            inner_configuration.store_uuid
        )
        restored_cache_configuration = reloaded.get_store_configuration(
            cache_configuration.store_uuid
        )
        assert restored_inner_configuration.backing == inner_configuration.backing
        assert restored_inner_configuration.supported_replica_modes == frozenset(
            {api.ReplicaMode.ARCHIVE}
        )
        assert restored_cache_configuration.supported_replica_modes == frozenset(
            {api.ReplicaMode.CACHE}
        )
        assert reloaded.get_store(
            inner_configuration.store_uuid
        ).read_file("book.epub") == b"nested book payload"
        reloaded.close()
