"""Exercise read-only policy through real registry, Store, driver and reload paths."""

from __future__ import annotations

import dataclasses
import hashlib
import io
import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.durable_manager import StorageManager
from LiuXin_alpha.storage.utils.backend_registry import DEFAULT_BACKEND_REGISTRY

KINDS = (
    "on_disk_existing_managed_drive",
    "on_disk_flat",
    "on_disk_calibre_like",
    "single_file_sqlite",
)


def _seed(tmp_path: Path, kind: str) -> tuple[api.StoreConfiguration, Path]:
    """Create a writable Store, then freeze a rollback-journal fixture for byte comparisons."""
    parent = tmp_path / "storage"
    parent.mkdir()
    root = parent / ("objects.sqlite" if kind == "single_file_sqlite" else "objects")
    configuration = api.StoreConfiguration(
        uuid4(),
        kind,
        kind,
        root.as_uri(),
        supports_folders=kind != "single_file_sqlite",
    )
    store = DEFAULT_BACKEND_REGISTRY.build(configuration)
    try:
        store.startup()
        store.store_bytes(b"original", location="existing")
    finally:
        store.close()
    if kind == "single_file_sqlite":
        connection = sqlite3.connect(root)
        try:
            connection.execute("PRAGMA journal_mode=DELETE")
        finally:
            connection.close()
    return configuration, parent


def _snapshot(parent: Path) -> dict[str, tuple[bytes, int] | None]:
    """Include bytes, modification times and directory names, including SQLite sidecars."""
    return {
        str(path.relative_to(parent)): (
            (path.read_bytes(), path.stat().st_mtime_ns) if path.is_file() else None
        )
        for path in parent.rglob("*")
    }


def _refuse_mutations(store, manager) -> None:
    """Reject publication/deletion without consuming borrowed streams or leaving Asset claims."""
    existing = store.locate("existing")
    new = store.locate("new")
    source = io.BytesIO(b"replacement")
    assets = tuple(manager.iter_digital_asset_records())
    replicas = tuple(manager.iter_replica_records())
    operations = (
        lambda: store.begin_write(new),
        lambda: store.store_bytes(b"new", location=new),
        lambda: store.store_bytes(
            b"replacement", location=existing, write_mode="replace"
        ),
        lambda: store.store_stream(source, location=new),
        lambda: store.delete(existing),
        lambda: store.copy(existing, new),
        lambda: store.move(existing, new),
        lambda: manager.put(new, source),
        lambda: manager.put(existing, source, mode=api.WriteMode.REPLACE),
        lambda: manager.delete(existing),
        lambda: manager.store_bytes(b"new", store=store.store_ref),
    )
    for operation in operations:
        with pytest.raises(api.StorageReadOnly):
            operation()
        assert source.tell() == 0
        assert store.read_bytes(existing) == b"original"
        assert store.stat(existing).size == len(b"original")
        assert tuple(manager.iter_digital_asset_records()) == assets
        assert tuple(manager.iter_replica_records()) == replicas
    driver = store.driver
    old_address = driver.parse_object_address("existing")
    new_address = driver.parse_object_address("new")
    for operation in (
        lambda: driver.begin_write(new_address),
        lambda: driver.begin_write(old_address, mode=api.WriteMode.REPLACE),
        lambda: driver.delete(old_address),
        lambda: driver.allocate_object_address(),
    ):
        with pytest.raises(api.StorageReadOnly):
            operation()


@pytest.mark.parametrize("kind", KINDS)
def test_registry_read_only_preserves_bytes_and_metadata(tmp_path, kind):
    """Registry-built facades and raw drivers reject writes without mutating storage."""
    configuration, parent = _seed(tmp_path, kind)
    before = _snapshot(parent)
    store = DEFAULT_BACKEND_REGISTRY.build(
        dataclasses.replace(configuration, read_only=True)
    )
    with StorageManager(stores=[store], startup_on_add=True) as manager:
        assert store.configuration.read_only
        assert store.startup().available
        assert not store.status().writable
        assert not store.capabilities.create
        assert not store.capabilities.replace
        assert not store.capabilities.delete
        assert list(location.key for location in store.iter_locations()) == ["existing"]
        _refuse_mutations(store, manager)
    assert _snapshot(parent) == before


@pytest.mark.parametrize("kind", KINDS)
def test_writable_registry_backends_still_publish_replace_and_delete(tmp_path, kind):
    """The read-only fix retains working writable defaults through both public routes."""
    configuration, _ = _seed(tmp_path, kind)
    store = DEFAULT_BACKEND_REGISTRY.build(configuration)
    with StorageManager(stores=[store], startup_on_add=True) as manager:
        existing = store.locate("existing")
        manager.put(existing, io.BytesIO(b"replacement"), mode=api.WriteMode.REPLACE)
        assert store.read_bytes(existing) == b"replacement"
        new = store.store_bytes(b"new", location="new").location
        assert manager.read_bytes(new) == b"new"
        # Manager deletion now requires registered claims and loss-policy evidence.
        with pytest.raises(api.StoragePolicyUnsatisfied):
            manager.delete(existing)
        assert store.read_bytes(existing) == b"replacement"
        store.delete(existing)
        store.delete(new)
        assert list(store.iter_locations()) == []


@pytest.mark.parametrize("kind", KINDS)
def test_read_only_construction_does_not_create_missing_storage(tmp_path, kind):
    """An unavailable read-only target must not become a new empty Store on startup."""
    root = tmp_path / "missing-parent" / "missing-store"
    configuration = api.StoreConfiguration(
        uuid4(), kind, kind, root.as_uri(), read_only=True
    )
    if kind == "single_file_sqlite":
        with pytest.raises(api.StorageError):
            DEFAULT_BACKEND_REGISTRY.build(configuration)
    else:
        store = DEFAULT_BACKEND_REGISTRY.build(configuration)
        try:
            assert not store.startup().available
            assert not store.probe().available
        finally:
            store.close()
    assert not root.parent.exists()


def test_intrinsically_read_only_backend_cannot_be_made_writable(tmp_path):
    """The unmanaged family retains its stricter policy even with a writable request."""
    (tmp_path / "existing").write_bytes(b"original")
    configuration = api.StoreConfiguration(
        uuid4(),
        "unmanaged",
        "on_disk_existing_unmanaged_drive",
        tmp_path.as_uri(),
        read_only=False,
    )
    store = DEFAULT_BACKEND_REGISTRY.build(configuration)
    with StorageManager(stores=[store], startup_on_add=True) as manager:
        assert store.configuration.read_only
        _refuse_mutations(store, manager)


@pytest.mark.parametrize("kind", KINDS)
def test_durable_reload_preserves_read_only_policy(tmp_path, driver_spec, kind):
    """Read-only rejection survives a real catalogue close/reopen and Store reload."""
    configuration, parent = _seed(tmp_path, kind)
    before = _snapshot(parent)
    database_path = tmp_path / "catalogue.sqlite"
    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        enable_storage_manager=False,
    ) as database:
        Row.from_idless_row_dict(
            database,
            table="stores",
            row_dict={
                "store_uuid": str(configuration.store_uuid),
                "store_name": kind,
                "store_kind": kind,
                "store_root_uri": configuration.store_root_uri,
                "store_access_protocol": "sqlite"
                if kind == "single_file_sqlite"
                else "file",
                "store_is_read_only": 1,
                "store_online_status": "online",
            },
        )
    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        enable_storage_manager=False,
    ) as database:
        with StorageManager(db=database, startup_on_add=True) as manager:
            report = manager.load_from_database()
            assert report.failed_configurations == 0
            assert report.loaded_stores == 1
            first = manager.get_store(configuration.store_uuid)
            _refuse_mutations(first, manager)
            report = manager.reload_stores(replace_existing=True)
            assert report.failed_configurations == 0
            second = manager.get_store(configuration.store_uuid)
            assert second is not first
            assert second.configuration.read_only
            _refuse_mutations(second, manager)
            row = database.search(
                "stores", "store_uuid", str(configuration.store_uuid)
            )[0]
            assert int(row["store_is_read_only"]) == 1
    assert _snapshot(parent) == before


def test_sqlite_read_only_skips_schema_setup_and_engine_rejects_updates(tmp_path):
    """A missing optional index stays absent, and SQLite itself refuses writes."""
    configuration, parent = _seed(tmp_path, "single_file_sqlite")
    connection = sqlite3.connect(parent / "objects.sqlite")
    try:
        connection.execute("DROP INDEX storage_objects_sha256")
        connection.commit()
    finally:
        connection.close()
    before = _snapshot(parent)
    store = DEFAULT_BACKEND_REGISTRY.build(
        dataclasses.replace(configuration, read_only=True)
    )
    try:
        assert store.read_file("existing") == b"original"
        with pytest.raises(api.StorageReadOnly):
            with store.driver._connection("read-only regression") as connection:
                connection.execute("DELETE FROM storage_objects")
        assert store.read_file("existing") == b"original"
    finally:
        store.close()
    assert _snapshot(parent) == before


def test_sqlite_read_only_does_not_initialize_an_unrelated_database(tmp_path):
    """An existing database without the object schema is rejected without adding it."""
    path = tmp_path / "unrelated.sqlite"
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE unrelated (value TEXT)")
        connection.commit()
    finally:
        connection.close()
    before = _snapshot(tmp_path)
    configuration = api.StoreConfiguration(
        uuid4(),
        "unrelated",
        "single_file_sqlite",
        path.as_uri(),
        read_only=True,
    )
    with pytest.raises(api.StorageError):
        DEFAULT_BACKEND_REGISTRY.build(configuration)
    assert _snapshot(tmp_path) == before


def test_sqlite_read_only_sees_live_wal_commits_with_escaped_filename(tmp_path):
    """Read-only mode must retain locking/change detection and consume committed WAL rows."""
    configuration, parent = _seed(tmp_path, "single_file_sqlite")
    path = parent / "objects ?# café.sqlite"
    (parent / "objects.sqlite").rename(path)
    configuration = dataclasses.replace(
        configuration,
        store_root_uri=path.as_uri(),
        read_only=True,
    )
    writer = sqlite3.connect(path)
    try:
        writer.execute("PRAGMA journal_mode=WAL")
        store = DEFAULT_BACKEND_REGISTRY.build(configuration)
        try:
            for payload in (b"first WAL commit", b"second WAL commit"):
                writer.execute(
                    "UPDATE storage_objects SET object_bytes=?, object_size=?, sha256=?, "
                    "version=version+1 WHERE object_key='existing'",
                    (payload, len(payload), hashlib.sha256(payload).hexdigest()),
                )
                writer.commit()
                database_bytes = path.read_bytes()
                wal_bytes = Path(str(path) + "-wal").read_bytes()
                assert store.read_file("existing") == payload
                assert store.probe().available
                assert path.read_bytes() == database_bytes
                assert Path(str(path) + "-wal").read_bytes() == wal_bytes
        finally:
            store.close()
    finally:
        writer.close()
