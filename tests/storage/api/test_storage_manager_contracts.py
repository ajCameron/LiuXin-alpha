"""Application-facing manager integration and bootstrap contracts."""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest

from LiuXin_alpha.storage.api import (
    Digest,
    StorageBootstrapReport,
    StoreAlreadyExists,
    StoreConfiguration,
    StoreConfigurationNotFound,
    StoreUnavailable,
)
from LiuXin_alpha.storage.store_manager import StorageManager
from LiuXin_alpha.storage.stores import FilesystemStore


def _store(path: Path, name: str) -> FilesystemStore:
    return FilesystemStore(path, name=name)


def test_manager_initialization_accepts_only_new_store_api_and_starts_stores(
    tmp_path: Path,
) -> None:
    first = _store(tmp_path / "first", "first")
    manager = StorageManager(stores=[first], startup_on_add=True)

    assert manager.get_store(first.store_ref) is first
    assert manager.get_default_store_ref() == first.store_ref
    assert first.status().available is True
    with pytest.raises(TypeError, match="StoreAPI"):
        StorageManager(stores=[object()])  # type: ignore[list-item]


def test_manager_registration_is_uuid_routed_and_duplicate_safe(tmp_path: Path) -> None:
    first = _store(tmp_path / "first", "same-name")
    second = FilesystemStore(
        tmp_path / "second",
        name="same-name",
        uuid=first.store_ref,
    )
    manager = StorageManager(stores=[first], startup_on_add=False)

    with pytest.raises(StoreAlreadyExists):
        manager.add_store(second, startup=False)
    with pytest.raises(StoreConfigurationNotFound):
        manager.get_store(uuid4())


def test_manager_adds_filesystem_store_from_a_path_without_configuration_boilerplate(
    tmp_path: Path,
) -> None:
    root = tmp_path / "primary 😀 store"

    with StorageManager() as manager:
        configuration = manager.add_filesystem_store(
            "primary",
            root,
            tags={"local", "fast"},
            operational_role="live",
        )
        asset = manager.store_bytes(
            b"configured through the public convenience API",
            original_name="book.epub",
        )

        assert configuration.store_root_uri == root.resolve().as_uri()
        assert configuration.store_kind == "filesystem"
        assert configuration.store_access_protocol == "file"
        assert set(configuration.store_tags) == {"local", "fast"}
        assert manager.get_default_store_ref() == configuration.store_uuid
        assert manager.read_asset(asset) == (
            b"configured through the public convenience API"
        )
        assert root.is_dir()


def test_concrete_manager_add_store_supports_generic_and_object_forms(
    tmp_path: Path,
) -> None:
    attached = FilesystemStore(tmp_path / "attached", name="attached")

    with StorageManager() as manager:
        generic = manager.add_store(
            "generic",
            "filesystem",
            tmp_path / "generic",
            protocol="file",
            startup=True,
        )
        keyword = manager.add_store(
            name="keyword",
            kind="filesystem",
            root=tmp_path / "keyword",
        )
        attached_configuration = manager.add_store(attached)

        assert generic.store_root_uri == (
            tmp_path / "generic"
        ).resolve().as_uri()
        assert manager.get_store(generic.store_uuid).status().available
        assert keyword.store_root_uri == (
            tmp_path / "keyword"
        ).resolve().as_uri()
        assert manager.get_store(keyword.store_uuid).status().available
        assert attached_configuration == attached.configuration
        assert manager.get_store(attached.store_ref) is attached

        with pytest.raises(TypeError, match="only configuration and startup"):
            manager.add_store(attached, "filesystem", tmp_path / "invalid")


def test_manager_convenience_stores_and_reads_by_asset_id_or_hash(tmp_path: Path) -> None:
    store = _store(tmp_path / "managed", "managed")
    manager = StorageManager(stores=[store], startup_on_add=True)

    asset = manager.store_bytes(
        b"manager-payload",
        original_name="book.epub",
        verify=True,
    )
    digest = next(
        digest
        for digest in asset.digests
        if digest.algorithm == "sha256"
    )

    assert manager.read_file(asset.digital_asset_id) == b"manager-payload"
    assert manager.read_file(digest) == b"manager-payload"
    assert manager.read_file(digest.value) == b"manager-payload"
    assert manager.read_file(asset.digital_asset_id, offset=8, length=7) == b"payload"


def test_manager_routes_locations_and_changes_default_store_explicitly(
    tmp_path: Path,
) -> None:
    first = _store(tmp_path / "first", "first")
    second = _store(tmp_path / "second", "second")
    manager = StorageManager(stores=[first, second], startup_on_add=True)
    manager.set_default_store(second.store_ref)

    asset = manager.store_bytes(b"two", store=second.store_ref)
    replica = next(
        manager.iter_replica_records(digital_asset_id=asset.digital_asset_id)
    )

    assert replica.location.store_ref == second.store_ref
    assert manager.read_file(asset.digital_asset_id) == b"two"


class _RowsDatabase:
    def __init__(self, rows):
        self.rows = rows

    def get_tables(self):
        return ["stores"]

    def get_all_rows(self, table: str, *, iterator_return: bool):
        assert table == "stores"
        assert iterator_return is False
        return self.rows

    def get_row_from_id(self, table: str, row_id: int):
        assert table == "stores"
        return next(
            (row for row in self.rows if int(row["store_id"]) == row_id),
            None,
        )


class _RowsMacros:
    def __init__(self, rows):
        self.rows = rows

    def update_row(self, table, row_id, values, *, id_column=None):
        assert table == "stores"
        assert id_column == "store_id"
        row = next(row for row in self.rows if row["store_id"] == row_id)
        row.update(values)


class _WritableRowsDatabase(_RowsDatabase):
    def __init__(self, rows):
        super().__init__(rows)
        self.macros = _RowsMacros(rows)


def test_database_bootstrap_reports_loaded_skipped_and_failed_configurations(
    tmp_path: Path,
) -> None:
    good_uuid = uuid4()
    rows = [
        {
            "store_id": 1,
            "store_uuid": str(good_uuid),
            "store_name": "good",
            "store_kind": "filesystem",
            "store_root_uri": (tmp_path / "good").resolve().as_uri(),
            "store_online_status": "online",
        },
        {
            "store_id": 2,
            "store_uuid": str(uuid4()),
            "store_name": "offline",
            "store_kind": "filesystem",
            "store_root_uri": (tmp_path / "offline").resolve().as_uri(),
            "store_online_status": "offline",
        },
        {
            "store_id": 3,
            "store_name": "broken",
            "store_kind": "unknown-kind",
            "store_root_uri": "unknown://broken",
        },
    ]
    manager = StorageManager(db=_RowsDatabase(rows), startup_on_add=False)

    report = manager.load_from_database(startup=False)

    assert report == StorageBootstrapReport(
        discovered_configurations=3,
        loaded_stores=1,
        skipped_configurations=1,
        failed_configurations=1,
        issues=report.issues,
    )
    assert report.ok is False
    assert manager.get_store(good_uuid).configuration.store_uuid == good_uuid
    assert [issue.store_name for issue in report.issues] == ["offline", "broken"]


def test_database_rows_without_uuid_get_stable_derived_identity(tmp_path: Path) -> None:
    row = {
        "store_id": 42,
        "store_name": "legacy-row",
        "store_kind": "filesystem",
        "store_root_uri": (tmp_path / "legacy").resolve().as_uri(),
    }
    database = _RowsDatabase([row])
    manager = StorageManager(db=database, startup_on_add=False)

    first = manager.get_store_configuration_from_db(42)
    second = manager.get_store_configuration_from_db(42)

    assert isinstance(first.store_uuid, UUID)
    assert first.store_uuid == second.store_uuid


def test_database_bootstrap_persists_a_derived_legacy_store_uuid(tmp_path: Path) -> None:
    row = {
        "store_id": 43,
        "store_uuid": None,
        "store_name": "legacy-row",
        "store_kind": "filesystem",
        "store_root_uri": (tmp_path / "legacy-persisted").resolve().as_uri(),
    }
    database = _WritableRowsDatabase([row])
    manager = StorageManager(db=database, startup_on_add=False)

    report = manager.load_from_database(startup=False)

    assert report.loaded_stores == 1
    persisted_ref = UUID(str(row["store_uuid"]))
    assert manager.get_store(persisted_ref).store_ref == persisted_ref


def test_database_bound_reload_reconciles_added_changed_and_removed_rows(
    tmp_path: Path,
) -> None:
    primary_ref = uuid4()
    archive_ref = uuid4()
    primary_row = {
        "store_id": 1,
        "store_uuid": str(primary_ref),
        "store_name": "primary",
        "store_kind": "filesystem",
        "store_root_uri": (tmp_path / "primary-v1").resolve().as_uri(),
        "store_online_status": "online",
    }
    archive_row = {
        "store_id": 2,
        "store_uuid": str(archive_ref),
        "store_name": "archive",
        "store_kind": "filesystem",
        "store_root_uri": (tmp_path / "archive").resolve().as_uri(),
        "store_online_status": "online",
    }
    database = _RowsDatabase([primary_row])
    manager = StorageManager(db=database, startup_on_add=False)

    assert manager.load_from_database(startup=False).loaded_stores == 1
    first_primary = manager.get_store(primary_ref)

    primary_row["store_name"] = "primary-renamed"
    primary_row["store_root_uri"] = (
        tmp_path / "primary-v2"
    ).resolve().as_uri()
    database.rows.append(archive_row)
    changed = manager.reload_stores()

    assert changed.discovered_configurations == 2
    assert changed.loaded_stores == 2
    assert changed.ok
    replacement = manager.get_store(primary_ref)
    assert replacement is not first_primary
    assert replacement.configuration.store_name == "primary-renamed"
    assert replacement.configuration.store_root_uri.endswith("/primary-v2")
    assert manager.get_store(archive_ref).store_ref == archive_ref

    database.rows[:] = [archive_row]
    removed = manager.reload_stores()

    assert removed.loaded_stores == 1
    with pytest.raises(StoreConfigurationNotFound):
        manager.get_store(primary_ref)

    archive_row["store_online_status"] = "offline"
    offline = manager.reload_stores()

    assert offline.skipped_configurations == 1
    assert offline.issues[0].store_ref == archive_ref
    with pytest.raises(StoreConfigurationNotFound):
        manager.get_store(archive_ref)


def test_database_reload_without_replacement_only_loads_new_rows(
    tmp_path: Path,
) -> None:
    primary_ref = uuid4()
    archive_ref = uuid4()
    primary_row = {
        "store_id": 1,
        "store_uuid": str(primary_ref),
        "store_name": "primary",
        "store_kind": "filesystem",
        "store_root_uri": (tmp_path / "primary").resolve().as_uri(),
    }
    database = _RowsDatabase([primary_row])
    manager = StorageManager(db=database, startup_on_add=False)
    manager.load_from_database(startup=False)
    original = manager.get_store(primary_ref)

    primary_row["store_name"] = "ignored-until-replacement"
    database.rows.append(
        {
            "store_id": 2,
            "store_uuid": str(archive_ref),
            "store_name": "archive",
            "store_kind": "filesystem",
            "store_root_uri": (tmp_path / "archive").resolve().as_uri(),
        }
    )
    report = manager.reload_stores(replace_existing=False)

    assert report.loaded_stores == 1
    assert report.skipped_configurations == 1
    assert manager.get_store(primary_ref) is original
    assert manager.get_store_configuration(primary_ref).store_name == "primary"
    assert manager.get_store(archive_ref).store_ref == archive_ref


def test_failed_database_replacement_keeps_existing_live_store(
    tmp_path: Path,
) -> None:
    store_ref = uuid4()
    row = {
        "store_id": 1,
        "store_uuid": str(store_ref),
        "store_name": "healthy",
        "store_kind": "filesystem",
        "store_root_uri": (tmp_path / "healthy").resolve().as_uri(),
    }
    database = _RowsDatabase([row])

    def factory(configuration: StoreConfiguration) -> FilesystemStore:
        if configuration.store_name == "broken-replacement":
            raise RuntimeError("replacement construction failed")
        return FilesystemStore.from_configuration(configuration)

    manager = StorageManager(
        db=database,
        store_factory=factory,
        startup_on_add=False,
    )
    manager.load_from_database(startup=False)
    original = manager.get_store(store_ref)

    row["store_name"] = "broken-replacement"
    report = manager.reload_stores()

    assert report.failed_configurations == 1
    assert "replacement construction failed" in report.issues[0].reason
    assert manager.get_store(store_ref) is original
    assert manager.get_store_configuration(store_ref).store_name == "healthy"


def test_malformed_database_replacement_keeps_existing_live_store(
    tmp_path: Path,
) -> None:
    store_ref = uuid4()
    row = {
        "store_id": 1,
        "store_uuid": str(store_ref),
        "store_name": "healthy",
        "store_kind": "filesystem",
        "store_root_uri": (tmp_path / "healthy").resolve().as_uri(),
    }
    manager = StorageManager(
        db=_RowsDatabase([row]),
        startup_on_add=False,
    )
    manager.load_from_database(startup=False)
    original = manager.get_store(store_ref)

    row["store_root_uri"] = None
    report = manager.reload_stores()

    assert report.failed_configurations == 1
    assert report.issues[0].store_ref == store_ref
    assert manager.get_store(store_ref) is original
    assert manager.get_store_configuration(store_ref).store_name == "healthy"


def test_offline_database_row_retains_configuration_for_live_replica(
    tmp_path: Path,
) -> None:
    store_ref = uuid4()
    row = {
        "store_id": 1,
        "store_uuid": str(store_ref),
        "store_name": "primary",
        "store_kind": "filesystem",
        "store_root_uri": (tmp_path / "primary").resolve().as_uri(),
        "store_online_status": "online",
    }
    manager = StorageManager(
        db=_RowsDatabase([row]),
        startup_on_add=True,
    )
    manager.load_from_database(startup=True)
    manager.store_bytes(b"claimed bytes")

    row["store_online_status"] = "offline"
    report = manager.reload_stores()

    assert report.skipped_configurations == 1
    assert manager.get_store_configuration(store_ref).store_uuid == store_ref
    with pytest.raises(StoreUnavailable):
        manager.get_store(store_ref)


def test_unbound_storage_manager_reload_uses_in_memory_configurations(
    tmp_path: Path,
) -> None:
    store_ref = uuid4()
    configuration = StoreConfiguration(
        store_uuid=store_ref,
        store_name="primary",
        store_kind="filesystem",
        store_root_uri=(tmp_path / "primary").resolve().as_uri(),
    )
    manager = StorageManager(startup_on_add=False)
    manager.create_store(configuration, startup=False)
    original = manager.get_store(store_ref)

    report = manager.reload_stores()

    assert report.loaded_stores == 1
    assert report.ok
    assert manager.get_store(store_ref) is not original


def test_manager_factory_can_create_configured_store_without_manual_construction(
    tmp_path: Path,
) -> None:
    configuration = StoreConfiguration(
        store_uuid=uuid4(),
        store_name="created",
        store_kind="filesystem",
        store_root_uri=(tmp_path / "created").resolve().as_uri(),
    )
    manager = StorageManager(startup_on_add=False)

    manager.create_store(configuration, startup=False)

    store = manager.get_store(configuration.store_uuid)
    assert store.store_ref == configuration.store_uuid
    assert store.store_bytes(
        b"created",
        location="created.bin",
        expected_digest=Digest(
            "sha256",
            "406effb1e9c59672c66a598c2b21e331b23b16c54024e96d6df3e7c173549791",
        ),
    ).size == 7
