"""Integration coverage for database-backed Store loading and reload."""

from __future__ import annotations

import json

from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.databases.runtime import bootstrap_storage_manager
from LiuXin_alpha.storage.api import (
    NoReadableReplica,
    StorageBootstrapIssue,
    StorageBootstrapReport,
    StorageManagementError,
    StoreConfigurationNotFound,
    StoreUnavailable,
)
from LiuXin_alpha.storage.store_manager import StorageManager
from LiuXin_alpha.storage.stores import EncryptedStore, StaticEncryptionKeyProvider


def _insert_filesystem_store(
    db: Database,
    *,
    store_ref: UUID | None,
    name: str,
    root: Path,
    kind: str = "filesystem",
    read_only: bool = False,
    online_status: str = "online",
) -> int:
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "store_uuid": None if store_ref is None else str(store_ref),
            "store_name": name,
            "store_kind": kind,
            "store_access_protocol": "file",
            "store_root_uri": root.resolve().as_uri(),
            "store_is_read_only": int(read_only),
            "store_online_status": online_status,
        },
        table="stores",
    )
    assert row.row_id is not None
    return int(row.row_id)


def _configuration_refs(database: Database) -> set[UUID]:
    assert database.storage is not None
    return {
        configuration.store_uuid
        for configuration in database.storage.iter_store_configurations()
    }


def _live_refs(database: Database) -> set[UUID]:
    assert database.storage is not None
    return {store.store_ref for store in database.storage.iter_stores()}


def test_database_startup_loads_rows_and_reload_tracks_database_changes(
    driver_spec,
    tmp_path: Path,
    assert_integrity,
) -> None:
    database_path = tmp_path / "database.sqlite"
    primary_ref = uuid4()
    archive_ref = uuid4()
    offline_ref = uuid4()

    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        enable_storage_manager=False,
    ) as setup_database:
        primary_id = _insert_filesystem_store(
            setup_database,
            store_ref=primary_ref,
            name="primary",
            root=tmp_path / "primary-v1",
        )

    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        storage_startup_on_add=False,
    ) as database:
        assert database.storage is not None
        assert database.storage_bootstrap_report.loaded_stores == 1
        assert database.storage.get_store(primary_ref).store_ref == primary_ref
        original_primary = database.storage.get_store(primary_ref)

        primary_row = database.get_row_from_id("stores", primary_id)
        assert primary_row is not None
        primary_row["store_name"] = "primary-renamed"
        primary_row["store_root_uri"] = (
            tmp_path / "primary-v2"
        ).resolve().as_uri()
        primary_row.sync()
        _insert_filesystem_store(
            database,
            store_ref=archive_ref,
            name="archive",
            root=tmp_path / "archive",
        )

        additive = database.storage.reload_stores(replace_existing=False)

        assert additive.discovered_configurations == 2
        assert additive.loaded_stores == 1
        assert additive.skipped_configurations == 1
        assert database.storage.get_store(primary_ref) is original_primary
        assert (
            database.storage.get_store_configuration(primary_ref).store_name
            == "primary"
        )

        changed = database.storage.reload_stores()

        assert changed.discovered_configurations == 2
        assert changed.loaded_stores == 2
        assert changed.ok
        assert (
            database.storage.get_store_configuration(primary_ref).store_name
            == "primary-renamed"
        )
        assert database.storage.get_store(archive_ref).store_ref == archive_ref

        # Authoritative refreshes are deliberately repeatable: each pass
        # rebuilds both facades from the same durable configuration.
        for _ in range(3):
            repeated = database.storage.reload_stores()
            assert repeated.discovered_configurations == 2
            assert repeated.loaded_stores == 2
            assert repeated.skipped_configurations == 0
            assert repeated.failed_configurations == 0
            assert _configuration_refs(database) == {primary_ref, archive_ref}
            assert _live_refs(database) == {primary_ref, archive_ref}

        _insert_filesystem_store(
            database,
            store_ref=offline_ref,
            name="cold archive",
            root=tmp_path / "cold-archive",
            online_status="offline",
        )
        without_offline = database.storage.reload_stores()

        assert without_offline.discovered_configurations == 3
        assert without_offline.loaded_stores == 2
        assert without_offline.skipped_configurations == 1
        assert without_offline.issues[0].store_ref == offline_ref
        assert _configuration_refs(database) == {primary_ref, archive_ref}

        with_offline = database.storage.reload_stores(include_offline=True)

        assert with_offline.discovered_configurations == 3
        assert with_offline.loaded_stores == 3
        assert with_offline.ok
        assert _live_refs(database) == {
            primary_ref,
            archive_ref,
            offline_ref,
        }

        database.storage.set_default_store(primary_ref)
        database.macros.delete_row("stores", primary_id)
        removed = database.storage.reload_stores()

        assert removed.discovered_configurations == 2
        assert removed.loaded_stores == 1
        assert removed.skipped_configurations == 1
        with pytest.raises(StoreConfigurationNotFound):
            database.storage.get_store(primary_ref)
        with pytest.raises(StoreConfigurationNotFound):
            database.storage.get_store(offline_ref)
        assert database.storage.get_default_store_ref() == archive_ref
        assert_integrity(database)
        database.storage.close()


def test_database_bootstrap_constructs_and_uses_local_backend_matrix(
    driver_spec,
    tmp_path: Path,
    assert_integrity,
) -> None:
    database_path = tmp_path / "backend-matrix.sqlite"
    store_specs = (
        ("filesystem", tmp_path / "filesystem", False),
        (
            "on_disk_existing_managed_drive",
            tmp_path / "managed",
            False,
        ),
        ("on_disk_existing_unmanaged_drive", tmp_path / "unmanaged", True),
        ("on_disk_flat", tmp_path / "flat", False),
        ("on_disk_calibre_like", tmp_path / "calibre-like", False),
        ("single_file_sqlite", tmp_path / "blobs.sqlite", False),
    )
    store_refs = {kind: uuid4() for kind, _, _ in store_specs}
    unmanaged_root = tmp_path / "unmanaged"
    unmanaged_root.mkdir()
    (unmanaged_root / "already-there.epub").write_bytes(b"unmanaged payload")

    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        enable_storage_manager=False,
    ) as database:
        for kind, root, read_only in store_specs:
            _insert_filesystem_store(
                database,
                store_ref=store_refs[kind],
                name=f"db {kind}",
                root=root,
                kind=kind,
                read_only=read_only,
            )

        report = database.bootstrap_storage_manager(startup_on_add=True)

        assert report.discovered_configurations == len(store_specs)
        assert report.loaded_stores == len(store_specs)
        assert report.skipped_configurations == 0
        assert report.failed_configurations == 0
        assert _configuration_refs(database) == set(store_refs.values())
        assert _live_refs(database) == set(store_refs.values())
        assert database.storage is not None
        assert (
            database.storage.get_store(
                store_refs["on_disk_existing_unmanaged_drive"]
            ).read_file("already-there.epub")
            == b"unmanaged payload"
        )

        for index, (kind, _, read_only) in enumerate(store_specs):
            if read_only:
                continue
            payload = f"database-backed payload {index}".encode()
            asset = database.storage.store_bytes(
                payload,
                original_name=f"book-{index}.epub",
                store=store_refs[kind],
            )
            assert database.storage.read_asset(asset) == payload

        assert_integrity(database)
        database.storage.close()


def test_database_bootstrap_orders_encrypted_store_after_its_inner_store(
    driver_spec,
    tmp_path: Path,
    assert_integrity,
) -> None:
    database_path = tmp_path / "encrypted-dependency.sqlite"
    encrypted_ref = uuid4()
    inner_ref = uuid4()

    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        enable_storage_manager=False,
    ) as database:
        # Insert the wrapper first to prove bootstrap uses dependency order,
        # rather than relying on incidental store_id order.
        encrypted_row = Row.from_idless_row_dict(
            database,
            row_dict={
                "store_uuid": str(encrypted_ref),
                "store_name": "encrypted",
                "store_kind": "encrypted",
                "store_access_protocol": "encrypted",
                "store_root_uri": f"encrypted://{inner_ref}",
                "store_policy_json": json.dumps(
                    {
                        "backend": "encrypted",
                        "encrypted": {
                            "inner_store_uuid": str(inner_ref),
                            "key_id": "database-key",
                            "chunk_size": 4096,
                            "inner_prefix": "vault",
                        },
                    }
                ),
                "store_online_status": "online",
            },
            table="stores",
        )
        inner_id = _insert_filesystem_store(
            database,
            store_ref=inner_ref,
            name="physical",
            root=tmp_path / "physical",
        )
        assert encrypted_row.row_id is not None
        assert int(encrypted_row.row_id) < inner_id
        database.storage = StorageManager(
            db=database,
            encryption_key_provider=StaticEncryptionKeyProvider(
                {"database-key": b"d" * 32},
                active_key_id="database-key",
            ),
        )

        report = database.bootstrap_storage_manager(startup_on_add=True)

        assert report.discovered_configurations == 2
        assert report.loaded_stores == 2
        assert report.ok
        encrypted = database.storage.get_store(encrypted_ref)
        assert isinstance(encrypted, EncryptedStore)
        stored = encrypted.store_bytes(
            b"database integrated secret",
            location="books/secret.epub",
        )
        assert encrypted.read_file(stored) == b"database integrated secret"
        physical = database.storage.get_store(inner_ref)
        assert physical.file_exists("vault/books/secret.epub")
        assert (
            b"database integrated secret"
            not in physical.read_file("vault/books/secret.epub")
        )
        assert_integrity(database)
        database.storage.close()


def test_database_reload_preserves_last_known_good_store_and_recovers(
    driver_spec,
    tmp_path: Path,
    assert_integrity,
) -> None:
    database_path = tmp_path / "failure-recovery.sqlite"
    healthy_root = tmp_path / "healthy"

    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        enable_storage_manager=False,
    ) as database:
        store_id = _insert_filesystem_store(
            database,
            store_ref=None,
            name="legacy healthy",
            root=healthy_root,
        )
        report = database.bootstrap_storage_manager(startup_on_add=True)

        assert report.loaded_stores == 1
        row = database.get_row_from_id("stores", store_id)
        assert row is not None
        derived_ref = UUID(str(row["store_uuid"]))
        original = database.storage.get_store(derived_ref)
        assert original.status().available
        assert database.storage.get_store_configuration_from_db(
            store_id
        ).store_uuid == derived_ref

        row["store_name"] = "malformed replacement"
        row["store_root_uri"] = None
        row.sync()
        malformed = database.storage.reload_stores()

        assert malformed.discovered_configurations == 1
        assert malformed.failed_configurations == 1
        assert malformed.issues[0].store_ref == derived_ref
        assert "store_root_uri" in malformed.issues[0].reason
        assert database.storage.get_store(derived_ref) is original
        assert (
            database.storage.get_store_configuration(derived_ref).store_name
            == "legacy healthy"
        )

        unavailable_root = tmp_path / "missing-read-only-root"
        row["store_name"] = "unavailable replacement"
        row["store_root_uri"] = unavailable_root.resolve().as_uri()
        row["store_is_read_only"] = 1
        row.sync()
        unavailable = database.storage.reload_stores()

        assert unavailable.failed_configurations == 0
        assert unavailable.skipped_configurations == 1
        assert "configured root does not exist" in unavailable.issues[0].reason
        assert database.storage.get_store(derived_ref) is original

        row["store_kind"] = "backend-that-does-not-exist"
        row.sync()
        unsupported = database.storage.reload_stores()

        assert unsupported.failed_configurations == 1
        assert "no Store factory is registered" in unsupported.issues[0].reason
        assert database.storage.get_store(derived_ref) is original

        recovered_root = tmp_path / "recovered"
        row["store_name"] = "recovered"
        row["store_kind"] = "filesystem"
        row["store_root_uri"] = recovered_root.resolve().as_uri()
        row["store_is_read_only"] = 0
        row.sync()
        recovered = database.storage.reload_stores()

        assert recovered.loaded_stores == 1
        assert recovered.ok
        assert database.storage.get_store(derived_ref) is not original
        assert (
            database.storage.get_store_configuration(derived_ref).store_name
            == "recovered"
        )
        assert recovered_root.is_dir()

        # An invalid durable identity is not the same Store. The authoritative
        # pass reports the bad row and removes the now-absent old identity.
        row["store_uuid"] = "definitely-not-a-uuid"
        row.sync()
        invalid_identity = database.storage.reload_stores()

        assert invalid_identity.failed_configurations == 1
        assert invalid_identity.issues[0].store_ref is None
        with pytest.raises(StoreConfigurationNotFound):
            database.storage.get_store(derived_ref)

        row["store_uuid"] = str(derived_ref)
        row.sync()
        restored_identity = database.storage.reload_stores()

        assert restored_identity.loaded_stores == 1
        assert restored_identity.ok
        assert database.storage.get_store(derived_ref).store_ref == derived_ref
        assert_integrity(database)
        database.storage.close()

    # The UUID backfill and the repaired configuration survive a full reopen,
    # including automatic manager construction.
    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        storage_startup_on_add=True,
    ) as reopened:
        assert reopened.storage_bootstrap_report.loaded_stores == 1
        assert reopened.storage.get_store(derived_ref).store_ref == derived_ref
        assert (
            reopened.storage.get_store_configuration(derived_ref).store_name
            == "recovered"
        )
        reopened.storage.close()


def test_database_reload_retains_claimed_store_identity_until_row_recovers(
    driver_spec,
    tmp_path: Path,
    assert_integrity,
) -> None:
    database_path = tmp_path / "replica-recovery.sqlite"
    store_ref = uuid4()
    store_root = tmp_path / "claimed-store"

    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        enable_storage_manager=False,
    ) as database:
        store_id = _insert_filesystem_store(
            database,
            store_ref=store_ref,
            name="claimed store",
            root=store_root,
        )
        assert database.bootstrap_storage_manager(
            startup_on_add=True
        ).loaded_stores == 1
        assert database.storage is not None
        asset = database.storage.store_bytes(
            b"bytes with a durable replica claim",
            original_name="claimed.epub",
        )
        assert database.storage.read_asset(asset) == (
            b"bytes with a durable replica claim"
        )

        row = database.get_row_from_id("stores", store_id)
        assert row is not None
        row["store_online_status"] = "offline"
        row.sync()
        offline = database.storage.reload_stores()

        assert offline.skipped_configurations == 1
        assert database.storage.get_store_configuration(store_ref).store_uuid == store_ref
        with pytest.raises(StoreUnavailable):
            database.storage.get_store(store_ref)
        with pytest.raises(NoReadableReplica):
            database.storage.read_asset(asset)

        included = database.storage.reload_stores(include_offline=True)

        assert included.loaded_stores == 1
        assert database.storage.read_asset(asset) == (
            b"bytes with a durable replica claim"
        )

        database.macros.delete_row("stores", store_id)
        removed = database.storage.reload_stores()

        assert removed.discovered_configurations == 0
        assert database.storage.get_store_configuration(store_ref).store_uuid == store_ref
        with pytest.raises(StoreUnavailable):
            database.storage.get_store(store_ref)
        with pytest.raises(NoReadableReplica):
            database.storage.read_asset(asset)

        _insert_filesystem_store(
            database,
            store_ref=store_ref,
            name="claimed store restored",
            root=store_root,
        )
        restored = database.storage.reload_stores()

        assert restored.loaded_stores == 1
        assert database.storage.read_asset(asset) == (
            b"bytes with a durable replica claim"
        )
        assert (
            database.storage.get_store_configuration(store_ref).store_name
            == "claimed store restored"
        )
        assert_integrity(database)
        database.storage.close()


def test_database_constructor_honours_strict_row_failure_policy(
    driver_spec,
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "strict-bootstrap.sqlite"
    bad_ref = uuid4()

    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        enable_storage_manager=False,
    ) as setup_database:
        _insert_filesystem_store(
            setup_database,
            store_ref=bad_ref,
            name="unknown backend row",
            root=tmp_path / "unknown",
            kind="not-a-real-storage-backend",
        )

    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        strict_storage_manager_bootstrap=False,
    ) as non_strict:
        assert non_strict.storage_bootstrap_report.failed_configurations == 1
        assert non_strict.storage_bootstrap_report.issues[0].store_ref == bad_ref
        non_strict.storage.close()

    with pytest.raises(
        StorageManagementError,
        match="unknown backend row.*no Store factory is registered",
    ):
        Database(
            metadata={"database_path": str(database_path)},
            db_type=driver_spec.db_type,
            create=False,
            backup=False,
            strict_storage_manager_bootstrap=True,
        )


def test_non_strict_database_bootstrap_returns_a_structured_failure() -> None:
    class _BrokenDatabase:
        storage = None
        storage_bootstrap_report = None
        metadata: dict[str, object] = {}

        @staticmethod
        def get_tables() -> list[str]:
            raise RuntimeError("database catalogue unavailable")

    database = _BrokenDatabase()

    report = bootstrap_storage_manager(
        cast(Any, database),
        strict=False,
    )

    assert not report.ok
    assert report.discovered_configurations == 1
    assert report.failed_configurations == 1
    assert report.issues[0].reason == "database catalogue unavailable"
    assert database.storage_bootstrap_report is report

    with pytest.raises(RuntimeError, match="database catalogue unavailable"):
        bootstrap_storage_manager(cast(Any, _BrokenDatabase()), strict=True)


def test_strict_database_bootstrap_rejects_reported_row_failures() -> None:
    failed_ref = uuid4()

    class _ReportingStorage:
        db = None
        startup_on_add = False

        @staticmethod
        def load_from_database(*args, **kwargs) -> StorageBootstrapReport:
            del args, kwargs
            return StorageBootstrapReport(
                discovered_configurations=2,
                loaded_stores=1,
                failed_configurations=1,
                issues=(
                    StorageBootstrapIssue(
                        failed_ref,
                        "bad archive",
                        "unsupported backend configuration",
                    ),
                ),
            )

    class _Database:
        storage = _ReportingStorage()
        storage_bootstrap_report = None

    database = _Database()
    non_strict = bootstrap_storage_manager(cast(Any, database), strict=False)

    assert non_strict.failed_configurations == 1
    assert database.storage_bootstrap_report is non_strict

    with pytest.raises(
        StorageManagementError,
        match="1 of 2 configured Stores.*bad archive.*unsupported backend",
    ):
        bootstrap_storage_manager(cast(Any, database), strict=True)
    assert database.storage_bootstrap_report.failed_configurations == 1


def test_database_refresh_applies_requested_startup_policy() -> None:
    class _TrackingStorage:
        db = None
        startup_on_add = False
        startup_arguments: list[bool | None] = []

        def load_from_database(
            self,
            db: object,
            *,
            include_offline: bool,
            clear_existing: bool,
            startup: bool | None,
        ):
            self.startup_arguments.append(startup)
            return type("Report", (), {"ok": True})()

    class _Database:
        storage = _TrackingStorage()
        storage_bootstrap_report = None

    database = _Database()

    report = bootstrap_storage_manager(
        cast(Any, database),
        startup_on_add=True,
    )

    assert report.ok
    assert database.storage.startup_on_add is True
    assert database.storage.startup_arguments == [True]
