"""Integration coverage for database-backed Store loading and reload."""

from __future__ import annotations

import dataclasses
import json
import sqlite3

from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.databases.runtime import bootstrap_storage_manager
from LiuXin_alpha.caches import CacheLookupStatus, create_cache
from LiuXin_alpha.storage.api import (
    NoReadableReplica,
    StorageBootstrapIssue,
    StorageBootstrapReport,
    StorageManagementError,
    StoragePreconditionFailed,
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


def test_database_metadata_unit_of_work_ports_commit_and_rollback(
    driver_spec,
    tmp_path: Path,
) -> None:
    from LiuXin_alpha.storage import api

    database_path = tmp_path / "storage-unit-of-work.sqlite"
    store_ref = uuid4()
    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        enable_storage_manager=False,
    ) as setup:
        _insert_filesystem_store(
            setup,
            store_ref=store_ref,
            name="unit-of-work",
            root=tmp_path / "unit-of-work-store",
        )

    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        storage_startup_on_add=False,
    ) as database:
        assert database.storage is not None
        manager = database.storage
        factory = manager.metadata_unit_of_work_factory
        assert isinstance(factory, api.StorageUnitOfWorkFactoryAPI)

        declaration = api.DigitalAssetDeclaration(
            4,
            (api.Digest("sha256", "a" * 64),),
            api.DigitalAssetMetadata(original_name="rolled-back.epub"),
        )
        with factory.begin() as unit_of_work:
            assert isinstance(unit_of_work, api.StorageUnitOfWorkAPI)
            assert isinstance(
                unit_of_work.assets,
                api.DigitalAssetRepositoryAPI,
            )
            assert isinstance(unit_of_work.replicas, api.ReplicaRepositoryAPI)
            assert isinstance(
                unit_of_work.composites,
                api.CompositeDigitalAssetRepositoryAPI,
            )
            assert isinstance(
                unit_of_work.derivations,
                api.DigitalAssetDerivationRepositoryAPI,
            )
            rolled_back = unit_of_work.assets.add(declaration)
            assert unit_of_work.assets.get(
                rolled_back.digital_asset_id
            ) == rolled_back
            # No commit request: leaving the UoW rolls the reservation and
            # record back together.

        with pytest.raises(api.DigitalAssetNotFound):
            factory.assets.get(rolled_back.digital_asset_id)

        with factory.begin() as unit_of_work:
            committed = unit_of_work.assets.add(
                dataclasses.replace(
                    declaration,
                    metadata=api.DigitalAssetMetadata(
                        original_name="committed.epub"
                    ),
                )
            )
            unit_of_work.commit()
        assert manager.get_digital_asset_record(
            committed.digital_asset_id
        ) == committed

        with factory.begin() as unit_of_work:
            unit_of_work.assets.replace_metadata(
                committed.digital_asset_id,
                api.DigitalAssetMetadata(original_name="discarded.epub"),
                if_revision=committed.revision,
            )
            unit_of_work.rollback()
        assert manager.get_digital_asset_record(
            committed.digital_asset_id
        ).metadata.original_name == "committed.epub"


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


def test_database_bound_manager_metadata_and_operation_ids_survive_restart(
    driver_spec,
    tmp_path: Path,
    assert_integrity,
) -> None:
    """Repository views survive a full manager restart without private state."""

    from LiuXin_alpha.storage.api import (
        BackupPolicy,
        CompositeDigitalAssetDeclaration,
        CompositeDigitalAssetMembership,
        DigitalAssetLossAction,
        DigitalAssetDerivationDeclaration,
        DigitalAssetDerivationKind,
        DigitalAssetDerivationSourceReference,
        ReplicationPolicy,
    )

    database_path = tmp_path / "durable-storage-manager.sqlite"
    store_ref = uuid4()
    operation_id = uuid4()
    interrupted_operation_id = uuid4()
    payload = "durable bytes — 😀".encode()
    interrupted_payload = b"published before metadata commit"
    root = tmp_path / "durable-store"

    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        enable_storage_manager=False,
    ) as setup_database:
        _insert_filesystem_store(
            setup_database,
            store_ref=store_ref,
            name="durable",
            root=root,
        )
        item_row = Row.from_idless_row_dict(
            setup_database,
            row_dict={"item_type": "digital", "item_source": "storage-test"},
            table="items",
        )
        item_id = int(item_row["item_id"])

    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        storage_startup_on_add=True,
    ) as database:
        assert database.storage is not None
        manager = database.storage
        cache = create_cache(database, "schema_backed")
        manager.bind_metadata_cache(cache)
        assert manager.metadata_is_durable
        assert manager.metadata_cache is cache
        source = manager.store_bytes(b"source", original_name="source.epub")
        result = manager.store_bytes(
            payload,
            original_name="tortured é ‮ name.epub",
            attributes={"source": "résumé", "emoji": "📚"},
            operation_id=operation_id,
        )
        replication = manager.create_replication_policy(
            ReplicationPolicy(
                name="restart replication",
                min_copies=0,
                target_copies=0,
                synchronous_write_copies=0,
                loss_action=DigitalAssetLossAction.ACCEPT_LOSS,
            )
        )
        backup = manager.create_backup_policy(
            BackupPolicy(
                name="restart backup",
                min_copies=0,
                target_copies=0,
            )
        )
        composite = manager.declare_composite_digital_asset(
            CompositeDigitalAssetDeclaration(
                (
                    CompositeDigitalAssetMembership(
                        source.digital_asset_id,
                        0,
                        role="source",
                        logical_path="inputs/source.epub",
                    ),
                    CompositeDigitalAssetMembership(
                        result.digital_asset_id,
                        1,
                        role="result",
                        logical_path="outputs/result.epub",
                    ),
                ),
                name="restart composite",
                attributes=(("kind", "conversion-pair"),),
            )
        )
        derivation = manager.record_digital_asset_derivation(
            DigitalAssetDerivationDeclaration(
                result.digital_asset_id,
                (
                    DigitalAssetDerivationSourceReference(
                        0,
                        digital_asset_id=source.digital_asset_id,
                        role="primary",
                    ),
                ),
                DigitalAssetDerivationKind.CONVERT,
                operator="test converter",
                notes="round trip all rich provenance",
            )
        )
        manager.link_item_to_digital_asset(
            item_id,
            result.digital_asset_id,
            role="custom_payload_role",
        )
        manager.link_item_to_composite_digital_asset(
            item_id,
            composite.composite_digital_asset_id,
            role="source_archive",
        )
        replica_count = len(tuple(manager.iter_replica_records()))
        operation_cache = manager._ingest_operations
        original_commit = operation_cache._upsert

        def _interrupt_metadata_commit(_operation) -> None:
            raise RuntimeError("simulated process stop before operation commit")

        operation_cache._upsert = _interrupt_metadata_commit
        with pytest.raises(RuntimeError, match="simulated process stop"):
            manager.store_bytes(
                interrupted_payload,
                original_name="interrupted.epub",
                operation_id=interrupted_operation_id,
            )
        operation_cache._upsert = original_commit
        operational_status = manager.get_operational_status()
        pending_ingests = operational_status.issues_for("ingest_pending")
        assert any(
            issue.operation_id == interrupted_operation_id
            for issue in pending_ingests
        )
        assert any(
            action.action == "recover_pending_ingests"
            and action.operation_id == interrupted_operation_id
            for action in operational_status.recovery_actions
        )
        assert_integrity(database)
        manager.bind_metadata_cache(None)
        cache.close()

    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        storage_startup_on_add=True,
    ) as reopened:
        assert reopened.storage is not None
        manager = reopened.storage
        assert manager.metadata_is_durable
        assert manager.read_asset(result.digital_asset_id) == payload
        assert manager.get_digital_asset_record(result.digital_asset_id) == result
        assert manager.get_replication_policy_record(
            replication.replication_policy_id
        ) == replication
        assert manager.get_backup_policy_record(backup.backup_policy_id) == backup
        assert manager.get_composite_digital_asset_record(
            composite.composite_digital_asset_id
        ) == composite
        assert manager.get_digital_asset_derivation_record(
            derivation.digital_asset_derivation_id
        ) == derivation
        atomic_link = manager.resolve_item_digital_asset(
            item_id, role="custom_payload_role"
        )
        assert (
            atomic_link.digital_asset_resolution.asset_record.digital_asset_id
            == result.digital_asset_id
        )
        composite_link = manager.resolve_item_digital_asset(
            item_id, role="source_archive"
        )
        assert (
            composite_link.composite_digital_asset_record.composite_digital_asset_id
            == composite.composite_digital_asset_id
        )
        recovered = manager.store_bytes(
            interrupted_payload,
            original_name="interrupted.epub",
            operation_id=interrupted_operation_id,
        )
        assert manager.read_asset(recovered.digital_asset_id) == interrupted_payload
        assert manager.ingest_recovery_issues == ()

        retried = manager.store_bytes(
            payload,
            original_name="tortured é ‮ name.epub",
            attributes={"source": "résumé", "emoji": "📚"},
            operation_id=operation_id,
        )
        assert retried == result
        assert len(tuple(manager.iter_replica_records())) == replica_count + 1
        assert_integrity(reopened)


def test_database_manager_shares_liuxin_cache_without_private_record_copies(
    driver_spec,
    tmp_path: Path,
    assert_integrity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "shared-cache.sqlite"
    store_ref = uuid4()
    root = tmp_path / "shared-cache-store"

    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        enable_storage_manager=False,
    ) as setup_database:
        _insert_filesystem_store(
            setup_database,
            store_ref=store_ref,
            name="cache-shared",
            root=root,
        )

    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        storage_startup_on_add=True,
    ) as database:
        assert database.storage is not None
        manager = database.storage
        cache = create_cache(database, "schema_backed")
        manager.bind_metadata_cache(cache)
        full_reloads: list[str] = []
        id_reloads: list[tuple[str, tuple[int, ...]]] = []
        original_reload_main_table = cache.storage.reload_main_table
        original_reload_ids = cache.storage.reload_ids

        def tracked_full_reload(table, db=None) -> None:
            full_reloads.append(str(table))
            original_reload_main_table(table, db=db)

        def tracked_id_reload(table, ids, db=None) -> None:
            normalized = tuple(sorted(int(row_id) for row_id in ids))
            id_reloads.append((str(table), normalized))
            original_reload_ids(table, normalized, db=db)

        monkeypatch.setattr(cache.storage, "reload_main_table", tracked_full_reload)
        monkeypatch.setattr(cache.storage, "reload_ids", tracked_id_reload)

        assert manager.metadata_cache is cache
        assert not isinstance(manager._assets, dict)
        assert not isinstance(manager._replicas, dict)

        asset = manager.store_bytes(
            b"shared cache bytes",
            original_name="cache-shared.epub",
        )

        cached = cache.get("digital_assets", int(asset.digital_asset_id))
        assert cached.status is CacheLookupStatus.HIT
        assert (
            "digital_assets",
            (int(asset.digital_asset_id),),
        ) in id_reloads
        assert full_reloads == []
        assert manager.get_digital_asset_record(asset.digital_asset_id) == asset

        manager.bind_metadata_cache(None)
        cache.close()
        assert manager.get_digital_asset_record(asset.digital_asset_id) == asset
        assert_integrity(database)


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


def test_concurrently_open_managers_use_database_generated_record_ids(
    driver_spec,
    tmp_path: Path,
    assert_integrity,
) -> None:
    """Managers opened from the same snapshot cannot overwrite each other's IDs."""

    from LiuXin_alpha.storage.api import (
        BackupPolicy,
        CompositeDigitalAssetDeclaration,
        CompositeDigitalAssetMembership,
        DigitalAssetDerivationDeclaration,
        DigitalAssetDerivationKind,
        DigitalAssetDerivationSourceReference,
        ReplicationPolicy,
    )

    database_path = tmp_path / "concurrent-identities.sqlite"
    store_ref = uuid4()
    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        enable_storage_manager=False,
    ) as setup_database:
        _insert_filesystem_store(
            setup_database,
            store_ref=store_ref,
            name="concurrent",
            root=tmp_path / "concurrent-store",
        )

    database_options = {
        "metadata": {"database_path": str(database_path)},
        "db_type": driver_spec.db_type,
        "create": False,
        "backup": False,
        "storage_startup_on_add": True,
    }
    with Database(**database_options) as first_database, Database(
        **database_options
    ) as second_database:
        first = first_database.storage
        second = second_database.storage
        assert first is not None
        assert second is not None

        first_replication = first.create_replication_policy(
            ReplicationPolicy(name="first replication")
        )
        second_replication = second.create_replication_policy(
            ReplicationPolicy(name="second replication")
        )
        observed_first_replication = second.get_replication_policy_record(
            first_replication.replication_policy_id
        )
        updated_first_replication = second.update_replication_policy(
            first_replication.replication_policy_id,
            ReplicationPolicy(name="first replication updated"),
            if_revision=observed_first_replication.revision,
        )
        assert updated_first_replication.revision != (
            observed_first_replication.revision
        )
        with pytest.raises(StoragePreconditionFailed, match="revision precondition"):
            first.update_replication_policy(
                first_replication.replication_policy_id,
                ReplicationPolicy(name="stale first replication"),
                if_revision=first_replication.revision,
            )
        first_backup = first.create_backup_policy(
            BackupPolicy(name="first backup")
        )
        second_backup = second.create_backup_policy(
            BackupPolicy(name="second backup")
        )

        first_ingest = first.store_bytes(b"first concurrent bytes")
        second_ingest = second.store_bytes(b"second concurrent bytes")
        third_ingest = second.store_bytes(b"third concurrent bytes")
        first_composite = first.declare_composite_digital_asset(
            CompositeDigitalAssetDeclaration(
                (
                    CompositeDigitalAssetMembership(
                        first_ingest.digital_asset_id,
                        0,
                    ),
                ),
                name="first composite",
            )
        )
        second_composite = second.declare_composite_digital_asset(
            CompositeDigitalAssetDeclaration(
                (
                    CompositeDigitalAssetMembership(
                        second_ingest.digital_asset_id,
                        0,
                    ),
                ),
                name="second composite",
            )
        )
        first_derivation = first.record_digital_asset_derivation(
            DigitalAssetDerivationDeclaration(
                second_ingest.digital_asset_id,
                (
                    DigitalAssetDerivationSourceReference(
                        0,
                        digital_asset_id=first_ingest.digital_asset_id,
                    ),
                ),
                DigitalAssetDerivationKind.CONVERT,
                operator="first converter",
            )
        )
        second_derivation = second.record_digital_asset_derivation(
            DigitalAssetDerivationDeclaration(
                third_ingest.digital_asset_id,
                (
                    DigitalAssetDerivationSourceReference(
                        0,
                        digital_asset_id=second_ingest.digital_asset_id,
                    ),
                ),
                DigitalAssetDerivationKind.CONVERT,
                operator="second converter",
            )
        )

        assert first_replication.replication_policy_id != (
            second_replication.replication_policy_id
        )
        assert first_backup.backup_policy_id != second_backup.backup_policy_id
        assert first_ingest.digital_asset_id != second_ingest.digital_asset_id
        first_replica = next(
            first.iter_replica_records(
                digital_asset_id=first_ingest.digital_asset_id
            )
        )
        second_replica = next(
            second.iter_replica_records(
                digital_asset_id=second_ingest.digital_asset_id
            )
        )
        assert first_replica.replica_id != second_replica.replica_id
        assert first_composite.composite_digital_asset_id != (
            second_composite.composite_digital_asset_id
        )
        assert first_derivation.digital_asset_derivation_id != (
            second_derivation.digital_asset_derivation_id
        )
        assert len(tuple(first.iter_digital_asset_records())) == 3
        assert len(tuple(second.iter_replica_records())) == 3
        assert_integrity(first_database)
        assert_integrity(second_database)


def test_pre_journal_storage_catalogue_is_migrated_during_bootstrap(
    driver_spec,
    tmp_path: Path,
    assert_integrity,
) -> None:
    database_path = tmp_path / "pre-journal.sqlite"
    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        enable_storage_manager=False,
    ) as old_database:
        with old_database.macros.transaction() as connection:
            connection.execute("DROP TABLE storage_ingest_operations")
            connection.execute("DROP TABLE storage_schema_migrations")

    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        storage_startup_on_add=False,
    ) as migrated:
        assert migrated.storage is not None
        assert {
            "storage_ingest_operations",
            "storage_schema_migrations",
        } <= set(migrated.get_tables())
        assert migrated.storage.storage_migration_report.applied_migrations == (
            "storage-0001-migration-ledger",
            "storage-0002-ingest-journal",
        )
        recorded = {
            row["storage_schema_migration_id"]
            for row in migrated.macros.get_rows("storage_schema_migrations")
        }
        assert recorded == {
            "storage-0001-migration-ledger",
            "storage-0002-ingest-journal",
            "storage-0003-envelope-v1",
        }
        assert_integrity(migrated)


def test_version_zero_storage_envelope_is_upgraded_in_place(
    driver_spec,
    tmp_path: Path,
    assert_integrity,
) -> None:
    from LiuXin_alpha.storage.api import Digest, DigitalAssetDeclaration

    database_path = tmp_path / "old-envelope.sqlite"
    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as current:
        assert current.storage is not None
        record = current.storage.declare_digital_asset(
            DigitalAssetDeclaration(4, (Digest("sha256", "abcd"),))
        )
        row = current.macros.get_row(
            "digital_assets",
            int(record.digital_asset_id),
            id_column="digital_asset_id",
        )
        assert row is not None
        envelope = json.loads(row["digital_asset_scratch"])
        old_envelope = {
            "format": envelope["format"],
            "version": 0,
            "record": envelope["payload"],
        }
        current.macros.update_row(
            "digital_assets",
            int(record.digital_asset_id),
            {"digital_asset_scratch": json.dumps(old_envelope)},
            id_column="digital_asset_id",
        )

    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        storage_startup_on_add=False,
    ) as migrated:
        assert migrated.storage is not None
        assert migrated.storage.storage_migration_report.envelope_rows_upgraded == 1
        assert migrated.storage.get_digital_asset_record(
            record.digital_asset_id
        ) == record
        row = migrated.macros.get_row(
            "digital_assets",
            int(record.digital_asset_id),
            id_column="digital_asset_id",
        )
        assert row is not None
        assert json.loads(row["digital_asset_scratch"])["version"] == 1
        assert_integrity(migrated)


def test_newer_storage_envelope_is_refused_without_rewriting(
    driver_spec,
    tmp_path: Path,
) -> None:
    from LiuXin_alpha.storage.api import Digest, DigitalAssetDeclaration

    database_path = tmp_path / "future-envelope.sqlite"
    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as current:
        assert current.storage is not None
        record = current.storage.declare_digital_asset(
            DigitalAssetDeclaration(6, (Digest("sha256", "future"),))
        )
        row = current.macros.get_row(
            "digital_assets",
            int(record.digital_asset_id),
            id_column="digital_asset_id",
        )
        assert row is not None
        envelope = json.loads(row["digital_asset_scratch"])
        envelope["version"] = 99
        current.macros.update_row(
            "digital_assets",
            int(record.digital_asset_id),
            {"digital_asset_scratch": json.dumps(envelope)},
            id_column="digital_asset_id",
        )

    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        enable_storage_manager=False,
    ) as future_database:
        with pytest.raises(
            StorageManagementError,
            match="envelope version 99.*newer than.*version 1",
        ):
            StorageManager(db=future_database, startup_on_add=False)
        row = future_database.macros.get_row(
            "digital_assets",
            int(record.digital_asset_id),
            id_column="digital_asset_id",
        )
        assert row is not None
        assert json.loads(row["digital_asset_scratch"])["version"] == 99


def test_store_update_and_explicit_forget_are_durable(
    driver_spec,
    tmp_path: Path,
    assert_integrity,
) -> None:
    database_path = tmp_path / "store-administration.sqlite"
    store_ref = uuid4()
    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        enable_storage_manager=False,
    ) as setup_database:
        store_id = _insert_filesystem_store(
            setup_database,
            store_ref=store_ref,
            name="before",
            root=tmp_path / "before",
        )

    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        storage_startup_on_add=False,
    ) as database:
        assert database.storage is not None
        original = database.storage.get_store_configuration(store_ref)
        replacement = dataclasses.replace(
            original,
            store_name="after",
            store_root_uri=(tmp_path / "after").resolve().as_uri(),
            store_region="test-region",
            store_tags=("durable", "updated"),
        )

        assert database.storage.update_store(store_ref, replacement) == replacement
        row = database.macros.get_row("stores", store_id, id_column="store_id")
        assert row is not None
        assert row["store_name"] == "after"
        assert row["store_root_uri"] == replacement.store_root_uri
        assert row["store_region"] == "test-region"
        assert json.loads(row["store_tags_json"]) == ["durable", "updated"]
        assert_integrity(database)

    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        storage_startup_on_add=False,
    ) as reopened:
        assert reopened.storage is not None
        assert reopened.storage.get_store_configuration(store_ref) == replacement
        assert reopened.storage.remove_store(
            store_ref,
            forget_configuration=True,
        )
        assert reopened.macros.get_row(
            "stores", store_id, id_column="store_id"
        ) is None
        assert_integrity(reopened)

    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        storage_startup_on_add=False,
    ) as forgotten:
        assert forgotten.storage is not None
        with pytest.raises(StoreConfigurationNotFound):
            forgotten.storage.get_store_configuration(store_ref)


def test_compound_policy_update_rolls_back_intermediate_repository_write(
    driver_spec,
    tmp_path: Path,
) -> None:
    from LiuXin_alpha.storage.api import ReplicationPolicy

    database_path = tmp_path / "compound-policy.sqlite"
    with Database(
        metadata={"database_path": str(database_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as database:
        manager = database.storage
        assert manager is not None
        original = manager.create_replication_policy(
            ReplicationPolicy(name="transactional original")
        )
        repository_mapping = manager._replication_policies
        original_upsert = repository_mapping._upsert
        calls = 0

        def fail_final_write(record):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("injected final policy write failure")
            original_upsert(record)

        repository_mapping._upsert = fail_final_write
        try:
            with pytest.raises(
                RuntimeError,
                match="injected final policy write failure",
            ):
                manager.update_replication_policy(
                    original.replication_policy_id,
                    ReplicationPolicy(name="must roll back"),
                    if_revision=original.revision,
                )
        finally:
            repository_mapping._upsert = original_upsert

        assert manager.get_replication_policy_record(
            original.replication_policy_id
        ) == original


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

        with pytest.raises(sqlite3.IntegrityError):
            database.macros.delete_row("stores", store_id)

        row = database.get_row_from_id("stores", store_id)
        assert row is not None
        row["store_online_status"] = "retired"
        row.sync()
        removed = database.storage.reload_stores()

        assert removed.discovered_configurations == 1
        assert removed.skipped_configurations == 1
        assert database.storage.get_store_configuration(store_ref).store_uuid == store_ref
        with pytest.raises(StoreUnavailable):
            database.storage.get_store(store_ref)
        with pytest.raises(NoReadableReplica):
            database.storage.read_asset(asset)

        row = database.get_row_from_id("stores", store_id)
        assert row is not None
        row["store_name"] = "claimed store restored"
        row["store_online_status"] = "online"
        row.sync()
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
