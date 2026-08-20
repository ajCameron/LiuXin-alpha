"""Integration coverage for database-backed Store loading and reload."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.databases.runtime import bootstrap_storage_manager
from LiuXin_alpha.storage.api import StoreConfigurationNotFound


def _insert_filesystem_store(
    db: Database,
    *,
    store_ref: UUID,
    name: str,
    root: Path,
) -> int:
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "store_uuid": str(store_ref),
            "store_name": name,
            "store_kind": "filesystem",
            "store_access_protocol": "file",
            "store_root_uri": root.resolve().as_uri(),
            "store_is_read_only": 0,
            "store_online_status": "online",
        },
        table="stores",
    )
    assert row.row_id is not None
    return int(row.row_id)


def test_database_startup_loads_rows_and_reload_tracks_database_changes(
    driver_spec,
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "database.sqlite"
    primary_ref = uuid4()
    archive_ref = uuid4()

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

        changed = database.storage.reload_stores()

        assert changed.discovered_configurations == 2
        assert changed.loaded_stores == 2
        assert changed.ok
        assert (
            database.storage.get_store_configuration(primary_ref).store_name
            == "primary-renamed"
        )
        assert database.storage.get_store(archive_ref).store_ref == archive_ref

        database.macros.delete_row("stores", primary_id)
        removed = database.storage.reload_stores()

        assert removed.discovered_configurations == 1
        assert removed.loaded_stores == 1
        with pytest.raises(StoreConfigurationNotFound):
            database.storage.get_store(primary_ref)
        database.storage.close()


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
