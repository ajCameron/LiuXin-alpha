"""Driver contract: database lifecycle.

This module focuses on operations that touch the on-disk database file:

* Creating a brand new database at a fresh path.
* Closing and reopening connections.
* Backups.
* Self-delete.

These tests are intentionally backend-agnostic and run for every selected
backend.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest


def _sqlite_tables(db_path: Path) -> set[str]:
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        return {r[0] for r in rows}
    finally:
        conn.close()


def _assert_sqlite_integrity(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute("PRAGMA integrity_check").fetchone()
        assert row is not None
        assert str(row[0]).lower() == "ok"
    finally:
        conn.close()


def test_direct_create_new_database_produces_schema(driver_spec, tmp_path):
    """Creating a new DB at a fresh path should yield a usable schema."""

    from LiuXin_alpha.databases.database_driver_plugins import loadDatabaseDriver

    db_path = tmp_path / f"contract_create_{driver_spec.id}.db"
    assert not db_path.exists()

    Driver = loadDatabaseDriver(driver_spec.db_type)
    drv = Driver({"database_path": str(db_path)}, db=None, set_conn=False)

    # Create schema
    drv.direct_create_new_database()

    # Reopen and sanity-check
    drv.reopen()
    try:
        tables = set(drv.direct_get_tables(force_refresh=True))
        assert "titles" in tables
        assert "creators" in tables
        assert "database_metadata" in tables
        # Ensure the file is a valid SQLite database
        _assert_sqlite_integrity(db_path)
    finally:
        try:
            drv.close()
        except Exception:
            pass


def test_close_and_reopen_preserves_operation(driver, assert_integrity):
    """Drivers must survive close/reopen and remain usable."""

    tables_before = set(driver.direct_get_tables(force_refresh=True))
    assert "titles" in tables_before

    driver.close()
    assert driver.conn is None

    driver.reopen()
    assert driver.conn is not None

    tables_after = set(driver.direct_get_tables(force_refresh=True))
    assert tables_before == tables_after

    assert_integrity(driver)


def test_direct_backup_creates_copy(driver_spec, provisioned_contract_db, tmp_path):
    """direct_backup should write a readable DB file at an explicit destination."""

    from LiuXin_alpha.databases.database_driver_plugins import loadDatabaseDriver

    src_path = Path(provisioned_contract_db.db_path)
    assert src_path.exists()

    Driver = loadDatabaseDriver(driver_spec.db_type)
    drv = Driver({"database_path": str(src_path)}, db=None, set_conn=True)

    backup_path = tmp_path / f"backup_{driver_spec.id}.db"
    assert not backup_path.exists()

    try:
        drv.direct_backup(path=str(backup_path))
    finally:
        # close regardless; on Windows, leaving it open can lock the file
        try:
            drv.close()
        except Exception:
            pass

    assert backup_path.exists(), "Backup file was not created"
    _assert_sqlite_integrity(backup_path)

    # Basic schema presence check
    assert "titles" in _sqlite_tables(backup_path)


def test_direct_self_delete_removes_db_file(driver_spec, tmp_path):
    """direct_self_delete should remove the database file when no handles are open."""

    from LiuXin_alpha.databases.database_driver_plugins import loadDatabaseDriver

    db_path = tmp_path / f"contract_delete_{driver_spec.id}.db"

    Driver = loadDatabaseDriver(driver_spec.db_type)
    drv = Driver({"database_path": str(db_path)}, db=None, set_conn=False)
    drv.direct_create_new_database()

    # IMPORTANT: close before deletion to be portable (Windows file locking).
    drv.close()

    assert db_path.exists()
    drv.direct_self_delete()
    assert not db_path.exists()


@pytest.mark.skipif(os.name != "nt", reason="Windows-only: validates delete with open handle semantics")
def test_direct_self_delete_works_even_if_conn_open_on_windows(driver_spec, tmp_path):
    """On Windows, direct_self_delete should deal with its own open connection(s).

    This is a contract test because Windows cannot delete open SQLite files.
    """

    from LiuXin_alpha.databases.database_driver_plugins import loadDatabaseDriver

    db_path = tmp_path / f"contract_delete_open_{driver_spec.id}.db"
    Driver = loadDatabaseDriver(driver_spec.db_type)

    drv = Driver({"database_path": str(db_path)}, db=None, set_conn=True)
    drv.direct_create_new_database()

    # If the driver doesn't close its handle internally, this should fail on Windows.
    drv.direct_self_delete()

    assert not db_path.exists()
