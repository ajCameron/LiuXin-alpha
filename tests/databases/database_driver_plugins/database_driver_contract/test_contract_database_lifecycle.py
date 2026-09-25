"""
Check schema creation, reopen, backup, and self-deletion for each selected SQLite backend.

Temporary and provisioned files isolate these destructive lifecycle checks. The
open-handle deletion case is collected only for Windows execution.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_database_lifecycle.py
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest


def _sqlite_relations(db_path: Path) -> set[str]:
    """
    Open the SQLite file and collect every table and view name from sqlite_master.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_database_lifecycle.py


    :param db_path: SQLite file path opened with sqlite3.connect; a missing file may be
        created.
    :return: Set of relation names; closes its connection in finally.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
        ).fetchall()
        return {r[0] for r in rows}
    finally:
        conn.close()


def _assert_sqlite_integrity(db_path: Path) -> None:
    """
    Open the SQLite file and require the first integrity_check row to contain ok, ignoring case.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_database_lifecycle.py


    :param db_path: SQLite file path opened with sqlite3.connect; a missing file may be
        created.
    :return: None when the first result passes; closes its connection in finally.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute("PRAGMA integrity_check").fetchone()
        assert row is not None
        assert str(row[0]).lower() == "ok"
    finally:
        conn.close()


def test_direct_create_new_database_produces_schema(driver_spec, tmp_path):
    """
    Create a database at a fresh temporary path, reopen it, and check core relations plus SQLite integrity.

    After reopening succeeds, the finally block attempts driver close and suppresses
    ordinary close errors.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_database_lifecycle.py::test_direct_create_new_database_produces_schema


    :param driver_spec: Selected database driver specification, including its ID and
        Database db_type.
    :param tmp_path: Pytest-provided temporary directory for isolated database and
        fixture files.
    :return: None; failed expectations raise AssertionError.
    """

    from LiuXin_alpha.databases.database_driver_plugins.registry import (
        load_database_driver,
    )

    db_path = tmp_path / f"contract_create_{driver_spec.id}.db"
    assert not db_path.exists()

    Driver = load_database_driver(driver_spec.db_type)
    drv = Driver({"database_path": str(db_path)}, db=None, set_conn=False)

    # Create schema
    drv.direct_create_new_database()

    # Reopen and sanity-check
    drv.reopen()
    try:
        tables = set(drv.direct_get_tables(force_refresh=True))
        assert "titles" in tables
        assert "creators" in tables or "agents" in tables
        assert "database_metadata" in tables
        # Ensure the file is a valid SQLite database
        _assert_sqlite_integrity(db_path)
    finally:
        try:
            drv.close()
        except Exception:
            pass


def test_close_and_reopen_preserves_operation(driver, assert_integrity):
    """
    Require close to clear the connection, reopen to restore it, and the full table-name set to remain equal.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_database_lifecycle.py::test_close_and_reopen_preserves_operation


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :param assert_integrity: Fixture callable checking the first retained
        integrity_check result for ok.
    :return: None; failed expectations raise AssertionError.
    """

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
    """
    Write an explicit backup, close the source driver, and require a readable copy containing the core relations.

    The checks cover integrity and schema presence; they do not compare all source rows.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_database_lifecycle.py::test_direct_backup_creates_copy


    :param driver_spec: Selected database driver specification, including its ID and
        Database db_type.
    :param provisioned_contract_db: Provisioned test database resource exposing db_path.
    :param tmp_path: Pytest-provided temporary directory for isolated database and
        fixture files.
    :return: None; failed expectations raise AssertionError.
    """

    from LiuXin_alpha.databases.database_driver_plugins.registry import (
        load_database_driver,
    )

    src_path = Path(provisioned_contract_db.db_path)
    assert src_path.exists()

    Driver = load_database_driver(driver_spec.db_type)
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
    relations = _sqlite_relations(backup_path)
    assert "titles" in relations
    assert "database_metadata" in relations
    assert "creators" in relations or "agents" in relations


def test_direct_self_delete_removes_db_file(driver_spec, tmp_path):
    """
    Create and close a temporary database, then require self-delete to remove its file.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_database_lifecycle.py::test_direct_self_delete_removes_db_file


    :param driver_spec: Selected database driver specification, including its ID and
        Database db_type.
    :param tmp_path: Pytest-provided temporary directory for isolated database and
        fixture files.
    :return: None; failed expectations raise AssertionError.
    """

    from LiuXin_alpha.databases.database_driver_plugins.registry import (
        load_database_driver,
    )

    db_path = tmp_path / f"contract_delete_{driver_spec.id}.db"

    Driver = load_database_driver(driver_spec.db_type)
    drv = Driver({"database_path": str(db_path)}, db=None, set_conn=False)
    drv.direct_create_new_database()

    # IMPORTANT: close before deletion to be portable (Windows file locking).
    drv.close()

    assert db_path.exists()
    drv.direct_self_delete()
    assert not db_path.exists()


@pytest.mark.skipif(os.name != "nt", reason="Windows-only: validates delete with open handle semantics")
def test_direct_self_delete_works_even_if_conn_open_on_windows(driver_spec, tmp_path):
    """
    On Windows, create a database with its driver connection enabled and require self-delete to remove the file.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_database_lifecycle.py::test_direct_self_delete_works_even_if_conn_open_on_windows


    :param driver_spec: Selected database driver specification, including its ID and
        Database db_type.
    :param tmp_path: Pytest-provided temporary directory for isolated database and
        fixture files.
    :return: None; failed expectations raise AssertionError.
    """

    from LiuXin_alpha.databases.database_driver_plugins.registry import (
        load_database_driver,
    )

    db_path = tmp_path / f"contract_delete_open_{driver_spec.id}.db"
    Driver = load_database_driver(driver_spec.db_type)

    drv = Driver({"database_path": str(db_path)}, db=None, set_conn=True)
    drv.direct_create_new_database()

    # If the driver doesn't close its handle internally, this should fail on Windows.
    drv.direct_self_delete()

    assert not db_path.exists()
