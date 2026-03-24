"""Database contract: surface + lifecycle.

This module is the first slice of Database-level tests. It focuses on:

* Construction surface: driver/wrapper/macros/queues are wired.
* Deterministic shutdown: Database.close() + context manager behavior.
* Handle release: the on-disk DB file can be renamed after close (Windows-meaningful).
* Driver reload hygiene: Database.set_driver() tears down old wrapper + driver resources.

These tests are intentionally *light* on heavy queries. Their job is to catch
resource leaks, thread leaks, and reference-cycle bugs early.
"""

from __future__ import annotations

import os
import types
from pathlib import Path

import pytest


def _assert_any_operation_fails(conn) -> None:
    """Assert that *conn* appears to be closed.

    We intentionally accept *any* exception type here, because different
    backends (sqlite3 / APSW / wrappers) raise different errors.
    """

    # Some connections expose .execute, some prefer cursor().execute.
    ops = []
    if hasattr(conn, "execute"):
        ops.append(lambda: conn.execute("SELECT 1"))
    if hasattr(conn, "cursor"):
        ops.append(lambda: conn.cursor().execute("SELECT 1"))
    if not ops:
        # If we cannot probe, treat as closed (best effort).
        return

    ok = False
    for op in ops:
        try:
            op()
        except Exception:
            ok = True
            break

    assert ok, "Expected closed connection to reject SQL operations"


def _rename_db_file(db_path: Path) -> Path:
    """Rename the db file, returning the new path.

    This is a reliable proxy for "no open SQLite handles" on Windows.
    """

    new_path = db_path.with_name(db_path.name + ".renamed")
    if new_path.exists():
        new_path.unlink()
    os.replace(db_path, new_path)
    assert new_path.exists()
    return new_path


def test_database_construction_surface(open_db):
    """Database should expose the key surfaces and wire them consistently."""

    db = open_db

    assert db.driver is not None
    assert db.driver_wrapper is not None
    assert db.macros is not None

    # Convenience aliases (a legacy quirk, but relied upon by much code).
    assert getattr(db, "conn", None) is db.driver.conn
    assert callable(getattr(db, "get", None))

    # Lock connection should exist and be distinct from the primary conn.
    assert getattr(db, "lock", None) is db.driver_wrapper.lock
    assert db.driver_wrapper.lock is not None
    assert db.driver.conn is not None
    assert db.driver_wrapper.lock is not db.driver.conn


def test_dirty_records_queue_is_shared(open_db):
    """Database should provide a single shared dirty-record queue."""

    db = open_db
    q = getattr(db, "dirty_records_queue", None)
    assert q is not None
    assert getattr(db.driver, "dirty_records_queue", None) is q
    assert getattr(db.driver_wrapper, "dirty_records_queue", None) is q


def test_maintenance_thread_is_started(open_db):
    """The Maintainer should spawn the background thread on startup."""

    db = open_db
    maint = getattr(db, "maintenance", None)
    assert maint is not None
    thread = getattr(maint, "maintainer", None)
    assert thread is not None
    # We don't assert is_alive() because scheduling can be noisy; we only
    # care that the object exists and is stop()-capable.
    assert hasattr(thread, "stop")


def test_close_stops_maintenance_thread(open_db):
    """close() should request the maintainer thread to stop."""

    db = open_db
    thread = db.maintenance.maintainer
    assert getattr(thread, "keep_running", True) is True

    db.close()

    # The thread may still be sleeping (daemon thread), but it must have been
    # asked to stop.
    assert getattr(thread, "keep_running", False) is False


def test_close_is_idempotent(open_db):
    """Calling close() multiple times should not raise."""

    db = open_db
    db.close()
    db.close()
    db.close()


def test_close_clears_convenience_aliases(open_db):
    """close() should clear the legacy convenience aliases that keep handles alive."""

    db = open_db
    assert getattr(db, "conn", None) is not None
    assert getattr(db, "get", None) is not None
    assert getattr(db, "lock", None) is not None

    db.close()

    assert getattr(db, "conn", None) is None
    assert getattr(db, "get", None) is None
    assert getattr(db, "lock", None) is None


def test_close_releases_driver_and_wrapper_connections(open_db):
    """close() should close both the primary and lock connections."""

    db = open_db
    driver = db.driver
    wrapper = db.driver_wrapper
    conn = driver.conn
    lock_conn = wrapper.lock

    assert conn is not None
    assert lock_conn is not None

    db.close()

    # Driver wrapper and driver should have been cleaned.
    assert getattr(driver, "conn", None) is None
    assert getattr(wrapper, "lock", None) is None

    # The captured connection objects should behave like closed resources.
    _assert_any_operation_fails(conn)
    _assert_any_operation_fails(lock_conn)


def test_close_allows_renaming_db_file(db_path: Path, driver_spec, db_metadata: dict):
    """After Database.close(), the database file should be renamable.

    This is a strong proxy for "no open SQLite handles" on Windows.
    """

    from LiuXin_alpha.databases.database import Database

    db = Database(metadata=db_metadata, db_type=driver_spec.db_type, create=False, backup=False)
    try:
        assert db.driver.conn is not None
        assert db.driver_wrapper.lock is not None
    finally:
        db.close()

    _rename_db_file(db_path)


def test_context_manager_closes_on_normal_exit(db_path: Path, driver_spec, db_metadata: dict):
    """Using Database as a context manager should always close resources."""

    from LiuXin_alpha.databases.database import Database

    with Database(metadata=db_metadata, db_type=driver_spec.db_type, create=False, backup=False) as db:
        assert db.driver.conn is not None
        assert db.driver_wrapper.lock is not None

    _rename_db_file(db_path)


def test_context_manager_closes_and_propagates_exceptions(db_path: Path, driver_spec, db_metadata: dict):
    """__exit__ should close resources and not swallow exceptions."""

    from LiuXin_alpha.databases.database import Database

    class _Sentinel(Exception):
        pass

    with pytest.raises(_Sentinel):
        with Database(metadata=db_metadata, db_type=driver_spec.db_type, create=False, backup=False):
            raise _Sentinel("boom")

    _rename_db_file(db_path)


def test_break_cycles_then_close_is_safe(open_db):
    """Even if break_cycles() runs early, close() should not explode."""

    db = open_db
    db.break_cycles()
    db.close()


def test_wrapper_close_then_database_close_is_safe(open_db):
    """If the wrapper is closed early, Database.close() should remain safe."""

    db = open_db
    wrapper = db.driver_wrapper
    wrapper.close()
    db.close()


def test_existing_driver_init_requires_metadata_none(db_metadata: dict, driver_spec):
    """existing_driver init path should enforce metadata=None (by design)."""

    from LiuXin_alpha.databases.database import Database
    from LiuXin_alpha.databases.database_driver_plugins import loadDatabaseDriver

    Driver = loadDatabaseDriver(driver_spec.db_type)
    drv = Driver(db_metadata, db=None, set_conn=True)
    try:
        with pytest.raises(AssertionError):
            Database(metadata=db_metadata, existing_driver=drv)
    finally:
        try:
            drv.close()
        except Exception:
            pass


def test_existing_driver_init_wires_db_refs(db_metadata: dict, driver_spec):
    """Database(existing_driver=...) should backfill db refs on driver + macros."""

    from LiuXin_alpha.databases.database import Database
    from LiuXin_alpha.databases.database_driver_plugins import loadDatabaseDriver

    Driver = loadDatabaseDriver(driver_spec.db_type)
    drv = Driver(db_metadata, db=None, set_conn=True)

    db = Database(metadata=None, existing_driver=drv)
    try:
        assert db.driver is drv
        assert getattr(drv, "db", None) is db
        assert getattr(db.driver_wrapper, "db", None) is db
        assert getattr(drv, "macros", None) is not None
        assert getattr(drv.macros, "db", None) is db
    finally:
        db.close()


def test_existing_driver_init_close_releases_handles(db_path: Path, db_metadata: dict, driver_spec):
    """Database(existing_driver=...) should still release file handles on close."""

    from LiuXin_alpha.databases.database import Database
    from LiuXin_alpha.databases.database_driver_plugins import loadDatabaseDriver

    Driver = loadDatabaseDriver(driver_spec.db_type)
    drv = Driver(db_metadata, db=None, set_conn=True)
    db = Database(metadata=None, existing_driver=drv)
    db.close()

    _rename_db_file(db_path)


def test_set_driver_replaces_wrapper_and_closes_old_resources(open_db, db_metadata: dict, driver_spec):
    """set_driver() should close the previous wrapper lock connection and driver conn."""

    from LiuXin_alpha.databases.database_driver_plugins import loadDatabaseDriver

    db = open_db

    old_driver = db.driver
    old_wrapper = db.driver_wrapper
    old_conn = old_driver.conn
    old_lock = old_wrapper.lock

    assert old_conn is not None
    assert old_lock is not None

    Driver = loadDatabaseDriver(driver_spec.db_type)
    new_driver = Driver(db_metadata, db=None, set_conn=True)

    db.set_driver(new_driver)

    # Old resources should be torn down.
    assert getattr(old_driver, "conn", None) is None
    assert getattr(old_wrapper, "lock", None) is None
    _assert_any_operation_fails(old_conn)
    _assert_any_operation_fails(old_lock)

    # New surfaces should be live.
    assert db.driver is new_driver
    assert db.driver.conn is not None
    assert db.driver_wrapper is not None
    assert db.driver_wrapper.lock is not None
    assert db.driver_wrapper.lock is not old_lock


def test_set_driver_with_same_driver_keeps_primary_conn(open_db):
    """Calling set_driver with the same driver should not close the primary conn."""

    db = open_db
    driver = db.driver
    conn = driver.conn
    old_wrapper = db.driver_wrapper
    old_lock = old_wrapper.lock

    db.set_driver(driver)

    # Primary connection remains usable.
    assert driver.conn is conn
    assert conn is not None
    assert driver.conn is not None

    # But the wrapper lock conn should have been replaced (old one closed).
    assert db.driver_wrapper is not old_wrapper
    assert db.driver_wrapper.lock is not None
    assert db.driver_wrapper.lock is not old_lock
    _assert_any_operation_fails(old_lock)


def test_set_driver_then_close_cleans_new_driver(open_db, db_metadata: dict, driver_spec):
    """If we swap drivers, a subsequent close() should clean the new resources too."""

    from LiuXin_alpha.databases.database_driver_plugins import loadDatabaseDriver

    db = open_db
    Driver = loadDatabaseDriver(driver_spec.db_type)
    new_driver = Driver(db_metadata, db=None, set_conn=True)
    db.set_driver(new_driver)

    # Capture new connection objects before close.
    conn = new_driver.conn
    lock_conn = db.driver_wrapper.lock

    db.close()

    assert getattr(new_driver, "conn", None) is None
    assert getattr(db.driver_wrapper, "lock", None) is None
    _assert_any_operation_fails(conn)
    _assert_any_operation_fails(lock_conn)


def test_wrapper_derived_schema_caches_reset_on_force_refresh(open_db, monkeypatch):
    """Wrapper-level derived schema caches should avoid redundant recompute and reset on refresh."""

    wrapper = open_db.driver_wrapper

    pair = None
    for left in sorted(wrapper.main_tables):
        for right in sorted(wrapper.main_tables):
            left_base = wrapper.driver.direct_get_column_base(left)
            right_base = wrapper.driver.direct_get_column_base(right)
            if left == right:
                candidate = "{}_{}_intralinks".format(left_base, left_base)
            else:
                ordered = sorted([left_base, right_base])
                candidate = "{}_{}_links".format(ordered[0], ordered[1])
            if candidate in wrapper.interlink_tables or candidate in wrapper.intralink_tables:
                pair = (left, right)
                break
        if pair is not None:
            break

    if pair is None:
        pytest.skip("No linkable table pair found in this contract DB")

    calls = {"count": 0}
    original = wrapper.driver.direct_get_column_base

    def counted(table_name):
        calls["count"] += 1
        return original(table_name)

    monkeypatch.setattr(wrapper.driver, "direct_get_column_base", counted)

    first = wrapper.get_link_table_name(*pair)
    assert first
    after_first = calls["count"]
    assert after_first > 0

    second = wrapper.get_link_table_name(*pair)
    assert second == first
    assert calls["count"] == after_first

    wrapper.get_tables(force_refresh=True)

    third = wrapper.get_link_table_name(*pair)
    assert third == first
    assert calls["count"] > after_first
