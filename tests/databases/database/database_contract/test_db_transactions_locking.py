"""Chunk 04: Transactions, locking, and concurrency behaviors exposed via Database.

These tests intentionally target *Database-level* access patterns:

* The `Database.lock` connection (provided by `DriverWrapper.get_connection()`).
* Real file-level locking semantics using an external `sqlite3` connection.
* Transaction / rollback correctness using connection context managers and savepoints.

Two tests at the bottom are intentionally *red* (failing) to capture a desirable
future-facing contract: `Database` should surface direct SQL helpers like
`execute()` / `executemany()` so callers don't need to reach into `driver_wrapper`.
"""

from __future__ import annotations

import sqlite3
import threading
import uuid
from pathlib import Path

import pytest


def _mk_table_name(prefix: str = "tx_lock_test") -> str:
    # Safe identifier: ASCII + underscores only.
    return f"{prefix}__{uuid.uuid4().hex}"


def _external_conn(db_path: Path) -> sqlite3.Connection:
    # timeout=0 -> fail fast when DB is locked.
    conn = sqlite3.connect(str(db_path), timeout=0.0)
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _rows(conn, sql: str, params=None):
    cur = conn.execute(sql, params or ())
    return list(cur)


def _get_main_dbfile(conn) -> str:
    rows = _rows(conn, "PRAGMA database_list")
    for _seq, name, file in rows:
        if name == "main":
            return file or ""
    return ""


def _ensure_test_table(conn, table: str) -> None:
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT)"
    )
    try:
        conn.commit()
    except Exception:
        # Some connection wrappers autocommit / don't expose commit; that's fine.
        pass


def _count_rows(conn, table: str) -> int:
    cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
    row = cur.fetchone()
    return int(row[0])


def _safe_rollback(conn) -> None:
    try:
        conn.rollback()
        return
    except Exception:
        pass
    # Fallback for wrappers that prefer SQL.
    try:
        conn.execute("ROLLBACK")
    except Exception:
        pass


def test_lock_connection_is_separate_and_points_to_same_db(open_db, db_path: Path):
    lock_conn = open_db.lock
    driver_conn = open_db.driver.conn

    assert lock_conn is not None
    assert driver_conn is not None
    assert lock_conn is not driver_conn

    # Both connections should point at the same on-disk DB file.
    lock_file = _get_main_dbfile(lock_conn)
    driver_file = _get_main_dbfile(driver_conn)

    # Path strings can differ across platforms; assert basename match + suffix path match.
    assert Path(lock_file).name == db_path.name
    assert Path(driver_file).name == db_path.name

    # Quick sanity: lock_conn can run a basic query.
    rows = _rows(lock_conn, "SELECT name FROM sqlite_master LIMIT 1")
    assert isinstance(rows, list)


def test_lock_connection_context_manager_commits(open_db, db_path: Path):
    lock_conn = open_db.lock
    table = _mk_table_name()
    _ensure_test_table(lock_conn, table)

    with lock_conn:
        lock_conn.execute(f"INSERT INTO {table} (payload) VALUES (?)", ("ok",))

    ext = _external_conn(db_path)
    try:
        assert _count_rows(ext, table) == 1
    finally:
        ext.close()


def test_lock_connection_context_manager_rolls_back_on_exception(open_db, db_path: Path):
    lock_conn = open_db.lock
    table = _mk_table_name()
    _ensure_test_table(lock_conn, table)

    with pytest.raises(RuntimeError):
        with lock_conn:
            lock_conn.execute(f"INSERT INTO {table} (payload) VALUES (?)", ("nope",))
            raise RuntimeError("boom")

    ext = _external_conn(db_path)
    try:
        assert _count_rows(ext, table) == 0
    finally:
        ext.close()


def test_lock_connection_savepoint_partial_rollback(open_db, db_path: Path):
    lock_conn = open_db.lock
    table = _mk_table_name()
    _ensure_test_table(lock_conn, table)

    with lock_conn:
        lock_conn.execute(f"INSERT INTO {table} (payload) VALUES ('a')")
        lock_conn.execute("SAVEPOINT sp1")
        lock_conn.execute(f"INSERT INTO {table} (payload) VALUES ('b')")
        lock_conn.execute("ROLLBACK TO sp1")
        lock_conn.execute("RELEASE sp1")

    ext = _external_conn(db_path)
    try:
        assert _count_rows(ext, table) == 1
    finally:
        ext.close()


def test_begin_immediate_blocks_external_writer_but_allows_reader(open_db, db_path: Path):
    lock_conn = open_db.lock
    table = _mk_table_name()
    _ensure_test_table(lock_conn, table)

    # Ensure we're not inside any transaction before taking the write lock.
    try:
        lock_conn.commit()
    except Exception:
        pass

    lock_conn.execute("BEGIN IMMEDIATE")
    try:
        # Reader should still work.
        reader = _external_conn(db_path)
        try:
            _ = _count_rows(reader, table)
        finally:
            reader.close()

        # Writer should fail fast while the IMMEDIATE lock is held.
        writer = _external_conn(db_path)
        try:
            with pytest.raises(sqlite3.OperationalError):
                writer.execute(f"INSERT INTO {table} (payload) VALUES ('x')")
        finally:
            writer.close()
    finally:
        _safe_rollback(lock_conn)


def test_begin_immediate_blocks_external_writer_in_other_thread(open_db, db_path: Path):
    lock_conn = open_db.lock
    table = _mk_table_name()
    _ensure_test_table(lock_conn, table)

    # Ensure clean state.
    try:
        lock_conn.commit()
    except Exception:
        pass

    lock_conn.execute("BEGIN IMMEDIATE")

    result = {"exc": None}

    def worker():
        try:
            c = _external_conn(db_path)
            try:
                c.execute(f"INSERT INTO {table} (payload) VALUES ('y')")
                c.commit()
            finally:
                c.close()
        except Exception as e:  # noqa: BLE001
            result["exc"] = e

    t = threading.Thread(target=worker, daemon=True)
    try:
        t.start()
        t.join(timeout=5.0)
        assert not t.is_alive(), "worker thread hung (possible lock release issue)"
        assert isinstance(result["exc"], sqlite3.OperationalError)
    finally:
        _safe_rollback(lock_conn)


# -------------------------------------------------------------------------------------------------
# Intentional RED tests: desired API surfacing on Database (keep failing for now)
# -------------------------------------------------------------------------------------------------


def test_database_should_surface_execute_helper(open_db):
    """Desired contract: Database.execute() should exist (currently does not)."""

    # Intentionally not guarded: should fail loudly until the API is added.
    open_db.execute("SELECT 1")


def test_database_should_surface_executemany_helper(open_db, db_path: Path):
    """Desired contract: Database.executemany() should exist (currently does not)."""

    table = _mk_table_name("tx_execmany")
    # Create via wrapper so failure pinpoints the missing Database API, not missing table.
    open_db.driver_wrapper.execute(
        f"CREATE TABLE IF NOT EXISTS {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT)"
    )
    values = [("a",), ("b",), ("c",)]

    # Intentionally not guarded: should fail loudly until the API is added.
    open_db.executemany(f"INSERT INTO {table} (payload) VALUES (?)", values)
