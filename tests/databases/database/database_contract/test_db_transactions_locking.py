"""
Check lock-connection transactions, savepoints, file locking, and direct Database SQL helper calls.

External sqlite3 connections fail immediately on lock contention and are closed by
their callers. SQL-helper tests exercise the current API without asserting helper
return values; the executemany case does not independently inspect inserted rows.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database/database_contract/test_db_transactions_locking.py
"""

from __future__ import annotations

import sqlite3
import threading
import uuid
from pathlib import Path

import pytest


def _mk_table_name(prefix: str = "tx_lock_test") -> str:
    # Safe identifier: ASCII + underscores only.
    """
    Append a double underscore and random UUID hex suffix to a caller-supplied prefix.

    The prefix is not validated or escaped, so callers must supply a trusted
    SQL-identifier prefix.

    Example:
        >>> name = _mk_table_name('sample')
        >>> name.startswith('sample__'), len(name.removeprefix('sample__'))
        (True, 32)


    :param prefix: Trusted prefix copied verbatim into the table name.
    :return: Generated string; uniqueness is probabilistic.
    """
    return f"{prefix}__{uuid.uuid4().hex}"


def _external_conn(db_path: Path) -> sqlite3.Connection:
    # timeout=0 -> fail fast when DB is locked.
    """
    Open sqlite3 with zero lock timeout and enable foreign keys.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_transactions_locking.py


    :param db_path: Isolated provisioned database file used by this test.
    :return: New caller-owned connection, which the caller must close; initialization
        errors propagate.
    """
    conn = sqlite3.connect(str(db_path), timeout=0.0)
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _rows(conn, sql: str, params=None):
    """
    Execute a statement and materialize its iterable cursor, replacing falsy params with an empty tuple.

    Example:
        >>> connection = sqlite3.connect(':memory:')
        >>> _rows(connection, 'SELECT ?', (7,))
        [(7,)]
        >>> connection.close()


    :param conn: Caller-owned connection supporting execute.
    :param sql: SQL statement passed unchanged to the connection.
    :param params: Optional bindings passed to execute.
    :return: List of returned rows; no commit or close is performed.
    """
    cur = conn.execute(sql, params or ())
    return list(cur)


def _get_main_dbfile(conn) -> str:
    """
    Read PRAGMA database_list and return the main database’s reported file.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_transactions_locking.py


    :param conn: Caller-owned connection used for the PRAGMA query.
    :return: File string, or an empty string when main is absent or has no file.
    """
    rows = _rows(conn, "PRAGMA database_list")
    for _seq, name, file in rows:
        if name == "main":
            return file or ""
    return ""


def _ensure_test_table(conn, table: str) -> None:
    """
    Create the trusted test table if absent and attempt commit, suppressing ordinary commit errors.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_transactions_locking.py


    :param conn: Caller-owned connection left open after the DDL.
    :param table: Trusted test table name, interpolated into SQL where needed.
    :return: None; successful return does not establish that commit succeeded.
    """
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT)"
    )
    try:
        conn.commit()
    except Exception:
        # Some connection wrappers autocommit / don't expose commit; that's fine.
        pass


def _count_rows(conn, table: str) -> int:
    """
    Query COUNT(*) from the trusted table and convert the first field to int.

    Example:
        >>> connection = sqlite3.connect(':memory:')
        >>> _ensure_test_table(connection, 'sample')
        >>> _count_rows(connection, 'sample')
        0
        >>> connection.close()


    :param conn: Caller-owned connection used for the query.
    :param table: Trusted test table name, interpolated into SQL where needed.
    :return: Integer row count; execution and malformed-result errors propagate.
    """
    cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
    row = cur.fetchone()
    return int(row[0])


def _safe_rollback(conn) -> None:
    """
    Try connection.rollback, falling back to SQL ROLLBACK, and suppress ordinary errors from both attempts.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_transactions_locking.py


    :param conn: Caller-owned connection whose active transaction should be rolled back.
    :return: None; rollback success is not guaranteed and the connection remains open.
    """
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
    """
    Check lock and primary connections are distinct, report the expected database basename, and the lock can run a query.

    Only basenames are compared; full path equality is not asserted.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_transactions_locking.py::test_lock_connection_is_separate_and_points_to_same_db


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param db_path: Isolated provisioned database file used by this test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Insert within the lock-connection context and check an external connection sees one committed row.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_transactions_locking.py::test_lock_connection_context_manager_commits


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param db_path: Isolated provisioned database file used by this test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Raise RuntimeError after insertion in the lock context and check an external connection sees zero rows.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_transactions_locking.py::test_lock_connection_context_manager_rolls_back_on_exception


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param db_path: Isolated provisioned database file used by this test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Roll back the second insertion to a savepoint and check one row remains visible externally after context exit.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_transactions_locking.py::test_lock_connection_savepoint_partial_rollback


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param db_path: Isolated provisioned database file used by this test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Hold BEGIN IMMEDIATE, allow an external count query, and require an external insert to raise sqlite3.OperationalError.

    Close external connections and attempt lock rollback in finally.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_transactions_locking.py::test_begin_immediate_blocks_external_writer_but_allows_reader


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param db_path: Isolated provisioned database file used by this test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Hold BEGIN IMMEDIATE and require a separate writer thread to terminate within five seconds with sqlite3.OperationalError.

    Attempt lock rollback in finally.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_transactions_locking.py::test_begin_immediate_blocks_external_writer_in_other_thread


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param db_path: Isolated provisioned database file used by this test.
    :return: None; failed expectations raise AssertionError.
    """
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
        """
        Open an external connection, attempt an insert and commit, close it, and record any ordinary exception.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database/database_contract/test_db_transactions_locking.py::test_begin_immediate_blocks_external_writer_in_other_thread


        :return: None; stores the caught exception in the enclosing result mapping.
        """
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
    """
    Call Database.execute with SELECT 1 and require completion without an exception; no returned value is inspected.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_transactions_locking.py::test_database_should_surface_execute_helper


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """

    # Intentionally not guarded: should fail loudly until the API is added.
    open_db.execute("SELECT 1")


def test_database_should_surface_executemany_helper(open_db, db_path: Path):
    """
    Create a dedicated table through the wrapper and call Database.executemany with three values.

    Require completion without an exception; do not independently check inserted rows.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_transactions_locking.py::test_database_should_surface_executemany_helper


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param db_path: Isolated provisioned database file used by this test.
    :return: None; failed expectations raise AssertionError.
    """

    table = _mk_table_name("tx_execmany")
    # Create via wrapper so failure pinpoints the missing Database API, not missing table.
    open_db.driver_wrapper.execute(
        f"CREATE TABLE IF NOT EXISTS {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT)"
    )
    values = [("a",), ("b",), ("c",)]

    # Intentionally not guarded: should fail loudly until the API is added.
    open_db.executemany(f"INSERT INTO {table} (payload) VALUES (?)", values)
