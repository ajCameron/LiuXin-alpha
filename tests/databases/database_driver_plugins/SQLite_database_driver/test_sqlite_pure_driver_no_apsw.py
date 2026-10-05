"""
Check pure SQLite driver import with APSW blocked and dump/restore preservation of row count and user_version.

Import helpers mutate global import state temporarily; their restoration only covers
the finder or module entries they explicitly saved.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_pure_driver_no_apsw.py
"""

from __future__ import annotations

import contextlib
import importlib
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import pytest


@contextlib.contextmanager
def _block_import(module_prefix: str) -> Iterator[None]:
    """
    Install a first-priority finder that rejects a module prefix and its submodules until context exit.

    Already cached modules can bypass finder lookup. Remove the finder in finally and
    ignore ValueError if it was already removed.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_pure_driver_no_apsw.py


    :param module_prefix: Exact module name and dotted-prefix family to block.
    :return: Context manager yielding None; surrounding code owns any cached-module
        purge.
    """

    # Meta path finder that refuses to resolve the blocked module.
    class _Blocker:
        """
        Reject matching module lookups while allowing other finders to resolve unrelated names.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_pure_driver_no_apsw.py
        """
        def find_spec(self, fullname, path=None, target=None):  # noqa: ANN001
            """
            Raise ImportError for the blocked name or its dotted children and return None otherwise.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_pure_driver_no_apsw.py


            :param fullname: Full import name being resolved.
            :param path: Ignored package search path supplied by import machinery.
            :param target: Ignored reload target supplied by import machinery.
            :return: None for unrelated modules; matching lookups always raise.
            """
            if fullname == module_prefix or fullname.startswith(module_prefix + "."):
                raise ImportError(f"Blocked import: {fullname}")
            return None

    blocker = _Blocker()
    sys.meta_path.insert(0, blocker)
    try:
        yield
    finally:
        try:
            sys.meta_path.remove(blocker)
        except ValueError:
            pass


@contextlib.contextmanager
def _temporary_module_purge(prefixes: tuple[str, ...]) -> Iterator[None]:
    """
    Remove currently cached modules matching any prefix and restore those saved entries in finally.

    Only names present in the initial snapshot are removed again before restoration.
    Newly imported matching names absent from that snapshot can remain afterward.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_pure_driver_no_apsw.py


    :param prefixes: Exact module names and dotted-prefix families to snapshot and
        purge.
    :return: Context manager yielding None while saved modules are absent.
    """
    snapshot: dict[str, Any] = {}
    to_remove: list[str] = []
    for k, v in list(sys.modules.items()):
        if any(k == p or k.startswith(p + ".") for p in prefixes):
            snapshot[k] = v
            to_remove.append(k)

    for k in to_remove:
        sys.modules.pop(k, None)

    try:
        yield
    finally:
        # Restore exactly what we removed.
        for k in to_remove:
            sys.modules.pop(k, None)
        sys.modules.update(snapshot)


def test_pure_driver_imports_without_apsw() -> None:
    """
    Purge cached SQLite/APSW modules, block APSW lookup, and require pure-driver import with no apsw entry in sys.modules.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_pure_driver_no_apsw.py::test_pure_driver_imports_without_apsw


    :return: None; failed expectations raise AssertionError.
    """

    mod = "LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver"

    with _temporary_module_purge(
        (
            mod,
            "LiuXin_alpha.databases.database_driver_plugins.SQLite",
            "apsw",
        )
    ):
        with _block_import("apsw"):
            imported = importlib.import_module(mod)
            assert imported is not None
            assert "apsw" not in sys.modules


def _table_info(conn: sqlite3.Connection, table: str):
    """
    Fetch SQLite table_info rows using the trusted relation name in a quoted PRAGMA.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_pure_driver_no_apsw.py


    :param conn: Caller-owned SQLite connection; this helper does not close it.
    :param table: Trusted test table name, interpolated into SQL where needed.
    :return: Fetched metadata rows; the caller retains the connection.
    """
    return conn.execute(f"PRAGMA table_info(`{table}`);").fetchall()


def _detect_pk_column(conn: sqlite3.Connection, table: str) -> str | None:
    """
    Return the name whose table_info primary-key ordinal equals one.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_pure_driver_no_apsw.py


    :param conn: Caller-owned SQLite connection; this helper does not close it.
    :param table: Trusted test table name, interpolated into SQL where needed.
    :return: First primary-key component name, or None; composite keys are not returned
        as a group.
    """
    for _cid, name, _t, _notnull, _dflt, pk in _table_info(conn, table):
        if int(pk) == 1:
            return str(name)
    return None


def _relation_type(conn: sqlite3.Connection, name: str) -> str | None:
    """
    Read sqlite_master for the exact bound table/view name.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_pure_driver_no_apsw.py


    :param conn: Caller-owned SQLite connection; this helper does not close it.
    :param name: Exact table or view name passed as a bound query value.
    :return: Relation type string, or None if absent; the caller retains the connection.
    """
    row = conn.execute(
        "SELECT type FROM sqlite_master WHERE (type='table' OR type='view') AND name=? LIMIT 1;",
        (name,),
    ).fetchone()
    return str(row[0]) if row else None


def _default_value_for_type(col_name: str, col_type: str) -> Any:
    """
    Choose UUID, numeric, blob, or empty-text placeholders by name/type heuristics.

    The uppercase DATE/TIME checks run against a lowercased name and therefore do not
    recognize date/time names. No schema-constraint validation occurs.

    Example:
        >>> _default_value_for_type('n', 'INTEGER')
        0
        >>> _default_value_for_type('created_date', 'TEXT')
        ''


    :param col_name: Column name used for UUID or preferred-text heuristics.
    :param col_type: Declared type string inspected by substring.
    :return: Placeholder scalar or empty string.
    """
    n = col_name.lower()
    t = (col_type or "").upper()

    if "UUID" in t or n.endswith("_uuid"):
        return "00000000-0000-0000-0000-000000000000"
    if "DATE" in n or "TIME" in n:
        return "2000-01-01 00:00:00"

    if "INT" in t:
        return 0
    if "REAL" in t or "FLOA" in t or "DOUB" in t:
        return 0.0
    if "BLOB" in t:
        return b""
    return ""


def _insert_minimal_row(conn: sqlite3.Connection, *, table: str, override: dict[str, Any] | None = None) -> int:
    """
    Insert known non-key overrides and placeholders for required columns without defaults on the caller’s connection.

    Ignore unknown override names and the first primary-key component. Use DEFAULT
    VALUES when no columns are required; otherwise check key detection only after
    inserting. The helper neither commits nor rolls back, and placeholders may violate
    other constraints.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_pure_driver_no_apsw.py


    :param conn: Caller-owned SQLite connection; this helper does not close it.
    :param table: Trusted test table name, interpolated into SQL where needed.
    :param override: Optional values for known non-primary-key columns; unknown names
        are ignored.
    :return: Integer lastrowid; the explicit-column branch raises RuntimeError after
        insertion when no key was detected.
    """

    override = dict(override or {})
    pk_col = _detect_pk_column(conn, table)
    cols = _table_info(conn, table)

    required_cols: list[str] = []
    values: list[Any] = []

    for _cid, name, col_type, notnull, dflt, pk in cols:
        name = str(name)
        if int(pk) == 1:
            continue
        if name in override:
            required_cols.append(name)
            values.append(override[name])
            continue
        if int(notnull) == 1 and dflt is None:
            required_cols.append(name)
            values.append(_default_value_for_type(name, str(col_type)))

    if not required_cols:
        cur = conn.execute(f"INSERT INTO `{table}` DEFAULT VALUES;")
        return int(cur.lastrowid)

    placeholders = ",".join(["?"] * len(required_cols))
    cols_sql = ",".join([f"`{c}`" for c in required_cols])
    cur = conn.execute(f"INSERT INTO `{table}` ({cols_sql}) VALUES ({placeholders});", values)
    if pk_col is None:
        raise RuntimeError(f"Could not detect PK for table {table!r}")
    return int(cur.lastrowid)


def _insert_minimal_title_row(conn: sqlite3.Connection, *, title: str) -> int:
    """
    Insert matching title and sort text into works when titles is a view, otherwise into titles.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_pure_driver_no_apsw.py


    :param conn: Caller-owned SQLite connection; this helper does not close it.
    :param title: Text assigned to the title column.
    :return: Integer lastrowid from the insertion helper; does not commit or close.
    """
    if _relation_type(conn, "titles") == "view":
        return _insert_minimal_row(
            conn,
            table="works",
            override={
                "work_title": title,
                "work_sort_title": title,
            },
        )

    return _insert_minimal_row(
        conn,
        table="titles",
        override={
            "title": title,
            "title_sort": title,
        },
    )


@dataclass(frozen=True)
class _DriverBundle:
    """
    Hold frozen driver and database-path references without managing their lifetime.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_pure_driver_no_apsw.py
    """
    driver: Any
    db_path: Path


@pytest.fixture
def sqlite_pure_driver_bundle(provision_test_database):
    """
    Provision test_db_13, construct the pure SQLite driver, and yield its bundle with best-effort driver cleanup.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_pure_driver_no_apsw.py


    :param provision_test_database: Fixture factory provisioning a named test database
        into isolated storage.
    :return: Iterator yielding one bundle; ordinary close errors are suppressed in
        finally.
    """
    from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver import DatabaseDriver

    provisioned = provision_test_database("test_db_13")
    metadata = {"database_path": str(provisioned.db_path)}
    drv = DatabaseDriver(db_metadata=metadata, db=None, set_conn=True)
    try:
        yield _DriverBundle(driver=drv, db_path=provisioned.db_path)
    finally:
        try:
            drv.close()
        except Exception:
            pass


def test_dump_and_restore_round_trips(sqlite_pure_driver_bundle) -> None:
    """
    Set user_version to 123, insert a title, run dump/restore, and check the total title count and version are retained.

    Create the configured scratch directory if needed. The temporary pre-restore count
    connection is not explicitly closed, and the inserted title’s exact text is not
    independently checked after restoration.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_pure_driver_no_apsw.py::test_dump_and_restore_round_trips


    :param sqlite_pure_driver_bundle: Fixture bundle containing the pure sqlite3 driver
        and its isolated database path.
    :return: None; failed expectations raise AssertionError.
    """

    drv = sqlite_pure_driver_bundle.driver

    # Seed some user data.
    conn = drv.get_connection()
    try:
        conn.execute("PRAGMA user_version=123;")
        _insert_minimal_title_row(conn, title="Dump/Restore Test")
        conn.commit()
    finally:
        conn.close()

    before_count = sqlite3.connect(str(sqlite_pure_driver_bundle.db_path)).execute(
        "SELECT COUNT(*) FROM titles;"
    ).fetchone()[0]

    # The legacy `TemporaryFile` helper defaults to the project's scratch dir.
    # Ensure it exists so dump/restore can create temp files.
    from LiuXin_alpha.constants.paths import LiuXin_scratch_folder

    Path(LiuXin_scratch_folder).mkdir(parents=True, exist_ok=True)

    drv.dump_and_restore(callback=lambda _x: None)

    after_conn = sqlite3.connect(str(sqlite_pure_driver_bundle.db_path))
    try:
        after_count = after_conn.execute("SELECT COUNT(*) FROM titles;").fetchone()[0]
        assert after_count == before_count

        uv = after_conn.execute("PRAGMA user_version;").fetchone()[0]
        assert int(uv) == 123
    finally:
        after_conn.close()
