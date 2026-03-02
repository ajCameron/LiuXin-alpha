"""Tests that the stdlib-backed SQLite driver can run without APSW.

These tests are intentionally small and "contract"-focused:

* Importing the driver must not require the optional APSW package.
* `dump_and_restore()` must round-trip the on-disk DB while preserving data.
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
    """Block imports of *module_prefix* (and its submodules) within the context."""

    # Meta path finder that refuses to resolve the blocked module.
    class _Blocker:
        def find_spec(self, fullname, path=None, target=None):  # noqa: ANN001
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
    """Temporarily remove matching modules from sys.modules (restored afterward)."""
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
    """Importing the stdlib driver must not touch APSW."""

    mod = "LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver"

    with _temporary_module_purge((mod, "LiuXin_alpha.databases.database_driver_plugins.SQLite")):
        with _block_import("apsw"):
            imported = importlib.import_module(mod)
            assert imported is not None
            assert "apsw" not in sys.modules


def _table_info(conn: sqlite3.Connection, table: str):
    return conn.execute(f"PRAGMA table_info(`{table}`);").fetchall()


def _detect_pk_column(conn: sqlite3.Connection, table: str) -> str | None:
    for _cid, name, _t, _notnull, _dflt, pk in _table_info(conn, table):
        if int(pk) == 1:
            return str(name)
    return None


def _relation_type(conn: sqlite3.Connection, name: str) -> str | None:
    row = conn.execute(
        "SELECT type FROM sqlite_master WHERE (type='table' OR type='view') AND name=? LIMIT 1;",
        (name,),
    ).fetchone()
    return str(row[0]) if row else None


def _default_value_for_type(col_name: str, col_type: str) -> Any:
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
    """Insert a single row into *table* satisfying NOT NULL constraints."""

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
    """Insert a row that is visible through the `titles` relation.

    In FRBR/WEMI schema variants, `titles` can be a read-only compatibility view.
    In that case, we seed data via `works` instead.
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
    driver: Any
    db_path: Path


@pytest.fixture
def sqlite_pure_driver_bundle(provision_test_database):
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
    """dump_and_restore should preserve user_version and user data."""

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