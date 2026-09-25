"""
Inspect generated interlink CREATE TABLE SQL for pair and role uniqueness.

Builds isolated SQLite catalogues and checks declared UNIQUE column groups. These
cases inspect SQL rather than probing duplicate inserts.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_frbr_interlink_constraints.py
"""

from __future__ import annotations

import pathlib
import re
import sqlite3

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as frbr_gen
from LiuXin_alpha.databases.database_driver_plugins.SQL.utility_mixins import ColumnNameMixin


def _table_sql(conn: sqlite3.Connection, table: str) -> str:
    """
    Read nonempty CREATE TABLE SQL for an exact bound table name.

    Raises AssertionError if the table or its SQL is missing.

    Example:
        >>> conn = sqlite3.connect(':memory:')
        >>> _ = conn.execute('CREATE TABLE demo (id INTEGER)')
        >>> 'demo' in _table_sql(conn, 'demo')
        True
        >>> conn.close()


    :param conn: Caller-owned SQLite connection; this helper does not close it.
    :param table: Trusted test table name, interpolated into SQL where needed.
    :return: Stored CREATE TABLE statement as a string.
    """
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?;",
        (table,),
    ).fetchone()
    assert row and row[0], f"Missing CREATE TABLE SQL for {table!r}"
    return str(row[0])


def _unique_groups(sql: str) -> list[set[str]]:
    """
    Extract column sets from simple table-level UNIQUE clauses.

    Uses a case-insensitive regex, comma splitting and quote stripping. Does not parse
    nested expressions or inline column uniqueness and discards column order.

    Example:
        >>> _unique_groups('CREATE TABLE t (a, b, UNIQUE (a, b))') == [{'a', 'b'}]
        True


    :param sql: CREATE TABLE SQL text to inspect.
    :return: List of column-name sets in matched clause order.
    """
    groups: list[set[str]] = []
    for m in re.finditer(r"UNIQUE\s*\(([^)]*)\)", sql, flags=re.IGNORECASE | re.MULTILINE):
        inner = m.group(1)
        cols = []
        for part in inner.split(","):
            c = part.strip().strip("`\" ")
            if c:
                cols.append(c)
        groups.append(set(cols))
    return groups


def test_interlink_many_to_many_pair_uniqueness(tmp_path: pathlib.Path) -> None:
    """
    Require two endpoint columns and a UNIQUE pair on the generated work/expression table.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_interlink_constraints.py::test_interlink_many_to_many_pair_uniqueness


    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :return: None; failed expectations raise AssertionError.
    """
    db_path = tmp_path / "frbr_interlink_pair_unique.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        frbr_gen.create_new_database(conn)

        table, col_prefix = ColumnNameMixin.get_interlink_table_name("works", "expressions")
        sql = _table_sql(conn, table)

        cols = [r[1] for r in conn.execute(f"PRAGMA table_info(`{table}`);")]
        fk_cols = [c for c in cols if c.endswith("_id") and c != f"{col_prefix}_id"]
        assert len(fk_cols) == 2, f"Expected 2 FK columns in {table!r}, got: {fk_cols!r}"

        uniqs = _unique_groups(sql)
        assert set(fk_cols) in uniqs, f"Expected UNIQUE({fk_cols}) in {table!r}. Found: {uniqs!r}"

    finally:
        conn.close()


def test_interlink_many_to_many_non_exclusive_unique_includes_type(tmp_path: pathlib.Path) -> None:
    """
    Require role uniqueness over both endpoints and type on agent/work links.

    When priority exists, also checks uniqueness over the selected primary endpoint,
    type and priority.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_interlink_constraints.py::test_interlink_many_to_many_non_exclusive_unique_includes_type


    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :return: None; failed expectations raise AssertionError.
    """
    db_path = tmp_path / "frbr_interlink_type_unique.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        frbr_gen.create_new_database(conn)

        table, col_prefix = ColumnNameMixin.get_interlink_table_name("agents", "works")
        sql = _table_sql(conn, table)

        cols = [r[1] for r in conn.execute(f"PRAGMA table_info(`{table}`);")]

        fk_cols = [c for c in cols if c.endswith("_id") and c != f"{col_prefix}_id"]
        assert len(fk_cols) == 2, f"Expected 2 FK columns in {table!r}, got: {fk_cols!r}"

        type_col = f"{col_prefix}_type"
        assert type_col in cols, f"Expected {type_col!r} in {table!r} columns: {cols!r}"

        uniqs = _unique_groups(sql)
        assert set(fk_cols + [type_col]) in uniqs, (
            f"Expected UNIQUE({fk_cols + [type_col]}) in {table!r}. Found: {uniqs!r}"
        )

        # If priority is present, ordering should be unique per (primary_id,type,priority)
        prio_col = f"{col_prefix}_priority"
        if prio_col in cols:
            # Prefer the 'agent' FK as primary if present; otherwise fall back to any FK col.
            primary_fk = next((c for c in fk_cols if c.endswith("_agent_id")), fk_cols[0])
            expect = {primary_fk, type_col, prio_col}
            assert expect in uniqs, (
                f"Expected ordering UNIQUE({sorted(expect)}) in {table!r}. Found: {uniqs!r}"
            )

    finally:
        conn.close()
