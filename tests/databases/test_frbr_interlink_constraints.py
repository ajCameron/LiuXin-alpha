"""Constraint-level tests for FRBR interlink tables.

These tests validate that the TOML link_type semantics are translated into the
intended SQLite uniqueness constraints (especially for role-style `type` links).
"""

from __future__ import annotations

import pathlib
import re
import sqlite3

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as frbr_gen
from LiuXin_alpha.databases.database_driver_plugins.SQL.utility_mixins import ColumnNameMixin


def _table_sql(conn: sqlite3.Connection, table: str) -> str:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?;",
        (table,),
    ).fetchone()
    assert row and row[0], f"Missing CREATE TABLE SQL for {table!r}"
    return str(row[0])


def _unique_groups(sql: str) -> list[set[str]]:
    groups: list[set[str]] = []
    for m in re.finditer(r"UNIQUE\s*\(([^)]*)\)", sql, flags=re.IGNORECASE | re.MULTILINE):
        inner = m.group(1)
        cols = []
        for part in inner.split(","):
            c = part.strip().strip("`" ")
            if c:
                cols.append(c)
        groups.append(set(cols))
    return groups


def test_interlink_many_to_many_pair_uniqueness(tmp_path: pathlib.Path) -> None:
    """A plain many-to-many link must be UNIQUE on the FK pair (A_id,B_id)."""
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
    """Role-style links (type column) must be UNIQUE on (A_id,B_id,type)."""
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
