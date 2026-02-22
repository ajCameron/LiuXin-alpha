from __future__ import annotations

import sqlite3
from typing import Optional


def _pick_typed_interlink_table(conn: sqlite3.Connection) -> Optional[str]:
    """Find a generated interlink table that has a `*_type` metadata column."""
    tables = [
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_links' ORDER BY name;"
        ).fetchall()
    ]
    for t in tables:
        cols = conn.execute(f"PRAGMA table_info(`{t}`);").fetchall()
        if any(str(c[1]).endswith("_type") for c in cols):
            return t
    return None


def test_interlink_tables_allow_blank_row_insert(provision_test_database) -> None:
    """Regression: interlink tables must allow placeholder inserts (blank row then fill)."""

    provisioned = provision_test_database("test_db_13")

    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        table = _pick_typed_interlink_table(conn)
        assert table is not None, "Expected at least one *_links table with a *_type column"

        cols = conn.execute(f"PRAGMA table_info(`{table}`);").fetchall()

        # All optional metadata columns should be nullable.
        for cid, name, decl, notnull, dflt, pk in cols:
            name = str(name)
            if pk:
                continue
            if name.endswith("_type"):
                assert int(notnull) == 0, f"{table}.{name} unexpectedly NOT NULL ({decl})"

        # The workflow that currently exists in DriverWrapper.get_blank_row is:
        #   INSERT with only scratch -> then update the row with FK/type/etc.
        # This insert must succeed.
        scratch_cols = [str(c[1]) for c in cols if str(c[1]).endswith("_scratch")]
        assert scratch_cols, f"{table} has no scratch column; cannot exercise blank-row insert"

        scratch_col = scratch_cols[0]
        conn.execute(f"INSERT INTO `{table}` (`{scratch_col}`) VALUES (?);", ("regression",))
        conn.commit()

    finally:
        conn.close()
