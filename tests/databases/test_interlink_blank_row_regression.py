"""
Check the scratch-only placeholder insertion used by the driver wrapper.

Copies test_db_13 and selects a typed link table, then verifies nullable type
metadata and commits a scratch-only row. The test does not fill the placeholder
afterward.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_interlink_blank_row_regression.py
"""
from __future__ import annotations

import sqlite3
from typing import Optional


def _pick_typed_interlink_table(conn: sqlite3.Connection) -> Optional[str]:
    """
    Find the first alphabetically ordered matching table with a column ending in _type.

    Uses the unescaped SQL LIKE pattern %_links, then PRAGMA inspection; underscore
    retains wildcard meaning in the table-name query.

    Example:
        >>> conn = sqlite3.connect(':memory:')
        >>> _ = conn.execute('CREATE TABLE sample_links (sample_type TEXT)')
        >>> _pick_typed_interlink_table(conn)
        'sample_links'
        >>> conn.close()


    :param conn: Caller-owned SQLite connection; this helper does not close it.
    :return: First matching table name, or None when none has a type column.
    """
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
    """
    Require nullable non-primary type columns and commit a scratch-only placeholder row.

    Selects one typed table from test_db_13, requires a scratch column and closes the
    raw SQLite connection afterward.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_interlink_blank_row_regression.py::test_interlink_tables_allow_blank_row_insert


    :param provision_test_database: Fixture factory provisioning a temporary test_db_13
        database copy.
    :return: None; failed expectations raise AssertionError.
    """

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
