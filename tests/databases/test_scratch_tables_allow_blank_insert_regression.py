"""
Check blank-row insertion for the supported scratch-table subset in test_db_13.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_scratch_tables_allow_blank_insert_regression.py
"""
from __future__ import annotations

import sqlite3

from LiuXin_alpha.databases.database import Database


def test_all_scratch_tables_allow_blank_insert(provision_test_database) -> None:
    """
    Check blank rows obtain a table name and ID for eligible scratch tables.

    Provision test_db_13 and inspect physical tables, excluding insertion-blocked tables
    and names outside the explicit supported subset. Close the Database context after
    the insertion probes; wrap insertion errors with table context.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_scratch_tables_allow_blank_insert_regression.py::test_all_scratch_tables_allow_blank_insert


    :param provision_test_database: Fixture factory that provisions the named test_db_13
        database.
    :return: None; failed expectations raise AssertionError.
    """

    provisioned = provision_test_database("test_db_13")
    with Database(
        metadata={"database_path": str(provisioned.db_path)},
        db_type="SQLite",
        create=False,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        conn = db.conn

        # Constant tables can intentionally be write-locked by FRBR generator triggers.
        read_only_tables = {
            str(r[0]).replace("block_insert_on_", "", 1)
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'block_insert_on_%';"
            ).fetchall()
        }

        tables = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
            ).fetchall()
        ]
        supported_tables = {
            "asset_replicas",
            "comments",
            "creators",
            "custom_columns",
            "folders",
            "genres",
            "identifiers",
            "languages",
            "publishers",
            "series",
            "subjects",
            "tags",
            "titles",
            "works",
        }

        for table in tables:
            if table in read_only_tables or table not in supported_tables:
                continue
            cols = conn.execute(f"PRAGMA table_info(`{table}`);").fetchall()
            scratch_cols = [str(c[1]) for c in cols if str(c[1]).endswith("_scratch")]
            if not scratch_cols:
                continue

            try:
                row = db.get_blank_row(table)
            except Exception as e:
                raise AssertionError(
                    f"{table}: get_blank_row() failed for a supported blank-row table."
                ) from e

            assert row.table == table
            assert row.row_id is not None
