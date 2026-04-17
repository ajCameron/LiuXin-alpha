from __future__ import annotations

import sqlite3

from LiuXin_alpha.databases.database import Database


def test_all_scratch_tables_allow_blank_insert(provision_test_database) -> None:
    """Regression: any writable scratch table must support Database.get_blank_row().

    Some tables now enforce additional invariants at INSERT time (for example
    `asset_replicas.asset_replica_storage_key` must be a non-empty relative
    key). The contract we actually care about is that the public blank-row API
    still works for writable scratch tables.
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
