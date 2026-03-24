from __future__ import annotations

import sqlite3

import pytest


EMPTY_CUSTOM_COLUMN_PROFILE_DB_NAMES = (
    "test_db_6",
    "test_db_22",
    "test_db_23",
    "test_db_24",
    "test_db_25",
)


@pytest.mark.db
@pytest.mark.parametrize("db_name", EMPTY_CUSTOM_COLUMN_PROFILE_DB_NAMES)
def test_profiled_fixtures_expose_empty_custom_column_profile(
    provision_test_database,
    db_name: str,
) -> None:
    provisioned = provision_test_database(db_name)
    conn = sqlite3.connect(str(provisioned.db_path))
    conn.row_factory = sqlite3.Row
    try:
        objects = {
            row["name"]: row["type"]
            for row in conn.execute(
                "SELECT name, type FROM sqlite_master "
                "WHERE name IN ('custom_columns', 'custom_column_defs', 'custom_column_links', 'custom_columns_v', 'titles', 'books') "
                "ORDER BY name;"
            ).fetchall()
        }

        assert objects["custom_columns"] == "table"
        assert objects["titles"] == "view"
        assert objects["books"] == "view"
        assert "custom_column_defs" not in objects
        assert "custom_column_links" not in objects
        assert "custom_columns_v" not in objects

        custom_column_count = int(
            conn.execute("SELECT COUNT(*) FROM custom_columns;").fetchone()[0]
        )
        assert custom_column_count == 0

        work_count = int(conn.execute("SELECT COUNT(*) FROM works;").fetchone()[0])
        title_count = int(conn.execute("SELECT COUNT(*) FROM titles;").fetchone()[0])
        book_count = int(conn.execute("SELECT COUNT(*) FROM books;").fetchone()[0])
        assert title_count == work_count
        assert book_count == work_count
    finally:
        conn.close()
