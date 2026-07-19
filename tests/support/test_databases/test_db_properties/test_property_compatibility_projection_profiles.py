from __future__ import annotations

import sqlite3

import pytest


COMPATIBILITY_PROJECTION_DB_NAMES = (
    "test_db_1",
    "test_db_14",
    "test_db_15",
    "test_db_16",
    "test_db_17",
)


@pytest.mark.catalog
@pytest.mark.parametrize("db_name", COMPATIBILITY_PROJECTION_DB_NAMES)
def test_profiled_fixtures_expose_current_compatibility_projection(
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
                "WHERE name IN ('titles', 'books', 'creators', 'comments', 'entity_identifiers', 'item_identifiers') "
                "ORDER BY name;"
            ).fetchall()
        }

        assert objects["titles"] == "view"
        assert objects["books"] == "view"
        assert "creators" not in objects
        assert objects["comments"] == "table"
        assert objects["entity_identifiers"] == "table"
        assert objects["item_identifiers"] == "table"

        title_count = int(conn.execute("SELECT COUNT(*) FROM titles;").fetchone()[0])
        work_count = int(conn.execute("SELECT COUNT(*) FROM works;").fetchone()[0])
        book_count = int(conn.execute("SELECT COUNT(*) FROM books;").fetchone()[0])

        assert title_count == work_count
        assert book_count == work_count

        title_sort_nulls = int(
            conn.execute(
                "SELECT COUNT(*) FROM titles WHERE title_creator_sort IS NULL;"
            ).fetchone()[0]
        )
        assert title_sort_nulls == title_count

        title_last_modified_distinct = int(
            conn.execute(
                "SELECT COUNT(DISTINCT title_last_modified) FROM titles;"
            ).fetchone()[0]
        )
        assert title_last_modified_distinct == 1

        book_last_modified_distinct = int(
            conn.execute(
                "SELECT COUNT(DISTINCT book_last_modified) FROM books;"
            ).fetchone()[0]
        )
        assert book_last_modified_distinct == 1

        mismatched_book_uuids = int(
            conn.execute(
                "SELECT COUNT(*) FROM books WHERE book_uuid != book_id;"
            ).fetchone()[0]
        )
        assert mismatched_book_uuids == 0

        for table in ("comments", "entity_identifiers", "item_identifiers"):
            count = int(conn.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0])
            assert count == 0, f"{db_name} unexpectedly populated compatibility-adjacent table {table!r}"
    finally:
        conn.close()
