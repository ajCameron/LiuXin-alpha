"""
Verify rich content profiles are generic across registered database profiles.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise test property rich content profiles are generic through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_db_properties/test_property_rich_content_profiles_are_generic.py
"""
from __future__ import annotations

import sqlite3

import pytest


RICH_CONTENT_PROFILE_DB_NAMES = (
    "test_db_4",
    "test_db_10",
)


@pytest.mark.catalog
@pytest.mark.parametrize("db_name", RICH_CONTENT_PROFILE_DB_NAMES)
def test_profiled_rich_content_fixtures_are_now_generic(
    provision_test_database,
    db_name: str,
) -> None:
    """
    Verify profiled rich content fixtures are now generic.

    Example:
        Exercise test profiled rich content fixtures are now generic through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_db_properties/test_property_rich_content_profiles_are_generic.py


    :param provision_test_database: Value supplied for provision test database under the
        deterministic fixture contract.
    :param db_name: Registered test-database profile name.
    :return: None; completion is expressed through state changes or assertions.
    """
    provisioned = provision_test_database(db_name)
    conn = sqlite3.connect(str(provisioned.db_path))
    conn.row_factory = sqlite3.Row
    try:
        objects = {
            row["name"]: row["type"]
            for row in conn.execute(
                "SELECT name, type FROM sqlite_master "
                "WHERE name IN ('titles', 'books', 'creators', 'comments', 'notes', 'synopses', 'annotations', 'human_agents', 'org_agents') "
                "ORDER BY name;"
            ).fetchall()
        }

        assert objects["titles"] == "view"
        assert objects["books"] == "view"
        assert "creators" not in objects
        for table in ("comments", "notes", "synopses", "annotations", "human_agents", "org_agents"):
            assert objects[table] == "table"
            count = int(conn.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0])
            assert count == 0, f"{db_name} unexpectedly populated rich-content table {table!r}"

        work_count = int(conn.execute("SELECT COUNT(*) FROM works;").fetchone()[0])
        title_count = int(conn.execute("SELECT COUNT(*) FROM titles;").fetchone()[0])
        book_count = int(conn.execute("SELECT COUNT(*) FROM books;").fetchone()[0])
        assert title_count == work_count
        assert book_count == work_count

        null_work_titles = int(
            conn.execute("SELECT COUNT(*) FROM works WHERE work_title IS NULL;").fetchone()[0]
        )
        assert null_work_titles == work_count

        title_rows = conn.execute(
            "SELECT title_id, title FROM titles ORDER BY title_id LIMIT 5;"
        ).fetchall()
        assert title_rows
        for index, row in enumerate(title_rows, start=1):
            assert row["title_id"] == index
            assert row["title"] == f"{db_name} title {index:03d}"
    finally:
        conn.close()
