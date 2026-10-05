"""
Verify identifier profiled fixture is empty across registered database profiles.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise test property identifier profiled fixture is empty through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_db_properties/test_property_identifier_profiled_fixture_is_empty.py
"""
from __future__ import annotations

import sqlite3

import pytest


@pytest.mark.catalog
def test_db_20_identifier_profile_is_empty_but_views_exist(
    provision_test_database,
) -> None:
    """
    Verify db 20 identifier profile is empty but views exist.

    Example:
        Exercise test db 20 identifier profile is empty but views exist through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_db_properties/test_property_identifier_profiled_fixture_is_empty.py


    :param provision_test_database: Value supplied for provision test database under the
        deterministic fixture contract.
    :return: None; completion is expressed through state changes or assertions.
    """
    provisioned = provision_test_database("test_db_20")
    conn = sqlite3.connect(str(provisioned.db_path))
    conn.row_factory = sqlite3.Row
    try:
        objects = {
            row["name"]: row["type"]
            for row in conn.execute(
                "SELECT name, type FROM sqlite_master "
                "WHERE name IN ('entity_identifiers', 'item_identifiers', 'identifiers', 'identifiers_v', 'identifier_title_links') "
                "ORDER BY name;"
            ).fetchall()
        }

        assert objects["entity_identifiers"] == "table"
        assert objects["item_identifiers"] == "table"
        assert objects["identifiers"] == "view"
        assert objects["identifiers_v"] == "view"
        assert "identifier_title_links" not in objects

        for table in ("entity_identifiers", "item_identifiers", "identifiers", "identifiers_v"):
            count = int(conn.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0])
            assert count == 0, f"test_db_20 unexpectedly populated identifier object {table!r}"
    finally:
        conn.close()
