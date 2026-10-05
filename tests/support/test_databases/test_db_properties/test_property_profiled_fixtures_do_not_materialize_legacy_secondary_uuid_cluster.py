"""
Verify profiled fixtures do not materialize legacy secondary uuid cluster across registered database profiles.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise test property profiled fixtures do not materialize legacy secondary uuid cluster through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_db_properties/test_property_profiled_fixtures_do_not_materialize_legacy_secondary_uuid_cluster.py
"""
from __future__ import annotations

import sqlite3

import pytest


LEGACY_SECONDARY_UUID_CLUSTER_DB_NAMES = (
    "test_db_18",
    "test_db_19",
    "test_db_21",
)


LEGACY_SPECIAL_TABLES = (
    "secondary_uuids",
    "books_secondary_uuid",
    "loc_shelf_numbers",
    "content_levels",
    "secondary_uuid_title_links",
    "book_books_secondary_uuid_links",
    "loc_shelf_number_title_links",
    "content_level_title_links",
    "publisher_owners",
    "year_first_published",
    "year_reprinted",
    "words",
    "character_introductions",
    "series_character_introductions",
    "not_series",
)


@pytest.mark.catalog
@pytest.mark.parametrize("db_name", LEGACY_SECONDARY_UUID_CLUSTER_DB_NAMES)
def test_profiled_fixtures_do_not_materialize_legacy_secondary_uuid_cluster(
    provision_test_database,
    db_name: str,
) -> None:
    """
    Verify profiled fixtures do not materialize legacy secondary uuid cluster.

    Example:
        Exercise test profiled fixtures do not materialize legacy secondary uuid cluster through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_db_properties/test_property_profiled_fixtures_do_not_materialize_legacy_secondary_uuid_cluster.py


    :param provision_test_database: Value supplied for provision test database under the
        deterministic fixture contract.
    :param db_name: Registered test-database profile name.
    :return: None; completion is expressed through state changes or assertions.
    """
    provisioned = provision_test_database(db_name)
    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        present = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table';"
            ).fetchall()
        }
        assert present.isdisjoint(LEGACY_SPECIAL_TABLES)
    finally:
        conn.close()
