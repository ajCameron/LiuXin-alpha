"""
Verify blank optional metadata profiles across registered database profiles.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise test property blank optional metadata profiles through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_db_properties/test_property_blank_optional_metadata_profiles.py
"""
from __future__ import annotations

import sqlite3

import pytest


BLANK_OPTIONAL_METADATA_DB_NAMES = (
    "test_db_1",
    "test_db_4",
    "test_db_6",
    "test_db_10",
    "test_db_14",
    "test_db_15",
    "test_db_16",
    "test_db_20",
    "test_db_21",
    "test_db_22",
    "test_db_23",
    "test_db_24",
    "test_db_25",
)


@pytest.mark.catalog
@pytest.mark.parametrize("db_name", BLANK_OPTIONAL_METADATA_DB_NAMES)
def test_blank_optional_metadata_profiles_are_stable(provision_test_database, db_name: str) -> None:
    """
    Verify blank optional metadata profiles are stable.

    Example:
        Exercise test blank optional metadata profiles are stable through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_db_properties/test_property_blank_optional_metadata_profiles.py


    :param provision_test_database: Value supplied for provision test database under the
        deterministic fixture contract.
    :param db_name: Registered test-database profile name.
    :return: None; completion is expressed through state changes or assertions.
    """
    provisioned = provision_test_database(db_name)
    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        for table in (
            "human_agents",
            "notes",
            "comments",
            "synopses",
            "annotations",
            "entity_identifiers",
            "item_identifiers",
        ):
            count = int(conn.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0])
            assert count == 0, f"{db_name} unexpectedly populated optional table {table!r}"

        agent_rows = conn.execute(
            "SELECT agent_id, agent_type, agent_canonical_name FROM agents ORDER BY agent_id;"
        ).fetchall()
        assert agent_rows == [(0, "organisation", "DELIBERATELY SET NULL")]
    finally:
        conn.close()
