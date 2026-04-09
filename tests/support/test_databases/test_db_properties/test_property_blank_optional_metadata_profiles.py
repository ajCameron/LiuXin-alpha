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


@pytest.mark.db
@pytest.mark.parametrize("db_name", BLANK_OPTIONAL_METADATA_DB_NAMES)
def test_blank_optional_metadata_profiles_are_stable(provision_test_database, db_name: str) -> None:
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
