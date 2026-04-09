from __future__ import annotations

import sqlite3

import pytest

from tests.support.test_databases.test_db_properties import TEST_DB_PROPERTY_CLASS_MAP


ALPHA_NORMALIZED_DB_NAMES = (
    "test_db_0",
    "test_db_1",
    "test_db_2",
    "test_db_3",
    "test_db_4",
    "test_db_5",
    "test_db_6",
    "test_db_7",
    "test_db_8",
    "test_db_9",
    "test_db_10",
    "test_db_11",
    "test_db_12",
    "test_db_13",
    "test_db_14",
    "test_db_15",
    "test_db_16",
    "test_db_17",
    "test_db_18",
    "test_db_19",
    "test_db_20",
    "test_db_21",
    "test_db_22",
    "test_db_23",
    "test_db_24",
    "test_db_25",
)


@pytest.mark.parametrize("db_name", ALPHA_NORMALIZED_DB_NAMES, ids=ALPHA_NORMALIZED_DB_NAMES)
def test_alpha_property_subset_matches_live_schema(provision_test_database, db_name: str) -> None:
    properties_class = TEST_DB_PROPERTY_CLASS_MAP[db_name]
    provisioned = provision_test_database(db_name)

    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        table_names = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        assert set(properties_class.alpha_focus_tables).issubset(table_names)

        for table_name, expected_columns in properties_class.alpha_focus_table_columns.items():
            actual_columns = [row[1] for row in conn.execute(f"PRAGMA table_info({table_name})")]
            assert actual_columns == expected_columns

        for table_name, expected_count in properties_class.alpha_focus_row_counts.items():
            actual_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            assert actual_count == expected_count

        version_value = conn.execute("SELECT database_version_version FROM database_version").fetchone()[0]
        assert isinstance(version_value, str)
        assert version_value
        for required_substring in properties_class.alpha_database_version_required_substrings:
            assert required_substring in version_value
    finally:
        conn.close()
