from __future__ import annotations

import sqlite3

import pytest

from tests.support.test_databases.test_db_properties import (
    ALL_TEST_DB_PROPERTY_CLASSES,
    TEST_DB_PROPERTY_CLASS_MAP,
    CommonDBProperties,
)


EXPECTED_TEST_DB_NAMES = tuple(f"test_db_{i}" for i in range(26))
PROVISION_SPOT_CHECK_NAMES = ("test_db_0", "test_db_1", "test_db_19", "test_db_25")


def test_property_registry_covers_full_legacy_db_range() -> None:
    assert len(ALL_TEST_DB_PROPERTY_CLASSES) == 26
    assert tuple(TEST_DB_PROPERTY_CLASS_MAP.keys()) == EXPECTED_TEST_DB_NAMES


@pytest.mark.parametrize(
    ("db_name", "properties_class"),
    tuple(TEST_DB_PROPERTY_CLASS_MAP.items()),
    ids=tuple(TEST_DB_PROPERTY_CLASS_MAP.keys()),
)
def test_property_registry_classes_are_structurally_consistent(db_name: str, properties_class) -> None:
    assert issubclass(properties_class, CommonDBProperties)
    assert properties_class.__name__ == f"TestDB{int(db_name.split('_')[-1])}Properties"

    db_main_tables = getattr(properties_class, "theo_db_main_tables", None)
    if db_main_tables is not None:
        assert isinstance(db_main_tables, set)
        assert db_main_tables
        assert all(isinstance(table, str) and table for table in db_main_tables)

    main_tables = getattr(properties_class, "theo_main_tables", None)
    if main_tables is not None:
        assert isinstance(main_tables, set)
        assert main_tables
        assert all(isinstance(table, str) and table for table in main_tables)
        if db_main_tables is not None:
            assert main_tables.issubset(db_main_tables)

    tables_and_columns = getattr(properties_class, "theo_tables_and_columns", None)
    if tables_and_columns is not None:
        assert isinstance(tables_and_columns, dict)
        assert tables_and_columns
        assert all(isinstance(table, str) and table for table in tables_and_columns)
        for columns in tables_and_columns.values():
            assert isinstance(columns, list)
            assert columns
            assert all(isinstance(column, str) and column for column in columns)
            assert len(columns) == len(set(columns))
        if db_main_tables is not None:
            assert db_main_tables.issubset(set(tables_and_columns))

    declared_uuid = getattr(properties_class, "db_uuid", None) or getattr(properties_class, "theo_db_uuid", None)
    if declared_uuid is not None:
        assert isinstance(declared_uuid, str)
        assert declared_uuid


def test_property_registry_db_names_exist_in_resource_manager_listing(test_resources_manager) -> None:
    available = set(test_resources_manager.available_test_databases())
    assert set(EXPECTED_TEST_DB_NAMES).issubset(available)


@pytest.mark.parametrize("db_name", PROVISION_SPOT_CHECK_NAMES, ids=PROVISION_SPOT_CHECK_NAMES)
def test_property_registry_spot_check_names_provision_against_live_resources(
    provision_test_database, db_name: str
) -> None:
    provisioned = provision_test_database(db_name)
    assert provisioned.db_path.exists()

    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        table_names = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        assert "database_version" in table_names
        assert "works" in table_names
    finally:
        conn.close()
