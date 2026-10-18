"""Build the legacy custom-column fixture through its live database owner."""

from LiuXin_alpha.databases.custom_columns import CustomColumns
from tests.support.test_databases.test_db_6 import (
    TestDBCustomColumns as CustomColumnFixture,
)


def test_custom_column_fixture_uses_existing_database_connection(db):
    CustomColumnFixture.detail_databases(db)
    facade = CustomColumns(db, table="manifestations")
    assert facade.table == "manifestations"
    expected_types = ["text", "bool", "comments", "text", "series"]
    assert len(facade.custom_column_num_map) == 5
    for index, datatype in enumerate(expected_types, start=1):
        definition = facade.custom_field_metadata(label=f"test_cc_{index}_label")
        assert definition["name"] == f"test_cc_{index}_name"
        assert definition["datatype"] == datatype
        assert definition["is_multiple"] is (index == 4)
        assert definition["editable"] is True
        assert definition["display"] == {}
        table, _ = facade.custom_table_names(definition["num"], in_table=facade.table)
        assert db.driver_wrapper.get_all_rows(table) == []
    # The fixture borrows the database; it must not close its owner's connection.
    assert len(list(db.get_all_rows("custom_columns"))) == 5
