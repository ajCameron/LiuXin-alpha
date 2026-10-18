"""Keep persisted custom-column definitions intact when refresh cannot parse them."""

import sqlite3
from copy import deepcopy

import pytest

from LiuXin_alpha.databases.custom_columns import CustomColumns
from LiuXin_alpha.errors import DatabaseIntegrityError


@pytest.mark.parametrize("bad_display", ["{", "[]", '"not an object"'])
def test_refresh_preserves_corrupt_definition_values_and_previous_snapshot(
    db, bad_display
):
    facade = CustomColumns(db, table="works")
    facade.create_custom_column("Valid", label="valid_refresh_column")
    number = facade.create_custom_column("Shelf", label="protected_refresh_column")
    value_table, _ = facade.custom_table_names(number, in_table="works")
    db.driver_wrapper.execute(
        f"INSERT INTO {value_table} ({value_table}_value) VALUES (?)", ("Keep me",)
    )
    previous = deepcopy(facade.custom_column_num_map)
    previous_triggers = list(facade.triggers)
    db.driver_wrapper.execute(
        "UPDATE custom_columns SET custom_column_display=? WHERE custom_column_id=?",
        (bad_display, number),
    )
    CustomColumns(db, table="manifestations")
    with pytest.raises(DatabaseIntegrityError, match=f"Invalid custom column {number}"):
        facade.refresh_db_custom_columns_metadata()
    assert facade.custom_column_num_map == previous
    assert (
        facade.custom_column_label_map["protected_refresh_column"]
        is facade.custom_column_num_map[number]
    )
    assert facade.triggers == previous_triggers
    with pytest.raises(DatabaseIntegrityError, match=f"Invalid custom column {number}"):
        CustomColumns(db, table="works")
    with sqlite3.connect(db.metadata["database_path"]) as persisted:
        assert persisted.execute(
            "SELECT custom_column_display FROM custom_columns WHERE custom_column_id=?",
            (number,),
        ).fetchone() == (bad_display,)
        assert persisted.execute(
            f"SELECT {value_table}_value FROM {value_table}"
        ).fetchall() == [("Keep me",)]
    db.driver_wrapper.execute(
        "UPDATE custom_columns SET custom_column_display=? WHERE custom_column_id=?",
        ('{"sort_alpha": true}', number),
    )
    facade.refresh_db_custom_columns_metadata()
    assert facade.custom_column_num_map[number]["display"] == {"sort_alpha": True}


@pytest.mark.parametrize("missing_kind", ["value", "link"])
def test_missing_backing_table_does_not_delete_the_definition(db, missing_kind):
    facade = CustomColumns(db, table="works")
    number = facade.create_custom_column("Shelf", label="missing_refresh_column")
    value_table, link_table = facade.custom_table_names(number, in_table="works")
    # Remove dependent browser views so schema introspection can reach the
    # missing-table validation, rather than fail on a dangling SQLite view.
    db.driver_wrapper.execute(f"DROP VIEW IF EXISTS tag_browser_{value_table}")
    db.driver_wrapper.execute(f"DROP VIEW IF EXISTS tag_browser_filtered_{value_table}")
    missing_table = value_table if missing_kind == "value" else link_table
    db.driver_wrapper.execute(f"DROP TABLE {missing_table}")
    db.refresh_db_metadata()
    with pytest.raises(DatabaseIntegrityError, match="missing backing tables"):
        CustomColumns(db, table="works")
    assert db.get_row_from_id("custom_columns", number) is not None
