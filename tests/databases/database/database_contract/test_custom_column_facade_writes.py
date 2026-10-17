"""Check reachable custom-column facade bookkeeping on real storage."""

from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from LiuXin_alpha.databases.custom_columns import CustomColumns


def test_create_edit_rename_and_delete_refresh_facade_state(db):
    facade = CustomColumns(db, table="works")
    preferences = []
    notifications = []
    facade.prefs = SimpleNamespace(set=lambda *args: preferences.append(args))
    facade.notify = lambda *args: notifications.append(args)
    column = facade.create_custom_column("Shelf", label="work_shelf", datatype="text")
    assert preferences == [("update_all_last_mod_dates_on_start", True)]
    record = facade.custom_field_metadata(num=column)
    assert record["in_table"] == "works"
    assert record["editable"] is True
    assert facade.custom_field_metadata(label="work_shelf") is record

    facade.set_custom_column_metadata(
        column,
        name="Reading shelf",
        label="reading_shelf",
        is_editable=False,
        display={"description": "Visible to readers"},
        notify=False,
    )
    record = facade.custom_field_metadata(num=column)
    assert record["name"] == "Reading shelf"
    assert record["editable"] is False
    assert record["display"] == {"description": "Visible to readers"}
    assert "work_shelf" not in facade.custom_column_label_map
    assert facade.custom_field_metadata(label="reading_shelf") is record
    assert notifications == []
    stored = db.get_row_from_id("custom_columns", column)
    assert stored["custom_column_label"] == "reading_shelf"
    assert int(stored["custom_column_editable"]) == 0
    assert stored["custom_column_in_table"] == "works"

    facade.set_custom_column_metadata(
        column, is_editable=True, update_last_modified=True
    )
    assert facade.custom_field_metadata(num=column)["editable"] is True
    assert notifications == [("metadata", [])]
    assert len(preferences) == 2
    facade.delete_custom_column(label="reading_shelf")
    assert (
        int(
            db.get_row_from_id("custom_columns", column)[
                "custom_column_mark_for_delete"
            ]
        )
        == 1
    )

    other = facade.create_custom_column("Other", label="other_shelf", datatype="int")
    facade.delete_custom_column(other)
    assert (
        int(
            db.get_row_from_id("custom_columns", other)["custom_column_mark_for_delete"]
        )
        == 1
    )


def test_failed_metadata_write_preserves_maps_and_skips_notifications(db, monkeypatch):
    facade = CustomColumns(db, table="works")
    column = facade.create_custom_column(
        "Shelf", label="unchanged_shelf", datatype="text"
    )
    before = deepcopy(facade.custom_column_num_map)
    notifications = []
    facade.notify = lambda *args: notifications.append(args)

    def fail_write(**kwargs):
        raise RuntimeError("injected metadata failure")

    monkeypatch.setattr(db.macros, "set_custom_column_metadata", fail_write)
    with pytest.raises(RuntimeError, match="injected"):
        facade.set_custom_column_metadata(column, is_editable=False)
    assert facade.custom_column_num_map == before
    assert notifications == []
    assert (
        int(db.get_row_from_id("custom_columns", column)["custom_column_editable"]) == 1
    )
