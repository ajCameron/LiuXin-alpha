"""Exercise cache, iterator and resource boundaries of the custom-column service."""

from __future__ import annotations

from contextlib import closing
from types import SimpleNamespace

import pytest

from LiuXin_alpha.utils.calibre_compat.custom_columns import (
    CustomColumns,
)
from LiuXin_alpha.databases.custom_columns.native_service import CustomFieldService
from LiuXin_alpha.databases.custom_columns.native_types import CustomFieldError
from tests.databases.custom_field_fixtures import sqlite_db as sqlite_db
from tests.databases.custom_field_fixtures import sqlite_template as sqlite_template


class Cache:
    def __init__(self):
        self._data = {7: [7, "old"], 8: [8, "old"]}
        self.writes = []

    def __getitem__(self, position):
        return self._data[(8, 7)[position]]

    def set(self, row, col, val, row_is_id=False):
        self.writes.append((row, col, val, row_is_id))
        self._data[row][col] = val


def facade_with_cache(db=None, *, datatype="text"):
    if db is not None:
        for owner in (7, 8):
            db.macros.insert_row("works", {"work_id": owner, "work_title": str(owner)})
        facade = CustomColumns(db, table="works")
        facade.create_custom_column("First", label="first")
        facade.create_custom_column("Second", label="second")
        assert (
            facade.create_custom_column(
                "Shelf",
                label="shelf",
                datatype=datatype,
                is_multiple=datatype == "text",
            )
            == 3
        )
        facade.set_custom_bulk([7, 8], ["old"] if datatype == "text" else "old", num=3)
        facade.data = Cache()
        facade.FIELD_MAP = {"id": 0, 3: 1}
        if datatype == "series":
            facade.FIELD_MAP["3_index"] = 2
            for row in facade.data._data.values():
                row.append(1.0)
        return facade

    facade = object.__new__(CustomColumns)
    facade.data = Cache()
    facade.notify = lambda *args: None
    facade.dirtied = lambda *args, **kwargs: None
    facade.FIELD_MAP = {"id": 0, 3: 1}
    facade.custom_column_num_map = {
        3: {
            "num": 3,
            "label": "shelf",
            "datatype": "text",
            "editable": True,
            "is_multiple": True,
            "normalized": True,
            "in_table": "books",
            "display": {},
            "multiple_seps": {"cache_to_list": "|"},
        }
    }
    facade.custom_column_label_map = {"shelf": facade.custom_column_num_map[3]}
    facade.field_metadata = SimpleNamespace(custom_field_prefix="#")
    return facade


def test_delete_custom_value_updates_its_owners_cache(sqlite_db):
    db = sqlite_db
    facade = facade_with_cache(db)
    value_id = facade.get_custom_items_with_ids(num=3)[0][0]
    facade.delete_custom_item_using_id(value_id, num=3)
    assert facade.get_custom_items_with_ids(num=3) == []
    assert facade.data.writes == [(7, 1, None, True), (8, 1, None, True)]


def test_bulk_multiple_reuses_single_pass_owner_ids(sqlite_db):
    db = sqlite_db
    facade = facade_with_cache(db)
    dirtied, notified = [], []
    facade.dirtied = lambda ids, **kwargs: dirtied.extend(ids)
    facade.notify = lambda event, ids: notified.extend(ids)
    facade.set_custom_bulk_multiple(
        iter((7, 8)), add=["new"], remove=["old"], num=3, notify=True
    )
    assert dirtied == notified == [7, 8]
    assert facade.data.writes == [(7, 1, "new", True), (8, 1, "new", True)]
    with closing(db.driver.get_connection()) as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM works_custom_column_3_link").fetchone()[
                0
            ]
            == 2
        )


@pytest.mark.parametrize("fail", [False, True])
def test_clean_custom_closes_its_owned_connection(sqlite_db, monkeypatch, fail):
    db = sqlite_db
    facade = facade_with_cache(db)
    facade.native.ensure_value(3, "unused")
    opened, closed = [], []
    original = db.driver.get_connection

    def tracked(*args, **kwargs):
        connection = original(*args, **kwargs)
        opened.append(connection)
        close = connection.close

        def close_tracked():
            closed.append(connection)
            close()

        monkeypatch.setattr(connection, "close", close_tracked)
        return connection

    monkeypatch.setattr(db.driver, "get_connection", tracked)
    if fail:

        def fail_delete(*args, **kwargs):
            raise RuntimeError("injected cleanup failure")

        monkeypatch.setattr(CustomFieldService, "delete_value", fail_delete)
        with pytest.raises(RuntimeError, match="injected"):
            facade.clean_custom()
    else:
        facade.clean_custom()
    assert opened and closed == opened


def test_cached_row_positions_are_not_persistent_ids(sqlite_db):
    db = sqlite_db
    facade = facade_with_cache(db)
    assert facade.id(0) == 8
    assert facade.get_custom_and_extra(0, num=3) == (["old"], None)
    facade.data = None
    with pytest.raises(RuntimeError, match="require a results cache"):
        facade.get_custom(0, num=3)


def test_standalone_notification_hooks_keep_embedded_failures_visible():
    facade = object.__new__(CustomColumns)
    facade.embed = False
    facade.dirtied = facade._default_dirtied
    facade.notify = facade._default_notify
    facade.dirtied([7])
    facade.notify("metadata", [7])
    facade.embed = True
    with pytest.raises(NotImplementedError, match="embedded"):
        facade.dirtied([7])
    with pytest.raises(NotImplementedError, match="embedded"):
        facade.notify("metadata", [7])


@pytest.mark.parametrize("datatype", ["text", "series"])
@pytest.mark.parametrize("value_id", [5, None])
def test_case_change_uses_value_id_and_rejects_a_missing_lookup(
    sqlite_db, datatype, value_id
):
    db = sqlite_db
    facade = facade_with_cache(db, datatype=datatype)
    actual_id = facade.get_custom_items_with_ids(num=3)[0][0]
    if value_id is None:
        with pytest.raises(CustomFieldError, match="Unknown inventory value"):
            facade.rename_custom_item(actual_id + 99, "OLD", num=3)
        assert facade.get_custom_items_with_ids(num=3) == [(actual_id, "old")]
    else:
        assert facade.set_custom(
            7,
            ["OLD"] if datatype == "text" else "OLD",
            num=3,
            append=True,
            extra=1.0,
            allow_case_change=True,
        ) == {7, 8}
        assert facade.get_custom_items_with_ids(num=3) == [(actual_id, "OLD")]
        assert [facade.data._data[owner][1] for owner in (7, 8)] == ["OLD", "OLD"]


def test_wrapper_table_cache_remains_rebindable():
    from types import SimpleNamespace
    from LiuXin_alpha.databases.driver_wrapper import DriverWrapper

    wrapper = DriverWrapper(SimpleNamespace(macros=object()))
    replacement = {"custom_column_3"}
    wrapper.custom_tables = replacement
    wrapper.custom_tables.add("books_custom_column_3_link")
    assert wrapper.custom_tables is replacement
    assert len(replacement) == 2
