"""Exercise cache, iterator and resource boundaries of the custom-column service."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from LiuXin_alpha.databases.custom_columns import CustomColumns
from LiuXin_alpha.databases.api.macros_api import MacrosAPI
from LiuXin_alpha.errors import DatabaseIntegrityError


class Cache:
    def __init__(self):
        self._data = {7: [7, "old"], 8: [8, "old"]}
        self.writes = []

    def __getitem__(self, position):
        return self._data[(8, 7)[position]]

    def set(self, row, col, val, row_is_id=False):
        self.writes.append((row, col, val, row_is_id))
        self._data[row][col] = val


def facade_with_cache():
    facade = object.__new__(CustomColumns)
    facade.data = Cache()
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
    return facade


def test_delete_custom_value_updates_its_owners_cache():
    facade = facade_with_cache()
    deleted = []
    facade.custom_dirty_books_referencing = lambda *args, **kwargs: [(7,), (8,)]
    facade.db = SimpleNamespace(
        macros=Mock(spec=MacrosAPI, delete_cc_item=lambda *args: deleted.append(args))
    )
    facade.delete_custom_item_using_id(9, num=3)
    assert deleted == [("custom_column_3", "books_custom_column_3_link", 9)]
    assert facade.data.writes == [(7, 1, None, True), (8, 1, None, True)]


def test_bulk_multiple_reuses_single_pass_owner_ids():
    facade = facade_with_cache()
    inserted, dirtied, notified, commits = [], [], [], []
    connection = SimpleNamespace(commit=lambda: commits.append(True))
    facade.db = SimpleNamespace(
        driver=SimpleNamespace(conn=connection),
        macros=Mock(spec=MacrosAPI,
            get_all_cc_custom_values=lambda *args, **kwargs: [("new",)],
            create_cc_temp_tables=lambda *args, **kwargs: None,
            insert_values_into_temp_table=lambda table, ids, **kwargs: inserted.extend(
                ids
            ),
            do_cc_db_bulk_addition=lambda *args, **kwargs: None,
            destroy_cc_temp_tables=lambda *args, **kwargs: None,
            read_cc_value_from_meta_2=lambda num, owner, **kwargs: f"new-{owner}",
        ),
    )
    facade.dirtied = lambda ids, **kwargs: dirtied.extend(ids)
    facade.notify = lambda event, ids: notified.extend(ids)
    facade.set_custom_bulk_multiple(iter((7, 8)), add=["new"], num=3, notify=True)
    assert inserted == dirtied == notified == [7, 8]
    assert facade.data.writes == [(7, 1, "new-7", True), (8, 1, "new-8", True)]
    assert commits == [True]


@pytest.mark.parametrize("fail", [False, True])
def test_clean_custom_closes_its_owned_connection(fail):
    closed = []
    connection = SimpleNamespace(close=lambda: closed.append(True))
    facade = facade_with_cache()

    def clean(**kwargs):
        assert kwargs["conn"] is connection
        if fail:
            raise RuntimeError("injected cleanup failure")

    facade.db = SimpleNamespace(
        driver=SimpleNamespace(get_connection=lambda: connection),
        macros=Mock(spec=MacrosAPI, clean_custom=clean),
    )
    if fail:
        with pytest.raises(RuntimeError, match="injected"):
            facade.clean_custom()
    else:
        facade.clean_custom()
    assert closed == [True]


def test_cached_row_positions_are_not_persistent_ids():
    facade = facade_with_cache()
    assert facade.id(0) == 8
    assert facade.get_custom_and_extra(0, num=3) == (["old"], None)
    facade.data = None
    with pytest.raises(RuntimeError, match="require a results cache"):
        facade.get_custom(7, num=3, index_is_id=True)


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
def test_case_change_uses_value_id_and_rejects_a_missing_lookup(datatype, value_id):
    facade = facade_with_cache()
    metadata = facade.custom_column_num_map[3]
    metadata["datatype"] = datatype
    updates, linked = [], []
    facade.custom_data_adapters = {datatype: lambda value, data: value}
    facade.db = SimpleNamespace(
        macros=Mock(spec=MacrosAPI,
            get_all_cc_custom_values=lambda *args, **kwargs: [("old",)],
            get_cc_id_from_value=lambda *args, **kwargs: value_id,
            update_cc_value=lambda table, cc_id, cc_value: updates.append(
                (table, cc_id, cc_value)
            ),
            check_for_cc_link=lambda *args, **kwargs: linked.append(args) or True,
            get_cc_lt_books_from_lt_value=lambda *args, **kwargs: [(8,)],
            read_cc_value_from_meta_2=lambda *args, **kwargs: "OLD",
        )
    )
    if value_id is None:
        with pytest.raises(DatabaseIntegrityError, match="disappeared"):
            facade._set_custom(
                7,
                ["OLD"],
                num=3,
                append=True,
                extra=1.0,
                notify=False,
                allow_case_change=True,
            )
        assert updates == linked == []
    else:
        assert facade._set_custom(
            7,
            ["OLD"],
            num=3,
            append=True,
            extra=1.0,
            notify=False,
            allow_case_change=True,
        ) == {8}
        assert updates == [("custom_column_3", 5, "OLD")]


def test_wrapper_table_cache_remains_rebindable():
    from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_custom_columns_mixin import (
        CustomColumnsDriverWrapperMixin,
    )

    wrapper = CustomColumnsDriverWrapperMixin(None, None)
    replacement = {"custom_column_3"}
    wrapper.custom_tables = replacement
    wrapper.custom_tables.add("books_custom_column_3_link")
    assert wrapper.custom_tables is replacement
    assert len(replacement) == 2
