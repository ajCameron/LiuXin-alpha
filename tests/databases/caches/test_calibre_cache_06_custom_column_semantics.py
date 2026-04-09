from __future__ import annotations

import pytest

from LiuXin_alpha.databases.categories import find_categories
from LiuXin_alpha.databases.caches.calibre.tables.one_one_tables import (
    CalibreCustomColumnsOneToOneTable,
)
from LiuXin_alpha.databases.field_metadata import FieldMetadata
from LiuXin_alpha.errors import InvalidCacheUpdate

def _add_custom_field(
    fm: FieldMetadata,
    *,
    label: str,
    datatype: str,
    colnum: int,
    is_category: bool,
    display: dict | None = None,
    in_table: str = "books",
    is_multiple: dict | None = None,
) -> None:
    fm.add_custom_field(
        label=label,
        table=f"custom_column_{colnum}",
        column="value",
        datatype=datatype,
        colnum=colnum,
        name=f"UT {label}",
        display=display or {},
        is_editable=True,
        is_multiple=is_multiple or {},
        is_category=is_category,
        in_table=in_table,
    )


def test_find_categories_custom_field_visibility_rules() -> None:
    fm = FieldMetadata()

    _add_custom_field(
        fm,
        label="books_tags",
        datatype="text",
        colnum=1,
        is_category=True,
        in_table="books",
        is_multiple={"cache_to_list": "|", "ui_to_list": ",", "list_to_ui": ", "},
    )
    _add_custom_field(
        fm,
        label="titles_only",
        datatype="text",
        colnum=2,
        is_category=True,
        in_table="titles",
    )
    _add_custom_field(
        fm,
        label="comp_cat",
        datatype="composite",
        colnum=3,
        is_category=False,
        in_table="books",
        display={"make_category": True},
    )
    _add_custom_field(
        fm,
        label="comp_nocat",
        datatype="composite",
        colnum=4,
        is_category=False,
        in_table="books",
        display={"make_category": False},
    )
    _add_custom_field(
        fm,
        label="comp_titles",
        datatype="composite",
        colnum=5,
        is_category=False,
        in_table="titles",
        display={"make_category": True},
    )

    categories = {name: is_composite for name, _, is_composite in find_categories(fm)}

    assert categories["#books_tags"] is False
    assert "#titles_only" not in categories
    assert categories["#comp_cat"] is True
    assert "#comp_nocat" not in categories
    assert "#comp_titles" not in categories


def test_custom_one_to_one_update_precheck_accepts_scalars_and_none() -> None:
    table = CalibreCustomColumnsOneToOneTable(
        "custom_column_1",
        metadata={"datatype": "int", "display": {}, "is_multiple": {}},
        custom=True,
    )
    table.seen_book_ids = {1, 2}

    table.update_precheck({1: 42, 2: None}, id_map_update={})


@pytest.mark.parametrize("bad_value", ([1, 2], {1, 2}, {"v": 1}, ("ordered",)))
def test_custom_one_to_one_update_precheck_rejects_container_values(bad_value) -> None:
    table = CalibreCustomColumnsOneToOneTable(
        "custom_column_1",
        metadata={"datatype": "int", "display": {}, "is_multiple": {}},
        custom=True,
    )
    table.seen_book_ids = {1}

    with pytest.raises(InvalidCacheUpdate):
        table.update_precheck({1: bad_value}, id_map_update={})


def test_custom_one_to_one_update_precheck_rejects_unknown_books_and_bad_scalars() -> None:
    table = CalibreCustomColumnsOneToOneTable(
        "custom_column_1",
        metadata={"datatype": "int", "display": {}, "is_multiple": {}},
        custom=True,
    )
    table.seen_book_ids = {1}

    with pytest.raises(InvalidCacheUpdate):
        table.update_precheck({9: 42}, id_map_update={})

    def _must_be_positive(value):
        if value < 0:
            raise ValueError("value must be positive")

    with pytest.raises(InvalidCacheUpdate):
        table.update_precheck({1: -1}, id_map_update={}, acceptance_functions=[_must_be_positive])
