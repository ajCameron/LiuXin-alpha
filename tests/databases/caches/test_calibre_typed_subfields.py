"""Exercise role projections through real cache tables and public field reads."""

from copy import deepcopy

import pytest

from LiuXin_alpha.errors import NotInCache
from LiuXin_alpha.library.caches.calibre.fields import (
    CalibreManyToManyField,
    CalibreManyToOneField,
)
from LiuXin_alpha.library.caches.calibre.tables.many_many_tables.priority_typed_many_to_many_table import (
    CalibrePriorityTypedManyToManyTable,
)
from LiuXin_alpha.library.caches.calibre.tables.many_many_tables.typed_many_to_many_table import (
    CalibreTypedManyToManyTable,
)
from LiuXin_alpha.library.caches.calibre.tables.many_one_tables.priority_many_to_one_table import (
    CalibrePriorityManyToOneTable,
)
from LiuXin_alpha.library.caches.calibre.tables.many_one_tables.priority_typed_many_to_one_table import (
    CalibrePriorityTypedManyToOneTable,
)
from LiuXin_alpha.library.caches.calibre.tables.many_one_tables.typed_many_to_one_table import (
    CalibreTypedManyToOneTable,
)


def _table(cls):
    table = cls(
        "creators",
        metadata={
            "datatype": "text",
            "table": "agents",
            "column": "value",
            "val_unique": False,
            "is_multiple": {},
            "is_custom": False,
            "display": {},
        },
    )
    table.id_map = {101: "Alice", 102: "Bob", 103: "Carol"}
    table.seen_book_ids = {1, 2, 3}
    table.seen_item_ids = {101, 102, 103}
    table.link_table = "agent_title_links"
    table.link_table_bt_id_column = "agent_title_link_title_id"
    return table


@pytest.mark.parametrize(
    "cls", [CalibreTypedManyToOneTable, CalibrePriorityTypedManyToOneTable]
)
def test_many_one_projection_filters_both_directions_and_preserves_known_ids(cls):
    table = _table(cls)
    collection = list if table.priority else set
    table.seen_book_ids.add(4)
    table.book_col_map = {1: 101, 2: 102, 3: None, 4: 101}
    table.book_type_map = {1: "author", 2: "editor", 3: None, 4: "author"}
    table.col_book_map = {
        "author": {101: collection([4, 1])},
        "editor": {102: collection([2])},
    }
    before = deepcopy((table.book_col_map, table.col_book_map))
    projected = table.get_subtable("author")
    field = CalibreManyToOneField("creators", projected)
    assert field.for_book(1) == "Alice"
    assert field.for_book(2) is None
    assert field.for_book(3) is None
    assert set(field.books_for(101)) == {1, 4}
    if table.priority:
        assert field.books_for(101) == [4, 1]
    assert field.books_for(102) is None
    assert projected.priority == table.priority
    assert projected.link_table_bt_id_column == table.link_table_bt_id_column
    with pytest.raises(NotInCache):
        field.for_book(99)
    empty = CalibreManyToOneField("creators", table.get_subtable("missing"))
    assert empty.for_book(1) is None
    assert empty.books_for(101, default_value=None) is None
    projected.book_col_map[1] = 103
    projected.col_book_map[101].clear()
    assert (table.book_col_map, table.col_book_map) == before
    if table.priority:
        direct = CalibrePriorityManyToOneTable.from_priority_typed_table(
            table, "editor"
        )
        assert CalibreManyToOneField("creators", direct).for_book(2) == "Bob"


@pytest.mark.parametrize(
    "cls", [CalibreTypedManyToManyTable, CalibrePriorityTypedManyToManyTable]
)
def test_many_many_subfield_needs_no_database_and_preserves_role_and_order(cls):
    table = _table(cls)
    collection = list if table.priority else set
    table.book_col_map = {
        "author": {1: collection([102, 101])},
        "editor": {1: collection([103]), 2: collection([102])},
    }
    table.col_book_map = {
        "author": {101: collection([1]), 102: collection([1])},
        "editor": {103: collection([1]), 102: collection([2])},
    }
    before = deepcopy((table.book_col_map, table.col_book_map))
    source = CalibreManyToManyField("creators", table)
    field = source.get_subfield("author")
    assert set(field.for_book(1)) == {"Alice", "Bob"}
    assert field.for_book(2) is None
    assert field.for_book(3) is None
    assert set(field.ids_for_book(1)) == {101, 102}
    assert list(field.books_for(101)) == [1]
    assert field.books_for(103, default_value=None) is None
    assert field.table.priority == table.priority
    assert field.table.link_table == table.link_table
    if table.priority:
        assert list(field.ids_for_book(1)) == [102, 101]
        assert field.for_book(1) == ["Bob", "Alice"]
        direct = table.get_priority_subtable(None, "editor")
        assert CalibreManyToManyField("creators", direct).for_book(2) == ["Bob"]
        direct.book_col_map[2].clear()
        assert table.book_col_map["editor"][2] == [102]
    with pytest.raises(NotInCache):
        field.for_book(99)
    empty = source.get_subfield("missing")
    assert empty.for_book(1) is None
    assert empty.books_for(101, default_value=None) is None
    field.table.book_col_map[1].clear()
    field.table.col_book_map[101].clear()
    assert (table.book_col_map, table.col_book_map) == before


def test_drop_priority_factory_returns_a_typed_projection():
    original = _table(CalibrePriorityTypedManyToManyTable)
    original.book_col_map = {"author": {1: [102, 101]}, "editor": {2: [103]}}
    original.col_book_map = {"author": {101: [1], 102: [1]}, "editor": {103: [2]}}
    projection = CalibreTypedManyToManyTable.from_priority_typed_table(
        None, original, "author"
    )
    assert projection.typed and not projection.priority
    field = CalibreManyToManyField("creators", projection)
    assert field.for_book(1) == {"author": {"Alice", "Bob"}}
    assert field.for_book(2) is None
    assert projection.known_link_types == {"author"}
