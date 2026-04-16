from __future__ import annotations

from collections import defaultdict

import pytest

from tests.support._import_compat import ensure_interfaces_field_metadata_alias

ensure_interfaces_field_metadata_alias()

from LiuXin_alpha.library.caches.calibre.fields import (
    CalibreManyToManyField,
    CalibreManyToOneField,
    CalibreOneToManyField,
)
from LiuXin_alpha.library.caches.calibre.tables.many_many_tables.many_to_many_table import (
    CalibreManyToManyTable,
)
from LiuXin_alpha.library.caches.calibre.tables.many_many_tables.priority_many_to_many_table import (
    CalibrePriorityManyToManyTable,
)
from LiuXin_alpha.library.caches.calibre.tables.many_many_tables.priority_typed_many_to_many_table import (
    CalibrePriorityTypedManyToManyTable,
)
from LiuXin_alpha.library.caches.calibre.tables.many_many_tables.typed_many_to_many_table import (
    CalibreTypedManyToManyTable,
)
from LiuXin_alpha.library.caches.calibre.tables.many_one_tables.many_to_one_table import (
    CalibreManyToOneTable,
)
from LiuXin_alpha.library.caches.calibre.tables.many_one_tables.priority_typed_many_to_one_table import (
    CalibrePriorityTypedManyToOneTable,
)
from LiuXin_alpha.library.caches.calibre.tables.many_one_tables.typed_many_to_one_table import (
    CalibreTypedManyToOneTable,
)
from LiuXin_alpha.library.caches.calibre.tables.one_many_tables.one_to_many_table import (
    CalibreOneToManyTable,
)
from LiuXin_alpha.library.caches.calibre.tables.one_many_tables.priority_one_to_many_table import (
    CalibrePriorityOneToManyTable,
)
from LiuXin_alpha.library.caches.calibre.tables.one_many_tables.priority_typed_one_to_many_table import (
    CalibrePriorityTypedOneToManyTable,
)
from LiuXin_alpha.library.caches.calibre.tables.one_many_tables.typed_one_to_many_table import (
    CalibreTypedOneToManyTable,
)
from LiuXin_alpha.errors import NotInCache


def _metadata(*, datatype: str = "text", val_unique: bool = True) -> dict:
    return {
        "datatype": datatype,
        "table": "dummy_table",
        "column": "value",
        "val_unique": val_unique,
        "is_multiple": {},
        "is_custom": False,
        "display": {},
    }


def _seed_one_to_many_default(*, priority: bool = False, val_unique: bool = False) -> CalibreOneToManyField:
    table_cls = CalibrePriorityOneToManyTable if priority else CalibreOneToManyTable
    table = table_cls("notes", metadata=_metadata(val_unique=val_unique))
    table.table_type = table._table_type
    table.id_map = {101: "alpha", 102: "beta", 104: "delta"}
    table.seen_book_ids = {1, 2}
    table.seen_item_ids = {101, 102, 104}
    table.book_col_map[1] = [101, 102] if priority else {101, 102}
    table.book_col_map[2] = [] if priority else set()
    if val_unique:
        table.col_book_map = {101: 1, 102: 1, 104: None}
    else:
        table.col_book_map = {
            101: [1, 7] if priority else {1, 7},
            102: [1] if priority else {1},
        }
    return CalibreOneToManyField("notes", table)


def _seed_one_to_many_typed(*, priority: bool = False, val_unique: bool = False) -> CalibreOneToManyField:
    table_cls = CalibrePriorityTypedOneToManyTable if priority else CalibreTypedOneToManyTable
    table = table_cls("notes", metadata=_metadata(val_unique=val_unique))
    table.table_type = table._table_type
    table.id_map = {101: "alpha", 102: "beta", 103: "gamma", 104: "delta"}
    table.seen_book_ids = {1, 2}
    table.seen_item_ids = {101, 102, 103, 104}
    table.seen_link_types = {"primary", "secondary"}
    empty = defaultdict(list if priority else set)
    table.book_col_map = {
        "primary": defaultdict(list if priority else set, {1: [101] if priority else {101}, 2: [] if priority else set()}),
        "secondary": defaultdict(
            list if priority else set,
            {1: [102, 103] if priority else {102, 103}, 2: [] if priority else set()},
        ),
    }
    if val_unique:
        table.col_book_map = {101: 1, 102: 1, 103: 1, 104: None}
    else:
        table.col_book_map = {
            101: {"primary": [1, 7]} if priority else {"primary": {1, 7}},
            102: {"secondary": [1]} if priority else {"secondary": {1}},
            103: {"secondary": [1]} if priority else {"secondary": {1}},
        }
    return CalibreOneToManyField("notes", table)


def _seed_many_to_one_default() -> CalibreManyToOneField:
    table = CalibreManyToOneTable("series", metadata=_metadata(datatype="series"))
    table.id_map = {201: "Series A", 202: "Series B"}
    table.seen_book_ids = {1, 2}
    table.seen_item_ids = {201, 202}
    table.book_col_map = {1: 201, 2: None}
    table.col_book_map = {201: {1, 7}, 202: set()}
    return CalibreManyToOneField("series", table)


def _seed_many_to_one_typed(*, priority: bool = False) -> CalibreManyToOneField:
    table_cls = CalibrePriorityTypedManyToOneTable if priority else CalibreTypedManyToOneTable
    table = table_cls("creators", metadata=_metadata())
    table.id_map = {201: "Alice", 202: "Bob"}
    table.seen_book_ids = {1, 2}
    table.seen_item_ids = {201, 202}
    table.seen_link_types = {"authors", "editors"}
    table.book_col_map = {1: 201, 2: None}
    table.book_type_map = {1: "authors", 2: None}
    table.col_book_map = {
        "authors": defaultdict(list if priority else set, {201: [1, 7] if priority else {1, 7}, 202: [] if priority else set()}),
        "editors": defaultdict(list if priority else set, {201: [] if priority else set(), 202: [] if priority else set()}),
    }
    return CalibreManyToOneField("creators", table)


def _seed_many_to_many_default(*, priority: bool = False) -> CalibreManyToManyField:
    table_cls = CalibrePriorityManyToManyTable if priority else CalibreManyToManyTable
    table = table_cls("tags", metadata=_metadata())
    table.id_map = {301: "fiction", 302: "classic"}
    table.seen_book_ids = {1, 2}
    table.seen_item_ids = {301, 302}
    table.book_col_map[1] = [301, 302] if priority else {301, 302}
    table.col_book_map = {
        301: [1, 7] if priority else {1, 7},
        302: [1] if priority else {1},
    }
    return CalibreManyToManyField("tags", table)


def _seed_many_to_many_typed(*, priority: bool = False) -> CalibreManyToManyField:
    table_cls = CalibrePriorityTypedManyToManyTable if priority else CalibreTypedManyToManyTable
    table = table_cls("creators", metadata=_metadata())
    table.id_map = {301: "Alice", 302: "Bob", 303: "Carol"}
    table.seen_book_ids = {1, 2}
    table.seen_item_ids = {301, 302, 303}
    table.known_link_types = {"authors", "editors"}
    table.book_col_map = {
        "authors": defaultdict(list if priority else set, {1: [301, 302] if priority else {301, 302}, 2: [] if priority else set()}),
        "editors": defaultdict(list if priority else set, {1: [303] if priority else {303}, 2: [] if priority else set()}),
    }
    table.col_book_map = {
        "authors": defaultdict(list if priority else set, {301: [1, 7] if priority else {1, 7}, 302: [1] if priority else {1}}),
        "editors": defaultdict(list if priority else set, {303: [1] if priority else {1}}),
    }
    return CalibreManyToManyField("creators", table)


@pytest.mark.parametrize(
    ("builder", "expected_for_book", "expected_ids", "expected_books_for"),
    [
        (
            lambda: _seed_one_to_many_default(priority=False, val_unique=False),
            {"alpha", "beta"},
            {101, 102},
            {1, 7},
        ),
        (
            lambda: _seed_one_to_many_default(priority=True, val_unique=False),
            ["alpha", "beta"],
            [101, 102],
            [1, 7],
        ),
        (
            lambda: _seed_one_to_many_typed(priority=False, val_unique=False),
            {"primary": {"alpha"}, "secondary": {"beta", "gamma"}},
            {"primary": {101}, "secondary": {102, 103}},
            {"primary": {1, 7}},
        ),
        (
            lambda: _seed_one_to_many_typed(priority=True, val_unique=False),
            {"primary": ["alpha"], "secondary": ["beta", "gamma"]},
            {"primary": [101], "secondary": [102, 103]},
            {"primary": [1, 7]},
        ),
    ],
)
def test_one_to_many_field_non_unique_variants_expose_expected_relation_shapes(
    builder,
    expected_for_book,
    expected_ids,
    expected_books_for,
) -> None:
    field = builder()

    assert isinstance(field, CalibreOneToManyField)
    assert field.for_book(1) == expected_for_book
    assert field.ids_for_book(1) == expected_ids
    assert field.books_for(101) == expected_books_for
    assert field.for_book(2, default_value="missing") == "missing"
    assert field.ids_for_book(2, default_value="missing") == "missing"
    with pytest.raises(NotInCache):
        field.for_book(999)
    with pytest.raises(NotInCache):
        field.books_for(999)


@pytest.mark.parametrize(
    "builder",
    [
        lambda: _seed_one_to_many_default(priority=False, val_unique=True),
        lambda: _seed_one_to_many_default(priority=True, val_unique=True),
        lambda: _seed_one_to_many_typed(priority=False, val_unique=True),
        lambda: _seed_one_to_many_typed(priority=True, val_unique=True),
    ],
)
def test_one_to_many_field_unique_variants_resolve_items_back_to_single_books(builder) -> None:
    field = builder()

    assert field.books_for(101) == 1
    assert field.books_for(104, default_value="missing") == "missing"
    with pytest.raises(NotInCache):
        field.books_for(999)


@pytest.mark.parametrize(
    ("builder", "expected_books_for"),
    [
        (_seed_many_to_one_default, {1, 7}),
        (lambda: _seed_many_to_one_typed(priority=False), {"authors": {1, 7}, "editors": set()}),
        (lambda: _seed_many_to_one_typed(priority=True), {"authors": [1, 7], "editors": []}),
    ],
)
def test_many_to_one_field_variants_expose_expected_reverse_relation_shapes(
    builder,
    expected_books_for,
) -> None:
    field = builder()

    assert isinstance(field, CalibreManyToOneField)
    assert field.for_book(1) == "Alice" if field.name == "creators" else "Series A"
    assert field.ids_for_book(1) == 201
    assert field.books_for(201) == expected_books_for
    assert field.for_book(2, default_value="missing") == "missing"
    assert field.ids_for_book(2, default_value="missing") == "missing"
    with pytest.raises(NotInCache):
        field.for_book(999)
    with pytest.raises(NotInCache):
        field.books_for(999)


@pytest.mark.parametrize(
    ("builder", "expected_for_book", "expected_ids", "expected_books_for"),
    [
        (
            lambda: _seed_many_to_many_default(priority=False),
            {"fiction", "classic"},
            {301, 302},
            {1, 7},
        ),
        (
            lambda: _seed_many_to_many_default(priority=True),
            ["fiction", "classic"],
            [301, 302],
            [1, 7],
        ),
        (
            lambda: _seed_many_to_many_typed(priority=False),
            {"authors": {"Alice", "Bob"}, "editors": {"Carol"}},
            {"authors": {301, 302}, "editors": {303}},
            {"authors": {1, 7}, "editors": set()},
        ),
        (
            lambda: _seed_many_to_many_typed(priority=True),
            {"authors": ["Alice", "Bob"], "editors": ["Carol"]},
            {"authors": [301, 302], "editors": [303]},
            {"authors": [1, 7], "editors": []},
        ),
    ],
)
def test_many_to_many_field_variants_expose_expected_relation_shapes(
    builder,
    expected_for_book,
    expected_ids,
    expected_books_for,
) -> None:
    field = builder()

    assert isinstance(field, CalibreManyToManyField)
    assert field.for_book(1) == expected_for_book
    assert field.ids_for_book(1) == expected_ids
    assert field.books_for(301) == expected_books_for
    assert field.for_book(2, default_value="missing") == "missing"
    assert field.ids_for_book(2, default_value="missing") == "missing"
    with pytest.raises(NotInCache):
        field.for_book(999)
    with pytest.raises(NotInCache):
        field.books_for(999)
