"""
Check relation-field shapes, default values, and unknown-ID errors using manually seeded cache tables.

These in-memory semantic tests do not use the legacy database-cache opt-in gate.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/caches/test_calibre_cache_07_relation_field_semantics.py
"""
from __future__ import annotations

from collections import defaultdict

import pytest

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
    """
    Build a fresh minimal metadata mapping for a noncustom dummy value table.

    Example:
        >>> _metadata()['datatype']
        'text'


    :param datatype: Datatype label, defaulting to text.
    :param val_unique: Whether values map back to at most one book.
    :return: New dictionary including empty display and multiplicity dictionaries.
    """
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
    """
    Seed an untyped notes relation with two linked values, one unlinked item, and an empty second book.

    Example:
        >>> sorted(_seed_one_to_many_default().ids_for_book(1))
        [101, 102]


    :param priority: Select the priority-aware table class and ordered list maps instead
        of unordered sets.
    :param val_unique: Whether reverse item maps contain a single book ID or collections
        of book IDs.
    :return: New CalibreOneToManyField with the requested ordering and reverse
        uniqueness.
    """
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
    """
    Seed notes grouped into primary and secondary link types, with optional ordering and unique reverse owners.

    Example:
        >>> sorted(_seed_one_to_many_typed().ids_for_book(1))
        ['primary', 'secondary']


    :param priority: Select the priority-aware table class and ordered list maps instead
        of unordered sets.
    :param val_unique: Whether reverse item maps contain a single book ID or collections
        of book IDs.
    :return: New CalibreOneToManyField backed by a typed table.
    """
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
    """
    Seed two series values with book one linked to Series A and book two unlinked.

    Example:
        >>> _seed_many_to_one_default().ids_for_book(1)
        201


    :return: New untyped CalibreManyToOneField; reverse maps also include book seven.
    """
    table = CalibreManyToOneTable("series", metadata=_metadata(datatype="series"))
    table.id_map = {201: "Series A", 202: "Series B"}
    table.seen_book_ids = {1, 2}
    table.seen_item_ids = {201, 202}
    table.book_col_map = {1: 201, 2: None}
    table.col_book_map = {201: {1, 7}, 202: set()}
    return CalibreManyToOneField("series", table)


def _seed_many_to_one_typed(*, priority: bool = False) -> CalibreManyToOneField:
    """
    Seed creator values, author/editor types, and an unlinked second book.

    Example:
        >>> _seed_many_to_one_typed().ids_for_book(1)
        201


    :param priority: Select the priority-aware table class and ordered list maps instead
        of unordered sets.
    :return: New typed CalibreManyToOneField with optional ordered reverse maps.
    """
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
    """
    Seed fiction/classic tags linked to book one and reverse maps including book seven.

    Example:
        >>> sorted(_seed_many_to_many_default().ids_for_book(1))
        [301, 302]


    :param priority: Select the priority-aware table class and ordered list maps instead
        of unordered sets.
    :return: New CalibreManyToManyField with list or set relation maps.
    """
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
    """
    Seed author/editor creator links with a known but unlinked second book.

    Example:
        >>> sorted(_seed_many_to_many_typed().ids_for_book(1))
        ['authors', 'editors']


    :param priority: Select the priority-aware table class and ordered list maps instead
        of unordered sets.
    :return: New typed CalibreManyToManyField with optional ordered list maps.
    """
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
    """
    Check one-to-many forward/reverse shapes, empty-book defaults, and unknown-ID errors across typed and ordered variants.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_07_relation_field_semantics.py::test_one_to_many_field_non_unique_variants_expose_expected_relation_shapes


    :param builder: Zero-argument callable that returns a freshly seeded relation field.
    :param expected_for_book: Expected value collection or typed mapping for book one.
    :param expected_ids: Expected related item IDs for book one.
    :param expected_books_for: Expected reverse book collection or typed mapping for the
        selected item.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Check unique one-to-many values resolve to one owner, unlinked values use the default, and unknown items raise.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_07_relation_field_semantics.py::test_one_to_many_field_unique_variants_resolve_items_back_to_single_books


    :param builder: Zero-argument callable that returns a freshly seeded relation field.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Check many-to-one ID/reverse shapes, unlinked defaults, and unknown-ID errors.

    The conditional forward-value assertion checks Alice only for the creators field;
    its series branch evaluates a truthy literal rather than comparing the returned
    value.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_07_relation_field_semantics.py::test_many_to_one_field_variants_expose_expected_reverse_relation_shapes


    :param builder: Zero-argument callable that returns a freshly seeded relation field.
    :param expected_books_for: Expected reverse book collection or typed mapping for the
        selected item.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Check many-to-many values, IDs, reverse shapes, empty-book defaults, and unknown-ID errors across typed and ordered variants.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_07_relation_field_semantics.py::test_many_to_many_field_variants_expose_expected_relation_shapes


    :param builder: Zero-argument callable that returns a freshly seeded relation field.
    :param expected_for_book: Expected value collection or typed mapping for book one.
    :param expected_ids: Expected related item IDs for book one.
    :param expected_books_for: Expected reverse book collection or typed mapping for the
        selected item.
    :return: None; failed expectations raise AssertionError.
    """
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
