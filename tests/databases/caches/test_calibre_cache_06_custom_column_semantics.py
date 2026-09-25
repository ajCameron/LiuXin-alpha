"""
Check custom-field category visibility and scalar update prechecks without initializing a legacy database cache.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/caches/test_calibre_cache_06_custom_column_semantics.py
"""
from __future__ import annotations

import pytest

from LiuXin_alpha.library.caches.calibre.tables.one_one_tables import (
    CalibreCustomColumnsOneToOneTable,
)
from LiuXin_alpha.catalog.field_metadata import FieldMetadata
from LiuXin_alpha.errors import InvalidCacheUpdate
from LiuXin_alpha.surfaces.categories import find_categories

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
    """
    Register an editable custom value field on the supplied FieldMetadata object.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_06_custom_column_semantics.py


    :param fm: FieldMetadata to update.
    :param label: Internal label used for the hash-prefixed custom key.
    :param datatype: Datatype supplied to field registration.
    :param colnum: Custom-column number used in the generated table name.
    :param is_category: Category flag passed to registration.
    :param display: Display mapping, or a new empty dictionary for false values.
    :param in_table: Owning relation, defaulting to books.
    :param is_multiple: Multi-value separator mapping, or a new empty dictionary for
        false values.
    :return: None; mutates fm.
    """
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
    """
    Check books custom categories and enabled composites are exposed while titles-owned and disabled composite fields are excluded.

    Example:
        >>> test_find_categories_custom_field_visibility_rules()


    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Check a custom integer table accepts a scalar and None for known books.

    Example:
        >>> test_custom_one_to_one_update_precheck_accepts_scalars_and_none()


    :return: None; failed expectations raise AssertionError.
    """
    table = CalibreCustomColumnsOneToOneTable(
        "custom_column_1",
        metadata={"datatype": "int", "display": {}, "is_multiple": {}},
        custom=True,
    )
    table.seen_book_ids = {1, 2}

    table.update_precheck({1: 42, 2: None}, id_map_update={})


@pytest.mark.parametrize("bad_value", ([1, 2], {1, 2}, {"v": 1}, ("ordered",)))
def test_custom_one_to_one_update_precheck_rejects_container_values(bad_value) -> None:
    """
    Check list, set, mapping, and tuple inputs are rejected for a scalar custom integer column.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_06_custom_column_semantics.py::test_custom_one_to_one_update_precheck_rejects_container_values


    :param bad_value: Container value selected by pytest for this rejection case.
    :return: None; failed expectations raise AssertionError.
    """
    table = CalibreCustomColumnsOneToOneTable(
        "custom_column_1",
        metadata={"datatype": "int", "display": {}, "is_multiple": {}},
        custom=True,
    )
    table.seen_book_ids = {1}

    with pytest.raises(InvalidCacheUpdate):
        table.update_precheck({1: bad_value}, id_map_update={})


def test_custom_one_to_one_update_precheck_rejects_unknown_books_and_bad_scalars() -> None:
    """
    Check unknown book IDs and values rejected by an acceptance callback raise InvalidCacheUpdate.

    Example:
        >>> test_custom_one_to_one_update_precheck_rejects_unknown_books_and_bad_scalars()


    :return: None; failed expectations raise AssertionError.
    """
    table = CalibreCustomColumnsOneToOneTable(
        "custom_column_1",
        metadata={"datatype": "int", "display": {}, "is_multiple": {}},
        custom=True,
    )
    table.seen_book_ids = {1}

    with pytest.raises(InvalidCacheUpdate):
        table.update_precheck({9: 42}, id_map_update={})

    def _must_be_positive(value):
        """
        Reject negative values for the scalar-acceptance probe, while allowing zero.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/caches/test_calibre_cache_06_custom_column_semantics.py::test_custom_one_to_one_update_precheck_rejects_unknown_books_and_bad_scalars


        :param value: Scalar compared with zero.
        :return: None for nonnegative input; raises ValueError for negative input.
        """
        if value < 0:
            raise ValueError("value must be positive")

    with pytest.raises(InvalidCacheUpdate):
        table.update_precheck({1: -1}, id_map_update={}, acceptance_functions=[_must_be_positive])
