"""
Check boolean coercion, link-table names, and series helpers.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_utils.py
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.databases.utils import (
    _get_next_series_num_for_list,
    _get_series_values,
    force_to_bool,
    get_link_table_name,
)


# ---------------------------------------------------------------------------
# force_to_bool
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (False, False),
        (True, True),
        (0, False),
        (1, True),
        (2, True),
        ("", None),
        ("0", False),
        ("1", True),
        ("false", False),
        ("no", False),
        ("unchecked", False),
        ("true", True),
        ("yes", True),
        ("checked", True),
        ("not-a-bool", None),
    ],
)
def test_force_to_bool_handles_typed_and_text_values(
    value: object,
    expected: object,
) -> None:
    """
    Check boolean coercion against the parametrized typed, textual, and unknown values.

    Example:
        >>> test_force_to_bool_handles_typed_and_text_values('checked', True)
        >>> test_force_to_bool_handles_typed_and_text_values('not-a-bool', None)


    :param value: Input value for this boolean-coercion case.
    :param expected: Expected result, compared by equality: True, False, or None.
    :return: None; failed expectations raise AssertionError.
    """
    assert force_to_bool(value) == expected


# ---------------------------------------------------------------------------
# get_link_table_name
# ---------------------------------------------------------------------------


class TestGetLinkTableName:
    """
    Check link/intralink suffixes, component names, and case normalization.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_utils.py
    """

    def test_two_different_tables_sorted_alphabetically(self) -> None:
        """
        Check that a title/agent link name contains both components and ends in _links.

        Example:
            >>> TestGetLinkTableName().test_two_different_tables_sorted_alphabetically()


        :return: None; failed expectations raise AssertionError.
        """
        result = get_link_table_name("titles", "agents")
        # singular(agent)=agent, singular(title)=title -> sorted: [agent, title]
        assert "agent" in result
        assert "title" in result
        assert result.endswith("_links")

    def test_same_table_returns_intralinks(self) -> None:
        """
        Check that a title self-link name contains title and ends in _intralinks.

        Example:
            >>> TestGetLinkTableName().test_same_table_returns_intralinks()


        :return: None; failed expectations raise AssertionError.
        """
        result = get_link_table_name("titles", "titles")
        assert result.endswith("_intralinks")
        assert "title" in result

    def test_table_name_normalised_to_lowercase(self) -> None:
        """
        Check that mixed-case and lowercase table inputs yield the same link name.

        Example:
            >>> TestGetLinkTableName().test_table_name_normalised_to_lowercase()


        :return: None; failed expectations raise AssertionError.
        """
        result_lower = get_link_table_name("titles", "agents")
        result_mixed = get_link_table_name("Titles", "Agents")
        assert result_lower == result_mixed

    def test_books_agents_link_table(self) -> None:
        """
        Check that a book/agent link name contains both components and the link suffix.

        Example:
            >>> TestGetLinkTableName().test_books_agents_link_table()


        :return: None; failed expectations raise AssertionError.
        """
        result = get_link_table_name("books", "agents")
        assert "agent" in result
        assert "book" in result
        assert "_links" in result

    def test_books_books_intralink_table(self) -> None:
        """
        Check that a book self-link name contains book and the intralink suffix.

        Example:
            >>> TestGetLinkTableName().test_books_books_intralink_table()


        :return: None; failed expectations raise AssertionError.
        """
        result = get_link_table_name("books", "books")
        assert "book" in result
        assert "_intralinks" in result


# ---------------------------------------------------------------------------
# _get_series_values
# ---------------------------------------------------------------------------


class TestGetSeriesValues:
    """
    Check series names and optional numeric indices, including empty and zero values.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_utils.py
    """

    def test_empty_string(self) -> None:
        """
        Check that an empty input returns an empty name and no index.

        Example:
            >>> TestGetSeriesValues().test_empty_string()


        :return: None; failed expectations raise AssertionError.
        """
        assert _get_series_values("") == ("", None)

    def test_none(self) -> None:
        # Function starts with `if not val`, so None/empty -> (None/empty, None)
        """
        Check that None returns no name and no index.

        Example:
            >>> TestGetSeriesValues().test_none()


        :return: None; failed expectations raise AssertionError.
        """
        assert _get_series_values(None) == (None, None)

    def test_series_with_integer_index(self) -> None:
        """
        Check that Dune with index one returns its name and 1.0.

        Example:
            >>> TestGetSeriesValues().test_series_with_integer_index()


        :return: None; failed expectations raise AssertionError.
        """
        series, idx = _get_series_values("Dune [1]")
        assert series == "Dune"
        assert idx == 1.0

    def test_series_with_float_index(self) -> None:
        """
        Check that Foundation retains its fractional index of 2.5.

        Example:
            >>> TestGetSeriesValues().test_series_with_float_index()


        :return: None; failed expectations raise AssertionError.
        """
        series, idx = _get_series_values("Foundation [2.5]")
        assert series == "Foundation"
        assert idx == 2.5

    def test_plain_name_no_index(self) -> None:
        """
        Check that a plain series name has no parsed index.

        Example:
            >>> TestGetSeriesValues().test_plain_name_no_index()


        :return: None; failed expectations raise AssertionError.
        """
        series, idx = _get_series_values("No Index Series")
        assert series == "No Index Series"
        assert idx is None

    def test_strips_whitespace(self) -> None:
        """
        Check that surrounding whitespace is removed from Discworld and its index twenty-one.

        Example:
            >>> TestGetSeriesValues().test_strips_whitespace()


        :return: None; failed expectations raise AssertionError.
        """
        series, idx = _get_series_values("  Discworld [21]  ")
        assert series == "Discworld"
        assert idx == 21.0

    def test_zero_index(self) -> None:
        """
        Check that a zero series index is retained as 0.0.

        Example:
            >>> TestGetSeriesValues().test_zero_index()


        :return: None; failed expectations raise AssertionError.
        """
        series, idx = _get_series_values("Prequels [0]")
        assert series == "Prequels"
        assert idx == 0.0


# ---------------------------------------------------------------------------
# _get_next_series_num_for_list
# ---------------------------------------------------------------------------


class TestGetNextSeriesNumForList:
    """
    Check next-series numbering for empty, nested, flat, and fractional inputs.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_utils.py
    """

    def test_empty_list_returns_one(self) -> None:
        """
        Check that an empty sequence produces series number 1.0.

        Example:
            >>> TestGetNextSeriesNumForList().test_empty_list_returns_one()


        :return: None; failed expectations raise AssertionError.
        """
        result = _get_next_series_num_for_list([])
        assert result == 1.0

    def test_next_after_list(self) -> None:
        # Default mode is "next": floor(last) + 1
        """
        Check that nested indices one through three produce 4.0.

        Example:
            >>> TestGetNextSeriesNumForList().test_next_after_list()


        :return: None; failed expectations raise AssertionError.
        """
        result = _get_next_series_num_for_list([[1.0], [2.0], [3.0]])
        assert result == 4.0

    def test_unwrap_true_unpacks_inner_list(self) -> None:
        # With unwrap=True (default), each element is treated as [index]
        """
        Check that the default nested-value form unwraps five and produces 6.0.

        Example:
            >>> TestGetNextSeriesNumForList().test_unwrap_true_unpacks_inner_list()


        :return: None; failed expectations raise AssertionError.
        """
        result = _get_next_series_num_for_list([[5.0]])
        assert result == 6.0

    def test_unwrap_false_uses_element_directly(self) -> None:
        # With unwrap=False, each element is the index directly
        """
        Check that unwrap=False accepts flat indices and produces 4.0.

        Example:
            >>> TestGetNextSeriesNumForList().test_unwrap_false_uses_element_directly()


        :return: None; failed expectations raise AssertionError.
        """
        result = _get_next_series_num_for_list([1.0, 2.0, 3.0], unwrap=False)
        assert result == 4.0

    def test_fractional_last_element_floored(self) -> None:
        # floor(3.7) + 1 = 4.0
        """
        Check that a final fractional index of 3.7 produces 4.0.

        Example:
            >>> TestGetNextSeriesNumForList().test_fractional_last_element_floored()


        :return: None; failed expectations raise AssertionError.
        """
        result = _get_next_series_num_for_list([[3.7]], unwrap=True)
        assert result == 4.0
