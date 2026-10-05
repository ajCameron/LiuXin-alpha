"""
Verify test field search operators behavior against the public catalog contracts.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test field search operators through its owning regression module::

        python -m pytest -q tests/catalog/test_field_search_operators.py
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from LiuXin_alpha.catalog.search.field_searches import date_search
from LiuXin_alpha.catalog.search.field_searches.boolean_search import BooleanSearch
from LiuXin_alpha.catalog.search.field_searches.date_search import DateSearch
from LiuXin_alpha.catalog.search.field_searches.numeric_search import NumericSearch
from LiuXin_alpha.utils.date import UNDEFINED_DATE
from LiuXin_alpha.utils.search_query_parser import ParseException


def _field_iter(
    values: list[tuple[Any, set[int]]],
):
    """
    Perform the field iter test-helper operation with deterministic inputs.

    Example:
        Exercise field iter through its owning regression module::

            python -m pytest -q tests/catalog/test_field_search_operators.py


    :param values: Values to normalize, compare or write in stable order.
    :return: The deterministic value, row, identity or collection described above.
    """
    return lambda: iter(values)


def test_boolean_search_rejects_unknown_query_values() -> None:
    """
    Verify boolean search rejects unknown query values.

    Example:
        Exercise test boolean search rejects unknown query values through its owning regression module::

            python -m pytest -q tests/catalog/test_field_search_operators.py


    :return: None; the function records state or raises through its assertions.
    """
    with pytest.raises(ParseException, match="Invalid boolean query"):
        BooleanSearch()("perhaps", _field_iter([]), bools_are_tristate=False)


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("false", {1, 2, 4, 6}),
        ("no", {1, 2, 4, 6}),
        ("unchecked", {1, 2, 4, 6}),
        ("true", {3, 5}),
        ("yes", {3, 5}),
        ("checked", {3, 5}),
        ("empty", set()),
    ],
)
def test_boolean_search_two_state_semantics(
    query: str,
    expected: set[int],
) -> None:
    """
    Verify boolean search two state semantics.

    Example:
        Exercise test boolean search two state semantics through its owning regression module::

            python -m pytest -q tests/catalog/test_field_search_operators.py


    :param query: Parsed or textual catalog query to evaluate.
    :param expected: Value supplied for expected under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    values = [
        (None, {1}),
        (False, {2}),
        (True, {3}),
        ("no", {4}),
        ("yes", {5}),
        ("not-a-bool", {6}),
    ]

    assert (
        BooleanSearch()(query, _field_iter(values), bools_are_tristate=False)
        == expected
    )


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("empty", {1, 6}),
        ("blank", {1, 6}),
        ("false", {1, 6}),
        ("no", {2, 4}),
        ("unchecked", {2, 4}),
        ("yes", {3, 5}),
        ("checked", {3, 5}),
        ("true", {2, 3, 4, 5}),
    ],
)
def test_boolean_search_tristate_semantics(
    query: str,
    expected: set[int],
) -> None:
    """
    Verify boolean search tristate semantics.

    Example:
        Exercise test boolean search tristate semantics through its owning regression module::

            python -m pytest -q tests/catalog/test_field_search_operators.py


    :param query: Parsed or textual catalog query to evaluate.
    :param expected: Value supplied for expected under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    values = [
        (None, {1}),
        (False, {2}),
        (True, {3}),
        ("no", {4}),
        ("yes", {5}),
        ("not-a-bool", {6}),
    ]

    assert (
        BooleanSearch()(query, _field_iter(values), bools_are_tristate=True)
        == expected
    )


@pytest.mark.parametrize(
    ("query", "location", "expected"),
    [
        ("", "value", set()),
        ("false", "value", {1}),
        ("true", "value", {2, 3, 4}),
        ("false", "cover", {1, 2}),
        ("true", "cover", {3, 4}),
    ],
)
def test_numeric_search_presence_queries(
    query: str,
    location: str,
    expected: set[int],
) -> None:
    """
    Verify numeric search presence queries.

    Example:
        Exercise test numeric search presence queries through its owning regression module::

            python -m pytest -q tests/catalog/test_field_search_operators.py


    :param query: Parsed or textual catalog query to evaluate.
    :param location: Value supplied for location under the catalog contract.
    :param expected: Value supplied for expected under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    values = [(None, {1}), (0, {2}), (3, {3}), ("present", {4})]

    assert (
        NumericSearch()(
            query,
            _field_iter(values),
            location,
            "int",
            {1, 2, 3, 4},
        )
        == expected
    )


def test_numeric_search_many_value_presence_and_rating_semantics() -> None:
    """
    Verify numeric search many value presence and rating semantics.

    Example:
        Exercise test numeric search many value presence and rating semantics through its owning regression module::

            python -m pytest -q tests/catalog/test_field_search_operators.py


    :return: None; the function records state or raises through its assertions.
    """
    values = [(0, {1}), (2, {2}), (5, {3})]
    search = NumericSearch()

    assert search(
        "true",
        _field_iter(values),
        "value",
        "int",
        {1, 2, 3, 4},
        is_many=True,
    ) == {1, 2, 3}
    assert search(
        "false",
        _field_iter(values),
        "value",
        "int",
        {1, 2, 3, 4},
        is_many=True,
    ) == {4}

    rating_values = [(None, {1}), (0, {2}), (-1, {3}), (2, {4})]
    assert search(
        "true",
        _field_iter(rating_values),
        "rating",
        "rating",
        {1, 2, 3, 4, 5},
        is_many=True,
    ) == {4}
    assert search(
        "false",
        _field_iter(rating_values),
        "rating",
        "rating",
        {1, 2, 3, 4, 5},
        is_many=True,
    ) == {1, 2, 3, 5}


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("2", {2}),
        ("=2", {2}),
        ("!=2", {0, 1, 3, 4, 5}),
        (">2", {3, 4}),
        (">=2", {2, 3, 4}),
        ("<2", {0, 1}),
        ("<=2", {0, 1, 2}),
    ],
)
def test_numeric_search_relational_operators(
    query: str,
    expected: set[int],
) -> None:
    """
    Verify numeric search relational operators.

    Example:
        Exercise test numeric search relational operators through its owning regression module::

            python -m pytest -q tests/catalog/test_field_search_operators.py


    :param query: Parsed or textual catalog query to evaluate.
    :param expected: Value supplied for expected under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    values = [
        (0, {0}),
        (1, {1}),
        (2, {2}),
        (3.5, {3}),
        ("4", {4}),
        ("not-a-number", {5}),
        (None, {6}),
    ]

    assert (
        NumericSearch()(
            query,
            _field_iter(values),
            "value",
            "int",
            set(range(7)),
        )
        == expected
    )


def test_numeric_search_casts_float_rating_and_binary_suffix_values() -> None:
    """
    Verify numeric search casts float rating and binary suffix values.

    Example:
        Exercise test numeric search casts float rating and binary suffix values through its owning regression module::

            python -m pytest -q tests/catalog/test_field_search_operators.py


    :return: None; the function records state or raises through its assertions.
    """
    search = NumericSearch()

    assert search(
        ">1.25",
        _field_iter([(1.25, {1}), ("1.5", {2}), (2, {3})]),
        "value",
        "composite",
        {1, 2, 3},
    ) == {2, 3}
    assert search(
        ">=4",
        _field_iter([(6, {1}), (8, {2}), (10, {3})]),
        "rating",
        "rating",
        {1, 2, 3},
    ) == {2, 3}
    assert search(
        ">=2k",
        _field_iter([(2047, {1}), (2048, {2}), (4096, {3})]),
        "size",
        "int",
        {1, 2, 3},
    ) == {2, 3}


@pytest.mark.parametrize("query", ["not-a-number", ">=oops"])
def test_numeric_search_rejects_non_numeric_queries(query: str) -> None:
    """
    Verify numeric search rejects non numeric queries.

    Example:
        Exercise test numeric search rejects non numeric queries through its owning regression module::

            python -m pytest -q tests/catalog/test_field_search_operators.py


    :param query: Parsed or textual catalog query to evaluate.
    :return: None; the function records state or raises through its assertions.
    """
    with pytest.raises(ParseException, match="Non-numeric value"):
        NumericSearch()(
            query,
            _field_iter([]),
            "value",
            "int",
            set(),
        )


@pytest.mark.parametrize(
    ("method", "dbdate", "query", "field_count", "expected"),
    [
        ("eq", datetime(2024, 5, 6), datetime(2024, 1, 1), 1, True),
        ("eq", datetime(2024, 5, 6), datetime(2024, 5, 1), 2, True),
        ("eq", datetime(2024, 5, 6), datetime(2024, 5, 6), 3, True),
        ("eq", datetime(2024, 5, 6), datetime(2024, 5, 7), 3, False),
        ("eq", datetime(2024, 5, 6), datetime(2024, 6, 1), 2, False),
        ("eq", datetime(2024, 5, 6), datetime(2025, 1, 1), 1, False),
        ("ne", datetime(2024, 5, 6), datetime(2024, 5, 7), 3, True),
        ("gt", datetime(2025, 1, 1), datetime(2024, 12, 31), 1, True),
        ("gt", datetime(2024, 6, 1), datetime(2024, 5, 31), 2, True),
        ("gt", datetime(2024, 5, 7), datetime(2024, 5, 6), 3, True),
        ("gt", datetime(2024, 5, 7), datetime(2024, 5, 6), 2, False),
        ("gt", datetime(2023, 12, 31), datetime(2024, 1, 1), 1, False),
        ("le", datetime(2024, 5, 6), datetime(2024, 5, 6), 3, True),
        ("lt", datetime(2023, 12, 31), datetime(2024, 1, 1), 1, True),
        ("lt", datetime(2024, 4, 30), datetime(2024, 5, 1), 2, True),
        ("lt", datetime(2024, 5, 5), datetime(2024, 5, 6), 3, True),
        ("lt", datetime(2024, 5, 5), datetime(2024, 5, 6), 2, False),
        ("lt", datetime(2025, 1, 1), datetime(2024, 12, 31), 1, False),
        ("ge", datetime(2024, 5, 6), datetime(2024, 5, 6), 3, True),
    ],
)
def test_date_search_comparison_precision(
    method: str,
    dbdate: datetime,
    query: datetime,
    field_count: int,
    expected: bool,
) -> None:
    """
    Verify date search comparison precision.

    Example:
        Exercise test date search comparison precision through its owning regression module::

            python -m pytest -q tests/catalog/test_field_search_operators.py


    :param method: Value supplied for method under the catalog contract.
    :param dbdate: Value supplied for dbdate under the catalog contract.
    :param query: Parsed or textual catalog query to evaluate.
    :param field_count: Value supplied for field count under the catalog contract.
    :param expected: Value supplied for expected under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    assert getattr(DateSearch(), method)(dbdate, query, field_count) is expected


def test_date_search_presence_queries_parse_string_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Verify date search presence queries parse string values.

    Example:
        Exercise test date search presence queries parse string values through its owning regression module::

            python -m pytest -q tests/catalog/test_field_search_operators.py


    :param monkeypatch: Value supplied for monkeypatch under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    parsed = datetime(2024, 5, 6, tzinfo=timezone.utc)

    def fake_parse_date(value: str, **_kwargs: Any) -> datetime:
        """
        Perform the fake parse date test-helper operation with deterministic inputs.

        Example:
            Exercise test date search presence queries parse string values.fake parse date through its owning regression module::

                python -m pytest -q tests/catalog/test_field_search_operators.py


        :param value: Public or stored value to normalize, compare or write.
        :param _kwargs: Value supplied for kwargs under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """
        assert value == "published"
        return parsed

    monkeypatch.setattr(date_search, "parse_date", fake_parse_date)
    values = [
        (None, {1}),
        (UNDEFINED_DATE, {2}),
        ("published", {3}),
        (datetime(2024, 5, 7, tzinfo=timezone.utc), {4}),
    ]
    search = DateSearch()

    assert search("false", _field_iter(values)) == {1, 2}
    assert search("true", _field_iter(values)) == {3, 4}


def test_date_search_short_queries_are_empty() -> None:
    """
    Verify date search short queries remain empty.

    Example:
        Exercise test date search short queries are empty through its owning regression module::

            python -m pytest -q tests/catalog/test_field_search_operators.py


    :return: None; the function records state or raises through its assertions.
    """
    assert DateSearch()("", _field_iter([])) == set()
    assert DateSearch()("1", _field_iter([])) == set()


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("=2024", {1, 2, 3}),
        ("!=2024-05", {3}),
        (">2024-05-06", {3}),
        (">=2024-05-06", {2, 3}),
        ("<2024-05-06", {1}),
        ("<=2024-05-06", {1, 2}),
    ],
)
def test_date_search_relational_operators(
    query: str,
    expected: set[int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Verify date search relational operators.

    Example:
        Exercise test date search relational operators through its owning regression module::

            python -m pytest -q tests/catalog/test_field_search_operators.py


    :param query: Parsed or textual catalog query to evaluate.
    :param expected: Value supplied for expected under the catalog contract.
    :param monkeypatch: Value supplied for monkeypatch under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    def fake_parse_date(value: str, **_kwargs: Any) -> datetime:
        """
        Perform the fake parse date test-helper operation with deterministic inputs.

        Example:
            Exercise test date search relational operators.fake parse date through its owning regression module::

                python -m pytest -q tests/catalog/test_field_search_operators.py


        :param value: Public or stored value to normalize, compare or write.
        :param _kwargs: Value supplied for kwargs under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """
        parts = [int(part) for part in value.split("-")]
        return datetime(
            parts[0],
            parts[1] if len(parts) > 1 else 1,
            parts[2] if len(parts) > 2 else 1,
        )

    monkeypatch.setattr(date_search, "parse_date", fake_parse_date)
    monkeypatch.setattr(date_search, "dt_as_local", lambda value: value)
    values = [
        (datetime(2024, 5, 5), {1}),
        ("2024-05-06", {2}),
        (datetime(2024, 6, 1), {3}),
        (None, {4}),
    ]

    assert DateSearch()(query, _field_iter(values)) == expected


def test_date_search_relative_date_aliases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Verify date search relative date aliases.

    Example:
        Exercise test date search relative date aliases through its owning regression module::

            python -m pytest -q tests/catalog/test_field_search_operators.py


    :param monkeypatch: Value supplied for monkeypatch under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    fixed_now = datetime(2024, 5, 6, 12, 0)
    monkeypatch.setattr(date_search, "now", lambda: fixed_now)
    monkeypatch.setattr(date_search, "dt_as_local", lambda value: value)
    values = [
        (datetime(2024, 5, 6), {1}),
        (datetime(2024, 5, 5), {2}),
        (datetime(2024, 5, 4), {3}),
        (datetime(2024, 4, 30), {4}),
    ]
    search = DateSearch()

    assert search("today", _field_iter(values)) == {1}
    assert search("_yesterday", _field_iter(values)) == {2}
    assert search("thismonth", _field_iter(values)) == {1, 2, 3}
    assert search("2daysago", _field_iter(values)) == {3}


def test_date_search_reports_relative_day_conversion_errors() -> None:
    """
    Verify date search reports relative day conversion errors.

    Example:
        Exercise test date search reports relative day conversion errors through its owning regression module::

            python -m pytest -q tests/catalog/test_field_search_operators.py


    :return: None; the function records state or raises through its assertions.
    """
    with pytest.raises(ParseException, match="Number conversion error"):
        DateSearch()("manydaysago", _field_iter([]))


def test_date_search_reports_date_conversion_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Verify date search reports date conversion errors.

    Example:
        Exercise test date search reports date conversion errors through its owning regression module::

            python -m pytest -q tests/catalog/test_field_search_operators.py


    :param monkeypatch: Value supplied for monkeypatch under the catalog contract.
    :return: None; the function records state or raises through its assertions.
    """
    def invalid_date(_value: str, **_kwargs: Any) -> datetime:
        """
        Perform the invalid date test-helper operation with deterministic inputs.

        Example:
            Exercise test date search reports date conversion errors.invalid date through its owning regression module::

                python -m pytest -q tests/catalog/test_field_search_operators.py


        :param _value: Value supplied for value under the catalog contract.
        :param _kwargs: Value supplied for kwargs under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise ValueError("invalid date")

    monkeypatch.setattr(date_search, "parse_date", invalid_date)

    with pytest.raises(ParseException, match="Date conversion error"):
        DateSearch()("not-a-date", _field_iter([]))
