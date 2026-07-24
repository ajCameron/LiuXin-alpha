"""Behavioral coverage for catalog numeric and date field searches."""

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
    return lambda: iter(values)


def test_boolean_search_rejects_unknown_query_values() -> None:
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
    assert getattr(DateSearch(), method)(dbdate, query, field_count) is expected


def test_date_search_presence_queries_parse_string_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parsed = datetime(2024, 5, 6, tzinfo=timezone.utc)

    def fake_parse_date(value: str, **_kwargs: Any) -> datetime:
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
    def fake_parse_date(value: str, **_kwargs: Any) -> datetime:
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
    with pytest.raises(ParseException, match="Number conversion error"):
        DateSearch()("manydaysago", _field_iter([]))


def test_date_search_reports_date_conversion_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def invalid_date(_value: str, **_kwargs: Any) -> datetime:
        raise ValueError("invalid date")

    monkeypatch.setattr(date_search, "parse_date", invalid_date)

    with pytest.raises(ParseException, match="Date conversion error"):
        DateSearch()("not-a-date", _field_iter([]))
