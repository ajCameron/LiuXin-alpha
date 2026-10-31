"""Exercise generic value conversion without bibliographic field policies."""

from __future__ import annotations

import datetime

import pytest

from LiuXin_alpha.utils.adaptors import (
    adapt_bool,
    adapt_date,
    adapt_datetime,
    adapt_number,
    multiple_text,
    single_text,
    string_to_datetime,
)


class TestSqliteDatetime:
    """
    Check SQLite datetime formatting and passthrough inputs.

    Example:
        >>> TestSqliteDatetime().test_passes_non_datetime_through()
    """

    def test_passes_non_datetime_through(self) -> None:
        """
        Keep an already formatted datetime string unchanged.

        Example:
            >>> TestSqliteDatetime().test_passes_non_datetime_through()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert string_to_datetime("2024-01-01 00:00:00") == "2024-01-01 00:00:00"

    def test_formats_datetime_object(self) -> None:
        """
        Format a UTC datetime as the exact space-separated timestamp with offset.

        Example:
            >>> TestSqliteDatetime().test_formats_datetime_object()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        dt = datetime.datetime(2024, 6, 15, 12, 30, 0, tzinfo=datetime.timezone.utc)
        result = string_to_datetime(dt)
        assert isinstance(result, str)
        assert result == "2024-06-15 12:30:00+00:00"

    def test_passes_none_through(self) -> None:
        """
        Preserve None when formatting a SQLite datetime value.

        Example:
            >>> TestSqliteDatetime().test_passes_none_through()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert string_to_datetime(None) is None


class TestSingleText:
    """
    Check trimming and missing-value rules for single text fields.

    Example:
        >>> TestSingleText().test_strips_whitespace()
    """

    def test_strips_whitespace(self) -> None:
        """
        Trim spaces around hello while retaining its content.

        Example:
            >>> TestSingleText().test_strips_whitespace()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert single_text("  hello  ") == "hello"

    def test_none_returns_none(self) -> None:
        """
        Preserve None as a missing single text value.

        Example:
            >>> TestSingleText().test_none_returns_none()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert single_text(None) is None

    def test_empty_string_returns_none(self) -> None:
        """
        Convert an empty string to a missing single text value.

        Example:
            >>> TestSingleText().test_empty_string_returns_none()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert single_text("") is None

    def test_whitespace_only_returns_none(self) -> None:
        """
        Convert a space-only string to a missing single text value.

        Example:
            >>> TestSingleText().test_whitespace_only_returns_none()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert single_text("   ") is None

    def test_normal_string_passthrough(self) -> None:
        """
        Keep the internal space in Penguin Books unchanged.

        Example:
            >>> TestSingleText().test_normal_string_passthrough()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert single_text("Penguin Books") == "Penguin Books"

    def test_strips_and_preserves_content(self) -> None:
        """
        Remove tabs, spaces and a newline surrounding Great Expectations.

        Example:
            >>> TestSingleText().test_strips_and_preserves_content()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert single_text("\t  Great Expectations  \n") == "Great Expectations"


class TestMultipleText:
    """
    Check tuple splitting, trimming and omitted empty text values.

    Example:
        >>> TestMultipleText().test_empty_input()
    """

    def test_empty_input(self) -> None:
        """
        Return an empty tuple for an empty multiple-text string.

        Example:
            >>> TestMultipleText().test_empty_input()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert multiple_text(",", ", ", "") == ()

    def test_none_input(self) -> None:
        """
        Return an empty tuple for a missing multiple-text value.

        Example:
            >>> TestMultipleText().test_none_input()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert multiple_text(",", ", ", None) == ()

    def test_splits_on_sep(self) -> None:
        """
        Split three comma-separated values in their original order.

        Example:
            >>> TestMultipleText().test_splits_on_sep()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        result = multiple_text(",", ", ", "a,b,c")
        assert result == ("a", "b", "c")

    def test_strips_items(self) -> None:
        """
        Trim spaces from both comma-separated text items.

        Example:
            >>> TestMultipleText().test_strips_items()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        result = multiple_text(",", ", ", "  alpha  ,  beta  ")
        assert result == ("alpha", "beta")

    def test_empty_items_skipped(self) -> None:
        """
        Omit the empty token between adjacent commas.

        Example:
            >>> TestMultipleText().test_empty_items_skipped()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        result = multiple_text(",", ", ", "a,,b")
        assert result == ("a", "b")

    def test_ui_sep_replacement(self) -> None:
        # When ui_sep is ";", commas in values are replaced with semicolons.
        """
        Check that choosing a semicolon UI separator still yields two items.

        This assertion checks cardinality only; it does not verify the contents of the
        resulting strings.

        Example:
            >>> TestMultipleText().test_ui_sep_replacement()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        result = multiple_text(",", ";", "a,b")
        assert len(result) == 2


class TestAdaptDatetime:
    """
    Check datetime object passthrough, parsing and missing inputs.

    Example:
        >>> TestAdaptDatetime().test_datetime_passthrough()
    """

    def test_datetime_passthrough(self) -> None:
        """
        Preserve the value of an existing naive datetime.

        Example:
            >>> TestAdaptDatetime().test_datetime_passthrough()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        dt = datetime.datetime(2024, 1, 1, 12, 0, 0)
        result = adapt_datetime(dt)
        assert result == dt

    def test_string_input_parsed(self) -> None:
        """
        Parse an ISO-like timestamp into a datetime instance.

        The assertion checks the result type, not its timezone or exact components.

        Example:
            >>> TestAdaptDatetime().test_string_input_parsed()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        result = adapt_datetime("2024-01-15T10:30:00")
        assert isinstance(result, datetime.datetime)

    def test_none_returns_none(self) -> None:
        # None is falsy, so `if x and x_is_date_undefined` is False; None is returned
        # as-is even though is_date_undefined(None) is True.
        """
        Keep None unchanged through the false-input datetime branch.

        Example:
            >>> TestAdaptDatetime().test_none_returns_none()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        result = adapt_datetime(None)
        assert result is None


class TestAdaptDate:
    """
    Check date parsing and the undefined-date sentinel.

    Example:
        >>> TestAdaptDate().test_none_returns_undefined()
    """

    def test_none_returns_undefined(self) -> None:
        """
        Convert None to the shared UNDEFINED_DATE sentinel.

        Example:
            >>> TestAdaptDate().test_none_returns_undefined()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        from LiuXin_alpha.utils.date import UNDEFINED_DATE

        result = adapt_date(None)
        assert result == UNDEFINED_DATE

    def test_string_parsed_as_date(self) -> None:
        """
        Parse a calendar date string into a datetime instance.

        Example:
            >>> TestAdaptDate().test_string_parsed_as_date()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        result = adapt_date("2024-06-01")
        assert isinstance(result, datetime.datetime)


class TestAdaptNumber:
    """
    Check generic integer and float conversion with missing values.

    Example:
        >>> TestAdaptNumber().test_none_returns_none()
    """

    def test_none_returns_none(self) -> None:
        """
        Keep None missing when requesting integer conversion.

        Example:
            >>> TestAdaptNumber().test_none_returns_none()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert adapt_number(int, None) is None

    def test_none_string_returns_none(self) -> None:
        """
        Treat lowercase and uppercase none strings as missing numbers.

        Example:
            >>> TestAdaptNumber().test_none_string_returns_none()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert adapt_number(int, "none") is None
        assert adapt_number(int, "NONE") is None

    def test_int_coercion(self) -> None:
        """
        Convert the string 42 to an actual int with value 42.

        Example:
            >>> TestAdaptNumber().test_int_coercion()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert adapt_number(int, "42") == 42
        assert isinstance(adapt_number(int, "42"), int)

    def test_float_coercion(self) -> None:
        """
        Convert the string 3.14 to a float within absolute tolerance 1e-9.

        Example:
            >>> TestAdaptNumber().test_float_coercion()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        result = adapt_number(float, "3.14")
        assert abs(result - 3.14) < 1e-9

    def test_already_numeric(self) -> None:
        """
        Preserve already numeric integer and fractional values.

        Example:
            >>> TestAdaptNumber().test_already_numeric()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert adapt_number(int, 7) == 7
        assert adapt_number(float, 2.5) == 2.5


class TestAdaptBool:
    """
    Check generic Boolean strings, integers and missing values.

    Example:
        >>> TestAdaptBool().test_true_string()
    """

    def test_true_string(self) -> None:
        """
        Convert the lowercase true string to True.

        Example:
            >>> TestAdaptBool().test_true_string()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert adapt_bool("true") is True

    def test_false_string(self) -> None:
        """
        Convert the lowercase false string to False.

        Example:
            >>> TestAdaptBool().test_false_string()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert adapt_bool("false") is False

    def test_none_string(self) -> None:
        """
        Convert the lowercase none string to None.

        Example:
            >>> TestAdaptBool().test_none_string()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert adapt_bool("none") is None

    def test_empty_string(self) -> None:
        """
        Treat an empty Boolean string as missing.

        Example:
            >>> TestAdaptBool().test_empty_string()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert adapt_bool("") is None

    def test_integer_string_one(self) -> None:
        """
        Convert the string 1 to True.

        Example:
            >>> TestAdaptBool().test_integer_string_one()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert adapt_bool("1") is True

    def test_integer_string_zero(self) -> None:
        """
        Convert the string 0 to False.

        Example:
            >>> TestAdaptBool().test_integer_string_zero()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert adapt_bool("0") is False

    def test_none_passthrough(self) -> None:
        """
        Preserve a missing Boolean value.

        Example:
            >>> TestAdaptBool().test_none_passthrough()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert adapt_bool(None) is None

    def test_bool_passthrough_true(self) -> None:
        """
        Preserve an existing True value.

        Example:
            >>> TestAdaptBool().test_bool_passthrough_true()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert adapt_bool(True) is True

    def test_bool_passthrough_false(self) -> None:
        """
        Preserve an existing False value.

        Example:
            >>> TestAdaptBool().test_bool_passthrough_false()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert adapt_bool(False) is False

    def test_integer_coercion(self) -> None:
        """
        Convert integer one and zero to the corresponding bool singletons.

        Example:
            >>> TestAdaptBool().test_integer_coercion()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert adapt_bool(1) is True
        assert adapt_bool(0) is False
