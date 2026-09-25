"""
Exercise database field adapters and custom-column conversion rules.

These tests use in-memory values and metadata dicts; no database fixtures are
required. Generic and custom adapters have different missing-value rules, and custom
Boolean tests deliberately preserve three existing AttributeError paths.

Example:
    Run the full module with pytest::

        python -m pytest -q tests/databases/test_adaptors.py
"""
from __future__ import annotations

import datetime
import pytest

from LiuXin_alpha.databases.adaptors import (
    adapt_bool,
    adapt_date,
    adapt_datetime,
    adapt_identifiers,
    adapt_number,
    cc_adapt_bool,
    cc_adapt_enum,
    cc_adapt_number,
    cc_adapt_rating,
    cc_adapt_text,
    clean_identifier,
    get_adapter,
    get_series_values,
    multiple_text,
    single_text,
    sqlite_datetime,
)
from LiuXin_alpha.errors import InvalidUpdate


# ---------------------------------------------------------------------------
# sqlite_datetime
# ---------------------------------------------------------------------------


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
        assert sqlite_datetime("2024-01-01 00:00:00") == "2024-01-01 00:00:00"

    def test_formats_datetime_object(self) -> None:
        """
        Format a UTC datetime as the exact space-separated timestamp with offset.

        Example:
            >>> TestSqliteDatetime().test_formats_datetime_object()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        dt = datetime.datetime(2024, 6, 15, 12, 30, 0, tzinfo=datetime.timezone.utc)
        result = sqlite_datetime(dt)
        assert isinstance(result, str)
        assert result == "2024-06-15 12:30:00+00:00"

    def test_passes_none_through(self) -> None:
        """
        Preserve None when formatting a SQLite datetime value.

        Example:
            >>> TestSqliteDatetime().test_passes_none_through()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert sqlite_datetime(None) is None


# ---------------------------------------------------------------------------
# single_text
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# get_series_values
# ---------------------------------------------------------------------------


class TestGetSeriesValues:
    """
    Check series names and optional bracketed numeric indexes.

    Example:
        >>> TestGetSeriesValues().test_none_input()
    """
    def test_none_input(self) -> None:
        """
        Return two None values for a missing series expression.

        Example:
            >>> TestGetSeriesValues().test_none_input()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert get_series_values(None) == (None, None)

    def test_empty_string(self) -> None:
        """
        Preserve an empty series name with no index.

        Example:
            >>> TestGetSeriesValues().test_empty_string()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert get_series_values("") == ("", None)

    def test_series_with_integer_index(self) -> None:
        """
        Split Dune [1] into its name and numeric index 1.0.

        Example:
            >>> TestGetSeriesValues().test_series_with_integer_index()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        series, idx = get_series_values("Dune [1]")
        assert series == "Dune"
        assert idx == 1.0

    def test_series_with_float_index(self) -> None:
        """
        Split Foundation [2.5] without losing the fractional index.

        Example:
            >>> TestGetSeriesValues().test_series_with_float_index()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        series, idx = get_series_values("Foundation [2.5]")
        assert series == "Foundation"
        assert idx == 2.5

    def test_plain_series_name_returns_none_index(self) -> None:
        """
        Preserve a plain series name and report no index.

        Example:
            >>> TestGetSeriesValues().test_plain_series_name_returns_none_index()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        series, idx = get_series_values("Some Series")
        assert series == "Some Series"
        assert idx is None

    def test_strips_surrounding_whitespace(self) -> None:
        """
        Trim the Wheel of Time expression and extract index 14.0.

        Example:
            >>> TestGetSeriesValues().test_strips_surrounding_whitespace()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        series, idx = get_series_values("  Wheel of Time [14]  ")
        assert series == "Wheel of Time"
        assert idx == 14.0


# ---------------------------------------------------------------------------
# multiple_text
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# adapt_datetime
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# adapt_date
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# adapt_number
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# adapt_bool
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# clean_identifier
# ---------------------------------------------------------------------------


class TestCleanIdentifier:
    """
    Check identifier scheme cleanup and value punctuation.

    Example:
        >>> TestCleanIdentifier().test_strips_colons_from_type()
    """
    def test_strips_colons_from_type(self) -> None:
        """
        Remove the trailing colon from the isbn scheme.

        Example:
            >>> TestCleanIdentifier().test_strips_colons_from_type()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        typ, val = clean_identifier("isbn:", "1234567890")
        assert typ == "isbn"

    def test_strips_commas_from_type(self) -> None:
        """
        Remove commas from a supplied identifier scheme.

        The assertion only checks absence of commas, not the entire resulting scheme.

        Example:
            >>> TestCleanIdentifier().test_strips_commas_from_type()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        typ, val = clean_identifier("my,type", "value")
        assert "," not in typ

    def test_replaces_commas_with_pipe_in_val(self) -> None:
        """
        Replace the comma inside an identifier value with a pipe.

        Example:
            >>> TestCleanIdentifier().test_replaces_commas_with_pipe_in_val()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        typ, val = clean_identifier("key", "val,ue")
        assert val == "val|ue"

    def test_normalises_type_to_lowercase(self) -> None:
        """
        Lowercase the uppercase ISBN scheme.

        Example:
            >>> TestCleanIdentifier().test_normalises_type_to_lowercase()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        typ, val = clean_identifier("ISBN", "9780000000000")
        assert typ == "isbn"

    def test_empty_type_and_val(self) -> None:
        """
        Preserve empty scheme and value strings.

        Example:
            >>> TestCleanIdentifier().test_empty_type_and_val()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        typ, val = clean_identifier("", "")
        assert typ == ""
        assert val == ""

    def test_none_type_becomes_empty(self) -> None:
        """
        Convert a None scheme to an empty string.

        Example:
            >>> TestCleanIdentifier().test_none_type_becomes_empty()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        typ, val = clean_identifier(None, "value")
        assert typ == ""

    def test_none_val_becomes_empty(self) -> None:
        """
        Convert a None identifier value to an empty string.

        Example:
            >>> TestCleanIdentifier().test_none_val_becomes_empty()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        typ, val = clean_identifier("isbn", None)
        assert val == ""


# ---------------------------------------------------------------------------
# adapt_identifiers
# ---------------------------------------------------------------------------


def _simple_to_tuple(x: str) -> list[str]:
    """
    Split comma-separated identifier entries, stripping and discarding empty tokens.

    False inputs return a new empty list. This test helper provides no quoting or
    escaped-comma grammar.

    Example:
        >>> _simple_to_tuple(' isbn:123, , asin:B1 ')
        ['isbn:123', 'asin:B1']


    :param x: Comma-separated text; false values produce an empty list.
    :return: List of nonempty stripped entries in input order.
    """
    if not x:
        return []
    return [p.strip() for p in x.split(",") if p.strip()]


class TestAdaptIdentifiers:
    """
    Check text and mapping identifier inputs using a simple comma splitter.

    Example:
        >>> TestAdaptIdentifiers().test_parses_colon_separated_pairs()
    """
    def test_parses_colon_separated_pairs(self) -> None:
        """
        Extract the expected isbn and asin values from two colon-separated entries.

        Example:
            >>> TestAdaptIdentifiers().test_parses_colon_separated_pairs()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        result = adapt_identifiers(_simple_to_tuple, "isbn:9780000000000,asin:B001234")
        assert result["isbn"] == "9780000000000"
        assert result["asin"] == "B001234"

    def test_dict_input_passthrough(self) -> None:
        """
        Retain the isbn value supplied by a mapping input.

        This checks the stored value, not whether the original dict object is returned.

        Example:
            >>> TestAdaptIdentifiers().test_dict_input_passthrough()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        d = {"isbn": "9780000000000"}
        result = adapt_identifiers(_simple_to_tuple, d)
        assert result["isbn"] == "9780000000000"

    def test_empty_string_returns_empty_dict(self) -> None:
        """
        Produce an empty mapping from an empty identifier string.

        Example:
            >>> TestAdaptIdentifiers().test_empty_string_returns_empty_dict()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        result = adapt_identifiers(_simple_to_tuple, "")
        assert result == {}

    def test_strips_colons_from_type(self) -> None:
        """
        Keep an isbn key when the input contains a double colon.

        The assertion does not check how the extra colon affects the value.

        Example:
            >>> TestAdaptIdentifiers().test_strips_colons_from_type()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        result = adapt_identifiers(_simple_to_tuple, "isbn::9780000000000")
        # clean_identifier strips colons from type
        assert "isbn" in result

    def test_skips_entries_with_empty_key_or_val(self) -> None:
        """
        Exclude empty identifier keys and values from malformed entries.

        Example:
            >>> TestAdaptIdentifiers().test_skips_entries_with_empty_key_or_val()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        result = adapt_identifiers(_simple_to_tuple, ":nokey,goodkey:")
        assert not any(k == "" for k in result)
        assert not any(v == "" for v in result.values())


# ---------------------------------------------------------------------------
# get_adapter
# ---------------------------------------------------------------------------


class TestGetAdapter:
    """
    Check metadata-driven adapter selection and field-specific defaults.

    Example:
        >>> TestGetAdapter().test_text_field_single_returns_stripped_string()
    """
    def _text_meta(self, is_multiple=None) -> dict:
        """
        Build fresh text-field metadata with the requested multiplicity descriptor.

        The descriptor is assigned directly, without copying or validation.

        Example:
            >>> TestGetAdapter()._text_meta()
            {'datatype': 'text', 'is_multiple': None}


        :param is_multiple: Multiplicity descriptor passed through unchanged; None selects
            single text.
        :return: New metadata dict containing datatype and is_multiple.
        """
        return {"datatype": "text", "is_multiple": is_multiple}

    def _multi_meta(self) -> dict:
        """
        Build fresh metadata for comma-separated multiple text values.

        Example:
            >>> TestGetAdapter()._multi_meta()['is_multiple']
            {'ui_to_list': ',', 'list_to_ui': ', '}


        :return: New text metadata dict with a fresh separator mapping.
        """
        return {
            "datatype": "text",
            "is_multiple": {"ui_to_list": ",", "list_to_ui": ", "},
        }

    def test_text_field_single_returns_stripped_string(self) -> None:
        """
        Trim a single publisher value through the text adapter.

        Example:
            >>> TestGetAdapter().test_text_field_single_returns_stripped_string()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        adapter = get_adapter("publisher", self._text_meta())
        assert adapter("  Penguin  ") == "Penguin"

    def test_text_field_none_returns_none(self) -> None:
        """
        Keep a missing publisher value as None.

        Example:
            >>> TestGetAdapter().test_text_field_none_returns_none()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        adapter = get_adapter("publisher", self._text_meta())
        assert adapter(None) is None

    def test_title_fallback_to_unknown(self) -> None:
        """
        Use Unknown for both missing and whitespace-only titles.

        Example:
            >>> TestGetAdapter().test_title_fallback_to_unknown()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        adapter = get_adapter("title", self._text_meta())
        assert adapter(None) == "Unknown"
        assert adapter("   ") == "Unknown"

    def test_author_sort_fallback_to_empty_string(self) -> None:
        """
        Use an empty author-sort string for missing input.

        Example:
            >>> TestGetAdapter().test_author_sort_fallback_to_empty_string()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        adapter = get_adapter("author_sort", self._text_meta())
        assert adapter(None) == ""

    def test_series_index_fallback_to_one(self) -> None:
        """
        Default a missing series index to 1.0 and preserve 3.0.

        Example:
            >>> TestGetAdapter().test_series_index_fallback_to_one()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        meta = {"datatype": "float", "is_multiple": None}
        adapter = get_adapter("series_index", meta)
        assert adapter(None) == 1.0
        assert adapter(3.0) == 3.0

    def test_bool_field(self) -> None:
        """
        Convert true and false strings through Boolean field metadata.

        Example:
            >>> TestGetAdapter().test_bool_field()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        meta = {"datatype": "bool", "is_multiple": None}
        adapter = get_adapter("read", meta)
        assert adapter("true") is True
        assert adapter("false") is False

    def test_int_field(self) -> None:
        """
        Use integer datatype metadata to turn the string 7 into numeric 7.

        Example:
            >>> TestGetAdapter().test_int_field()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        meta = {"datatype": "int", "is_multiple": None}
        adapter = get_adapter("rating", meta)
        assert adapter("7") == 7

    def test_float_field(self) -> None:
        """
        Convert custom float text to 2.5 within absolute tolerance 1e-9.

        Example:
            >>> TestGetAdapter().test_float_field()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        meta = {"datatype": "float", "is_multiple": None}
        adapter = get_adapter("custom_float", meta)
        result = adapter("2.5")
        assert abs(result - 2.5) < 1e-9

    def test_datetime_field(self) -> None:
        """
        Use UNDEFINED_DATE for a missing timestamp field.

        Example:
            >>> TestGetAdapter().test_datetime_field()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        meta = {"datatype": "datetime", "is_multiple": None}
        adapter = get_adapter("timestamp", meta)
        from LiuXin_alpha.utils.date import UNDEFINED_DATE

        assert adapter(None) == UNDEFINED_DATE

    def test_pubdate_uses_adapt_date(self) -> None:
        """
        Produce a datetime for a pubdate calendar string.

        Example:
            >>> TestGetAdapter().test_pubdate_uses_adapt_date()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        meta = {"datatype": "datetime", "is_multiple": None}
        adapter = get_adapter("pubdate", meta)
        result = adapter("2024-01-01")
        assert isinstance(result, datetime.datetime)

    def test_series_datatype_uses_single_text(self) -> None:
        """
        Trim Dune through the series datatype adapter.

        Example:
            >>> TestGetAdapter().test_series_datatype_uses_single_text()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        meta = {"datatype": "series", "is_multiple": None}
        adapter = get_adapter("series", meta)
        assert adapter("  Dune  ") == "Dune"

    def test_comments_field(self) -> None:
        """
        Preserve an ordinary comments string through its adapter.

        Example:
            >>> TestGetAdapter().test_comments_field()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        meta = {"datatype": "comments", "is_multiple": None}
        adapter = get_adapter("comments", meta)
        assert adapter("A great book.") == "A great book."

    def test_enumeration_field(self) -> None:
        """
        Preserve Active through enumeration datatype metadata.

        Example:
            >>> TestGetAdapter().test_enumeration_field()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        meta = {"datatype": "enumeration", "is_multiple": None}
        adapter = get_adapter("status", meta)
        assert adapter("Active") == "Active"

    def test_composite_field_passthrough(self) -> None:
        """
        Pass a raw composite field value through unchanged.

        Example:
            >>> TestGetAdapter().test_composite_field_passthrough()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        meta = {"datatype": "composite", "is_multiple": None}
        adapter = get_adapter("template", meta)
        assert adapter("raw value") == "raw value"

    def test_rating_field_clamps_at_10(self) -> None:
        """
        Clamp a rating input of 15 to 10.

        Example:
            >>> TestGetAdapter().test_rating_field_clamps_at_10()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        meta = {"datatype": "rating", "is_multiple": None}
        adapter = get_adapter("rating", meta)
        assert adapter(15) == 10

    def test_rating_field_none_returns_none(self) -> None:
        """
        Keep a missing rating as None.

        Example:
            >>> TestGetAdapter().test_rating_field_none_returns_none()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        meta = {"datatype": "rating", "is_multiple": None}
        adapter = get_adapter("rating", meta)
        assert adapter(None) is None

    def test_rating_field_zero_returns_none(self) -> None:
        """
        Convert rating zero to None rather than a numeric zero.

        Example:
            >>> TestGetAdapter().test_rating_field_zero_returns_none()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        meta = {"datatype": "rating", "is_multiple": None}
        adapter = get_adapter("rating", meta)
        assert adapter(0) is None

    def test_unknown_datatype_raises(self) -> None:
        """
        Reject an unknown datatype with NotImplementedError.

        Example:
            >>> TestGetAdapter().test_unknown_datatype_raises()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        meta = {"datatype": "unknown_type_xyz", "is_multiple": None}
        with pytest.raises(NotImplementedError):
            get_adapter("field", meta)

    def test_multiple_text_field_splits(self) -> None:
        """
        Include both sci-fi and fantasy after splitting tag input.

        Membership is checked without asserting the container type or order.

        Example:
            >>> TestGetAdapter().test_multiple_text_field_splits()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        adapter = get_adapter("tags", self._multi_meta())
        result = adapter("sci-fi, fantasy")
        assert "sci-fi" in result
        assert "fantasy" in result

    def test_authors_field_replaces_pipe_with_comma(self) -> None:
        """
        Produce author entries containing commas from pipe-separated name parts.

        The assertion checks each returned entry, without fixing the exact names or count.

        Example:
            >>> TestGetAdapter().test_authors_field_replaces_pipe_with_comma()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        meta = {
            "datatype": "text",
            "is_multiple": {"ui_to_list": ",", "list_to_ui": " & "},
        }
        adapter = get_adapter("authors", meta)
        result = adapter("Adams|Douglas,Doe|John")
        assert all("," in a for a in result)

    def test_last_modified_fallback_to_undefined_date(self) -> None:
        """
        Use UNDEFINED_DATE for a missing last-modified field.

        Example:
            >>> TestGetAdapter().test_last_modified_fallback_to_undefined_date()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        meta = {"datatype": "datetime", "is_multiple": None}
        adapter = get_adapter("last_modified", meta)
        from LiuXin_alpha.utils.date import UNDEFINED_DATE

        assert adapter(None) == UNDEFINED_DATE


# ---------------------------------------------------------------------------
# cc_adapt_text
# ---------------------------------------------------------------------------


class TestCcAdaptText:
    """
    Check custom-column text passthrough, list splitting and rejection.

    Example:
        >>> TestCcAdaptText().test_single_string_passthrough()
    """
    _d_single = {"is_multiple": None, "datatype": "text"}
    _d_multi = {
        "is_multiple": True,
        "datatype": "text",
        "multiple_seps": {"ui_to_list": ","},
    }

    def test_single_string_passthrough(self) -> None:
        """
        Preserve hello for a single custom text column.

        Example:
            >>> TestCcAdaptText().test_single_string_passthrough()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert cc_adapt_text("hello", self._d_single) == "hello"

    def test_single_none_passthrough(self) -> None:
        """
        Preserve None for a single custom text column.

        Example:
            >>> TestCcAdaptText().test_single_none_passthrough()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert cc_adapt_text(None, self._d_single) is None

    def test_multi_splits_on_sep(self) -> None:
        """
        Split three comma-separated custom text values into an ordered list.

        Example:
            >>> TestCcAdaptText().test_multi_splits_on_sep()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        result = cc_adapt_text("a,b,c", self._d_multi)
        assert result == ["a", "b", "c"]

    def test_multi_none_returns_empty_list(self) -> None:
        """
        Produce an empty list for a missing multiple custom text value.

        Example:
            >>> TestCcAdaptText().test_multi_none_returns_empty_list()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert cc_adapt_text(None, self._d_multi) == []

    def test_multi_strips_whitespace_from_items(self) -> None:
        """
        Trim both items in a multiple custom text column.

        Example:
            >>> TestCcAdaptText().test_multi_strips_whitespace_from_items()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        result = cc_adapt_text("  alpha  ,  beta  ", self._d_multi)
        assert result == ["alpha", "beta"]

    def test_multi_skips_empty_tokens(self) -> None:
        """
        Exclude empty strings after splitting adjacent commas.

        Example:
            >>> TestCcAdaptText().test_multi_skips_empty_tokens()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        result = cc_adapt_text("a,,b", self._d_multi)
        assert "" not in result

    def test_single_non_string_non_none_raises(self) -> None:
        """
        Reject integer 42 in a single custom text column with InvalidUpdate.

        Example:
            >>> TestCcAdaptText().test_single_non_string_non_none_raises()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        with pytest.raises(InvalidUpdate):
            cc_adapt_text(42, self._d_single)


# ---------------------------------------------------------------------------
# cc_adapt_bool
# ---------------------------------------------------------------------------


class TestCcAdaptBool:
    """
    Check custom Boolean strings and preserve current non-string failures.

    None and bool inputs currently reach a shadowed datetime import and raise
    AttributeError. These tests record that existing bug rather than desired conversion
    semantics.

    Example:
        >>> TestCcAdaptBool().test_true_string()
    """
    def test_true_string(self) -> None:
        """
        Convert custom Boolean text true to True.

        Example:
            >>> TestCcAdaptBool().test_true_string()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert cc_adapt_bool("true", {}) is True

    def test_false_string(self) -> None:
        """
        Convert custom Boolean text false to False.

        Example:
            >>> TestCcAdaptBool().test_false_string()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert cc_adapt_bool("false", {}) is False

    def test_one_string(self) -> None:
        """
        Convert custom Boolean text 1 to True.

        Example:
            >>> TestCcAdaptBool().test_one_string()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert cc_adapt_bool("1", {}) is True

    def test_zero_string(self) -> None:
        """
        Convert custom Boolean text 0 to False.

        Example:
            >>> TestCcAdaptBool().test_zero_string()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert cc_adapt_bool("0", {}) is False

    def test_none_string(self) -> None:
        """
        Convert custom Boolean text none to None.

        Example:
            >>> TestCcAdaptBool().test_none_string()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert cc_adapt_bool("none", {}) is None

    def test_float_raises_invalid_update(self) -> None:
        # Float is checked before the broken datetime branch, so InvalidUpdate fires.
        """
        Reject float 3.14 with InvalidUpdate before the broken datetime branch.

        Example:
            >>> TestCcAdaptBool().test_float_raises_invalid_update()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        with pytest.raises(InvalidUpdate):
            cc_adapt_bool(3.14, {})

    def test_invalid_string_raises(self) -> None:
        """
        Reject not_a_bool with InvalidUpdate.

        Example:
            >>> TestCcAdaptBool().test_invalid_string_raises()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        with pytest.raises(InvalidUpdate):
            cc_adapt_bool("not_a_bool", {})

    def test_none_input_raises_attribute_error(self) -> None:
        # Pre-existing bug: `from datetime import datetime` in adaptors.py shadows the
        # `datetime` module, so `isinstance(x, datetime.datetime)` fails with
        # AttributeError for any non-string, non-float input (including None, bool).
        """
        Preserve the existing AttributeError for a None custom Boolean input.

        Example:
            >>> TestCcAdaptBool().test_none_input_raises_attribute_error()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        with pytest.raises(AttributeError):
            cc_adapt_bool(None, {})

    def test_bool_true_input_raises_attribute_error(self) -> None:
        # Pre-existing bug: same shadowed import causes AttributeError for bool inputs.
        """
        Preserve the existing AttributeError for a True custom Boolean input.

        Example:
            >>> TestCcAdaptBool().test_bool_true_input_raises_attribute_error()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        with pytest.raises(AttributeError):
            cc_adapt_bool(True, {})

    def test_bool_false_input_raises_attribute_error(self) -> None:
        # Pre-existing bug: same shadowed import causes AttributeError for bool inputs.
        """
        Preserve the existing AttributeError for a False custom Boolean input.

        Example:
            >>> TestCcAdaptBool().test_bool_false_input_raises_attribute_error()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        with pytest.raises(AttributeError):
            cc_adapt_bool(False, {})


# ---------------------------------------------------------------------------
# cc_adapt_enum
# ---------------------------------------------------------------------------


class TestCcAdaptEnum:
    """
    Check single enumeration values, missing inputs and whitespace retention.

    Example:
        >>> TestCcAdaptEnum().test_valid_enum_value()
    """
    _d = {"is_multiple": None, "datatype": "enumeration"}

    def test_valid_enum_value(self) -> None:
        """
        Preserve Active as a single enumeration value.

        Example:
            >>> TestCcAdaptEnum().test_valid_enum_value()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert cc_adapt_enum("Active", self._d) == "Active"

    def test_empty_string_returns_none(self) -> None:
        """
        Convert an empty enumeration string to None.

        Example:
            >>> TestCcAdaptEnum().test_empty_string_returns_none()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert cc_adapt_enum("", self._d) is None

    def test_none_returns_none(self) -> None:
        """
        Preserve None as a missing enumeration value.

        Example:
            >>> TestCcAdaptEnum().test_none_returns_none()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert cc_adapt_enum(None, self._d) is None

    def test_does_not_strip_whitespace(self) -> None:
        # cc_adapt_enum delegates to cc_adapt_text for single fields, which
        # does not strip whitespace for single (non-multiple) text columns.
        """
        Keep spaces surrounding Active in a single enumeration value.

        Example:
            >>> TestCcAdaptEnum().test_does_not_strip_whitespace()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        result = cc_adapt_enum("  Active  ", self._d)
        assert result == "  Active  "


# ---------------------------------------------------------------------------
# cc_adapt_number
# ---------------------------------------------------------------------------


class TestCcAdaptNumber:
    """
    Check custom numeric coercion and InvalidUpdate rejection paths.

    Example:
        >>> TestCcAdaptNumber().test_none_returns_none()
    """
    def test_none_returns_none(self) -> None:
        """
        Preserve None for an integer custom column.

        Example:
            >>> TestCcAdaptNumber().test_none_returns_none()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert cc_adapt_number(None, {"datatype": "int"}) is None

    def test_none_string_returns_none(self) -> None:
        """
        Convert the none string to None for an integer custom column.

        Example:
            >>> TestCcAdaptNumber().test_none_string_returns_none()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert cc_adapt_number("none", {"datatype": "int"}) is None

    def test_int_coercion(self) -> None:
        """
        Preserve integer 42 through integer custom-column conversion.

        Example:
            >>> TestCcAdaptNumber().test_int_coercion()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert cc_adapt_number(42, {"datatype": "int"}) == 42

    def test_float_coercion(self) -> None:
        """
        Convert 3.14 text to a float within absolute tolerance 1e-9.

        Example:
            >>> TestCcAdaptNumber().test_float_coercion()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        result = cc_adapt_number("3.14", {"datatype": "float"})
        assert abs(result - 3.14) < 1e-9

    def test_bool_true_raises(self) -> None:
        """
        Reject True as an integer custom-column value with InvalidUpdate.

        Example:
            >>> TestCcAdaptNumber().test_bool_true_raises()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        with pytest.raises(InvalidUpdate):
            cc_adapt_number(True, {"datatype": "int"})

    def test_bool_false_raises(self) -> None:
        """
        Reject False as a float custom-column value with InvalidUpdate.

        Example:
            >>> TestCcAdaptNumber().test_bool_false_raises()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        with pytest.raises(InvalidUpdate):
            cc_adapt_number(False, {"datatype": "float"})

    def test_invalid_string_raises(self) -> None:
        """
        Reject abc as integer input with InvalidUpdate.

        Example:
            >>> TestCcAdaptNumber().test_invalid_string_raises()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        with pytest.raises(InvalidUpdate):
            cc_adapt_number("abc", {"datatype": "int"})


# ---------------------------------------------------------------------------
# cc_adapt_rating
# ---------------------------------------------------------------------------


class TestCcAdaptRating:
    """
    Check custom ratings, clamping and rejected Boolean or invalid inputs.

    Example:
        >>> TestCcAdaptRating().test_none_returns_none()
    """
    def test_none_returns_none(self) -> None:
        """
        Preserve None as a missing custom rating.

        Example:
            >>> TestCcAdaptRating().test_none_returns_none()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert cc_adapt_rating(None, {}) is None

    def test_valid_float(self) -> None:
        """
        Preserve rating 5.0 within the accepted range.

        Example:
            >>> TestCcAdaptRating().test_valid_float()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert cc_adapt_rating(5.0, {}) == 5.0

    def test_clamps_above_ten(self) -> None:
        """
        Clamp rating 11.0 to the upper bound 10.0.

        Example:
            >>> TestCcAdaptRating().test_clamps_above_ten()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert cc_adapt_rating(11.0, {}) == 10.0

    def test_clamps_below_zero(self) -> None:
        """
        Clamp rating -1.0 to the lower bound 0.0.

        Example:
            >>> TestCcAdaptRating().test_clamps_below_zero()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert cc_adapt_rating(-1.0, {}) == 0.0

    def test_bool_true_raises(self) -> None:
        """
        Reject True as a rating with InvalidUpdate.

        Example:
            >>> TestCcAdaptRating().test_bool_true_raises()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        with pytest.raises(InvalidUpdate):
            cc_adapt_rating(True, {})

    def test_bool_false_raises(self) -> None:
        """
        Reject False as a rating with InvalidUpdate.

        Example:
            >>> TestCcAdaptRating().test_bool_false_raises()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        with pytest.raises(InvalidUpdate):
            cc_adapt_rating(False, {})

    def test_invalid_string_raises(self) -> None:
        """
        Reject nonnumeric rating text with InvalidUpdate.

        Example:
            >>> TestCcAdaptRating().test_invalid_string_raises()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        with pytest.raises(InvalidUpdate):
            cc_adapt_rating("not_a_number", {})

    def test_string_number_coerced(self) -> None:
        """
        Convert numeric rating text 7 to 7.0.

        Example:
            >>> TestCcAdaptRating().test_string_number_coerced()


        :return: None; raises AssertionError if the stated contract regresses.
        """
        assert cc_adapt_rating("7", {}) == 7.0
