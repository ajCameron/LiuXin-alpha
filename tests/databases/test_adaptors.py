"""Tests for LiuXin_alpha.databases.adaptors.

Covers the full set of field-adaptor helpers: single_text, get_series_values,
multiple_text, adapt_datetime, adapt_date, adapt_number, adapt_bool,
clean_identifier, adapt_identifiers, get_adapter, sqlite_datetime,
and the custom-column (cc_*) helpers.
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
    def test_passes_non_datetime_through(self) -> None:
        assert sqlite_datetime("2024-01-01 00:00:00") == "2024-01-01 00:00:00"

    def test_formats_datetime_object(self) -> None:
        dt = datetime.datetime(2024, 6, 15, 12, 30, 0)
        result = sqlite_datetime(dt)
        assert isinstance(result, str)
        assert "2024" in result
        assert "12" in result

    def test_passes_none_through(self) -> None:
        assert sqlite_datetime(None) is None


# ---------------------------------------------------------------------------
# single_text
# ---------------------------------------------------------------------------


class TestSingleText:
    def test_strips_whitespace(self) -> None:
        assert single_text("  hello  ") == "hello"

    def test_none_returns_none(self) -> None:
        assert single_text(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert single_text("") is None

    def test_whitespace_only_returns_none(self) -> None:
        assert single_text("   ") is None

    def test_normal_string_passthrough(self) -> None:
        assert single_text("Penguin Books") == "Penguin Books"

    def test_strips_and_preserves_content(self) -> None:
        assert single_text("\t  Great Expectations  \n") == "Great Expectations"


# ---------------------------------------------------------------------------
# get_series_values
# ---------------------------------------------------------------------------


class TestGetSeriesValues:
    def test_none_input(self) -> None:
        assert get_series_values(None) == (None, None)

    def test_empty_string(self) -> None:
        assert get_series_values("") == ("", None)

    def test_series_with_integer_index(self) -> None:
        series, idx = get_series_values("Dune [1]")
        assert series == "Dune"
        assert idx == 1.0

    def test_series_with_float_index(self) -> None:
        series, idx = get_series_values("Foundation [2.5]")
        assert series == "Foundation"
        assert idx == 2.5

    def test_plain_series_name_returns_none_index(self) -> None:
        series, idx = get_series_values("Some Series")
        assert series == "Some Series"
        assert idx is None

    def test_strips_surrounding_whitespace(self) -> None:
        series, idx = get_series_values("  Wheel of Time [14]  ")
        assert series == "Wheel of Time"
        assert idx == 14.0


# ---------------------------------------------------------------------------
# multiple_text
# ---------------------------------------------------------------------------


class TestMultipleText:
    def test_empty_input(self) -> None:
        assert multiple_text(",", ", ", "") == ()

    def test_none_input(self) -> None:
        assert multiple_text(",", ", ", None) == ()

    def test_splits_on_sep(self) -> None:
        result = multiple_text(",", ", ", "a,b,c")
        assert result == ("a", "b", "c")

    def test_strips_items(self) -> None:
        result = multiple_text(",", ", ", "  alpha  ,  beta  ")
        assert result == ("alpha", "beta")

    def test_empty_items_skipped(self) -> None:
        result = multiple_text(",", ", ", "a,,b")
        assert result == ("a", "b")

    def test_ui_sep_replacement(self) -> None:
        # When ui_sep is ";", commas in values are replaced with semicolons.
        result = multiple_text(",", ";", "a,b")
        assert len(result) == 2


# ---------------------------------------------------------------------------
# adapt_datetime
# ---------------------------------------------------------------------------


class TestAdaptDatetime:
    def test_datetime_passthrough(self) -> None:
        dt = datetime.datetime(2024, 1, 1, 12, 0, 0)
        result = adapt_datetime(dt)
        assert result == dt

    def test_string_input_parsed(self) -> None:
        result = adapt_datetime("2024-01-15T10:30:00")
        assert isinstance(result, datetime.datetime)

    def test_none_returns_none(self) -> None:
        # None is falsy, so `if x and x_is_date_undefined` is False; None is returned
        # as-is even though is_date_undefined(None) is True.
        result = adapt_datetime(None)
        assert result is None


# ---------------------------------------------------------------------------
# adapt_date
# ---------------------------------------------------------------------------


class TestAdaptDate:
    def test_none_returns_undefined(self) -> None:
        from LiuXin_alpha.utils.date import UNDEFINED_DATE

        result = adapt_date(None)
        assert result == UNDEFINED_DATE

    def test_string_parsed_as_date(self) -> None:
        result = adapt_date("2024-06-01")
        assert isinstance(result, datetime.datetime)


# ---------------------------------------------------------------------------
# adapt_number
# ---------------------------------------------------------------------------


class TestAdaptNumber:
    def test_none_returns_none(self) -> None:
        assert adapt_number(int, None) is None

    def test_none_string_returns_none(self) -> None:
        assert adapt_number(int, "none") is None
        assert adapt_number(int, "NONE") is None

    def test_int_coercion(self) -> None:
        assert adapt_number(int, "42") == 42
        assert isinstance(adapt_number(int, "42"), int)

    def test_float_coercion(self) -> None:
        result = adapt_number(float, "3.14")
        assert abs(result - 3.14) < 1e-9

    def test_already_numeric(self) -> None:
        assert adapt_number(int, 7) == 7
        assert adapt_number(float, 2.5) == 2.5


# ---------------------------------------------------------------------------
# adapt_bool
# ---------------------------------------------------------------------------


class TestAdaptBool:
    def test_true_string(self) -> None:
        assert adapt_bool("true") is True

    def test_false_string(self) -> None:
        assert adapt_bool("false") is False

    def test_none_string(self) -> None:
        assert adapt_bool("none") is None

    def test_empty_string(self) -> None:
        assert adapt_bool("") is None

    def test_integer_string_one(self) -> None:
        assert adapt_bool("1") is True

    def test_integer_string_zero(self) -> None:
        assert adapt_bool("0") is False

    def test_none_passthrough(self) -> None:
        assert adapt_bool(None) is None

    def test_bool_passthrough_true(self) -> None:
        assert adapt_bool(True) is True

    def test_bool_passthrough_false(self) -> None:
        assert adapt_bool(False) is False

    def test_integer_coercion(self) -> None:
        assert adapt_bool(1) is True
        assert adapt_bool(0) is False


# ---------------------------------------------------------------------------
# clean_identifier
# ---------------------------------------------------------------------------


class TestCleanIdentifier:
    def test_strips_colons_from_type(self) -> None:
        typ, val = clean_identifier("isbn:", "1234567890")
        assert typ == "isbn"

    def test_strips_commas_from_type(self) -> None:
        typ, val = clean_identifier("my,type", "value")
        assert "," not in typ

    def test_replaces_commas_with_pipe_in_val(self) -> None:
        typ, val = clean_identifier("key", "val,ue")
        assert val == "val|ue"

    def test_normalises_type_to_lowercase(self) -> None:
        typ, val = clean_identifier("ISBN", "9780000000000")
        assert typ == "isbn"

    def test_empty_type_and_val(self) -> None:
        typ, val = clean_identifier("", "")
        assert typ == ""
        assert val == ""

    def test_none_type_becomes_empty(self) -> None:
        typ, val = clean_identifier(None, "value")
        assert typ == ""

    def test_none_val_becomes_empty(self) -> None:
        typ, val = clean_identifier("isbn", None)
        assert val == ""


# ---------------------------------------------------------------------------
# adapt_identifiers
# ---------------------------------------------------------------------------


def _simple_to_tuple(x: str) -> list[str]:
    """Minimal to_tuple function for testing adapt_identifiers."""
    if not x:
        return []
    return [p.strip() for p in x.split(",") if p.strip()]


class TestAdaptIdentifiers:
    def test_parses_colon_separated_pairs(self) -> None:
        result = adapt_identifiers(_simple_to_tuple, "isbn:9780000000000,asin:B001234")
        assert result["isbn"] == "9780000000000"
        assert result["asin"] == "B001234"

    def test_dict_input_passthrough(self) -> None:
        d = {"isbn": "9780000000000"}
        result = adapt_identifiers(_simple_to_tuple, d)
        assert result["isbn"] == "9780000000000"

    def test_empty_string_returns_empty_dict(self) -> None:
        result = adapt_identifiers(_simple_to_tuple, "")
        assert result == {}

    def test_strips_colons_from_type(self) -> None:
        result = adapt_identifiers(_simple_to_tuple, "isbn::9780000000000")
        # clean_identifier strips colons from type
        assert "isbn" in result

    def test_skips_entries_with_empty_key_or_val(self) -> None:
        result = adapt_identifiers(_simple_to_tuple, ":nokey,goodkey:")
        assert not any(k == "" for k in result)
        assert not any(v == "" for v in result.values())


# ---------------------------------------------------------------------------
# get_adapter
# ---------------------------------------------------------------------------


class TestGetAdapter:
    def _text_meta(self, is_multiple=None) -> dict:
        return {"datatype": "text", "is_multiple": is_multiple}

    def _multi_meta(self) -> dict:
        return {
            "datatype": "text",
            "is_multiple": {"ui_to_list": ",", "list_to_ui": ", "},
        }

    def test_text_field_single_returns_stripped_string(self) -> None:
        adapter = get_adapter("publisher", self._text_meta())
        assert adapter("  Penguin  ") == "Penguin"

    def test_text_field_none_returns_none(self) -> None:
        adapter = get_adapter("publisher", self._text_meta())
        assert adapter(None) is None

    def test_title_fallback_to_unknown(self) -> None:
        adapter = get_adapter("title", self._text_meta())
        assert adapter(None) == "Unknown"
        assert adapter("   ") == "Unknown"

    def test_author_sort_fallback_to_empty_string(self) -> None:
        adapter = get_adapter("author_sort", self._text_meta())
        assert adapter(None) == ""

    def test_series_index_fallback_to_one(self) -> None:
        meta = {"datatype": "float", "is_multiple": None}
        adapter = get_adapter("series_index", meta)
        assert adapter(None) == 1.0
        assert adapter(3.0) == 3.0

    def test_bool_field(self) -> None:
        meta = {"datatype": "bool", "is_multiple": None}
        adapter = get_adapter("read", meta)
        assert adapter("true") is True
        assert adapter("false") is False

    def test_int_field(self) -> None:
        meta = {"datatype": "int", "is_multiple": None}
        adapter = get_adapter("rating", meta)
        assert adapter("7") == 7

    def test_float_field(self) -> None:
        meta = {"datatype": "float", "is_multiple": None}
        adapter = get_adapter("custom_float", meta)
        result = adapter("2.5")
        assert abs(result - 2.5) < 1e-9

    def test_datetime_field(self) -> None:
        meta = {"datatype": "datetime", "is_multiple": None}
        adapter = get_adapter("timestamp", meta)
        from LiuXin_alpha.utils.date import UNDEFINED_DATE

        assert adapter(None) == UNDEFINED_DATE

    def test_pubdate_uses_adapt_date(self) -> None:
        meta = {"datatype": "datetime", "is_multiple": None}
        adapter = get_adapter("pubdate", meta)
        result = adapter("2024-01-01")
        assert isinstance(result, datetime.datetime)

    def test_series_datatype_uses_single_text(self) -> None:
        meta = {"datatype": "series", "is_multiple": None}
        adapter = get_adapter("series", meta)
        assert adapter("  Dune  ") == "Dune"

    def test_comments_field(self) -> None:
        meta = {"datatype": "comments", "is_multiple": None}
        adapter = get_adapter("comments", meta)
        assert adapter("A great book.") == "A great book."

    def test_enumeration_field(self) -> None:
        meta = {"datatype": "enumeration", "is_multiple": None}
        adapter = get_adapter("status", meta)
        assert adapter("Active") == "Active"

    def test_composite_field_passthrough(self) -> None:
        meta = {"datatype": "composite", "is_multiple": None}
        adapter = get_adapter("template", meta)
        assert adapter("raw value") == "raw value"

    def test_rating_field_clamps_at_10(self) -> None:
        meta = {"datatype": "rating", "is_multiple": None}
        adapter = get_adapter("rating", meta)
        assert adapter(15) == 10

    def test_rating_field_none_returns_none(self) -> None:
        meta = {"datatype": "rating", "is_multiple": None}
        adapter = get_adapter("rating", meta)
        assert adapter(None) is None

    def test_rating_field_zero_returns_none(self) -> None:
        meta = {"datatype": "rating", "is_multiple": None}
        adapter = get_adapter("rating", meta)
        assert adapter(0) is None

    def test_unknown_datatype_raises(self) -> None:
        meta = {"datatype": "unknown_type_xyz", "is_multiple": None}
        with pytest.raises(NotImplementedError):
            get_adapter("field", meta)

    def test_multiple_text_field_splits(self) -> None:
        adapter = get_adapter("tags", self._multi_meta())
        result = adapter("sci-fi, fantasy")
        assert "sci-fi" in result
        assert "fantasy" in result

    def test_authors_field_replaces_pipe_with_comma(self) -> None:
        meta = {
            "datatype": "text",
            "is_multiple": {"ui_to_list": ",", "list_to_ui": " & "},
        }
        adapter = get_adapter("authors", meta)
        result = adapter("Adams|Douglas,Doe|John")
        assert all("," in a for a in result)

    def test_last_modified_fallback_to_undefined_date(self) -> None:
        meta = {"datatype": "datetime", "is_multiple": None}
        adapter = get_adapter("last_modified", meta)
        from LiuXin_alpha.utils.date import UNDEFINED_DATE

        assert adapter(None) == UNDEFINED_DATE


# ---------------------------------------------------------------------------
# cc_adapt_text
# ---------------------------------------------------------------------------


class TestCcAdaptText:
    _d_single = {"is_multiple": None, "datatype": "text"}
    _d_multi = {
        "is_multiple": True,
        "datatype": "text",
        "multiple_seps": {"ui_to_list": ","},
    }

    def test_single_string_passthrough(self) -> None:
        assert cc_adapt_text("hello", self._d_single) == "hello"

    def test_single_none_passthrough(self) -> None:
        assert cc_adapt_text(None, self._d_single) is None

    def test_multi_splits_on_sep(self) -> None:
        result = cc_adapt_text("a,b,c", self._d_multi)
        assert result == ["a", "b", "c"]

    def test_multi_none_returns_empty_list(self) -> None:
        assert cc_adapt_text(None, self._d_multi) == []

    def test_multi_strips_whitespace_from_items(self) -> None:
        result = cc_adapt_text("  alpha  ,  beta  ", self._d_multi)
        assert result == ["alpha", "beta"]

    def test_multi_skips_empty_tokens(self) -> None:
        result = cc_adapt_text("a,,b", self._d_multi)
        assert "" not in result

    def test_single_non_string_non_none_raises(self) -> None:
        with pytest.raises(InvalidUpdate):
            cc_adapt_text(42, self._d_single)


# ---------------------------------------------------------------------------
# cc_adapt_bool
# ---------------------------------------------------------------------------


class TestCcAdaptBool:
    def test_true_string(self) -> None:
        assert cc_adapt_bool("true", {}) is True

    def test_false_string(self) -> None:
        assert cc_adapt_bool("false", {}) is False

    def test_one_string(self) -> None:
        assert cc_adapt_bool("1", {}) is True

    def test_zero_string(self) -> None:
        assert cc_adapt_bool("0", {}) is False

    def test_none_string(self) -> None:
        assert cc_adapt_bool("none", {}) is None

    def test_float_raises_invalid_update(self) -> None:
        # Float is checked before the broken datetime branch, so InvalidUpdate fires.
        with pytest.raises(InvalidUpdate):
            cc_adapt_bool(3.14, {})

    def test_invalid_string_raises(self) -> None:
        with pytest.raises(InvalidUpdate):
            cc_adapt_bool("not_a_bool", {})

    def test_none_input_raises_attribute_error(self) -> None:
        # Pre-existing bug: `from datetime import datetime` in adaptors.py shadows the
        # `datetime` module, so `isinstance(x, datetime.datetime)` fails with
        # AttributeError for any non-string, non-float input (including None, bool).
        with pytest.raises(AttributeError):
            cc_adapt_bool(None, {})

    def test_bool_true_input_raises_attribute_error(self) -> None:
        # Pre-existing bug: same shadowed import causes AttributeError for bool inputs.
        with pytest.raises(AttributeError):
            cc_adapt_bool(True, {})

    def test_bool_false_input_raises_attribute_error(self) -> None:
        # Pre-existing bug: same shadowed import causes AttributeError for bool inputs.
        with pytest.raises(AttributeError):
            cc_adapt_bool(False, {})


# ---------------------------------------------------------------------------
# cc_adapt_enum
# ---------------------------------------------------------------------------


class TestCcAdaptEnum:
    _d = {"is_multiple": None, "datatype": "enumeration"}

    def test_valid_enum_value(self) -> None:
        assert cc_adapt_enum("Active", self._d) == "Active"

    def test_empty_string_returns_none(self) -> None:
        assert cc_adapt_enum("", self._d) is None

    def test_none_returns_none(self) -> None:
        assert cc_adapt_enum(None, self._d) is None

    def test_does_not_strip_whitespace(self) -> None:
        # cc_adapt_enum delegates to cc_adapt_text for single fields, which
        # does not strip whitespace for single (non-multiple) text columns.
        result = cc_adapt_enum("  Active  ", self._d)
        assert result == "  Active  "


# ---------------------------------------------------------------------------
# cc_adapt_number
# ---------------------------------------------------------------------------


class TestCcAdaptNumber:
    def test_none_returns_none(self) -> None:
        assert cc_adapt_number(None, {"datatype": "int"}) is None

    def test_none_string_returns_none(self) -> None:
        assert cc_adapt_number("none", {"datatype": "int"}) is None

    def test_int_coercion(self) -> None:
        assert cc_adapt_number(42, {"datatype": "int"}) == 42

    def test_float_coercion(self) -> None:
        result = cc_adapt_number("3.14", {"datatype": "float"})
        assert abs(result - 3.14) < 1e-9

    def test_bool_true_raises(self) -> None:
        with pytest.raises(InvalidUpdate):
            cc_adapt_number(True, {"datatype": "int"})

    def test_bool_false_raises(self) -> None:
        with pytest.raises(InvalidUpdate):
            cc_adapt_number(False, {"datatype": "float"})

    def test_invalid_string_raises(self) -> None:
        with pytest.raises(InvalidUpdate):
            cc_adapt_number("abc", {"datatype": "int"})


# ---------------------------------------------------------------------------
# cc_adapt_rating
# ---------------------------------------------------------------------------


class TestCcAdaptRating:
    def test_none_returns_none(self) -> None:
        assert cc_adapt_rating(None, {}) is None

    def test_valid_float(self) -> None:
        assert cc_adapt_rating(5.0, {}) == 5.0

    def test_clamps_above_ten(self) -> None:
        assert cc_adapt_rating(11.0, {}) == 10.0

    def test_clamps_below_zero(self) -> None:
        assert cc_adapt_rating(-1.0, {}) == 0.0

    def test_bool_true_raises(self) -> None:
        with pytest.raises(InvalidUpdate):
            cc_adapt_rating(True, {})

    def test_bool_false_raises(self) -> None:
        with pytest.raises(InvalidUpdate):
            cc_adapt_rating(False, {})

    def test_invalid_string_raises(self) -> None:
        with pytest.raises(InvalidUpdate):
            cc_adapt_rating("not_a_number", {})

    def test_string_number_coerced(self) -> None:
        assert cc_adapt_rating("7", {}) == 7.0
