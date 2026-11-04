"""Exercise legacy bibliographic and custom-field conversion contracts.

These tests use values and field descriptors without database fixtures. Generic
and custom-field adapters retain different missing-value and whitespace rules;
custom boolean tests also document the existing datetime-class failure paths.
"""

from __future__ import annotations

import datetime

import pytest

from LiuXin_alpha.errors import InvalidUpdate
from LiuXin_alpha.metadata.field_adaptors import (
    adapt_identifiers,
    adapt_languages,
    cc_adapt_bool,
    cc_adapt_enum,
    cc_adapt_number,
    cc_adapt_rating,
    cc_adapt_text,
    get_adapter,
    get_series_values,
)
from LiuXin_alpha.utils.adaptors import adapt_date, adapt_datetime


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


def test_adapt_languages_converts_serialized_input_before_normalizing() -> None:
    """Keep serialized field parsing at the database adapter boundary."""

    assert adapt_languages(_simple_to_tuple, "eng,spa,eng") == (
        "English",
        "Spanish; Castilian",
    )


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
        # Pre-existing bug: field_adaptors.py binds datetime to the class rather than
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
