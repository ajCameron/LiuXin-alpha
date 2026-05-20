from __future__ import annotations

from io import BytesIO

import pytest

import LiuXin_alpha.file_formats as file_formats
import LiuXin_alpha.file_formats.utils as utils


@pytest.fixture(params=[file_formats, utils], ids=["top-level", "utils"])
def helper_module(request):
    return request.param


def test_check_ebook_format_detects_topaz_and_preserves_stream_position(helper_module) -> None:
    stream = BytesIO(b"TPZ payload")
    stream.seek(2)

    assert helper_module.check_ebook_format(stream, "azw3") == "tpz"
    assert stream.tell() == 0


def test_check_ebook_format_ignores_non_mobi_family_without_reading(helper_module) -> None:
    stream = BytesIO(b"TPZ payload")
    stream.seek(3)

    assert helper_module.check_ebook_format(stream, "epub") == "epub"
    assert stream.tell() == 3


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("12pt", 12.0),
        ("1pc", 12.0),
        ("96px", 72.0),
        ("2em", 20.0),
        ("2ex", 10.0),
        ("2en", 10.0),
        ("-1.5in", -108.0),
        (3, 3),
        (2.5, 2.5),
    ],
)
def test_unit_convert_handles_remaining_units(helper_module, value, expected) -> None:
    assert helper_module.unit_convert(value, base=12, font=10, dpi=96, body_font_size=12) == pytest.approx(expected)


def test_parse_css_length_rejects_empty_and_unitless_values(helper_module) -> None:
    assert helper_module.parse_css_length("") == (None, None)
    assert helper_module.parse_css_length("12") == (None, None)
    assert helper_module.parse_css_length(object()) == (None, None)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("plain", '"plain"'),
        ('has "double"', '\'has "double"\''),
    ],
)
def test_escape_xpath_attr_uses_simple_quotes_when_possible(helper_module, value, expected) -> None:
    assert helper_module.escape_xpath_attr(value) == expected


def test_normalize_applies_nfc_only_to_strings(helper_module) -> None:
    assert helper_module.normalize("Cafe\u0301") == "Caf\xe9"
    sentinel = object()
    assert helper_module.normalize(sentinel) is sentinel
