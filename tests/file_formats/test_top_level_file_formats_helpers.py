from __future__ import annotations

from io import BytesIO

import pytest

import LiuXin_alpha.file_formats as ff
from LiuXin_alpha.file_formats import tweak


def test_check_ebook_format_detects_tpz_marker() -> None:
    stream = BytesIO(b"TPZx")
    assert ff.check_ebook_format(stream, "mobi") == "tpz"
    assert stream.tell() == 0


def test_check_ebook_format_leaves_other_formats_unchanged() -> None:
    stream = BytesIO(b"TPZx")
    assert ff.check_ebook_format(stream, "epub") == "epub"
    assert stream.tell() == 0


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("1in", 72.0),
        ("25.4mm", 72.0),
        ("2.54cm", 72.0),
        ("2rem", 24.0),
        ("150%", 18.0),
    ],
)
def test_unit_convert_handles_common_units(value: str, expected: float) -> None:
    assert ff.unit_convert(value, base=12, font=10, dpi=96, body_font_size=12) == pytest.approx(expected)


def test_unit_convert_returns_original_for_unparseable_values() -> None:
    assert ff.unit_convert("not-a-length", base=1, font=1, dpi=96) == "not-a-length"


def test_parse_css_length_and_escape_xpath_attr_corner_cases() -> None:
    assert ff.parse_css_length("2.5em") == (2.5, "em")
    assert ff.parse_css_length(None) == (None, None)

    expr = ff.escape_xpath_attr('he said "it\'s ok"')
    assert expr.startswith("concat(")


def test_tweak_get_tools_for_supported_and_unsupported_formats() -> None:
    exploder, rebuilder = tweak.get_tools("epub")
    assert callable(exploder)
    assert callable(rebuilder)

    exploder, rebuilder = tweak.get_tools("unknown")
    assert exploder is None
    assert rebuilder is None

