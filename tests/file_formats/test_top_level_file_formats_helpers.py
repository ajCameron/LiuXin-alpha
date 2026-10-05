"""
Provide test top level file formats helpers utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test top level file formats helpers through a consuming regression::

        python -m pytest -q tests/file_formats/test_top_level_file_formats_helpers.py
"""
from __future__ import annotations

from io import BytesIO

import pytest

import LiuXin_alpha.file_formats as ff
from LiuXin_alpha.file_formats import tweak


def test_check_ebook_format_detects_tpz_marker() -> None:
    """
    Perform the test check ebook format detects tpz marker operation under explicit file-format and conversion rules.

    Example:
        Exercise test check ebook format detects tpz marker through a consuming regression::

            python -m pytest -q tests/file_formats/test_top_level_file_formats_helpers.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    stream = BytesIO(b"TPZx")
    assert ff.check_ebook_format(stream, "mobi") == "tpz"
    assert stream.tell() == 0


def test_check_ebook_format_leaves_other_formats_unchanged() -> None:
    """
    Perform the test check ebook format leaves other formats unchanged operation under explicit file-format and conversion rules.

    Example:
        Exercise test check ebook format leaves other formats unchanged through a consuming regression::

            python -m pytest -q tests/file_formats/test_top_level_file_formats_helpers.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test unit convert handles common units operation under explicit file-format and conversion rules.

    Example:
        Exercise test unit convert handles common units through a consuming regression::

            python -m pytest -q tests/file_formats/test_top_level_file_formats_helpers.py


    :param value: Value normalized, stored, formatted or returned.
    :param expected: Value supplied for expected under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    assert ff.unit_convert(value, base=12, font=10, dpi=96, body_font_size=12) == pytest.approx(expected)


def test_unit_convert_returns_original_for_unparseable_values() -> None:
    """
    Perform the test unit convert returns original for unparseable values operation under explicit file-format and conversion rules.

    Example:
        Exercise test unit convert returns original for unparseable values through a consuming regression::

            python -m pytest -q tests/file_formats/test_top_level_file_formats_helpers.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    assert ff.unit_convert("not-a-length", base=1, font=1, dpi=96) == "not-a-length"


def test_parse_css_length_and_escape_xpath_attr_corner_cases() -> None:
    """
    Perform the test parse css length and escape xpath attr corner cases operation under explicit file-format and conversion rules.

    Example:
        Exercise test parse css length and escape xpath attr corner cases through a consuming regression::

            python -m pytest -q tests/file_formats/test_top_level_file_formats_helpers.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    assert ff.parse_css_length("2.5em") == (2.5, "em")
    assert ff.parse_css_length(None) == (None, None)

    expr = ff.escape_xpath_attr('he said "it\'s ok"')
    assert expr.startswith("concat(")


def test_tweak_get_tools_for_supported_and_unsupported_formats() -> None:
    """
    Perform the test tweak get tools for supported and unsupported formats operation under explicit file-format and conversion rules.

    Example:
        Exercise test tweak get tools for supported and unsupported formats through a consuming regression::

            python -m pytest -q tests/file_formats/test_top_level_file_formats_helpers.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    exploder, rebuilder = tweak.get_tools("epub")
    assert callable(exploder)
    assert callable(rebuilder)

    exploder, rebuilder = tweak.get_tools("unknown")
    assert exploder is None
    assert rebuilder is None

