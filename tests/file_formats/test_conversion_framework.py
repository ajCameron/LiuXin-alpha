"""
Provide test conversion framework utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test conversion framework through a consuming regression::

        python -m pytest -q tests/file_formats/test_conversion_framework.py
"""
from __future__ import annotations

import pytest

from tests.support.file_format_conversion import (
    TEXT_OUTPUT_MATRIX_CASES,
    assert_newline_style,
    assert_text_output_matrix_case,
    conversion_case_ids,
)


def test_text_output_matrix_case_ids_are_stable() -> None:
    """
    Perform the test text output matrix case ids are stable operation under explicit file-format and conversion rules.

    Example:
        Exercise test text output matrix case ids are stable through a consuming regression::

            python -m pytest -q tests/file_formats/test_conversion_framework.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    assert conversion_case_ids(TEXT_OUTPUT_MATRIX_CASES) == (
        "utf_8_unix",
        "utf_8_sig_windows",
        "utf_16_native_old_mac",
        "utf_16_le_windows",
        "utf_16_be_unix",
    )


@pytest.mark.parametrize("case", TEXT_OUTPUT_MATRIX_CASES, ids=lambda case: case.case_id)
def test_text_output_matrix_cases_decode_and_validate_newlines(case) -> None:
    """
    Perform the test text output matrix cases decode and validate newlines operation under explicit file-format and conversion rules.

    Example:
        Exercise test text output matrix cases decode and validate newlines through a consuming regression::

            python -m pytest -q tests/file_formats/test_conversion_framework.py


    :param case: Value supplied for case under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    source = f"Line one {case.case_id}\nLine two café Ω"
    payload = source.replace("\n", case.expected_newline).encode(case.encoding)

    rendered = assert_text_output_matrix_case(payload, case, ("café", "Ω"))

    assert "Line one" in rendered


def test_newline_style_assertion_rejects_mixed_styles() -> None:
    """
    Perform the test newline style assertion rejects mixed styles operation under explicit file-format and conversion rules.

    Example:
        Exercise test newline style assertion rejects mixed styles through a consuming regression::

            python -m pytest -q tests/file_formats/test_conversion_framework.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(AssertionError, match="mixed newline"):
        assert_newline_style("A\r\nB\nC", "\r\n", context="mixed")
