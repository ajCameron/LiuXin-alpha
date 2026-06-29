from __future__ import annotations

import pytest

from tests.support.file_format_conversion import (
    TEXT_OUTPUT_MATRIX_CASES,
    assert_newline_style,
    assert_text_output_matrix_case,
    conversion_case_ids,
)


def test_text_output_matrix_case_ids_are_stable() -> None:
    assert conversion_case_ids(TEXT_OUTPUT_MATRIX_CASES) == (
        "utf_8_unix",
        "utf_8_sig_windows",
        "utf_16_native_old_mac",
        "utf_16_le_windows",
        "utf_16_be_unix",
    )


@pytest.mark.parametrize("case", TEXT_OUTPUT_MATRIX_CASES, ids=lambda case: case.case_id)
def test_text_output_matrix_cases_decode_and_validate_newlines(case) -> None:
    source = f"Line one {case.case_id}\nLine two café Ω"
    payload = source.replace("\n", case.expected_newline).encode(case.encoding)

    rendered = assert_text_output_matrix_case(payload, case, ("café", "Ω"))

    assert "Line one" in rendered


def test_newline_style_assertion_rejects_mixed_styles() -> None:
    with pytest.raises(AssertionError, match="mixed newline"):
        assert_newline_style("A\r\nB\nC", "\r\n", context="mixed")
