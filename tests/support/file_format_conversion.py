from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence


@dataclass(frozen=True)
class TextOutputMatrixCase:
    case_id: str
    encoding: str
    newline_option: str
    expected_newline: str
    description: str = ""


TEXT_OUTPUT_MATRIX_CASES: tuple[TextOutputMatrixCase, ...] = (
    TextOutputMatrixCase("utf_8_unix", "utf-8", "unix", "\n"),
    TextOutputMatrixCase("utf_8_sig_windows", "utf-8-sig", "windows", "\r\n"),
    TextOutputMatrixCase("utf_16_native_old_mac", "utf-16", "old_mac", "\r"),
    TextOutputMatrixCase("utf_16_le_windows", "utf-16-le", "windows", "\r\n"),
    TextOutputMatrixCase("utf_16_be_unix", "utf-16-be", "unix", "\n"),
)


def conversion_case_ids(cases: Iterable[TextOutputMatrixCase]) -> tuple[str, ...]:
    return tuple(case.case_id for case in cases)


def decode_text_output(payload: bytes, case: TextOutputMatrixCase) -> str:
    return payload.decode(case.encoding, "strict")


def assert_newline_style(text: str, expected_newline: str, *, context: str = "") -> None:
    detail = f" for {context}" if context else ""
    if expected_newline == "\n":
        if "\r" in text:
            raise AssertionError(f"unexpected carriage return in unix-newline text{detail}")
        return

    if expected_newline == "\r":
        if "\n" in text:
            raise AssertionError(f"unexpected line feed in old-mac-newline text{detail}")
        return

    if expected_newline == "\r\n":
        remainder = text.replace("\r\n", "")
        if "\r" in remainder or "\n" in remainder:
            raise AssertionError(f"mixed newline styles in windows-newline text{detail}")
        return

    raise ValueError(f"unsupported expected newline: {expected_newline!r}")


def assert_text_output_matrix_case(
    payload: bytes,
    case: TextOutputMatrixCase,
    fragments: Sequence[str],
) -> str:
    rendered = decode_text_output(payload, case)
    missing = [fragment for fragment in fragments if fragment not in rendered]
    if missing:
        raise AssertionError(f"missing converted fragments for {case.case_id}: {missing!r}")
    if "\ufffd" in rendered:
        raise AssertionError(f"unexpected replacement character for {case.case_id}")
    assert_newline_style(rendered, case.expected_newline, context=case.case_id)
    return rendered
