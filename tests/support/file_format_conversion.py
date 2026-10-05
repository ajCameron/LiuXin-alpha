"""
Build deterministic CONVERSION fixtures and test doubles.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise file format conversion through a consuming regression::

        python -m pytest -q tests/metadata/file_sources/test_dispatcher_modernized.py
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence


@dataclass(frozen=True)
class TextOutputMatrixCase:
    """
    Carry the deterministic TextOutputMatrixCase inputs and expected values used by format tests.

    Example:
        Exercise TextOutputMatrixCase through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_dispatcher_modernized.py
    """
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
    """
    Perform the conversion case ids step with deterministic fixture inputs.

    Example:
        Exercise conversion case ids through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_dispatcher_modernized.py


    :param cases: Value supplied for cases under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return tuple(case.case_id for case in cases)


def decode_text_output(payload: bytes, case: TextOutputMatrixCase) -> str:
    """
    Decode text output under the fixture contract.

    Example:
        Exercise decode text output through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_dispatcher_modernized.py


    :param payload: Binary or structured payload encoded into the fixture.
    :param case: Value supplied for case under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return payload.decode(case.encoding, "strict")


def assert_newline_style(text: str, expected_newline: str, *, context: str = "") -> None:
    """
    Assert newline style under the fixture contract.

    Example:
        Exercise assert newline style through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_dispatcher_modernized.py


    :param text: Text encoded, parsed or embedded in the fixture.
    :param expected_newline: Value supplied for expected newline under the deterministic
        fixture contract.
    :param context: Value supplied for context under the deterministic fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
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
    """
    Assert text output matrix case under the fixture contract.

    Example:
        Exercise assert text output matrix case through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_dispatcher_modernized.py


    :param payload: Binary or structured payload encoded into the fixture.
    :param case: Value supplied for case under the deterministic fixture contract.
    :param fragments: Value supplied for fragments under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    rendered = decode_text_output(payload, case)
    missing = [fragment for fragment in fragments if fragment not in rendered]
    if missing:
        raise AssertionError(f"missing converted fragments for {case.case_id}: {missing!r}")
    if "\ufffd" in rendered:
        raise AssertionError(f"unexpected replacement character for {case.case_id}")
    assert_newline_style(rendered, case.expected_newline, context=case.case_id)
    return rendered
