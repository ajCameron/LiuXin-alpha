"""
Provide test lit malformed hostile utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test lit malformed hostile through a consuming regression::

        python -m pytest -q tests/file_formats/lit/test_lit_malformed_hostile.py
"""
from __future__ import annotations

from struct import pack

import pytest

from LiuXin_alpha.file_formats.lit import LitError
from LiuXin_alpha.file_formats.lit.reader import (
    LitFile,
    encint,
    read_utf8_char,
)
from tests.support.file_format_lit import (
    LitLog,
    build_lit_header_payload,
    build_lit_namelist_payload,
    build_lit_secondary_header,
    in_memory_lit_file,
    lit_binary_element,
    lit_manifest_item,
    lit_stream,
    read_manifest_from_payload,
    read_namelist_from_payload,
    render_unbinary_html,
)


def test_lit_file_rejects_wrong_magic_as_lit_error() -> None:
    """
    Perform the test lit file rejects wrong magic as lit error operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit file rejects wrong magic as lit error through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_malformed_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(LitError, match="Not a valid LIT file"):
        LitFile(lit_stream(b"not-lit!"), LitLog())


def test_lit_file_reports_truncated_primary_header_as_lit_error() -> None:
    """
    Perform the test lit file reports truncated primary header as lit error operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit file reports truncated primary header as lit error through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_malformed_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(LitError, match="Truncated"):
        LitFile(lit_stream(b"ITOLITLS"), LitLog())


def test_lit_file_reports_truncated_secondary_header_as_lit_error() -> None:
    """
    Perform the test lit file reports truncated secondary header as lit error operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit file reports truncated secondary header as lit error through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_malformed_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    payload = build_lit_header_payload(sec_hdr_len=4, secondary_header=b"\0\0\0\0")

    with pytest.raises(LitError, match="secondary header|Truncated"):
        LitFile(lit_stream(payload), LitLog())


def test_lit_file_rejects_unknown_secondary_header_block_without_hanging() -> None:
    """
    Perform the test lit file rejects unknown secondary header block without hanging operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit file rejects unknown secondary header block without hanging through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_malformed_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    secondary = build_lit_secondary_header(b"NOPE" + pack("<I", 1) + (b"\0" * 40))
    payload = build_lit_header_payload(secondary_header=secondary)

    with pytest.raises(LitError, match="secondary header block"):
        LitFile(lit_stream(payload), LitLog())


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"\x01\\\x00",  # root plus partial file-count field
        b"\x01\\" + pack("<I", 1) + b"\0",  # file count but truncated entry
    ],
)
def test_lit_manifest_truncated_payloads_raise_lit_error(payload: bytes) -> None:
    """
    Perform the test lit manifest truncated payloads raise lit error operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit manifest truncated payloads raise lit error through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_malformed_hostile.py


    :param payload: Value supplied for payload under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(LitError, match="manifest|Truncated|Invalid UTF8"):
        read_manifest_from_payload(payload)


def test_lit_manifest_rejects_invalid_utf8_inside_sized_strings() -> None:
    """
    Perform the test lit manifest rejects invalid utf8 inside sized strings operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit manifest rejects invalid utf8 inside sized strings through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_malformed_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    payload = (
        b"\x01\\"
        + pack("<I", 1)
        + pack("<I", 0)
        + b"\x01\xff"
        + b"\x00"
        + b"\x00"
        + (pack("<I", 0) * 3)
        + b"\0"
    )

    with pytest.raises(LitError, match="Invalid UTF8"):
        read_manifest_from_payload(payload)


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        pack("<HH", 0x3C, 1) + b"\x01",  # truncated section-name length field
        build_lit_namelist_payload(("Uncompressed",))[:-1],
    ],
)
def test_lit_namelist_truncated_payloads_raise_lit_error(payload: bytes) -> None:
    """
    Perform the test lit namelist truncated payloads raise lit error operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit namelist truncated payloads raise lit error through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_malformed_hostile.py


    :param payload: Value supplied for payload under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(LitError, match="Namelist|Truncated"):
        read_namelist_from_payload(payload)


@pytest.mark.parametrize(
    "payload",
    [
        b"\x80",
        b"\x81",
        b"\x81\x80",
    ],
)
def test_lit_encoded_integer_rejects_unterminated_values(payload: bytes) -> None:
    """
    Perform the test lit encoded integer rejects unterminated values operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit encoded integer rejects unterminated values through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_malformed_hostile.py


    :param payload: Value supplied for payload under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(LitError, match="encoded integer"):
        encint(payload, len(payload))


@pytest.mark.parametrize(
    "payload",
    [
        b"\0",
        b"\0\x01",
        lit_binary_element("p", attrs={"id": "broken"})[:-1],
    ],
)
def test_lit_unbinary_rejects_truncated_control_sequences(payload: bytes) -> None:
    """
    Perform the test lit unbinary rejects truncated control sequences operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit unbinary rejects truncated control sequences through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_malformed_hostile.py


    :param payload: Value supplied for payload under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(LitError, match="binary markup|UTF8"):
        render_unbinary_html(payload)


def test_lit_unbinary_rejects_invalid_utf8_text_inside_controlled_element() -> None:
    """
    Perform the test lit unbinary rejects invalid utf8 text inside controlled element operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit unbinary rejects invalid utf8 text inside controlled element through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_malformed_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    payload = lit_binary_element("p", "Valid Καλημέρα ".encode("utf-8") + b"\xff")

    with pytest.raises(LitError, match="Invalid UTF8"):
        render_unbinary_html(payload)


def test_lit_atoms_reject_truncated_atom_table_header() -> None:
    """
    Perform the test lit atoms reject truncated atom table header operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit atoms reject truncated atom table header through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_malformed_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    lit = in_memory_lit_file({"/data/chapter/atom": b"\x01"})
    entry = lit_manifest_item(internal="chapter", original="chapter.xhtml")

    with pytest.raises(LitError, match="Truncated"):
        lit.get_atoms(entry)


@pytest.mark.parametrize("payload", [b"", b"\xf4\x90\x80\x80"])
def test_lit_utf8_reader_keeps_hostile_byte_errors_reportable(payload: bytes) -> None:
    """
    Perform the test lit utf8 reader keeps hostile byte errors reportable operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit utf8 reader keeps hostile byte errors reportable through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_malformed_hostile.py


    :param payload: Value supplied for payload under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(LitError, match="Invalid UTF8"):
        read_utf8_char(payload, 0)
