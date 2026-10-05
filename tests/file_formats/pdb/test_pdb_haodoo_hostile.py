"""
Provide test pdb haodoo hostile utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test pdb haodoo hostile through a consuming regression::

        python -m pytest -q tests/file_formats/pdb/test_pdb_haodoo_hostile.py
"""
from __future__ import annotations

import pytest

from LiuXin_alpha.file_formats.pdb import PDBError
from tests.support.file_format_pdb import (
    PdbLog,
    build_haodoo_legacy_header_record,
    build_haodoo_unicode_header_record,
    build_minimal_haodoo_pdb,
    build_pdb,
    pdb_input_options,
    pdb_stream,
)


def _pdb_header(payload: bytes):
    """
    Perform the pdb header operation under explicit file-format and conversion rules.

    Example:
        Exercise  pdb header through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_haodoo_hostile.py


    :param payload: Value supplied for payload under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.file_formats.pdb.header import PdbHeaderReader

    return PdbHeaderReader(pdb_stream(payload))


def _reader(payload: bytes):
    """
    Perform the reader operation under explicit file-format and conversion rules.

    Example:
        Exercise  reader through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_haodoo_hostile.py


    :param payload: Value supplied for payload under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.file_formats.pdb.haodoo.reader import Reader

    return Reader(_pdb_header(payload), pdb_stream(payload), PdbLog(), pdb_input_options())


@pytest.mark.parametrize(
    ("ident", "record0"),
    (
        ("BOOKMTIT", b"only a title"),
        ("BOOKMTIU", "only a title".encode("utf_16_le")),
    ),
)
def test_haodoo_reader_rejects_missing_header_fields(ident: str, record0: bytes) -> None:
    """
    Perform the test haodoo reader rejects missing header fields operation under explicit file-format and conversion rules.

    Example:
        Exercise test haodoo reader rejects missing header fields through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_haodoo_hostile.py


    :param ident: Value supplied for ident under the utility contract.
    :param record0: Value supplied for record0 under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    payload = build_pdb([record0], title="Bad Haodoo Header", ident=ident)

    with pytest.raises(PDBError, match="header"):
        _reader(payload)


@pytest.mark.parametrize(
    ("ident", "record0"),
    (
        ("BOOKMTIT", b"Title\x1bnot-a-count\x1bChapter"),
        (
            "BOOKMTIU",
            "Title".encode("utf_16_le")
            + b"\x1b\x00not-a-count\x1b\x00"
            + "Chapter".encode("utf_16_le"),
        ),
    ),
)
def test_haodoo_reader_rejects_non_integer_record_count(ident: str, record0: bytes) -> None:
    """
    Perform the test haodoo reader rejects non integer record count operation under explicit file-format and conversion rules.

    Example:
        Exercise test haodoo reader rejects non integer record count through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_haodoo_hostile.py


    :param ident: Value supplied for ident under the utility contract.
    :param record0: Value supplied for record0 under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    payload = build_pdb([record0], title="Bad Haodoo Count", ident=ident)

    with pytest.raises(PDBError, match="record count"):
        _reader(payload)


@pytest.mark.parametrize(
    ("ident", "record0"),
    (
        (
            "BOOKMTIT",
            build_haodoo_legacy_header_record(
                title="Legacy",
                record_count=2,
                chapter_titles=("Only One",),
            ),
        ),
        (
            "BOOKMTIU",
            build_haodoo_unicode_header_record(
                title="Unicode",
                record_count=2,
                chapter_titles=("Only One",),
            ),
        ),
    ),
)
def test_haodoo_reader_rejects_chapter_title_count_mismatch(ident: str, record0: bytes) -> None:
    """
    Perform the test haodoo reader rejects chapter title count mismatch operation under explicit file-format and conversion rules.

    Example:
        Exercise test haodoo reader rejects chapter title count mismatch through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_haodoo_hostile.py


    :param ident: Value supplied for ident under the utility contract.
    :param record0: Value supplied for record0 under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    payload = build_pdb([record0, b"body"], title="Bad Haodoo Titles", ident=ident)

    with pytest.raises(PDBError, match="chapter title"):
        _reader(payload)


@pytest.mark.parametrize(
    ("ident", "record0"),
    (
        (
            "BOOKMTIT",
            build_haodoo_legacy_header_record(
                title="Legacy",
                record_count=2,
                chapter_titles=("One", "Two"),
            ),
        ),
        (
            "BOOKMTIU",
            build_haodoo_unicode_header_record(
                title="Unicode",
                record_count=2,
                chapter_titles=("One", "Two"),
            ),
        ),
    ),
)
def test_haodoo_reader_rejects_declared_chapters_outside_sections(ident: str, record0: bytes) -> None:
    """
    Perform the test haodoo reader rejects declared chapters outside sections operation under explicit file-format and conversion rules.

    Example:
        Exercise test haodoo reader rejects declared chapters outside sections through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_haodoo_hostile.py


    :param ident: Value supplied for ident under the utility contract.
    :param record0: Value supplied for record0 under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    payload = build_pdb([record0, b"one body section"], title="Bad Haodoo Sections", ident=ident)

    with pytest.raises(PDBError, match="chapter records"):
        _reader(payload)


def test_haodoo_reader_rejects_direct_out_of_range_sections() -> None:
    """
    Perform the test haodoo reader rejects direct out of range sections operation under explicit file-format and conversion rules.

    Example:
        Exercise test haodoo reader rejects direct out of range sections through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_haodoo_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    payload = build_minimal_haodoo_pdb(
        book_title="Direct Sections",
        chapter_title="Chapter",
        body_text="Chapter\nBody",
    )
    reader = _reader(payload)

    for bad_section in (-1, 2):
        with pytest.raises(PDBError):
            reader.section_data(bad_section)

    with pytest.raises(PDBError):
        reader.decompress_text(2)
