"""
Provide test pdb malformed hostile utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test pdb malformed hostile through a consuming regression::

        python -m pytest -q tests/file_formats/pdb/test_pdb_malformed_hostile.py
"""
from __future__ import annotations

import pytest

from LiuXin_alpha.file_formats.pdb import PDBError
from tests.support.file_format_pdb import (
    PALMDB_HEADER_SIZE,
    PALMDB_RECORD_TABLE_ENTRY_SIZE,
    build_minimal_palmdoc_pdb,
    pdb_record_offsets,
    pdb_stream,
    rewrite_pdb_record_offset,
    rewrite_pdb_record_offsets,
    truncate_pdb_payload,
)


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"\0" * 60,
        b"\0" * 77,
    ],
)
def test_pdb_header_reader_rejects_truncated_headers_as_pdb_error(payload: bytes) -> None:
    """
    Perform the test pdb header reader rejects truncated headers as pdb error operation under explicit file-format and conversion rules.

    Example:
        Exercise test pdb header reader rejects truncated headers as pdb error through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_malformed_hostile.py


    :param payload: Value supplied for payload under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.pdb.header import PdbHeaderReader

    with pytest.raises(PDBError):
        PdbHeaderReader(pdb_stream(payload))


def test_pdb_header_reader_rejects_short_record_table_as_pdb_error() -> None:
    """
    Perform the test pdb header reader rejects short record table as pdb error operation under explicit file-format and conversion rules.

    Example:
        Exercise test pdb header reader rejects short record table as pdb error through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_malformed_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.pdb.header import PdbHeaderReader

    payload = build_minimal_palmdoc_pdb()
    short_table = truncate_pdb_payload(
        payload,
        PALMDB_HEADER_SIZE + PALMDB_RECORD_TABLE_ENTRY_SIZE,
    )

    with pytest.raises(PDBError):
        PdbHeaderReader(pdb_stream(short_table))


@pytest.mark.parametrize("mutator", ["inside_table", "duplicate", "reversed", "out_of_file"])
def test_pdb_header_reader_rejects_invalid_record_offsets_as_pdb_error(mutator: str) -> None:
    """
    Perform the test pdb header reader rejects invalid record offsets as pdb error operation under explicit file-format and conversion rules.

    Example:
        Exercise test pdb header reader rejects invalid record offsets as pdb error through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_malformed_hostile.py


    :param mutator: Value supplied for mutator under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.pdb.header import PdbHeaderReader

    payload = build_minimal_palmdoc_pdb()
    offsets = pdb_record_offsets(payload)
    if mutator == "inside_table":
        hostile = rewrite_pdb_record_offset(payload, 0, PALMDB_HEADER_SIZE)
    elif mutator == "duplicate":
        hostile = rewrite_pdb_record_offset(payload, 1, offsets[0])
    elif mutator == "reversed":
        hostile = rewrite_pdb_record_offsets(payload, list(reversed(offsets)))
    else:
        hostile = rewrite_pdb_record_offset(payload, 1, len(payload) + 100)

    with pytest.raises(PDBError):
        PdbHeaderReader(pdb_stream(hostile))


@pytest.mark.parametrize("method_name", ["section_data", "section_offset", "full_section_info"])
def test_pdb_header_reader_rejects_out_of_range_section_access_as_pdb_error(method_name: str) -> None:
    """
    Perform the test pdb header reader rejects out of range section access as pdb error operation under explicit file-format and conversion rules.

    Example:
        Exercise test pdb header reader rejects out of range section access as pdb error through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_malformed_hostile.py


    :param method_name: Value supplied for method name under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.pdb.header import PdbHeaderReader

    header = PdbHeaderReader(pdb_stream(build_minimal_palmdoc_pdb()))

    with pytest.raises(PDBError):
        getattr(header, method_name)(99)


def test_pdb_metadata_reader_keeps_strict_default_for_malformed_wrapper() -> None:
    """
    Perform the test pdb metadata reader keeps strict default for malformed wrapper operation under explicit file-format and conversion rules.

    Example:
        Exercise test pdb metadata reader keeps strict default for malformed wrapper through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_malformed_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.metadata.file_sources.pdb import PdbFormatError, get_metadata

    payload = truncate_pdb_payload(build_minimal_palmdoc_pdb(), 77)

    with pytest.raises(PdbFormatError):
        get_metadata(pdb_stream(payload), extract_cover=False)
