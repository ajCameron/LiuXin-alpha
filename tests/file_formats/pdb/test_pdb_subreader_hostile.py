"""
Provide test pdb subreader hostile utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test pdb subreader hostile through a consuming regression::

        python -m pytest -q tests/file_formats/pdb/test_pdb_subreader_hostile.py
"""
from __future__ import annotations

import pytest

from LiuXin_alpha.file_formats.pdb import PDBError
from LiuXin_alpha.file_formats.pdb.ztxt import zTXTError
from tests.support.file_format_pdb import (
    PdbLog,
    build_palmdoc_header_record,
    build_pdb,
    build_ztxt_header_record,
    pdb_input_options,
    pdb_stream,
    ztxt_compressed_records,
)


def _pdb_header(payload: bytes):
    """
    Perform the pdb header operation under explicit file-format and conversion rules.

    Example:
        Exercise  pdb header through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_subreader_hostile.py


    :param payload: Value supplied for payload under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.file_formats.pdb.header import PdbHeaderReader

    return PdbHeaderReader(pdb_stream(payload))


def test_palmdoc_reader_rejects_short_record0_as_pdb_error() -> None:
    """
    Perform the test palmdoc reader rejects short record0 as pdb error operation under explicit file-format and conversion rules.

    Example:
        Exercise test palmdoc reader rejects short record0 as pdb error through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_subreader_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.pdb.palmdoc.reader import Reader

    payload = build_pdb([b"\0"], title="Short PalmDOC", ident="TEXtREAd")

    with pytest.raises(PDBError):
        Reader(_pdb_header(payload), pdb_stream(payload), PdbLog(), pdb_input_options())


def test_palmdoc_reader_rejects_declared_text_records_outside_sections() -> None:
    """
    Perform the test palmdoc reader rejects declared text records outside sections operation under explicit file-format and conversion rules.

    Example:
        Exercise test palmdoc reader rejects declared text records outside sections through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_subreader_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.pdb.palmdoc.reader import Reader

    record0 = build_palmdoc_header_record(text_length=4, record_count=2, compression=1)
    payload = build_pdb([record0, b"body"], title="Bad PalmDOC Count", ident="TEXtREAd")

    with pytest.raises(PDBError):
        Reader(_pdb_header(payload), pdb_stream(payload), PdbLog(), pdb_input_options())


def test_palmdoc_reader_rejects_unknown_compression_as_pdb_error() -> None:
    """
    Perform the test palmdoc reader rejects unknown compression as pdb error operation under explicit file-format and conversion rules.

    Example:
        Exercise test palmdoc reader rejects unknown compression as pdb error through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_subreader_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.pdb.palmdoc.reader import Reader

    record0 = build_palmdoc_header_record(text_length=4, record_count=1, compression=999)
    payload = build_pdb([record0, b"body"], title="Bad PalmDOC Compression", ident="TEXtREAd")

    with pytest.raises(PDBError):
        Reader(_pdb_header(payload), pdb_stream(payload), PdbLog(), pdb_input_options())


def test_palmdoc_reader_wraps_decompression_failures(monkeypatch) -> None:
    """
    Perform the test palmdoc reader wraps decompression failures operation under explicit file-format and conversion rules.

    Example:
        Exercise test palmdoc reader wraps decompression failures through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_subreader_hostile.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.compression import palmdoc as palmdoc_compression
    from LiuXin_alpha.file_formats.pdb.palmdoc.reader import Reader

    def fail_decompress(_payload):
        """
        Perform the fail decompress operation under explicit file-format and conversion rules.

        Example:
            Exercise test palmdoc reader wraps decompression failures.fail decompress through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_subreader_hostile.py


        :param _payload: Value supplied for payload under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise RuntimeError("synthetic decompressor failure")

    monkeypatch.setattr(palmdoc_compression, "decompress_doc", fail_decompress)
    record0 = build_palmdoc_header_record(text_length=4, record_count=1, compression=2)
    payload = build_pdb([record0, b"bad"], title="Bad PalmDOC Deflate", ident="TEXtREAd")
    reader = Reader(_pdb_header(payload), pdb_stream(payload), PdbLog(), pdb_input_options())

    with pytest.raises(PDBError):
        reader.decompress_text(1)


def test_palmdoc_reader_rejects_direct_out_of_range_sections() -> None:
    """
    Perform the test palmdoc reader rejects direct out of range sections operation under explicit file-format and conversion rules.

    Example:
        Exercise test palmdoc reader rejects direct out of range sections through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_subreader_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.pdb.palmdoc.reader import Reader

    record0 = build_palmdoc_header_record(text_length=4, record_count=1, compression=1)
    payload = build_pdb([record0, b"body"], title="PalmDOC Sections", ident="TEXtREAd")
    reader = Reader(_pdb_header(payload), pdb_stream(payload), PdbLog(), pdb_input_options())

    for bad_section in (-1, 2):
        with pytest.raises(PDBError):
            reader.section_data(bad_section)

    with pytest.raises(PDBError):
        reader.decompress_text(2)


def test_ztxt_reader_rejects_short_record0_as_ztxt_error() -> None:
    """
    Perform the test ztxt reader rejects short record0 as ztxt error operation under explicit file-format and conversion rules.

    Example:
        Exercise test ztxt reader rejects short record0 as ztxt error through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_subreader_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.pdb.ztxt.reader import Reader

    payload = build_pdb([b"\0"], title="Short zTXT", ident="zTXTGPlm")

    with pytest.raises(zTXTError):
        Reader(_pdb_header(payload), pdb_stream(payload), PdbLog(), pdb_input_options())


def test_ztxt_reader_rejects_declared_text_records_outside_sections() -> None:
    """
    Perform the test ztxt reader rejects declared text records outside sections operation under explicit file-format and conversion rules.

    Example:
        Exercise test ztxt reader rejects declared text records outside sections through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_subreader_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.pdb.ztxt.reader import Reader

    compressed, total_length, crc32 = ztxt_compressed_records([b"body"])
    record0 = build_ztxt_header_record(
        text_length=total_length,
        record_count=2,
        crc32=crc32,
    )
    payload = build_pdb([record0, *compressed], title="Bad zTXT Count", ident="zTXTGPlm")

    with pytest.raises(zTXTError):
        Reader(_pdb_header(payload), pdb_stream(payload), PdbLog(), pdb_input_options())


@pytest.mark.parametrize(
    "record0",
    [
        build_ztxt_header_record(text_length=4, record_count=1, version=0x0100),
        build_ztxt_header_record(text_length=4, record_count=1, flags=0),
    ],
)
def test_ztxt_reader_keeps_unsupported_version_and_flags_named(record0: bytes) -> None:
    """
    Perform the test ztxt reader keeps unsupported version and flags named operation under explicit file-format and conversion rules.

    Example:
        Exercise test ztxt reader keeps unsupported version and flags named through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_subreader_hostile.py


    :param record0: Value supplied for record0 under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.pdb.ztxt.reader import Reader

    compressed, _total_length, _crc32 = ztxt_compressed_records([b"body"])
    payload = build_pdb([record0, *compressed], title="Unsupported zTXT", ident="zTXTGPlm")

    with pytest.raises(zTXTError):
        Reader(_pdb_header(payload), pdb_stream(payload), PdbLog(), pdb_input_options())


def test_ztxt_reader_wraps_malformed_zlib_section_as_ztxt_error() -> None:
    """
    Perform the test ztxt reader wraps malformed zlib section as ztxt error operation under explicit file-format and conversion rules.

    Example:
        Exercise test ztxt reader wraps malformed zlib section as ztxt error through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_subreader_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.pdb.ztxt.reader import Reader

    record0 = build_ztxt_header_record(text_length=8, record_count=1, crc32=0)
    payload = build_pdb([record0, b"not-zlib"], title="Bad zTXT Deflate", ident="zTXTGPlm")

    with pytest.raises(zTXTError):
        Reader(_pdb_header(payload), pdb_stream(payload), PdbLog(), pdb_input_options())


def test_ztxt_reader_wraps_later_malformed_zlib_sections_as_ztxt_error() -> None:
    """
    Perform the test ztxt reader wraps later malformed zlib sections as ztxt error operation under explicit file-format and conversion rules.

    Example:
        Exercise test ztxt reader wraps later malformed zlib sections as ztxt error through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_subreader_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.pdb.ztxt.reader import Reader

    compressed, total_length, _crc32 = ztxt_compressed_records([b"body"])
    record0 = build_ztxt_header_record(text_length=total_length, record_count=2, crc32=0)
    payload = build_pdb(
        [record0, compressed[0], b"not-zlib"],
        title="Bad Later zTXT Deflate",
        ident="zTXTGPlm",
    )
    reader = Reader(_pdb_header(payload), pdb_stream(payload), PdbLog(), pdb_input_options())

    with pytest.raises(zTXTError):
        reader.decompress_text(2)


def test_ztxt_reader_rejects_direct_out_of_range_sections() -> None:
    """
    Perform the test ztxt reader rejects direct out of range sections operation under explicit file-format and conversion rules.

    Example:
        Exercise test ztxt reader rejects direct out of range sections through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_subreader_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.pdb.ztxt.reader import Reader

    compressed, total_length, crc32 = ztxt_compressed_records([b"body"])
    record0 = build_ztxt_header_record(text_length=total_length, record_count=1, crc32=crc32)
    payload = build_pdb([record0, *compressed], title="zTXT Sections", ident="zTXTGPlm")
    reader = Reader(_pdb_header(payload), pdb_stream(payload), PdbLog(), pdb_input_options())

    for bad_section in (-1, 2):
        with pytest.raises(zTXTError):
            reader.section_data(bad_section)

    with pytest.raises(zTXTError):
        reader.decompress_text(2)
