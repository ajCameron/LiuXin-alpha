from __future__ import annotations

from struct import pack

import pytest

from tests.support.file_format_pdb import (
    PdbLog,
    build_pdb,
    build_plucker_composite_image_section,
    build_plucker_header_record,
    build_plucker_record,
    build_plucker_section,
    build_plucker_text_section,
    pdb_input_options,
    pdb_stream,
)


def _pdb_header(payload: bytes):
    from LiuXin_alpha.file_formats.pdb.header import PdbHeaderReader

    return PdbHeaderReader(pdb_stream(payload))


def _reader(payload: bytes):
    from LiuXin_alpha.file_formats.pdb.plucker.reader import Reader

    return Reader(_pdb_header(payload), pdb_stream(payload), PdbLog(), pdb_input_options(input_encoding="utf-8"))


def test_plucker_reader_rejects_short_record0_as_plucker_error() -> None:
    from LiuXin_alpha.file_formats.pdb.plucker import PluckerError

    payload = build_pdb([b"\0"], title="Short Plucker", ident="DataPlkr")

    with pytest.raises(PluckerError):
        _reader(payload)


def test_plucker_reader_rejects_record0_reserved_table_overrun() -> None:
    from LiuXin_alpha.file_formats.pdb.plucker import PluckerError

    record0 = pack(">HHH", 1, 2, 2) + pack(">HH", 0, 10)
    payload = build_pdb([record0], title="Short Plucker Map", ident="DataPlkr")

    with pytest.raises(PluckerError):
        _reader(payload)


def test_plucker_reader_rejects_short_section_header() -> None:
    from LiuXin_alpha.file_formats.pdb.plucker import PluckerError

    payload = build_pdb(
        [build_plucker_header_record(records=((0, 10),)), b"short"],
        title="Short Plucker Section",
        ident="DataPlkr",
    )

    with pytest.raises(PluckerError):
        _reader(payload)


def test_plucker_reader_rejects_text_paragraph_table_overrun() -> None:
    from LiuXin_alpha.file_formats.pdb.plucker import PluckerError
    from LiuXin_alpha.file_formats.pdb.plucker.reader import DATATYPE_PHTML

    section = build_plucker_section(
        uid=10,
        datatype=DATATYPE_PHTML,
        paragraphs=1,
        payload=b"\x00\x04",
    )
    payload = build_pdb(
        [build_plucker_header_record(records=((0, 10),)), section],
        title="Bad Plucker Paragraphs",
        ident="DataPlkr",
    )

    with pytest.raises(PluckerError):
        _reader(payload)


def test_plucker_reader_rejects_metadata_declared_size_overrun() -> None:
    from LiuXin_alpha.file_formats.pdb.plucker import PluckerError
    from LiuXin_alpha.file_formats.pdb.plucker.reader import DATATYPE_METADATA

    section = build_plucker_section(
        uid=20,
        datatype=DATATYPE_METADATA,
        payload=pack(">H", 0),
        size=99,
    )
    payload = build_pdb(
        [build_plucker_header_record(records=((0, 10),)), build_plucker_text_section(uid=10, data=b"body"), section],
        title="Bad Plucker Metadata Size",
        ident="DataPlkr",
    )

    with pytest.raises(PluckerError):
        _reader(payload)


def test_plucker_reader_rejects_malformed_metadata_record_length() -> None:
    from LiuXin_alpha.file_formats.pdb.plucker import PluckerError
    from LiuXin_alpha.file_formats.pdb.plucker.reader import DATATYPE_METADATA

    metadata = pack(">H", 1) + pack(">HH", 1, 10) + b"\0"
    section = build_plucker_section(uid=20, datatype=DATATYPE_METADATA, payload=metadata)
    payload = build_pdb(
        [build_plucker_header_record(records=((0, 10),)), build_plucker_text_section(uid=10, data=b"body"), section],
        title="Bad Plucker Metadata Record",
        ident="DataPlkr",
    )

    with pytest.raises(PluckerError):
        _reader(payload)


def test_plucker_reader_rejects_composite_layout_overrun() -> None:
    from LiuXin_alpha.file_formats.pdb.plucker import PluckerError
    from LiuXin_alpha.file_formats.pdb.plucker.reader import DATATYPE_COMPOSITE_IMAGE

    section = build_plucker_section(
        uid=30,
        datatype=DATATYPE_COMPOSITE_IMAGE,
        payload=pack(">HHH", 2, 2, 10),
    )
    payload = build_pdb(
        [build_plucker_header_record(records=((0, 10),)), build_plucker_text_section(uid=10, data=b"body"), section],
        title="Bad Plucker Composite",
        ident="DataPlkr",
    )

    with pytest.raises(PluckerError):
        _reader(payload)


def test_plucker_reader_rejects_composite_missing_image_reference() -> None:
    from LiuXin_alpha.file_formats.pdb.plucker import PluckerError

    section = build_plucker_composite_image_section(uid=30, columns=1, rows=1, image_uids=(99,))
    payload = build_pdb(
        [build_plucker_header_record(records=((0, 10),)), build_plucker_text_section(uid=10, data=b"body"), section],
        title="Missing Plucker Composite Image",
        ident="DataPlkr",
    )

    with pytest.raises(PluckerError):
        _reader(payload)


@pytest.mark.parametrize(
    "phtml",
    [
        b"\0",
        b"\0\x0a\0",
        b"\0\x0c\0\x01\0",
        b"\0\x11",
        b"\0\x1a\0",
        b"\0\x5c\0\0\0",
        b"\0\x97\0\0",
    ],
)
def test_plucker_process_phtml_rejects_truncated_operands(phtml: bytes) -> None:
    from LiuXin_alpha.file_formats.pdb.plucker import PluckerError
    from LiuXin_alpha.file_formats.pdb.plucker.reader import Reader

    reader = Reader.__new__(Reader)
    reader.uid_text_secion_number = {}
    reader.uid_image_section_number = {}
    reader.uid_composite_image_section_number = {}

    with pytest.raises(PluckerError):
        reader.process_phtml(phtml)


def test_plucker_process_phtml_rejects_missing_image_reference() -> None:
    from LiuXin_alpha.file_formats.pdb.plucker import PluckerError
    from LiuXin_alpha.file_formats.pdb.plucker.reader import Reader

    reader = Reader.__new__(Reader)
    reader.uid_text_secion_number = {}
    reader.uid_image_section_number = {}
    reader.uid_composite_image_section_number = {}

    with pytest.raises(PluckerError):
        reader.process_phtml(b"\0\x1a\0\x2a")


def test_plucker_decompress_phtml_wraps_bad_payload() -> None:
    from LiuXin_alpha.file_formats.pdb.plucker import PluckerError
    from LiuXin_alpha.file_formats.pdb.plucker.reader import Reader

    reader = Reader.__new__(Reader)
    reader.header_record = type("Header", (), {"compression": 2})()
    reader.owner_id = None

    with pytest.raises(PluckerError):
        reader.decompress_phtml(b"not-zlib")
