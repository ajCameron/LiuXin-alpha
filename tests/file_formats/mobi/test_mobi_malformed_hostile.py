from __future__ import annotations

from struct import pack

import pytest

from LiuXin_alpha.file_formats.mobi import MobiError
from LiuXin_alpha.file_formats.mobi.reader.headers import EXTHHeader, MetadataHeader
from LiuXin_alpha.file_formats.mobi.reader.mobi6 import MobiReader
from tests.support.file_format_mobi import (
    MobiLog,
    build_minimal_mobi,
    build_mobi_exth,
    build_mobi_record0,
    build_palmdb,
    mobi_exth_record,
    mobi_stream,
    palmdb_record_offsets,
    rewrite_palmdb_record_offset,
    rewrite_palmdb_record_offsets,
    truncate_mobi_payload,
)


def _set_record0_field(payload: bytes, offset: int, value: int) -> bytes:
    mutated = bytearray(payload)
    record0_offset = palmdb_record_offsets(payload)[0]
    mutated[record0_offset + offset : record0_offset + offset + 4] = pack(">I", value)
    return bytes(mutated)


def _set_record_count(payload: bytes, count: int) -> bytes:
    mutated = bytearray(payload)
    mutated[76:78] = pack(">H", count)
    return bytes(mutated)


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"\0" * 12,
        b"\0" * 60 + b"BOOKMOBI" + b"\0" * 9,
    ],
)
def test_mobi_reader_rejects_truncated_palmdb_header_as_mobi_error(payload: bytes) -> None:
    with pytest.raises(MobiError):
        MobiReader(mobi_stream(payload), MobiLog())


def test_metadata_header_rejects_short_record_count_as_mobi_error() -> None:
    payload = b"\0" * 60 + b"BOOKMOBI" + b"\0" * 8

    with pytest.raises(MobiError):
        MetadataHeader(mobi_stream(payload), MobiLog())


def test_mobi_reader_rejects_short_record_table_as_mobi_error() -> None:
    payload = build_minimal_mobi()
    declared_three_records = _set_record_count(payload, 3)
    short_table = truncate_mobi_payload(declared_three_records, 78 + (2 * 8) + 2)

    with pytest.raises(MobiError):
        MobiReader(mobi_stream(short_table), MobiLog())


@pytest.mark.parametrize("mutator", ["duplicate", "reversed", "out_of_file"])
def test_mobi_reader_rejects_invalid_record_offsets_as_mobi_error(mutator: str) -> None:
    payload = build_minimal_mobi(title="Offset Attack", authors=["Offset Author"])
    offsets = palmdb_record_offsets(payload)

    if mutator == "duplicate":
        hostile = rewrite_palmdb_record_offset(payload, 1, offsets[0])
    elif mutator == "reversed":
        hostile = rewrite_palmdb_record_offsets(payload, list(reversed(offsets)))
    else:
        hostile = rewrite_palmdb_record_offset(payload, 1, len(payload) + 8192)

    with pytest.raises(MobiError):
        MobiReader(mobi_stream(hostile), MobiLog())


def test_mobi_reader_rejects_short_record0_as_mobi_error() -> None:
    payload = build_palmdb([b"\0" * 20, b"<html><body>body</body></html>"])

    with pytest.raises(MobiError):
        MobiReader(mobi_stream(payload), MobiLog())


def test_mobi_reader_rejects_impossible_mobi_header_length_as_mobi_error() -> None:
    payload = build_minimal_mobi(title="Bad Header Length")
    hostile = _set_record0_field(payload, 0x14, 0xFFFF)

    with pytest.raises(MobiError):
        MobiReader(mobi_stream(hostile), MobiLog())


def test_mobi_reader_rejects_out_of_range_title_offset_as_mobi_error() -> None:
    payload = build_minimal_mobi(title="Bad Title Offset")
    hostile = _set_record0_field(payload, 0x54, len(payload) + 4096)

    with pytest.raises(MobiError):
        MobiReader(mobi_stream(hostile), MobiLog())


@pytest.mark.parametrize(
    "exth",
    [
        b"",
        b"NOPE" + b"\0" * 8,
        build_mobi_exth([(100, "Alice")], length=999),
        build_mobi_exth([mobi_exth_record(100, b"payload", declared_size=7)], item_count=1),
    ],
)
def test_exth_header_rejects_malformed_blocks_as_mobi_error(exth: bytes) -> None:
    with pytest.raises(MobiError):
        EXTHHeader(exth, "utf-8", "fallback")


def test_metadata_header_rejects_out_of_range_section_access_as_mobi_error() -> None:
    header = MetadataHeader(mobi_stream(build_minimal_mobi()), MobiLog())

    with pytest.raises(MobiError):
        header.section_data(10)


def test_mobi_reader_rejects_malformed_exth_in_record0_as_mobi_error() -> None:
    record0 = build_mobi_record0(
        title="Bad EXTH",
        authors=["Bad Author"],
        exth_records=[mobi_exth_record(100, b"payload", declared_size=7)],
    )
    payload = build_palmdb([record0, b"<html><body>body</body></html>"])

    with pytest.raises(MobiError):
        MobiReader(mobi_stream(payload), MobiLog())
