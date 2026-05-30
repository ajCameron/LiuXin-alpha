from __future__ import annotations

from struct import pack
from types import SimpleNamespace

import pytest

from LiuXin_alpha.file_formats.mobi import MobiError
from LiuXin_alpha.file_formats.mobi.reader.headers import NULL_INDEX
from LiuXin_alpha.file_formats.mobi.reader.index import InvalidFile
from tests.support.file_format_mobi import (
    MobiLog,
    build_mobi_record0,
    build_palmdb,
    mobi_stream,
)


def _fake_mobi8_reader(*, header=None, sections=None, raw_ml=b"<html></html>"):
    from LiuXin_alpha.file_formats.mobi.reader.mobi8 import Mobi8Reader

    reader = Mobi8Reader.__new__(Mobi8Reader)
    reader.log = MobiLog()
    reader.header = header or SimpleNamespace(
        fdstidx=NULL_INDEX,
        skelidx=NULL_INDEX,
        dividx=NULL_INDEX,
        othidx=NULL_INDEX,
        ncxidx=NULL_INDEX,
        codec="utf-8",
        exth=SimpleNamespace(start_offset=None),
    )
    reader.kf8_sections = sections if sections is not None else []
    reader.mobi6_reader = SimpleNamespace(mobi_html=raw_ml)
    reader.raw_ml = raw_ml
    reader.resource_offsets = []
    reader.encrypted_fonts = []
    return reader


def test_huff_reader_rejects_missing_records_as_mobi_error() -> None:
    from LiuXin_alpha.file_formats.mobi.huffcdic import HuffReader

    with pytest.raises(MobiError):
        HuffReader([])


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"HUFF\x00\x00\x00\x18",
        b"HUFF\x00\x00\x00\x18" + pack(">LL", 12, 4096),
    ],
)
def test_huff_reader_rejects_truncated_huff_tables_as_mobi_error(payload: bytes) -> None:
    from LiuXin_alpha.file_formats.mobi.huffcdic import Reader

    with pytest.raises(MobiError):
        Reader().load_huff(payload)


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"CDIC\x00\x00\x00\x10",
        b"CDIC\x00\x00\x00\x10" + pack(">LL", 4, 31),
        b"CDIC\x00\x00\x00\x10" + pack(">LL", 4, 2) + b"\0\4",
    ],
)
def test_huff_reader_rejects_malformed_cdic_tables_as_mobi_error(payload: bytes) -> None:
    from LiuXin_alpha.file_formats.mobi.huffcdic import Reader

    with pytest.raises(MobiError):
        Reader().load_cdic(payload)


def test_mobi_reader_rejects_huff_record_ranges_outside_sections_as_mobi_error() -> None:
    from LiuXin_alpha.file_formats.mobi.reader.mobi6 import MobiReader

    record0 = bytearray(
        build_mobi_record0(
            title="Bad HUFF",
            authors=["Bad Author"],
            compression=b"DH",
            include_exth=False,
        )
    )
    record0[0x70:0x78] = pack(">LL", 99, 1)
    payload = build_palmdb([record0, b"compressed-text"])
    reader = MobiReader(mobi_stream(payload), MobiLog())

    with pytest.raises(MobiError):
        reader.extract_text()


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"INDX",
        b"NOPE" + b"\0" * 256,
    ],
)
def test_mobi_index_parser_rejects_truncated_or_wrong_indx_records(payload: bytes) -> None:
    from LiuXin_alpha.file_formats.mobi.reader.index import parse_indx_header

    with pytest.raises(InvalidFile):
        parse_indx_header(payload)


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"TAGX",
        b"TAGX" + pack(">LL", 1000, 1),
    ],
)
def test_mobi_index_parser_rejects_truncated_or_wrong_tagx_records(payload: bytes) -> None:
    from LiuXin_alpha.file_formats.mobi.reader.index import parse_tagx_section

    with pytest.raises(InvalidFile):
        parse_tagx_section(payload)


def test_mobi_read_index_rejects_out_of_range_index_as_invalid_file() -> None:
    from LiuXin_alpha.file_formats.mobi.reader.index import read_index

    with pytest.raises(InvalidFile):
        read_index([], 0, "utf-8")


def test_mobi8_reader_rejects_fdst_index_out_of_range_as_mobi_error() -> None:
    header = SimpleNamespace(
        fdstidx=5,
        skelidx=NULL_INDEX,
        dividx=NULL_INDEX,
        othidx=NULL_INDEX,
        ncxidx=NULL_INDEX,
        codec="utf-8",
        exth=SimpleNamespace(start_offset=None),
    )
    reader = _fake_mobi8_reader(header=header, sections=[(b"FDST", (0, 0, 0))])

    with pytest.raises(MobiError):
        reader.read_indices()


@pytest.mark.parametrize(
    "fdst",
    [
        b"NOPE" + b"\0" * 12,
        b"FDST" + pack(">LL", 12, 2),
        b"FDST" + pack(">LL", 12, 1) + pack(">LL", 10, 5),
        b"FDST" + pack(">LL", 12, 1) + pack(">LL", 0, 9999),
    ],
)
def test_mobi8_reader_rejects_malformed_fdst_records_as_mobi_error(fdst: bytes) -> None:
    header = SimpleNamespace(
        fdstidx=0,
        skelidx=NULL_INDEX,
        dividx=NULL_INDEX,
        othidx=NULL_INDEX,
        ncxidx=NULL_INDEX,
        codec="utf-8",
        exth=SimpleNamespace(start_offset=None),
    )
    reader = _fake_mobi8_reader(
        header=header,
        sections=[(fdst, (0, 0, 0))],
        raw_ml=b"<html><body>short</body></html>",
    )

    with pytest.raises(MobiError):
        reader.read_indices()


def test_mobi8_reader_wraps_invalid_skeleton_index_as_mobi_error() -> None:
    header = SimpleNamespace(
        fdstidx=NULL_INDEX,
        skelidx=0,
        dividx=NULL_INDEX,
        othidx=NULL_INDEX,
        ncxidx=NULL_INDEX,
        codec="utf-8",
        exth=SimpleNamespace(start_offset=None),
    )
    reader = _fake_mobi8_reader(header=header, sections=[(b"not-an-index", (0, 0, 0))])

    with pytest.raises(MobiError):
        reader.read_indices()


def test_mobi8_create_ncx_wraps_invalid_ncx_index_as_mobi_error() -> None:
    header = SimpleNamespace(
        fdstidx=NULL_INDEX,
        skelidx=NULL_INDEX,
        dividx=NULL_INDEX,
        othidx=NULL_INDEX,
        ncxidx=0,
        codec="utf-8",
        exth=SimpleNamespace(start_offset=None),
    )
    reader = _fake_mobi8_reader(header=header, sections=[(b"not-an-index", (0, 0, 0))])
    reader.partinfo = []
    reader.parts = []

    with pytest.raises(MobiError):
        reader.create_ncx()


def _png_bytes() -> bytes:
    return (
        b"\x89PNG\r\n\x1a\n"
        b"\x00\x00\x00\rIHDR"
        b"\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00"
        b"\x90wS\xde"
        b"\x00\x00\x00\x0cIDAT\x08\xd7c\xf8\xff\xff?\x00\x05\xfe\x02\xfeA\xd5\x9f\xdd"
        b"\x00\x00\x00\x00IEND\xaeB`\x82"
    )


def test_mobi_reader_rejects_palmdoc_text_record_expansion_beyond_declared_size() -> None:
    from LiuXin_alpha.file_formats.compression.palmdoc import compress_doc
    from LiuXin_alpha.file_formats.mobi.reader.mobi6 import MobiReader

    raw_text = b"x" * 5000
    record0 = build_mobi_record0(
        title="PalmDOC Expansion",
        authors=["Fixture Author"],
        compression=b"\x00\x02",
        include_exth=False,
    )
    payload = build_palmdb([record0, compress_doc(raw_text)], name="PalmDOC Expansion")
    reader = MobiReader(mobi_stream(payload), MobiLog())

    with pytest.raises(MobiError, match="expands beyond limit"):
        reader.extract_text()


def test_huff_reader_rejects_text_record_expansion_beyond_limit() -> None:
    from LiuXin_alpha.file_formats.mobi.huffcdic import Reader

    reader = Reader()
    reader.dict1 = tuple((8, 0x80, 0) for _ in range(256))
    reader.dictionary = [(b"x" * 10, 1)]

    with pytest.raises(MobiError, match="expands beyond limit"):
        reader.unpack(b"\x00", max_output_size=5)


def test_mobi8_extract_resources_writes_direct_image_product(tmp_path, monkeypatch) -> None:
    png = _png_bytes()
    reader = _fake_mobi8_reader()
    reader.resource_offsets = [(0, 1)]
    monkeypatch.chdir(tmp_path)

    resource_map = reader.extract_resources([(png, (0, 0, 0))])

    assert resource_map == ["images/00001.png"]
    assert (tmp_path / "images" / "00001.png").read_bytes() == png


def test_mobi8_extract_resources_writes_contained_cres_image_product(tmp_path, monkeypatch) -> None:
    from struct import pack

    png = _png_bytes()
    payload = b"application/image"
    container = (
        b"CONT"
        + b"\x00" * 44
        + b"EXTH"
        + pack(">LL", 12 + 8 + len(payload), 1)
        + pack(">II", 539, 8 + len(payload))
        + payload
    )
    cres = b"CRES" + b"\x00" * 8 + png
    reader = _fake_mobi8_reader()
    reader.resource_offsets = [(0, 2)]
    monkeypatch.chdir(tmp_path)

    resource_map = reader.extract_resources([(container, (0, 0, 0)), (cres, (0, 0, 0))])

    assert resource_map == [None, "images/00001.png"]
    assert (tmp_path / "images" / "00001.png").read_bytes() == png


def test_mobi8_extract_resources_rejects_out_of_range_resource_offsets() -> None:
    reader = _fake_mobi8_reader()
    reader.resource_offsets = [(2, 3)]

    with pytest.raises(MobiError):
        reader.extract_resources([(b"image", (0, 0, 0))])


def test_mobi8_extract_resources_rejects_cres_without_container() -> None:
    reader = _fake_mobi8_reader()
    reader.resource_offsets = [(0, 1)]

    with pytest.raises(MobiError):
        reader.extract_resources([(b"CRES" + b"payload", (0, 0, 0))])
