from __future__ import annotations

import io
import struct
from types import SimpleNamespace

import pytest

from LiuXin_alpha.file_formats.pdb.header import PdbHeaderBuilder
from LiuXin_alpha.metadata.utils import calibreMetaInformation


def _values(raw):
    if raw is None:
        return []
    if isinstance(raw, dict):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    try:
        return list(raw)
    except TypeError:
        return [raw]


def _build_pdb(identity: str, title: str, sections: list[bytes]) -> io.BytesIO:
    stream = io.BytesIO()
    PdbHeaderBuilder(identity, title).build_header([len(s) for s in sections], stream)
    for section in sections:
        stream.write(section)
    stream.seek(0)
    return stream


def _ereader_header_record(
    *,
    metadata_offset: int = 1,
    last_data_offset: int = 2,
    image_count: int = 0,
    image_data_offset: int = 0,
    compression: int = 10,
    has_metadata: int = 1,
) -> bytes:
    header = bytearray(132)

    def put(offset: int, value: int) -> None:
        header[offset : offset + 2] = struct.pack(">H", value)

    put(0, compression)
    put(12, 1)
    put(20, image_count)
    put(24, has_metadata)
    put(40, image_data_offset)
    put(44, metadata_offset)
    put(52, last_data_offset)
    return bytes(header)


def _make_plucker_record(rtype: int, payload: bytes, *, length_words: int | None = None) -> bytes:
    if length_words is None:
        if len(payload) % 2:
            payload += b"\x00"
        length_words = (4 + len(payload)) // 2
    return struct.pack(">HH", rtype, length_words) + payload


def test_pdb_package_path_type_and_header_error_edges(tmp_path) -> None:
    import LiuXin_alpha.metadata.file_sources.pdb as pdb_md

    with pytest.raises(TypeError, match="binary stream"):
        pdb_md.get_metadata(object())
    with pytest.raises(TypeError, match="read/write"):
        pdb_md.set_metadata(object(), calibreMetaInformation("Title", ["Author"]))
    with pytest.raises(ValueError, match="Unable to parse PDB header identity"):
        pdb_md.get_pheader_ident(io.BytesIO(b"not-a-pdb"))
    with pytest.raises(ValueError, match="Cannot set metadata"):
        pdb_md.set_metadata(io.BytesIO(b"not-a-pdb"), calibreMetaInformation("Title", ["Author"]))

    broken_path = tmp_path / "broken_title.pdb"
    broken_path.write_bytes(b"bad")
    with pytest.raises(pdb_md.PdbFormatError):
        pdb_md.get_metadata(broken_path)

    md = pdb_md.get_metadata(broken_path, fallback_on_parse_error=True)
    assert md.title == "broken_title"
    assert _values(md.authors) == ["Unknown"]

    fallback_stream = _build_pdb("zTXTGPlm", "Header Only", [b"payload"])
    assert pdb_md.get_metadata(fallback_stream).title == "Header Only"

    unsupported_write = _build_pdb("zTXTGPlm", "Old", [b"payload"])
    pdb_md.set_metadata(unsupported_write, calibreMetaInformation("New*Title", ["Author"]))
    unsupported_write.seek(0)
    assert b"New_Title" in unsupported_write.read(40)


def test_ereader_helper_cover_and_metadata_write_edges(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.pdb.ereader as ereader_md

    assert ereader_md._clean_text(None) == ""
    assert ereader_md._clean_text(" title\x00 ") == "title"
    assert ereader_md._decode_field(b"Caf\xe9") == "Café"
    assert ereader_md._safe_section(SimpleNamespace(section_data=lambda _idx: (_ for _ in ()).throw(IndexError)), 9) == b""
    assert ereader_md._normalize_authors({"Ada": 1, "": 2}) == ["Ada"]
    assert ereader_md._normalize_authors(("Ada", "", "Bob")) == ["Ada", "Bob"]
    assert ereader_md._normalize_authors(42) == ["42"]

    image_section = b"PNG " + b"ignored.png".ljust(32, b"\x00") + b"\x00" * 26 + b"ignored"
    cover_section = b"PNG " + b"cover.png".ljust(32, b"\x00") + b"\x00" * 26 + b"cover-bytes"
    pheader = SimpleNamespace(section_data=lambda idx: b"short" if idx == 3 else image_section if idx == 4 else cover_section)
    eheader = SimpleNamespace(image_count=3, image_data_offset=3)
    assert ereader_md.get_cover(pheader, eheader) == ("png", b"cover-bytes")

    fallback = ereader_md.get_metadata(_build_pdb("PNPdPPrs", "Fallback Title", [b"short"]), extract_cover=True)
    assert fallback.title == "Fallback Title"

    stream = _build_pdb(
        "PNPdPPrs",
        "No Metadata",
        [_ereader_header_record(has_metadata=0, last_data_offset=1), b"text-section"],
    )
    ereader_md.set_metadata(stream, calibreMetaInformation("Created Metadata", ["Alice", "Bob"]))
    reread = ereader_md.get_metadata(stream, extract_cover=False)
    assert reread.title == "Created Metadata"
    assert _values(reread.authors) == ["Alice & Bob"]

    invalid_offset = _build_pdb(
        "PNPdPPrs",
        "Invalid Offset",
        [_ereader_header_record(metadata_offset=99), b"metadata"],
    )
    ereader_md.set_metadata(invalid_offset, calibreMetaInformation("Ignored", ["Author"]))

    unsupported_compression = _build_pdb(
        "PNPdPPrs",
        "Unsupported Compression",
        [_ereader_header_record(compression=1), b"metadata"],
    )
    before = unsupported_compression.getvalue()
    ereader_md.set_metadata(unsupported_compression, calibreMetaInformation("Ignored", ["Author"]))
    assert unsupported_compression.getvalue() == before


def test_plucker_record_iteration_decode_and_timestamp_edges(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.pdb.plucker as plucker_md
    from LiuXin_alpha.file_formats.pdb.plucker.reader import DATATYPE_METADATA

    assert plucker_md._decode_text(None, "utf-8") == ""
    assert plucker_md._decode_text(b"Caf\xe9\x00   title", "not-a-codec") == "Café title"
    assert list(plucker_md._iter_records(b"\x00")) == []
    assert list(plucker_md._iter_records(struct.pack(">HH", 1, 1))) == []
    assert list(plucker_md._iter_records(struct.pack(">HH", 1, 100))) == []

    metadata = (
        struct.pack(">H", 4)
        + _make_plucker_record(1, struct.pack(">H", 65535))
        + _make_plucker_record(4, b"Solo Author\x00")
        + _make_plucker_record(5, b"Decoded Title\x00")
        + _make_plucker_record(6, struct.pack(">I", 123))
    )
    section_header = struct.pack(">HHHBB", 1, 0, len(metadata), DATATYPE_METADATA, 0)
    stream = _build_pdb("DataPlkr", "Header Fallback", [b"\x00" * 8, section_header + metadata])
    stream.seek(5)

    class _Datetime:
        @staticmethod
        def fromtimestamp(*_args, **_kwargs):
            raise OverflowError("bad timestamp")

    monkeypatch.setattr(plucker_md, "datetime", _Datetime)
    md = plucker_md.get_metadata(stream)
    assert stream.tell() == 5
    assert md.title == "Decoded Title"
    assert _values(md.authors) == ["Solo Author"]
    assert not getattr(md, "pubdate", None)

    no_metadata = plucker_md.get_metadata(_build_pdb("DataPlkr", "No Metadata", [b"\x00" * 8, b"short"]))
    assert no_metadata.title == "No Metadata"
    assert _values(no_metadata.authors) == ["Unknown"]
