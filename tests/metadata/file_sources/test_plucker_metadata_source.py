from __future__ import annotations

import io
import struct

from LiuXin_alpha.file_formats.pdb.header import PdbHeaderBuilder
from LiuXin_alpha.file_formats.pdb.plucker.reader import DATATYPE_METADATA


def _build_pdb(identity: str, title: str, sections: list[bytes]) -> io.BytesIO:
    stream = io.BytesIO()
    PdbHeaderBuilder(identity, title).build_header([len(s) for s in sections], stream)
    for section in sections:
        stream.write(section)
    stream.seek(0)
    return stream


def _make_plucker_record(rtype: int, payload: bytes) -> bytes:
    if len(payload) % 2:
        payload += b"\x00"
    length_words = (4 + len(payload)) // 2
    return struct.pack(">HH", rtype, length_words) + payload


def _plucker_metadata_section_from_records(records: list[bytes]) -> bytes:
    payload = struct.pack(">H", len(records)) + b"".join(records)
    section_header = struct.pack(">HHHBB", 1, 0, len(payload), DATATYPE_METADATA, 0)
    return section_header + payload


def _plucker_metadata_section(*, title_bytes: bytes, author_bytes: bytes, mibnum: int, pubdate: int) -> bytes:
    records = [
        _make_plucker_record(1, struct.pack(">H", mibnum)),
        _make_plucker_record(4, author_bytes),
        _make_plucker_record(5, title_bytes),
        _make_plucker_record(6, struct.pack(">I", pubdate)),
    ]
    return _plucker_metadata_section_from_records(records)


def test_plucker_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.plucker as legacy_plucker
    import LiuXin_alpha.metadata.file_sources.pdb.plucker as modern_plucker

    assert legacy_plucker is not None
    assert modern_plucker is not None


def test_plucker_metadata_utf8_unicode_torture() -> None:
    from LiuXin_alpha.metadata.file_sources.pdb.plucker import get_metadata

    section = _plucker_metadata_section(
        title_bytes=("Título — Καλημέρα — こんにちは 😀".encode("utf-8") + b"\x00"),
        author_bytes=("Renée Faßbinder,李白,مريم,👩🏽\u200d💻".encode("utf-8") + b"\x00"),
        mibnum=106,
        pubdate=1700000000,
    )
    stream = _build_pdb("DataPlkr", "Fallback Header", [b"\x00" * 8, section])

    mi = get_metadata(stream, extract_cover=False)
    assert mi.title == "Título — Καλημέρα — こんにちは 😀"
    assert mi.authors == ["Renée Faßbinder", "李白", "مريم", "👩🏽\u200d💻"]
    assert (mi.pubdate.year, mi.pubdate.month, mi.pubdate.day) == (2023, 11, 14)


def test_plucker_metadata_utf16be_charset_decode() -> None:
    from LiuXin_alpha.metadata.file_sources.pdb.plucker import get_metadata

    title = "UTF16 題名 🚀"
    author = "Αλέξης,דוד"
    section = _plucker_metadata_section(
        title_bytes=title.encode("utf_16_be") + b"\x00\x00",
        author_bytes=author.encode("utf_16_be") + b"\x00\x00",
        mibnum=1013,
        pubdate=1700000000,
    )
    stream = _build_pdb("DataPlkr", "Fallback Header", [b"\x00" * 8, section])

    mi = get_metadata(stream, extract_cover=False)
    assert mi.title == title
    assert mi.authors == ["Αλέξης", "דוד"]


def test_plucker_metadata_unknown_mibnum_falls_back_to_latin1() -> None:
    from LiuXin_alpha.metadata.file_sources.pdb.plucker import get_metadata

    title = "Le père Goriot"
    author = "Émile Zola"
    section = _plucker_metadata_section(
        title_bytes=title.encode("latin-1") + b"\x00",
        author_bytes=author.encode("latin-1") + b"\x00",
        mibnum=65535,
        pubdate=1700000000,
    )
    stream = _build_pdb("DataPlkr", "Fallback Header", [b"\x00" * 8, section])

    mi = get_metadata(stream, extract_cover=False)
    assert mi.title == title
    assert mi.authors == [author]


def test_plucker_corrupt_metadata_section_falls_back_cleanly() -> None:
    from LiuXin_alpha.metadata.file_sources.pdb.plucker import get_metadata

    # Declares one record, but only contains a truncated record header.
    corrupted_payload = struct.pack(">H", 1) + b"\x00\x05\x00"
    section_header = struct.pack(">HHHBB", 1, 0, len(corrupted_payload), DATATYPE_METADATA, 0)
    section = section_header + corrupted_payload

    stream = _build_pdb("DataPlkr", "Header Fallback", [b"\x00" * 8, section])
    mi = get_metadata(stream, extract_cover=False)

    assert mi.title == "Header Fallback"
    assert mi.authors == ["Unknown"]


def test_plucker_pubdate_zero_is_ignored() -> None:
    from LiuXin_alpha.metadata.file_sources.pdb.plucker import get_metadata

    section = _plucker_metadata_section(
        title_bytes=b"No Date\x00",
        author_bytes=b"Author\x00",
        mibnum=106,
        pubdate=0,
    )
    stream = _build_pdb("DataPlkr", "Fallback Header", [b"\x00" * 8, section])

    mi = get_metadata(stream, extract_cover=False)
    assert mi.title == "No Date"
    assert mi.authors == ["Author"]
    assert getattr(mi, "pubdate", None) is None


def test_plucker_get_metadata_preserves_stream_position() -> None:
    from LiuXin_alpha.metadata.file_sources.pdb.plucker import get_metadata

    section = _plucker_metadata_section(
        title_bytes=b"Position Title\x00",
        author_bytes=b"A,B\x00",
        mibnum=106,
        pubdate=1700000000,
    )
    stream = _build_pdb("DataPlkr", "Fallback Header", [b"\x00" * 8, section])

    stream.seek(13)
    mi = get_metadata(stream, extract_cover=False)
    assert mi.title == "Position Title"
    assert stream.tell() == 13


def test_legacy_plucker_forwarder_matches_modern_output() -> None:
    from LiuXin_alpha.metadata.file_sources import plucker as legacy_plucker
    from LiuXin_alpha.metadata.file_sources.pdb import plucker as modern_plucker

    section = _plucker_metadata_section(
        title_bytes=b"Legacy Forward\x00",
        author_bytes=b"One,Two\x00",
        mibnum=106,
        pubdate=1700000000,
    )
    stream_legacy = _build_pdb("DataPlkr", "Fallback Header", [b"\x00" * 8, section])
    stream_modern = _build_pdb("DataPlkr", "Fallback Header", [b"\x00" * 8, section])

    legacy = legacy_plucker.get_metadata(stream_legacy, extract_cover=False)
    modern = modern_plucker.get_metadata(stream_modern, extract_cover=False)

    assert legacy.title == modern.title == "Legacy Forward"
    assert legacy.authors == modern.authors == ["One", "Two"]
    assert legacy.pubdate == modern.pubdate
