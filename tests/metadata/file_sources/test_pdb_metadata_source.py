"""
Verify PDB dispatch and shared metadata extraction.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test pdb metadata source through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py
"""
from __future__ import annotations

import importlib
import io
import re
import struct
from pathlib import Path

from LiuXin_alpha.file_formats.pdb.header import PdbHeaderBuilder, PdbHeaderReader
from LiuXin_alpha.file_formats.pdb.plucker.reader import DATATYPE_METADATA
from LiuXin_alpha.metadata.utils import calibreMetaInformation


def _build_pdb(identity: str, title: str, sections: list[bytes]) -> io.BytesIO:
    """
    Perform the build pdb test-helper operation with deterministic inputs.

    Example:
        Exercise build pdb through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param identity: Value supplied for identity in the focused test operation.
    :param title: Value supplied for title in the focused test operation.
    :param sections: Value supplied for sections in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    stream = io.BytesIO()
    PdbHeaderBuilder(identity, title).build_header([len(s) for s in sections], stream)
    for section in sections:
        stream.write(section)
    stream.seek(0)
    return stream


def _ereader_header_record(
    *,
    metadata_offset: int,
    last_data_offset: int,
    image_count: int = 0,
    image_data_offset: int = 0,
    compression: int = 10,
) -> bytes:
    """
    Perform the ereader header record test-helper operation with deterministic inputs.

    Example:
        Exercise ereader header record through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param metadata_offset: Value supplied for metadata offset in the focused test
        operation.
    :param last_data_offset: Value supplied for last data offset in the focused test
        operation.
    :param image_count: Value supplied for image count in the focused test operation.
    :param image_data_offset: Value supplied for image data offset in the focused test
        operation.
    :param compression: Value supplied for compression in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    header = bytearray(132)

    def put(offset: int, value: int) -> None:
        """
        Perform the put test-helper operation with deterministic inputs.

        Example:
            Exercise ereader header record.put through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


        :param offset: Value supplied for offset in the focused test operation.
        :param value: Value stored, compared or projected by the operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        header[offset : offset + 2] = struct.pack(">H", value)

    put(0, compression)
    put(12, 1)  # non_text_offset: no text pages
    put(20, image_count)
    put(24, 1)  # has_metadata
    put(40, image_data_offset)
    put(44, metadata_offset)
    put(52, last_data_offset)
    return bytes(header)


def _make_plucker_record(rtype: int, payload: bytes) -> bytes:
    """
    Perform the make plucker record test-helper operation with deterministic inputs.

    Example:
        Exercise make plucker record through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param rtype: Value supplied for rtype in the focused test operation.
    :param payload: Value supplied for payload in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    if len(payload) % 2:
        payload += b"\x00"
    length_words = (4 + len(payload)) // 2
    return struct.pack(">HH", rtype, length_words) + payload


def _plucker_metadata_section(title: str, author: str, pubdate: int) -> bytes:
    """
    Perform the plucker metadata section test-helper operation with deterministic inputs.

    Example:
        Exercise plucker metadata section through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param title: Value supplied for title in the focused test operation.
    :param author: Value supplied for author in the focused test operation.
    :param pubdate: Value supplied for pubdate in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    records = [
        _make_plucker_record(1, struct.pack(">H", 106)),  # utf-8
        _make_plucker_record(4, author.encode("utf-8") + b"\x00"),
        _make_plucker_record(5, title.encode("utf-8") + b"\x00"),
        _make_plucker_record(6, struct.pack(">I", pubdate)),
    ]
    payload = struct.pack(">H", len(records)) + b"".join(records)
    section_header = struct.pack(">HHHBB", 1, 0, len(payload), DATATYPE_METADATA, 0)
    return section_header + payload


def test_pdb_metadata_modules_import_smoke() -> None:
    """
    Verify pdb metadata modules import smoke.

    Example:
        Exercise test pdb metadata modules import smoke through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    modules = (
        "LiuXin_alpha.metadata.file_sources.pdb",
        "LiuXin_alpha.metadata.file_sources.pdb.ereader",
        "LiuXin_alpha.metadata.file_sources.pdb.haodoo",
        "LiuXin_alpha.metadata.file_sources.pdb.plucker",
    )
    for module_name in modules:
        importlib.import_module(module_name)


def test_pdb_fallback_metadata_and_ident_roundtrip() -> None:
    """
    Verify pdb fallback metadata and ident roundtrip.

    Example:
        Exercise test pdb fallback metadata and ident roundtrip through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.pdb import get_metadata, get_pheader_ident

    stream = _build_pdb("zTXTGPlm", "Header Title", [b"payload"])
    assert get_pheader_ident(stream) == "zTXTGPlm"

    mi = get_metadata(stream, extract_cover=False)
    assert mi.title == "Header Title"
    assert mi.authors == ["Unknown"]


def test_pdb_metadata_reader_accepts_pathlike(tmp_path: Path) -> None:
    """
    Verify pdb metadata reader accepts pathlike.

    Example:
        Exercise test pdb metadata reader accepts pathlike through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.pdb import get_metadata

    stream = _build_pdb("zTXTGPlm", "Pathlike Title", [b"payload"])
    path = tmp_path / "sample.pdb"
    path.write_bytes(stream.getvalue())

    mi = get_metadata(path, extract_cover=False)
    assert mi.title == "Pathlike Title"
    assert mi.authors == ["Unknown"]


def test_pdb_ereader_reads_metadata_and_cover() -> None:
    """
    Verify pdb ereader reads metadata and cover.

    Example:
        Exercise test pdb ereader reads metadata and cover through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.pdb import get_metadata

    metadata_record = b"My eReader Title\x00Jane Doe\x00\x00Pub House\x009781234567890\x00"
    image_section = b"PNG " + b"cover.png".ljust(32, b"\x00") + b"\x00" * (62 - 36) + b"RAW-COVER-BYTES"
    header_record = _ereader_header_record(metadata_offset=1, image_count=1, image_data_offset=2, last_data_offset=3)

    stream = _build_pdb("PNRdPPrs", "Wrapper Title", [header_record, metadata_record, image_section, b"MeTaInFo\x00"])
    mi = get_metadata(stream, extract_cover=True)

    assert mi.title == "My eReader Title"
    assert mi.authors == ["Jane Doe"]
    assert mi.publisher == "Pub House"
    assert mi.isbn == "9781234567890"
    assert mi.cover_data == ("png", b"RAW-COVER-BYTES")


def test_pdb_set_metadata_updates_ereader_payload_and_wrapper_title() -> None:
    """
    Verify pdb set metadata updates ereader payload and wrapper title.

    Example:
        Exercise test pdb set metadata updates ereader payload and wrapper title through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.pdb import get_metadata, set_metadata

    metadata_record = b"Old Title\x00Old Author\x00\x00Old Pub\x001111111111111\x00"
    header_record = _ereader_header_record(metadata_offset=1, last_data_offset=2)
    stream = _build_pdb("PNPdPPrs", "Old Wrapper", [header_record, metadata_record, b"MeTaInFo\x00"])

    update = calibreMetaInformation("Updated: Title*Here", ["A New Author"])
    update.publisher = "New Pub"
    update.isbn = "9999999999999"
    set_metadata(stream, update)

    read_back = get_metadata(stream, extract_cover=False)
    assert read_back.title == "Updated: Title*Here"
    assert read_back.authors == ["A New Author"]
    assert read_back.publisher == "New Pub"
    assert read_back.isbn == "9999999999999"

    expected_wrapper = re.sub(r"[^-A-Za-z0-9 ]+", "_", "Updated: Title*Here")
    wrapper_title = PdbHeaderReader(stream).title
    assert wrapper_title == expected_wrapper


def test_pdb_set_metadata_sanitizes_hostile_ereader_text_without_field_shift() -> None:
    """
    Verify pdb set metadata sanitizes hostile ereader text without field shift.

    Example:
        Exercise test pdb set metadata sanitizes hostile ereader text without field shift through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.pdb import get_metadata, set_metadata

    metadata_record = b"Old Title\x00Old Author\x00\x00Old Pub\x001111111111111\x00"
    header_record = _ereader_header_record(metadata_offset=1, last_data_offset=2)
    stream = _build_pdb("PNPdPPrs", "Old Wrapper", [header_record, metadata_record, b"MeTaInFo\x00"])

    title = "Bad\x00Title\ud800 Café"
    authors = ["Alice\x00Injected", "Bob\udfff"]
    update = calibreMetaInformation(title, authors)
    update.publisher = "Pub\x01House"
    update.isbn = "978\x02123"

    set_metadata(stream, update)

    read_back = get_metadata(stream, extract_cover=False)
    assert read_back.title == "BadTitle Café"
    assert read_back.authors == ["AliceInjected & Bob"]
    assert read_back.publisher == "PubHouse"
    assert read_back.isbn == "978123"

    pheader = PdbHeaderReader(stream)
    raw_metadata = pheader.section_data(1)
    assert raw_metadata.split(b"\x00")[:5] == [
        "BadTitle Café".encode("cp1252"),
        b"AliceInjected & Bob",
        b"",
        b"PubHouse",
        b"978123",
    ]
    assert b"\x01" not in raw_metadata
    assert b"\x02" not in raw_metadata
    assert pheader.title == "BadTitle Caf_"

    assert update.title == title
    assert update.authors == authors


def test_pdb_plucker_metadata_reader_extracts_fields() -> None:
    """
    Verify pdb plucker metadata reader extracts fields.

    Example:
        Exercise test pdb plucker metadata reader extracts fields through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.pdb import get_metadata

    section = _plucker_metadata_section("Plucker 世界", "Alice,Bob", 1700000000)
    stream = _build_pdb("DataPlkr", "Fallback", [b"\x00" * 8, section])

    mi = get_metadata(stream, extract_cover=False)
    assert mi.title == "Plucker 世界"
    assert mi.authors == ["Alice", "Bob"]
    assert (mi.pubdate.year, mi.pubdate.month, mi.pubdate.day) == (2023, 11, 14)


def test_pdb_haodoo_reader_falls_back_cleanly_on_bad_input() -> None:
    """
    Verify pdb haodoo reader falls back cleanly on bad input.

    Example:
        Exercise test pdb haodoo reader falls back cleanly on bad input through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.pdb import get_metadata

    # Synthetic malformed section data should not crash the metadata path.
    stream = _build_pdb("BOOKMTIT", "Haodoo Fallback", [b"\x00"])
    mi = get_metadata(stream, extract_cover=False)
    assert mi.title == "Haodoo Fallback"
    assert mi.authors == ["Unknown"]


def test_legacy_top_level_plucker_module_forwards_to_pdb_plucker() -> None:
    """
    Verify legacy top level plucker module forwards to pdb plucker.

    Example:
        Exercise test legacy top level plucker module forwards to pdb plucker through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources import plucker as legacy_plucker
    from LiuXin_alpha.metadata.file_sources.pdb import plucker as pdb_plucker

    section = _plucker_metadata_section("Legacy Alias", "One,Two", 1700000000)
    stream_legacy = _build_pdb("DataPlkr", "Fallback", [b"\x00" * 8, section])
    stream_modern = _build_pdb("DataPlkr", "Fallback", [b"\x00" * 8, section])

    mi_legacy = legacy_plucker.get_metadata(stream_legacy, extract_cover=False)
    mi_modern = pdb_plucker.get_metadata(stream_modern, extract_cover=False)

    assert mi_legacy.title == mi_modern.title == "Legacy Alias"
    assert mi_legacy.authors == mi_modern.authors == ["One", "Two"]
    assert mi_legacy.pubdate == mi_modern.pubdate
