from __future__ import annotations

import zlib

import pytest

from LiuXin_alpha.file_formats.pdb.ereader import EreaderError
from tests.support.file_format_pdb import (
    PdbLog,
    build_ereader202_header_record,
    build_ereader_header_record,
    build_ereader_image_record,
    build_ereader_metadata_record,
    build_pdb,
    pdb_input_options,
    pdb_stream,
)


def _pdb_header(payload: bytes):
    from LiuXin_alpha.file_formats.pdb.header import PdbHeaderReader

    return PdbHeaderReader(pdb_stream(payload))


def _dropbook_reader(payload: bytes):
    from LiuXin_alpha.file_formats.pdb.ereader.reader132 import Reader132

    return Reader132(_pdb_header(payload), pdb_stream(payload), PdbLog(), pdb_input_options(input_encoding="cp1252"))


def _makebook_reader(payload: bytes):
    from LiuXin_alpha.file_formats.pdb.ereader.reader202 import Reader202

    return Reader202(_pdb_header(payload), pdb_stream(payload), PdbLog(), pdb_input_options(input_encoding="cp1252"))


def test_ereader_dispatcher_rejects_short_record0_as_ereader_error() -> None:
    from LiuXin_alpha.file_formats.pdb.ereader.reader import Reader

    payload = build_pdb([b"\0"], title="Short eReader", ident="PNRdPPrs")

    with pytest.raises(EreaderError):
        Reader(_pdb_header(payload), pdb_stream(payload), PdbLog(), pdb_input_options())


def test_reader132_rejects_text_range_beyond_sections() -> None:
    record0 = build_ereader_header_record(
        compression=10,
        non_text_offset=5,
        has_metadata=0,
        metadata_offset=1,
        image_data_offset=1,
        last_data_offset=1,
    )
    payload = build_pdb([record0, zlib.compress(b"body")], title="Bad Dropbook Text", ident="PNPdPPrs")

    with pytest.raises(EreaderError):
        _dropbook_reader(payload)


def test_reader132_rejects_image_range_beyond_sections() -> None:
    record0 = build_ereader_header_record(
        compression=10,
        non_text_offset=1,
        image_count=2,
        image_data_offset=2,
        metadata_offset=4,
        has_metadata=0,
        last_data_offset=4,
    )
    payload = build_pdb([record0, b"metadata"], title="Bad Dropbook Images", ident="PNPdPPrs")

    with pytest.raises(EreaderError):
        _dropbook_reader(payload)


def test_reader132_wraps_bad_zlib_text_as_ereader_error() -> None:
    record0 = build_ereader_header_record(
        compression=10,
        non_text_offset=2,
        has_metadata=0,
        metadata_offset=2,
        image_data_offset=2,
        last_data_offset=2,
    )
    payload = build_pdb([record0, b"not-zlib"], title="Bad Dropbook Zlib", ident="PNPdPPrs")
    reader = _dropbook_reader(payload)

    with pytest.raises(EreaderError):
        reader.dump_pml()


def test_reader132_sanitizes_image_names_for_dump(tmp_path) -> None:
    record0 = build_ereader_header_record(
        compression=10,
        non_text_offset=2,
        image_count=1,
        image_data_offset=2,
        metadata_offset=3,
        has_metadata=1,
        last_data_offset=4,
    )
    image = build_ereader_image_record(name="../escape.png", payload=b"image-bytes")
    metadata = build_ereader_metadata_record(title="Dropbook", author="Author", encoding="cp1252")
    payload = build_pdb([record0, zlib.compress(b""), image, metadata, b"MeTaInFo\0"], title="Image Paths", ident="PNPdPPrs")
    reader = _dropbook_reader(payload)

    output_dir = tmp_path / "images"
    reader.dump_images(output_dir)

    assert not (tmp_path / "escape.png").exists()
    assert (output_dir / "escape.png").read_bytes() == b"image-bytes"


def test_reader202_rejects_text_range_beyond_sections() -> None:
    record0 = build_ereader202_header_record(version=2, non_text_offset=4)
    payload = build_pdb([record0, b"text"], title="Bad Makebook Text", ident="PNRdPPrs")

    with pytest.raises(EreaderError):
        _makebook_reader(payload)


def test_reader202_wraps_palmdoc_text_failures(monkeypatch) -> None:
    from LiuXin_alpha.file_formats.compression import palmdoc as palmdoc_compression

    def fail_decompress(_payload):
        raise RuntimeError("synthetic eReader decompressor failure")

    monkeypatch.setattr(palmdoc_compression, "decompress_doc", fail_decompress)
    record0 = build_ereader202_header_record(version=2, non_text_offset=2)
    payload = build_pdb([record0, b"bad"], title="Bad Makebook Text", ident="PNRdPPrs")
    reader = _makebook_reader(payload)

    with pytest.raises(EreaderError):
        reader.dump_pml()


def test_reader202_sanitizes_image_names() -> None:
    record0 = build_ereader202_header_record(version=2, non_text_offset=2)
    image = build_ereader_image_record(name="..\\escape.png", payload=b"image-bytes")
    payload = build_pdb([record0, b"x", image], title="Makebook Image", ident="PNRdPPrs")
    reader = _makebook_reader(payload)

    assert reader.get_image(2) == ("escape.png", b"image-bytes")
