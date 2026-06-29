from __future__ import annotations

from pathlib import Path

import pytest

from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation import CalibreSidecarReader, CalibreUnsafePathError
from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation import parse_metadata_opf


def _write_min_opf(path: Path) -> None:
    # A deliberately small OPF2-ish payload (Calibre sidecar style).
    # Use single quotes for JSON attributes to avoid XML escaping noise.
    opf = """<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf" unique-identifier="uuid_id" version="2.0">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:opf="http://www.idpf.org/2007/opf">
    <dc:title>Test Book</dc:title>
    <dc:creator opf:role="aut">Ada Lovelace</dc:creator>
    <dc:creator opf:role="aut">Alan Turing</dc:creator>
    <dc:language>en</dc:language>
    <dc:identifier opf:scheme="uuid">urn:uuid:12345678-1234-1234-1234-123456789abc</dc:identifier>
    <dc:subject>math</dc:subject>
    <dc:subject>history</dc:subject>
    <dc:description><![CDATA[<p>Hello</p>]]></dc:description>

    <meta name="calibre:series" content="Great Ideas"/>
    <meta name="calibre:series_index" content="1"/>
    <meta name="calibre:rating" content="8"/>

    <meta name="calibre:user_metadata:#mood" content='{"datatype":"text","is_multiple":null,"#value#":"brooding","display":{}}'/>
    <meta name="calibre:user_metadata:#saga" content='{"datatype":"series","is_multiple":null,"#value#":"SagaName","#extra#":2.5}'/>
    <meta name="calibre:user_metadata:#multi" content='{"datatype":"text","is_multiple":"|","#value#":"a|b|c"}'/>
  </metadata>
</package>
"""
    path.write_text(opf, encoding="utf-8")


def test_parse_metadata_opf_extracts_core_fields(tmp_path: Path) -> None:
    opf_path = tmp_path / "metadata.opf"
    _write_min_opf(opf_path)

    parsed = parse_metadata_opf(opf_path)
    assert parsed.title == "Test Book"
    assert parsed.authors == ("Ada Lovelace", "Alan Turing")
    assert parsed.tags == ("math", "history")
    assert parsed.languages == ("en",)
    assert parsed.series is not None
    assert parsed.series.name == "Great Ideas"
    assert parsed.series.index == 1.0
    assert parsed.extras.get("calibre:rating") == 8

    # Custom values (user_metadata)
    assert parsed.user_metadata.get("#mood") == "brooding"
    assert parsed.user_metadata.get("#saga") == {"name": "SagaName", "index": 2.5}
    assert parsed.user_metadata.get("#multi") == ["a", "b", "c"]


def test_sidecar_reader_streams_without_metadata_db(tmp_path: Path) -> None:
    # Create a minimal Calibre-ish folder layout.
    root = tmp_path / "Library"
    book_dir = root / "Ada Lovelace" / "Test Book (1)"
    book_dir.mkdir(parents=True)

    _write_min_opf(book_dir / "metadata.opf")
    (book_dir / "cover.jpg").write_bytes(b"\xFF\xD8\xFF" + b"fakejpeg")
    (book_dir / "Test Book - Ada.epub").write_bytes(b"epubdata")

    r = CalibreSidecarReader.from_root(root)
    payloads = list(r.iter_book_payloads())
    assert len(payloads) == 1
    p = payloads[0]
    assert p.title == "Test Book"
    assert "sidecar_mode:synthetic_book_id" in p.warnings
    assert p.formats and p.formats[0].fmt in {"EPUB"}
    assert p.cover_path is not None and p.cover_path.name.lower().startswith("cover")


def test_sidecar_reader_best_effort_on_mangled_xml(tmp_path: Path) -> None:
    root = tmp_path / "Library"
    book_dir = root / "Someone" / "Broken (2)"
    book_dir.mkdir(parents=True)

    # Not valid XML.
    (book_dir / "metadata.opf").write_text("<package><metadata><dc:title>Oops", encoding="utf-8")

    r = CalibreSidecarReader.from_root(root)
    payloads = list(r.iter_book_payloads())
    assert len(payloads) == 1
    assert payloads[0].title == "Broken (2)"  # folder fallback


def test_open_cover_guardrail(tmp_path: Path) -> None:
    root = tmp_path / "Library"
    root.mkdir()
    outside = tmp_path / "outside.jpg"
    outside.write_bytes(b"x")

    r = CalibreSidecarReader.from_root(root)
    with pytest.raises(CalibreUnsafePathError):
        r.open_cover(outside)
