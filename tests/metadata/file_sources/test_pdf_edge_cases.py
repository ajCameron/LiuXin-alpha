from __future__ import annotations

import io
import zlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.metadata.file_sources import pdf
from LiuXin_alpha.metadata.metadata import MetaData


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


def _pdf_with_info(info_obj: bytes, *, trailer: bool = True, extra_objects: list[bytes] | None = None) -> bytes:
    objects = [b"<< /Type /Catalog >>", info_obj]
    objects.extend(extra_objects or [])
    out = bytearray(b"%PDF-1.4\n")
    for i, obj in enumerate(objects, start=1):
        out += f"{i} 0 obj\n".encode() + obj + b"\nendobj\n"
    if trailer:
        out += b"trailer\n<< /Size 3 /Root 1 0 R /Info 2 0 R >>\n%%EOF\n"
    return bytes(out)


class _TextStream(io.StringIO):
    name = "text-stream.pdf"


class _TellSeekBroken:
    name = "broken-stream.pdf"

    def __init__(self, payload):
        self.payload = payload

    def tell(self):
        raise OSError("tell unavailable")

    def seek(self, _pos):
        raise OSError("seek unavailable")

    def read(self):
        return self.payload


class _RestoreBroken(io.BytesIO):
    name = "restore-broken.pdf"

    def seek(self, pos, whence=0):
        if getattr(self, "_break_restore", False) and pos != 0:
            raise OSError("restore unavailable")
        return super().seek(pos, whence)


class _Scalar:
    def __str__(self):
        return "scalar-value"


class _FakeMetadata:
    def __init__(self):
        self.title = None
        self.authors = None
        self.identifiers = {}
        self.finalized = False

    def get_identifiers(self):
        raise RuntimeError("identifier snapshot unavailable")

    def set_identifier(self, _scheme, _value):
        raise RuntimeError("single identifier setter unavailable")

    def set_identifiers(self, identifiers):
        self.identifiers.update(identifiers)

    def finalize(self):
        self.finalized = True
        raise RuntimeError("finalize unavailable")


def test_pdf_low_level_token_parser_unicode_and_malformed_edges() -> None:
    assert pdf._normalize_text(None) == ""
    assert pdf._safe_decode(None) == ""
    assert pdf._safe_decode("  café\t世界  ") == "café 世界"
    assert pdf._safe_decode(b"") == ""
    assert pdf._safe_decode(b"\xff\xfeA\x00B\x00") == "AB"
    assert pdf._safe_decode(b"\xfe\xff\x03\xa9") == "Ω"

    data = b" \t%comment here\r\n/name"
    assert pdf._skip_ws_and_comments(data, 0) == data.index(b"/")
    assert pdf._read_balanced(b"<< /A [1 2] /B << /C 3 >>", 0, b"<<", b">>")[0].startswith(b"<<")

    literal, pos = pdf._read_literal_string(
        rb"(line\n tab\t paren\( nested(inner) octal\101 cont\ \
done)",
        0,
    )
    assert b"line\n" in literal
    assert b"tab\t" in literal
    assert b"paren(" in literal
    assert b"nested(inner)" in literal
    assert b"octalA" in literal
    assert pos > 0

    assert pdf._read_literal_string(rb"(dangling\)", 0)[0] == b"dangling)"
    assert pdf._read_hex_string(b"<4142F> trailing", 0)[0] == b"AB\xf0"
    assert pdf._read_hex_string(b"<not-hex>", 0)[0] == b"not-hex0"
    assert pdf._read_name(b"/A#20Name#ZZ rest", 0)[0] == "A Name#ZZ"

    assert pdf._read_token(b"   ", 0)[0] == "eof"
    assert pdf._read_token(b"/Name", 0)[:2] == ("name", "Name")
    assert pdf._read_token(b"(Value)", 0)[:2] == ("string", b"Value")
    assert pdf._read_token(b"<< /A /B >>", 0)[0] == "dict"
    assert pdf._read_token(b"<4142>", 0)[:2] == ("hex", b"AB")
    assert pdf._read_token(b"[/Name (String) bare]", 0)[0] == "array"
    assert pdf._read_token(b"bare-token", 0)[0] == "bare"

    parsed = pdf._parse_array(b"[/Name (String) <4142> bare-token]")
    assert parsed == ["Name", "String", "AB", "bare-token"]
    assert pdf._parse_array(b"") == []

    assert pdf._parse_pdf_dict(b"no dictionary") == {}
    pdf._parse_pdf_dict(b"<< garbage /Skipped (value) >>")
    parsed_dict = pdf._parse_pdf_dict(b"<< /Title (T) /Mode /UseOutlines /Tags [(a) /b bare] /Nested << /X 1 >> >>")
    assert parsed_dict["Title"] == "T"
    assert parsed_dict["Mode"] == "UseOutlines"
    assert parsed_dict["Tags"] == ["a", "b", "bare"]
    assert parsed_dict["Nested"].startswith(b"<<")


def test_pdf_object_info_stream_and_xmp_extraction_edges() -> None:
    info_obj = b"<< /Title (Heuristic Title) /Author (A) >>"
    payload = _pdf_with_info(info_obj, trailer=False)
    objects = pdf._extract_objects(payload)
    assert pdf._find_info_ref(_pdf_with_info(info_obj)) == (2, 0)
    assert pdf._find_info_ref(b"%PDF /Info 9 2 R") == (9, 2)
    assert pdf._extract_info_dict(payload, objects)["Title"] == "Heuristic Title"
    assert pdf._extract_info_dict(b"%PDF no info", {}) == {}

    assert pdf._extract_stream_data(b"<< /Length 3 >>") is None
    compressed = zlib.compress(b"<xmp>compressed</xmp>")
    assert pdf._extract_stream_data(b"<< /Filter /FlateDecode >>\nstream\n" + compressed + b"\nendstream") == b"<xmp>compressed</xmp>"
    assert pdf._extract_stream_data(b"<< /Filter /FlateDecode >>\nstream\nnot-zlib\nendstream") == b"not-zlib"
    assert pdf._extract_stream_data(b"<< /Filter [/ASCII85Decode /FlateDecode] >>\nstream\n" + compressed + b"\nendstream") == b"<xmp>compressed</xmp>"
    assert pdf._extract_stream_data(b"<< /Filter [/FlateDecode] >>\nstream\nnot-zlib\nendstream") == b"not-zlib"
    assert pdf._extract_stream_data(b"<< /Filter /Other >>\nstream\nraw\nendstream") == b"raw"

    xmp = b"<rdf:RDF><rdf:Description /></rdf:RDF>"
    assert pdf._extract_xmp_packet(xmp, {}) == xmp
    xmp_meta = b"<x:xmpmeta><rdf:RDF /></x:xmpmeta>"
    assert pdf._extract_xmp_packet(b"prefix" + xmp_meta + b"suffix", {}) == xmp_meta
    metadata_obj = {  # explicit metadata stream wins
        (4, 0): b"<< /Type /Metadata /Filter /FlateDecode >>\nstream\n"
        + zlib.compress(xmp_meta)
        + b"\nendstream"
    }
    assert pdf._extract_xmp_packet(b"fallback" + xmp + b"tail", metadata_obj) == xmp_meta


def test_pdf_source_reading_defaults_and_field_value_helpers(tmp_path: Path) -> None:
    path = tmp_path / "名字.pdf"
    path.write_bytes(b"%PDF path")
    assert pdf._source_name(path).endswith("名字.pdf")
    assert pdf._source_name("plain.pdf") == "plain.pdf"
    assert pdf._read_source_bytes(path)[0] == b"%PDF path"
    assert pdf._read_source_bytes(str(path))[0] == b"%PDF path"
    assert pdf._read_source_bytes(b"%PDF bytes")[0] == b"%PDF bytes"
    assert pdf._read_source_bytes(bytearray(b"%PDF bytearray"))[0] == b"%PDF bytearray"

    text_stream = _TextStream("%PDF café")
    text_stream.seek(2)
    assert pdf._read_source_bytes(text_stream) == ("%PDF café".encode(), "text-stream.pdf")
    assert text_stream.tell() == 2

    broken = _TellSeekBroken("%PDF broken")
    assert pdf._read_source_bytes(broken) == (b"%PDF broken", "broken-stream.pdf")

    restore = _RestoreBroken(b"%PDF restore")
    restore.seek(5)
    restore._break_restore = True
    assert pdf._read_source_bytes(restore)[0] == b"%PDF restore"

    with pytest.raises(TypeError):
        pdf._read_source_bytes(object())

    assert pdf._default_metadata(str(tmp_path / "fallback title.pdf")).title == "fallback title"
    assert pdf._field_values(None) == []
    assert pdf._field_values({"α": "ignored"}) == ["α"]
    assert pdf._field_values("tag") == ["tag"]
    assert pdf._field_values(["a", 1]) == ["a", "1"]
    assert pdf._field_values(_Scalar()) == ["scalar-value"]
    assert pdf._first_value(["", " first "]) == "first"
    assert pdf._first_value([]) is None


def test_pdf_info_pair_processing_and_xmp_dict_edges(monkeypatch) -> None:
    md = MetaData()
    assert pdf.process_key_value_pair("", "value", set(), md) == (md, False)
    assert pdf.process_key_value_pair("author", ["Alice", "Bob"], set(), md)[1]
    assert _values(md.authors) == ["Alice, Bob"]
    assert pdf.process_key_value_pair("creator", "Creator Tool", set(), md)[1]
    assert pdf.process_key_value_pair("producer", "Producer Tool", set(), md)[1]
    assert pdf.process_key_value_pair("publisher", "/Publisher Name", set(), md)[1]
    assert _values(md.publisher) == ["Publisher Name"]
    assert pdf.process_key_value_pair("title", "Title Value", set(), md)[1]
    assert pdf.process_key_value_pair("subject", "Subject Tag", set(), md)[1]
    assert pdf.process_key_value_pair("creationdate", "D:20260516102030", set(), md)[1]
    assert pdf.process_key_value_pair("moddate", "D:20260516103030", set(), md)[1]
    assert pdf.process_key_value_pair("pdfversion", "1.7", set(), md)[1]
    assert not pdf.process_key_value_pair("unknown", "value", set(), md)[1]

    events = []
    monkeypatch.setattr(pdf.default_log, "log_variables", lambda *args, **_kwargs: events.append(args))
    md = pdf.process_metadata_info_dict({"CustomCreator": "Creator", "UnknownField": "value"}, MetaData())
    assert _values(md.producers) == ["Creator"]

    xmp_md = MetaData()
    result = pdf.process_xmp_metadata_dict(
        {
            "xapmm": {"DocumentID": "not-a-uuid-prefix"},
            "dc": {
                "title": {"fr": "Titre", "x-default": "Default Title"},
                "creator": "Alice & Bob",
                "publisher": ["", "Publisher XMP"],
                "description": {"fr": "Description FR"},
                "subject": "single-tag",
            },
        },
        xmp_md,
    )
    assert _values(result.uuid) == ["not-a-uuid-prefix"]
    assert result.title == "Default Title"
    assert _values(result.authors) == ["Alice", "Bob"]
    assert _values(result.publisher) == ["Publisher XMP"]
    assert _values(result.comments) == ["Description FR"]
    assert _values(result.tags) == ["single-tag"]
    assert pdf.process_xmp_metadata_dict({"dc": "not-a-dict"}, result) is result


def test_pdf_xmp_parser_no_rdf_and_parse_variants() -> None:
    assert pdf.xmp_to_dict("<x:xmpmeta xmlns:x='adobe:ns:meta/' />") == {}
    xmp = """<x:xmpmeta xmlns:x="adobe:ns:meta/">
    <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
      <rdf:Description xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:custom="urn:custom">
        <dc:title><rdf:Alt><rdf:li xml:lang="x-default">Alt Title</rdf:li></rdf:Alt></dc:title>
        <dc:creator><rdf:Seq><rdf:li>Alice</rdf:li></rdf:Seq></dc:creator>
        <dc:subject><rdf:Bag><rdf:li>tag</rdf:li></rdf:Bag></dc:subject>
        <custom:value>custom text</custom:value>
      </rdf:Description>
    </rdf:RDF>
    </x:xmpmeta>"""
    parsed = pdf.xmp_to_dict(xmp)
    assert parsed["dc"]["title"] == {"x-default": "Alt Title"}
    assert parsed["dc"]["creator"] == ["Alice"]
    assert parsed["dc"]["subject"] == ["tag"]
    assert parsed["urn:custom"]["value"] == "custom text"


def test_pdf_get_metadata_defensive_fallbacks(monkeypatch) -> None:
    events = []
    monkeypatch.setattr(pdf.default_log, "log_exception", lambda *args, **_kwargs: events.append(args))

    empty = pdf.get_metadata(b"")
    assert empty.title == "Unknown"
    assert _values(empty.authors) == ["Unknown Author"]

    payload = _pdf_with_info(b"<< /Title (Fallback Title) /Keywords [(10.5555/fallback) (9780306406157)] >>")
    fake_md = _FakeMetadata()
    monkeypatch.setattr(pdf, "_default_metadata", lambda _source_name="": fake_md)
    monkeypatch.setattr(pdf, "process_metadata_info_dict", lambda _info, md: md)
    monkeypatch.setattr(pdf, "_extract_xmp_packet", lambda *_args: b"<broken")

    md = pdf.get_metadata(payload)
    assert md is fake_md
    assert fake_md.identifiers == {"doi": "10.5555/fallback", "isbn": "9780306406157"}
    assert fake_md.finalized
    assert _values(fake_md.authors) == ["Unknown Author"]
    assert any("embedded PDF XMP" in str(event[0]) for event in events)

    info_calls = {"count": 0}

    def fail_once_info_dict(*_args):
        info_calls["count"] += 1
        if info_calls["count"] == 1:
            raise RuntimeError("info boom")
        return {}

    monkeypatch.setattr(pdf, "_extract_info_dict", fail_once_info_dict)
    pdf.get_metadata(_pdf_with_info(b"<< /Title (Ignored) >>"))
    assert any("PDF info dictionary" in str(event[0]) for event in events)


def test_pdf_writer_dict_tool_read_info_and_page_image_edges(tmp_path: Path, monkeypatch) -> None:
    mi = MetaData()
    mi.title = "Title"
    mi.authors = ["Alice", "Bob"]
    mi.comments = "Comments"
    mi.tags = ["tag-one", "tag-two"]
    mi.producers = "Producer"
    mi.creator_sort = "Creator"
    mi.publisher = "Publisher"
    mi.series = "Series"
    mi.series_index = {"Series": "2"}
    assert pdf._metadata_to_pdf_dict(mi) == {
        "/Title": "Title",
        "/Author": "Alice, Bob",
        "/Subject": "Comments",
        "/Keywords": "tag-one, tag-two",
        "/Producer": "Producer",
        "/Creator": "Creator",
        "/Publisher": "Publisher",
        "/Series": "Series",
        "/SeriesIndex": "Series",
    }

    with pytest.raises(TypeError):
        pdf.set_metadata(object(), mi)
    monkeypatch.setattr(pdf.importlib.util, "find_spec", lambda _name: None)
    with pytest.raises(RuntimeError, match="pypdf"):
        pdf.set_metadata(io.BytesIO(b"%PDF"), mi)

    tool = tmp_path / "pdftoppm"
    tool.write_text("#!/bin/sh\n", encoding="utf-8")
    import LiuXin_alpha.file_formats.pdf.pdftohtml as pdftohtml_mod

    monkeypatch.setattr(pdftohtml_mod, "PDFTOHTML", str(tool), raising=False)
    monkeypatch.setattr(pdf.os.path, "exists", lambda path: path == str(tool))
    assert pdf.get_tool("pdftoppm") == str(tool)
    monkeypatch.setattr(pdf.os.path, "exists", lambda _path: False)
    monkeypatch.setattr(pdf.shutil, "which", lambda name: f"/usr/bin/{name}")
    assert pdf.get_tool("pdftoppm") == "/usr/bin/pdftoppm"

    assert pdf.read_info(tmp_path, get_cover=True) is None
    src = tmp_path / "src.pdf"
    src.write_bytes(_pdf_with_info(b"<< /Title (Read Info) /Author (Alice) /Keywords (tag) /Producer (Tool) >>"))
    info = pdf.read_info(tmp_path, get_cover=False)
    assert info == {"Title": "Read Info", "Author": "Alice", "Keywords": "tag", "Producer": "Tool"}

    monkeypatch.setattr(pdf, "get_tool", lambda _name: None)
    with pytest.raises(RuntimeError, match="pdftoppm"):
        pdf.page_images("in.pdf", str(tmp_path))
    calls = []
    monkeypatch.setattr(pdf, "get_tool", lambda _name: "/bin/pdftoppm")
    monkeypatch.setattr(pdf.subprocess, "check_call", lambda args: calls.append(args))
    pdf.page_images("in.pdf", str(tmp_path), first=2, last=3)
    assert calls and calls[0][0] == "/bin/pdftoppm"
