"""
Exercise PDF Info/XMP parsing, updates and malformed-document fallbacks.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test pdf edge cases through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py
"""
from __future__ import annotations

import base64
import io
import sys
import types
import zlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData as MetaData,
)
from LiuXin_alpha.metadata.file_sources import pdf, pdf_merge


def _values(raw):
    """
    Perform the values test-helper operation with deterministic inputs.

    Example:
        Exercise values through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


    :param raw: Value supplied for raw in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
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
    """
    Perform the pdf with info test-helper operation with deterministic inputs.

    Example:
        Exercise pdf with info through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


    :param info_obj: Value supplied for info obj in the focused test operation.
    :param trailer: Value supplied for trailer in the focused test operation.
    :param extra_objects: Value supplied for extra objects in the focused test
        operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    objects = [b"<< /Type /Catalog >>", info_obj]
    objects.extend(extra_objects or [])
    out = bytearray(b"%PDF-1.4\n")
    for i, obj in enumerate(objects, start=1):
        out += f"{i} 0 obj\n".encode() + obj + b"\nendobj\n"
    if trailer:
        out += b"trailer\n<< /Size 3 /Root 1 0 R /Info 2 0 R >>\n%%EOF\n"
    return bytes(out)


class _TextStream(io.StringIO):
    """
    Provide the TextStream test fixture or double with explicit deterministic behavior.

    Example:
        Exercise TextStream through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py
    """
    name = "text-stream.pdf"


class _TellSeekBroken:
    """
    Provide the TellSeekBroken test fixture or double with explicit deterministic behavior.

    Example:
        Exercise TellSeekBroken through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py
    """
    name = "broken-stream.pdf"

    def __init__(self, payload):
        """
        Initialize the TellSeekBroken test double.

        Example:
            Exercise TellSeekBroken.init through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


        :param payload: Value supplied for payload in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self.payload = payload

    def tell(self):
        """
        Perform the tell test-helper operation with deterministic inputs.

        Example:
            Exercise TellSeekBroken.tell through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        raise OSError("tell unavailable")

    def seek(self, _pos):
        """
        Perform the seek test-helper operation with deterministic inputs.

        Example:
            Exercise TellSeekBroken.seek through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


        :param _pos: Value supplied for pos in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise OSError("seek unavailable")

    def read(self):
        """
        Perform the read test-helper operation with deterministic inputs.

        Example:
            Exercise TellSeekBroken.read through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self.payload


class _RestoreBroken(io.BytesIO):
    """
    Provide the RestoreBroken test fixture or double with explicit deterministic behavior.

    Example:
        Exercise RestoreBroken through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py
    """
    name = "restore-broken.pdf"

    def seek(self, pos, whence=0):
        """
        Perform the seek test-helper operation with deterministic inputs.

        Example:
            Exercise RestoreBroken.seek through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


        :param pos: Value supplied for pos in the focused test operation.
        :param whence: Value supplied for whence in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        if getattr(self, "_break_restore", False) and pos != 0:
            raise OSError("restore unavailable")
        return super().seek(pos, whence)


class _Scalar:
    """
    Provide the Scalar test fixture or double with explicit deterministic behavior.

    Example:
        Exercise Scalar through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py
    """
    def __str__(self):
        """
        Perform the str test-helper operation with deterministic inputs.

        Example:
            Exercise Scalar.str through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return "scalar-value"


class _FakeMetadata:
    """
    Provide the FakeMetadata test fixture or double with explicit deterministic behavior.

    Example:
        Exercise FakeMetadata through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py
    """
    def __init__(self):
        """
        Initialize the FakeMetadata test double.

        Example:
            Exercise FakeMetadata.init through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


        :return: None; the function records state or raises through its assertions.
        """
        self.title = None
        self.authors = None
        self.identifiers = {}
        self.finalized = False

    def get_identifiers(self):
        """
        Return identifiers from deterministic test state.

        Example:
            Exercise FakeMetadata.get identifiers through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        raise RuntimeError("identifier snapshot unavailable")

    def set_identifier(self, _scheme, _value):
        """
        Perform the set identifier test-helper operation with deterministic inputs.

        Example:
            Exercise FakeMetadata.set identifier through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


        :param _scheme: Value supplied for scheme in the focused test operation.
        :param _value: Value supplied for value in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise RuntimeError("single identifier setter unavailable")

    def set_identifiers(self, identifiers):
        """
        Perform the set identifiers test-helper operation with deterministic inputs.

        Example:
            Exercise FakeMetadata.set identifiers through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


        :param identifiers: Value supplied for identifiers in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.identifiers.update(identifiers)

    def finalize(self):
        """
        Perform the finalize test-helper operation with deterministic inputs.

        Example:
            Exercise FakeMetadata.finalize through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        self.finalized = True
        raise RuntimeError("finalize unavailable")


def _contains_forbidden_text_char(text: str) -> bool:
    """
    Perform the contains forbidden text char test-helper operation with deterministic inputs.

    Example:
        Exercise contains forbidden text char through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


    :param text: Value supplied for text in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    for ch in text:
        cp = ord(ch)
        if cp == 0x7F:
            return True
        if cp in (0x9, 0xA, 0xD):
            continue
        if 0x20 <= cp <= 0xD7FF:
            continue
        if 0xE000 <= cp <= 0xFFFD:
            continue
        if 0x10000 <= cp <= 0x10FFFF:
            continue
        return True
    return False


def test_pdf_text_decoding_unicode_and_malformed_edges() -> None:
    """
    Verify that Info strings decode as the PDF specification says, and that nothing raises.

    Strings with a byte order mark are UTF-16 or UTF-8; others are PDFDocEncoding,
    except that valid non-ASCII UTF-8 is read as UTF-8 because many tools write it.

    Example:
        Exercise test pdf text decoding unicode and malformed edges through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.file_formats.pdf.objects import decode_text

    assert pdf._normalize_text(None) == ""
    assert pdf._normalize_text("  café\t世界  ") == "café 世界"
    assert decode_text(b"") == ""
    assert decode_text(b"\xff\xfeA\x00B\x00") == "AB"
    assert decode_text(b"\xfe\xff\x03\xa9") == "Ω"
    assert decode_text(b"\xfe\xff\x03") == "\ufffd"
    assert decode_text(b"\xfe\xff\x00T\x00\x00") == "T"
    assert decode_text(b"\xfe\xff\x00\x1bfr\x00\x1b\x00T") == "T"
    assert decode_text(b"\xef\xbb\xbfcaf\xc3\xa9") == "café"
    assert decode_text("世界".encode()) == "世界"
    # PDFDocEncoding differs from Latin-1 in 0x18-0x1F and 0x80-0xA0.
    assert decode_text(b"caf\xe9 \x84 \x8dq\x8e \x93 \xa0 \x18") == "café — \u201cq\u201d \ufb01 \u20ac \u02d8"


def test_pdf_info_stream_and_xmp_extraction_edges() -> None:
    """
    Verify Info and XMP reading from files without a trailer, with filter chains and with corrupt streams.

    Example:
        Exercise test pdf info stream and xmp extraction edges through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.file_formats.pdf.objects import read_metadata

    # No trailer at all: a scan finds the catalog and a dictionary that looks like Info.
    payload = _pdf_with_info(b"<< /Title (Heuristic Title) /Author (A) >>", trailer=False)
    assert read_metadata(payload).info == {"Title": "Heuristic Title", "Author": "A"}
    assert pdf.get_metadata(payload).title == "Heuristic Title"
    # An outline item has a /Title too, but it is not Info.
    outline_only = _pdf_with_info(b"<< /Title (Chapter 1) /Parent 1 0 R /Dest [3 0 R /Fit] >>", trailer=False)
    assert read_metadata(outline_only).info == {}

    xmp_meta = b"<x:xmpmeta xmlns:x='adobe:ns:meta/'><rdf:RDF xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#'/></x:xmpmeta>"
    compressed = zlib.compress(xmp_meta)
    ascii85 = base64.a85encode(compressed) + b"~>"

    def with_metadata(stream: bytes) -> bytes:
        """
        Build a PDF whose catalog points to a metadata stream.

        Example:
            Exercise with metadata through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


        :param stream: The metadata stream object's source.
        :return: The PDF.
        """
        objects = [b"<< /Type /Catalog /Metadata 3 0 R >>", b"<< /Title (T) >>", stream]
        out = bytearray(b"%PDF-1.4\n")
        for number, body in enumerate(objects, start=1):
            out += f"{number} 0 obj\n".encode() + body + b"\nendobj\n"
        out += b"trailer\n<< /Size 4 /Root 1 0 R /Info 2 0 R >>\n%%EOF\n"
        return bytes(out)

    def stream(header: bytes, data: bytes) -> bytes:
        """
        Build a stream object's source with a correct /Length.

        Example:
            Exercise stream through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


        :param header: The dictionary's entries other than /Length.
        :param data: The stream's data.
        :return: The source.
        """
        return b"<< " + header + b" /Length %d >>\nstream\n" % len(data) + data + b"\nendstream"

    assert read_metadata(with_metadata(stream(b"/Type /Metadata /Filter /FlateDecode", compressed))).xmp == xmp_meta
    chained = stream(b"/Type /Metadata /Filter [/ASCII85Decode /FlateDecode]", ascii85)
    assert read_metadata(with_metadata(chained)).xmp == xmp_meta
    # Corrupt or unknown filters cost the XMP packet, not the Info dictionary.
    for broken in (
        stream(b"/Type /Metadata /Filter /FlateDecode", b"not-zlib"),
        stream(b"/Type /Metadata /Filter /JBIG2Decode", b"raw"),
    ):
        metadata = read_metadata(with_metadata(broken))
        assert metadata.xmp is None
        assert metadata.info == {"Title": "T"}
        assert metadata.problems and metadata.problems[0].startswith("XMP packet:")
    assert pdf.get_metadata(with_metadata(stream(b"/Filter /JBIG2Decode", b"raw"))).title == "T"


def test_pdf_source_reading_defaults_and_field_value_helpers(tmp_path: Path) -> None:
    """
    Verify pdf source reading defaults and field value helpers.

    Example:
        Exercise test pdf source reading defaults and field value helpers through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify pdf info pair processing and xmp dict edges.

    Example:
        Exercise test pdf info pair processing and xmp dict edges through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify pdf xmp parser no rdf and parse variants.

    Example:
        Exercise test pdf xmp parser no rdf and parse variants through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


    :return: None; the function records state or raises through its assertions.
    """
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


@pytest.mark.parametrize("merge", ["newest", "calibre", "union"])
def test_pdf_get_metadata_defensive_fallbacks(monkeypatch, merge: str) -> None:
    """
    Verify pdf get metadata defensive fallbacks, under every merge policy.

    Example:
        Exercise test pdf get metadata defensive fallbacks through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :param merge: The merge policy.
    :return: None; the function records state or raises through its assertions.
    """
    events = []
    monkeypatch.setattr(pdf.default_log, "log_exception", lambda *args, **_kwargs: events.append(args))

    with pytest.raises(pdf.PdfParseError):
        pdf.get_metadata(b"")

    empty = pdf.get_metadata(b"", fallback_on_parse_error=True)
    assert empty.title == "Unknown"
    assert _values(empty.authors) == ["Unknown Author"]

    fake_md = _FakeMetadata()
    monkeypatch.setattr(pdf, "_default_metadata", lambda _source_name="": fake_md)
    monkeypatch.setattr(pdf, "process_metadata_info_dict", lambda _info, md: md)

    def broken_xmp(_packet, **_options):
        """
        Fail as an unparseable XMP packet does.

        Example:
            Exercise test pdf get metadata defensive fallbacks.broken xmp through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


        :param _packet: The packet.
        :param _options: Parser options.
        :return: Never returns.
        """
        raise SyntaxError("broken XMP")

    monkeypatch.setattr(pdf, "xmp_to_dict", broken_xmp)
    xmp = b"<< /Type /Metadata /Length 6 >>\nstream\n<broke\nendstream"
    payload = _pdf_with_info(
        b"<< /Title (Fallback Title) /Keywords [(10.5555/fallback) (9780306406157)] >>",
        extra_objects=[xmp],
    ).replace(b"<< /Type /Catalog >>", b"<< /Type /Catalog /Metadata 3 0 R >>")
    md = pdf.get_metadata(payload, merge=merge)
    assert md is fake_md
    assert fake_md.identifiers == {"doi": "10.5555/fallback", "isbn": "9780306406157"}
    assert fake_md.finalized
    assert _values(fake_md.authors) == ["Unknown Author"]
    assert any("embedded PDF XMP" in str(event[0]) for event in events)

    def fail_info_dict(*_args, **_options):
        """
        Fail as a broken Info mapping does.

        Example:
            Exercise test pdf get metadata defensive fallbacks.fail info dict through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


        :param _args: Value supplied for args in the focused test operation.
        :param _options: Reading options.
        :return: Never returns.
        """
        raise RuntimeError("info boom")

    monkeypatch.setattr(pdf, "process_metadata_info_dict", fail_info_dict)
    monkeypatch.setattr(pdf_merge, "info_fields", fail_info_dict)
    pdf.get_metadata(_pdf_with_info(b"<< /Title (Ignored) >>"), merge=merge)
    assert any("PDF info dictionary" in str(event[0]) for event in events)


def test_pdf_writer_dict_tool_and_page_image_edges(tmp_path: Path, monkeypatch) -> None:
    """
    Verify pdf writer dict tool and page image edges.

    Example:
        Exercise test pdf writer dict tool and page image edges through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
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

    monkeypatch.setattr(pdf, "get_tool", lambda _name: None)
    with pytest.raises(RuntimeError, match="pdftoppm"):
        pdf.page_images("in.pdf", str(tmp_path))
    calls = []
    monkeypatch.setattr(pdf, "get_tool", lambda _name: "/bin/pdftoppm")
    monkeypatch.setattr(pdf.subprocess, "check_call", lambda args: calls.append(args))
    pdf.page_images("in.pdf", str(tmp_path), first=2, last=3)
    assert calls and calls[0][0] == "/bin/pdftoppm"


def test_pdf_metadata_dict_sanitizes_hostile_text_without_mutating_input() -> None:
    """
    Verify pdf metadata dict sanitizes hostile text without mutating input.

    Example:
        Exercise test pdf metadata dict sanitizes hostile text without mutating input through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


    :return: None; the function records state or raises through its assertions.
    """
    title = "PDF\x00Title\ud800 😀"
    authors = ["Alice\x01 One", "Bob\udfff Two"]
    comments = "Comment\x02 with (paren) and \\ slash"
    tags = ["tag\x03one", "emoji 😀"]

    mi = SimpleNamespace(
        title=title,
        authors=authors,
        comments=comments,
        tags=tags,
        producers="Producer\x04Name",
        creator_sort="Creator\x05Name",
        publisher="Pub\x06lisher",
        series="Series\x07Name",
        series_index="2\x08",
    )

    out = pdf._metadata_to_pdf_dict(mi)

    assert out["/Title"] == "PDFTitle 😀"
    assert out["/Author"] == "Alice One, Bob Two"
    assert out["/Subject"] == "Comment with (paren) and \\ slash"
    assert out["/Keywords"] == "tagone, emoji 😀"
    assert out["/Producer"] == "ProducerName"
    assert out["/Creator"] == "CreatorName"
    assert out["/Publisher"] == "Publisher"
    assert out["/Series"] == "SeriesName"
    assert out["/SeriesIndex"] == "2"
    assert not any(_contains_forbidden_text_char(value) for value in out.values())

    assert mi.title == title
    assert mi.authors == authors
    assert mi.comments == comments
    assert mi.tags == tags


def test_pdf_set_metadata_fake_backend_sanitizes_and_rewrites_stream(monkeypatch) -> None:
    """
    Verify pdf set metadata fake backend sanitizes and rewrites stream.

    Example:
        Exercise test pdf set metadata fake backend sanitizes and rewrites stream through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    captured = {}

    class _FakeReader:
        """
        Provide the FakeReader test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test pdf set metadata fake backend sanitizes and rewrites stream.FakeReader through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py
        """
        def __init__(self, stream):
            """
            Initialize the FakeReader test double.

            Example:
                Exercise test pdf set metadata fake backend sanitizes and rewrites stream.FakeReader.init through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


            :param stream: Value supplied for stream in the focused test operation.
            :return: None; the function records state or raises through its assertions.
            """
            captured["reader_payload"] = stream.read()
            self.pages = ["page-one", "page-two"]
            self.metadata = {"/Producer": "Existing Producer"}

    class _FakeWriter:
        """
        Provide the FakeWriter test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test pdf set metadata fake backend sanitizes and rewrites stream.FakeWriter through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py
        """
        def __init__(self):
            """
            Initialize the FakeWriter test double.

            Example:
                Exercise test pdf set metadata fake backend sanitizes and rewrites stream.FakeWriter.init through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


            :return: None; the function records state or raises through its assertions.
            """
            captured["writer"] = self
            self.pages = []
            self.metadata = None

        def add_page(self, page):
            """
            Perform the add page test-helper operation with deterministic inputs.

            Example:
                Exercise test pdf set metadata fake backend sanitizes and rewrites stream.FakeWriter.add page through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


            :param page: Value supplied for page in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            self.pages.append(page)

        def add_metadata(self, metadata):
            """
            Perform the add metadata test-helper operation with deterministic inputs.

            Example:
                Exercise test pdf set metadata fake backend sanitizes and rewrites stream.FakeWriter.add metadata through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


            :param metadata: Metadata container or mapping supplied to the assertion helper.
            :return: The deterministic value, row, identity or collection described above.
            """
            self.metadata = metadata

        def write(self, stream):
            """
            Record a batch mutation and return its configured result.

            Example:
                Exercise test pdf set metadata fake backend sanitizes and rewrites stream.FakeWriter.write through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_pdf_edge_cases.py


            :param stream: Value supplied for stream in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            stream.write(b"%PDF-1.4\nrewritten")

    fake_pypdf = types.ModuleType("pypdf")
    fake_pypdf.PdfReader = _FakeReader
    fake_pypdf.PdfWriter = _FakeWriter

    monkeypatch.setattr(pdf.importlib.util, "find_spec", lambda name: object() if name == "pypdf" else None)
    monkeypatch.setitem(sys.modules, "pypdf", fake_pypdf)

    title = "PDF\x00Title\ud800 😀"
    authors = ["Alice\x01 One", "Bob\udfff Two"]
    mi = MetaData()
    mi.title = title
    mi.authors = authors
    mi.comments = "Comment\x02"
    mi.tags = ["tag\x03one"]

    stream = io.BytesIO(b"%PDF-1.4\noriginal payload")
    stream.seek(5)
    pdf.set_metadata(stream, mi)

    writer = captured["writer"]
    assert captured["reader_payload"].startswith(b"%PDF-1.4")
    assert writer.pages == ["page-one", "page-two"]
    assert writer.metadata["/Producer"] == "Existing Producer"
    assert writer.metadata["/Title"] == "PDFTitle 😀"
    assert writer.metadata["/Author"] == "Alice One, Bob Two"
    assert writer.metadata["/Subject"] == "Comment"
    assert writer.metadata["/Keywords"] == "tagone"
    assert not any(_contains_forbidden_text_char(value) for value in writer.metadata.values())

    assert stream.getvalue() == b"%PDF-1.4\nrewritten"
    assert stream.tell() == 5
    assert mi.title == title
    assert _values(mi.authors) == authors
