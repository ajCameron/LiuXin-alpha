from __future__ import annotations

import importlib.util
import io
from collections.abc import Mapping
from pathlib import Path

import pytest

from LiuXin_alpha.metadata.metadata import MetaData


def _values(raw):
    if raw is None:
        return []
    if isinstance(raw, Mapping):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    try:
        return list(raw)
    except TypeError:
        return [raw]


def _pdf_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _assemble_pdf(objects: list[bytes], *, info_obj_num: int) -> bytes:
    out = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]

    for i, obj in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n".encode("ascii")
        out += obj
        if not obj.endswith(b"\n"):
            out += b"\n"
        out += b"endobj\n"

    xref_pos = len(out)
    out += f"xref\n0 {len(objects) + 1}\n".encode("ascii")
    out += b"0000000000 65535 f \n"
    for off in offsets[1:]:
        out += f"{off:010d} 00000 n \n".encode("ascii")

    out += (
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R /Info {info_obj_num} 0 R >>\n".encode("ascii")
    )
    out += f"startxref\n{xref_pos}\n%%EOF\n".encode("ascii")
    return bytes(out)


def _build_pdf(
    *,
    title: str = "Info Title",
    author: str = "Alice Example",
    subject: str = "Info Subject",
    keywords: str = "tag-one,tag-two",
    creator: str = "Creator Tool",
    producer: str = "Producer Tool",
    xmp_xml: str | None = None,
) -> bytes:
    pages_obj = b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>"
    page_obj = b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 144] /Contents 4 0 R >>"
    content_stream = b"<< /Length 31 >>\nstream\nBT /F1 24 Tf 100 100 Td (Hello) Tj ET\nendstream"
    info_obj = (
        f"<< /Title ({_pdf_escape(title)}) "
        f"/Author ({_pdf_escape(author)}) "
        f"/Subject ({_pdf_escape(subject)}) "
        f"/Keywords ({_pdf_escape(keywords)}) "
        f"/Creator ({_pdf_escape(creator)}) "
        f"/Producer ({_pdf_escape(producer)}) >>"
    ).encode("utf-8")

    if xmp_xml is None:
        catalog_obj = b"<< /Type /Catalog /Pages 2 0 R >>"
        objects = [catalog_obj, pages_obj, page_obj, content_stream, info_obj]
        return _assemble_pdf(objects, info_obj_num=5)

    xmp_bytes = xmp_xml.encode("utf-8")
    metadata_obj = (
        f"<< /Type /Metadata /Subtype /XML /Length {len(xmp_bytes)} >>\nstream\n".encode("ascii")
        + xmp_bytes
        + b"\nendstream"
    )
    catalog_obj = b"<< /Type /Catalog /Pages 2 0 R /Metadata 5 0 R >>"
    objects = [catalog_obj, pages_obj, page_obj, content_stream, metadata_obj, info_obj]
    return _assemble_pdf(objects, info_obj_num=6)


def _build_pdf_with_raw_info(info_obj: bytes) -> bytes:
    pages_obj = b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>"
    page_obj = b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 144] /Contents 4 0 R >>"
    content_stream = b"<< /Length 31 >>\nstream\nBT /F1 24 Tf 100 100 Td (Hello) Tj ET\nendstream"
    catalog_obj = b"<< /Type /Catalog /Pages 2 0 R >>"
    objects = [catalog_obj, pages_obj, page_obj, content_stream, info_obj]
    return _assemble_pdf(objects, info_obj_num=5)


def _xmp_unicode_packet() -> str:
    return """<?xpacket begin="﻿" id="W5M0MpCehiHzreSzNTczkc9d"?>
<x:xmpmeta xmlns:x="adobe:ns:meta/">
  <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
    <rdf:Description rdf:about=""
      xmlns:dc="http://purl.org/dc/elements/1.1/"
      xmlns:xmpMM="http://ns.adobe.com/xap/1.0/mm/">
      <dc:title><rdf:Alt><rdf:li xml:lang="x-default">XMP Title — 測試 😀</rdf:li></rdf:Alt></dc:title>
      <dc:creator><rdf:Seq><rdf:li>Alice XMP</rdf:li><rdf:li>Боб XMP</rdf:li></rdf:Seq></dc:creator>
      <dc:subject><rdf:Bag><rdf:li>tag-xmp</rdf:li><rdf:li>タグ</rdf:li></rdf:Bag></dc:subject>
      <dc:description><rdf:Alt><rdf:li xml:lang="x-default">XMP comment Καλημέρα</rdf:li></rdf:Alt></dc:description>
      <xmpMM:DocumentID>uuid:12345678-9abc-def0-1234-56789abcdef0</xmpMM:DocumentID>
    </rdf:Description>
  </rdf:RDF>
</x:xmpmeta>
<?xpacket end="w"?>"""


def _metadata_snapshot(md) -> dict:
    return {
        "title": md.title,
        "authors": sorted(_values(getattr(md, "authors", None))),
        "tags": sorted(_values(getattr(md, "tags", None))),
        "comments": sorted(_values(getattr(md, "comments", None))),
        "publisher": sorted(_values(getattr(md, "publisher", None))),
        "producers": sorted(_values(getattr(md, "producers", None))),
        "isbn": sorted(_values(getattr(md, "isbn", None))),
        "uuid": sorted(_values(getattr(md, "uuid", None))),
        "identifiers": {
            str(key): sorted(str(v) for v in value)
            for key, value in sorted((getattr(md, "get_identifiers", lambda: {})() or {}).items())
        },
    }


def test_pdf_metadata_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.pdf as pdf_md

    assert pdf_md is not None


def test_pdf_reader_plugin_is_available_and_preserves_stream_position() -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    payload = _build_pdf(title="Reader Title")
    plugins = get_metadata_reader_plugins()
    pdf_cls = next((p for p in plugins if p.__name__ == "PDFMetadataReader"), None)
    assert pdf_cls is not None

    stream = io.BytesIO(payload)
    stream.seek(11)
    reader = pdf_cls(None)
    md = reader.get_metadata(stream=stream, ftype="pdf")

    assert "Reader Title" in md.title
    assert stream.tell() == 11


def test_pdf_get_metadata_parses_info_dict_unicode() -> None:
    from LiuXin_alpha.metadata.file_sources.pdf import get_metadata

    payload = _build_pdf(
        title="Info Title — café 日本語 😀",
        author="Renée Faßbinder & 李白",
        subject="Info Subject",
        keywords="tag-one, Κατηγορία;emoji😀,9780306406157",
    )
    md = get_metadata(io.BytesIO(payload))

    assert "Info Title" in md.title
    assert _values(md.authors) == ["Renée Faßbinder", "李白"]
    tags = {x.lower() for x in _values(md.tags)}
    assert tags >= {"tag-one", "κατηγορία", "emoji😀", "info subject"}
    assert _values(getattr(md, "isbn", None)) == ["9780306406157"]


def test_pdf_get_metadata_prefers_xmp_when_present() -> None:
    from LiuXin_alpha.metadata.file_sources.pdf import get_metadata

    payload = _build_pdf(
        title="Info Title",
        author="Info Author",
        keywords="info-tag",
        xmp_xml=_xmp_unicode_packet(),
    )
    md = get_metadata(io.BytesIO(payload))

    assert md.title == "XMP Title — 測試 😀"
    assert _values(md.authors) == ["Alice XMP", "Боб XMP"]
    assert set(_values(md.tags)) >= {"tag-xmp", "タグ"}
    assert "XMP comment" in _values(md.comments)[0]
    assert _values(getattr(md, "uuid", None)) == ["12345678-9abc-def0-1234-56789abcdef0"]


def test_pdf_get_metadata_inplace_pathlike(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.pdf import get_metadata_inplace

    path = tmp_path / "fixture.pdf"
    path.write_bytes(_build_pdf(title="Path Title", author="Path Author"))
    md = get_metadata_inplace(path)

    assert md.title == "Path Title"
    assert _values(md.authors) == ["Path Author"]


def test_pdf_invalid_payload_raises_by_default_and_can_opt_into_fallback() -> None:
    from LiuXin_alpha.metadata.file_sources.pdf import PdfParseError, get_metadata

    with pytest.raises(PdfParseError):
        get_metadata(io.BytesIO(b"this is not a pdf"))

    md = get_metadata(io.BytesIO(b"this is not a pdf"), fallback_on_parse_error=True)
    assert md.title == "Unknown"
    assert _values(md.authors) == ["Unknown Author"]


def test_pdf_set_metadata_backend_fallback_behavior() -> None:
    from LiuXin_alpha.metadata.file_sources.pdf import set_metadata

    payload = _build_pdf(title="Seed", author="Seed Author")
    stream = io.BytesIO(payload)
    mi = MetaData()
    mi.title = "Updated Title"
    mi.authors = "Updated Author"

    if importlib.util.find_spec("pypdf") is None:
        with pytest.raises(RuntimeError, match="pypdf"):
            set_metadata(stream, mi)
    else:
        set_metadata(stream, mi)
        stream.seek(0)
        updated = stream.read()
        assert updated.startswith(b"%PDF-")


def test_pdf_set_metadata_roundtrip_unicode_torture_if_backend_available() -> None:
    pytest.importorskip("pypdf")
    from LiuXin_alpha.metadata.file_sources.pdf import get_metadata, set_metadata

    stream = io.BytesIO(_build_pdf(title="Seed", author="Seed Author"))
    mi = MetaData()
    mi.title = "Roundtrip Unicode Torture — café Καλημέρα 日本語 😀"
    mi.authors = ["Alice Δ", "李白", "Боб"]
    mi.tags = ["rt-tag", "δοκιμή", "漢字", "emoji😀"]
    mi.comments = "Comments e\u0301 / é / 👩🏽\u200d💻"
    mi.publisher = "Publisher 測試"

    set_metadata(stream, mi)
    stream.seek(0)
    md = get_metadata(stream)

    assert "Roundtrip Unicode Torture" in md.title
    assert any("Alice" in str(author) for author in _values(getattr(md, "authors", None)))
    tags = {str(tag).casefold() for tag in _values(getattr(md, "tags", None))}
    assert "rt-tag" in tags
    # The lightweight local parser treats PDF /Subject as a tag-like field.
    assert any("comments" in str(tag).casefold() for tag in tags)


def test_pdf_set_metadata_invalid_payload_raises_if_backend_available() -> None:
    pytest.importorskip("pypdf")
    from LiuXin_alpha.metadata.file_sources.pdf import set_metadata

    stream = io.BytesIO(b"not-a-pdf")
    mi = MetaData()
    mi.title = "x"
    mi.authors = "y"

    with pytest.raises(Exception):
        set_metadata(stream, mi)


def test_pdf_unicode_torture_info_dict_literal_strings() -> None:
    from LiuXin_alpha.metadata.file_sources.pdf import get_metadata

    payload = _build_pdf(
        title="(Torture) \\ path — e\u0301 vs é — 👩🏽\u200d💻🚀",
        author="Zoë Δ & 李白 & مريم",
        subject="موضوع",
        keywords="café;Καλημέρα,こんにちは,emoji😀,9780306406157,10.5555/12345678",
    )
    md = get_metadata(io.BytesIO(payload))

    assert md.title == "(Torture) \\ path — é vs é — 👩🏽‍💻🚀"
    assert _values(md.authors) == ["Zoë Δ", "李白", "مريم"]
    tags = {x.casefold() for x in _values(md.tags)}
    assert {"café", "καλημέρα", "こんにちは", "emoji😀", "موضوع"} <= tags
    assert _values(getattr(md, "isbn", None)) == ["9780306406157"]
    assert "10.5555/12345678" in {str(x) for x in md.get_identifiers().get("doi", set())}


def test_pdf_unicode_torture_info_dict_utf16_hex_strings() -> None:
    from LiuXin_alpha.metadata.file_sources.pdf import get_metadata

    title = "Hex Title — 你好 😀"
    author = "Αλέξανδρος & יוסף"
    info_obj = (
        f"<< /Title <FEFF{title.encode('utf-16-be').hex().upper()}> "
        f"/Author <FEFF{author.encode('utf-16-be').hex().upper()}> "
        "/Keywords (hex-tag,9780306406157,10.1234/hex) "
        "/Producer (Hex Producer) >>"
    ).encode("ascii")
    payload = _build_pdf_with_raw_info(info_obj)
    md = get_metadata(io.BytesIO(payload))

    assert md.title == title
    assert _values(md.authors) == ["Αλέξανδρος", "יוסף"]
    assert "hex-tag" in {x.casefold() for x in _values(md.tags)}
    assert _values(getattr(md, "isbn", None)) == ["9780306406157"]
    assert "10.1234/hex" in {str(x) for x in md.get_identifiers().get("doi", set())}
    assert _values(getattr(md, "producers", None)) == ["Hex Producer"]


def test_pdf_unicode_torture_xmp_multilingual_and_rtl() -> None:
    from LiuXin_alpha.metadata.file_sources.pdf import get_metadata

    xmp_packet = """<?xpacket begin="﻿" id="W5M0MpCehiHzreSzNTczkc9d"?>
<x:xmpmeta xmlns:x="adobe:ns:meta/">
  <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
    <rdf:Description rdf:about=""
      xmlns:dc="http://purl.org/dc/elements/1.1/"
      xmlns:xmpMM="http://ns.adobe.com/xap/1.0/mm/">
      <dc:title>
        <rdf:Alt>
          <rdf:li xml:lang="en">English fallback</rdf:li>
          <rdf:li xml:lang="x-default">عنوان — XMP ✅</rdf:li>
        </rdf:Alt>
      </dc:title>
      <dc:creator>
        <rdf:Seq>
          <rdf:li>نور الهدى</rdf:li>
          <rdf:li>देवेश 😀</rdf:li>
        </rdf:Seq>
      </dc:creator>
      <dc:subject>
        <rdf:Bag>
          <rdf:li>タグ</rdf:li>
          <rdf:li>δοκιμή</rdf:li>
          <rdf:li>ספר</rdf:li>
        </rdf:Bag>
      </dc:subject>
      <dc:description>
        <rdf:Alt>
          <rdf:li xml:lang="x-default">تعليق XMP متعدد اللغات</rdf:li>
        </rdf:Alt>
      </dc:description>
      <xmpMM:DocumentID>uuid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee</xmpMM:DocumentID>
    </rdf:Description>
  </rdf:RDF>
</x:xmpmeta>
<?xpacket end="w"?>"""
    payload = _build_pdf(
        title="Info Title",
        author="Info Author",
        subject="Info Subject",
        keywords="info-tag",
        creator="Info Creator",
        producer="Info Producer",
        xmp_xml=xmp_packet,
    )
    md = get_metadata(io.BytesIO(payload))

    assert md.title == "عنوان — XMP ✅"
    assert _values(md.authors) == ["نور الهدى", "देवेश 😀"]
    assert set(_values(md.tags)) >= {"タグ", "δοκιμή", "ספר"}
    assert "تعليق XMP" in _values(md.comments)[0]
    assert _values(getattr(md, "uuid", None)) == ["aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"]


@pytest.mark.parametrize(
    "token",
    [
        "café",
        "नमस्ते",
        "こんにちは",
        "مرحبا",
        "Здравствуй",
        "👩🏽\u200d💻🚀",
        "e\u0301",
        "𝔘𝔫𝔦𝔠𝔬𝔡𝔢",
        "漢字かな交じり文",
        "𐍈𐐷",
    ],
)
def test_pdf_unicode_torture_deterministic_matrix(token: str) -> None:
    from LiuXin_alpha.metadata.file_sources.pdf import get_metadata

    payload = _build_pdf(
        title=f"Matrix {token}",
        author=f"A {token} & B {token}",
        subject=f"S {token}",
        keywords=f"tag-{token},9780306406157",
    )
    md_1 = get_metadata(io.BytesIO(payload))
    md_2 = get_metadata(io.BytesIO(payload))

    assert _metadata_snapshot(md_1) == _metadata_snapshot(md_2)
    assert token in md_1.title
    authors = _values(md_1.authors)
    assert any(author.startswith("A ") for author in authors)
    assert any(author.startswith("B ") for author in authors)


def test_pdf_invalid_utf8_bytes_in_info_dict_degrades_gracefully() -> None:
    from LiuXin_alpha.metadata.file_sources.pdf import get_metadata

    info_obj = b"<< /Title (Broken\x80Title\xff) /Author (A) /Keywords (tag-one) >>"
    payload = _build_pdf_with_raw_info(info_obj)
    md = get_metadata(io.BytesIO(payload))

    assert "Broken" in md.title
    assert _values(md.authors) == ["A"]
