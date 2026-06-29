from __future__ import annotations

import io
import importlib
import struct
import zlib
import binascii
import zipfile
from pathlib import Path

import pytest

from LiuXin_alpha.file_formats.odf.dc import Creator, Date, Description, Language, Subject, Title
from LiuXin_alpha.file_formats.odf.draw import Frame, Image
from LiuXin_alpha.file_formats.odf.meta import Keyword, UserDefined
from LiuXin_alpha.file_formats.odf.opendocument import OpenDocumentText
from LiuXin_alpha.file_formats.odf.teletype import addTextToElement
from LiuXin_alpha.file_formats.odf.text import P
from LiuXin_alpha.utils.libraries.liuxin_etree import etree


def _inject_dc_identifiers(path: Path, identifiers: list[str]) -> None:
    with zipfile.ZipFile(path, "r") as zf:
        members = {info.filename: zf.read(info.filename) for info in zf.infolist()}

    root = etree.fromstring(members["meta.xml"])
    meta_nodes = root.xpath("//*[local-name() = 'meta']")
    assert meta_nodes, "ODT fixture is missing office:meta"
    meta = meta_nodes[0]
    for value in identifiers:
        ident = etree.SubElement(meta, "{http://purl.org/dc/elements/1.1/}identifier")
        ident.text = value
    members["meta.xml"] = etree.tostring(root, encoding="utf-8", xml_declaration=True)

    tmp = path.with_suffix(".tmp.odt")
    with zipfile.ZipFile(tmp, "w") as out:
        for name, data in members.items():
            out.writestr(name, data)
    tmp.replace(path)


def _build_odt_with_metadata(path: Path, *, series_index: str = "3.0") -> None:
    doc = OpenDocumentText()
    para = P()
    addTextToElement(para, "metadata source smoke")
    doc.text.addElement(para)

    doc.meta.addElement(Title(text="Main Title"))
    doc.meta.addElement(Creator(text="Alice and Bob"))
    doc.meta.addElement(Description(text="A multilingual description: Καλημέρα 世界"))
    doc.meta.addElement(Subject(text="tag-one, tag-two"))
    doc.meta.addElement(Keyword(text="tag-two;tag-three"))
    doc.meta.addElement(Language(text="en"))
    doc.meta.addElement(Date(text="2024-01-02"))

    # ODT custom metadata convention used by calibre/LiuXin metadata paths.
    doc.meta.addElement(UserDefined(name="opf.authors", valuetype="string", text="Carol & Dan"))
    doc.meta.addElement(UserDefined(name="opf.titlesort", valuetype="string", text="Title, Main"))
    doc.meta.addElement(UserDefined(name="opf.series", valuetype="string", text="Series A"))
    doc.meta.addElement(UserDefined(name="opf.series_index", valuetype="string", text=series_index))

    doc.save(path)
    _inject_dc_identifiers(path, ["not-an-isbn", "9780306406157"])


def _build_odt_with_unicode_torture_metadata(path: Path) -> None:
    doc = OpenDocumentText()
    para = P()
    addTextToElement(para, "unicode metadata source smoke")
    doc.text.addElement(para)

    doc.meta.addElement(Title(text="主題 🙂 — Καλημέρα — مرحبا — नमस्ते — 漢字"))
    doc.meta.addElement(Creator(text="Renée Faßbinder and 李白"))
    doc.meta.addElement(Description(text="Combining: cafe\u0301 co\u0308perate A\u030A. Emoji: 👩🏽\u200d🔬"))
    doc.meta.addElement(Subject(text="タグ;Κατηγορία;Тег;العربية"))
    doc.meta.addElement(Keyword(text="हिंदी,日本語;العربية"))
    doc.meta.addElement(Language(text="ja"))
    doc.meta.addElement(Date(text="2025-05-06"))

    doc.meta.addElement(UserDefined(name="opf.authors", valuetype="string", text="Renée & 李白 and कुमारी"))
    doc.meta.addElement(UserDefined(name="opf.publisher", valuetype="string", text="Éditions Δ"))
    doc.meta.addElement(UserDefined(name="opf.series", valuetype="string", text="シリーズΩ"))
    doc.meta.addElement(UserDefined(name="opf.series_index", valuetype="string", text="7,5"))

    doc.save(path)


def _png_bytes(width: int, height: int, rgb: tuple[int, int, int] = (180, 90, 40)) -> bytes:
    signature = b"\x89PNG\r\n\x1a\n"

    def chunk(tag: bytes, payload: bytes) -> bytes:
        return (
            struct.pack(">I", len(payload))
            + tag
            + payload
            + struct.pack(">I", binascii.crc32(tag + payload) & 0xFFFFFFFF)
        )

    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    row = bytes([0]) + bytes(rgb) * width
    raw = row * height
    idat = zlib.compress(raw, 9)
    return signature + chunk(b"IHDR", ihdr) + chunk(b"IDAT", idat) + chunk(b"IEND", b"")


def _build_odt_with_cover(path: Path, *, opf_nocover: bool = False) -> None:
    doc = OpenDocumentText()
    para = P()
    addTextToElement(para, "cover test")
    doc.text.addElement(para)

    # ODF custom metadata flags used by cover selection logic.
    doc.meta.addElement(UserDefined(name="opf.metadata", valuetype="boolean", text="true"))
    doc.meta.addElement(UserDefined(name="opf.nocover", valuetype="boolean", text="true" if opf_nocover else "false"))

    href = doc.addPictureFromString(_png_bytes(120, 180), "image/png")
    frame = Frame(name="opf.cover", anchortype="paragraph", width="120pt", height="180pt")
    frame.addElement(Image(href=href, type="simple", show="embed", actuate="onLoad"))
    para.addElement(frame)
    doc.save(path)


def test_odt_metadata_module_import_smoke() -> None:
    importlib.import_module("LiuXin_alpha.metadata.file_sources.odt")


def test_odt_metadata_extracts_core_fields_and_opf_overrides(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.odt import get_metadata

    path = tmp_path / "odt_md_source.odt"
    _build_odt_with_metadata(path)

    with path.open("rb") as stream:
        mi = get_metadata(stream, extract_cover=False)

    assert mi.title == "Main Title"
    assert mi.authors == ["Carol", "Dan"]
    assert mi.comments and "Καλημέρα" in mi.comments
    assert set(mi.tags) == {"tag-one", "tag-two", "tag-three"}
    assert mi.language == "en"
    assert mi.title_sort == "Title, Main"
    assert mi.series == "Series A"
    assert float(mi.series_index) == 3.0
    assert mi.isbn == "9780306406157"


def test_odt_metadata_accepts_comma_series_index(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.odt import get_metadata

    path = tmp_path / "odt_md_series_comma.odt"
    _build_odt_with_metadata(path, series_index="3,5")

    with path.open("rb") as stream:
        mi = get_metadata(stream, extract_cover=False)

    assert float(mi.series_index) == 3.5


def test_odt_metadata_reader_plugin_is_available(tmp_path: Path) -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    path = tmp_path / "odt_md_plugin.odt"
    _build_odt_with_metadata(path)

    plugins = get_metadata_reader_plugins()
    odt_cls = next((p for p in plugins if p.__name__ == "ODTMetadataReader"), None)
    assert odt_cls is not None

    reader = odt_cls(None)
    with path.open("rb") as stream:
        mi = reader.get_metadata(stream=stream, ftype="odt", extract_cover=False)
    assert mi.title == "Main Title"
    assert mi.authors == ["Carol", "Dan"]


def test_odt_metadata_source_accepts_pathlike_and_inplace(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.odt import get_metadata, get_metadata_inplace

    path = tmp_path / "odt_md_pathlike.odt"
    _build_odt_with_metadata(path)

    from_path = get_metadata(path, extract_cover=False)
    from_inplace = get_metadata_inplace(path, extract_cover=False)

    assert from_path.title == "Main Title"
    assert from_inplace.title == "Main Title"


def test_odt_metadata_rejects_malformed_container_and_preserves_cursor() -> None:
    from LiuXin_alpha.metadata.file_sources.odt import OdtFormatError, get_metadata

    stream = io.BytesIO(b"not an odt")
    stream.name = "bad.odt"
    stream.seek(3)

    with pytest.raises(OdtFormatError, match="Not a valid ODT file"):
        get_metadata(stream, extract_cover=False)

    assert stream.tell() == 3


def test_odt_metadata_fallback_is_explicit_opt_in() -> None:
    from LiuXin_alpha.metadata.file_sources.odt import get_metadata

    stream = io.BytesIO(b"not an odt")
    stream.name = "Fallback Title.odt"
    stream.seek(4)

    mi = get_metadata(stream, extract_cover=False, fallback_on_parse_error=True)

    assert mi.title == "Fallback Title"
    assert mi.authors == ["Unknown"]
    assert stream.tell() == 4


def test_odt_metadata_extracts_cover_from_opf_cover_frame(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.odt import get_metadata

    path = tmp_path / "odt_md_cover.odt"
    _build_odt_with_cover(path, opf_nocover=False)

    with path.open("rb") as stream:
        mi = get_metadata(stream, extract_cover=True)

    assert mi.cover_data is not None
    fmt, raw = mi.cover_data
    assert fmt in {"png", "jpeg", "gif", "webp"}
    assert isinstance(raw, (bytes, bytearray))
    assert len(raw) > 32


def test_odt_metadata_respects_opf_nocover(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.odt import get_metadata

    path = tmp_path / "odt_md_nocover.odt"
    _build_odt_with_cover(path, opf_nocover=True)

    with path.open("rb") as stream:
        mi = get_metadata(stream, extract_cover=True)

    assert getattr(mi, "cover_data", (None, None))[1] is None


def test_odt_metadata_unicode_torture_fields(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.odt import get_metadata

    path = tmp_path / "odt_md_unicode_torture.odt"
    _build_odt_with_unicode_torture_metadata(path)

    with path.open("rb") as stream:
        mi = get_metadata(stream, extract_cover=False)

    assert "主題" in mi.title and "🙂" in mi.title
    assert set(mi.authors) == {"Renée", "李白", "कुमारी"}
    assert "Combining: cafe" in mi.comments
    assert "Éditions Δ" == mi.publisher
    assert mi.series == "シリーズΩ"
    assert float(mi.series_index) == 7.5
    assert set(mi.tags) >= {"タグ", "Κατηγορία", "Тег", "العربية", "हिंदी", "日本語"}


def test_odt_metadata_stream_cursor_is_preserved(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.odt import get_metadata

    path = tmp_path / "odt_md_cursor_restore.odt"
    _build_odt_with_metadata(path)

    with path.open("rb") as stream:
        stream.seek(11)
        before = stream.tell()
        mi = get_metadata(stream, extract_cover=False)
        after = stream.tell()

    assert mi.title == "Main Title"
    assert before == after == 11


def test_odt_metadata_recover_parses_mildly_malformed_meta_xml_when_lxml_available(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.odt import get_metadata
    from LiuXin_alpha.utils.libraries import liuxin_etree

    if not getattr(liuxin_etree, "LXML_AVAILABLE", False):
        return

    path = tmp_path / "odt_md_recoverable_meta_xml.odt"
    _build_odt_with_metadata(path)

    with zipfile.ZipFile(path, "r") as zf:
        members = {info.filename: zf.read(info.filename) for info in zf.infolist()}
    meta_xml = members["meta.xml"].decode("utf-8", "replace")
    # Broken tail after the closing root keeps the XML recoverable under lxml's
    # recover parser while strict parsing fails.
    members["meta.xml"] = (meta_xml + "<broken-tail").encode("utf-8")

    tmp = path.with_suffix(".tmp.odt")
    with zipfile.ZipFile(tmp, "w") as out:
        for name, data in members.items():
            out.writestr(name, data)
    tmp.replace(path)

    with path.open("rb") as stream:
        mi = get_metadata(stream, extract_cover=False)

    assert mi.title == "Main Title"
    assert mi.authors == ["Carol", "Dan"]
