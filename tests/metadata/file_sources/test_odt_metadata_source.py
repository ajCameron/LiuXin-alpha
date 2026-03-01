from __future__ import annotations

import importlib
import struct
import zlib
import binascii
import zipfile
from pathlib import Path

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
