from __future__ import annotations

import binascii
import importlib
import struct
import zlib
import zipfile
from pathlib import Path

import LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.utils
from LiuXin_alpha.file_formats.odf.dc import Creator, Date, Description, Language, Subject, Title
from LiuXin_alpha.file_formats.odf.draw import Frame, Image
from LiuXin_alpha.file_formats.odf.meta import Keyword, UserDefined
from LiuXin_alpha.file_formats.odf.opendocument import OpenDocumentText
from LiuXin_alpha.file_formats.odf.teletype import addTextToElement
from LiuXin_alpha.file_formats.odf.text import P


def _build_odt_with_metadata(path: Path) -> None:
    doc = OpenDocumentText()
    para = P()
    addTextToElement(para, "odt beta metadata source smoke")
    doc.text.addElement(para)

    doc.meta.addElement(Title(text="Main Beta Title"))
    doc.meta.addElement(Creator(text="Alice and Bob"))
    doc.meta.addElement(Description(text="Unicode: Καλημέρα 世界 🙂"))
    doc.meta.addElement(Subject(text="tag-one, tag-two"))
    doc.meta.addElement(Keyword(text="tag-two;tag-three"))
    doc.meta.addElement(Language(text="en"))
    doc.meta.addElement(Date(text="2024-01-02"))

    doc.meta.addElement(UserDefined(name="opf.authors", valuetype="string", text="Carol & Dan"))
    doc.meta.addElement(UserDefined(name="opf.titlesort", valuetype="string", text="Title, Main"))
    doc.meta.addElement(UserDefined(name="opf.series", valuetype="string", text="Series A"))
    doc.meta.addElement(UserDefined(name="opf.series_index", valuetype="string", text="3,5"))
    doc.meta.addElement(UserDefined(name="opf.metadata", valuetype="boolean", text="true"))
    doc.meta.addElement(UserDefined(name="opf.nocover", valuetype="boolean", text="false"))
    doc.meta.addElement(UserDefined(name="opf.publisher", valuetype="string", text="Éditions Δ"))

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
    addTextToElement(para, "odt beta cover test")
    doc.text.addElement(para)

    doc.meta.addElement(UserDefined(name="opf.metadata", valuetype="boolean", text="true"))
    doc.meta.addElement(UserDefined(name="opf.nocover", valuetype="boolean", text="true" if opf_nocover else "false"))

    href = doc.addPictureFromString(_png_bytes(120, 180), "image/png")
    frame = Frame(name="opf.cover", anchortype="paragraph", width="120pt", height="180pt")
    frame.addElement(Image(href=href, type="simple", show="embed", actuate="onLoad"))
    para.addElement(frame)
    doc.save(path)


def test_odt_beta_module_import_smoke() -> None:
    importlib.import_module("LiuXin_alpha.metadata.file_sources.odt_beta")


def test_odt_beta_extracts_core_fields_and_overrides(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.odt_beta import get_metadata

    path = tmp_path / "odt_beta_md_source.odt"
    _build_odt_with_metadata(path)

    with path.open("rb") as stream:
        mi = get_metadata(stream, extract_cover=False)

    assert mi.title == "Main Beta Title"
    assert mi.authors == ["Carol", "Dan"]
    assert "Καλημέρα" in mi.comments
    assert set(mi.tags) == {"tag-one", "tag-two", "tag-three"}
    assert LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.utils.title_sort == "Title, Main"
    assert mi.series == "Series A"
    assert float(mi.series_index) == 3.5
    assert mi.publisher == "Éditions Δ"


def test_odt_beta_get_metadata_inplace_and_pathlike(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.odt_beta import get_metadata, get_metadata_inplace

    path = tmp_path / "odt_beta_pathlike.odt"
    _build_odt_with_metadata(path)

    from_path = get_metadata(path, extract_cover=False)
    from_inplace = get_metadata_inplace(path, extract_cover=False)

    assert from_path.title == "Main Beta Title"
    assert from_inplace.title == "Main Beta Title"


def test_odt_beta_stream_cursor_is_preserved(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.odt_beta import get_metadata

    path = tmp_path / "odt_beta_cursor_restore.odt"
    _build_odt_with_metadata(path)

    with path.open("rb") as stream:
        stream.seek(19)
        before = stream.tell()
        mi = get_metadata(stream, extract_cover=False)
        after = stream.tell()

    assert mi.title == "Main Beta Title"
    assert before == after == 19


def test_odt_beta_extracts_cover_from_opf_cover_frame(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.odt_beta import get_metadata

    path = tmp_path / "odt_beta_cover.odt"
    _build_odt_with_cover(path, opf_nocover=False)

    with path.open("rb") as stream:
        mi = get_metadata(stream, extract_cover=True)

    assert mi.cover_data is not None
    fmt, raw = mi.cover_data
    assert fmt in {"png", "jpeg", "jpg", "gif", "webp"}
    assert isinstance(raw, (bytes, bytearray))
    assert len(raw) > 32


def test_odt_beta_respects_opf_nocover(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.odt_beta import get_metadata

    path = tmp_path / "odt_beta_nocover.odt"
    _build_odt_with_cover(path, opf_nocover=True)

    with path.open("rb") as stream:
        mi = get_metadata(stream, extract_cover=True)

    assert getattr(mi, "cover_data", (None, None))[1] is None


def test_odt_beta_xml_get_bool_compat(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.odt_beta import xml_get_bool
    from LiuXin_alpha.utils.libraries.liuxin_etree import etree

    path = tmp_path / "odt_beta_bool_compat.odt"
    _build_odt_with_metadata(path)

    with zipfile.ZipFile(path, "r") as zf:
        root = etree.fromstring(zf.read("meta.xml"))

    assert xml_get_bool(root, "opf.metadata", False) is True
    assert xml_get_bool(root, "opf.nocover", True) is False
    assert xml_get_bool(root, "does.not.exist", True) is True
