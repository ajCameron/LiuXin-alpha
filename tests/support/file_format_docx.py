from __future__ import annotations

import binascii
import struct
import zipfile
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape

from tests.support.file_format_unicode import COMMON_TEXT_FRAGMENTS, MULTISCRIPT_TEXT


DOCX_TITLE = "DOCX Καλημέρα 世界"
DOCX_AUTHORS = ("José", "Иван")
DOCX_DESCRIPTION = "DOCX container description: مرحبا שלום नमस्ते 你好 cafe\u0301"
DOCX_PUBLISHER = "Éditions Δ"
DOCX_SUBJECT = "containers, unicode"
DOCX_KEYWORDS = "Κατηγορία, タグ, cafe\u0301"
DOCX_IMAGE_MEMBER = "word/media/深/cover_世界.png"


@dataclass(frozen=True)
class DOCXFixture:
    path: Path
    document_member: str
    styles_member: str
    metadata_members: tuple[str, ...]
    media_members: tuple[str, ...]
    text_fragments: tuple[str, ...]


class NullLog:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def __call__(self, message: str = "", *args) -> None:
        self.messages.append(message % args if args else message)

    def debug(self, message: str = "", *args) -> None:
        self(message, *args)

    def info(self, message: str = "", *args) -> None:
        self(message, *args)

    def warning(self, message: str = "", *args) -> None:
        self(message, *args)

    warn = warning

    def exception(self, message: str = "", *args) -> None:
        self(message, *args)


def png_bytes(width: int = 16, height: int = 16, rgb: tuple[int, int, int] = (120, 88, 180)) -> bytes:
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
    return signature + chunk(b"IHDR", ihdr) + chunk(b"IDAT", zlib.compress(raw, 9)) + chunk(b"IEND", b"")


def _xml_text(text: str) -> str:
    return escape(text, {'"': "&quot;"})


def _paragraph(text: str) -> str:
    return f'<w:p><w:r><w:t xml:space="preserve">{_xml_text(text)}</w:t></w:r></w:p>'


def _inline_image() -> str:
    return """
<w:p>
  <w:r>
    <w:drawing>
      <wp:inline>
        <wp:extent cx="304800" cy="304800"/>
        <wp:docPr id="1" name="cover_世界.png" descr="封面 世界"/>
        <a:graphic>
          <a:graphicData uri="http://schemas.openxmlformats.org/drawingml/2006/picture">
            <pic:pic>
              <pic:nvPicPr>
                <pic:cNvPr id="0" name="cover_世界.png" descr="封面 世界"/>
                <pic:cNvPicPr/>
              </pic:nvPicPr>
              <pic:blipFill>
                <a:blip r:embed="rIdImage1"/>
                <a:stretch><a:fillRect/></a:stretch>
              </pic:blipFill>
              <pic:spPr>
                <a:xfrm>
                  <a:off x="0" y="0"/>
                  <a:ext cx="304800" cy="304800"/>
                </a:xfrm>
                <a:prstGeom prst="rect"><a:avLst/></a:prstGeom>
              </pic:spPr>
            </pic:pic>
          </a:graphicData>
        </a:graphic>
      </wp:inline>
    </w:drawing>
  </w:r>
</w:p>
"""


def _content_types(extra_assets: Mapping[str, tuple[str, bytes]], include_image: bool) -> bytes:
    defaults = [
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>',
        '<Default Extension="xml" ContentType="application/xml"/>',
    ]
    if include_image or any(name.rpartition(".")[-1].lower() == "png" for name in extra_assets):
        defaults.append('<Default Extension="png" ContentType="image/png"/>')

    overrides = [
        '<Override PartName="/word/document.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>',
        '<Override PartName="/word/styles.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>',
        '<Override PartName="/docProps/core.xml" '
        'ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>',
        '<Override PartName="/docProps/app.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>',
    ]
    for member_name, (content_type, _data) in extra_assets.items():
        if content_type:
            overrides.append(
                '<Override PartName="/%s" ContentType="%s"/>'
                % (_xml_text(member_name), _xml_text(content_type))
            )

    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        + "".join(defaults)
        + "".join(overrides)
        + "</Types>"
    ).encode("utf-8")


def build_unicode_docx(
    path: Path,
    *,
    lines: Sequence[str] | None = None,
    include_image: bool = True,
    extra_assets: Mapping[str, tuple[str, bytes]] | None = None,
) -> DOCXFixture:
    body_lines = tuple(lines or MULTISCRIPT_TEXT.splitlines())
    extra_assets = dict(extra_assets or {})
    document_member = "word/document.xml"
    styles_member = "word/styles.xml"
    metadata_members = ("docProps/core.xml", "docProps/app.xml")
    media_members = (DOCX_IMAGE_MEMBER,) if include_image else ()

    paragraphs = "\n".join(_paragraph(line) for line in body_lines)
    image_markup = (
        _paragraph("Archive image holder 画像") + _inline_image()
        if include_image
        else ""
    )
    document = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document
  xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"
  xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"
  xmlns:wp="http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing"
  xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main"
  xmlns:pic="http://schemas.openxmlformats.org/drawingml/2006/picture">
  <w:body>
    {_paragraph(DOCX_TITLE)}
    {paragraphs}
    {image_markup}
    <w:sectPr/>
  </w:body>
</w:document>
""".encode("utf-8")

    styles = b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:styles xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:docDefaults>
    <w:pPrDefault><w:pPr><w:spacing w:before="0" w:after="0"/><w:ind w:left="0" w:right="0" w:firstLine="0"/></w:pPr></w:pPrDefault>
    <w:rPrDefault><w:rPr><w:lang w:val="en-US"/></w:rPr></w:rPrDefault>
  </w:docDefaults>
</w:styles>
"""
    package_rels = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="{document_member}"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="{metadata_members[0]}"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="{metadata_members[1]}"/>
</Relationships>
""".encode("utf-8")

    document_relationships = [
        '<Relationship Id="rIdStyles" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>',
    ]
    if include_image:
        document_relationships.append(
            '<Relationship Id="rIdImage1" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" '
            'Target="media/深/cover_世界.png"/>'
        )
    document_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        + "".join(document_relationships)
        + "</Relationships>"
    ).encode("utf-8")

    creators = " & ".join(DOCX_AUTHORS)
    core_props = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties
  xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties"
  xmlns:dc="http://purl.org/dc/elements/1.1/"
  xmlns:dcterms="http://purl.org/dc/terms/"
  xmlns:dcmitype="http://purl.org/dc/dcmitype/"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:title>{_xml_text(DOCX_TITLE)}</dc:title>
  <dc:creator>{_xml_text(creators)}</dc:creator>
  <dc:subject>{_xml_text(DOCX_SUBJECT)}</dc:subject>
  <dc:description>{_xml_text(DOCX_DESCRIPTION)}</dc:description>
  <dc:language>en-US</dc:language>
  <cp:keywords>{_xml_text(DOCX_KEYWORDS)}</cp:keywords>
</cp:coreProperties>
""".encode("utf-8")

    app_props = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties">
  <Company>{_xml_text(DOCX_PUBLISHER)}</Company>
</Properties>
""".encode("utf-8")

    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as zf:
        for name, data in (
            ("[Content_Types].xml", _content_types(extra_assets, include_image)),
            ("_rels/.rels", package_rels),
            (document_member, document),
            ("word/_rels/document.xml.rels", document_rels),
            (styles_member, styles),
            (metadata_members[0], core_props),
            (metadata_members[1], app_props),
        ):
            info = zipfile.ZipInfo(name)
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, data)

        if include_image:
            info = zipfile.ZipInfo(DOCX_IMAGE_MEMBER)
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, png_bytes())

        for member_name, (_content_type, data) in extra_assets.items():
            info = zipfile.ZipInfo(member_name)
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, data)

    return DOCXFixture(
        path=path,
        document_member=document_member,
        styles_member=styles_member,
        metadata_members=metadata_members,
        media_members=media_members + tuple(extra_assets),
        text_fragments=tuple(COMMON_TEXT_FRAGMENTS),
    )


def zip_members(path: Path) -> tuple[str, ...]:
    with zipfile.ZipFile(path, "r") as zf:
        return tuple(info.filename for info in zf.infolist())


def read_docx_member(path: Path, member: str) -> bytes:
    with zipfile.ZipFile(path, "r") as zf:
        return zf.read(member)


def document_text(path: Path, member: str = "word/document.xml") -> str:
    root = ET.fromstring(read_docx_member(path, member))
    return "\n".join(node.text or "" for node in root.iter() if node.tag.rsplit("}", 1)[-1] == "t")


def rewrite_docx_zip(
    src: Path,
    dst: Path,
    *,
    remove: Sequence[str] = (),
    replace: Mapping[str, bytes] | None = None,
    add: Mapping[str, bytes] | None = None,
    add_compression: int = zipfile.ZIP_STORED,
) -> None:
    replacements = dict(replace or {})
    additions = dict(add or {})
    removed = set(remove)
    with zipfile.ZipFile(src, "r") as zin, zipfile.ZipFile(dst, "w") as zout:
        for info in zin.infolist():
            if info.filename in removed:
                continue
            data = replacements.pop(info.filename, zin.read(info.filename))
            zout.writestr(info, data)
        for name, data in {**replacements, **additions}.items():
            info = zipfile.ZipInfo(name)
            info.compress_type = add_compression
            zout.writestr(info, data)
