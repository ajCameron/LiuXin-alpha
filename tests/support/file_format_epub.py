from __future__ import annotations

import binascii
import posixpath
import struct
import zipfile
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape

from tests.support.file_format_unicode import COMMON_TEXT_FRAGMENTS, MULTISCRIPT_TEXT


EPUB_TITLE = "EPUB Καλημέρα 世界"
EPUB_AUTHORS = ("José", "Иван")
EPUB_DESCRIPTION = "EPUB container description: مرحبا שלום नमस्ते 你好 cafe\u0301"
EPUB_IDENTIFIER = "urn:uuid:11111111-2222-3333-4444-555555555555"
EPUB_PUBLISHER = "Éditions Δ"
EPUB_SUBJECT = "containers, unicode, Κατηγορία, タグ"
EPUB_IMAGE_MEMBER = "OPS/images/深/cover_世界.png"


@dataclass(frozen=True)
class EPUBFixture:
    path: Path
    opf_path: str
    chapter_members: tuple[str, ...]
    asset_members: tuple[str, ...]
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


def png_bytes(width: int = 16, height: int = 16, rgb: tuple[int, int, int] = (70, 130, 170)) -> bytes:
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


def _opf_href(opf_path: str, member_name: str) -> str:
    base = posixpath.dirname(opf_path)
    if not base:
        return member_name
    rel = posixpath.relpath(member_name, base)
    return rel


def _xml_text(text: str) -> str:
    return escape(text, {'"': "&quot;"})


def build_unicode_epub(
    path: Path,
    *,
    lines: Sequence[str] | None = None,
    opf_path: str = "OPS/content.opf",
    include_image: bool = True,
    extra_assets: Mapping[str, tuple[str, bytes]] | None = None,
) -> EPUBFixture:
    body_lines = tuple(lines or MULTISCRIPT_TEXT.splitlines())
    extra_assets = dict(extra_assets or {})
    opf_base = posixpath.dirname(opf_path)
    chapter_member = posixpath.join(opf_base, "text/chapter_Καλημέρα.xhtml") if opf_base else "text/chapter_Καλημέρα.xhtml"
    css_member = posixpath.join(opf_base, "styles/main.css") if opf_base else "styles/main.css"
    ncx_member = posixpath.join(opf_base, "toc.ncx") if opf_base else "toc.ncx"
    image_member = EPUB_IMAGE_MEMBER if include_image else ""
    if include_image and opf_base != "OPS":
        image_member = posixpath.join(opf_base, "images/深/cover_世界.png") if opf_base else "images/深/cover_世界.png"

    chapter_href = _opf_href(opf_path, chapter_member)
    css_href = _opf_href(opf_path, css_member)
    ncx_href = _opf_href(opf_path, ncx_member)
    image_href = _opf_href(opf_path, image_member) if include_image else ""
    image_from_chapter = posixpath.relpath(image_member, posixpath.dirname(chapter_member)) if include_image else ""
    css_from_chapter = posixpath.relpath(css_member, posixpath.dirname(chapter_member))

    paragraph_markup = "\n".join(f"<p>{_xml_text(line)}</p>" for line in body_lines)
    image_markup = (
        f'<p><img src="{_xml_text(image_from_chapter)}" alt="封面 世界"/></p>'
        if include_image
        else ""
    )
    chapter = f"""<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">
  <head>
    <title>{_xml_text(EPUB_TITLE)}</title>
    <link rel="stylesheet" type="text/css" href="{_xml_text(css_from_chapter)}"/>
  </head>
  <body>
    <h1 id="intro">{_xml_text(EPUB_TITLE)} 👩🏽‍💻</h1>
    {paragraph_markup}
    {image_markup}
  </body>
</html>
""".encode("utf-8")

    manifest_image = (
        f'<item id="cover-image" href="{_xml_text(image_href)}" media-type="image/png"/>'
        if include_image
        else ""
    )
    cover_meta = '<meta name="cover" content="cover-image"/>' if include_image else ""
    guide_cover = (
        f'<guide><reference type="cover" title="Cover" href="{_xml_text(image_href)}"/></guide>'
        if include_image
        else "<guide/>"
    )
    creators = "".join(
        f'<dc:creator opf:role="aut">{_xml_text(author)}</dc:creator>' for author in EPUB_AUTHORS
    )
    extra_manifest_items = "\n".join(
        (
            '<item id="extra-%d" href="%s" media-type="%s"/>'
            % (idx, _xml_text(_opf_href(opf_path, member_name)), _xml_text(media_type))
        )
        for idx, (member_name, (media_type, _data)) in enumerate(extra_assets.items(), start=1)
    )
    opf = f"""<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="BookId">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:opf="http://www.idpf.org/2007/opf">
    <dc:title>{_xml_text(EPUB_TITLE)}</dc:title>
    {creators}
    <dc:language>en</dc:language>
    <dc:identifier id="BookId">{_xml_text(EPUB_IDENTIFIER)}</dc:identifier>
    <dc:description>{_xml_text(EPUB_DESCRIPTION)}</dc:description>
    <dc:publisher>{_xml_text(EPUB_PUBLISHER)}</dc:publisher>
    <dc:subject>{_xml_text(EPUB_SUBJECT)}</dc:subject>
    {cover_meta}
  </metadata>
  <manifest>
    <item id="ncx" href="{_xml_text(ncx_href)}" media-type="application/x-dtbncx+xml"/>
    <item id="style" href="{_xml_text(css_href)}" media-type="text/css"/>
    <item id="chapter" href="{_xml_text(chapter_href)}" media-type="application/xhtml+xml"/>
    {manifest_image}
    {extra_manifest_items}
  </manifest>
  <spine toc="ncx">
    <itemref idref="chapter"/>
  </spine>
  {guide_cover}
</package>
""".encode("utf-8")

    container_xml = f"""<?xml version="1.0" encoding="utf-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="{_xml_text(opf_path)}" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
""".encode("utf-8")

    ncx = f"""<?xml version="1.0" encoding="utf-8"?>
<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/" version="2005-1">
  <head>
    <meta name="dtb:uid" content="{_xml_text(EPUB_IDENTIFIER)}"/>
  </head>
  <docTitle><text>{_xml_text(EPUB_TITLE)}</text></docTitle>
  <navMap>
    <navPoint id="nav1" playOrder="1">
      <navLabel><text>{_xml_text(EPUB_TITLE)}</text></navLabel>
      <content src="{_xml_text(chapter_href)}#intro"/>
    </navPoint>
  </navMap>
</ncx>
""".encode("utf-8")

    css = "body { font-family: serif; } h1 { color: #24476b; }\n".encode("utf-8")

    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as zf:
        mimetype = zipfile.ZipInfo("mimetype")
        mimetype.compress_type = zipfile.ZIP_STORED
        zf.writestr(mimetype, b"application/epub+zip")

        for name, data in (
            ("META-INF/container.xml", container_xml),
            (opf_path, opf),
            (chapter_member, chapter),
            (css_member, css),
            (ncx_member, ncx),
        ):
            info = zipfile.ZipInfo(name)
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, data)

        if include_image:
            info = zipfile.ZipInfo(image_member)
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, png_bytes())

        for member_name, (_media_type, data) in extra_assets.items():
            info = zipfile.ZipInfo(member_name)
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, data)

    asset_members = (
        (css_member, ncx_member)
        + ((image_member,) if include_image else ())
        + tuple(extra_assets)
    )
    return EPUBFixture(
        path=path,
        opf_path=opf_path,
        chapter_members=(chapter_member,),
        asset_members=asset_members,
        text_fragments=tuple(COMMON_TEXT_FRAGMENTS),
    )


def zip_members(path: Path) -> tuple[str, ...]:
    with zipfile.ZipFile(path, "r") as zf:
        return tuple(info.filename for info in zf.infolist())


def read_epub_member(path: Path, member: str) -> bytes:
    with zipfile.ZipFile(path, "r") as zf:
        return zf.read(member)


def read_container_opf_path(path: Path) -> str:
    root = ET.fromstring(read_epub_member(path, "META-INF/container.xml"))
    for node in root.iter():
        if node.tag.rsplit("}", 1)[-1] != "rootfile":
            continue
        if node.attrib.get("media-type") == "application/oebps-package+xml":
            full_path = node.attrib.get("full-path")
            if full_path:
                return full_path
    raise AssertionError("EPUB container.xml did not contain an OPF rootfile")


def rewrite_epub_zip(
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
