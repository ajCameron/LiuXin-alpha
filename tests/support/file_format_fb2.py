from __future__ import annotations

import base64
import binascii
import struct
import textwrap
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape

from tests.support.file_format_unicode import COMMON_TEXT_FRAGMENTS, MULTISCRIPT_TEXT


FB2_NS = "http://www.gribuser.ru/xml/fictionbook/2.0"
XLINK_NS = "http://www.w3.org/1999/xlink"

FB2_TITLE = "FB2 Καλημέρα 世界"
FB2_AUTHORS = (
    ("José", "María", "Niño"),
    ("Иван", "", "Петров"),
)
FB2_DESCRIPTION = "FB2 description: مرحبا שלום नमस्ते 你好 cafe\u0301"
FB2_KEYWORDS = "fictionbook, unicode, Κατηγορία, タグ"
FB2_PUBLISHER = "Éditions Δ"
FB2_COVER_ID = "cover_世界"
FB2_EXTRA_BINARY_ID = "illustration_cafe\u0301"


@dataclass(frozen=True)
class FB2Fixture:
    path: Path
    encoding: str
    binary_ids: tuple[str, ...]
    cover_id: str | None
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

    def error(self, message: str = "", *args) -> None:
        self(message, *args)

    def exception(self, message: str = "", *args) -> None:
        self(message, *args)


def png_bytes(width: int = 16, height: int = 16, rgb: tuple[int, int, int] = (95, 120, 175)) -> bytes:
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


def _author_markup(author: tuple[str, str, str]) -> str:
    first, middle, last = author
    middle_markup = f"<middle-name>{_xml_text(middle)}</middle-name>" if middle else ""
    return (
        "<author>"
        f"<first-name>{_xml_text(first)}</first-name>"
        f"{middle_markup}"
        f"<last-name>{_xml_text(last)}</last-name>"
        "</author>"
    )


def _paragraph_markup(lines: Sequence[str]) -> str:
    return "\n".join(f"<p>{_xml_text(line)}</p>" for line in lines)


def _binary_markup(binary_id: str, content_type: str, payload: bytes) -> str:
    encoded = base64.b64encode(payload).decode("ascii")
    wrapped = "\n".join(textwrap.wrap(encoded, width=76))
    return (
        f'<binary id="{_xml_text(binary_id)}" '
        f'content-type="{_xml_text(content_type)}">\n{wrapped}\n</binary>'
    )


def fb2_bytes(
    *,
    lines: Sequence[str] | None = None,
    encoding: str = "utf-8",
    title: str = FB2_TITLE,
    authors: Sequence[tuple[str, str, str]] = FB2_AUTHORS,
    include_cover: bool = True,
    cover_id: str = FB2_COVER_ID,
    cover_data: bytes | None = None,
    extra_binaries: Mapping[str, tuple[str, bytes]] | None = None,
) -> bytes:
    body_lines = tuple(lines or MULTISCRIPT_TEXT.splitlines())
    extra_binaries = dict(extra_binaries or {})
    cover_payload = png_bytes() if cover_data is None else cover_data

    binary_entries: list[tuple[str, str, bytes]] = []
    if include_cover:
        binary_entries.append((cover_id, "image/png", cover_payload))
    binary_entries.extend(
        (binary_id, content_type, payload)
        for binary_id, (content_type, payload) in extra_binaries.items()
    )

    cover_markup = (
        f'<coverpage><image l:href="#{_xml_text(cover_id)}"/></coverpage>'
        if include_cover
        else ""
    )
    section_image = (
        f'<image l:href="#{_xml_text(cover_id)}"/>'
        if include_cover
        else ""
    )
    authors_markup = "\n      ".join(_author_markup(author) for author in authors)
    body_markup = _paragraph_markup(body_lines)
    binary_markup = "\n".join(
        _binary_markup(binary_id, content_type, payload)
        for binary_id, content_type, payload in binary_entries
    )

    xml = f"""<?xml version="1.0" encoding="{_xml_text(encoding)}"?>
<FictionBook xmlns="{FB2_NS}" xmlns:l="{XLINK_NS}">
  <stylesheet type="text/css">
    body {{ font-family: serif; }}
    section.title {{ color: #24476b; }}
  </stylesheet>
  <description>
    <title-info>
      <genre>sf</genre>
      {authors_markup}
      <book-title>{_xml_text(title)}</book-title>
      <annotation><p>{_xml_text(FB2_DESCRIPTION)}</p></annotation>
      <keywords>{_xml_text(FB2_KEYWORDS)}</keywords>
      <date value="2026-05-21">2026</date>
      {cover_markup}
      <lang>en</lang>
    </title-info>
    <document-info>
      <author><first-name>Fixture</first-name><last-name>Builder</last-name></author>
      <program-used>LiuXin test fixture</program-used>
      <date value="2026-05-21">2026-05-21</date>
      <id>urn:uuid:22222222-3333-4444-5555-666666666666</id>
      <version>1.0</version>
    </document-info>
    <publish-info>
      <book-name>{_xml_text(title)}</book-name>
      <publisher>{_xml_text(FB2_PUBLISHER)}</publisher>
      <city>Montréal</city>
      <year>2026</year>
      <isbn>978-1-4028-9462-6</isbn>
    </publish-info>
  </description>
  <body>
    <title><p>{_xml_text(title)}</p></title>
    <section id="intro">
      <title><p>Intro Καλημέρα</p></title>
      {body_markup}
      {section_image}
    </section>
    <section id="notes">
      <title><p>Notes 世界</p></title>
      <p>Fixture tail text keeps nested sections visible.</p>
    </section>
  </body>
  {binary_markup}
</FictionBook>
"""
    return xml.encode(encoding)


def build_unicode_fb2(
    path: Path,
    *,
    lines: Sequence[str] | None = None,
    encoding: str = "utf-8",
    include_cover: bool = True,
    cover_id: str = FB2_COVER_ID,
    cover_data: bytes | None = None,
    extra_binaries: Mapping[str, tuple[str, bytes]] | None = None,
) -> FB2Fixture:
    extra_binaries = dict(extra_binaries or {})
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(
        fb2_bytes(
            lines=lines,
            encoding=encoding,
            include_cover=include_cover,
            cover_id=cover_id,
            cover_data=cover_data,
            extra_binaries=extra_binaries,
        )
    )
    binary_ids = ((cover_id,) if include_cover else ()) + tuple(extra_binaries)
    return FB2Fixture(
        path=path,
        encoding=encoding,
        binary_ids=binary_ids,
        cover_id=cover_id if include_cover else None,
        text_fragments=tuple(COMMON_TEXT_FRAGMENTS),
    )


def parse_fb2(path: Path) -> ET.Element:
    return ET.fromstring(path.read_bytes())


def fb2_body_text(path: Path) -> str:
    root = parse_fb2(path)
    body = root.find(f"{{{FB2_NS}}}body")
    if body is None:
        return ""
    return "\n".join(text for text in body.itertext() if text)


def read_fb2_binary(path: Path, binary_id: str) -> bytes:
    root = parse_fb2(path)
    for elem in root.iter(f"{{{FB2_NS}}}binary"):
        if elem.attrib.get("id") != binary_id:
            continue
        encoded = "".join((elem.text or "").split())
        return base64.b64decode(encoded)
    raise KeyError(binary_id)


def rewrite_fb2_text(
    source: Path,
    target: Path,
    *,
    remove: Sequence[str] = (),
    replace: Mapping[str, str] | None = None,
    append: str = "",
    encoding: str = "utf-8",
) -> bytes:
    text = source.read_bytes().decode(encoding)
    for fragment in remove:
        text = text.replace(fragment, "")
    for old, new in dict(replace or {}).items():
        text = text.replace(old, new)
    if append:
        text += append
    payload = text.encode(encoding)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(payload)
    return payload
