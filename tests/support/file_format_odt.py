from __future__ import annotations

import binascii
import struct
import zipfile
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from tests.support.file_format_unicode import COMMON_TEXT_FRAGMENTS, MULTISCRIPT_TEXT


ODT_TITLE = "ODT Καλημέρα 世界"
ODT_AUTHORS = "José and Иван"
ODT_DESCRIPTION = "ODT container description: مرحبا שלום नमस्ते 你好 cafe\u0301"
ODT_IMAGE_BYTES = b"odt-nested-image-\xce\xa9-\xe4\xb8\x96\xe7\x95\x8c"


@dataclass(frozen=True)
class ODTFixture:
    path: Path
    text_fragments: tuple[str, ...]
    picture_members: tuple[str, ...]


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


def png_bytes(width: int = 16, height: int = 16, rgb: tuple[int, int, int] = (90, 120, 180)) -> bytes:
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


def build_unicode_odt(
    path: Path,
    *,
    lines: Sequence[str] | None = None,
    include_image: bool = False,
) -> ODTFixture:
    from LiuXin_alpha.file_formats.odf.dc import Creator, Description, Language, Subject, Title
    from LiuXin_alpha.file_formats.odf.meta import Keyword, UserDefined
    from LiuXin_alpha.file_formats.odf.opendocument import OpenDocumentText
    from LiuXin_alpha.file_formats.odf.teletype import addTextToElement
    from LiuXin_alpha.file_formats.odf.text import P

    doc = OpenDocumentText()
    doc.meta.addElement(Title(text=ODT_TITLE))
    doc.meta.addElement(Creator(text=ODT_AUTHORS))
    doc.meta.addElement(Description(text=ODT_DESCRIPTION))
    doc.meta.addElement(Subject(text="containers, unicode"))
    doc.meta.addElement(Keyword(text="Κατηγορία;タグ"))
    doc.meta.addElement(Language(text="en"))
    doc.meta.addElement(UserDefined(name="opf.publisher", valuetype="string", text="Éditions Δ"))

    body_lines = tuple(lines or MULTISCRIPT_TEXT.splitlines())
    for line in body_lines:
        para = P()
        addTextToElement(para, line)
        doc.text.addElement(para)

    if include_image:
        para = P()
        addTextToElement(para, "Archive image holder 画像")
        doc.addPictureFromString(png_bytes(), "image/png")
        doc.text.addElement(para)

    doc.save(path)
    picture_members = tuple(name for name in zip_members(path) if name.startswith("Pictures/") and not name.endswith("/"))
    return ODTFixture(path=path, text_fragments=tuple(COMMON_TEXT_FRAGMENTS), picture_members=picture_members)


def zip_members(path: Path) -> tuple[str, ...]:
    with zipfile.ZipFile(path, "r") as zf:
        return tuple(info.filename for info in zf.infolist())


def rewrite_odt_zip(
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
