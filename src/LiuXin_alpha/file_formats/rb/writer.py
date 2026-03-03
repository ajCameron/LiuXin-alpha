# -*- coding: utf-8 -*-

from __future__ import annotations

import io
import struct
import zlib
from typing import Iterable

from LiuXin_alpha.constants import __appname__, __version__
from LiuXin_alpha.file_formats.rb import HEADER, unique_name
from LiuXin_alpha.file_formats.rb.rbml import RBMLizer
from LiuXin_alpha.metadata.utils import authors_to_string

try:
    from PIL import Image as _PILImage
except Exception:  # pragma: no cover - optional dependency
    _PILImage = None

__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


TEXT_RECORD_SIZE = 4096


class TocItem(object):
    def __init__(self, name: bytes, size: int, flags: int):
        self.name = name
        self.size = size
        self.flags = flags


class RBWriter(object):
    def __init__(self, opts, log):
        self.opts = opts
        self.log = log
        self.name_map = {}

    def write_content(self, oeb_book, out_stream, metadata=None):
        info_data = self._info_section(metadata)
        hidx_data = b" "
        images = self._images(oeb_book.manifest)
        text_size, chunks = self._text(oeb_book)
        chunk_sizes = [len(x) for x in chunks]

        sections = [
            ("info.info", info_data, "info"),
            ("index.html", chunks, "text"),
            ("index.hidx", hidx_data, "binary"),
        ]
        for image_name, image_data in images:
            sections.append((image_name, image_data, "binary"))

        toc_items = []
        page_count = 0
        text_blob_size = 8 + (len(chunk_sizes) * 4) + sum(chunk_sizes)
        for name, data, section_type in sections:
            page_count += 1
            if section_type == "text":
                flags = 8
                size = text_blob_size
            elif section_type == "info":
                flags = 2
                size = len(data)
            else:
                flags = 0
                size = len(data)
            toc_items.append(TocItem(self._toc_name(name), size, flags))

        self.log.debug("Writing file header...")
        out_stream.write(HEADER)
        out_stream.write(struct.pack("<I", 0))
        out_stream.write(struct.pack("<IH", 0, 0))
        out_stream.write(struct.pack("<I", 0x128))
        out_stream.write(struct.pack("<I", 0))

        for _ in range(0x20, 0x128, 4):
            out_stream.write(struct.pack("<I", 0))

        out_stream.write(struct.pack("<I", page_count))
        offset = out_stream.tell() + (len(toc_items) * 44)

        for item in toc_items:
            out_stream.write(item.name)
            out_stream.write(struct.pack("<I", item.size))
            out_stream.write(struct.pack("<I", offset))
            out_stream.write(struct.pack("<I", item.flags))
            offset += item.size

        out_stream.write(info_data)

        self.log.debug("Writing compressed RB HTML...")
        out_stream.write(struct.pack("<I", len(chunks)))
        out_stream.write(struct.pack("<I", text_size))
        for size in chunk_sizes:
            out_stream.write(struct.pack("<I", size))
        for chunk in chunks:
            out_stream.write(chunk)

        self.log.debug("Writing images...")
        out_stream.write(hidx_data)
        for _name, image_data in images:
            out_stream.write(image_data)

        total_size = out_stream.tell()
        out_stream.seek(0x1C)
        out_stream.write(struct.pack("<I", total_size))

    def _toc_name(self, name: str) -> bytes:
        return name.encode("utf-8", "replace")[:32].ljust(32, b"\x00")

    def _text(self, oeb_book):
        rbmlizer = RBMLizer(self.log, name_map=self.name_map)
        text = rbmlizer.extract_content(oeb_book, self.opts).encode("cp1252", "xmlcharrefreplace")
        size = len(text)

        pages = []
        page_count = (len(text) + TEXT_RECORD_SIZE - 1) // TEXT_RECORD_SIZE
        for i in range(0, page_count):
            zobj = zlib.compressobj(9, zlib.DEFLATED, 13, 8, 0)
            start = i * TEXT_RECORD_SIZE
            end = start + TEXT_RECORD_SIZE
            pages.append(zobj.compress(text[start:end]) + zobj.flush())

        return size, pages

    def _images(self, manifest: Iterable):
        from LiuXin_alpha.file_formats.oeb.base import OEB_RASTER_IMAGES

        if _PILImage is None:
            warn = getattr(self.log, "warning", None) or getattr(self.log, "warn", None)
            if warn is not None:
                warn("Pillow is not installed, RB output will skip embedded raster images.")
            return []

        images = []
        used_names = []

        for item in manifest:
            if item.media_type not in OEB_RASTER_IMAGES:
                continue
            try:
                payload = self._as_bytes(item.data)
                im = _PILImage.open(io.BytesIO(payload)).convert("L")
                out = io.BytesIO()
                im.save(out, "PNG")
                data = out.getvalue()

                name = unique_name("%s.png" % len(used_names), used_names)
                used_names.append(name)
                self.name_map[item.href] = name

                images.append((name, data))
            except Exception as err:
                self.log.error("Error: Could not include file %s because %s." % (item.href, err))

        return images

    def _as_bytes(self, payload) -> bytes:
        if isinstance(payload, bytes):
            return payload
        if isinstance(payload, bytearray):
            return bytes(payload)
        if isinstance(payload, str):
            return payload.encode("utf-8", "replace")
        if hasattr(payload, "read"):
            current_pos = None
            if hasattr(payload, "tell"):
                try:
                    current_pos = payload.tell()
                except Exception:
                    current_pos = None
            try:
                if hasattr(payload, "seek"):
                    payload.seek(0)
            except Exception:
                pass
            raw = payload.read()
            if current_pos is not None and hasattr(payload, "seek"):
                try:
                    payload.seek(current_pos)
                except Exception:
                    pass
            if isinstance(raw, bytes):
                return raw
            if isinstance(raw, str):
                return raw.encode("utf-8", "replace")
        return bytes(payload)

    def _info_section(self, metadata):
        lines = ["TYPE=2"]
        if metadata:
            title_items = getattr(metadata, "title", ())
            if len(title_items) >= 1:
                title_value = getattr(title_items[0], "value", title_items[0])
                lines.append("TITLE=%s" % title_value)
            creator_items = getattr(metadata, "creator", ())
            if len(creator_items) >= 1:
                authors = [getattr(item, "value", item) for item in creator_items]
                lines.append("AUTHOR=%s" % authors_to_string(authors))
        lines.append("GENERATOR=%s - %s" % (__appname__, __version__))
        lines.append("PARSE=1")
        lines.append("OUTPUT=1")
        lines.append("BODY=index.html")
        text = "\n".join(lines) + "\n"
        return text.encode("cp1252", "replace")
