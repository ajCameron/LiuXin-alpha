"""
Read metadata from LRX files.
"""

from __future__ import annotations

import os
import struct
from zlib import decompress

import LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.utils
from LiuXin_alpha.metadata.utils import calibreMetaInformation, string_to_authors
from LiuXin_alpha.utils.libraries.liuxin_etree import etree
from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"

VALID_FOR = ["LRX"]
PRIORITY_FOR = ["LRX"]
RUN_COST = ["LOW"]


class LrxFormatError(Exception):
    pass


def _default_metadata(source_name: str = ""):
    title = _("Unknown")
    if source_name:
        stem = os.path.splitext(os.path.basename(source_name))[0].strip()
        if stem:
            title = stem
    return calibreMetaInformation(title, [_("Unknown")])


def _warn(message: str) -> None:
    warn = getattr(default_log, "warning", None) or getattr(default_log, "warn", None)
    if warn is not None:
        warn(message)


def _log_exception(base: str, exc: Exception, source_name: str) -> None:
    if hasattr(default_log, "log_exception"):
        default_log.log_exception(base, exc, "ERROR", ("source", source_name or "<stream>"))
        return
    _warn("%s (source=%s): %s" % (base, source_name or "<stream>", exc))


def _read_at(stream, at: int, amount: int) -> bytes:
    stream.seek(at)
    data = stream.read(amount)
    if not isinstance(data, (bytes, bytearray)):
        raise ValueError("LRX metadata parser expected a binary stream")
    if len(data) != amount:
        raise ValueError("LRX file is truncated while reading at offset %d" % at)
    return bytes(data)


def _word_be(buf: bytes) -> int:
    return struct.unpack(">L", buf)[0]


def _word_le(buf: bytes) -> int:
    return struct.unpack("<L", buf)[0]


def _short_le(buf: bytes) -> int:
    return struct.unpack("<H", buf)[0]


def _clean_text(raw) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def _parse_lrx_xml(payload: bytes, mi) -> None:
    root = etree.fromstring(payload)

    book_info = root.find("BookInfo")
    if book_info is None:
        return

    title_node = book_info.find("Title")
    if title_node is not None:
        title = _clean_text(getattr(title_node, "text", None))
        if title:
            mi.title = title
        title_sort = _clean_text(title_node.get("reading", None))
        if title_sort:
            LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.utils.title_sort = title_sort

    author_node = book_info.find("Author")
    if author_node is not None:
        author = _clean_text(getattr(author_node, "text", None))
        if author:
            mi.authors = string_to_authors(author)
        author_sort = _clean_text(author_node.get("reading", None))
        if author_sort:
            mi.author_sort = author_sort

    publisher_node = book_info.find("Publisher")
    publisher = _clean_text(getattr(publisher_node, "text", None))
    if publisher:
        mi.publisher = publisher

    tags = []
    for cat in book_info.findall("Category"):
        tag = _clean_text(getattr(cat, "text", None))
        if tag:
            tags.append(tag)
    if tags:
        mi.tags = tags

    doc_info = root.find("DocInfo")
    if doc_info is not None:
        language_node = doc_info.find("Language")
        language = _clean_text(getattr(language_node, "text", None))
        if language:
            mi.language = language

    if not getattr(mi, "authors", None):
        mi.authors = [_("Unknown")]


def read_metadata_from_stream(stream, source_name: str = "", *, fallback_on_parse_error: bool = False):
    mi = _default_metadata(source_name)
    stream.seek(0)
    header = stream.read(12)
    if len(header) < 12:
        _warn("LRX metadata read failed: file header is too short")
        if not fallback_on_parse_error:
            raise LrxFormatError("LRX file header is too short.")
        return mi

    try:
        if header[4:] == b"ftypLRX2":
            offset = 0
            while True:
                offset += _word_be(header[:4])
                header = _read_at(stream, offset, 8)
                if header[4:] == b"bbeb":
                    break

            offset += 8
            chunk = _read_at(stream, offset, 16)
            if chunk[:8].decode("utf-16-le", "strict") != "LRF\x00":
                raise ValueError("Not a valid LRX file")

            lrf_version = _word_le(chunk[8:12])
            offset += 0x4C
            compressed_size = _short_le(_read_at(stream, offset, 2))
            offset += 2
            if lrf_version >= 800:
                offset += 6

            compressed_size -= 4
            if compressed_size < 0:
                raise ValueError("LRX metadata block has invalid compressed size")

            uncompressed_size = _word_le(_read_at(stream, offset, 4))
            compressed_payload = _read_at(stream, offset + 4, compressed_size)
            info = decompress(compressed_payload)
            if len(info) != uncompressed_size:
                raise ValueError("LRX file has malformed metadata section")

            _parse_lrx_xml(info, mi)
            return mi

        if header[4:7] == b"LRX":
            _warn("Librie LRX format metadata parsing is not supported")
            return mi

        _warn("Not a valid LRX file")
        if not fallback_on_parse_error:
            raise LrxFormatError("Payload is not a valid LRX file.")
        return mi
    except Exception as err:
        _log_exception("Failed to read metadata from LRX file.", err, source_name)
        if not fallback_on_parse_error:
            if isinstance(err, LrxFormatError):
                raise
            raise LrxFormatError("Failed to read metadata from LRX file.") from err
        return mi


def get_metadata(target_file, *, fallback_on_parse_error: bool = False):
    """
    Read metadata from a LRX filesystem path or readable binary stream.
    """
    stream_needs_close = False
    source_name = ""

    if isinstance(target_file, six_string_types):
        source_name = target_file
        stream_needs_close = True
        stream = open(target_file, "rb")
    elif isinstance(target_file, os.PathLike):
        source_name = os.fspath(target_file)
        stream_needs_close = True
        stream = open(source_name, "rb")
    elif hasattr(target_file, "read"):
        stream = target_file
        source_name = getattr(stream, "name", "") or ""
    else:
        raise TypeError("target_file must be a filesystem path or a binary stream")

    pos = None
    if hasattr(stream, "tell"):
        try:
            pos = stream.tell()
        except Exception:
            pos = None

    try:
        return read_metadata_from_stream(stream, source_name=source_name, fallback_on_parse_error=fallback_on_parse_error)
    finally:
        if stream_needs_close:
            stream.close()
        elif pos is not None and hasattr(stream, "seek"):
            try:
                stream.seek(pos)
            except Exception:
                pass


__all__ = [
    "VALID_FOR",
    "PRIORITY_FOR",
    "RUN_COST",
    "LrxFormatError",
    "get_metadata",
    "read_metadata_from_stream",
]
