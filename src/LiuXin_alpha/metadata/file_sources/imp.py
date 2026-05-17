"""
Read metadata from IMP files.
"""

from __future__ import annotations

import os
from typing import BinaryIO

from LiuXin_alpha.metadata.metadata import MetaData as Metadata
from LiuXin_alpha.metadata.utils import string_to_authors
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types

__license__ = "GPL v3"
__copyright__ = "2008, Ashish Kulkarni <kulkarni.ashish@gmail.com>"

MAGIC = (b"\x00\x01BOOKDOUG", b"\x00\x02BOOKDOUG")

VALID_FOR = ["IMP"]
PRIORITY_FOR = ["IMP"]
RUN_COST = ["LOW"]


class ImpFormatError(Exception):
    pass


def _default_metadata(_source_name: str = ""):
    return Metadata(_("Unknown"), [])


def _ensure_default_authors(mi) -> None:
    try:
        if hasattr(mi, "is_null") and mi.is_null("authors"):
            mi.authors = [_("Unknown")]
            return
    except Exception:
        pass

    raw = getattr(mi, "authors", None)
    if not raw:
        mi.authors = [_("Unknown")]


def _warn(msg: str) -> None:
    warn = getattr(default_log, "warning", None) or getattr(default_log, "warn", None)
    if warn is not None:
        warn(msg)


def _decode_bytes(raw: bytes) -> str:
    if not raw:
        return ""
    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("utf-8", "replace")


def _read_cstring(stream: BinaryIO, *, skip: int = 0, max_bytes: int = 128 * 1024) -> str:
    """
    Read a null-terminated string.

    `skip` keeps legacy semantics from calibre's IMP parser: skip the first
    `skip` terminated strings, then return the next one.
    """
    result = bytearray()
    consumed = 0
    while True:
        data = stream.read(1)
        if not data:
            # Gracefully stop on truncated files.
            return _decode_bytes(bytes(result))
        consumed += 1
        if consumed > max_bytes:
            raise ValueError("IMP metadata string exceeded safety limit.")
        if data == b"\x00":
            if not skip:
                return _decode_bytes(bytes(result))
            skip -= 1
            result.clear()
            continue
        result.extend(data)


def read_metadata_from_stream(stream: BinaryIO, source_name: str = "", *, fallback_on_parse_error: bool = False):
    mi = _default_metadata(source_name)
    stream.seek(0)
    try:
        if stream.read(10) not in MAGIC:
            _warn("Couldn't read IMP header from file")
            if not fallback_on_parse_error:
                raise ImpFormatError("IMP payload does not start with an IMP header.")
            _ensure_default_authors(mi)
            return mi

        # Skip uninteresting header bytes.
        stream.read(38)
        _read_cstring(stream)  # Legacy unused field.
        category = _read_cstring(stream).strip()
        title = _read_cstring(stream, skip=1).strip()
        author = _read_cstring(stream, skip=2).strip()

        if title:
            mi.title = title
        if author:
            mi.authors = string_to_authors(author)
        if category:
            # Keep legacy behaviour where IMP category is exposed.
            setattr(mi, "category", category)
    except Exception as err:
        msg = "Couldn't read metadata from imp: %s with error %s" % (getattr(mi, "title", "Unknown"), err)
        if hasattr(default_log, "log_exception"):
            default_log.log_exception(msg, err, "ERROR")
        else:
            _warn(msg)
        if not fallback_on_parse_error:
            if isinstance(err, ImpFormatError):
                raise
            raise ImpFormatError("Failed to read metadata from IMP file.") from err
    _ensure_default_authors(mi)
    return mi


def get_metadata(target_file, *, fallback_on_parse_error: bool = False):
    """
    Return metadata as a calibre-compatible metadata object.
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
    "MAGIC",
    "VALID_FOR",
    "PRIORITY_FOR",
    "RUN_COST",
    "ImpFormatError",
    "get_metadata",
    "read_metadata_from_stream",
]
