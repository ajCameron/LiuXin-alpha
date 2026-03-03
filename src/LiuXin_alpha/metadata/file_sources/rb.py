"""
Read metadata information from RB files.
"""

from __future__ import annotations

import os
import struct

from LiuXin_alpha.metadata.utils import calibreMetaInformation, string_to_authors
from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types, six_unicode
from LiuXin_alpha.utils.logging import default_log

MAGIC = b"\xb0\x0c\xb0\x0c\x02\x00NUVO\x00\x00\x00\x00"

__license__ = "GPL v3"
__copyright__ = "2008, Ashish Kulkarni <kulkarni.ashish@gmail.com>"

VALID_FOR = ["RB"]
PRIORITY_FOR = ["RB"]
RUN_COST = ["LOW"]


def get_metadata(target_file):
    """
    Return metadata as a calibre-compatible metadata object.
    """
    stream_needs_close = False
    source_name = ""

    if isinstance(target_file, six_string_types):
        source_name = target_file
        stream_needs_close = True
        stream = open(target_file, "rb")
    elif hasattr(target_file, "read"):
        stream = target_file
        source_name = getattr(stream, "name", "") or ""
    else:
        raise TypeError("target_file must be a filesystem path or a binary stream")

    try:
        return read_metadata_from_stream(stream, source_name=source_name)
    finally:
        if stream_needs_close:
            stream.close()


def _log_warning(message: str) -> None:
    logger = getattr(default_log, "warning", None) or getattr(default_log, "warn", None)
    if logger is not None:
        logger(message)


def _default_title_from_source(source_name: str) -> str:
    if not source_name:
        return "Unknown"
    return os.path.splitext(os.path.basename(source_name))[0] or "Unknown"


def _decode_info_line(raw_line: bytes) -> str:
    try:
        return raw_line.decode("utf-8")
    except Exception:
        return raw_line.decode("cp1252", "replace")


def read_metadata_from_stream(stream, source_name: str = ""):
    mi = calibreMetaInformation(_default_title_from_source(source_name), [])
    stream.seek(0)
    try:
        if stream.read(14) != MAGIC:
            _log_warning("Couldn't read RB header from file")
            return mi
        stream.read(10)

        def read_i32():
            return struct.unpack("<I", stream.read(4))[0]

        stream.seek(read_i32())
        toc_count = read_i32()

        info_length = info_offset = None
        for _ in range(toc_count):
            stream.read(32)
            length, offset, flag = read_i32(), read_i32(), read_i32()
            if flag == 2:
                info_length, info_offset = length, offset
                break

        if info_length is None or info_offset is None:
            _log_warning("Couldn't find INFO from RB file")
            return mi

        stream.seek(info_offset)
        info = stream.read(info_length).splitlines()
        for raw_line in info:
            if b"=" not in raw_line:
                continue
            key, value = _decode_info_line(raw_line).split("=", 1)
            key = key.strip().upper()
            value = value.strip()
            if key == "TITLE" and value:
                mi.title = value
            elif key == "AUTHOR" and value:
                mi.authors = string_to_authors(value)
    except Exception as err:
        title = getattr(mi, "title", "Unknown")
        msg = six_unicode("Couldn't read metadata from rb: %s with error %s") % (title, six_unicode(err))
        if hasattr(default_log, "log_exception"):
            default_log.log_exception(msg, err, "ERROR")
        else:
            _log_warning(msg)
        raise
    return mi
