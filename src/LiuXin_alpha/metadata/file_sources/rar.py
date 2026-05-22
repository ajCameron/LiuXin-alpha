"""
Read metadata from RAR archives.
"""

from __future__ import annotations

import os
from io import BytesIO

from LiuXin_alpha.metadata.file_sources.archive import is_comic
from LiuXin_alpha.utils.decompression.unrar import extract_member, names
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal kovid@kovidgoyal.net"
__docformat__ = "restructuredtext en"

VALID_FOR = ["RAR"]
PRIORITY_FOR = ["RAR"]
RUN_COST = ["LOW"]

_SUPPORTED_MEMBER_EXTENSIONS = {
    "azw",
    "azw1",
    "azw3",
    "azw4",
    "epub",
    "fb2",
    "fbz",
    "imp",
    "lit",
    "lrf",
    "mobi",
    "opf",
    "pdb",
    "pdf",
    "pml",
    "pmlz",
    "prc",
    "rb",
    "rtf",
}


def _member_type(member_name: str) -> str:
    ext = os.path.splitext(member_name.replace("\\", "/"))[1].lower()
    return ext[1:] if ext.startswith(".") else ext


def _dispatch_metadata(target, *, force_type: str):
    """
    Dispatch to the metadata reader registry for the given force type.
    """
    from LiuXin_alpha.metadata.file_sources import get_metadata as dispatch_get_metadata

    return dispatch_get_metadata(target, force_type=force_type)


def _set_timestamp_none(mi) -> None:
    try:
        mi.timestamp = None
    except Exception:
        pass


def _find_first_supported_member(file_names: list[str]) -> tuple[str, str] | None:
    for file_name in file_names:
        stream_type = _member_type(file_name)
        if stream_type in _SUPPORTED_MEMBER_EXTENSIONS:
            return file_name, stream_type
    return None


def _source_label(stream) -> str:
    name = getattr(stream, "name", "") or ""
    if not name:
        return "<stream>"
    return os.path.basename(name)


def _read_metadata_from_rar_stream(stream):
    file_names = list(names(stream))
    if is_comic(file_names):
        mi = _dispatch_metadata(stream, force_type="cbr")
        _set_timestamp_none(mi)
        return mi

    chosen = _find_first_supported_member(file_names)
    if chosen is None:
        raise ValueError(f"No ebook found in RAR archive ({_source_label(stream)})")
    member_name, stream_type = chosen

    extracted = extract_member(stream, match=None, name=member_name)
    if extracted is None:
        raise ValueError(f"Unable to extract selected archive member: {member_name}")

    extracted_name, extracted_data = extracted
    payload_stream = BytesIO(extracted_data)
    payload_stream.name = os.path.basename(extracted_name)
    mi = _dispatch_metadata(payload_stream, force_type=stream_type)
    _set_timestamp_none(mi)
    return mi


def get_metadata(target_file):
    """
    Read metadata from a RAR stream or path.
    """
    stream_needs_close = False
    if isinstance(target_file, os.PathLike):
        target_file = os.fspath(target_file)
    if isinstance(target_file, str):
        stream = open(target_file, "rb")
        stream_needs_close = True
    elif hasattr(target_file, "read"):
        stream = target_file
    else:
        raise TypeError("RAR metadata reader expects a filesystem path or binary stream.")

    pos = None
    if hasattr(stream, "tell"):
        try:
            pos = stream.tell()
        except Exception:
            pos = None

    try:
        return _read_metadata_from_rar_stream(stream)
    except Exception as err:
        default_log.log_exception(
            "Failed reading metadata from RAR archive.",
            err,
            "ERROR",
            ("source", _source_label(stream)),
        )
        raise
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
    "get_metadata",
]
