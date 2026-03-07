"""
Read metadata from ZIP archives.
"""

from __future__ import annotations

import os
from io import BytesIO
from pathlib import PurePosixPath

from LiuXin_alpha.metadata.file_sources.archive import is_comic
from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"

VALID_FOR = ["ZIP", "OEBZIP"]
PRIORITY_FOR = ["ZIP", "OEBZIP"]
RUN_COST = ["LOW"]

_SUPPORTED_MEMBER_EXTENSIONS = {
    "azw",
    "azw1",
    "azw3",
    "azw4",
    "epub",
    "fb2",
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


def _normalize_member_name(name: str) -> str:
    return str(name).replace("\\", "/").lstrip("./")


def _member_type(member_name: str) -> str:
    ext = os.path.splitext(_normalize_member_name(member_name))[1].lower()
    return ext[1:] if ext.startswith(".") else ext


def _dispatch_metadata(target, *, force_type: str):
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


def zip_opf_metadata(opf_member_name: str, zf: ZipFile):
    """
    Parse OPF metadata from a zip member and attempt to resolve cover bytes.
    """
    from LiuXin_alpha.file_formats.opf.opf2 import OPF

    opf_name = _normalize_member_name(opf_member_name)
    opf_bytes = zf.read(opf_member_name)
    opf_stream = BytesIO(opf_bytes)
    opf_stream.name = opf_name

    opf_dir = PurePosixPath(opf_name).parent
    opf_dir_str = "" if str(opf_dir) == "." else str(opf_dir)
    opf_obj = OPF(opf_stream, opf_dir_str)
    mi = opf_obj.to_book_metadata()

    cover = getattr(mi, "cover", None)
    if not cover:
        return mi

    lookup: dict[str, str] = {}
    for name in zf.namelist():
        if str(name).endswith("/"):
            continue
        lookup.setdefault(_normalize_member_name(name).lower(), str(name))

    cover_name = _normalize_member_name(str(cover))
    candidates = []
    if opf_dir_str:
        candidates.append(_normalize_member_name(f"{opf_dir_str}/{cover_name}"))
    candidates.append(cover_name)
    candidates.append(os.path.basename(cover_name))

    seen: set[str] = set()
    for candidate in candidates:
        key = candidate.lower()
        if key in seen:
            continue
        seen.add(key)
        member = lookup.get(key)
        if not member:
            continue
        data = zf.read(member)
        fmt = os.path.splitext(member)[1].lower().lstrip(".") or "jpg"
        mi.cover = None
        mi.cover_data = (fmt, data)
        break

    return mi


def _read_metadata_from_zip_stream(stream):
    with ZipFile(stream, "r") as zf:
        file_names = [_normalize_member_name(name) for name in zf.namelist()]
        if is_comic(file_names):
            mi = _dispatch_metadata(stream, force_type="cbz")
            _set_timestamp_none(mi)
            return mi

        chosen = _find_first_supported_member(file_names)
        if chosen is None:
            raise ValueError(f"No ebook found in ZIP archive ({_source_label(stream)})")
        member_name, stream_type = chosen

        payload = zf.read(member_name)
        payload_stream = BytesIO(payload)
        payload_stream.name = os.path.basename(member_name)
        mi = _dispatch_metadata(payload_stream, force_type=stream_type)

        if stream_type == "opf" and getattr(mi, "application_id", None) is None:
            try:
                mi = zip_opf_metadata(member_name, zf)
            except Exception as err:
                default_log.log_exception(
                    "Unable to resolve ZIP OPF metadata with cover fallback.",
                    err,
                    "DEBUG",
                    ("member", member_name),
                )

        _set_timestamp_none(mi)
        return mi


def get_metadata(target_file):
    """
    Read metadata from a ZIP stream or path.
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
        raise TypeError("ZIP metadata reader expects a filesystem path or binary stream.")

    pos = None
    if hasattr(stream, "tell"):
        try:
            pos = stream.tell()
        except Exception:
            pos = None

    try:
        if hasattr(stream, "seek"):
            try:
                stream.seek(0)
            except Exception:
                pass
        return _read_metadata_from_zip_stream(stream)
    except Exception as err:
        default_log.log_exception(
            "Failed reading metadata from ZIP archive.",
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


def get_metadata_inplace(target_file):
    return get_metadata(target_file)


__all__ = [
    "VALID_FOR",
    "PRIORITY_FOR",
    "RUN_COST",
    "get_metadata",
    "get_metadata_inplace",
    "zip_opf_metadata",
]
