"""
Read/write metadata from TXTZ archives.
"""

from __future__ import annotations

import io
import os
import posixpath
from collections.abc import Iterable

from LiuXin_alpha.metadata.file_sources.extz import get_metadata as extz_get_metadata
from LiuXin_alpha.metadata.file_sources.extz import set_metadata as extz_set_metadata
from LiuXin_alpha.metadata.file_sources.txt import get_metadata as txt_get_metadata
from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile
from LiuXin_alpha.utils.logging import default_log

VALID_FOR = ["TXTZ"]
PRIORITY_FOR = ["TXTZ"]
RUN_COST = ["LOW"]

_COVER_EXTENSIONS = {"jpg", "jpeg", "png", "webp", "gif", "bmp"}


def _values(raw):
    if raw is None:
        return []
    if isinstance(raw, dict):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    try:
        return list(raw)
    except TypeError:
        return [raw]


def _first(raw):
    vals = _values(raw)
    return vals[0] if vals else None


def _title_is_unknown(md) -> bool:
    title = str(_first(getattr(md, "title", None)) or "").strip()
    return title == "" or title.lower() == "unknown"


def _authors_are_unknown(md) -> bool:
    authors = [str(x).strip() for x in _values(getattr(md, "authors", None)) if str(x).strip()]
    if not authors:
        return True
    return len(authors) == 1 and authors[0].lower() == "unknown"


def _clear_default_authors(md) -> None:
    try:
        raw_data = object.__getattribute__(md, "_data")
    except Exception:
        raw_data = None
    if isinstance(raw_data, dict) and isinstance(raw_data.get("authors"), dict):
        raw_data["authors"].clear()
        return
    try:
        md.authors = []
    except Exception:
        pass


def _set_authors(md, authors: Iterable[str]) -> None:
    vals = [str(x).strip() for x in authors if str(x).strip()]
    if not vals:
        return
    _clear_default_authors(md)
    try:
        md.authors = vals
    except Exception:
        for author in vals:
            try:
                md.authors = author
            except Exception:
                break


def _source_name(target_file) -> str:
    if isinstance(target_file, os.PathLike):
        return os.fspath(target_file)
    if isinstance(target_file, str):
        return target_file
    return getattr(target_file, "name", "") or ""


def _safe_seek(stream, pos: int | None) -> None:
    if pos is None or not hasattr(stream, "seek"):
        return
    try:
        stream.seek(pos)
    except Exception:
        pass


def _read_source_bytes(target_file) -> tuple[bytes, str]:
    source_name = _source_name(target_file)

    if isinstance(target_file, os.PathLike):
        target_file = os.fspath(target_file)

    if isinstance(target_file, str):
        with open(target_file, "rb") as stream:
            return stream.read(), source_name

    if isinstance(target_file, (bytes, bytearray, memoryview)):
        return bytes(target_file), source_name

    if hasattr(target_file, "read"):
        stream = target_file
        pos = None
        if hasattr(stream, "tell"):
            try:
                pos = stream.tell()
            except Exception:
                pos = None
        try:
            _safe_seek(stream, 0)
            raw = stream.read()
            if isinstance(raw, str):
                raw = raw.encode("utf-8", "replace")
            return bytes(raw), source_name
        finally:
            _safe_seek(stream, pos)

    raise TypeError("TXTZ metadata reader expects a filesystem path or readable binary stream.")


def _txt_member_key(name: str) -> tuple[int, int, str]:
    norm = name.replace("\\", "/").lstrip("./")
    base = posixpath.basename(norm).lower()
    pri = {"index.txt": 0, "book.txt": 1, "text.txt": 2}.get(base, 10)
    return (pri, norm.count("/"), norm.lower())


def _find_txt_member(zf: ZipFile) -> str | None:
    candidates = [name for name in zf.namelist() if str(name).lower().endswith(".txt")]
    if not candidates:
        return None
    return sorted(candidates, key=_txt_member_key)[0]


def _find_cover_member(zf: ZipFile) -> str | None:
    candidates = []
    for name in zf.namelist():
        norm = str(name).replace("\\", "/").lstrip("./")
        ext = posixpath.splitext(norm)[1].lower().lstrip(".")
        if ext not in _COVER_EXTENSIONS:
            continue
        base = posixpath.basename(norm).lower()
        pri = 10
        if base in {"cover.jpg", "cover.jpeg", "cover.png", "cover.webp"}:
            pri = 0
        elif base.startswith("cover."):
            pri = 1
        candidates.append((pri, norm.count("/"), norm.lower(), name))
    if not candidates:
        return None
    return sorted(candidates)[0][-1]


def _fallback_from_txt_member(target_file, md, *, extract_cover: bool):
    try:
        raw, source_name = _read_source_bytes(target_file)
        with ZipFile(io.BytesIO(raw), "r") as zf:
            txt_member = _find_txt_member(zf)
            if txt_member:
                payload = zf.read(txt_member)
                txt_stream = io.BytesIO(payload)
                txt_stream.name = txt_member
                txt_md = txt_get_metadata(txt_stream)

                if _title_is_unknown(md) and not _title_is_unknown(txt_md):
                    md.title = txt_md.title
                if _authors_are_unknown(md) and not _authors_are_unknown(txt_md):
                    _set_authors(md, _values(getattr(txt_md, "authors", None)))

            if extract_cover and not getattr(md, "cover_data", None):
                cover_member = _find_cover_member(zf)
                if cover_member:
                    ext = posixpath.splitext(cover_member)[1].lower().lstrip(".")
                    if ext == "jpeg":
                        ext = "jpg"
                    md.cover_data = (ext or "jpg", zf.read(cover_member))
    except Exception as err:
        default_log.log_exception(
            "TXTZ fallback metadata extraction failed.",
            err,
            "DEBUG",
            ("source", source_name if "source_name" in locals() else _source_name(target_file) or "<stream>"),
        )


def get_metadata(target_file, extract_cover: bool = True):
    """
    Read TXTZ metadata. Prefer OPF/EXTZ metadata; fall back to embedded .txt parsing.
    """
    md = extz_get_metadata(target_file, extract_cover=extract_cover)
    if _title_is_unknown(md) or _authors_are_unknown(md):
        _fallback_from_txt_member(target_file, md, extract_cover=extract_cover)
    return md


def set_metadata(target_file, mi):
    """
    Write TXTZ metadata via the EXTZ writer.
    """
    return extz_set_metadata(target_file, mi)


__all__ = [
    "VALID_FOR",
    "PRIORITY_FOR",
    "RUN_COST",
    "get_metadata",
    "set_metadata",
]
