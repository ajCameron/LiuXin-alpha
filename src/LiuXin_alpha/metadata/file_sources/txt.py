"""
Read metadata from TXT files.
"""

from __future__ import annotations

import io
import os
import re

from LiuXin_alpha.metadata.utils import calibreMetaInformation, string_to_authors
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"

VALID_FOR = ["TXT"]
PRIORITY_FOR = ["TXT"]
RUN_COST = ["LOW"]

_MAX_SCAN_BYTES = 16 * 1024
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
_LEGACY_BLOCK_RE = re.compile(
    r"(?u)^[ ]*(?P<title>.+?)[ ]*(\n{3}|(\r\n){3}|\r{3})[ ]*(?P<author>.+?)[ ]*(\n|\r\n|\r|$)"
)
_BYLINE_RE = re.compile(r"(?iu)^\s*by\s+(?P<author>.+?)\s*$")


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


def _default_metadata(source_name: str):
    title = "Unknown"
    if source_name:
        base = os.path.basename(source_name)
        stem, _ext = os.path.splitext(base)
        if stem:
            title = stem
    return calibreMetaInformation(title, [_("Unknown")])


def _decode_head(raw: bytes) -> str:
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8-sig", "replace")
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16", "replace")

    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("utf-8", "replace")


def _sanitize_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = _CONTROL_CHARS_RE.sub("", text)
    return text


def _clean_field(value: str) -> str:
    value = " ".join((value or "").strip().split())
    value = value.strip(" \t-_,.;:")
    return value


def _parse_gutenberg(lines: list[str]) -> tuple[str | None, str | None]:
    """
    Parse common Project Gutenberg headers.
    """
    for idx, raw in enumerate(lines[:30]):
        line = raw.strip("\ufeff ").strip()
        if not line:
            continue
        lower = line.lower()
        if "project gutenberg" not in lower:
            continue
        if " of " not in lower or " by " not in lower:
            continue

        # Typical form:
        # "The Project Gutenberg Etext of <title> by <author>"
        mo = re.search(r"(?iu)\bof\b\s+(?P<title>.+?)\s+\bby\b\s+(?P<author>.+)$", line)
        if mo is None:
            continue

        title = _clean_field(mo.group("title"))
        author = _clean_field(mo.group("author"))

        # Some files line-wrap surname onto next line.
        if idx + 1 < len(lines):
            nxt = _clean_field(lines[idx + 1])
            if nxt and "copyright" not in nxt.lower() and len(nxt.split()) <= 2 and len(author.split()) <= 2:
                if nxt.lower() not in {"by"} and not author.lower().endswith(nxt.lower()):
                    author = _clean_field(author + " " + nxt)

        if title or author:
            return (title or None, author or None)
    return (None, None)


def _parse_legacy_block(text: str) -> tuple[str | None, str | None]:
    mo = _LEGACY_BLOCK_RE.search(text[:2048])
    if mo is None:
        return (None, None)
    title = _clean_field(mo.group("title"))
    author = _clean_field(mo.group("author"))
    return (title or None, author or None)


def _parse_title_and_byline(lines: list[str]) -> tuple[str | None, str | None]:
    title = None
    author = None

    for idx, raw in enumerate(lines[:60]):
        line = _clean_field(raw)
        if not line:
            continue

        if title is None:
            # Avoid obvious non-title starts.
            lower = line.lower()
            if lower.startswith("chapter ") or lower.startswith("part "):
                continue
            if lower.startswith("copyright "):
                continue
            title = line
            # Optional same-line "by X" form.
            mo = re.search(r"(?iu)^(?P<title>.+?)\s+\bby\b\s+(?P<author>.+)$", line)
            if mo is not None:
                title = _clean_field(mo.group("title")) or title
                author = _clean_field(mo.group("author")) or author
            continue

        if author is None:
            mo = _BYLINE_RE.match(line)
            if mo is not None:
                author = _clean_field(mo.group("author"))
                break

    return (title, author)


def _set_authors(mi, raw_author: str) -> None:
    raw_author = _clean_field(raw_author)
    if not raw_author:
        return
    if raw_author.lower().startswith("by "):
        raw_author = _clean_field(raw_author[3:])
        if not raw_author:
            return

    parsed = [x.strip() for x in string_to_authors(raw_author) if x and x.strip()]
    if not parsed:
        parsed = [raw_author]

    try:
        raw_data = object.__getattribute__(mi, "_data")
        if isinstance(raw_data, dict) and isinstance(raw_data.get("authors"), dict):
            raw_data["authors"].clear()
    except Exception:
        pass
    mi.authors = parsed


def _extract_metadata_from_text(text: str) -> tuple[str | None, str | None]:
    lines = text.split("\n")

    title, author = _parse_gutenberg(lines)
    if title or author:
        return (title, author)

    title, author = _parse_legacy_block(text)
    if title or author:
        return (title, author)

    return _parse_title_and_byline(lines)


def get_metadata(target_file):
    """
    Return metadata from a TXT path/stream.
    """
    source_name = _source_name(target_file)
    mi = _default_metadata(source_name)

    stream_needs_close = False
    stream = None
    pos = None
    try:
        if isinstance(target_file, os.PathLike):
            target_file = os.fspath(target_file)

        if isinstance(target_file, str):
            stream = open(target_file, "rb")
            stream_needs_close = True
        elif isinstance(target_file, (bytes, bytearray, memoryview)):
            stream = io.BytesIO(bytes(target_file))
            stream_needs_close = True
        elif hasattr(target_file, "read"):
            stream = target_file
            if hasattr(stream, "tell"):
                try:
                    pos = stream.tell()
                except Exception:
                    pos = None
            _safe_seek(stream, 0)
        else:
            raise TypeError("TXT metadata reader expects a filesystem path or readable binary stream.")

        raw = stream.read(_MAX_SCAN_BYTES)
        if isinstance(raw, str):
            text = raw
        else:
            text = _decode_head(bytes(raw))
        text = _sanitize_text(text)

        title, author = _extract_metadata_from_text(text)
        if title:
            mi.title = title
        if author:
            _set_authors(mi, author)
    except Exception as err:
        default_log.log_exception(
            "Failed to read TXT metadata; using defaults.",
            err,
            "DEBUG",
            ("source", source_name or "<stream>"),
        )
    finally:
        if stream_needs_close and stream is not None:
            stream.close()
        elif stream is not None:
            _safe_seek(stream, pos)

    return mi


__all__ = [
    "VALID_FOR",
    "PRIORITY_FOR",
    "RUN_COST",
    "get_metadata",
]
