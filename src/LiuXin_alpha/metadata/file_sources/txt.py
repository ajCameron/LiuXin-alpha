"""
Infer title and authors from plain text using Gutenberg, legacy header and title/byline conventions.

The module keeps malformed-input, optional dependency and resource ownership
behavior explicit for registry callers.

Example:
    Exercise txt with pytest::

        python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py
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
_BINARY_SIGNATURES = (
    b"\x89PNG\r\n\x1a\n",
    b"\xff\xd8\xff",
    b"GIF87a",
    b"GIF89a",
    b"%PDF-",
    b"PK\x03\x04",
    b"PK\x05\x06",
    b"Rar!",
)
_LEGACY_BLOCK_RE = re.compile(
    r"(?u)^[ ]*(?P<title>.+?)[ ]*(\n{3}|(\r\n){3}|\r{3})[ ]*(?P<author>.+?)[ ]*(\n|\r\n|\r|$)"
)
_BYLINE_RE = re.compile(r"(?iu)^\s*by\s+(?P<author>.+?)\s*$")


def _source_name(target_file) -> str:
    """
    Return the best available source label for fallback titles and diagnostics.

    Example:
        Exercise  source name with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :return: Parsed, normalized or serialized value described above.
    """
    if isinstance(target_file, os.PathLike):
        return os.fspath(target_file)
    if isinstance(target_file, str):
        return target_file
    return getattr(target_file, "name", "") or ""


def _safe_seek(stream, pos: int | None) -> None:
    """
    Perform seek without propagating optional or recovery failures.

    Example:
        Exercise  safe seek with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :param pos: Offset, bound or scalar value used by the operation.
    :return: None.
    """
    if pos is None or not hasattr(stream, "seek"):
        return
    try:
        stream.seek(pos)
    except Exception:
        pass


def _default_metadata(source_name: str):
    """
    Build minimally usable metadata for missing or explicitly tolerated malformed input.

    Example:
        Exercise  default metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :param source_name: Source or member label used for lookup, fallback titles or
        diagnostics.
    :return: Parsed, normalized or serialized value described above.
    """
    title = "Unknown"
    if source_name:
        base = os.path.basename(source_name)
        stem, _ext = os.path.splitext(base)
        if stem:
            title = stem
    return calibreMetaInformation(title, [_("Unknown")])


def _decode_head(raw: bytes) -> str:
    """
    Decode head using the format's ordered fallback policy.

    Example:
        Exercise  decode head with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or serialized value described above.
    """
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


def _looks_binaryish(raw: bytes) -> bool:
    """
    Return whether the supplied state satisfies the looks binaryish condition.

    Example:
        Exercise  looks binaryish with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: True when the described condition is satisfied; otherwise False.
    """
    if not raw:
        return False
    if raw.startswith((b"\xff\xfe", b"\xfe\xff", b"\xef\xbb\xbf")):
        return False
    if raw.startswith(_BINARY_SIGNATURES):
        return True
    sample = raw[:1024]
    control_count = sum(1 for byte in sample if byte < 32 and byte not in (9, 10, 13))
    return bool(sample) and (control_count / len(sample)) > 0.20


def _sanitize_text(text: str) -> str:
    """
    Perform the format-specific sanitize text operation used by this metadata source.

    Example:
        Exercise  sanitize text with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :param text: Policy flag controlling the behavior described above.
    :return: Parsed, normalized or serialized value described above.
    """
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = _CONTROL_CHARS_RE.sub("", text)
    return text


def _clean_field(value: str) -> str:
    """
    Perform the format-specific clean field operation used by this metadata source.

    Example:
        Exercise  clean field with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :param value: Offset, bound or scalar value used by the operation.
    :return: Parsed, normalized or serialized value described above.
    """
    value = " ".join((value or "").strip().split())
    value = value.strip(" \t-_,.;:")
    return value


def _parse_gutenberg(lines: list[str]) -> tuple[str | None, str | None]:
    """
    Extract title and author fields from a Project Gutenberg-style header.

    Example:
        Exercise  parse gutenberg with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :param lines: Ordered input values processed by this operation.
    :return: Parsed, normalized or serialized value described above.
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
    """
    Extract title and author fields from a short legacy key/value header block.

    Example:
        Exercise  parse legacy block with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :param text: Policy flag controlling the behavior described above.
    :return: Parsed, normalized or serialized value described above.
    """
    mo = _LEGACY_BLOCK_RE.search(text[:2048])
    if mo is None:
        return (None, None)
    title = _clean_field(mo.group("title"))
    author = _clean_field(mo.group("author"))
    return (title or None, author or None)


def _parse_title_and_byline(lines: list[str]) -> tuple[str | None, str | None]:
    """
    Infer a title and author from the first useful plain-text lines and byline conventions.

    Example:
        Exercise  parse title and byline with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :param lines: Ordered input values processed by this operation.
    :return: Parsed, normalized or serialized value described above.
    """
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
    """
    Set authors while preserving unrelated metadata state.

    Example:
        Exercise  set authors with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :param mi: Metadata object supplying or receiving the supported fields.
    :param raw_author: Raw value or payload to normalize, parse or serialize.
    :return: None.
    """
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
    """
    Apply the supported plain-text heuristics in priority order.

    Example:
        Exercise  extract metadata from text with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :param text: Policy flag controlling the behavior described above.
    :return: Parsed, normalized or serialized value described above.
    """
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
    Read metadata from the supported path, bytes or stream input while applying module ownership and fallback policy.

    Example:
        Exercise get metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :return: Parsed, normalized or serialized value described above.
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
            raw = bytes(raw)
            if _looks_binaryish(raw):
                return mi
            text = _decode_head(raw)
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
