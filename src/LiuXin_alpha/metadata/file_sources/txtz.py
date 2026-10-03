"""
Read and update TXTZ metadata with embedded OPF, plain-text and cover fallbacks.

The module keeps malformed-input, optional dependency and resource ownership
behavior explicit for registry callers.

Example:
    Exercise txtz with pytest::

        python -m pytest -q tests/metadata/file_sources/test_txtz_metadata_source.py
"""

from __future__ import annotations

import io
import os
import posixpath
from collections.abc import Iterable

from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData as MetaInformation,
)
from LiuXin_alpha.metadata.file_sources.extz import ExtzFormatError
from LiuXin_alpha.metadata.file_sources.extz import get_metadata as extz_get_metadata
from LiuXin_alpha.metadata.file_sources.extz import set_metadata as extz_set_metadata
from LiuXin_alpha.metadata.file_sources.txt import get_metadata as txt_get_metadata
from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

VALID_FOR = ["TXTZ"]
PRIORITY_FOR = ["TXTZ"]
RUN_COST = ["LOW"]

_COVER_EXTENSIONS = {"jpg", "jpeg", "png", "webp", "gif", "bmp"}


def _values(raw):
    """
    Perform the format-specific values operation used by this metadata source.

    Example:
        Exercise  values with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txtz_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or serialized value described above.
    """
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
    """
    Return the first usable first under fallback policy.

    Example:
        Exercise  first with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txtz_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or serialized value described above.
    """
    vals = _values(raw)
    return vals[0] if vals else None


def _title_is_unknown(md) -> bool:
    """
    Perform the format-specific title is unknown operation used by this metadata source.

    Example:
        Exercise  title is unknown with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txtz_metadata_source.py


    :param md: Metadata object supplying or receiving the supported fields.
    :return: Parsed, normalized or serialized value described above.
    """
    title = str(_first(getattr(md, "title", None)) or "").strip()
    return title == "" or title.lower() == "unknown"


def _authors_are_unknown(md) -> bool:
    """
    Perform the format-specific authors are unknown operation used by this metadata source.

    Example:
        Exercise  authors are unknown with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txtz_metadata_source.py


    :param md: Metadata object supplying or receiving the supported fields.
    :return: Parsed, normalized or serialized value described above.
    """
    authors = [str(x).strip() for x in _values(getattr(md, "authors", None)) if str(x).strip()]
    if not authors:
        return True
    return len(authors) == 1 and authors[0].lower() == "unknown"


def _clear_default_authors(md) -> None:
    """
    Clear default authors while keeping shared state coherent.

    Example:
        Exercise  clear default authors with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txtz_metadata_source.py


    :param md: Metadata object supplying or receiving the supported fields.
    :return: None.
    """
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
    """
    Set authors while preserving unrelated metadata state.

    Example:
        Exercise  set authors with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txtz_metadata_source.py


    :param md: Metadata object supplying or receiving the supported fields.
    :param authors: Ordered input values processed by this operation.
    :return: None.
    """
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
    """
    Return the best available source label for fallback titles and diagnostics.

    Example:
        Exercise  source name with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txtz_metadata_source.py


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

            python -m pytest -q tests/metadata/file_sources/test_txtz_metadata_source.py


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


def _read_source_bytes(target_file) -> tuple[bytes, str]:
    """
    Read the complete source payload from bytes, a path or a stream and restore a caller-owned stream position when available.

    Example:
        Exercise  read source bytes with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txtz_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :return: Parsed, normalized or serialized value described above.
    """
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


def _fallback_metadata() -> MetaInformation:
    """
    Build minimally usable metadata for missing or explicitly tolerated malformed input.

    Example:
        Exercise  fallback metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txtz_metadata_source.py


    :return: Parsed, normalized or serialized value described above.
    """
    return MetaInformation(_("Unknown"), [_("Unknown")])


def _txt_member_key(name: str) -> tuple[int, int, str]:
    """
    Return the deterministic preference key used to select a TXT member from TXTZ.

    Example:
        Exercise  txt member key with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txtz_metadata_source.py


    :param name: Name, type or encoding selector used for lookup or interpretation.
    :return: Parsed, normalized or serialized value described above.
    """
    norm = name.replace("\\", "/").lstrip("./")
    base = posixpath.basename(norm).lower()
    pri = {"index.txt": 0, "book.txt": 1, "text.txt": 2}.get(base, 10)
    return (pri, norm.count("/"), norm.lower())


def _find_txt_member(zf: ZipFile) -> str | None:
    """
    Return the preferred plain-text member from an open TXTZ archive.

    Example:
        Exercise  find txt member with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txtz_metadata_source.py


    :param zf: Open container used for member lookup and reads; ownership remains with
        the caller.
    :return: Parsed, normalized or serialized value described above.
    """
    candidates = [name for name in zf.namelist() if str(name).lower().endswith(".txt")]
    if not candidates:
        return None
    return sorted(candidates, key=_txt_member_key)[0]


def _find_cover_member(zf: ZipFile) -> str | None:
    """
    Return the preferred conventional cover member from an open TXTZ archive.

    Example:
        Exercise  find cover member with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txtz_metadata_source.py


    :param zf: Open container used for member lookup and reads; ownership remains with
        the caller.
    :return: Parsed, normalized or serialized value described above.
    """
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


def _fallback_from_txt_member(target_file, md, *, extract_cover: bool) -> bool:
    """
    Fill missing metadata and optional cover data from TXTZ members.

    Example:
        Exercise  fallback from txt member with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txtz_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :param md: Metadata object supplying or receiving the supported fields.
    :param extract_cover: Request cover discovery or cover payload extraction when true.
    :return: Parsed, normalized or serialized value described above.
    """
    found_metadata_source = False
    try:
        raw, source_name = _read_source_bytes(target_file)
        with ZipFile(io.BytesIO(raw), "r") as zf:
            txt_member = _find_txt_member(zf)
            if txt_member:
                found_metadata_source = True
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
                    found_metadata_source = True
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
        return False
    return found_metadata_source


def get_metadata(target_file, extract_cover: bool = True, *, fallback_on_parse_error: bool = False):
    """
    Read metadata from the supported path, bytes or stream input while applying module ownership and fallback policy.

    Example:
        Exercise get metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txtz_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :param extract_cover: Request cover discovery or cover payload extraction when true.
    :param fallback_on_parse_error: Return safe default metadata after parse errors when
        true; otherwise raise the format error.
    :return: Parsed, normalized or serialized value described above.
    """
    try:
        md = extz_get_metadata(target_file, extract_cover=extract_cover)
    except ExtzFormatError as err:
        md = _fallback_metadata()
        if _fallback_from_txt_member(target_file, md, extract_cover=extract_cover):
            return md
        if fallback_on_parse_error:
            return md
        raise ExtzFormatError("TXTZ archive does not contain OPF metadata or an embedded text source.") from err

    if _title_is_unknown(md) or _authors_are_unknown(md):
        _fallback_from_txt_member(target_file, md, extract_cover=extract_cover)
    return md


def set_metadata(target_file, mi):
    """
    Rewrite supported metadata fields without taking ownership of a caller-supplied stream.

    Example:
        Exercise set metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_txtz_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :param mi: Metadata object supplying or receiving the supported fields.
    :return: None.
    """
    return extz_set_metadata(target_file, mi)


__all__ = [
    "VALID_FOR",
    "PRIORITY_FOR",
    "RUN_COST",
    "get_metadata",
    "set_metadata",
]
