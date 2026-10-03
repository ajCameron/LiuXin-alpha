"""
Read SNB book XML, authors, tags and cover payloads from the container without retaining caller resources.

The module keeps malformed-input, optional dependency and resource ownership
behavior explicit for registry callers.

Example:
    Exercise snb with pytest::

        python -m pytest -q tests/metadata/file_sources/test_snb_metadata_source.py
"""

from __future__ import annotations

import io
import os
import re
from typing import Iterable

from LiuXin_alpha.file_formats.snb.snbfile import SNBFile
from LiuXin_alpha.metadata.utils import calibreMetaInformation, string_to_authors
from LiuXin_alpha.utils.libraries.liuxin_etree import etree
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2010, Li Fanxi <lifanxi@freemindworld.com>"

VALID_FOR = ["SNB"]
PRIORITY_FOR = ["SNB"]
RUN_COST = ["LOW"]


class SnbFormatError(Exception):
    """
    Signal malformed or unreadable SNB metadata input.

    Example:
        Exercise SnbFormatError with pytest::

            python -m pytest -q tests/metadata/file_sources/test_snb_metadata_source.py
    """
    pass


def _default_metadata():
    """
    Build minimally usable metadata for missing or explicitly tolerated malformed input.

    Example:
        Exercise  default metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_snb_metadata_source.py


    :return: Parsed, normalized or serialized value described above.
    """
    return calibreMetaInformation(_("Unknown"), [_("Unknown")])


def _source_name(target_file) -> str:
    """
    Return the best available source label for fallback titles and diagnostics.

    Example:
        Exercise  source name with pytest::

            python -m pytest -q tests/metadata/file_sources/test_snb_metadata_source.py


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

            python -m pytest -q tests/metadata/file_sources/test_snb_metadata_source.py


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


def _iter_local(root, name: str):
    """
    Return local in deterministic source or registry order.

    Example:
        Exercise  iter local with pytest::

            python -m pytest -q tests/metadata/file_sources/test_snb_metadata_source.py


    :param root: Parsed XML, PDF or metadata node used as the operation context.
    :param name: Name, type or encoding selector used for lookup or interpretation.
    :return: Parsed, normalized or serialized value described above.
    """
    expected = name.lower()
    for node in root.iter():
        tag = getattr(node, "tag", "")
        if not isinstance(tag, str):
            continue
        local = tag.rsplit("}", 1)[-1].lower()
        if local == expected:
            yield node


def _first_text(root, *names: str) -> str:
    """
    Return the first usable text under fallback policy.

    Example:
        Exercise  first text with pytest::

            python -m pytest -q tests/metadata/file_sources/test_snb_metadata_source.py


    :param root: Parsed XML, PDF or metadata node used as the operation context.
    :param names: Value supplied for names.
    :return: Parsed, normalized or serialized value described above.
    """
    for name in names:
        node = next(_iter_local(root, name), None)
        if node is None:
            continue
        text = (node.text or "").strip()
        if text:
            return text
    return ""


def _set_authors(mi, raw_author: str) -> None:
    """
    Set authors while preserving unrelated metadata state.

    Example:
        Exercise  set authors with pytest::

            python -m pytest -q tests/metadata/file_sources/test_snb_metadata_source.py


    :param mi: Metadata object supplying or receiving the supported fields.
    :param raw_author: Raw value or payload to normalize, parse or serialize.
    :return: None.
    """
    authors = [x.strip() for x in string_to_authors(raw_author) if x and x.strip()]
    if not authors and raw_author.strip():
        authors = [raw_author.strip()]
    if not authors:
        return
    try:
        raw_data = object.__getattribute__(mi, "_data")
        if isinstance(raw_data, dict) and isinstance(raw_data.get("authors"), dict):
            raw_data["authors"].clear()
    except Exception:
        pass
    mi.authors = authors


def _set_tags(mi, raw: str) -> None:
    """
    Set tags while preserving unrelated metadata state.

    Example:
        Exercise  set tags with pytest::

            python -m pytest -q tests/metadata/file_sources/test_snb_metadata_source.py


    :param mi: Metadata object supplying or receiving the supported fields.
    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: None.
    """
    if not raw:
        return
    tags = [x.strip() for x in re.split(r"[;,]", raw) if x and x.strip()]
    if tags:
        mi.tags = tags


def _cover_candidates(cover_value: str) -> Iterable[str]:
    """
    Yield normalized SNB cover paths in preferred container lookup order.

    Example:
        Exercise  cover candidates with pytest::

            python -m pytest -q tests/metadata/file_sources/test_snb_metadata_source.py


    :param cover_value: Source or member label used for lookup, fallback titles or
        diagnostics.
    :return: Parsed, normalized or serialized value described above.
    """
    raw = cover_value.strip().replace("\\", "/")
    if not raw:
        return ()

    out: list[str] = []
    out.append(raw)
    if not raw.startswith("snbc/"):
        out.append("snbc/" + raw.lstrip("/"))
    if "/" not in raw:
        out.append("snbc/images/" + raw)
    return out


def _read_cover_data(snb_file: SNBFile, cover_value: str) -> tuple[str, bytes] | None:
    """
    Return the first readable SNB cover candidate together with its normalized format.

    Example:
        Exercise  read cover data with pytest::

            python -m pytest -q tests/metadata/file_sources/test_snb_metadata_source.py


    :param snb_file: Open container used for member lookup and reads; ownership remains
        with the caller.
    :param cover_value: Source or member label used for lookup, fallback titles or
        diagnostics.
    :return: Parsed, normalized or serialized value described above.
    """
    for candidate in _cover_candidates(cover_value):
        payload = snb_file.GetFileStream(candidate)
        if not payload:
            continue
        ext = os.path.splitext(candidate)[1].lower().lstrip(".")
        if ext == "jpeg":
            ext = "jpg"
        if not ext:
            ext = "jpg"
        return ext, payload
    return None


def _parse_book_snbf(meta_blob: bytes, *, mi, snb_file: SNBFile, extract_cover: bool) -> None:
    """
    Apply SNB book XML values and optional cover bytes to the destination metadata object.

    Example:
        Exercise  parse book snbf with pytest::

            python -m pytest -q tests/metadata/file_sources/test_snb_metadata_source.py


    :param meta_blob: Raw value or payload to normalize, parse or serialize.
    :param mi: Metadata object supplying or receiving the supported fields.
    :param snb_file: Open container used for member lookup and reads; ownership remains
        with the caller.
    :param extract_cover: Request cover discovery or cover payload extraction when true.
    :return: Parsed, normalized or serialized value described above.
    """
    try:
        parser = etree.XMLParser(recover=True, no_network=True)
        root = etree.fromstring(meta_blob, parser=parser)
    except TypeError:
        root = etree.fromstring(meta_blob)
    except Exception:
        return

    title = _first_text(root, "name", "title")
    if title:
        mi.title = title

    author = _first_text(root, "author", "creator")
    if author:
        _set_authors(mi, author)

    language = _first_text(root, "language", "lang")
    if language:
        mi.language = language.lower().replace("_", "-")

    publisher = _first_text(root, "publisher")
    if publisher:
        mi.publisher = publisher

    _set_tags(mi, _first_text(root, "keywords", "tags", "category", "subject"))

    if extract_cover:
        cover = _first_text(root, "cover")
        if cover:
            cover_data = _read_cover_data(snb_file, cover)
            if cover_data is not None:
                mi.cover_data = cover_data


def get_metadata(target_file, extract_cover: bool = True, *, fallback_on_parse_error: bool = False):
    """
    Read metadata from the supported path, bytes or stream input while applying module ownership and fallback policy.

    Example:
        Exercise get metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_snb_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :param extract_cover: Request cover discovery or cover payload extraction when true.
    :param fallback_on_parse_error: Return safe default metadata after parse errors when
        true; otherwise raise the format error.
    :return: Parsed, normalized or serialized value described above.
    """
    mi = _default_metadata()
    source_name = _source_name(target_file)

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
            raise TypeError("SNB metadata reader expects a filesystem path or readable binary stream.")

        snb_file = SNBFile()
        snb_file.Parse(stream, metaOnly=False)
        if not snb_file.IsValid():
            if not fallback_on_parse_error:
                raise SnbFormatError("SNB payload is not a valid SNB archive.")
            return mi

        meta_blob = snb_file.GetFileStream("snbf/book.snbf")
        if meta_blob:
            _parse_book_snbf(meta_blob, mi=mi, snb_file=snb_file, extract_cover=extract_cover)
    except Exception as err:
        if isinstance(err, TypeError) and "SNB metadata reader expects" in str(err):
            raise
        default_log.log_exception(
            "Failed to read SNB metadata; using defaults.",
            err,
            "DEBUG",
            ("source", source_name or "<stream>"),
        )
        if not fallback_on_parse_error:
            if isinstance(err, SnbFormatError):
                raise
            raise SnbFormatError("Failed to read SNB metadata.") from err
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
    "SnbFormatError",
    "get_metadata",
]
