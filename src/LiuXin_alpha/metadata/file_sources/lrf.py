"""
Adapt the LRF metadata reader to paths, streams and pre-opened LRFMetaFile objects with explicit fallback policy.

The module keeps binary parsing, optional dependency and stream ownership policy
explicit for registry callers.

Example:
    Exercise lrf with pytest::

        python -m pytest -q tests/metadata/file_sources/test_lrf_metadata_source.py
"""

from __future__ import annotations

import os

from LiuXin_alpha.file_formats.lrf.meta import LRFMetaFile
from LiuXin_alpha.file_formats.lrf.meta import get_metadata as _lrf_get_metadata
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData as MetaData,
)
from LiuXin_alpha.metadata.utils import calibreMetaInformation
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

VALID_FOR = ["LRF"]
PRIORITY_FOR = ["LRF"]
RUN_COST = ["LOW"]


class LrfFormatError(Exception):
    """
    Signal an LRF metadata read failure when fallback is disabled.

    Example:
        Exercise LrfFormatError with pytest::

            python -m pytest -q tests/metadata/file_sources/test_lrf_metadata_source.py
    """
    pass


def _source_name(target_file) -> str:
    """
    Derive the name used for fallback metadata and diagnostics.

    Example:
        Exercise  source name with pytest::

            python -m pytest -q tests/metadata/file_sources/test_lrf_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :return: Parsed, normalized or updated value described above.
    """
    if isinstance(target_file, LRFMetaFile):
        return getattr(getattr(target_file, "_file", None), "name", "") or ""
    if isinstance(target_file, os.PathLike):
        return os.fspath(target_file)
    if isinstance(target_file, str):
        return target_file
    return getattr(target_file, "name", "") or ""


def _source_title(target_file) -> str:
    """
    Derive the title used for fallback metadata and diagnostics.

    Example:
        Exercise  source title with pytest::

            python -m pytest -q tests/metadata/file_sources/test_lrf_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :return: Parsed, normalized or updated value described above.
    """
    source = _source_name(target_file)
    if source:
        stem = os.path.splitext(os.path.basename(source))[0].strip()
        if stem:
            return stem
    return _("Unknown")


def _default_metadata(target_file, *, calibre_md: bool):
    """
    Build a minimally usable metadata object for missing or explicitly tolerated malformed input.

    Example:
        Exercise  default metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_lrf_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :param calibre_md: Value supplied for calibre md.
    :return: Parsed, normalized or updated value described above.
    """
    title = _source_title(target_file)
    authors = [_("Unknown")]
    if calibre_md:
        mi = calibreMetaInformation(title, authors)
    else:
        mi = MetaData(title, authors)
    try:
        mi.finalize()
    except Exception:
        pass
    return mi


def _log_exception(err: Exception, source_name: str) -> None:
    """
    Report a format-specific parsing failure through the project logger with source context.

    Example:
        Exercise  log exception with pytest::

            python -m pytest -q tests/metadata/file_sources/test_lrf_metadata_source.py


    :param err: Value supplied for err.
    :param source_name: Source label used for fallback titles and diagnostics.
    :return: None.
    """
    default_log.log_exception(
        "Failed to read metadata from LRF file.",
        err,
        "ERROR",
        ("source", source_name or "<stream>"),
    )


def get_metadata(target_file, calibre_md: bool = True, *, fallback_on_parse_error: bool = False):
    """
    Read metadata from the supported path or stream input while applying the module's ownership and fallback policy.

    Example:
        Exercise get metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_lrf_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :param calibre_md: Value supplied for calibre md.
    :param fallback_on_parse_error: Return default metadata after parse errors when
        true; otherwise raise the format error.
    :return: Parsed, normalized or updated value described above.
    """
    if isinstance(target_file, LRFMetaFile):
        try:
            return _lrf_get_metadata(target_file, calibre_md=calibre_md)
        except Exception as err:
            _log_exception(err, _source_name(target_file))
            if fallback_on_parse_error:
                return _default_metadata(target_file, calibre_md=calibre_md)
            raise LrfFormatError("Failed to read metadata from LRF file.") from err

    stream_needs_close = False
    if isinstance(target_file, os.PathLike):
        target_file = os.fspath(target_file)
    if isinstance(target_file, str):
        stream = open(target_file, "rb")
        stream_needs_close = True
    elif hasattr(target_file, "read"):
        stream = target_file
    else:
        raise TypeError("LRF metadata reader expects a filesystem path, LRFMetaFile or binary stream.")

    pos = None
    if hasattr(stream, "tell"):
        try:
            pos = stream.tell()
        except Exception:
            pos = None

    try:
        return _lrf_get_metadata(stream, calibre_md=calibre_md)
    except Exception as err:
        _log_exception(err, _source_name(stream))
        if fallback_on_parse_error:
            return _default_metadata(stream, calibre_md=calibre_md)
        raise LrfFormatError("Failed to read metadata from LRF file.") from err
    finally:
        if stream_needs_close:
            stream.close()
        elif pos is not None and hasattr(stream, "seek"):
            try:
                stream.seek(pos)
            except Exception:
                pass


def get_metadata_inplace(path, calibre_md: bool = True, *, fallback_on_parse_error: bool = False):
    """
    Read metadata through the path-oriented adapter used by registry plugins that support in-place access.

    Example:
        Exercise get metadata inplace with pytest::

            python -m pytest -q tests/metadata/file_sources/test_lrf_metadata_source.py


    :param path: Caller-supplied path, path-like object or stream described by this
        operation.
    :param calibre_md: Value supplied for calibre md.
    :param fallback_on_parse_error: Return default metadata after parse errors when
        true; otherwise raise the format error.
    :return: Parsed, normalized or updated value described above.
    """
    return get_metadata(path, calibre_md=calibre_md, fallback_on_parse_error=fallback_on_parse_error)


__all__ = [
    "VALID_FOR",
    "PRIORITY_FOR",
    "RUN_COST",
    "LrfFormatError",
    "get_metadata",
    "get_metadata_inplace",
]
