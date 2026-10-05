"""
Read cover and embedded metadata from CBR and CBZ archives while preserving caller-owned stream positions.

The module keeps binary parsing, optional dependency and stream ownership policy
explicit for registry callers.

Example:
    Exercise comic with pytest::

        python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py
"""

from __future__ import annotations

import os
import zipfile

from LiuXin_alpha.metadata.file_sources.archive import archive_type, get_comic_metadata
from LiuXin_alpha.metadata.utils import calibreMetaInformation
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

VALID_FOR = ["CBR", "CBZ"]
PRIORITY_FOR = ["CBR", "CBZ"]
RUN_COST = ["LOW"]

_COMIC_TYPES = {"cbr", "cbz"}


class ComicFormatError(Exception):
    """
    Signal malformed, unsupported or unreadable comic archive input.

    Example:
        Exercise ComicFormatError with pytest::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py
    """
    pass


def _source_name(target_file) -> str:
    """
    Derive the name used for fallback metadata and diagnostics.

    Example:
        Exercise  source name with pytest::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :return: Parsed, normalized or updated value described above.
    """
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

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


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


def _default_metadata(target_file):
    """
    Build a minimally usable metadata object for missing or explicitly tolerated malformed input.

    Example:
        Exercise  default metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :return: Parsed, normalized or updated value described above.
    """
    mi = calibreMetaInformation(_source_title(target_file), [_("Unknown")])
    try:
        mi.finalize()
    except Exception:
        pass
    return mi


def _normalize_requested_type(ftype: str | None) -> str:
    """
    Normalize requested type into the representation expected by later parsing or serialization steps.

    Example:
        Exercise  normalize requested type with pytest::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param ftype: Format or encoding hint used for interpretation.
    :return: Parsed, normalized or updated value described above.
    """
    normalized = (ftype or "").lower().lstrip(".")
    if normalized not in _COMIC_TYPES:
        raise ComicFormatError("Comic metadata reader expects CBR or CBZ input.")
    return normalized


def _detected_comic_type(stream, requested_type: str) -> str:
    """
    Perform the format-specific detected comic type operation used by the metadata reader or writer.

    Example:
        Exercise  detected comic type with pytest::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :param requested_type: Format or encoding hint used for interpretation.
    :return: Parsed, normalized or updated value described above.
    """
    pos = None
    if hasattr(stream, "tell"):
        try:
            pos = stream.tell()
        except Exception:
            pos = None
    if hasattr(stream, "seek"):
        try:
            stream.seek(0)
        except Exception:
            pass
    try:
        actual_archive_type = archive_type(stream)
        if actual_archive_type is None:
            if hasattr(stream, "seek"):
                try:
                    stream.seek(0)
                except Exception:
                    pass
            try:
                if zipfile.is_zipfile(stream):
                    actual_archive_type = "zip"
            except Exception:
                pass
    finally:
        if pos is not None and hasattr(stream, "seek"):
            try:
                stream.seek(pos)
            except Exception:
                pass
    if actual_archive_type == "zip":
        return "cbz"
    if actual_archive_type == "rar":
        return "cbr"
    raise ComicFormatError("Not a valid comic archive for %s input." % requested_type.upper())


def _extract_first_image(stream, stream_type: str) -> tuple[str, bytes]:
    """
    Extract first image using the format-specific ordering and validation rules.

    Example:
        Exercise  extract first image with pytest::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :param stream_type: Format or encoding hint used for interpretation.
    :return: Parsed, normalized or updated value described above.
    """
    try:
        if hasattr(stream, "seek"):
            stream.seek(0)
        if stream_type == "cbr":
            from LiuXin_alpha.utils.decompression.unrar import extract_first_alphabetically

            extracted = extract_first_alphabetically(stream)
        else:
            from LiuXin_alpha.utils.decompression.libunzip import extract_member

            extracted = extract_member(stream, sort_alphabetically=True)
    except ComicFormatError:
        raise
    except Exception as err:
        raise ComicFormatError("Failed to read comic archive image members.") from err

    if extracted is None:
        raise ComicFormatError("Comic archive does not contain any readable image members.")
    member_name, data = extracted
    if not data:
        raise ComicFormatError("Comic archive first image member is empty.")
    return str(member_name), bytes(data)


def _log_exception(err: Exception, source_name: str) -> None:
    """
    Report a format-specific parsing failure through the project logger with source context.

    Example:
        Exercise  log exception with pytest::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param err: Value supplied for err.
    :param source_name: Source label used for fallback titles and diagnostics.
    :return: None.
    """
    default_log.log_exception(
        "Failed to read metadata from comic archive.",
        err,
        "ERROR",
        ("source", source_name or "<stream>"),
    )


def read_metadata_from_stream(
    stream,
    ftype: str,
    *,
    series_index: str = "volume",
    fallback_on_parse_error: bool = False,
):
    """
    Parse metadata from a caller-owned binary stream and apply the requested malformed-input fallback policy.

    Example:
        Exercise read metadata from stream with pytest::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param stream: Caller-supplied path, path-like object or stream described by this
        operation.
    :param ftype: Format or encoding hint used for interpretation.
    :param series_index: Value supplied for series index.
    :param fallback_on_parse_error: Return default metadata after parse errors when
        true; otherwise raise the format error.
    :return: Parsed, normalized or updated value described above.
    """
    requested_type = _normalize_requested_type(ftype)
    try:
        stream_type = _detected_comic_type(stream, requested_type)
        member_name, data = _extract_first_image(stream, stream_type)

        if hasattr(stream, "seek"):
            stream.seek(0)
        mi = calibreMetaInformation(None, None)
        try:
            mi.smart_update(get_comic_metadata(stream, stream_type, series_index=series_index))
        except Exception as err:
            default_log.log_exception(
                "Failed to read optional comic archive comment metadata.",
                err,
                "DEBUG",
                ("stream_type", stream_type),
                ("stream_name", getattr(stream, "name", "<stream>")),
            )

        ext = os.path.splitext(member_name)[1][1:].lower()
        mi.cover_data = (ext, data)
        try:
            mi.finalize()
        except Exception:
            pass
        return mi
    except Exception as err:
        if fallback_on_parse_error:
            _log_exception(err, _source_name(stream))
            return _default_metadata(stream)
        if isinstance(err, ComicFormatError):
            raise
        raise ComicFormatError("Failed to read metadata from comic archive.") from err


def get_metadata(
    target_file,
    ftype: str | None = None,
    *,
    series_index: str = "volume",
    fallback_on_parse_error: bool = False,
):
    """
    Read metadata from the supported path or stream input while applying the module's ownership and fallback policy.

    Example:
        Exercise get metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param target_file: Caller-supplied path, path-like object or stream described by
        this operation.
    :param ftype: Format or encoding hint used for interpretation.
    :param series_index: Value supplied for series index.
    :param fallback_on_parse_error: Return default metadata after parse errors when
        true; otherwise raise the format error.
    :return: Parsed, normalized or updated value described above.
    """
    stream_needs_close = False
    if isinstance(target_file, os.PathLike):
        target_file = os.fspath(target_file)
    if isinstance(target_file, str):
        stream = open(target_file, "rb")
        stream_needs_close = True
        requested_type = ftype or os.path.splitext(target_file)[1][1:]
    elif hasattr(target_file, "read"):
        stream = target_file
        requested_type = ftype or os.path.splitext(_source_name(stream))[1][1:]
    else:
        raise TypeError("Comic metadata reader expects a filesystem path or binary stream.")

    pos = None
    if hasattr(stream, "tell"):
        try:
            pos = stream.tell()
        except Exception:
            pos = None

    try:
        return read_metadata_from_stream(
            stream,
            requested_type,
            series_index=series_index,
            fallback_on_parse_error=fallback_on_parse_error,
        )
    finally:
        if stream_needs_close:
            stream.close()
        elif pos is not None and hasattr(stream, "seek"):
            try:
                stream.seek(pos)
            except Exception:
                pass


def get_metadata_inplace(
    path,
    ftype: str | None = None,
    *,
    series_index: str = "volume",
    fallback_on_parse_error: bool = False,
):
    """
    Read metadata through the path-oriented adapter used by registry plugins that support in-place access.

    Example:
        Exercise get metadata inplace with pytest::

            python -m pytest -q tests/metadata/file_sources/test_comic_metadata_source.py


    :param path: Caller-supplied path, path-like object or stream described by this
        operation.
    :param ftype: Format or encoding hint used for interpretation.
    :param series_index: Value supplied for series index.
    :param fallback_on_parse_error: Return default metadata after parse errors when
        true; otherwise raise the format error.
    :return: Parsed, normalized or updated value described above.
    """
    return get_metadata(path, ftype=ftype, series_index=series_index, fallback_on_parse_error=fallback_on_parse_error)


__all__ = [
    "VALID_FOR",
    "PRIORITY_FOR",
    "RUN_COST",
    "ComicFormatError",
    "get_metadata",
    "get_metadata_inplace",
    "read_metadata_from_stream",
]
