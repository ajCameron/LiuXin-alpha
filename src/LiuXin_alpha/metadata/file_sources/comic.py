"""
Read metadata from comic archive files.
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
    pass


def _source_name(target_file) -> str:
    if isinstance(target_file, os.PathLike):
        return os.fspath(target_file)
    if isinstance(target_file, str):
        return target_file
    return getattr(target_file, "name", "") or ""


def _source_title(target_file) -> str:
    source = _source_name(target_file)
    if source:
        stem = os.path.splitext(os.path.basename(source))[0].strip()
        if stem:
            return stem
    return _("Unknown")


def _default_metadata(target_file):
    mi = calibreMetaInformation(_source_title(target_file), [_("Unknown")])
    try:
        mi.finalize()
    except Exception:
        pass
    return mi


def _normalize_requested_type(ftype: str | None) -> str:
    normalized = (ftype or "").lower().lstrip(".")
    if normalized not in _COMIC_TYPES:
        raise ComicFormatError("Comic metadata reader expects CBR or CBZ input.")
    return normalized


def _detected_comic_type(stream, requested_type: str) -> str:
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
    Read metadata from a CBR/CBZ filesystem path or readable binary stream.
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
