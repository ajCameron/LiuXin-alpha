from __future__ import annotations

import posixpath
from typing import TypeVar


DEFAULT_MAX_ARCHIVE_MEMBERS = 4096
DEFAULT_MAX_MEMBER_UNCOMPRESSED_SIZE = 256 * 1024 * 1024
DEFAULT_MAX_TOTAL_UNCOMPRESSED_SIZE = 512 * 1024 * 1024
DEFAULT_MAX_COMPRESSION_RATIO = 1000
DEFAULT_MIN_COMPRESSION_RATIO_CHECK_SIZE = 1024 * 1024


class ArchivePreflightError(ValueError):
    pass


_ErrorT = TypeVar("_ErrorT", bound=Exception)


def _raise(error_type: type[_ErrorT], message: str) -> None:
    raise error_type(message)


def normalized_zip_member_name(
    name: str,
    *,
    member_label: str = "archive",
    error_type: type[_ErrorT] = ArchivePreflightError,
) -> str:
    raw_name = str(name)
    normalized = raw_name.replace("\\", "/")
    parts = normalized.split("/")
    if (
        "\\" in raw_name
        or normalized.startswith("/")
        or (len(normalized) > 1 and normalized[1] == ":")
        or ".." in parts
    ):
        _raise(error_type, "%s member has unsafe path: %s" % (member_label, name))
    normalized = posixpath.normpath(normalized)
    if normalized in {"", ".", ".."} or normalized.startswith("../"):
        _raise(error_type, "%s member has unsafe path: %s" % (member_label, name))
    return normalized


def validate_zip_member_infos(
    infos,
    *,
    container_label: str = "archive",
    member_label: str | None = None,
    error_type: type[_ErrorT] = ArchivePreflightError,
    allow_unsafe_paths: bool = False,
    max_archive_members: int = DEFAULT_MAX_ARCHIVE_MEMBERS,
    max_member_uncompressed_size: int = DEFAULT_MAX_MEMBER_UNCOMPRESSED_SIZE,
    max_total_uncompressed_size: int = DEFAULT_MAX_TOTAL_UNCOMPRESSED_SIZE,
    max_compression_ratio: int = DEFAULT_MAX_COMPRESSION_RATIO,
    min_compression_ratio_check_size: int = DEFAULT_MIN_COMPRESSION_RATIO_CHECK_SIZE,
) -> dict[str, str]:
    member_label = member_label or container_label
    if len(infos) > max_archive_members:
        _raise(
            error_type,
            "%s has too many archive members: %d > %d"
            % (container_label, len(infos), max_archive_members),
        )

    names: dict[str, str] = {}
    total_uncompressed = 0
    for info in infos:
        filename = str(getattr(info, "filename", ""))
        try:
            normalized_name = normalized_zip_member_name(
                filename,
                member_label=member_label,
                error_type=error_type,
            )
        except error_type:
            if not allow_unsafe_paths:
                raise
        else:
            names[normalized_name] = filename

        if filename.endswith("/"):
            continue

        file_size = max(int(getattr(info, "file_size", 0) or 0), 0)
        compress_size = max(int(getattr(info, "compress_size", 0) or 0), 0)
        total_uncompressed += file_size
        if file_size > max_member_uncompressed_size:
            _raise(
                error_type,
                "%s member is too large: %s (%d bytes)"
                % (member_label, filename, file_size),
            )
        if total_uncompressed > max_total_uncompressed_size:
            _raise(
                error_type,
                "%s expands to too much data: %d > %d bytes"
                % (member_label, total_uncompressed, max_total_uncompressed_size),
            )
        if file_size > 0 and compress_size == 0:
            _raise(
                error_type,
                "%s member has invalid compressed size: %s" % (member_label, filename),
            )
        if file_size >= min_compression_ratio_check_size and compress_size > 0:
            ratio = file_size / float(compress_size)
            if ratio > max_compression_ratio:
                _raise(
                    error_type,
                    "%s member has suspicious compression ratio: %s (%.1f)"
                    % (member_label, filename, ratio),
                )
    return names
