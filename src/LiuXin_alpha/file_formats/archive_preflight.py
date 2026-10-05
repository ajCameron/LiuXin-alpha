"""
Inspect archive members and reject unsafe or unsupported extraction plans.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise archive preflight through a consuming regression::

        python -m pytest -q tests/file_formats/test_archive_preflight.py
"""
from __future__ import annotations

import posixpath
from collections.abc import Collection
from typing import Protocol


DEFAULT_MAX_ARCHIVE_MEMBERS = 4096
DEFAULT_MAX_MEMBER_UNCOMPRESSED_SIZE = 256 * 1024 * 1024
DEFAULT_MAX_TOTAL_UNCOMPRESSED_SIZE = 512 * 1024 * 1024
DEFAULT_MAX_COMPRESSION_RATIO = 1000
DEFAULT_MIN_COMPRESSION_RATIO_CHECK_SIZE = 1024 * 1024


class ArchivePreflightError(ValueError):
    """
    Report a archivepreflighterror encountered while processing an ebook format.

    Example:
        Exercise ArchivePreflightError through a consuming regression::

            python -m pytest -q tests/file_formats/test_archive_preflight.py
    """
    pass


class ZipMemberInfo(Protocol):
    """
    Archive-member fields consumed by the shared preflight checks.

    Example:
        Exercise ZipMemberInfo through a consuming regression::

            python -m pytest -q tests/file_formats/test_archive_preflight.py
    """

    filename: str
    file_size: int
    compress_size: int


def _raise(error_type: type[Exception], message: str) -> None:
    """
    Perform the raise operation under explicit file-format and conversion rules.

    Example:
        Exercise  raise through a consuming regression::

            python -m pytest -q tests/file_formats/test_archive_preflight.py


    :param error_type: Value supplied for error type under the utility contract.
    :param message: Value supplied for message under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    raise error_type(message)


def normalized_zip_member_name(
    name: str,
    *,
    member_label: str = "archive",
    error_type: type[Exception] = ArchivePreflightError,
) -> str:
    """
    Perform the normalized zip member name operation under explicit file-format and conversion rules.

    Example:
        Exercise normalized zip member name through a consuming regression::

            python -m pytest -q tests/file_formats/test_archive_preflight.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param member_label: Value supplied for member label under the utility contract.
    :param error_type: Value supplied for error type under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    infos: Collection[ZipMemberInfo],
    *,
    container_label: str = "archive",
    member_label: str | None = None,
    error_type: type[Exception] = ArchivePreflightError,
    allow_unsafe_paths: bool = False,
    max_archive_members: int = DEFAULT_MAX_ARCHIVE_MEMBERS,
    max_member_uncompressed_size: int = DEFAULT_MAX_MEMBER_UNCOMPRESSED_SIZE,
    max_total_uncompressed_size: int = DEFAULT_MAX_TOTAL_UNCOMPRESSED_SIZE,
    max_compression_ratio: int = DEFAULT_MAX_COMPRESSION_RATIO,
    min_compression_ratio_check_size: int = DEFAULT_MIN_COMPRESSION_RATIO_CHECK_SIZE,
) -> dict[str, str]:
    """
    Validate zip member infos under the format's safety and compatibility rules.

    Example:
        Exercise validate zip member infos through a consuming regression::

            python -m pytest -q tests/file_formats/test_archive_preflight.py


    :param infos: Value supplied for infos under the utility contract.
    :param container_label: Value supplied for container label under the utility
        contract.
    :param member_label: Value supplied for member label under the utility contract.
    :param error_type: Value supplied for error type under the utility contract.
    :param allow_unsafe_paths: Value supplied for allow unsafe paths under the utility
        contract.
    :param max_archive_members: Value supplied for max archive members under the utility
        contract.
    :param max_member_uncompressed_size: Value supplied for max member uncompressed size
        under the utility contract.
    :param max_total_uncompressed_size: Value supplied for max total uncompressed size
        under the utility contract.
    :param max_compression_ratio: Value supplied for max compression ratio under the
        utility contract.
    :param min_compression_ratio_check_size: Value supplied for min compression ratio
        check size under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
