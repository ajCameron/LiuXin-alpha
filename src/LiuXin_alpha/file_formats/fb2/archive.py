from __future__ import annotations

import posixpath
from io import BytesIO
from typing import Any

from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile


class FB2ZipError(ValueError):
    pass


DEFAULT_MAX_ARCHIVE_MEMBERS = 4096
DEFAULT_MAX_MEMBER_UNCOMPRESSED_SIZE = 256 * 1024 * 1024
DEFAULT_MAX_TOTAL_UNCOMPRESSED_SIZE = 512 * 1024 * 1024
DEFAULT_MAX_COMPRESSION_RATIO = 1000
DEFAULT_MIN_COMPRESSION_RATIO_CHECK_SIZE = 1024 * 1024


def ensure_bytes(raw: Any) -> bytes:
    if isinstance(raw, bytes):
        return raw
    if isinstance(raw, bytearray):
        return bytes(raw)
    if isinstance(raw, str):
        return raw.encode("utf-8", "replace")
    return bytes(raw)


def normalized_archive_member_name(name: str, *, label: str = "FB2 archive") -> str:
    normalized = str(name).replace("\\", "/")
    parts = normalized.split("/")
    if (
        "\\" in str(name)
        or normalized.startswith("/")
        or (len(normalized) > 1 and normalized[1] == ":")
        or ".." in parts
    ):
        raise FB2ZipError("%s member has unsafe path: %s" % (label, name))
    normalized = posixpath.normpath(normalized)
    if normalized in {"", ".", ".."} or normalized.startswith("../"):
        raise FB2ZipError("%s member has unsafe path: %s" % (label, name))
    return normalized


def validate_archive_infos(
    infos,
    *,
    label: str = "FB2 archive",
    max_archive_members: int = DEFAULT_MAX_ARCHIVE_MEMBERS,
    max_member_uncompressed_size: int = DEFAULT_MAX_MEMBER_UNCOMPRESSED_SIZE,
    max_total_uncompressed_size: int = DEFAULT_MAX_TOTAL_UNCOMPRESSED_SIZE,
    max_compression_ratio: int = DEFAULT_MAX_COMPRESSION_RATIO,
    min_compression_ratio_check_size: int = DEFAULT_MIN_COMPRESSION_RATIO_CHECK_SIZE,
) -> dict[str, str]:
    if len(infos) > max_archive_members:
        raise FB2ZipError(
            "%s has too many archive members: %d > %d"
            % (label, len(infos), max_archive_members)
        )

    names: dict[str, str] = {}
    total_uncompressed = 0
    for info in infos:
        normalized_name = normalized_archive_member_name(info.filename, label=label)
        names[normalized_name] = info.filename

        if str(info.filename).endswith("/"):
            continue

        file_size = max(int(getattr(info, "file_size", 0) or 0), 0)
        compress_size = max(int(getattr(info, "compress_size", 0) or 0), 0)
        total_uncompressed += file_size
        if file_size > max_member_uncompressed_size:
            raise FB2ZipError(
                "%s member is too large: %s (%d bytes)"
                % (label, info.filename, file_size)
            )
        if total_uncompressed > max_total_uncompressed_size:
            raise FB2ZipError(
                "%s expands to too much data: %d > %d bytes"
                % (label, total_uncompressed, max_total_uncompressed_size)
            )
        if file_size > 0 and compress_size == 0:
            raise FB2ZipError("%s member has invalid compressed size: %s" % (label, info.filename))
        if file_size >= min_compression_ratio_check_size and compress_size > 0:
            ratio = file_size / float(compress_size)
            if ratio > max_compression_ratio:
                raise FB2ZipError(
                    "%s member has suspicious compression ratio: %s (%.1f)"
                    % (label, info.filename, ratio)
                )
    return names


def select_single_fb2_member(names: dict[str, str], *, label: str = "FB2 archive") -> str:
    fb2_members = [
        original
        for normalized, original in names.items()
        if not normalized.endswith("/") and normalized.lower().endswith(".fb2")
    ]
    if not fb2_members:
        raise FB2ZipError("%s has no FB2 member" % label)
    if len(fb2_members) > 1:
        raise FB2ZipError(
            "%s has multiple FB2 members: %s"
            % (label, ", ".join(sorted(fb2_members)))
        )
    return fb2_members[0]


def extract_fb2_payload_from_bytes(
    raw_container: Any,
    *,
    label: str = "FB2 archive",
    force_zip: bool = False,
    max_archive_members: int = DEFAULT_MAX_ARCHIVE_MEMBERS,
    max_member_uncompressed_size: int = DEFAULT_MAX_MEMBER_UNCOMPRESSED_SIZE,
    max_total_uncompressed_size: int = DEFAULT_MAX_TOTAL_UNCOMPRESSED_SIZE,
    max_compression_ratio: int = DEFAULT_MAX_COMPRESSION_RATIO,
    min_compression_ratio_check_size: int = DEFAULT_MIN_COMPRESSION_RATIO_CHECK_SIZE,
) -> tuple[bytes, str | None]:
    raw_bytes = ensure_bytes(raw_container)
    if not raw_bytes:
        return b"", None

    if not raw_bytes.startswith(b"PK"):
        if force_zip:
            raise FB2ZipError("%s appears to be invalid ZIP file" % label)
        return raw_bytes, None

    try:
        zf = ZipFile(BytesIO(raw_bytes), "r")
    except Exception as err:
        raise FB2ZipError("%s appears to be invalid ZIP file" % label) from err

    try:
        names = validate_archive_infos(
            zf.infolist(),
            label=label,
            max_archive_members=max_archive_members,
            max_member_uncompressed_size=max_member_uncompressed_size,
            max_total_uncompressed_size=max_total_uncompressed_size,
            max_compression_ratio=max_compression_ratio,
            min_compression_ratio_check_size=min_compression_ratio_check_size,
        )
        member = select_single_fb2_member(names, label=label)
        try:
            payload = zf.read(member)
        except Exception as err:
            raise FB2ZipError("%s could not read FB2 member: %s" % (label, member)) from err
    finally:
        zf.close()

    return ensure_bytes(payload), member
