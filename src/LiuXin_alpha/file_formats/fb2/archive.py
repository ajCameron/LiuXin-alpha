"""
Read FB2 content from plain or compressed ebook sources.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise archive through a consuming regression::

        python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py
"""
from __future__ import annotations

from collections.abc import Collection, Iterable
from io import BytesIO
from typing import TypeAlias

from LiuXin_alpha.file_formats.archive_preflight import (
    DEFAULT_MAX_ARCHIVE_MEMBERS,
    DEFAULT_MAX_COMPRESSION_RATIO,
    DEFAULT_MAX_MEMBER_UNCOMPRESSED_SIZE,
    DEFAULT_MAX_TOTAL_UNCOMPRESSED_SIZE,
    DEFAULT_MIN_COMPRESSION_RATIO_CHECK_SIZE,
    ZipMemberInfo,
    normalized_zip_member_name,
    validate_zip_member_infos,
)
from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile


class FB2ZipError(ValueError):
    """
    Report a fb2ziperror encountered while processing an ebook format.

    Example:
        Exercise FB2ZipError through a consuming regression::

            python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py
    """
    pass


FB2Input: TypeAlias = str | bytes | bytearray | memoryview | Iterable[int]


def ensure_bytes(raw: FB2Input) -> bytes:
    """
    Perform the ensure bytes operation under explicit file-format and conversion rules.

    Example:
        Exercise ensure bytes through a consuming regression::

            python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


    :param raw: Value supplied for raw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(raw, bytes):
        return raw
    if isinstance(raw, bytearray):
        return bytes(raw)
    if isinstance(raw, str):
        return raw.encode("utf-8", "replace")
    return bytes(raw)


def normalized_archive_member_name(name: str, *, label: str = "FB2 archive") -> str:
    """
    Perform the normalized archive member name operation under explicit file-format and conversion rules.

    Example:
        Exercise normalized archive member name through a consuming regression::

            python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param label: Value supplied for label under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return normalized_zip_member_name(name, member_label=label, error_type=FB2ZipError)


def validate_archive_infos(
    infos: Collection[ZipMemberInfo],
    *,
    label: str = "FB2 archive",
    max_archive_members: int = DEFAULT_MAX_ARCHIVE_MEMBERS,
    max_member_uncompressed_size: int = DEFAULT_MAX_MEMBER_UNCOMPRESSED_SIZE,
    max_total_uncompressed_size: int = DEFAULT_MAX_TOTAL_UNCOMPRESSED_SIZE,
    max_compression_ratio: int = DEFAULT_MAX_COMPRESSION_RATIO,
    min_compression_ratio_check_size: int = DEFAULT_MIN_COMPRESSION_RATIO_CHECK_SIZE,
) -> dict[str, str]:
    """
    Validate archive infos under the format's safety and compatibility rules.

    Example:
        Exercise validate archive infos through a consuming regression::

            python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


    :param infos: Value supplied for infos under the utility contract.
    :param label: Value supplied for label under the utility contract.
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
    return validate_zip_member_infos(
        infos,
        container_label=label,
        member_label=label,
        error_type=FB2ZipError,
        max_archive_members=max_archive_members,
        max_member_uncompressed_size=max_member_uncompressed_size,
        max_total_uncompressed_size=max_total_uncompressed_size,
        max_compression_ratio=max_compression_ratio,
        min_compression_ratio_check_size=min_compression_ratio_check_size,
    )


def select_single_fb2_member(names: dict[str, str], *, label: str = "FB2 archive") -> str:
    """
    Perform the select single fb2 member operation under explicit file-format and conversion rules.

    Example:
        Exercise select single fb2 member through a consuming regression::

            python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


    :param names: Value supplied for names under the utility contract.
    :param label: Value supplied for label under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    raw_container: FB2Input,
    *,
    label: str = "FB2 archive",
    force_zip: bool = False,
    max_archive_members: int = DEFAULT_MAX_ARCHIVE_MEMBERS,
    max_member_uncompressed_size: int = DEFAULT_MAX_MEMBER_UNCOMPRESSED_SIZE,
    max_total_uncompressed_size: int = DEFAULT_MAX_TOTAL_UNCOMPRESSED_SIZE,
    max_compression_ratio: int = DEFAULT_MAX_COMPRESSION_RATIO,
    min_compression_ratio_check_size: int = DEFAULT_MIN_COMPRESSION_RATIO_CHECK_SIZE,
) -> tuple[bytes, str | None]:
    """
    Extract fb2 payload from bytes under the format's safety and compatibility rules.

    Example:
        Exercise extract fb2 payload from bytes through a consuming regression::

            python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


    :param raw_container: Value supplied for raw container under the utility contract.
    :param label: Value supplied for label under the utility contract.
    :param force_zip: Value supplied for force zip under the utility contract.
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
