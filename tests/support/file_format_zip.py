"""
Build deterministic ZIP fixtures and test doubles.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise file format zip through a consuming regression::

        python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py
"""
from __future__ import annotations

import os
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path


@dataclass(frozen=True)
class ZipMember:
    """
    Represent the ZipMember state used by deterministic test-support operations.

    Example:
        Exercise ZipMember through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py
    """
    name: str
    payload: bytes
    compression: int | None = None


def _coerce_payload(payload: bytes | bytearray) -> bytes:
    """
    Perform the coerce payload step with deterministic fixture inputs.

    Example:
        Exercise  coerce payload through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


    :param payload: Binary or structured payload encoded into the fixture.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    if isinstance(payload, bytearray):
        return bytes(payload)
    return payload


def _iter_zip_members(
    members: Mapping[str, bytes] | Sequence[ZipMember | tuple[str, bytes]],
    *,
    default_compression: int,
):
    """
    Iterate zip members under the fixture contract.

    Example:
        Exercise  iter zip members through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


    :param members: Archive or container members included in the fixture.
    :param default_compression: Value supplied for default compression under the
        deterministic fixture contract.
    :return: An iterator yielding the deterministic fixture values described above.
    """
    if isinstance(members, Mapping):
        for name, payload in members.items():
            yield ZipMember(str(name), _coerce_payload(payload), default_compression)
        return

    for member in members:
        if isinstance(member, ZipMember):
            compression = member.compression
            if compression is None:
                compression = default_compression
            yield ZipMember(member.name, _coerce_payload(member.payload), compression)
        else:
            name, payload = member
            yield ZipMember(str(name), _coerce_payload(payload), default_compression)


def _zip_info(name: str, compression: int) -> zipfile.ZipInfo:
    """
    Perform the zip info step with deterministic fixture inputs.

    Example:
        Exercise  zip info through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


    :param name: Stable fixture, profile, member or field name.
    :param compression: Value supplied for compression under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    info = zipfile.ZipInfo(name)
    info.compress_type = compression
    return info


def write_zip_archive(
    target: str | os.PathLike | BytesIO,
    members: Mapping[str, bytes] | Sequence[ZipMember | tuple[str, bytes]],
    *,
    default_compression: int = zipfile.ZIP_DEFLATED,
    comment: bytes = b"",
) -> None:
    """
    Write zip archive for deterministic fixture consumers.

    Example:
        Exercise write zip archive through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


    :param target: Value supplied for target under the deterministic fixture contract.
    :param members: Archive or container members included in the fixture.
    :param default_compression: Value supplied for default compression under the
        deterministic fixture contract.
    :param comment: Value supplied for comment under the deterministic fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    if isinstance(target, (str, os.PathLike)):
        Path(target).parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(target, "w") as zf:
        for member in _iter_zip_members(members, default_compression=default_compression):
            zf.writestr(_zip_info(member.name, member.compression), member.payload)
        if comment:
            zf.comment = comment


def zip_archive_bytes(
    members: Mapping[str, bytes] | Sequence[ZipMember | tuple[str, bytes]],
    *,
    default_compression: int = zipfile.ZIP_DEFLATED,
    comment: bytes = b"",
) -> bytes:
    """
    Perform the zip archive bytes step with deterministic fixture inputs.

    Example:
        Exercise zip archive bytes through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


    :param members: Archive or container members included in the fixture.
    :param default_compression: Value supplied for default compression under the
        deterministic fixture contract.
    :param comment: Value supplied for comment under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    stream = BytesIO()
    write_zip_archive(
        stream,
        members,
        default_compression=default_compression,
        comment=comment,
    )
    return stream.getvalue()


def zip_member_names(path: str | os.PathLike) -> tuple[str, ...]:
    """
    Perform the zip member names step with deterministic fixture inputs.

    Example:
        Exercise zip member names through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    with zipfile.ZipFile(path, "r") as zf:
        return tuple(info.filename for info in zf.infolist())


def read_zip_member(path: str | os.PathLike, member: str) -> bytes:
    """
    Read zip member under the fixture contract.

    Example:
        Exercise read zip member through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :param member: Archive or container member addressed by the operation.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    with zipfile.ZipFile(path, "r") as zf:
        return zf.read(member)


def rewrite_zip_archive(
    src: str | os.PathLike,
    dst: str | os.PathLike,
    *,
    remove: Sequence[str] = (),
    replace: Mapping[str, bytes] | None = None,
    add: Mapping[str, bytes] | None = None,
    add_compression: int = zipfile.ZIP_STORED,
) -> None:
    """
    Perform the rewrite zip archive step with deterministic fixture inputs.

    Example:
        Exercise rewrite zip archive through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


    :param src: Source path or value copied into the fixture.
    :param dst: Destination path or object receiving generated fixture data.
    :param remove: Value supplied for remove under the deterministic fixture contract.
    :param replace: Value supplied for replace under the deterministic fixture contract.
    :param add: Value supplied for add under the deterministic fixture contract.
    :param add_compression: Value supplied for add compression under the deterministic
        fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    replacements = dict(replace or {})
    additions = dict(add or {})
    removed = set(remove)

    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(src, "r") as zin, zipfile.ZipFile(dst, "w") as zout:
        for info in zin.infolist():
            if info.filename in removed:
                continue
            data = replacements.pop(info.filename, zin.read(info.filename))
            zout.writestr(info, data)
        for member in _iter_zip_members(
            {**replacements, **additions},
            default_compression=add_compression,
        ):
            zout.writestr(_zip_info(member.name, member.compression), member.payload)
