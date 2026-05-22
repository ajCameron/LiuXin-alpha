from __future__ import annotations

import os
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path


@dataclass(frozen=True)
class ZipMember:
    name: str
    payload: bytes
    compression: int | None = None


def _coerce_payload(payload: bytes | bytearray) -> bytes:
    if isinstance(payload, bytearray):
        return bytes(payload)
    return payload


def _iter_zip_members(
    members: Mapping[str, bytes] | Sequence[ZipMember | tuple[str, bytes]],
    *,
    default_compression: int,
):
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
    stream = BytesIO()
    write_zip_archive(
        stream,
        members,
        default_compression=default_compression,
        comment=comment,
    )
    return stream.getvalue()


def zip_member_names(path: str | os.PathLike) -> tuple[str, ...]:
    with zipfile.ZipFile(path, "r") as zf:
        return tuple(info.filename for info in zf.infolist())


def read_zip_member(path: str | os.PathLike, member: str) -> bytes:
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
