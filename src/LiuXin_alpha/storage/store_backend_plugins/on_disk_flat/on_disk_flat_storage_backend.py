"""Flat content-addressed local Store."""

from __future__ import annotations

import hashlib
import os

from pathlib import Path
from uuid import UUID

from LiuXin_alpha.storage.api import Digest, FileInfo, StoreUnsupportedOperation
from LiuXin_alpha.storage.stores import FilesystemStore


class OnDiskFlatStorageBackend(FilesystemStore):
    """Store each payload under its SHA-256 digest at the Store root."""

    store_kind = "on_disk_flat"

    def __init__(
        self,
        url: str | os.PathLike[str],
        name: str | None = None,
        uuid: str | UUID | None = None,
    ) -> None:
        super().__init__(
            url,
            name=name,
            uuid=uuid,
            allocation_prefix="objects",
        )

    def store_bytes(
        self,
        data: bytes,
        *,
        location=None,
        name: str | None = None,
        metadata=None,
        write_mode=None,
        expected_digest: Digest | None = None,
        mode=None,
    ) -> FileInfo:
        digest = expected_digest or Digest(
            "sha256",
            hashlib.sha256(data).hexdigest(),
        )
        destination = location or f"{digest.value}.file"
        return super().store_bytes(
            data,
            location=destination,
            name=name,
            metadata=metadata,
            write_mode=write_mode,
            expected_digest=digest,
            mode=mode,
        )

    def store_file(
        self,
        path: str | os.PathLike[str],
        *,
        location=None,
        name: str | None = None,
        metadata=None,
        write_mode=None,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        mode=None,
    ) -> FileInfo:
        source_path = Path(path)
        digest = expected_digest or _file_digest(source_path)
        destination = location or f"{digest.value}.file"
        return super().store_file(
            source_path,
            location=destination,
            name=name,
            metadata=metadata,
            write_mode=write_mode,
            expected_size=expected_size,
            expected_digest=digest,
            mode=mode,
        )

    def store_stream(self, source, *, location=None, expected_digest=None, **kwargs):
        if location is None and expected_digest is None:
            raise StoreUnsupportedOperation(
                "flat streaming writes require an expected digest or location."
            )
        destination = location
        if destination is None:
            assert expected_digest is not None
            destination = f"{expected_digest.value}.file"
        return super().store_stream(
            source,
            location=destination,
            expected_digest=expected_digest,
            **kwargs,
        )


def _file_digest(path: Path) -> Digest:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return Digest("sha256", digest.hexdigest())


__all__ = ["OnDiskFlatStorageBackend"]
