"""Compatibility facade for one file in an unmanaged filesystem Store."""

from __future__ import annotations

import pathlib

from typing import BinaryIO
from uuid import NAMESPACE_URL, uuid5

from LiuXin_alpha.storage.api import FileInfo

from .on_disk_existing_unmanaged_drive_storage_backend import (
    OnDiskUnmanagedStorageBackend,
)


class OnDiskUnmanagedSingleFile:
    """Bind one local file to a read-only Store and its owned ``Location``.

    New code should keep the Store and Location separately.  This small facade
    remains for older import and archive helpers that naturally start from one
    local filename, while exposing only the current read/stat vocabulary.
    """

    def __init__(self, file_url: str | pathlib.Path) -> None:
        path = pathlib.Path(file_url).expanduser().resolve(strict=False)
        self.path = path
        self.store = OnDiskUnmanagedStorageBackend(
            path.parent,
            name=f"unmanaged-file:{path.name}",
            uuid=uuid5(NAMESPACE_URL, path.as_uri()),
        )
        self.location = self.store.locate(path.name)

    @property
    def store_ref(self):
        """Return the stable UUID of the file's configured Store."""

        return self.store.store_ref

    def stat(self) -> FileInfo:
        """Return current authoritative metadata for the file."""

        return self.store.stat(self.location)

    def open_read(
        self,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """Open a binary, read-only stream for the file."""

        return self.store.open_read(
            self.location,
            offset=offset,
            length=length,
            if_version=if_version,
        )

    def read_bytes(
        self,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> bytes:
        """Read the file or a byte range fully into memory."""

        return self.store.read_bytes(
            self.location,
            offset=offset,
            length=length,
            if_version=if_version,
        )

    def read_text(self, *, encoding: str = "utf-8") -> str:
        """Decode the complete file with an explicit text encoding."""

        return self.read_bytes().decode(encoding)

    def close(self) -> None:
        """Close the underlying configured Store."""

        self.store.close()


__all__ = ["OnDiskUnmanagedSingleFile"]
