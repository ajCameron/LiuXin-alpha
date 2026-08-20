"""Read-only Store for an existing unmanaged local directory."""

from __future__ import annotations

import os

from uuid import UUID

from LiuXin_alpha.storage.stores import FilesystemStore


class OnDiskUnmanagedStorageBackend(FilesystemStore):
    """Expose existing files without permitting LiuXin to mutate them."""

    store_kind = "on_disk_existing_unmanaged"

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
            read_only=True,
            create_root=False,
        )


__all__ = ["OnDiskUnmanagedStorageBackend"]
