"""Managed local-directory Store using transactional filesystem writes."""

from __future__ import annotations

import os

from uuid import UUID

from LiuXin_alpha.storage.stores import FilesystemStore


class OnDiskExistingManagedStorageBackend(FilesystemStore):
    """Read existing files and publish managed objects below a reserved root."""

    store_kind = "on_disk_existing_managed"

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
            read_only=False,
            create_root=True,
            allocation_prefix=".liuxin-managed/objects",
        )

    @property
    def managed_area_root(self):
        """Return the private root used for automatically allocated objects."""

        return self.root_path / ".liuxin-managed"

    def is_reserved_managed_path(self, identifier) -> bool:
        """Return whether an identifier belongs to the managed allocation area."""

        return self.locate(identifier).key.startswith(".liuxin-managed/")


__all__ = ["OnDiskExistingManagedStorageBackend"]
