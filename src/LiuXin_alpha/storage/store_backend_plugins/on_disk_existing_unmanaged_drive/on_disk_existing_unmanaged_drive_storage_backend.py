"""
Expose an existing local directory through a read-only filesystem Store.

Construction disables root creation and mutations. Missing-root availability is
reported by startup/probe rather than rejected by this adapter's constructor.
"""

from __future__ import annotations

import os

from uuid import UUID

from LiuXin_alpha.storage.stores import FilesystemStore


class OnDiskUnmanagedStorageBackend(FilesystemStore):
    """
    Read and enumerate local files while disabling Store mutations and automatic root creation.

    Construction can retain a missing root; startup then reports it unavailable. Read-only policy
    applies to this Store and does not prevent external filesystem changes.

    Example:
        >>> store = OnDiskUnmanagedStorageBackend("existing-library")  # doctest: +SKIP
        >>> store.startup().writable  # doctest: +SKIP
        False
    """

    store_kind = "on_disk_existing_unmanaged"

    def __init__(
        self,
        url: str | os.PathLike[str],
        name: str | None = None,
        uuid: str | UUID | None = None,
    ) -> None:
        """
        Configure the filesystem adapter with read_only=True and create_root=False.

        Path/identity normalization is inherited; this constructor does not check root availability
        or enumerate its files.

        Example:
            >>> store = OnDiskUnmanagedStorageBackend("existing-library", name="Source")  # doctest: +SKIP


        :param url: Filesystem root path or supported local file URI, normalized by FilesystemStore.
        :param name: Optional display name; None or empty text uses the filesystem Store's root-name fallback.
        :param uuid: UUID or UUID string for Store identity; None generates a fresh UUID.
        :return: None after retaining the read-only filesystem configuration and driver.
        """
        super().__init__(
            url,
            name=name,
            uuid=uuid,
            read_only=True,
            create_root=False,
        )


__all__ = ["OnDiskUnmanagedStorageBackend"]
