"""Single-file wrapper for the calibre-like on-disk managed store backend."""

from __future__ import annotations

from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive.on_disk_existing_managed_drive_single_file import (
    OnDiskExistingManagedSingleFile,
)


class OnDiskCalibreLikeSingleFile(OnDiskExistingManagedSingleFile):
    """
    Single-file wrapper for the calibre-like on-disk store.
    """

