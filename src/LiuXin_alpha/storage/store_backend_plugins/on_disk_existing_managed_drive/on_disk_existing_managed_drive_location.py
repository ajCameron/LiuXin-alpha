"""Location type for the managed on-disk store backend."""

from __future__ import annotations

from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_existing_unmanaged_drive_location import (
    OnDiskLocalStoreLocation,
)


class OnDiskExistingManagedStoreLocation(OnDiskLocalStoreLocation):
    """
    Existing-managed store location.

    Reuses the proven local filesystem location implementation.
    """
