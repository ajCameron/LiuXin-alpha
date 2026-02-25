from __future__ import annotations

from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_unmanaged_location import (
    OnDiskUnmanagedStoreLocation,
)


class OnDiskExistingManagedStoreLocation(OnDiskUnmanagedStoreLocation):
    """
    Existing-managed store location.

    Reuses the proven local filesystem location implementation.
    """

