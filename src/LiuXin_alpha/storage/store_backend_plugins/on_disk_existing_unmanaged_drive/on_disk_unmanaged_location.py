"""
Re-export OnDiskUnmanagedStoreLocation through the shorter legacy module path.

The value remains the common Location class; no separate path parser or
filesystem representation is introduced.
"""

from __future__ import annotations

from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_existing_unmanaged_drive_location import (
    OnDiskUnmanagedStoreLocation,
)

__all__ = ["OnDiskUnmanagedStoreLocation"]
