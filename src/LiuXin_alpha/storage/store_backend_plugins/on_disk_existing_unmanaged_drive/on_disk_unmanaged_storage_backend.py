"""
Re-export the unmanaged-directory Store through its shorter legacy module path.

The exported class preserves the canonical read-only, no-root-creation behavior
without wrapping construction or changing method dispatch.
"""

from __future__ import annotations

from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_existing_unmanaged_drive_storage_backend import (
    OnDiskUnmanagedStorageBackend,
)

__all__ = ["OnDiskUnmanagedStorageBackend"]
