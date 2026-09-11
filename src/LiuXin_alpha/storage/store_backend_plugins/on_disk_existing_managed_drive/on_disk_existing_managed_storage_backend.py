"""
Re-export the managed-directory Store through its older module path.

The exported class is the same implementation object as the canonical backend
module and adds no constructor adaptation or alternate allocation policy.
"""

from __future__ import annotations

from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive.on_disk_existing_managed_drive_storage_backend import (
    OnDiskExistingManagedStorageBackend,
)

__all__ = ["OnDiskExistingManagedStorageBackend"]
