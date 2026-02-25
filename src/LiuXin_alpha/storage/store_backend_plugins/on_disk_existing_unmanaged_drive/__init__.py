
"""
On-disk unmanaged drive store plugin (read-only).

This is for when you're indexing an existing drive which you don't want LiuXin changing.
"""

from __future__ import annotations

from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_unmanaged_location import OnDiskUnmanagedStoreLocation
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_unmanaged_single_file import OnDiskUnmanagedSingleFile
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_unmanaged_storage_backend import OnDiskUnmanagedStorageBackend

__all__ = [
    "OnDiskUnmanagedSingleFile",
    "OnDiskUnmanagedStoreLocation",
    "OnDiskUnmanagedStorageBackend",
]
