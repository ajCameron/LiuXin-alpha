
"""
On-disk unmanaged drive store plugin (read-only).

This is for when you're indexing an existing drive which you don't want LiuXin changing.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "OnDiskUnmanagedSingleFile",
    "OnDiskUnmanagedStoreLocation",
    "OnDiskUnmanagedStorageBackend",
]


def __getattr__(name: str) -> Any:
    if name == "OnDiskUnmanagedSingleFile":
        return import_module(
            "LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_existing_unmanaged_drive_single_file"
        ).OnDiskUnmanagedSingleFile
    if name == "OnDiskUnmanagedStoreLocation":
        return import_module(
            "LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_existing_unmanaged_drive_location"
        ).OnDiskUnmanagedStoreLocation
    if name == "OnDiskUnmanagedStorageBackend":
        return import_module(
            "LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_existing_unmanaged_drive_storage_backend"
        ).OnDiskUnmanagedStorageBackend
    raise AttributeError(name)
