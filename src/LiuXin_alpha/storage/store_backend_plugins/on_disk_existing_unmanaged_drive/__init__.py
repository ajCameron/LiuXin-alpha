"""On-disk unmanaged drive store plugin (read-only)."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "OnDiskUnmanagedStoreLocation",
    "OnDiskUnmanagedStorageBackend",
]


def __getattr__(name: str) -> Any:
    if name == "OnDiskUnmanagedStoreLocation":
        return import_module("LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_existing_unmanaged_drive_location").OnDiskUnmanagedStoreLocation
    if name == "OnDiskUnmanagedStorageBackend":
        return import_module("LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_existing_unmanaged_drive_storage_backend").OnDiskUnmanagedStorageBackend
    raise AttributeError(name)
