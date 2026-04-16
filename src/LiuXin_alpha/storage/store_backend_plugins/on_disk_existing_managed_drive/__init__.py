"""On-disk existing-managed store plugin."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "OnDiskExistingManagedStoreLocation",
    "OnDiskExistingManagedStorageBackend",
]


def __getattr__(name: str) -> Any:
    if name == "OnDiskExistingManagedStoreLocation":
        return import_module("LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive.on_disk_existing_managed_drive_location").OnDiskExistingManagedStoreLocation
    if name == "OnDiskExistingManagedStorageBackend":
        return import_module("LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive.on_disk_existing_managed_drive_storage_backend").OnDiskExistingManagedStorageBackend
    raise AttributeError(name)
