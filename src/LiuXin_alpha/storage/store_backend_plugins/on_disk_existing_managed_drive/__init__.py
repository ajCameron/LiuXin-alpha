"""
On-disk existing-managed store plugin.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "OnDiskExistingManagedSingleFile",
    "OnDiskExistingManagedStoreLocation",
    "OnDiskExistingManagedStorageBackend",
]


def __getattr__(name: str) -> Any:
    if name == "OnDiskExistingManagedSingleFile":
        return import_module(
            "LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed.on_disk_existing_managed_single_file"
        ).OnDiskExistingManagedSingleFile
    if name == "OnDiskExistingManagedStoreLocation":
        return import_module(
            "LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed.on_disk_existing_managed_location"
        ).OnDiskExistingManagedStoreLocation
    if name == "OnDiskExistingManagedStorageBackend":
        return import_module(
            "LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed.on_disk_existing_managed_storage_backend"
        ).OnDiskExistingManagedStorageBackend
    raise AttributeError(name)

