"""Flat on-disk hash-named store plugin."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "OnDiskFlatStoreLocation",
    "OnDiskFlatStorageBackend",
]


def __getattr__(name: str) -> Any:
    if name == "OnDiskFlatStoreLocation":
        return import_module(
            "LiuXin_alpha.storage.store_backend_plugins.on_disk_flat.on_disk_flat_location"
        ).OnDiskFlatStoreLocation
    if name == "OnDiskFlatStorageBackend":
        return import_module(
            "LiuXin_alpha.storage.store_backend_plugins.on_disk_flat.on_disk_flat_storage_backend"
        ).OnDiskFlatStorageBackend
    raise AttributeError(name)
