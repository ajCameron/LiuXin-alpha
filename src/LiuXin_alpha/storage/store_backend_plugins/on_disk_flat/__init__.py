"""Flat on-disk hash-named store plugin."""

from __future__ import annotations

from .on_disk_flat_location import OnDiskFlatStoreLocation
from .on_disk_flat_storage_backend import OnDiskFlatStorageBackend

__all__ = [
    "OnDiskFlatStoreLocation",
    "OnDiskFlatStorageBackend",
]
