"""On-disk unmanaged drive store plugin (read-only)."""

from __future__ import annotations

from .on_disk_existing_unmanaged_drive_location import (
    OnDiskUnmanagedStoreLocation,
)
from .on_disk_existing_unmanaged_drive_storage_backend import (
    OnDiskUnmanagedStorageBackend,
)

__all__ = [
    "OnDiskUnmanagedStoreLocation",
    "OnDiskUnmanagedStorageBackend",
]
