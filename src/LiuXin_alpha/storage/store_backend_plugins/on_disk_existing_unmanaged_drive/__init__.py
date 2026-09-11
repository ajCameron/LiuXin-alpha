"""
Expose the read-only unmanaged-directory Store and common Location alias.

Importing this package does not probe a directory or instantiate the separate
legacy single-file facade.
"""

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
