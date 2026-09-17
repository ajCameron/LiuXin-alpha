"""
Expose the read-only unmanaged-directory Store.

Locations and observed file metadata use the shared storage API values.
The imported implementation owns backend construction and physical operations.
"""

from __future__ import annotations

from .on_disk_existing_unmanaged_drive_storage_backend import (
    OnDiskUnmanagedStorageBackend,
)

__all__ = [
    "OnDiskUnmanagedStorageBackend",
]
