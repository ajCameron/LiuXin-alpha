"""
Expose the writable managed-directory Store.

Locations and observed file metadata use the shared storage API values.
The imported implementation owns backend construction and physical operations.
"""

from __future__ import annotations

from .on_disk_existing_managed_drive_storage_backend import (
    OnDiskExistingManagedStorageBackend,
)

__all__ = [
    "OnDiskExistingManagedStorageBackend",
]
