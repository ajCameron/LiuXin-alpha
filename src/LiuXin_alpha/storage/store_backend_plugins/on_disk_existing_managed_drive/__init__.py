"""
Expose the writable managed-directory Store and common Location alias.

The separate legacy single-file wrapper is not exported here. Managed allocation
policy belongs to the backend rather than the Location value.
"""

from __future__ import annotations

from .on_disk_existing_managed_drive_location import (
    OnDiskExistingManagedStoreLocation,
)
from .on_disk_existing_managed_drive_storage_backend import (
    OnDiskExistingManagedStorageBackend,
)

__all__ = [
    "OnDiskExistingManagedStoreLocation",
    "OnDiskExistingManagedStorageBackend",
]
