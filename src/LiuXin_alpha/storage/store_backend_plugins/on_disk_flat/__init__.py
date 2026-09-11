"""
Expose the flat local Store and its common Location compatibility alias.

Importing the package resolves those classes without constructing a Store or
creating a filesystem root.
"""

from __future__ import annotations

from .on_disk_flat_location import OnDiskFlatStoreLocation
from .on_disk_flat_storage_backend import OnDiskFlatStorageBackend

__all__ = [
    "OnDiskFlatStoreLocation",
    "OnDiskFlatStorageBackend",
]
