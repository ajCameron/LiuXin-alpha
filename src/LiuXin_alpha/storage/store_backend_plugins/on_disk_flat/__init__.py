"""
Expose the local Store that allocates flat digest-based paths.

Locations and observed file metadata use the shared storage API values.
The imported implementation owns backend construction and physical operations.
"""

from __future__ import annotations

from .on_disk_flat_storage_backend import OnDiskFlatStorageBackend

__all__ = [
    "OnDiskFlatStorageBackend",
]
