"""
Expose the SquashFS staging Store for explicit archive validation and sealing.

Locations and observed file metadata use the shared storage API values.
The imported implementation owns backend construction and physical operations.
"""

from .squashfs_build_storage_backend import SquashfsBuildStorageBackend

__all__ = [
    "SquashfsBuildStorageBackend",
]
