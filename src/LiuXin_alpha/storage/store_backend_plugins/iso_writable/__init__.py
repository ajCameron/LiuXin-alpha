"""
Expose the configured writable ISO Store.

Locations and observed file metadata use the shared storage API values.
The imported implementation owns backend construction and physical operations.
"""

from LiuXin_alpha.storage.store_backend_plugins.iso_writable.iso_writable_storage_backend import (
    IsoWritableStorageBackend,
)

__all__ = [
    "IsoWritableStorageBackend",
]
