"""
Expose the configured read-only ISO Store.

Locations and observed file metadata use the shared storage API values.
The imported implementation owns backend construction and physical operations.
"""

from LiuXin_alpha.storage.store_backend_plugins.iso_readonly.iso_readonly_storage_backend import (
    IsoReadOnlyStorageBackend,
)

__all__ = [
    "IsoReadOnlyStorageBackend",
]
