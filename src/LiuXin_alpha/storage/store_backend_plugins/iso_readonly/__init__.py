"""
Read-only ISO image Store backend.
"""

from LiuXin_alpha.storage.store_backend_plugins.iso_readonly.iso_readonly_location import (
    IsoReadOnlyStoreLocation,
)
from LiuXin_alpha.storage.store_backend_plugins.iso_readonly.iso_readonly_storage_backend import (
    IsoReadOnlyStorageBackend,
)


__all__ = ["IsoReadOnlyStorageBackend", "IsoReadOnlyStoreLocation"]
