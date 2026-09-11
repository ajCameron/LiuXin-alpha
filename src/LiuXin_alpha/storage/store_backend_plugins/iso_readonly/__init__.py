"""
Export the configured read-only ISO Store and its legacy Location alias.

Importing the plugin exposes both public names without constructing a Store,
opening an image, or eagerly importing optional UDF parser support.
"""

from LiuXin_alpha.storage.store_backend_plugins.iso_readonly.iso_readonly_location import (
    IsoReadOnlyStoreLocation,
)
from LiuXin_alpha.storage.store_backend_plugins.iso_readonly.iso_readonly_storage_backend import (
    IsoReadOnlyStorageBackend,
)


__all__ = ["IsoReadOnlyStorageBackend", "IsoReadOnlyStoreLocation"]
