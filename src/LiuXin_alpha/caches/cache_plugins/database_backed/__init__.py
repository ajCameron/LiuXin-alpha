"""
Export the live database-backed cache under concrete and plugin entry names.

StorageCache and DatabaseBackedStorageCache refer to the same class. The
registry uses the concrete name; the generic alias supports plugin-style
imports. Importing this package does not instantiate or initialize a cache.
"""

from LiuXin_alpha.caches.cache_plugins.database_backed.storage_cache import (
    DatabaseBackedStorageCache,
)

StorageCache = DatabaseBackedStorageCache

__all__ = [
    "DatabaseBackedStorageCache",
    "StorageCache",
]
