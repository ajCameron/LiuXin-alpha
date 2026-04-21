"""Live database-backed storage-cache plugin."""

from LiuXin_alpha.caches.cache_plugins.database_backed.storage_cache import (
    DatabaseBackedStorageCache,
)

StorageCache = DatabaseBackedStorageCache

__all__ = [
    "DatabaseBackedStorageCache",
    "StorageCache",
]
