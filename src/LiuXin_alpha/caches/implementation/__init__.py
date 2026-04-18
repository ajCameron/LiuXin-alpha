"""
Schema-backed implementations of the storage cache APIs.
"""

from LiuXin_alpha.caches.implementation.storage_cache import (
    SchemaBackedStorageCache,
)
from LiuXin_alpha.caches.implementation.storage_fields.one_one_field import (
    SchemaBackedSameTableField,
)
from LiuXin_alpha.caches.implementation.storage_tables.link_tables.link_table import (
    SchemaBackedLinkTable,
)
from LiuXin_alpha.caches.implementation.storage_tables.single_table import (
    SchemaBackedMainTableCache,
)
from LiuXin_alpha.caches.implementation.storage_view import (
    SchemaBackedCacheView,
    SchemaBackedCacheViewRow,
)

StorageCache = SchemaBackedStorageCache
StorageCacheMainTable = SchemaBackedMainTableCache
StorageCacheField = SchemaBackedSameTableField
StorageCacheLinkTable = SchemaBackedLinkTable
StorageCacheView = SchemaBackedCacheView

__all__ = [
    "SchemaBackedCacheView",
    "SchemaBackedCacheViewRow",
    "SchemaBackedLinkTable",
    "SchemaBackedMainTableCache",
    "SchemaBackedSameTableField",
    "SchemaBackedStorageCache",
    "StorageCache",
    "StorageCacheField",
    "StorageCacheLinkTable",
    "StorageCacheMainTable",
    "StorageCacheView",
]
