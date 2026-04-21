"""
Schema-backed implementations of the storage cache APIs.
"""

from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_cache import (
    SchemaBackedStorageCache,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.many_many_field import (
    SchemaBackedManyManyField,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.many_one_field import (
    SchemaBackedManyOneField,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.one_one_field import (
    SchemaBackedSameTableField,
    SchemaBackedTwoTableOneOneField,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.one_many_field import (
    SchemaBackedOneManyField,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.link_tables.link_table import (
    SchemaBackedLinkTable,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.single_table import (
    SchemaBackedMainTableCache,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_view import (
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
    "SchemaBackedManyManyField",
    "SchemaBackedManyOneField",
    "SchemaBackedOneManyField",
    "SchemaBackedSameTableField",
    "SchemaBackedStorageCache",
    "SchemaBackedTwoTableOneOneField",
    "StorageCache",
    "StorageCacheField",
    "StorageCacheLinkTable",
    "StorageCacheMainTable",
    "StorageCacheView",
]
