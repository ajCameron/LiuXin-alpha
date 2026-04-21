"""Backwards-compatible compatibility surface for the schema-backed cache plugin."""

from LiuXin_alpha.caches.cache_plugins.schema_backed import (
    SchemaBackedCacheView,
    SchemaBackedCacheViewRow,
    SchemaBackedLinkTable,
    SchemaBackedMainTableCache,
    SchemaBackedManyManyField,
    SchemaBackedManyOneField,
    SchemaBackedOneManyField,
    SchemaBackedSameTableField,
    SchemaBackedStorageCache,
    SchemaBackedTwoTableOneOneField,
    StorageCache,
    StorageCacheField,
    StorageCacheLinkTable,
    StorageCacheMainTable,
    StorageCacheView,
)

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
