"""Storage-cache plugin registry and builtin plugin exports."""

from __future__ import annotations

from LiuXin_alpha.caches.cache_plugins.numpy_vectorized import (
    NumpyVectorizedStorageCache,
)
from LiuXin_alpha.caches.cache_plugins.registry import (
    CachePluginError,
    create_storage_cache,
    get_cache_plugin_location,
    get_registered_cache_plugin_names,
    load_cache_plugin,
    register_cache_plugin,
)
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
    "CachePluginError",
    "NumpyVectorizedStorageCache",
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
    "create_storage_cache",
    "get_cache_plugin_location",
    "get_registered_cache_plugin_names",
    "load_cache_plugin",
    "register_cache_plugin",
]
