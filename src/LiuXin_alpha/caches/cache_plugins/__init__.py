"""
Expose bundled storage backends and their shared registration helpers.

The registry resolves canonical names and aliases for schema_backed,
database_backed and numpy_vectorized. Public table/field exports are the
canonical schema-backed implementations. Constructing a backend and loading
its data remain separate operations.
"""

from __future__ import annotations

from LiuXin_alpha.caches.api.storage_cache_api.storage_cache_api import (
    StorageCacheCapabilities,
)
from LiuXin_alpha.caches.cache_plugins.database_backed import (
    DatabaseBackedStorageCache,
)
from LiuXin_alpha.caches.cache_plugins.numpy_vectorized import (
    NumpyVectorizedStorageCache,
)
from LiuXin_alpha.caches.cache_plugins.registry import (
    CachePluginError,
    create_storage_cache,
    get_cache_plugin_capabilities,
    get_cache_plugin_location,
    get_registered_cache_plugin_names,
    load_cache_plugin,
    register_cache_plugin,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed import (
    SchemaBackedLinkTable,
    SchemaBackedMainTableCache,
    SchemaBackedManyManyField,
    SchemaBackedManyOneField,
    SchemaBackedOneManyField,
    SchemaBackedSameTableField,
    SchemaBackedStorageCache,
    SchemaBackedTwoTableOneOneField,
)

__all__ = [
    "CachePluginError",
    "DatabaseBackedStorageCache",
    "NumpyVectorizedStorageCache",
    "StorageCacheCapabilities",
    "SchemaBackedLinkTable",
    "SchemaBackedMainTableCache",
    "SchemaBackedManyManyField",
    "SchemaBackedManyOneField",
    "SchemaBackedOneManyField",
    "SchemaBackedSameTableField",
    "SchemaBackedStorageCache",
    "SchemaBackedTwoTableOneOneField",
    "create_storage_cache",
    "get_cache_plugin_capabilities",
    "get_cache_plugin_location",
    "get_registered_cache_plugin_names",
    "load_cache_plugin",
    "register_cache_plugin",
]
