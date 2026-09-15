"""
Expose the application cache, storage contracts and bundled backends.

Use create_cache(database) to construct and load the default schema-backed
facade, or Cache(database) to defer loading. Storage backends provide the
row/field machinery; Cache owns structured queries and Catalog-mediated writes.
The exported classes and plugin helpers retain their defining-module identities.
"""

from LiuXin_alpha.caches.api import (
    CacheAPI,
    CacheCapabilities,
    CacheClosedError,
    CacheConsistency,
    CacheDirtyError,
    CacheError,
    CacheFilterOperator,
    CacheLookup,
    CacheLookupStatus,
    CacheNotReadyError,
    CachePredicate,
    CacheQuery,
    CacheQueryResult,
    CacheRecord,
    CacheReconciliationError,
    CacheRelation,
    CacheSort,
    CacheState,
    FieldBasicInterfaceAPI,
    StorageCacheCapabilities,
    StorageCacheAPI,
    StorageCacheBaseTableAPI,
    StorageCacheSingleTableAPI,
    TableTypes,
    UnknownCacheFieldError,
    UnknownCacheTableError,
    UnsupportedCacheQueryError,
)
from LiuXin_alpha.caches.cache import Cache, create_cache
from LiuXin_alpha.caches.cache_plugins import (
    create_storage_cache,
    get_cache_plugin_capabilities,
    get_cache_plugin_location,
    get_registered_cache_plugin_names,
    load_cache_plugin,
    register_cache_plugin,
)
from LiuXin_alpha.caches.cache_plugins.database_backed import (
    DatabaseBackedStorageCache,
)
from LiuXin_alpha.caches.cache_plugins.numpy_vectorized import (
    NumpyVectorizedStorageCache,
)
from LiuXin_alpha.caches.schema_backed import (
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
    "Cache",
    "CacheAPI",
    "CacheCapabilities",
    "CacheClosedError",
    "CacheConsistency",
    "CacheDirtyError",
    "CacheError",
    "CacheFilterOperator",
    "CacheLookup",
    "CacheLookupStatus",
    "CacheNotReadyError",
    "CachePredicate",
    "CacheQuery",
    "CacheQueryResult",
    "CacheRecord",
    "CacheReconciliationError",
    "CacheRelation",
    "CacheSort",
    "CacheState",
    "DatabaseBackedStorageCache",
    "NumpyVectorizedStorageCache",
    "SchemaBackedLinkTable",
    "SchemaBackedMainTableCache",
    "SchemaBackedManyManyField",
    "SchemaBackedManyOneField",
    "SchemaBackedOneManyField",
    "SchemaBackedSameTableField",
    "SchemaBackedStorageCache",
    "SchemaBackedTwoTableOneOneField",
    "StorageCacheAPI",
    "StorageCacheBaseTableAPI",
    "StorageCacheCapabilities",
    "StorageCacheSingleTableAPI",
    "TableTypes",
    "UnknownCacheFieldError",
    "UnknownCacheTableError",
    "UnsupportedCacheQueryError",
    "FieldBasicInterfaceAPI",
    "create_storage_cache",
    "create_cache",
    "get_cache_plugin_capabilities",
    "get_cache_plugin_location",
    "get_registered_cache_plugin_names",
    "load_cache_plugin",
    "register_cache_plugin",
]
