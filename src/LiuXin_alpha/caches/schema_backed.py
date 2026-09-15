"""
Expose canonical schema-backed storage types through their public module.

The storage cache, main/link tables and relation-field classes are aliases of
the implementation classes in cache_plugins.schema_backed. Importing this
module constructs no cache instance and loads no database snapshot.
"""

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
    "SchemaBackedLinkTable",
    "SchemaBackedMainTableCache",
    "SchemaBackedManyManyField",
    "SchemaBackedManyOneField",
    "SchemaBackedOneManyField",
    "SchemaBackedSameTableField",
    "SchemaBackedStorageCache",
    "SchemaBackedTwoTableOneOneField",
]
