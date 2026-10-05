"""
Export schema-backed main-table and directed link-table implementations.

These canonical classes are used by the root storage cache. Importing
them does not load table rows or construct database handles.
"""

from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.single_table import (
    SchemaBackedMainTableCache,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.link_tables.link_table import (
    SchemaBackedLinkTable,
)

__all__ = [
    "SchemaBackedLinkTable",
    "SchemaBackedMainTableCache",
]
