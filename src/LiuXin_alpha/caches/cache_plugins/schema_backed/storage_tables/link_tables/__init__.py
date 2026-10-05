"""
Export the canonical schema-backed directed link-table implementation.

The root cache uses this class for each registered source/destination
orientation. Importing it does not read schema or physical link rows.
"""

from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.link_tables.link_table import (
    SchemaBackedLinkTable,
)

__all__ = ["SchemaBackedLinkTable"]
