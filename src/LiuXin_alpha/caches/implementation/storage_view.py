"""Compatibility wrapper for the canonical schema-backed storage view."""

from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_view import (
    SchemaBackedCacheView,
    SchemaBackedCacheViewRow,
)

__all__ = ["SchemaBackedCacheView", "SchemaBackedCacheViewRow"]
