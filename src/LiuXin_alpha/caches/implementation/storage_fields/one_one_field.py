"""Compatibility wrapper for the canonical schema-backed one-one fields."""

from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.one_one_field import (
    SchemaBackedSameTableField,
    SchemaBackedTwoTableOneOneField,
)

__all__ = [
    "SchemaBackedSameTableField",
    "SchemaBackedTwoTableOneOneField",
]
