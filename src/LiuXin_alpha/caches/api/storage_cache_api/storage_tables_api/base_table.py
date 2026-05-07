"""Compatibility re-export for the storage table base API contracts."""

from .base_table_api import (
    MANY_MANY,
    MANY_ONE,
    ONE_MANY,
    ONE_ONE,
    StorageCacheBaseTableAPI,
    TableMetadata,
    TableTypes,
    null,
)

__all__ = [
    "MANY_MANY",
    "MANY_ONE",
    "ONE_MANY",
    "ONE_ONE",
    "StorageCacheBaseTableAPI",
    "TableMetadata",
    "TableTypes",
    "null",
]
