"""
Re-export canonical table metadata, cardinalities and base contracts.

Keep the legacy integer cardinality constants, enum, null identity sentinel
and base table API available at this import path without creating wrappers.
"""

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
