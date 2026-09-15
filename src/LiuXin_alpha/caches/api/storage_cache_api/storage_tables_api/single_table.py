"""
Preserve both public spellings of the canonical single-table cache API.

StorageStorageCacheSingleTableAPI is the same class object as
StorageCacheSingleTableAPI. This compatibility module does not implement
rows or introduce a second table type.
"""

from .single_table_api import (
    StorageCacheSingleTableAPI,
    StorageStorageCacheSingleTableAPI,
)

__all__ = [
    "StorageCacheSingleTableAPI",
    "StorageStorageCacheSingleTableAPI",
]
