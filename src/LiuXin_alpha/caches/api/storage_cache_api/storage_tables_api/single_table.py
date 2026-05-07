"""Compatibility re-export for the storage single-table API contract."""

from .single_table_api import (
    StorageCacheSingleTableAPI,
    StorageStorageCacheSingleTableAPI,
)

__all__ = [
    "StorageCacheSingleTableAPI",
    "StorageStorageCacheSingleTableAPI",
]
