"""Single-file SQLite blob store backend."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "SingleFileSqliteStoreLocation",
    "SingleFileSqliteStorageBackend",
]


def __getattr__(name: str) -> Any:
    if name == "SingleFileSqliteStoreLocation":
        return import_module("LiuXin_alpha.storage.store_backend_plugins.single_file_sqlite.single_file_sqlite_location").SingleFileSqliteStoreLocation
    if name == "SingleFileSqliteStorageBackend":
        return import_module("LiuXin_alpha.storage.store_backend_plugins.single_file_sqlite.single_file_sqlite_storage_backend").SingleFileSqliteStorageBackend
    raise AttributeError(name)
