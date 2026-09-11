"""
Export the SQLite Store compatibility class and its Location alias.

The backend class inherits the configured SQLite Store implementation, while
SingleFileSqliteStoreLocation is the shared Store Location value itself. Importing
this package creates no database or Store instance.
"""

from __future__ import annotations

from .single_file_sqlite_location import SingleFileSqliteStoreLocation
from .single_file_sqlite_storage_backend import SingleFileSqliteStorageBackend

__all__ = [
    "SingleFileSqliteStoreLocation",
    "SingleFileSqliteStorageBackend",
]
