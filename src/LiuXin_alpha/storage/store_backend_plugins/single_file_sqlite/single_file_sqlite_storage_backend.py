"""
Expose the legacy single-file SQLite backend name through the configured Store API.

The compatibility subclass changes the configured kind to single_file_sqlite.
All lifecycle, object routing, BLOB operations, and driver ownership are inherited
from SQLiteStore, whose constructor starts the database driver.
"""

from LiuXin_alpha.storage.stores import SQLiteStore


class SingleFileSqliteStorageBackend(SQLiteStore):
    """
    Keep the historical class name and Store kind while inheriting SQLiteStore behavior.

    Construction can create/open the SQLite container through inherited startup. Locations and
    FileInfo results use the shared Store API values, with flat opaque key validation delegated to
    SQLiteStorageDriver.

    Example:
        >>> store = SingleFileSqliteStorageBackend(database_path)  # doctest: +SKIP
        >>> store.configuration.store_kind  # doctest: +SKIP
        'single_file_sqlite'
    """

    store_kind = "single_file_sqlite"


__all__ = ["SingleFileSqliteStorageBackend"]
