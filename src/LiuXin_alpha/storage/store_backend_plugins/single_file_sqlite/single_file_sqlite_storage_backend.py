"""Single-file SQLite Store implemented on the new storage contracts."""

from LiuXin_alpha.storage.stores import SQLiteStore


class SingleFileSqliteStorageBackend(SQLiteStore):
    """Compatibility class name for LiuXin's transactional SQLite Store."""

    store_kind = "single_file_sqlite"


__all__ = ["SingleFileSqliteStorageBackend"]
