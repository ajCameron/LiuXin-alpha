"""
Retain SingleFileSqliteSingleFile as an exact alias of shared FileInfo metadata.

SQLite Store operations return the common FileInfo record, not an object that
opens BLOBs or owns a database connection. The compatibility name adds no behavior.
"""

from LiuXin_alpha.storage.api import FileInfo


SingleFileSqliteSingleFile = FileInfo


__all__ = ["SingleFileSqliteSingleFile"]
