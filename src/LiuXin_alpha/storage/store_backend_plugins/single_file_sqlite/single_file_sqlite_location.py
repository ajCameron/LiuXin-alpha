"""
Retain SingleFileSqliteStoreLocation as an exact alias of the shared Location value.

This compatibility name adds no parsing, database access, or backend-specific
state. Flat SQLite key validation belongs to the configured Store/driver path.
"""

from LiuXin_alpha.storage.api import Location


SingleFileSqliteStoreLocation = Location


__all__ = ["SingleFileSqliteStoreLocation"]
