"""
Database driver plugin registry and compatibility helpers.
"""

from __future__ import annotations

from LiuXin_alpha.databases.database_driver_plugins.registry import (
    create_new_database,
    get_database_builder_module,
    get_direct_access_module,
    get_driver_location,
    get_registered_database_driver_names,
    load_database_driver,
    register_database_driver,
)


# Todo: NO SHIMS
# Backwards-compatible public name.
def loadDatabaseDriver(db_type: str):
    return load_database_driver(db_type)


__all__ = [
    "create_new_database",
    "get_database_builder_module",
    "get_direct_access_module",
    "get_driver_location",
    "get_registered_database_driver_names",
    "loadDatabaseDriver",
    "load_database_driver",
    "register_database_driver",
]
