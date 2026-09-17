"""
Expose the FRBR-first SQLite schema creation entry point.

create_new_database receives an open connection and builds from packaged SQL/TOML;
it does not choose a filesystem path or close the caller connection.
"""

# Front end for the FRBR-first database generator.

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr.database_generator import (
    create_new_database,
)

__author__ = "Cameron"

__all__ = ["create_new_database"]
