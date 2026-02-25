"""Backward-compatible entrypoint for the SQL database generator.

Historically callers imported ``create_new_database`` from this module path.
The FRBR-first generator now lives under ``database_generator_frbr``.
"""

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import create_new_database

__all__ = ["create_new_database"]
