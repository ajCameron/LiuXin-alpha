"""Calibre database generator helpers.

This package provides utilities for creating and validating *Calibre-style*
SQLite databases using the canonical SQL shipped with Calibre.

Design goals:
- Reuse LiuXin's package-owned Calibre resource system via
  `LiuXin_alpha.utils.resources.get_path`.
- Keep Calibre schema versioning observable (pragma user_version).
- Keep Phase 1 lightweight: locate SQL resources and expose version metadata.
"""

from __future__ import annotations

from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator.database_generator import (
    create_new_database,
    create_calibre_library_skeleton,
    CalibreLibraryPaths,
    validate_metadata_database,
    ensure_library_id_row,
    calibre_sql_paths,
    calibre_metadata_schema_info,
    calibre_metadata_user_version,
    calibre_metadata_application_id,
    read_calibre_sql,
)

from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator.library_builder import (
    CalibreLibraryBuilder,
    AddedBook,
    AddedFormat,
)

__all__ = [
    "create_new_database",
    "create_calibre_library_skeleton",
    "CalibreLibraryPaths",
    "validate_metadata_database",
    "ensure_library_id_row",
    "calibre_sql_paths",
    "calibre_metadata_schema_info",
    "calibre_metadata_user_version",
    "calibre_metadata_application_id",
    "read_calibre_sql",
    "CalibreLibraryBuilder",
    "AddedBook",
    "AddedFormat",
]
