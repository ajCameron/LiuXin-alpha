"""
Expose Calibre SQLite schema creation and library-population helpers.

Resources come from the package-owned Calibre snapshot. Skeleton creation produces
metadata.db and optional auxiliary databases; CalibreLibraryBuilder adds metadata
and files using the minimal trigger functions required by that snapshot.
"""

from __future__ import annotations

from .database_generator import (
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

from .library_builder import (
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
