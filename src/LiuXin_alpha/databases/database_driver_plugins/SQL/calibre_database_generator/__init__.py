"""Calibre database generator helpers.

This package provides utilities for creating and validating *Calibre-style*
SQLite databases using the canonical SQL shipped with Calibre.

Design goals:
- Reuse LiuXin's calibre resources system (LiuXin_resources/calibre_resources)
  via `LiuXin_alpha.utils.resources.get_path`.
- Keep Calibre schema versioning observable (pragma user_version).
- Keep Phase 1 lightweight: locate SQL resources and expose version metadata.
"""

from __future__ import annotations

from .database_generator import (
    calibre_sql_paths,
    calibre_metadata_user_version,
    calibre_metadata_application_id,
    read_calibre_sql,
)

__all__ = [
    "calibre_sql_paths",
    "calibre_metadata_user_version",
    "calibre_metadata_application_id",
    "read_calibre_sql",
]
