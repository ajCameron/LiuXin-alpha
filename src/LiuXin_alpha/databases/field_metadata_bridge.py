"""Local bridge for field-metadata dependencies.

The database layer should not directly depend on a particular surface package
layout. Import this bridge instead.
"""

from __future__ import annotations

from LiuXin_alpha.surfaces.field_metadata import FieldMetadata


__all__ = ["FieldMetadata"]
