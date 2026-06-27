"""Local bridge for field-metadata dependencies.

The database layer should not need to care about the exact module layout of the
surface package. Import this bridge instead.
"""

from __future__ import annotations

from LiuXin_alpha.surfaces.field_metadata import FieldMetadata


__all__ = ["FieldMetadata"]
