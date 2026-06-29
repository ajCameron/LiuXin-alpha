"""
Local bridge for field-metadata dependencies.

The database layer should not directly depend on a particular surface package
layout. Import this bridge instead.
"""

# Todo: Probably best just to kill this... and, ideally, the underlying dependency

from __future__ import annotations

from LiuXin_alpha.surfaces.field_metadata import FieldMetadata


__all__ = ["FieldMetadata"]
