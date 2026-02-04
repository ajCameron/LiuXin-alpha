"""Calibre emulation helpers.

This package will host *readers* and other compatibility helpers for working
with existing Calibre libraries (metadata.db + on-disk layout).

Stage A1: shared types used by the readers/import pipeline.

Stage A2: read-only DB wrapper + schema discovery.
"""

from __future__ import annotations

from .types import (
    CalibreLibraryPaths,
    CalibreSchemaInfo,
    CalibreCustomColumnDef,
    CalibreSeriesRef,
    CalibreFormatRef,
    CalibreBookRow,
    CalibreBookNormalized,
)

from .errors import CalibreError, CalibreLibraryNotFoundError, CalibreSchemaError
from .db import CalibreDB

__all__ = [
    "CalibreDB",
    "CalibreError",
    "CalibreLibraryNotFoundError",
    "CalibreSchemaError",
    "CalibreLibraryPaths",
    "CalibreSchemaInfo",
    "CalibreCustomColumnDef",
    "CalibreSeriesRef",
    "CalibreFormatRef",
    "CalibreBookRow",
    "CalibreBookNormalized",
]
