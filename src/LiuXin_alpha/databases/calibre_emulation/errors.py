"""Error types for Calibre emulation readers."""

from __future__ import annotations


class CalibreError(Exception):
    """Base error for Calibre emulation."""


class CalibreLibraryNotFoundError(CalibreError):
    """Raised when a Calibre library (or its metadata.db) cannot be found."""


class CalibreSchemaError(CalibreError):
    """Raised when an existing Calibre database does not match expectations."""


class CalibreCorruptError(CalibreError):
    """Raised when SQLite reports the database file is corrupt or unreadable."""


class CalibreUnsupportedVersionError(CalibreSchemaError):
    """Raised when a Calibre database version is outside an enforced policy."""


class CalibreUnsafePathError(CalibreSchemaError):
    """Raised when a DB path attempts to escape the library root."""
