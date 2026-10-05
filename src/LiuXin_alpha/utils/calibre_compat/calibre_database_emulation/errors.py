"""
Define typed failures for Calibre library discovery, schema, corruption and path safety.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise errors through a consuming regression::

        python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_db.py
"""

from __future__ import annotations


class CalibreError(Exception):
    """
    Base error for Calibre emulation.

    Example:
        Exercise CalibreError through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_db.py
    """


class CalibreLibraryNotFoundError(CalibreError):
    """
    Raised when a Calibre library (or its metadata.db) cannot be found.

    Example:
        Exercise CalibreLibraryNotFoundError through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_db.py
    """


class CalibreSchemaError(CalibreError):
    """
    Raised when an existing Calibre database does not match expectations.

    Example:
        Exercise CalibreSchemaError through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_db.py
    """


class CalibreCorruptError(CalibreError):
    """
    Raised when SQLite reports the database file is corrupt or unreadable.

    Example:
        Exercise CalibreCorruptError through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_db.py
    """


class CalibreUnsupportedVersionError(CalibreSchemaError):
    """
    Raised when a Calibre database version is outside an enforced policy.

    Example:
        Exercise CalibreUnsupportedVersionError through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_db.py
    """


class CalibreUnsafePathError(CalibreSchemaError):
    """
    Raised when a DB path attempts to escape the library root.

    Example:
        Exercise CalibreUnsafePathError through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_db.py
    """
