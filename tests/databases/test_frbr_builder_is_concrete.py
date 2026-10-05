"""
Guard against accidentally making the SQLite FRBR generator abstract.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_frbr_builder_is_concrete.py
"""

from __future__ import annotations

import inspect
import sqlite3

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as frbr_gen


def test_frbr_builder_is_concrete() -> None:
    """
    Require a concrete generator class and instantiate it against an in-memory connection.

    Closes the connection in finally. Construction is exercised without asserting a full
    schema build.

    Example:
        >>> test_frbr_builder_is_concrete()


    :return: None; failed expectations raise AssertionError.
    """
    assert not inspect.isabstract(frbr_gen.SQLiteDatabaseGenerator)

    conn = sqlite3.connect(":memory:")
    try:
        frbr_gen.SQLiteDatabaseGenerator(conn=conn)
    finally:
        conn.close()
