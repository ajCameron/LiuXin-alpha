"""Regression tests for FRBR database generator builder concreteness."""

from __future__ import annotations

import inspect
import sqlite3

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as frbr_gen


def test_frbr_builder_is_concrete() -> None:
    """SQLiteDatabaseBuilder must be instantiable (not abstract)."""
    assert not inspect.isabstract(frbr_gen.SQLiteDatabaseBuilder)

    conn = sqlite3.connect(":memory:")
    try:
        frbr_gen.SQLiteDatabaseBuilder(conn=conn)
    finally:
        conn.close()
