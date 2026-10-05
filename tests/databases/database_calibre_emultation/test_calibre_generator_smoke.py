"""
Check bundled Calibre SQL resources, positive version metadata, and in-memory schema creation when SQLite supports FTS5.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_generator_smoke.py
"""

from __future__ import annotations

import os
import pathlib
import sqlite3

import pytest

from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import database_generator as cal_gen


def test_calibre_sql_resources_are_present_and_nonempty() -> None:
    """
    Require a nonempty SQL-resource mapping and check every path is a file containing non-whitespace UTF-8-decoded text.

    Decoding uses replacement for invalid bytes rather than asserting valid UTF-8.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_generator_smoke.py::test_calibre_sql_resources_are_present_and_nonempty


    :return: None; failed expectations raise AssertionError.
    """
    paths = cal_gen.calibre_sql_paths()
    assert paths, "Expected calibre SQL resources mapping"

    for key, path in paths.items():
        p = pathlib.Path(path)
        assert p.exists(), f"Missing calibre SQL resource for {key}: {p}"
        assert p.is_file(), f"Expected file for {key}: {p}"
        text = p.read_text(encoding="utf-8", errors="replace")
        assert text.strip(), f"Empty calibre SQL file for {key}: {p}"


def test_calibre_metadata_version_metadata_is_extractable() -> None:
    """
    Check both extracted user_version and application_id are positive integers.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_generator_smoke.py::test_calibre_metadata_version_metadata_is_extractable


    :return: None; failed expectations raise AssertionError.
    """
    user_version = cal_gen.calibre_metadata_user_version()
    application_id = cal_gen.calibre_metadata_application_id()

    assert isinstance(user_version, int) and user_version > 0
    assert isinstance(application_id, int) and application_id > 0


def _sqlite_has_fts5(conn: sqlite3.Connection) -> bool:
    """
    Create and drop a temporary FTS5 virtual table to probe support on the supplied connection.

    Return False for sqlite3.OperationalError from either statement; other errors
    propagate. Do not close the connection or explicitly commit.

    Example:
        >>> connection = sqlite3.connect(':memory:')
        >>> isinstance(_sqlite_has_fts5(connection), bool)
        True
        >>> connection.close()


    :param conn: Caller-owned sqlite3 connection; the probe mutates its temporary
        schema.
    :return: True only when both probe statements succeed.
    """
    try:
        conn.execute("CREATE VIRTUAL TABLE temp._fts5_probe USING fts5(x)")
        conn.execute("DROP TABLE temp._fts5_probe")
        return True
    except sqlite3.OperationalError:
        return False


def test_create_new_calibre_metadata_db_in_memory() -> None:
    """
    Create and validate a Calibre schema in memory and check the books table exists; skip without FTS5.

    The test does not explicitly close its in-memory connection.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_generator_smoke.py::test_create_new_calibre_metadata_db_in_memory


    :return: None; failed expectations raise AssertionError.
    """
    conn = sqlite3.connect(":memory:")

    if not _sqlite_has_fts5(conn):
        pytest.skip("SQLite build lacks FTS5; Calibre metadata schema requires it")

    # Should create schema and validate application_id/user_version.
    cal_gen.create_new_database(conn, validate=True)

    # Spot check that a core Calibre table exists.
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='books'"
    ).fetchone()
    assert row is not None
