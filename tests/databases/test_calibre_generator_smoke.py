"""Smoke tests for the Calibre schema access helpers.

These tests ensure LiuXin's calibre resources are present and that we can
extract schema version metadata from the canonical SQL snapshot.
"""

from __future__ import annotations

import os
import pathlib
import sqlite3

import pytest

from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import database_generator as cal_gen


def test_calibre_sql_resources_are_present_and_nonempty() -> None:
    paths = cal_gen.calibre_sql_paths()
    assert paths, "Expected calibre SQL resources mapping"

    for key, path in paths.items():
        p = pathlib.Path(path)
        assert p.exists(), f"Missing calibre SQL resource for {key}: {p}"
        assert p.is_file(), f"Expected file for {key}: {p}"
        text = p.read_text(encoding="utf-8", errors="replace")
        assert text.strip(), f"Empty calibre SQL file for {key}: {p}"


def test_calibre_metadata_version_metadata_is_extractable() -> None:
    user_version = cal_gen.calibre_metadata_user_version()
    application_id = cal_gen.calibre_metadata_application_id()

    assert isinstance(user_version, int) and user_version > 0
    assert isinstance(application_id, int) and application_id > 0


def _sqlite_has_fts5(conn: sqlite3.Connection) -> bool:
    try:
        conn.execute("CREATE VIRTUAL TABLE temp._fts5_probe USING fts5(x)")
        conn.execute("DROP TABLE temp._fts5_probe")
        return True
    except sqlite3.OperationalError:
        return False


def test_create_new_calibre_metadata_db_in_memory() -> None:
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
