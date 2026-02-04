"""Tests for creating a minimal on-disk Calibre library skeleton."""

from __future__ import annotations

import sqlite3

import pytest

from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import (
    create_calibre_library_skeleton,
)


def _sqlite_has_fts5(conn: sqlite3.Connection) -> bool:
    try:
        conn.execute("CREATE VIRTUAL TABLE temp._fts5_probe USING fts5(x)")
        conn.execute("DROP TABLE temp._fts5_probe")
        return True
    except sqlite3.OperationalError:
        return False


def test_create_calibre_library_skeleton_metadata_only(tmp_path) -> None:
    # metadata.db requires FTS5 in Calibre's schema snapshot.
    probe = sqlite3.connect(":memory:")
    try:
        if not _sqlite_has_fts5(probe):
            pytest.skip("SQLite build lacks FTS5; Calibre metadata schema requires it")
    finally:
        probe.close()

    root = tmp_path / "My Library"
    paths = create_calibre_library_skeleton(root)

    assert (root / "metadata.db").exists()
    assert (root / "data").exists()

    conn = sqlite3.connect(str(root / "metadata.db"))
    try:
        row = conn.execute("SELECT uuid FROM library_id LIMIT 1").fetchone()
        assert row is not None
        assert isinstance(row[0], str) and row[0]
    finally:
        conn.close()


def test_create_calibre_library_skeleton_with_aux_dbs_best_effort(tmp_path) -> None:
    # Even if FTS5/custom tokenizers are missing, best-effort mode should create the files
    # with at least the core non-virtual tables.
    probe = sqlite3.connect(":memory:")
    try:
        if not _sqlite_has_fts5(probe):
            pytest.skip("SQLite build lacks FTS5; Calibre metadata schema requires it")
    finally:
        probe.close()

    root = tmp_path / "Library With Aux"
    paths = create_calibre_library_skeleton(
        root,
        create_notes_db=True,
        create_fts_db=True,
        best_effort_aux_dbs=True,
    )

    assert (root / "metadata.db").exists()
    assert (root / ".calnotes" / "notes.db").exists()
    assert (root / "full-text-search.db").exists()

    # Notes DB: core tables must exist
    nconn = sqlite3.connect(str(root / ".calnotes" / "notes.db"))
    try:
        tables = {r[0] for r in nconn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"notes", "resources", "notes_resources_link"}.issubset(tables)
    finally:
        nconn.close()

    # FTS DB: core tables must exist
    fconn = sqlite3.connect(str(root / "full-text-search.db"))
    try:
        tables = {r[0] for r in fconn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"dirtied_formats", "books_text"}.issubset(tables)
    finally:
        fconn.close()
