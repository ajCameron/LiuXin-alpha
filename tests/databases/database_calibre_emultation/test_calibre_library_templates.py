from __future__ import annotations

import sqlite3

import pytest


def _sqlite_has_fts5(conn: sqlite3.Connection) -> bool:
    try:
        conn.execute("CREATE VIRTUAL TABLE temp._fts5_probe USING fts5(x)")
        conn.execute("DROP TABLE temp._fts5_probe")
        return True
    except sqlite3.OperationalError:
        return False


def test_provision_calibre_library_reseeds_library_uuid(provision_calibre_library) -> None:
    lib1 = provision_calibre_library(name="lib1")
    lib2 = provision_calibre_library(name="lib2")

    assert lib1.root.exists()
    assert lib2.root.exists()
    assert lib1.library_uuid != lib2.library_uuid

    # Verify the UUID is actually in the DB.
    conn = sqlite3.connect(str(lib1.metadata_db))
    try:
        row = conn.execute("SELECT uuid FROM library_id LIMIT 1").fetchone()
        assert row and row[0] == lib1.library_uuid
    finally:
        conn.close()


def test_provision_calibre_library_with_aux_dbs_best_effort(provision_calibre_library) -> None:
    # Notes/FTS aux dbs may not be fully creatable without Calibre tokenizer;
    # best-effort should still create base tables and files.
    lib = provision_calibre_library(
        name="lib_aux",
        create_notes_db=True,
        create_fts_db=True,
        best_effort_aux_dbs=True,
    )

    assert lib.metadata_db.exists()
    assert lib.notes_db is not None and lib.notes_db.exists()
    assert lib.fts_db is not None and lib.fts_db.exists()

    # Notes DB should at least have core tables.
    nconn = sqlite3.connect(str(lib.notes_db))
    try:
        tables = {r[0] for r in nconn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"notes", "resources", "notes_resources_link"}.issubset(tables)
    finally:
        nconn.close()

    # FTS DB should at least have core tables.
    fconn = sqlite3.connect(str(lib.fts_db))
    try:
        tables = {r[0] for r in fconn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"dirtied_formats", "books_text"}.issubset(tables)
    finally:
        fconn.close()
