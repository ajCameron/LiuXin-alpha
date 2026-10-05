"""
Check copied library templates receive distinct UUIDs and requested best-effort auxiliary database files and core tables.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_library_templates.py
"""
from __future__ import annotations

import sqlite3

import pytest


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


def test_provision_calibre_library_reseeds_library_uuid(provision_calibre_library) -> None:
    """
    Provision two roots, check distinct reported UUIDs, and verify the first library’s UUID is stored in its database.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_library_templates.py::test_provision_calibre_library_reseeds_library_uuid


    :param provision_calibre_library: Fixture factory creating an isolated blank
        library; skips when SQLite lacks required FTS5 support.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Request notes and full-text databases and check their files and required core-table subsets exist.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_library_templates.py::test_provision_calibre_library_with_aux_dbs_best_effort


    :param provision_calibre_library: Fixture factory creating an isolated blank
        library; skips when SQLite lacks required FTS5 support.
    :return: None; failed expectations raise AssertionError.
    """
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
