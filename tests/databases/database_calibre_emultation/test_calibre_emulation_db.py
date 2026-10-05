"""
Check read-only Calibre database access, schema metadata, custom and auxiliary database discovery, and version-plan warnings.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_db.py
"""
from __future__ import annotations

import sqlite3

import pytest

from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation import (
    CalibreDB,
    CalibreLibraryNotFoundError,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import (
    CalibreLibraryBuilder,
    calibre_metadata_schema_info,
)


def test_calibre_db_from_root_raises_when_missing(tmp_path) -> None:
    """
    Construct a database descriptor for an empty directory and check schema_info raises CalibreLibraryNotFoundError.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_db.py::test_calibre_db_from_root_raises_when_missing


    :param tmp_path: Pytest-provided temporary directory for isolated database and
        fixture files.
    :return: None; failed expectations raise AssertionError.
    """
    db = CalibreDB.from_root(tmp_path)
    with pytest.raises(CalibreLibraryNotFoundError):
        _ = db.schema_info()


def test_calibre_db_connects_readonly_and_blocks_writes(provision_calibre_library) -> None:
    """
    Open a reader connection and check CREATE TABLE raises sqlite3.OperationalError; close the connection in finally.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_db.py::test_calibre_db_connects_readonly_and_blocks_writes


    :param provision_calibre_library: Fixture factory creating an isolated blank
        library; skips when SQLite lacks the required FTS5 support.
    :return: None; failed expectations raise AssertionError.
    """
    lib = provision_calibre_library(name="lib_ro")
    db = CalibreDB.from_root(lib.root)

    conn = db.connect()
    try:
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("CREATE TABLE should_fail(x INT)")
    finally:
        conn.close()


def test_schema_info_matches_calibre_sql_snapshot(provision_calibre_library) -> None:
    """
    Check schema application_id and user_version match the bundled SQL snapshot metadata.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_db.py::test_schema_info_matches_calibre_sql_snapshot


    :param provision_calibre_library: Fixture factory creating an isolated blank
        library; skips when SQLite lacks the required FTS5 support.
    :return: None; failed expectations raise AssertionError.
    """
    lib = provision_calibre_library(name="lib_schema")
    db = CalibreDB.from_root(lib.root)

    info = db.schema_info()
    expected = calibre_metadata_schema_info()

    assert info.application_id == expected.application_id
    assert info.user_version == expected.user_version


def test_schema_info_contains_core_tables_and_triggers(provision_calibre_library) -> None:
    """
    Check a required subset of core tables exists and at least one trigger is reported.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_db.py::test_schema_info_contains_core_tables_and_triggers


    :param provision_calibre_library: Fixture factory creating an isolated blank
        library; skips when SQLite lacks the required FTS5 support.
    :return: None; failed expectations raise AssertionError.
    """
    lib = provision_calibre_library(name="lib_core")
    db = CalibreDB.from_root(lib.root)

    info = db.schema_info()
    tables = set(info.tables)
    assert {"books", "authors", "books_authors_link", "custom_columns", "library_id"}.issubset(tables)
    # Calibre metadata schema is trigger-heavy.
    assert len(info.triggers) > 0


def test_schema_info_custom_columns_detected(provision_calibre_library) -> None:
    """
    Create a series custom column and check introspection includes its label, numeric ID, and datatype.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_db.py::test_schema_info_custom_columns_detected


    :param provision_calibre_library: Fixture factory creating an isolated blank
        library; skips when SQLite lacks the required FTS5 support.
    :return: None; failed expectations raise AssertionError.
    """
    lib = provision_calibre_library(name="lib_cc")

    # Create a custom column using the builder so the DB is realistic.
    b = CalibreLibraryBuilder(lib.root)
    cc_num = b.create_custom_column(label="cc_series", name="Series", datatype="series")

    db = CalibreDB.from_root(lib.root)
    info = db.schema_info(include_custom_columns=True)

    assert len(info.custom_columns) >= 1
    found = [c for c in info.custom_columns if c.label == "cc_series"]
    assert found, "Expected to find newly created custom column"
    assert found[0].num == cc_num
    assert found[0].datatype == "series"


def test_schema_info_detects_optional_aux_dbs(provision_calibre_library) -> None:
    """
    Provision notes and full-text auxiliary databases in best-effort mode and check both presence flags.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_db.py::test_schema_info_detects_optional_aux_dbs


    :param provision_calibre_library: Fixture factory creating an isolated blank
        library; skips when SQLite lacks the required FTS5 support.
    :return: None; failed expectations raise AssertionError.
    """
    lib = provision_calibre_library(
        name="lib_aux",
        create_notes_db=True,
        create_fts_db=True,
        best_effort_aux_dbs=True,
    )

    db = CalibreDB.from_root(lib.root)
    info = db.schema_info()
    assert info.has_notes is True
    assert info.has_fts is True

def test_schema_info_includes_version_plan(provision_calibre_library) -> None:
    """
    Check the generated library’s plan agrees with its PRAGMAs, has a known version ceiling, and contains no warnings.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_db.py::test_schema_info_includes_version_plan


    :param provision_calibre_library: Fixture factory creating an isolated blank
        library; skips when SQLite lacks the required FTS5 support.
    :return: None; failed expectations raise AssertionError.
    """
    lib = provision_calibre_library(name="lib_version_plan")
    db = CalibreDB.from_root(lib.root)

    info = db.schema_info()
    assert info.version_plan is not None
    assert info.version_plan.application_id == info.application_id
    assert info.version_plan.user_version == info.user_version
    # The shipped SQL snapshot should be detectable in this repo.
    assert info.version_plan.known_user_version_max is not None
    assert info.version_plan.warnings == ()


def test_schema_info_version_plan_warns_on_mismatch_and_newer_schema(tmp_path) -> None:
    # Build a minimal-but-valid Calibre-ish DB with mismatched pragma values.
    """
    Build a core schema with mismatched application ID and a newer version, then require both warning categories and an accepted mismatch/newer status.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_db.py::test_schema_info_version_plan_warns_on_mismatch_and_newer_schema


    :param tmp_path: Pytest-provided temporary directory for isolated database and
        fixture files.
    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import calibre_metadata_user_version

    root = tmp_path / "badlib"
    root.mkdir(parents=True, exist_ok=True)
    db_path = root / "metadata.db"

    import sqlite3

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA application_id = 0")
        conn.execute(f"PRAGMA user_version = {calibre_metadata_user_version() + 999}")
        conn.execute("CREATE TABLE books(id INTEGER PRIMARY KEY, title TEXT, path TEXT)")
        conn.execute("CREATE TABLE authors(id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("CREATE TABLE books_authors_link(id INTEGER PRIMARY KEY, book INTEGER, author INTEGER)")
        conn.execute("CREATE TABLE data(id INTEGER PRIMARY KEY, book INTEGER, format TEXT, name TEXT)")
        conn.execute("CREATE TABLE custom_columns(id INTEGER PRIMARY KEY, label TEXT, name TEXT, datatype TEXT, is_multiple INTEGER, display TEXT)")
        conn.commit()
    finally:
        conn.close()

    db = CalibreDB.from_root(root)
    info = db.schema_info()
    assert info.version_plan is not None
    assert info.version_plan.status in {"application_id_mismatch", "newer_than_supported"}
    assert any(w.startswith("application_id_mismatch") for w in info.version_plan.warnings)
    assert any(w.startswith("schema_newer_than_supported") for w in info.version_plan.warnings)

