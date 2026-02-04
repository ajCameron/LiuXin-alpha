from __future__ import annotations

import sqlite3

import pytest

from LiuXin_alpha.databases.calibre_emulation import (
    CalibreDB,
    CalibreLibraryNotFoundError,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import (
    CalibreLibraryBuilder,
    calibre_metadata_schema_info,
)


def test_calibre_db_from_root_raises_when_missing(tmp_path) -> None:
    db = CalibreDB.from_root(tmp_path)
    with pytest.raises(CalibreLibraryNotFoundError):
        _ = db.schema_info()


def test_calibre_db_connects_readonly_and_blocks_writes(provision_calibre_library) -> None:
    lib = provision_calibre_library(name="lib_ro")
    db = CalibreDB.from_root(lib.root)

    conn = db.connect()
    try:
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("CREATE TABLE should_fail(x INT)")
    finally:
        conn.close()


def test_schema_info_matches_calibre_sql_snapshot(provision_calibre_library) -> None:
    lib = provision_calibre_library(name="lib_schema")
    db = CalibreDB.from_root(lib.root)

    info = db.schema_info()
    expected = calibre_metadata_schema_info()

    assert info.application_id == expected.application_id
    assert info.user_version == expected.user_version


def test_schema_info_contains_core_tables_and_triggers(provision_calibre_library) -> None:
    lib = provision_calibre_library(name="lib_core")
    db = CalibreDB.from_root(lib.root)

    info = db.schema_info()
    tables = set(info.tables)
    assert {"books", "authors", "books_authors_link", "custom_columns", "library_id"}.issubset(tables)
    # Calibre metadata schema is trigger-heavy.
    assert len(info.triggers) > 0


def test_schema_info_custom_columns_detected(provision_calibre_library) -> None:
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
