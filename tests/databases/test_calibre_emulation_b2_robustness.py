from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from LiuXin_alpha.databases.calibre_emulation import (
    CalibreDB,
    CalibreReader,
    CalibreUnsupportedVersionError,
    CalibreVersionPolicy,
)


def test_iter_book_payloads_best_effort_survives_missing_author_tables(provision_calibre_library) -> None:
    lib = provision_calibre_library(name="lib_b2_missing_authors")

    from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import CalibreLibraryBuilder

    b = CalibreLibraryBuilder(lib.root)
    b.add_book(
        title="Mangled, but readable",
        authors=["Someone"],
        formats={"EPUB": b"epub"},
    )

    # Mangle: rename author tables away.
    conn = sqlite3.connect(str(lib.metadata_db))
    try:
        conn.execute("ALTER TABLE authors RENAME TO authors_gone")
        conn.execute("ALTER TABLE books_authors_link RENAME TO books_authors_link_gone")
        conn.commit()
    finally:
        conn.close()

    r = CalibreReader.from_root(lib.root)
    payloads = list(r.iter_book_payloads(batch_size=10, best_effort=True))
    assert len(payloads) == 1
    p = payloads[0]
    assert p.title == "Mangled, but readable"
    assert p.authors == ()
    assert any(w == "missing_tables:authors" for w in p.warnings)


def test_schema_info_best_effort_records_missing_core_tables(tmp_path: Path) -> None:
    root = tmp_path / "badlib_missing_core"
    root.mkdir(parents=True, exist_ok=True)
    db_path = root / "metadata.db"

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA application_id = 0")
        conn.execute("PRAGMA user_version = 0")
        # Deliberately incomplete.
        conn.execute("CREATE TABLE books(id INTEGER PRIMARY KEY, title TEXT, path TEXT)")
        conn.commit()
    finally:
        conn.close()

    db = CalibreDB.from_root(root)
    info = db.schema_info(require_core_tables=True, best_effort=True)
    assert any(i.code == "missing_core_tables" for i in info.issues)


def test_schema_info_strict_version_policy_can_refuse(tmp_path: Path) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator.database_generator import (
        calibre_metadata_application_id,
        calibre_metadata_user_version,
    )

    root = tmp_path / "badlib_version_refuse"
    root.mkdir(parents=True, exist_ok=True)
    db_path = root / "metadata.db"

    max_uv = int(calibre_metadata_user_version())
    exp_app = int(calibre_metadata_application_id())

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA application_id = 0")  # mismatch
        conn.execute(f"PRAGMA user_version = {max_uv + 999}")
        # Minimal core tables so version policy is the failure point.
        conn.execute("CREATE TABLE books(id INTEGER PRIMARY KEY, title TEXT, path TEXT)")
        conn.execute("CREATE TABLE authors(id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("CREATE TABLE books_authors_link(id INTEGER PRIMARY KEY, book INTEGER, author INTEGER)")
        conn.execute("CREATE TABLE data(id INTEGER PRIMARY KEY, book INTEGER, format TEXT, name TEXT)")
        conn.execute("CREATE TABLE custom_columns(id INTEGER PRIMARY KEY, label TEXT, name TEXT, datatype TEXT, is_multiple INTEGER, display TEXT)")
        conn.commit()
    finally:
        conn.close()

    policy = CalibreVersionPolicy(
        expected_application_id=exp_app,
        latest_supported_user_version=max_uv,
        known_user_version_max=max_uv,
        allow_application_id_mismatch=False,
        allow_newer_user_version=False,
    )

    db = CalibreDB.from_root(root)
    with pytest.raises(CalibreUnsupportedVersionError):
        _ = db.schema_info(version_policy=policy)
