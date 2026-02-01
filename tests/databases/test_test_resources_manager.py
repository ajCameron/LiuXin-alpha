from __future__ import annotations

import sqlite3
from pathlib import Path


def _count_titles(db_path: Path) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute("SELECT COUNT(*) FROM titles;").fetchone()
        assert row is not None
        return int(row[0])
    finally:
        conn.close()


def test_resources_manager_lists_default_dbs(test_resources_manager) -> None:
    names = test_resources_manager.available_test_databases()
    assert "test_db_0" in names
    assert "test_db_2" in names
    assert "test_db_3" in names
    assert "test_db_13" in names


def test_provisioned_database_opens(provision_test_database) -> None:
    provisioned = provision_test_database("test_db_0")

    # DB should be a valid sqlite file with expected schema.
    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='titles' LIMIT 1;"
        ).fetchone()
        assert row is not None
    finally:
        conn.close()


def test_test_db_2_generates_and_is_pruned(provision_test_database) -> None:
    provisioned = provision_test_database("test_db_2")

    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        # Should have the standard schema.
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='titles' LIMIT 1;"
        ).fetchone()
        assert row is not None

        # Historical test_db_2 is derived from test_db_1 but pruned down to title_id=1.
        max_id = conn.execute("SELECT MAX(title_id) FROM titles;").fetchone()[0]
        assert int(max_id) == 1

        title_count = conn.execute("SELECT COUNT(*) FROM titles;").fetchone()[0]
        assert int(title_count) == 1

        book_count = conn.execute("SELECT COUNT(*) FROM books;").fetchone()[0]
        assert int(book_count) == 1
    finally:
        conn.close()


def test_test_db_3_generates_formats_fixture(provision_test_database) -> None:
    provisioned = provision_test_database("test_db_3")

    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        folder_count = int(conn.execute("SELECT COUNT(*) FROM folders;").fetchone()[0])
        file_count = int(conn.execute("SELECT COUNT(*) FROM files;").fetchone()[0])
        bfl_count = int(conn.execute("SELECT COUNT(*) FROM book_folder_links;").fetchone()[0])
        ffl_count = int(conn.execute("SELECT COUNT(*) FROM file_folder_links;").fetchone()[0])

        # These counts are deterministic (ported from legacy test_db_3 generation).
        assert folder_count == 497
        assert file_count == 2440
        assert bfl_count == 497
        assert ffl_count == 2440

        ext_counts = dict(
            (row[0], int(row[1]))
            for row in conn.execute(
                "SELECT file_extension, COUNT(*) FROM files GROUP BY file_extension;"
            ).fetchall()
        )
        assert ext_counts == {"epub": 814, "mobi": 813, "pdf": 813}

        viol = conn.execute("PRAGMA foreign_key_check;").fetchall()
        assert viol == []
    finally:
        conn.close()

def test_provisioned_copies_are_independent(tmp_path, test_resources_manager) -> None:
    db1 = test_resources_manager.provision_named_test_database(name="test_db_0", dst_dir=tmp_path / "a")
    db2 = test_resources_manager.provision_named_test_database(name="test_db_0", dst_dir=tmp_path / "b")

    assert _count_titles(db1.db_path) == _count_titles(db2.db_path)

    from tests.support.test_resources_manager import _insert_minimal_row, _register_sqlite_test_functions

    conn = sqlite3.connect(str(db1.db_path))
    try:
        _register_sqlite_test_functions(conn)
        _insert_minimal_row(conn, table="titles", preferred_text_value="Another Title")
        conn.commit()
    finally:
        conn.close()

    assert _count_titles(db1.db_path) == _count_titles(db2.db_path) + 1
