from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from tests.support import test_resources_manager as trm


ALL_TEST_DB_NAMES = tuple(f"test_db_{i}" for i in range(26))
ASSET_HEAVY_DB_NAMES = ("test_db_3", "test_db_5", "test_db_11")


def _relation_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ? LIMIT 1;",
        (name,),
    ).fetchone()
    return row is not None


def _expected_book_count(name: str) -> int:
    if name == "test_db_0":
        return 0
    if name == "test_db_2":
        return 1
    if name == "test_db_3":
        return 1
    if name == "test_db_13":
        return 0

    profile = trm._legacy_test_db_profiles().get(name, {"books": 12})
    return int(profile.get("books", 12))


def _expected_title_count(name: str) -> int:
    if name == "test_db_0":
        return 1
    return _expected_book_count(name)


def _expected_asset_counts(name: str) -> tuple[int, int]:
    if name == "test_db_3":
        return 497, 2440
    if name in {"test_db_5", "test_db_11"}:
        profile = trm._legacy_test_db_profiles()[name]
        return int(profile["folders"]), int(profile["files"])
    return 0, 0


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


def test_resources_manager_lists_full_legacy_range(test_resources_manager) -> None:
    names = set(test_resources_manager.available_test_databases())
    expected = set(ALL_TEST_DB_NAMES)
    assert expected.issubset(names)


def test_provisioned_database_opens(provision_test_database) -> None:
    provisioned = provision_test_database("test_db_0")

    # DB should be a valid sqlite file with expected schema.
    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name='titles' LIMIT 1;"
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
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name='titles' LIMIT 1;"
        ).fetchone()
        assert row is not None

        # test_db_2 is expected to expose a single title/book projection.
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
        book_count = int(conn.execute("SELECT COUNT(*) FROM books;").fetchone()[0])
        folder_count = int(conn.execute("SELECT COUNT(*) FROM folders;").fetchone()[0])
        file_count = int(conn.execute("SELECT COUNT(*) FROM files;").fetchone()[0])
        fwl_count = int(conn.execute("SELECT COUNT(*) FROM folder_work_links;").fetchone()[0])
        ffl_count = int(conn.execute("SELECT COUNT(*) FROM file_folder_links;").fetchone()[0])

        # These counts are deterministic.
        assert book_count == 1
        assert folder_count == 497
        assert file_count == 2440
        assert fwl_count == 497
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


@pytest.mark.db
@pytest.mark.slow
@pytest.mark.parametrize("db_name", ALL_TEST_DB_NAMES)
def test_all_test_db_profiles_smoke_and_shape(provision_test_database, db_name: str) -> None:
    provisioned = provision_test_database(db_name)
    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        assert conn.execute("PRAGMA integrity_check;").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check;").fetchall() == []

        assert _relation_exists(conn, "titles")
        assert _relation_exists(conn, "books")
        assert _relation_exists(conn, "works")
        assert _relation_exists(conn, "files")
        assert _relation_exists(conn, "folders")

        title_count = int(conn.execute("SELECT COUNT(*) FROM titles;").fetchone()[0])
        book_count = int(conn.execute("SELECT COUNT(*) FROM books;").fetchone()[0])
        folder_count = int(conn.execute("SELECT COUNT(*) FROM folders;").fetchone()[0])
        file_count = int(conn.execute("SELECT COUNT(*) FROM files;").fetchone()[0])

        exp_titles = _expected_title_count(db_name)
        exp_books = _expected_book_count(db_name)
        exp_folders, exp_files = _expected_asset_counts(db_name)

        assert title_count == exp_titles
        assert book_count == exp_books
        assert folder_count == exp_folders
        assert file_count == exp_files

        if exp_folders > 0:
            fwl_count = int(conn.execute("SELECT COUNT(*) FROM folder_work_links;").fetchone()[0])
            ffl_count = int(conn.execute("SELECT COUNT(*) FROM file_folder_links;").fetchone()[0])
            assert fwl_count == exp_folders
            assert ffl_count == exp_files
    finally:
        conn.close()


@pytest.mark.db
def test_semantic_asset_profile_partition(provision_test_database) -> None:
    asset_set = set(ASSET_HEAVY_DB_NAMES)
    all_file_counts: dict[str, int] = {}
    all_folder_counts: dict[str, int] = {}

    for db_name in ALL_TEST_DB_NAMES:
        provisioned = provision_test_database(db_name)
        conn = sqlite3.connect(str(provisioned.db_path))
        try:
            all_folder_counts[db_name] = int(conn.execute("SELECT COUNT(*) FROM folders;").fetchone()[0])
            all_file_counts[db_name] = int(conn.execute("SELECT COUNT(*) FROM files;").fetchone()[0])
        finally:
            conn.close()

    for db_name in asset_set:
        assert all_folder_counts[db_name] > 0
        assert all_file_counts[db_name] > 0

    for db_name in (set(ALL_TEST_DB_NAMES) - asset_set):
        assert all_folder_counts[db_name] == 0
        assert all_file_counts[db_name] == 0


@pytest.mark.db
def test_semantic_book_count_bands(provision_test_database) -> None:
    counts: dict[str, int] = {}
    for db_name in ("test_db_2", "test_db_16", "test_db_7", "test_db_8", "test_db_14", "test_db_17", "test_db_4", "test_db_20"):
        provisioned = provision_test_database(db_name)
        conn = sqlite3.connect(str(provisioned.db_path))
        try:
            counts[db_name] = int(conn.execute("SELECT COUNT(*) FROM books;").fetchone()[0])
        finally:
            conn.close()

    assert counts["test_db_2"] == 1
    assert counts["test_db_16"] == 1
    assert counts["test_db_7"] == 6
    assert counts["test_db_8"] == 6
    assert counts["test_db_14"] == 10
    assert counts["test_db_17"] == 10
    assert counts["test_db_20"] > counts["test_db_4"] > counts["test_db_17"]

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
