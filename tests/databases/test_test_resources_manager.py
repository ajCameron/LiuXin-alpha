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


def test_provisioned_copies_are_independent(tmp_path, test_resources_manager) -> None:
    db1 = test_resources_manager.provision_test_database(name="test_db_0", dst_dir=tmp_path / "a")
    db2 = test_resources_manager.provision_test_database(name="test_db_0", dst_dir=tmp_path / "b")

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
