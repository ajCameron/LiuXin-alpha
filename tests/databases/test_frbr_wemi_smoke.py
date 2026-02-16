# tests/databases/database_driver_plugins/sqlite_database_driver/test_sqlite_database_driver_generator_frbr_smoke.py

"""Smoke tests for the FRBR database generator.

These tests are intentionally small and are meant to fail loudly if the
generator cannot run end-to-end.
"""

from __future__ import annotations

import pathlib
import sqlite3

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as frbr_gen
from LiuXin_alpha.databases.database_driver_plugins.SQL.utility_mixins import ColumnNameMixin


def _frbr_pkg_root() -> pathlib.Path:
    return pathlib.Path(frbr_gen.__file__).resolve().parent


def test_frbr_generator_resources_are_present() -> None:
    """Sanity-check that the FRBR generator resources are packaged and non-empty."""
    pkg_root = _frbr_pkg_root()

    # TOML-first generator inputs.
    for rel in ["interlink_table_requests.toml", "intralink_table_requests.toml", "aggregate_tables.toml"]:
        p = pkg_root / rel
        assert p.is_file(), f"Missing FRBR generator spec file: {p}"
        assert p.read_text(encoding="utf-8", errors="replace").strip(), f"Empty FRBR generator spec file: {p}"

    # Main-table & trigger DDL still lives in folders (until TOML fully replaces legacy SQL bundles).
    for folder in ["table_sql", "trigger_sql"]:
        root = pkg_root / folder
        assert root.is_dir(), f"Expected {folder}/ under {pkg_root}"
        sql_files = sorted(root.rglob("*.sql"))
        assert sql_files, f"No .sql files found under {root}"

        # Guard against accidentally packaging empty placeholder files.
        for path in sql_files[:20]:
            text = path.read_text(encoding="utf-8", errors="replace")
            assert text.strip(), f"SQL file is empty: {path}"


def test_frbr_generator_create_new_database_smoke(tmp_path: pathlib.Path) -> None:
    """Run the generator end-to-end and assert that core tables exist afterwards."""
    db_path = tmp_path / "frbr_smoke.db"
    conn = sqlite3.connect(str(db_path))
    try:
        # Make FK issues surface immediately during generation.
        conn.execute("PRAGMA foreign_keys = ON;")
        try:
            frbr_gen.create_new_database(conn)
        except Exception as e:  # pragma: no cover
            raise AssertionError("FRBR generator did not run to completion") from e

        # Basic existence checks for core WEMI tables.
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table';")}

        expected_main = {"agents", "works", "expressions", "manifestations", "items"}
        missing_main = sorted(expected_main - tables)
        assert not missing_main, f"Missing expected main tables: {missing_main}. Present: {sorted(tables)[:50]}"

        # Basic existence check for at least one canonical interlink table.
        agent_work_link, _ = ColumnNameMixin.get_interlink_table_name("agents", "works")
        agent_expr_link, _ = ColumnNameMixin.get_interlink_table_name("agents", "expressions")
        assert (
            agent_work_link in tables or agent_expr_link in tables
        ), f"Expected interlink table missing ({agent_work_link!r} or {agent_expr_link!r}). Present: {sorted(tables)[:80]}"

    finally:
        conn.close()
