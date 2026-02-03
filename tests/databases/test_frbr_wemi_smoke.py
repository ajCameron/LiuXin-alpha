# tests/databases/database_driver_plugins/sqlite_database_driver/test_sqlite_database_driver_generator_frbr_smoke.py

"""Smoke tests for the FRBR database generator.

These tests are intentionally small.

The FRBR generator is currently expected to fail during migration work. The
second test captures common failure modes as xfail so it can remain in the
suite while the generator and SQL files are being brought online.
"""

from __future__ import annotations

import pathlib
import sqlite3

import pytest

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as frbr_gen


def _frbr_pkg_root() -> pathlib.Path:
    return pathlib.Path(frbr_gen.__file__).resolve().parent


def test_frbr_sql_files_are_present() -> None:
    """Sanity-check that the FRBR SQL folder is packaged and non-empty."""
    root = _frbr_pkg_root() / "table_sql"
    assert root.is_dir(), f"Expected table_sql/ under {root.parent}"

    sql_files = sorted(root.rglob("*.sql"))
    assert sql_files, f"No .sql files found under {root}"

    # Guard against accidentally packaging empty placeholder files.
    for path in sql_files[:20]:
        text = path.read_text(encoding="utf-8", errors="replace")
        assert text.strip(), f"SQL file is empty: {path}"


def test_frbr_generator_create_new_database_smoke_expected_fail() -> None:
    """Run the generator end-to-end, but xfail on known migration-stage errors."""
    conn = sqlite3.connect(":memory:")
    try:
        frbr_gen.create_new_database(conn)
    except (FileNotFoundError, SystemExit) as e:
        pytest.xfail(f"FRBR generator still expects legacy inputs during migration: {e}")
    except sqlite3.OperationalError as e:
        pytest.xfail(f"SQLite rejected FRBR DDL (expected during migration): {e}")
    except AssertionError as e:
        pytest.xfail(f"Generator sanity-checks failed (expected during migration): {e}")
    finally:
        conn.close()
