"""
Check language alias resolution, index reuse and repair of an empty locked language table.

Each test creates and closes its own SQLite database; cache tests temporarily
replace internal lookup functions through pytest.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_frbr_language_id_lookup.py
"""

from __future__ import annotations

import pathlib
import sqlite3

import pytest


def test_best_effort_language_id_matches_common_tokens(tmp_path):
    """
    Resolve English and French codes, region tags and the French language name to seeded IDs.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_language_id_lookup.py::test_best_effort_language_id_matches_common_tokens


    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import (
        database_generator as frbr_gen,
    )
    from LiuXin_alpha.utils.language_tools import best_effort_language_id

    db_path = tmp_path / "frbr_lang_lookup.test_db"
    conn = sqlite3.connect(str(db_path))
    try:
        frbr_gen.create_new_database(conn)

        eng_id = int(
            conn.execute("SELECT language_id FROM languages WHERE language_code='eng' LIMIT 1;").fetchone()[0]
        )
        fra_id = int(
            conn.execute(
                "SELECT language_id FROM languages WHERE language_code IN ('fre','fra') ORDER BY language_code LIMIT 1;"
            ).fetchone()[0]
        )

        assert best_effort_language_id(conn, "eng") == eng_id
        assert best_effort_language_id(conn, "en") == eng_id
        assert best_effort_language_id(conn, "en-GB") == eng_id
        assert best_effort_language_id(conn, "EN_gb") == eng_id

        assert best_effort_language_id(conn, "fre") == fra_id
        assert best_effort_language_id(conn, "fra") == fra_id
        assert best_effort_language_id(conn, "fr-FR") == fra_id

        # Best-effort name matching.
        assert best_effort_language_id(conn, "French") == fra_id
    finally:
        conn.close()


def test_lookup_uses_cached_index(monkeypatch, tmp_path):
    """
    Resolve an English region tag after replacing index construction with a failing sentinel.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_language_id_lookup.py::test_lookup_uses_cached_index


    :param monkeypatch: Pytest patch fixture; restores replaced generator or lookup
        attributes after the test.
    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import (
        database_generator as frbr_gen,
    )
    import LiuXin_alpha.utils.language_tools.db_language_lookup as lk

    db_path = tmp_path / "frbr_lang_lookup_cache.test_db"
    conn = sqlite3.connect(str(db_path))
    try:
        frbr_gen.create_new_database(conn)

        # First call builds and caches the index.
        assert lk.best_effort_language_id(conn, "en") is not None

        # If caching works, subsequent lookups should not rebuild the index.
        def boom(_conn):  # pragma: no cover
            """
            Fail immediately if a supposedly cached lookup tries to rebuild its index.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/test_frbr_language_id_lookup.py::test_lookup_uses_cached_index


            :param _conn: Ignored connection passed by the replaced index builder.
            :return: Never returns; always raises AssertionError.
            """
            raise AssertionError("Index rebuild should not be needed")

        monkeypatch.setattr(lk, "_build_index", boom)
        assert lk.best_effort_language_id(conn, "en-GB") is not None
    finally:
        conn.close()


def test_ensure_seeded_repairs_locked_empty_languages_table(tmp_path):
    """
    Repair an empty language table protected by write-blocking triggers.

    Requires a True repair result, at least 100 rows and insert/update/delete
    lock-trigger names. Trigger existence is inspected rather than probing all three
    write operations.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_language_id_lookup.py::test_ensure_seeded_repairs_locked_empty_languages_table


    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :return: None; failed expectations raise AssertionError.
    """

    from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import (
        database_generator as frbr_gen,
    )
    from LiuXin_alpha.utils.language_tools import ensure_languages_seeded_and_locked

    db_path = tmp_path / "frbr_lang_lookup_repair.test_db"
    conn = sqlite3.connect(str(db_path))
    try:
        # Create only the languages table (unseeded), then (incorrectly) lock it.
        sql_path = (
            pathlib.Path(frbr_gen.__file__).resolve().parent
            / "table_sql"
            / "constants_tables"
            / "languages.sql"
        )
        raw = sql_path.read_text(encoding="utf-8")
        # Strip comment lines and BREAK markers.
        cleaned = "\n".join(
            line for line in raw.splitlines() if not line.strip().startswith("--") and "BREAK" not in line
        )
        conn.executescript(cleaned)

        # Add lock triggers without seeding (simulates a botched migration).
        for action in ("INSERT", "UPDATE", "DELETE"):
            conn.executescript(
                f"""
                CREATE TRIGGER IF NOT EXISTS block_{action.lower()}_on_languages
                BEFORE {action} ON languages
                BEGIN
                    SELECT RAISE(ABORT, 'languages is read-only');
                END;
                """
            )

        assert int(conn.execute("SELECT COUNT(*) FROM languages;").fetchone()[0]) == 0

        # Repair should seed + restore the lock.
        assert ensure_languages_seeded_and_locked(conn) is True

        assert int(conn.execute("SELECT COUNT(*) FROM languages;").fetchone()[0]) >= 100
        triggers = {
            str(r[0])
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='languages';"
            ).fetchall()
        }
        assert any(t.startswith("block_insert_on_languages") for t in triggers)
        assert any(t.startswith("block_update_on_languages") for t in triggers)
        assert any(t.startswith("block_delete_on_languages") for t in triggers)
    finally:
        conn.close()
