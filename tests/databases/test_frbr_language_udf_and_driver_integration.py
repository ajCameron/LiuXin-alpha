"""Driver integration tests for language lookup helpers.

These tests cover:
  - The LANGUAGE_ID(<token>) SQL function registration on driver connections.
  - Resolver behaviour when passed a Database instance (not a raw sqlite3 connection).
  - The per-token LRU caching layer.

We intentionally keep these tests independent of external fixture repositories.
"""

from __future__ import annotations

import sqlite3

import pytest


def _fetch_scalar(cursor):
    """Return the first column of the first row from a DB-API/iterable cursor."""

    if hasattr(cursor, "fetchone"):
        row = cursor.fetchone()
        return None if row is None else row[0]

    for row in cursor:
        return row[0]
    return None


@pytest.mark.usefixtures("_seed_random")
def test_language_id_udf_registered_on_driver_connections(driver_spec, tmp_path):
    """Opening a DB via any supported driver should expose LANGUAGE_ID()."""

    from LiuXin_alpha.databases.database import Database
    from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import (
        database_generator as frbr_gen,
    )

    db_path = tmp_path / f"frbr_lang_udf_{driver_spec.id}.test_db"

    # Build a fresh FRBR DB with sqlite3 (generator target)
    conn = sqlite3.connect(str(db_path))
    try:
        frbr_gen.create_new_database(conn)
        eng_id = int(
            conn.execute("SELECT language_id FROM languages WHERE language_code='eng' LIMIT 1;")
            .fetchone()[0]
        )
    finally:
        conn.close()

    db = Database(metadata={"database_path": str(db_path)}, db_type=driver_spec.db_type, create=False, backup=False)
    try:
        c2 = db.driver.get_connection()
        try:
            got = _fetch_scalar(c2.execute("SELECT LANGUAGE_ID('en-GB');"))
            assert got == eng_id

            got2 = _fetch_scalar(c2.execute("SELECT LANGUAGE_ID('eng');"))
            assert got2 == eng_id
        finally:
            try:
                c2.close()
            except Exception:
                pass
    finally:
        try:
            db.close()
        except Exception:
            pass


def test_best_effort_language_id_accepts_database_object(driver_spec, tmp_path):
    """best_effort_language_id() should work when passed a Database instance."""

    from LiuXin_alpha.databases.database import Database
    from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import (
        database_generator as frbr_gen,
    )
    from LiuXin_alpha.utils.language_tools import best_effort_language_id

    db_path = tmp_path / f"frbr_lang_obj_{driver_spec.id}.test_db"

    conn = sqlite3.connect(str(db_path))
    try:
        frbr_gen.create_new_database(conn)
        eng_id = int(
            conn.execute("SELECT language_id FROM languages WHERE language_code='eng' LIMIT 1;")
            .fetchone()[0]
        )
    finally:
        conn.close()

    db = Database(metadata={"database_path": str(db_path)}, db_type=driver_spec.db_type, create=False, backup=False)
    try:
        assert best_effort_language_id(db, "en") == eng_id
        assert best_effort_language_id(db, "en-GB") == eng_id
        assert best_effort_language_id(db, "English") == eng_id
    finally:
        try:
            db.close()
        except Exception:
            pass


def test_per_token_lru_avoids_index_roundtrips(monkeypatch, tmp_path):
    """Repeated lookups for the exact same token should use the per-db LRU."""

    from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import (
        database_generator as frbr_gen,
    )
    import LiuXin_alpha.utils.language_tools.db_language_lookup as lk

    db_path = tmp_path / "frbr_lang_lru.test_db"
    conn = sqlite3.connect(str(db_path))
    try:
        frbr_gen.create_new_database(conn)

        # First call populates caches.
        assert lk.best_effort_language_id(conn, "en", ensure_seeded=False) is not None

        # If the per-token LRU is hit, we should not need to re-enter index logic.
        def boom(*_a, **_k):  # pragma: no cover
            raise AssertionError("index access should not be required for an LRU hit")

        monkeypatch.setattr(lk, "_get_index", boom)
        assert lk.best_effort_language_id(conn, "en", ensure_seeded=False) is not None
    finally:
        conn.close()


def test_language_lookup_safe_on_calibre_like_languages_table(tmp_path):
    """The UDF should be a safe convenience even on non-FRBR DBs."""

    from LiuXin_alpha.utils.language_tools import register_language_id_sql_function

    db_path = tmp_path / "calibre_like_langs.test_db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(
            """
            CREATE TABLE languages (
              id INTEGER PRIMARY KEY,
              lang_code TEXT
            );
            INSERT INTO languages(id, lang_code) VALUES (1, 'eng');
            """.strip()
        )

        # Should not raise.
        register_language_id_sql_function(conn, function_name="LANGUAGE_ID", ensure_seeded=True)

        got = _fetch_scalar(conn.execute("SELECT LANGUAGE_ID('eng');"))
        assert got == 1
    finally:
        conn.close()
