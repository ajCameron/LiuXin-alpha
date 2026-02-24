"""FRBR generator: languages constant table.

The FRBR-first schema treats `languages` as a seeded, locked reference table.
This makes language identifiers stable across imports and link-table usage.
"""

from __future__ import annotations

import sqlite3

import pytest


def test_languages_table_seeded_and_locked(tmp_path):
    from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import (
        database_generator as frbr_gen,
    )

    db_path = tmp_path / "frbr_langs.test_db"
    conn = sqlite3.connect(str(db_path))
    try:
        frbr_gen.create_new_database(conn)

        cols = [str(r[1]) for r in conn.execute("PRAGMA table_info(languages);").fetchall()]

        for required in (
            "language_id",
            "language",
            "language_code",
            "language_iso639_1",
            "language_iso639_2_b",
            "language_iso639_2_t",
            "language_bcp47_primary",
            "language_bcp47_variants",
        ):
            assert required in cols

        # We expect a reasonably complete ISO-639-2 corpus (hundreds of rows).
        count = int(conn.execute("SELECT COUNT(*) FROM languages;").fetchone()[0])
        assert count >= 100

        # Spot-check a couple of high-value languages.
        eng = conn.execute(
            """
            SELECT language_iso639_1, language_iso639_2_b, language_iso639_2_t, language_bcp47_primary, language_bcp47_variants
            FROM languages
            WHERE language_code = 'eng'
            LIMIT 1;
            """.strip()
        ).fetchone()
        assert eng is not None
        assert eng[0] == "en"
        assert eng[1] == "eng"
        assert eng[3] == "en"
        assert eng[4] is None or "en-GB" in str(eng[4]).split()

        fra = conn.execute(
            """
            SELECT language_iso639_1, language_iso639_2_b, language_iso639_2_t, language_bcp47_primary
            FROM languages
            WHERE language_code IN ('fre','fra')
            ORDER BY language_code
            LIMIT 1;
            """.strip()
        ).fetchone()
        assert fra is not None
        assert fra[0] == "fr"
        assert fra[3] == "fr"

        # Locked: writes should fail.
        with pytest.raises(sqlite3.DatabaseError):
            conn.execute(
                "INSERT INTO languages (language_code, language_iso639_2_b, language_bcp47_primary) VALUES ('zzz','zzz','zzz');"
            )

        # The lock is implemented as triggers.
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
