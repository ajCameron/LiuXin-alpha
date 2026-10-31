"""Exercise normalized SQL lookups, index lifecycle and borrowed transactions."""

import sqlite3

import pytest

from LiuXin_alpha.catalog.api.common import IdentifierCandidate
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases.db_types.query_types import TextMatch
from LiuXin_alpha.databases.query_indexes import QUERY_INDEXES, SQLITE_SEARCH_INDEXES
from LiuXin_alpha.databases.search_functions import register_search_functions
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError


def test_fresh_generator_installs_every_query_index():
    from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr.database_generator import (
        create_new_database,
    )

    with sqlite3.connect(":memory:") as conn:
        create_new_database(conn)
        names = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
        }
        assert {
            index.name for index in (*QUERY_INDEXES, *SQLITE_SEARCH_INDEXES)
        } <= names
        conn.execute("INSERT INTO works (work_title) VALUES (?)", ("  Ｃafe\u0301  ",))
        assert conn.execute(
            "SELECT liuxin_text_key_v1(work_title) FROM works"
        ).fetchone() == ("café",)


def test_search_index_migration_is_explicit_atomic_and_checks_definitions(db):
    macros = db.macros
    macros.migrate_query_indexes()
    with macros.transaction() as conn:
        for index in SQLITE_SEARCH_INDEXES:
            conn.execute(f'DROP INDEX "{index.name}"')
    assert macros.audit_query_indexes().missing == tuple(
        i.name for i in SQLITE_SEARCH_INDEXES
    )
    catalog = Catalog(db)
    title = "Before index migration"
    work_id = catalog.works.create({"title": title})
    assert catalog.works.find_by_title(title)[0]["work_id"] == work_id
    assert len(macros.audit_query_indexes().missing) == len(SQLITE_SEARCH_INDEXES)
    with pytest.raises(RuntimeError, match="undo"):
        with macros.transaction():
            assert macros.migrate_query_indexes().clean
            raise RuntimeError("undo")
    assert len(macros.audit_query_indexes().missing) == len(SQLITE_SEARCH_INDEXES)
    index = SQLITE_SEARCH_INDEXES[0]
    with macros.transaction() as conn:
        conn.execute(f'CREATE INDEX "{index.name}" ON works (work_title)')
    with pytest.raises(DatabaseIntegrityError, match="Conflicting query indexes"):
        macros.migrate_query_indexes()
    with macros.transaction() as conn:
        conn.execute(f'DROP INDEX "{index.name}"')
    assert macros.migrate_query_indexes().created == tuple(
        i.name for i in SQLITE_SEARCH_INDEXES
    )
    assert macros.migrate_query_indexes().created == ()


def test_indexed_matching_preserves_unicode_identifier_rules_and_overrides(
    db, monkeypatch
):
    catalog = Catalog(db)
    macros = db.macros
    macros.migrate_query_indexes()
    macros.insert_rows(
        "works", ({"work_title": f"Unrelated title {i}"} for i in range(1000))
    )
    wanted = catalog.works.create({"title": "  Ｃafe\u0301   100%_  "})
    canonical = catalog.works.create(
        {"title": "Different", "canonical_title": "café 100%_"}
    )
    item = catalog.items.create({"inventory_code": "index-test"})
    for table, prefix in (
        ("entity_identifiers", "entity_identifier"),
        ("item_identifiers", "item_identifier"),
    ):
        owner = (
            {} if table == "entity_identifiers" else {"item_identifier_item_id": item}
        )
        macros.insert_rows(
            table,
            (
                {
                    **owner,
                    f"{prefix}_scheme": "barcode",
                    f"{prefix}_value": f"unrelated-{i}",
                }
                for i in range(1000)
            ),
        )
        for scheme, value in (
            ("isbn_13", "978-0-306-40615-7"),
            ("doi", "https://doi.org/10.1000/ABC"),
            ("doi", "broken"),
            ("barcode", "CaseSensitive"),
            ("oclc", "ocm00123"),
            ("uuid", "{12345678-1234-5678-1234-567812345678}"),
        ):
            macros.insert_row(
                table, {**owner, f"{prefix}_scheme": scheme, f"{prefix}_value": value}
            )
    plans = []
    original = macros._macro_connection

    class Connection:
        def execute(self, sql, bindings=()):
            conn = original()
            if sql.startswith("SELECT root."):
                plans.append(
                    " ".join(
                        str(r[3])
                        for r in conn.execute("EXPLAIN QUERY PLAN " + sql, bindings)
                    )
                )
            return conn.execute(sql, bindings)

        def __getattr__(self, name):
            return getattr(original(), name)

    monkeypatch.setattr(macros, "_macro_connection", Connection)

    def no_scan(*args, **kwargs):
        raise AssertionError("Indexed matcher read whole table")

    for repository in (catalog.works, catalog.identifiers, catalog.item_identifiers):
        monkeypatch.setattr(repository, "_all_rows", no_scan)
        monkeypatch.setattr(repository, "_scan_rows", no_scan)
    assert [r["work_id"] for r in catalog.works.find_by_title("café 100%_")] == [
        wanted,
        canonical,
    ]
    assert catalog.works.find_by_title("café 100%_", limit=1)[0]["work_id"] == wanted
    for scheme, value in (
        ("isbn", "9780306406157"),
        ("DOI", "doi:10.1000/abc"),
        ("oclc", "00123"),
        ("uuid", "12345678-1234-5678-1234-567812345678"),
    ):
        assert catalog.identifiers.find(identifier_type=scheme, value=value)
        candidate = IdentifierCandidate(scheme, value)
        assert catalog.identifiers.match(candidate).is_match
        assert catalog.item_identifiers.match(candidate, item_id=item).is_match
        assert not catalog.item_identifiers.match(candidate, item_id=str(item)).is_match
    assert (
        catalog.identifiers.find(identifier_type="barcode", value="casesensitive")
        is None
    )
    assert catalog.identifiers.find(identifier_type="barcode", value="CaseSensitive")
    override = IdentifierCandidate(
        "barcode", "different-original", normalised_value="CaseSensitive"
    )
    assert catalog.identifiers.match(override).is_match
    assert catalog.item_identifiers.match(override).is_match
    assert plans and all("SEARCH" in p and "_key_v1" in p for p in plans)
    assert (
        "idx_works_title_key_v1" in plans[0]
        and "idx_works_canonical_title_key_v1" in plans[0]
    )


def test_predicate_validation_and_current_transaction(db):
    macros = db.macros
    catalog = Catalog(db)
    with pytest.raises(RuntimeError, match="undo"):
        with macros.transaction():
            work = catalog.works.create({"title": "Pending Unicode Ａ"})
            assert (
                catalog.search.find('work.title:"=pending unicode a"')[0]["work_id"]
                == work
            )
            catalog.works.update(work, {"title": "Pending Unicode Ｂ"})
            assert not catalog.works.find_by_title("Pending Unicode A")
            assert (
                catalog.works.find_by_title("Pending Unicode B")[0]["work_id"] == work
            )
            raise RuntimeError("undo")
    assert not catalog.works.find_by_title("Pending Unicode B")
    with pytest.raises(InputIntegrityError):
        macros.find_rows_matching(
            "works; DROP TABLE works", TextMatch(("work_title",), "x")
        )
    with pytest.raises(InputIntegrityError):
        macros.find_rows_matching("works", TextMatch(("not_a_column",), "x"))
    with pytest.raises(InputIntegrityError):
        macros.find_rows_matching("works", TextMatch(("work_title",), "x"), offset=True)


def test_agent_resolution_uses_the_normalized_name_index(db, monkeypatch):
    catalog = Catalog(db)
    db.macros.migrate_query_indexes()
    agent = catalog.agents.create({"name": "  Straße  "})

    def no_scan():
        raise AssertionError("Agent resolution scanned candidate payloads")

    monkeypatch.setattr(catalog.agents, "_scan_rows", no_scan)
    assert catalog.agents.resolve(name="STRASSE")["agent_id"] == agent
    assert catalog.agents.resolve(name="Other name") is None
    plan = db.macros._macro_connection().execute(
        "EXPLAIN QUERY PLAN SELECT * FROM agents "
        "WHERE liuxin_text_key_v1(agent_canonical_name)=? ORDER BY agent_id",
        ("strasse",),
    )
    assert "idx_agents_name_key_v1" in " ".join(str(row[3]) for row in plan)


def test_shared_functions_on_stdlib_and_apsw_connections():
    import apsw

    for conn in (sqlite3.connect(":memory:"), apsw.Connection(":memory:")):
        try:
            register_search_functions(conn)
            assert next(
                iter(
                    conn.execute(
                        "SELECT liuxin_text_key_v1(?), liuxin_identifier_key_v1(?, ?)",
                        ("Straße", "ISBN", "978-0-306-40615-7"),
                    )
                )
            ) == ("strasse", '["isbn13","9780306406157"]')
        finally:
            conn.close()


def test_recovery_and_replacement_verify_indexed_databases(db, tmp_path):
    from pathlib import Path

    from LiuXin_alpha.databases.creation import replace_sqlite_file
    from LiuXin_alpha.surfaces.terminal.commands.db import _run_recovery_pragmas

    db.macros.migrate_query_indexes()
    Catalog(db).works.create({"title": "Indexed recovery Ａ"})
    path = Path(db.driver.database_path)
    db.driver.close()
    assert (
        _run_recovery_pragmas(path, run_integrity_check=True)["integrity_check"] == "ok"
    )

    def build(candidate):
        with sqlite3.connect(candidate) as conn:
            conn.execute("CREATE TABLE replacement (id INTEGER PRIMARY KEY)")

    backup = tmp_path / "indexed.backup"
    replace_sqlite_file(path, backup_path=backup, build=build)
    with sqlite3.connect(backup) as conn:
        register_search_functions(conn)
        assert conn.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert conn.execute(
            "SELECT work_title FROM works WHERE liuxin_text_key_v1(work_title)=?",
            ("indexed recovery a",),
        ).fetchone() == ("Indexed recovery Ａ",)
