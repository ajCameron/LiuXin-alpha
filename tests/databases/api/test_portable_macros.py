from __future__ import annotations

from dataclasses import replace
import sqlite3
import threading

import pytest

from LiuXin_alpha.databases.column_metadata import infer_column_metadata
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.databasedriver import (
    PostgresDatabaseMacros,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.macros import (
    SQLiteDatabaseMacros,
)
from LiuXin_alpha.databases.macro_types import LinkValue, UnreferencedRowsSpec
from LiuXin_alpha.databases.schema_specs import (
    LinkCardinality,
    StorageColumnSpec,
    StorageLinkSpec,
)
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError


def _pynocase(left, right) -> int:
    left = str(left).casefold()
    right = str(right).casefold()
    return (left > right) - (left < right)


class _Driver:
    def __init__(self, conn: sqlite3.Connection, *, postgres_shaped: bool) -> None:
        self.conn = conn
        self.invalidations = 0
        if postgres_shaped:
            self.schema = "main"

    def _table_sql(self, table: str) -> str:
        return f'"main"."{table}"'

    def _zero_prop_cache(self) -> None:
        self.invalidations += 1


class _Wrapper:
    def __init__(self, driver: _Driver) -> None:
        self.driver = driver

    def execute(self, sql, values=None):
        return self.driver.conn.execute(sql, values or ())

    def executemany(self, sql, values=None):
        return self.driver.conn.executemany(sql, values or ())

    def get_column_headings(self, table: str) -> list[str]:
        return [row[1] for row in self.driver.conn.execute(f'PRAGMA table_info("{table}")')]

    def get_tables(self) -> tuple[str, ...]:
        return tuple(
            row[0]
            for row in self.driver.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        )

    def get_id_column(self, table: str) -> str:
        info = list(self.driver.conn.execute(f'PRAGMA table_info("{table}")'))
        primary = [row[1] for row in info if row[5]]
        if primary:
            return primary[0]
        candidates = [row[1] for row in info if row[1].endswith("_id")]
        if not candidates:
            raise InputIntegrityError(f"No id column for {table!r}")
        return candidates[0]

    def get_column_base(self, table: str) -> str:
        return table[:-1] if table.endswith("s") else table

    def get_record_count(self, table: str) -> int:
        return int(self.driver.conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])

    def get_allowed_link_types(
        self,
        link_spec: StorageLinkSpec,
    ) -> tuple[str, ...] | None:
        if link_spec.allowed_types_table is None:
            return None
        return tuple(
            row[0]
            for row in self.driver.conn.execute(
                f'SELECT type FROM "{link_spec.allowed_types_table}" '
                "ORDER BY type"
            )
        )

    def get_column_metadata(self, table: str, column: str):
        declaration = next(
            (
                row[2]
                for row in self.driver.conn.execute(f'PRAGMA table_info("{table}")')
                if row[1] == column
            ),
            None,
        )
        return infer_column_metadata(table, column, declaration)


class _DB:
    def __init__(self, *, postgres_shaped: bool) -> None:
        conn = sqlite3.connect(":memory:")
        conn.create_collation("PYNOCASE", _pynocase)
        conn.execute("PRAGMA foreign_keys=ON")
        self.driver = _Driver(conn, postgres_shaped=postgres_shaped)
        self.driver_wrapper = _Wrapper(self.driver)
        self.lock = threading.RLock()


def _column(name: str, ordinal: int, *, primary: bool = False) -> StorageColumnSpec:
    return StorageColumnSpec(
        name=name,
        ordinal=ordinal,
        declared_type="INTEGER" if name.endswith("_id") else "TEXT",
        is_primary_key=primary,
    )


def _strict_link_spec() -> StorageLinkSpec:
    return StorageLinkSpec(
        primary_table="left_rows",
        secondary_table="right_rows",
        link_table="strict_links",
        primary_id_col="left_id",
        secondary_id_col="right_id",
        primary_link_col="left_id",
        secondary_link_col="right_id",
        priority_link_col="priority",
        type_link_col="link_type",
        ordered=True,
        typed=True,
        extra_link_columns=(
            _column("strict_link_id", 0, primary=True),
            _column("note", 5),
        ),
    )


def _role_link_spec() -> StorageLinkSpec:
    return replace(
        _strict_link_spec(),
        link_table="role_links",
        type_part_of_identity=True,
        extra_link_columns=(_column("role_link_id", 0, primary=True),),
    )


def _owned_link_spec() -> StorageLinkSpec:
    return StorageLinkSpec(
        primary_table="owned_sources",
        secondary_table="owned_values",
        link_table="owned_links",
        cardinality=LinkCardinality.ONE_TO_ONE,
        primary_id_col="owned_source_id",
        secondary_id_col="owned_value_id",
        primary_link_col="owned_source_id",
        secondary_link_col="owned_value_id",
    )


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE left_rows (
            left_id INTEGER PRIMARY KEY,
            name TEXT
        );
        CREATE TABLE right_rows (
            right_id INTEGER PRIMARY KEY,
            name TEXT
        );
        CREATE TABLE strict_links (
            strict_link_id INTEGER PRIMARY KEY,
            left_id INTEGER NOT NULL REFERENCES left_rows(left_id) ON DELETE CASCADE,
            right_id INTEGER NOT NULL REFERENCES right_rows(right_id) ON DELETE CASCADE,
            link_type TEXT,
            priority INTEGER NOT NULL,
            note TEXT,
            UNIQUE(left_id, right_id),
            UNIQUE(left_id, priority)
        );
        CREATE TABLE role_links (
            role_link_id INTEGER PRIMARY KEY,
            left_id INTEGER NOT NULL REFERENCES left_rows(left_id) ON DELETE CASCADE,
            right_id INTEGER NOT NULL REFERENCES right_rows(right_id) ON DELETE CASCADE,
            link_type TEXT,
            priority INTEGER NOT NULL,
            UNIQUE(left_id, right_id, link_type),
            UNIQUE(left_id, link_type, priority)
        );
        CREATE TABLE tags (
            tag_id INTEGER PRIMARY KEY,
            tag TEXT,
            tag_phash TEXT UNIQUE
        );
        CREATE TABLE works (
            work_id INTEGER PRIMARY KEY,
            work_title TEXT
        );
        CREATE TABLE genres (
            genre_id INTEGER PRIMARY KEY,
            genre TEXT,
            genre_phash TEXT,
            genre_parent_id INTEGER REFERENCES genres(genre_id)
        );
        CREATE UNIQUE INDEX genres_root_identity
          ON genres(genre_phash)
          WHERE genre_parent_id IS NULL AND genre_phash IS NOT NULL;
        CREATE UNIQUE INDEX genres_scoped_identity
          ON genres(genre_parent_id, genre_phash)
          WHERE genre_parent_id IS NOT NULL AND genre_phash IS NOT NULL;
        CREATE TABLE owned_sources (
            owned_source_id INTEGER PRIMARY KEY
        );
        CREATE TABLE owned_values (
            owned_value_id INTEGER PRIMARY KEY,
            owned_value TEXT NOT NULL
        );
        CREATE TABLE owned_links (
            owned_source_id INTEGER NOT NULL UNIQUE
                REFERENCES owned_sources(owned_source_id),
            owned_value_id INTEGER NOT NULL UNIQUE
                REFERENCES owned_values(owned_value_id),
            UNIQUE(owned_source_id, owned_value_id)
        );
        CREATE TABLE demo_links (
            demo_link_id INTEGER PRIMARY KEY,
            demo_link_left_id INTEGER,
            demo_link_right_id INTEGER,
            demo_link_priority INTEGER,
            UNIQUE(demo_link_left_id, demo_link_right_id),
            UNIQUE(demo_link_left_id, demo_link_priority)
        );
        CREATE TABLE database_version (
            database_version_id INTEGER PRIMARY KEY,
            database_version_version TEXT
        );
        CREATE TABLE library_id (
            library_id_id INTEGER PRIMARY KEY,
            library_id_uuid TEXT
        );
        INSERT INTO left_rows(left_id, name) VALUES (1, 'left one'), (2, 'left two'), (3, 'protected');
        INSERT INTO right_rows(right_id, name) VALUES (10, 'ten'), (11, 'eleven'), (12, 'twelve');
        INSERT INTO owned_sources(owned_source_id) VALUES (1), (2);
        INSERT INTO database_version(database_version_id, database_version_version) VALUES (1, 'old');
        """
    )


@pytest.fixture(params=("sqlite", "postgres"), ids=("sqlite", "postgres-shaped"))
def macro_db(request):
    postgres_shaped = request.param == "postgres"
    db = _DB(postgres_shaped=postgres_shaped)
    _create_schema(db.driver.conn)
    macros = (
        PostgresDatabaseMacros(db)
        if postgres_shaped
        else SQLiteDatabaseMacros(db)
    )
    try:
        yield macros, db
    finally:
        db.driver.conn.close()


def test_link_upsert_bulk_read_and_atomic_replace(macro_db):
    macros, _db = macro_db
    spec = _strict_link_spec()

    created = macros.upsert_link(
        spec,
        1,
        LinkValue(secondary_id=10, link_type="author", priority=1, extra={"note": "keep"}),
    )
    assert created.secondary_id == 10
    assert created.extra["note"] == "keep"

    updated = macros.upsert_link(
        spec,
        1,
        LinkValue(secondary_id=10, link_type="editor", priority=2, extra={"note": "changed"}),
    )
    assert updated.link_type == "editor"
    assert updated.priority == 2

    macros.upsert_links(
        spec,
        1,
        (LinkValue(secondary_id=11, link_type="author", priority=1),),
    )
    replaced = macros.replace_links(
        spec,
        1,
        (
            LinkValue(secondary_id=11, link_type="author"),
            LinkValue(secondary_id=10, link_type="editor"),
        ),
    )
    assert [row.secondary_id for row in replaced] == [11, 10]
    assert [row.priority for row in replaced] == [2, 1]
    assert next(row for row in replaced if row.secondary_id == 10).extra["note"] == "changed"

    grouped = macros.get_link_rows_bulk(spec, (1, 2))
    assert [row.secondary_id for row in grouped[1]] == [11, 10]
    assert grouped[2] == ()

    bulk = macros.replace_links_bulk(
        spec,
        {
            1: (LinkValue(secondary_id=12, link_type="author"),),
            2: (LinkValue(secondary_id=10, link_type="author"),),
        },
    )
    assert [row.secondary_id for row in bulk[1]] == [12]
    assert [row.secondary_id for row in bulk[2]] == [10]

    before = macros.get_link_rows_bulk(spec, (1, 2))
    with pytest.raises(sqlite3.IntegrityError):
        macros.replace_links_bulk(
            spec,
            {
                1: (LinkValue(10, link_type="author"),),
                2: (LinkValue(999, link_type="author"),),
            },
        )
    assert macros.get_link_rows_bulk(spec, (1, 2)) == before


def test_link_writes_enforce_static_and_live_allowed_types(macro_db):
    macros, db = macro_db
    db.driver.conn.executescript(
        """
        CREATE TABLE strict_links__types (type TEXT PRIMARY KEY);
        INSERT INTO strict_links__types VALUES ('author');
        """
    )
    spec = replace(
        _strict_link_spec(),
        allowed_types=("author", "reviewer"),
        allowed_types_table="strict_links__types",
    )

    with pytest.raises(InputIntegrityError, match="does not exist"):
        macros.upsert_link(
            spec,
            1,
            LinkValue(10, link_type="reviewer", priority=1),
        )
    with pytest.raises(InputIntegrityError, match="not allowed by the link spec"):
        macros.upsert_link(
            spec,
            1,
            LinkValue(10, link_type="editor", priority=1),
        )
    assert db.driver.conn.execute(
        "SELECT COUNT(*) FROM strict_links"
    ).fetchone()[0] == 0

    db.driver.conn.execute(
        "INSERT INTO strict_links__types VALUES ('reviewer')"
    )
    written = macros.upsert_link(
        spec,
        1,
        LinkValue(10, link_type="reviewer", priority=1),
    )
    null_type = macros.upsert_link(
        spec,
        1,
        LinkValue(11, link_type=None, priority=2),
    )

    assert written.link_type == "reviewer"
    assert null_type.link_type is None


def test_owned_one_to_one_values_update_create_unlink_and_rollback(macro_db):
    macros, db = macro_db
    spec = _owned_link_spec()

    with pytest.raises(InputIntegrityError, match="destination id"):
        macros.replace_owned_one_to_one_values_bulk(
            spec,
            "owned_value_id",
            {1: "invalid"},
        )

    created = macros.replace_owned_one_to_one_values_bulk(
        spec,
        "owned_value",
        {1: "first", 2: "second"},
    )
    first_id = created[1][0].secondary_id
    second_id = created[2][0].secondary_id

    changed = macros.replace_owned_one_to_one_values_bulk(
        spec,
        "owned_value",
        {1: "changed", 2: None},
    )

    assert changed[1][0].secondary_id == first_id
    assert changed[2] == ()
    assert db.driver.conn.execute(
        "SELECT owned_value FROM owned_values WHERE owned_value_id=?",
        (first_id,),
    ).fetchone()[0] == "changed"
    assert db.driver.conn.execute(
        "SELECT owned_value FROM owned_values WHERE owned_value_id=?",
        (second_id,),
    ).fetchone()[0] == "second"
    assert db.driver_wrapper.get_record_count("owned_values") == 2

    with pytest.raises(sqlite3.IntegrityError):
        macros.replace_owned_one_to_one_values_bulk(
            spec,
            "owned_value",
            {1: "rolled back", 999: "invalid source"},
        )
    assert db.driver.conn.execute(
        "SELECT owned_value FROM owned_values WHERE owned_value_id=?",
        (first_id,),
    ).fetchone()[0] == "changed"
    assert db.driver_wrapper.get_record_count("owned_values") == 2


def test_typed_link_replacement_can_be_scoped(macro_db):
    macros, _db = macro_db
    spec = _role_link_spec()
    macros.upsert_links(
        spec,
        1,
        (
            LinkValue(10, link_type="author", priority=1),
            LinkValue(10, link_type="editor", priority=1),
        ),
    )

    author_rows = macros.replace_links(
        spec,
        1,
        (LinkValue(11),),
        link_type="author",
    )
    assert [(row.secondary_id, row.link_type) for row in author_rows] == [(11, "author")]
    all_rows = macros.get_link_rows(spec, 1)
    assert {(row.secondary_id, row.link_type) for row in all_rows} == {
        (10, "editor"),
        (11, "author"),
    }

    with pytest.raises(InputIntegrityError, match="part of the link identity"):
        macros.replace_links(
            _strict_link_spec(),
            1,
            (LinkValue(12),),
            link_type="author",
        )


def test_policy_aware_ensure_uses_comparison_column_and_preserves_display_text(macro_db):
    macros, db = macro_db
    first = macros.ensure_table_value("tags", "tag", "Science Fiction")
    second = macros.ensure_table_value("tags", "tag", " sciencefiction ")
    assert second == first
    assert db.driver.conn.execute(
        "SELECT tag, tag_phash FROM tags WHERE tag_id=?", (first,)
    ).fetchone() == ("Science Fiction", "sciencefiction")

    ensured = macros.ensure_table_values("tags", "tag", ("Fantasy", "FANTASY"))
    assert ensured["Fantasy"] == ensured["FANTASY"]
    with pytest.raises(InputIntegrityError):
        macros.ensure_table_value("tags", "tag", "   ")

    work_id = macros.ensure_table_value("works", "work_title", "Example Title")
    assert macros.ensure_table_value(
        "works",
        "work_title",
        "  example title  ",
    ) == work_id
    assert db.driver.conn.execute(
        "SELECT work_title FROM works WHERE work_id=?",
        (work_id,),
    ).fetchone()[0] == "Example Title"
    unicode_work_id = macros.ensure_table_value(
        "works",
        "work_title",
        "Die Straße",
    )
    assert macros.ensure_table_value(
        "works",
        "work_title",
        "DIE STRASSE",
    ) == unicode_work_id

    assert macros.derive_identity_value(
        "tags",
        "tag",
        " SCIENCE FICTION ",
    ) == "sciencefiction"
    identity = macros.get_canonical_identity_by_key(
        "tags",
        "tag",
        "sciencefiction",
    )
    assert identity is not None
    assert identity.row_id == first
    assert identity.canonical_value == "Science Fiction"
    assert macros.get_canonical_value(
        "tags",
        "tag",
        "  SCIENCE FICTION ",
    ) == "Science Fiction"


def test_policy_aware_find_uses_ensure_matching_without_creating_rows(macro_db):
    macros, db = macro_db
    tag_id = macros.ensure_table_value("tags", "tag", "Science Fiction")
    before = db.driver_wrapper.get_record_count("tags")

    assert macros.find_table_value(
        "tags",
        "tag",
        " sciencefiction ",
    ) == tag_id
    assert macros.find_table_value("tags", "tag", "Missing") is None
    assert db.driver_wrapper.get_record_count("tags") == before


def test_scoped_identity_lookup_and_ensure(macro_db):
    macros, db = macro_db
    db.driver.conn.executemany(
        "INSERT INTO genres(genre_id, genre, genre_phash) VALUES (?, ?, ?)",
        ((1, "Fiction", "fiction"), (2, "Media", "media")),
    )

    first = macros.ensure_table_value(
        "genres",
        "genre",
        "Science Fiction",
        additional_values={"genre_parent_id": 1},
    )
    assert macros.ensure_table_value(
        "genres",
        "genre",
        "science fiction",
        additional_values={"genre_parent_id": 1},
    ) == first
    second = macros.ensure_table_value(
        "genres",
        "genre",
        "Science Fiction",
        additional_values={"genre_parent_id": 2},
    )
    assert second != first
    assert macros.find_table_value(
        "genres",
        "genre",
        " SCIENCE FICTION ",
        additional_values={"genre_parent_id": 1},
    ) == first
    assert macros.find_table_value(
        "genres",
        "genre",
        "Missing",
        additional_values={"genre_parent_id": 1},
    ) is None
    with pytest.raises(InputIntegrityError, match="scope"):
        macros.find_table_value(
            "genres",
            "genre",
            "Science Fiction",
        )

    key = macros.derive_identity_value("genres", "genre", "SCIENCE FICTION")
    assert macros.get_canonical_value_by_identity(
        "genres",
        "genre",
        key,
        scope_values={"genre_parent_id": 1},
    ) == "Science Fiction"
    with pytest.raises(InputIntegrityError, match="scope"):
        macros.get_canonical_value_by_identity(
            "genres",
            "genre",
            key,
        )


def test_normalized_identity_migration_reports_collisions_then_backfills():
    db = _DB(postgres_shaped=False)
    _create_schema(db.driver.conn)
    macros = SQLiteDatabaseMacros(db)
    db.driver.conn.execute(
        """
        CREATE TABLE custom_columns (
            custom_column_id INTEGER PRIMARY KEY,
            custom_column_label TEXT,
            custom_column_name TEXT
        )
        """
    )
    db.driver.conn.execute(
        "INSERT INTO custom_columns(custom_column_label, custom_column_name) "
        "VALUES ('Reading State', 'reading_state')"
    )
    db.driver.conn.executemany(
        "INSERT INTO tags(tag) VALUES (?)",
        (("Duplicate Tag",), (" duplicate tag ",)),
    )
    db.driver.conn.commit()

    audit = macros.audit_normalized_identities()
    assert audit.rows_needing_update == 4
    assert len(audit.collisions) == 1
    with pytest.raises(DatabaseIntegrityError, match="collisions"):
        macros.migrate_normalized_identities()
    assert "normalized_identities" not in db.driver_wrapper.get_tables()

    db.driver.conn.execute("DELETE FROM tags WHERE tag_id = 2")
    report = macros.migrate_normalized_identities()
    assert report.clean
    assert report.rows_updated == 3
    assert set(report.columns_added) == {
        "custom_columns.custom_column_label_norm",
        "custom_columns.custom_column_name_norm",
    }
    assert db.driver.conn.execute(
        "SELECT tag_phash FROM tags WHERE tag_id = 1"
    ).fetchone()[0] == "duplicatetag"
    assert "normalized_identities" in db.driver_wrapper.get_tables()
    db.driver.conn.close()


def test_temporary_value_and_id_tables_are_scoped_and_removed(macro_db):
    macros, db = macro_db
    if isinstance(macros, PostgresDatabaseMacros):
        assert macros._macro_temporary_declared_type("BLOB") == "BYTEA"
        pytest.skip("The PostgreSQL-shaped SQLite harness has no pg_temp schema.")

    with macros.temporary_value_table(("a", "b"), prefix="safe_values") as table:
        assert table.startswith("safe_values_")
        assert db.driver.conn.execute(
            f'SELECT COUNT(*) FROM temp."{table}"'
        ).fetchone()[0] == 2
    assert db.driver.conn.execute(
        "SELECT COUNT(*) FROM sqlite_temp_master WHERE name=?", (table,)
    ).fetchone()[0] == 0

    with macros.temporary_id_table((1, 2, 3)) as id_table:
        assert db.driver.conn.execute(
            f'SELECT SUM(id) FROM temp."{id_table}"'
        ).fetchone()[0] == 6
    with pytest.raises(InputIntegrityError):
        macros.temporary_value_table((1,), prefix='bad"; DROP TABLE tags; --').__enter__()
    with pytest.raises(InputIntegrityError, match="30 characters"):
        macros.temporary_value_table((1,), prefix="x" * 31).__enter__()

    prefix = "failing_values"

    def failing_values():
        yield "first"
        raise RuntimeError("source failed")

    with pytest.raises(RuntimeError, match="source failed"):
        with macros.temporary_value_table(failing_values(), prefix=prefix):
            pass
    assert db.driver.conn.execute(
        "SELECT COUNT(*) FROM sqlite_temp_master WHERE name LIKE ?",
        (f"{prefix}_%",),
    ).fetchone()[0] == 0


def test_orphan_pruning_requires_real_links_and_honours_protected_ids(macro_db):
    macros, db = macro_db
    spec = _strict_link_spec()
    macros.upsert_link(spec, 1, LinkValue(10, link_type="author", priority=1))

    deleted = macros.delete_unreferenced_rows(
        "left_rows",
        (spec,),
        protected_ids=(3,),
    )
    assert deleted == (2,)
    assert [row[0] for row in db.driver.conn.execute("SELECT left_id FROM left_rows ORDER BY left_id")] == [1, 3]

    with pytest.raises(InputIntegrityError):
        macros.delete_unreferenced_rows("right_rows", ())

    db.driver.conn.execute("INSERT INTO left_rows(left_id, name) VALUES (4, 'four')")
    bulk = macros.delete_unreferenced_rows_bulk(
        (
            UnreferencedRowsSpec(
                table="left_rows",
                link_specs=(spec,),
                protected_ids=(3,),
            ),
        )
    )
    assert bulk == {"left_rows": (4,)}


def test_table_fingerprint_is_stable_filtered_and_sensitive_to_content(macro_db):
    macros, db = macro_db
    first = macros.fingerprint_table("right_rows", ("right_id", "name"))
    assert first == macros.fingerprint_table("right_rows", ("right_id", "name"))
    filtered = macros.fingerprint_table(
        "right_rows",
        ("right_id", "name"),
        where={"right_id": 10},
    )
    assert filtered != first

    db.driver.conn.execute("UPDATE right_rows SET name='TEN' WHERE right_id=10")
    assert macros.fingerprint_table("right_rows", ("right_id", "name")) != first
    if isinstance(macros, SQLiteDatabaseMacros):
        assert len(macros.hash_table("right_rows", ("right_id", "name"))) == 32


def test_legacy_macro_correctness_repairs_and_temp_table_safety():
    db = _DB(postgres_shaped=False)
    _create_schema(db.driver.conn)
    macros = SQLiteDatabaseMacros(db)

    macros.direct_update_column_in_table("left_rows", "name", "left_id", 1, "updated")
    assert db.driver.conn.execute("SELECT name FROM left_rows WHERE left_id=1").fetchone()[0] == "updated"

    macros.bulk_add_links(
        "demo_links",
        "demo_link_left_id",
        "demo_link_right_id",
        ((1, 10), (1, 11), (2, 12)),
    )
    assert list(
        db.driver.conn.execute(
            "SELECT demo_link_left_id, demo_link_priority FROM demo_links "
            "ORDER BY demo_link_left_id, demo_link_priority"
        )
    ) == [(1, 1), (1, 2), (2, 1)]

    macros.set_database_version("new")
    assert db.driver.conn.execute(
        "SELECT database_version_version FROM database_version"
    ).fetchone()[0] == "new"

    db.driver.conn.execute("CREATE TABLE temp_bulk_safe(id INTEGER PRIMARY KEY)")
    macros.create_cc_temp_tables(("temp_bulk_safe",), conn=db.driver.conn)
    macros.destroy_cc_temp_tables(("temp_bulk_safe",), conn=db.driver.conn)
    assert db.driver.conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='temp_bulk_safe'"
    ).fetchone()[0] == 1
    with pytest.raises(InputIntegrityError):
        macros.create_cc_temp_tables(('bad"; DROP TABLE tags; --',), conn=db.driver.conn)
    assert db.driver.conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='tags'"
    ).fetchone()[0] == 1
    db.driver.conn.close()
