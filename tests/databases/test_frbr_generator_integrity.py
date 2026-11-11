"""Exercise the generated schema, including resources and polymorphic lifetimes."""

import re
import shutil
import sqlite3
from contextlib import closing

import pytest

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import (
    database_generator as frbr,
)


@pytest.fixture(scope="module")
def schema_template(tmp_path_factory):
    path = tmp_path_factory.mktemp("frbr_integrity") / "template.db"
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        frbr.create_new_database(conn)
    return path


@pytest.fixture
def schema(schema_template, tmp_path):
    path = tmp_path / "catalog.db"
    with closing(sqlite3.connect(schema_template)) as source:
        with closing(sqlite3.connect(path)) as conn:
            source.backup(conn)
            from LiuXin_alpha.databases.search_functions import (
                register_search_functions,
            )

            register_search_functions(conn)
            conn.execute("PRAGMA foreign_keys=ON")
            yield conn


def test_every_packaged_declaration_is_installed(schema):
    declared = set()
    for path in frbr.get_main_table_sql_files() + frbr.get_trigger_sql_files():
        declared.update(
            match.group(1)
            for match in re.finditer(
                r"CREATE\s+(?:UNIQUE\s+)?(?:TABLE|INDEX|TRIGGER|VIEW)\s+"
                r"(?:IF\s+NOT\s+EXISTS\s+)?[`\"]?([a-zA-Z_][a-zA-Z_0-9]*)",
                path.read_text(),
                re.IGNORECASE,
            )
        )
    installed = {row[0] for row in schema.execute("SELECT name FROM sqlite_master")}
    assert len(declared) >= 257
    assert not declared - installed
    assert schema.execute("PRAGMA foreign_key_check").fetchall() == []
    assert schema.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    for (name,) in schema.execute("SELECT name FROM sqlite_master WHERE type='view'"):
        schema.execute(f'SELECT * FROM "{name}" LIMIT 0')


def identifier(conn, kind="work", owner=1, value="valid", primary=1):
    scheme = "url" if kind == "agent" else "uuid"
    return conn.execute(
        "INSERT INTO entity_identifiers (entity_identifier_entity_type, "
        "entity_identifier_entity_id, entity_identifier_scheme, "
        "entity_identifier_value, entity_identifier_is_primary) VALUES (?,?,?,?,?)",
        (kind, owner, scheme, value, primary),
    ).lastrowid


def test_identifier_guards_and_blank_placeholder(schema):
    schema.execute("INSERT INTO works(work_id) VALUES(1)")
    blank = schema.execute(
        "INSERT INTO entity_identifiers(entity_identifier_scratch) VALUES('draft')"
    ).lastrowid
    schema.execute(
        "UPDATE entity_identifiers SET entity_identifier_entity_type='work', "
        "entity_identifier_entity_id=1, entity_identifier_scheme='uuid', "
        "entity_identifier_value='valid', entity_identifier_is_primary=1 "
        "WHERE entity_identifier_id=?",
        (blank,),
    )
    with pytest.raises(sqlite3.IntegrityError, match="missing work_id"):
        identifier(schema, owner=999)
    with pytest.raises(sqlite3.IntegrityError, match="non-empty"):
        identifier(schema, value="")
    with pytest.raises(sqlite3.IntegrityError, match="primary already exists"):
        identifier(schema, value="other")
    for assignment in (
        "entity_identifier_entity_id=999",
        "entity_identifier_entity_type=NULL",
        "entity_identifier_value=''",
        "entity_identifier_scheme=NULL",
    ):
        with pytest.raises(sqlite3.IntegrityError):
            schema.execute(
                f"UPDATE entity_identifiers SET {assignment} WHERE entity_identifier_id=?",
                (blank,),
            )
    alternative = identifier(schema, value="alternative", primary=0)
    with pytest.raises(sqlite3.IntegrityError, match="primary already exists"):
        schema.execute(
            "UPDATE entity_identifiers SET entity_identifier_is_primary=1 WHERE entity_identifier_id=?",
            (alternative,),
        )


@pytest.mark.parametrize("flag", [2, -1, "true", "false", 0.5, b"1"])
@pytest.mark.parametrize("operation", ["insert", "update"])
def test_identifier_primary_flag_domain_is_structural(schema, flag, operation):
    schema.execute("INSERT INTO works(work_id) VALUES(1)")
    primary = identifier(schema)
    alternative = identifier(schema, value="alternative", primary=0)
    with pytest.raises(sqlite3.IntegrityError, match="entity_identifier_primary_valid"):
        if operation == "insert":
            identifier(schema, value="invalid", primary=flag)
        else:
            schema.execute(
                "UPDATE entity_identifiers SET entity_identifier_is_primary=? "
                "WHERE entity_identifier_id=?",
                (flag, alternative),
            )
    assert schema.execute(
        "SELECT entity_identifier_id, entity_identifier_is_primary "
        "FROM entity_identifiers ORDER BY entity_identifier_id"
    ).fetchall() == [(primary, 1), (alternative, 0)]
    # NULL remains valid for placeholders and for an unspecified primary flag.
    schema.execute(
        "UPDATE entity_identifiers SET entity_identifier_is_primary=NULL "
        "WHERE entity_identifier_id=?",
        (alternative,),
    )
    identifier(schema, value="unspecified", primary=None)


@pytest.mark.parametrize("backend", ["SQLite", "SQLite_apsw"])
def test_catalog_primary_flag_validation_preserves_readable_identifiers(
    schema_template, tmp_path, backend
):
    from LiuXin_alpha.catalog.catalog import Catalog
    from LiuXin_alpha.databases.database import Database

    path = tmp_path / "primary-flags.db"
    shutil.copyfile(schema_template, path)
    with Database(
        metadata={"database_path": str(path)},
        db_type=backend,
        create=False,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as db:
        catalog = Catalog(db)
        owner = catalog.works.create({"title": "Primary flags"})
        value = "12345678-1234-4234-8234-123456789abc"
        payload = {
            "entity_type": "work",
            "entity_id": owner,
            "scheme": "uuid",
            "value": value,
        }
        primary = catalog.identifiers.create({**payload, "is_primary": True})
        alternative = catalog.identifiers.create({**payload, "is_primary": False})
        before = catalog.identifiers.list_for_wemi(level="work", entity_id=owner)
        for column in ("is_primary", "entity_identifier_is_primary"):
            for flag in (2, -1, "true", "false", "0", "1", 0.0, 1.0, 0.5, b"1", [], {}):
                error = ValueError if isinstance(flag, int) else TypeError
                with pytest.raises(error, match="is_primary"):
                    catalog.identifiers.create({**payload, column: flag})
                with pytest.raises(error, match="is_primary"):
                    catalog.identifiers.update(alternative, {column: flag})
                assert (
                    catalog.identifiers.list_for_wemi(level="work", entity_id=owner)
                    == before
                )
                assert catalog.identifiers.primary_values_for_wemi(
                    level="work", entity_id=owner
                ) == {"uuid": value}
        # Both entry points accept only None, booleans, and integer 0/1.
        other = catalog.works.create({"title": "Valid primary flags"})
        for flag in (None, False, True, 0, 1):
            row = catalog.identifiers.create(
                {**payload, "entity_id": other, "is_primary": flag}
            )
            expected = None if flag is None else int(flag)
            assert (
                catalog.identifiers.require(row)["entity_identifier_is_primary"]
                == expected
            )
            catalog.identifiers.update(row, {"entity_identifier_is_primary": flag})
            assert (
                catalog.identifiers.require(row)["entity_identifier_is_primary"]
                == expected
            )
            catalog.identifiers.delete(row)
        with closing(sqlite3.connect(path)) as reader:
            assert reader.execute(
                "SELECT entity_identifier_id, entity_identifier_is_primary "
                "FROM entity_identifiers ORDER BY entity_identifier_id"
            ).fetchall() == [(primary, 1), (alternative, 0)]


@pytest.mark.parametrize(
    "kind", ["work", "expression", "manifestation", "item", "agent"]
)
def test_owner_lifetime_is_structural_and_transactional(schema, kind):
    table = kind + "s"
    schema.execute(f"INSERT INTO {table}({kind}_id) VALUES(1)")
    identifier(schema, kind)
    schema.commit()
    schema.execute("SAVEPOINT lifetime")
    schema.execute(f"DELETE FROM {table} WHERE {kind}_id=1")
    assert schema.execute("SELECT count(*) FROM entity_identifiers").fetchone() == (0,)
    schema.execute("ROLLBACK TO lifetime")
    assert schema.execute(
        "SELECT entity_identifier_entity_id FROM entity_identifiers"
    ).fetchone() == (1,)
    # Keys are stable while identifiers reference them. Unreferenced keys may change.
    with pytest.raises(sqlite3.IntegrityError, match="identifier"):
        schema.execute(f"UPDATE {table} SET {kind}_id=2 WHERE {kind}_id=1")
    schema.execute(f"DELETE FROM {table} WHERE {kind}_id=1")
    schema.execute(f"INSERT INTO {table}({kind}_id) VALUES(1)")
    schema.commit()
    path = schema.execute("PRAGMA database_list").fetchone()[2]
    with closing(sqlite3.connect(path)) as observer:
        assert observer.execute(
            "SELECT count(*) FROM entity_identifiers"
        ).fetchone() == (0,)
        assert observer.execute("PRAGMA foreign_key_check").fetchall() == []


@pytest.mark.parametrize(
    "requested",
    [
        '["sequence_number"]',
        '["is_required"]',
        '["sequence_number", "is_required"]',
        '"all"',
    ],
)
def test_intralink_columns_from_toml(tmp_path, monkeypatch, requested):
    (tmp_path / "intralink_table_requests.toml").write_text(
        f'[[intralinks]]\ntable="works"\nrequested_cols={requested}\n'
    )
    monkeypatch.setattr(frbr, "__folder__", str(tmp_path))
    with closing(sqlite3.connect(":memory:")) as conn:
        conn.execute("CREATE TABLE works(work_id INTEGER PRIMARY KEY)")
        builder = frbr.SQLiteDatabaseGenerator(conn)
        builder.intralink_tables = builder.get_requested_intralink_tables()
        builder.create_intralink_table("works", conn)
        columns = {
            r[1]: r for r in conn.execute("PRAGMA table_info(work_work_intralinks)")
        }
        for suffix, default in (("sequence_number", None), ("is_required", "1")):
            if suffix in requested or requested == '"all"':
                assert columns[f"work_work_intralink_{suffix}"][2:5] == (
                    "INTEGER",
                    0,
                    default,
                )


@pytest.mark.parametrize("allow", [False, True])
def test_redundant_interlink_filter_retains_only_admitted_specs(
    tmp_path, monkeypatch, allow
):
    (tmp_path / "interlink_table_requests.toml").write_text(
        f"allow_redundant_links={str(allow).lower()}\n[[interlinks]]\n"
        'left="works"\nright="expressions"\nlink_type="one_to_many"\n'
        'requested_cols=["type"]\ntypes=["translation"]\n'
    )
    monkeypatch.setattr(frbr, "__folder__", str(tmp_path))
    with closing(sqlite3.connect(":memory:")) as conn:
        conn.executescript(
            "CREATE TABLE works(work_id INTEGER PRIMARY KEY);"
            "CREATE TABLE expressions(expression_id INTEGER PRIMARY KEY, "
            "expression_work_id INTEGER REFERENCES works(work_id));"
        )
        builder = frbr.SQLiteDatabaseGenerator(conn)
        builder.interlink_tables_pairs = builder.get_requested_interlink_tables()
        assert builder.interlink_tables_pairs == set(builder.interlink_specs_by_pair)
        builder.interlink_tables = {
            builder.get_interlink_name(p) for p in builder.interlink_tables_pairs
        }
        builder.apply_interlink_constraints_from_spec()
        for left, right in builder.interlink_tables_pairs:
            builder.create_interlink_table(left, right, conn)
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert ("expression_work_links" in tables) is allow
        assert ("expression_work_links__types" in tables) is allow
        if allow:
            assert conn.execute(
                "SELECT type FROM expression_work_links__types"
            ).fetchall() == [("translation",)]


@pytest.mark.parametrize(
    "path", [r"a\b", r"\root", r"C:\root", r"a\\b", "../b", "a//b"]
)
@pytest.mark.parametrize("operation", ["insert", "update"])
def test_folder_paths_reject_noncanonical_separators(schema, path, operation):
    row = schema.execute("INSERT INTO folders(folder_relpath) VALUES('a/b')").lastrowid
    with pytest.raises(sqlite3.IntegrityError):
        if operation == "insert":
            schema.execute("INSERT INTO folders(folder_relpath) VALUES(?)", (path,))
        else:
            schema.execute(
                "UPDATE folders SET folder_relpath=? WHERE folder_id=?", (path, row)
            )
    schema.execute("UPDATE folders SET folder_relpath='a/c' WHERE folder_id=?", (row,))


@pytest.mark.parametrize("backend", ["SQLite", "SQLite_apsw"])
@pytest.mark.parametrize(
    "kind,field",
    [
        ("work", "title"),
        ("expression", "label"),
        ("manifestation", "edition_statement"),
        ("item", "inventory_code"),
        ("agent", "name"),
    ],
)
def test_catalog_owner_deletion_cannot_transfer_identifier(
    tmp_path, backend, kind, field
):
    from LiuXin_alpha.catalog.catalog import Catalog
    from LiuXin_alpha.databases.database import Database

    path = tmp_path / "public.db"
    with Database(
        metadata={"database_path": str(path)},
        db_type=backend,
        create=True,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as db:
        catalog = Catalog(db)
        repo = getattr(catalog, kind + "s")
        owner = repo.create({field: "Original"})
        identifier_id = catalog.identifiers.create(
            {
                "entity_type": kind,
                "entity_id": owner,
                "scheme": "url" if kind == "agent" else "uuid",
                "value": "https://example.org/owner"
                if kind == "agent"
                else "12345678-1234-4234-8234-123456789abc",
            }
        )
        with pytest.raises(RuntimeError, match="rollback"):
            with db.macros.transaction():
                repo.delete(owner)
                assert catalog.identifiers.get(identifier_id) is None
                raise RuntimeError("rollback")
        assert catalog.identifiers.get(identifier_id) is not None
        repo.delete(owner)
        repo.create({field: "Replacement"})
        with closing(sqlite3.connect(path)) as reader:
            assert reader.execute(
                "SELECT count(*) FROM entity_identifiers"
            ).fetchone() == (0,)


@pytest.mark.parametrize(
    "kind", ["work", "expression", "manifestation", "item", "agent"]
)
@pytest.mark.parametrize("recursive", [False, True])
def test_replace_cannot_transfer_an_identified_owner(schema, kind, recursive):
    schema.execute(f"PRAGMA recursive_triggers={int(recursive)}")
    schema.execute(f"INSERT INTO {kind}s({kind}_id) VALUES(1)")
    identifier(schema, kind)
    with pytest.raises(sqlite3.IntegrityError, match="Cannot replace"):
        schema.execute(f"INSERT OR REPLACE INTO {kind}s({kind}_id) VALUES(1)")
    assert schema.execute(
        "SELECT entity_identifier_entity_id FROM entity_identifiers"
    ).fetchone() == (1,)


def test_identifier_cleanup_keeps_other_owner_namespaces(schema):
    schema.execute("INSERT INTO works(work_id) VALUES(1)")
    schema.execute("INSERT INTO agents(agent_id) VALUES(1)")
    identifier(schema, "work")
    identifier(schema, "agent")
    schema.execute("DELETE FROM works WHERE work_id=1")
    assert schema.execute(
        "SELECT entity_identifier_entity_type FROM entity_identifiers"
    ).fetchall() == [("agent",)]


def test_genre_insert_cannot_complete_a_cycle(schema):
    # Existing dangling edges are possible after importing with foreign_keys off.
    # Installing the formerly omitted AFTER INSERT guard must close this path.
    schema.execute("PRAGMA foreign_keys=OFF")
    schema.execute("INSERT INTO genres(genre_id, genre_parent_id) VALUES(1, 2)")
    with pytest.raises(sqlite3.IntegrityError, match="cycle"):
        schema.execute("INSERT INTO genres(genre_id, genre_parent_id) VALUES(2, 1)")


def test_unassigned_identifier_can_be_attached_without_half_filled_references(schema):
    schema.execute("INSERT INTO works(work_id) VALUES(1)")
    candidate = schema.execute(
        "INSERT INTO entity_identifiers(entity_identifier_scheme, entity_identifier_value) "
        "VALUES('uuid', 'candidate')"
    ).lastrowid
    for column, value in (
        ("entity_identifier_entity_type", "work"),
        ("entity_identifier_entity_id", 1),
    ):
        with pytest.raises(sqlite3.IntegrityError, match="both"):
            schema.execute(
                f"UPDATE entity_identifiers SET {column}=? WHERE entity_identifier_id=?",
                (value, candidate),
            )
    schema.execute(
        "UPDATE entity_identifiers SET entity_identifier_entity_type='work', "
        "entity_identifier_entity_id=1 WHERE entity_identifier_id=?",
        (candidate,),
    )
    schema.execute("DELETE FROM works WHERE work_id=1")
    assert schema.execute("SELECT count(*) FROM entity_identifiers").fetchone() == (0,)


@pytest.mark.parametrize("backend", ["SQLite", "SQLite_apsw"])
def test_catalog_identifier_replacement_preserves_primary_uniqueness_and_rollback(
    tmp_path, monkeypatch, backend
):
    from LiuXin_alpha.catalog.catalog import Catalog
    from LiuXin_alpha.databases.database import Database

    path = tmp_path / "replace.db"
    with Database(
        metadata={"database_path": str(path)},
        db_type=backend,
        create=True,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as db:
        catalog = Catalog(db)
        owner = catalog.works.create({"title": "Owner"})
        first = "12345678-1234-4234-8234-123456789abc"
        second = "12345678-1234-4234-8234-123456789def"
        original = catalog.identifiers.replace_for_wemi(
            level="work", entity_id=owner, identifiers={"uuid": first}
        )
        with monkeypatch.context() as patch:
            delete = catalog.identifiers.delete

            def fail_after_delete(identifier_id):
                delete(identifier_id)
                raise RuntimeError("late replacement failure")

            patch.setattr(catalog.identifiers, "delete", fail_after_delete)
            with pytest.raises(RuntimeError, match="late replacement failure"):
                catalog.identifiers.replace_for_wemi(
                    level="work", entity_id=owner, identifiers={"uuid": second}
                )
        with closing(sqlite3.connect(path)) as reader:
            assert reader.execute(
                "SELECT entity_identifier_id, entity_identifier_value, entity_identifier_is_primary "
                "FROM entity_identifiers"
            ).fetchall() == [(original["uuid"], first, 1)]
        catalog.identifiers.replace_for_wemi(
            level="work", entity_id=owner, identifiers={"uuid": second}
        )
        assert catalog.identifiers.primary_values_for_wemi(
            level="work", entity_id=owner
        ) == {"uuid": second}
        assert (
            len(catalog.identifiers.list_for_wemi(level="work", entity_id=owner)) == 1
        )


@pytest.mark.parametrize(
    "kind", ["work", "expression", "manifestation", "item", "agent"]
)
@pytest.mark.parametrize("alias", ["primary_key", "rowid", "_rowid_", "oid"])
@pytest.mark.parametrize("identified", ["source", "destination"])
def test_all_owner_key_spellings_preserve_identifier_ownership(
    schema, kind, alias, identified
):
    column = f"{kind}_id" if alias == "primary_key" else alias
    schema.execute(f"INSERT INTO {kind}s({kind}_id) VALUES(1)")
    schema.execute(f"INSERT INTO {kind}s({kind}_id) VALUES(2)")
    owner = 1 if identified == "source" else 2
    identifier(schema, kind, owner=owner)
    schema.commit()
    schema.execute("PRAGMA recursive_triggers=OFF")
    with pytest.raises(sqlite3.IntegrityError, match="identifiers"):
        schema.execute(f"UPDATE OR REPLACE {kind}s SET {column}=2 WHERE {kind}_id=1")
    assert schema.execute(
        f"SELECT {kind}_id FROM {kind}s ORDER BY {kind}_id"
    ).fetchall() == [(1,), (2,)]
    assert schema.execute(
        "SELECT entity_identifier_entity_id FROM entity_identifiers"
    ).fetchone() == (owner,)
    if identified == "source":
        with pytest.raises(sqlite3.IntegrityError, match="identifiers"):
            schema.execute(f"UPDATE {kind}s SET {column}=3 WHERE {kind}_id=1")
    # An unreferenced owner can still change its key, through any spelling.
    unreferenced = 2 if identified == "source" else 1
    schema.execute(f"UPDATE {kind}s SET {column}=3 WHERE {kind}_id=?", (unreferenced,))
    schema.commit()
    path = schema.execute("PRAGMA database_list").fetchone()[2]
    with closing(sqlite3.connect(path)) as reader:
        assert reader.execute(
            "SELECT entity_identifier_entity_id FROM entity_identifiers"
        ).fetchone() == (owner,)
