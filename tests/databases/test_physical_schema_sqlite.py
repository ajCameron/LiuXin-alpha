"""Verify native SQLite facts including unusual keys and SQL defaults."""

import sqlite3
from contextlib import closing
from dataclasses import fields

import pytest

from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.physical_schema import (
    inspect_relation,
)
from LiuXin_alpha.databases.schema_specs import (
    StorageTableSpec,
    build_row_dataclass_for_table,
)


def test_composite_foreign_keys_partial_and_expression_indexes():
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.executescript("""
            CREATE TABLE parent(a INTEGER, b TEXT, PRIMARY KEY(a,b)) WITHOUT ROWID;
            CREATE TABLE child(id INTEGER PRIMARY KEY, a INTEGER, b TEXT,
                one TEXT UNIQUE, pair_a TEXT, pair_b TEXT,
                FOREIGN KEY(a,b) REFERENCES parent ON DELETE CASCADE,
                UNIQUE(pair_a,pair_b));
            CREATE UNIQUE INDEX partial_one ON child(b) WHERE b IS NOT NULL;
            CREATE UNIQUE INDEX expression_one ON child(lower(one));
        """)
        spec = inspect_relation(connection, "child")
        columns = {column.name: column for column in spec.columns}
        assert columns["id"].nullable is False and columns["id"].identity
        assert columns["one"].is_unique
        assert not columns["pair_a"].is_unique
        assert not columns["b"].is_unique
        fk = spec.foreign_keys[0]
        assert fk.columns == ("a", "b")
        assert fk.references_columns == ("a", "b")
        assert fk.on_delete == "CASCADE" and fk.deferrable is None
        assert columns["a"].references_column is None
        assert next(
            index for index in spec.indexes if index.name == "partial_one"
        ).partial
        assert next(
            index for index in spec.indexes if index.name == "expression_one"
        ).columns == (None,)
        assert all(
            column.nullable is False
            for column in inspect_relation(connection, "parent").columns
        )


@pytest.mark.parametrize(
    "definition,nullable,identity",
    [
        ("id TEXT PRIMARY KEY", True, False),
        ("id INTEGER PRIMARY KEY DESC", True, False),
        ("id INTEGER PRIMARY KEY", False, True),
        ("id TEXT PRIMARY KEY NOT NULL", False, False),
    ],
)
def test_sqlite_primary_key_nullability_is_not_assumed(definition, nullable, identity):
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute(f"CREATE TABLE target({definition})").close()
        column = inspect_relation(connection, "target").columns[0]
        assert column.nullable is nullable and column.identity is identity


def test_defaults_generated_columns_and_unknown_view_nullability():
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute("""CREATE TABLE "quoted table" (
            id INTEGER PRIMARY KEY,
            required TEXT NOT NULL DEFAULT 'book',
            empty TEXT DEFAULT NULL,
            stamp TEXT DEFAULT CURRENT_TIMESTAMP,
            calculated INTEGER GENERATED ALWAYS AS (id + 1) STORED
        )""").close()
        spec = inspect_relation(connection, "quoted table")
        columns = {column.name: column for column in spec.columns}
        assert columns["required"].default_sql == "'book'"
        assert columns["empty"].has_default and columns["empty"].default_sql == "NULL"
        assert columns["stamp"].default_sql == "CURRENT_TIMESTAMP"
        assert columns["calculated"].generated and not columns["calculated"].has_default
        value_type = build_row_dataclass_for_table(
            StorageTableSpec(spec.name, spec.relation_kind, spec.columns)
        )
        # A SQL default is metadata, never a Python default or an evaluated expression.
        import dataclasses

        assert all(field.default is dataclasses.MISSING for field in fields(value_type))
        connection.execute('CREATE VIEW named AS SELECT * FROM "quoted table"').close()
        assert all(
            column.nullable is None
            for column in inspect_relation(connection, "named").columns
        )


def test_pending_temp_shadowing_and_binding_limit():
    from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.physical_schema import (
        capabilities,
    )

    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute("CREATE TABLE target(original TEXT)").close()
        connection.execute("BEGIN").close()
        connection.execute("CREATE TEMP TABLE target(pending INTEGER NOT NULL)").close()
        assert inspect_relation(connection, "target").columns[0].name == "pending"
        assert connection.in_transaction
        connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 77)
        assert capabilities(connection).max_bind_parameters == 77
        connection.rollback()
        assert inspect_relation(connection, "target").columns[0].name == "original"
