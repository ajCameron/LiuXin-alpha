from __future__ import annotations

import importlib
import sys
import types
from typing import Any

import pytest

from LiuXin_alpha.databases.column_metadata import (
    ColumnEmptyValuePolicy,
    ColumnMergePolicy,
    ColumnNormalizationProfile,
    ColumnSemanticRole,
    ColumnValidationProfile,
)
from LiuXin_alpha.databases.schema_specs import LinkKind
from LiuXin_alpha.errors import InputIntegrityError


def test_postgresql_driver_is_registered() -> None:
    from LiuXin_alpha.databases.database_driver_plugins.registry import (
        get_registered_database_driver_names,
        load_database_driver,
    )

    assert "PostgreSQL" in get_registered_database_driver_names()
    assert load_database_driver("PostgreSQL") is load_database_driver("pg")
    assert load_database_driver("postgres").__name__ == "DatabaseDriver"


def test_postgresql_driver_exposes_shared_column_base_contract() -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.databasedriver import (
        DatabaseDriver,
    )

    assert DatabaseDriver.direct_get_column_base("ratings") == "rating"
    assert DatabaseDriver.direct_get_column_base("digital_assets") == (
        "digital_asset"
    )


def test_postgresql_driver_import_does_not_require_psycopg2() -> None:
    mod = importlib.import_module("LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.databasedriver")

    assert mod.DatabaseDriver is not None


def test_postgresql_url_redaction() -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.config import redact_postgres_url

    url = "postgresql://liuxin:secret@example.invalid:5432/library?sslpassword=hidden&application_name=lx"

    redacted = redact_postgres_url(url)

    assert "secret" not in redacted
    assert "hidden" not in redacted
    assert "liuxin:***@" in redacted
    assert "sslpassword=%2A%2A%2A" in redacted
    assert "application_name=lx" in redacted


def test_postgresql_schema_configuration_prefers_explicit_metadata_env(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.config import configured_postgres_schema

    monkeypatch.setenv("LIUXIN_POSTGRES_SCHEMA", "env_schema")

    assert configured_postgres_schema() == "env_schema"
    assert configured_postgres_schema({"schema": "metadata_schema"}) == "metadata_schema"
    assert configured_postgres_schema({"schema": "metadata_schema"}, explicit="explicit_schema") == "explicit_schema"


def test_postgresql_service_target_configuration_prefers_explicit_metadata_env(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.config import (
        configured_postgres_service,
        configured_postgres_target,
    )

    monkeypatch.setenv("PGSERVICE", "pg_service")
    monkeypatch.setenv("LIUXIN_POSTGRES_SERVICE", "env_service")
    monkeypatch.delenv("LIUXIN_POSTGRES_URL", raising=False)
    monkeypatch.delenv("LIUXIN_DATABASE_URL", raising=False)

    assert configured_postgres_service() == "env_service"
    assert configured_postgres_service({"postgres_service": "metadata_service"}) == "metadata_service"
    assert configured_postgres_service(explicit="explicit_service") == "explicit_service"

    target = configured_postgres_target({"postgres_service": "metadata_service"})
    assert target.kind == "service"
    assert target.value == "metadata_service"
    assert target.label == "service=metadata_service"


def test_postgresql_connect_uses_native_service_profile(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import connect_postgres

    calls: list[dict[str, object]] = []
    fake_psycopg2 = types.SimpleNamespace(
        connect=lambda **kwargs: calls.append(dict(kwargs)) or object(),
    )
    monkeypatch.setitem(sys.modules, "psycopg2", fake_psycopg2)

    conn = connect_postgres(
        {"postgres_service": "liuxin_runtime"},
        password="service-secret",
        prompt_for_password=False,
    )

    assert conn is not None
    assert calls == [
        {
            "service": "liuxin_runtime",
            "password": "service-secret",
            "connect_timeout": 10,
            "application_name": "liuxin-alpha",
        }
    ]


def test_postgresql_connect_hints_when_python_driver_is_missing(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
        PostgresConnectionError,
        connect_postgres,
    )

    monkeypatch.setitem(sys.modules, "psycopg2", None)

    with pytest.raises(PostgresConnectionError) as raised:
        connect_postgres(
            {},
            "postgresql://liuxin@example.invalid/library",
            prompt_for_password=False,
        )

    message = str(raised.value)
    assert ".[postgres]" in message
    assert "PostgreSQL Python support" in message


def test_postgresql_errors_hint_for_missing_database_and_unavailable_server() -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
        redact_postgres_error,
    )

    missing_database = RuntimeError(
        'connection to postgresql://owner:secret@example.invalid/missing failed: '
        'database "missing" does not exist'
    )
    missing_database.pgcode = "3D000"  # type: ignore[attr-defined]
    database_message = redact_postgres_error(
        missing_database,
        "postgresql://owner:secret@example.invalid/missing",
    )
    assert "secret" not in database_message
    assert "setup-sql --help" in database_message
    assert "does not exist" in database_message

    unavailable_message = redact_postgres_error(
        RuntimeError("connection refused: could not connect to server")
    )
    assert "pg_isready" in unavailable_message
    assert "installed and running" in unavailable_message

    service_message = redact_postgres_error(
        RuntimeError('definition of service "missing_profile" not found')
    )
    assert "PGSERVICEFILE" in service_message
    assert "postgresql:// URL" in service_message


def test_postgresql_shared_connection_helper_uses_configured_schema(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import connection as pg_connection

    class FakeCursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...] | None]] = []

        def execute(self, sql: str, values=None):
            self.calls.append((sql, values))
            return self

        def fetchone(self):
            return {"exists": True}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def __init__(self) -> None:
            self.closed = False
            self.cursor = FakeCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def close(self) -> None:
            self.closed = True

    conn = FakeConnection()
    monkeypatch.setattr(pg_connection, "connect_postgres", lambda *args, **kwargs: conn)
    monkeypatch.setattr(pg_connection, "postgres_cursor", lambda raw_conn: raw_conn.cursor)

    result = pg_connection.with_postgres_connection(
        "schema-aware check",
        lambda cur: "done",
        metadata={
            "postgres_url": "postgresql://liuxin@example.invalid/library",
            "schema": "liuxin_test",
        },
        required_tables=("ratings",),
        prompt_for_password=False,
    )

    assert result == "done"
    assert conn.closed is True
    assert ("set local search_path to \"liuxin_test\"", None) in conn.cursor.calls
    assert (
        "select to_regclass(%s) is not null as exists",
        ("liuxin_test.ratings",),
    ) in conn.cursor.calls


def test_postgresql_sql_translation_is_small_and_explicit() -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import translate_sql_for_postgres

    translated = translate_sql_for_postgres("insert into `asset_replicas` (`asset_replica_storage_key`) values (?, '?')")

    assert translated == 'insert into "asset_replicas" ("asset_replica_storage_key") values (%s, \'?\')'


def test_database_init_classifies_postgres_as_server_backend() -> None:
    from LiuXin_alpha.databases.database import _metadata_uses_server_database

    assert _metadata_uses_server_database({"database_path": "postgresql://liuxin@example.invalid/library"}, "SQLite")
    assert _metadata_uses_server_database({"postgres_url": "postgres://liuxin@example.invalid/library"}, "pg")
    assert _metadata_uses_server_database({"postgres_service": "liuxin_runtime"}, "SQLite")
    assert _metadata_uses_server_database({"database_service": "liuxin_runtime"}, "SQLite")
    assert _metadata_uses_server_database({"service": "liuxin_runtime"}, "PostgreSQL")
    assert not _metadata_uses_server_database({"database_path": "/tmp/liuxin.db"}, "SQLite")
    assert not _metadata_uses_server_database({"service": "unrelated"}, "SQLite")


def test_postgresql_schema_catalog_satisfies_checker_contract() -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.checker import (
        LIUXIN_POSTGRES_REQUIRED_COLUMNS,
        LIUXIN_POSTGRES_REQUIRED_TABLES,
    )
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.schema import schema_table_catalog

    catalog = schema_table_catalog()

    assert set(LIUXIN_POSTGRES_REQUIRED_TABLES) <= set(catalog)
    for table_name, required_columns in LIUXIN_POSTGRES_REQUIRED_COLUMNS.items():
        assert set(required_columns) <= set(catalog[table_name])
    assert "custom_column_label" in catalog["custom_columns"]
    assert {
        "column_metadata_table_name",
        "column_metadata_column_name",
        "column_metadata_case_sensitive",
        "column_metadata_semantic_role",
        "column_metadata_normalization_profile",
        "column_metadata_comparison_column",
        "column_metadata_empty_value_policy",
        "column_metadata_merge_policy",
        "column_metadata_validation_profile",
        "column_metadata_formatting_options_json",
        "column_metadata_display_options_json",
    } <= set(catalog["column_metadata"])
    assert "workflow_step_code" in catalog["workflow_steps"]


class _RecordingSchemaConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str, values=None):
        self.statements.append(sql)
        return _ResultCursor([(1,)])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_postgresql_schema_builder_executes_core_and_storage_tables() -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.schema import (
        create_postgres_schema,
        schema_table_catalog,
    )

    conn = _RecordingSchemaConnection()

    create_postgres_schema(conn, schema="liuxin_test")
    ddl = "\n".join(conn.statements)
    ddl_lower = ddl.lower()

    assert 'create schema if not exists "liuxin_test"' in ddl
    assert 'create table if not exists "works"' in ddl
    assert 'create table if not exists "stores"' in ddl
    assert 'create table if not exists "digital_assets"' in ddl
    assert '"digital_asset_size_bytes" bigint null' in ddl
    assert 'create table if not exists "asset_replicas"' in ddl
    assert ddl_lower.index(
        'create table if not exists "transform_runs"'
    ) < ddl_lower.index(
        'create table if not exists "digital_asset_derivations"'
    )
    assert (
        'references "stores" ("store_id") on delete restrict on update cascade'
        in ddl_lower
    )
    assert 'create table if not exists "column_metadata"' in ddl_lower
    assert (
        "values ('works', 'work_title', 0, 'title', "
        "'unicode_nfc_trim_casefold', null, 'null_or_blank_is_missing', "
        "'replace', 'display_text', '{}', '{}') on conflict "
        '("column_metadata_table_name", "column_metadata_column_name") do nothing'
    ) in ddl
    assert (
        "values ('works', 'work_id', 1, 'identifier', 'none', null, "
        "'null_is_missing', 'preserve_existing', 'identifier', '{}', '{}') on conflict "
        '("column_metadata_table_name", "column_metadata_column_name") do nothing'
    ) in ddl
    assert ddl.count('insert into "column_metadata"') == sum(
        len(columns) for columns in schema_table_catalog().values()
    )


def test_postgresql_schema_builder_entrypoint_uses_metadata_schema(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import schema as pg_schema

    class FakeRawConnection:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    raw = FakeRawConnection()
    calls: list[tuple[object, str]] = []

    monkeypatch.setattr(pg_schema, "connect_postgres", lambda metadata, url: raw)

    def fake_create(conn, *, schema: str):
        calls.append((conn, schema))

    monkeypatch.setattr(pg_schema, "create_postgres_schema", fake_create)

    pg_schema.create_new_database(
        {
            "postgres_url": "postgresql://liuxin@example.invalid/library",
            "schema": "liuxin_test",
        }
    )

    assert raw.closed is True
    assert calls and calls[0][1] == "liuxin_test"


def test_postgresql_runtime_grant_statements_validate_identifiers() -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.runtime_privileges import (
        PostgresRuntimePrivilegeError,
        build_postgres_setup_statements,
        build_runtime_grant_statements,
    )

    statements = build_runtime_grant_statements(
        role="liuxin_runtime",
        database="liuxin",
        schema="public",
    )

    assert 'grant connect on database "liuxin" to "liuxin_runtime"' in statements
    assert 'grant usage, select on all sequences in schema "public" to "liuxin_runtime"' in statements

    import pytest

    with pytest.raises(PostgresRuntimePrivilegeError):
        build_runtime_grant_statements(role="bad role", database="liuxin", schema="public")

    setup_statements = build_postgres_setup_statements(
        database="liuxin",
        owner_role="liuxin_owner",
        runtime_role="liuxin_runtime",
        schema="liuxin",
        create_database=False,
    )
    joined = "\n".join(setup_statements)
    assert "create database" not in joined.casefold()
    assert "pg_catalog.pg_roles" in joined
    assert 'create schema if not exists "liuxin" authorization "liuxin_owner"' in joined
    assert 'grant connect on database "liuxin" to "liuxin_runtime"' in joined
    assert (
        'alter default privileges for role "liuxin_owner" in schema "liuxin" '
        'grant select, insert, update, delete on tables to "liuxin_runtime"'
    ) in joined

    server_only = "\n".join(
        build_postgres_setup_statements(
            database="liuxin",
            owner_role="liuxin_owner",
            runtime_role="liuxin_runtime",
            schema="liuxin",
            section="server",
        )
    )
    assert 'create database "liuxin" owner "liuxin_owner"' in server_only
    assert "create schema if not exists" not in server_only

    database_only = "\n".join(
        build_postgres_setup_statements(
            database="liuxin",
            owner_role="liuxin_owner",
            runtime_role="liuxin_runtime",
            schema="liuxin",
            section="database",
        )
    )
    assert "pg_catalog.pg_roles" not in database_only
    assert "create database" not in database_only.casefold()
    assert 'create schema if not exists "liuxin" authorization "liuxin_owner"' in database_only
    assert 'alter default privileges for role "liuxin_owner" in schema "liuxin"' in database_only

    with pytest.raises(PostgresRuntimePrivilegeError):
        build_postgres_setup_statements(
            database="liuxin",
            owner_role="liuxin_owner",
            runtime_role="bad role",
        )
    with pytest.raises(PostgresRuntimePrivilegeError):
        build_postgres_setup_statements(
            database="liuxin",
            owner_role="liuxin_owner",
            runtime_role="liuxin_runtime",
            section="bad",
        )
    with pytest.raises(PostgresRuntimePrivilegeError):
        build_runtime_grant_statements(
            role="liuxin_runtime",
            database="liuxin",
            schema="public",
            default_privileges_for_role="bad role",
        )


class _FakeDriverCursor:
    def __init__(self) -> None:
        self.sql = ""
        self.values: tuple[Any, ...] | None = None

    def execute(self, sql: str, values: tuple[Any, ...] | None = None):
        self.sql = sql
        self.values = values
        return self

    def executemany(self, sql: str, values):
        self.sql = sql
        self.values = tuple(values)
        return self

    def fetchone(self):
        lowered = self.sql.lower()
        if "select 1" in lowered:
            return (1,)
        if "schema_fingerprint" in lowered:
            return ("fingerprint",)
        return None

    def fetchall(self):
        lowered = self.sql.lower()
        if "from pg_catalog.pg_index" in lowered:
            return [(["digital_asset_id", "digital_asset_size_bytes"],)]
        if "from information_schema.tables" in lowered:
            return [("database_metadata",), ("stores",), ("digital_assets",)]
        if "from information_schema.columns" in lowered and "table_name, column_name" in lowered:
            return [
                ("database_metadata", "database_metadata_id"),
                ("database_metadata", "database_metadata_unique_id"),
                ("stores", "store_id"),
                ("stores", "store_kind"),
                ("digital_assets", "digital_asset_id"),
                ("digital_assets", "digital_asset_size_bytes"),
            ]
        if "from information_schema.columns" in lowered and "data_type" in lowered:
            table = self.values[1]
            if table == "digital_assets":
                return [
                    ("digital_asset_id", "bigint"),
                    ("digital_asset_size_bytes", "bigint"),
                ]
            return []
        return []

    def close(self) -> None:
        pass

    def __iter__(self):
        return iter(self.fetchall())


class _FakeDriverConnection:
    def __init__(self) -> None:
        self.closed = False
        self.cursors: list[_FakeDriverCursor] = []

    def cursor(self, *args, **kwargs):
        cur = _FakeDriverCursor()
        self.cursors.append(cur)
        return cur

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _ResultCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.lastrowid = None

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)

    def __iter__(self):
        return iter(self.rows)


class _RecordingDriverConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, values=None):
        self.calls.append((sql, values))
        lowered = sql.lower()
        if (
            lowered.lstrip().startswith("select")
            and "column_metadata_case_sensitive" in lowered
        ):
            return _ResultCursor(
                [
                    (
                        1,
                        "title",
                        "unicode_nfc_trim_casefold",
                        None,
                        "null_or_blank_is_missing",
                        "replace",
                        "display_text",
                        "{}",
                        "{}",
                    )
                ]
            )
        if "from information_schema.columns" in lowered and "rating_view" in tuple(str(value) for value in (values or ())):
            return _ResultCursor([("id",), ("label",), ("rating",)])
        if "from information_schema.triggers" in lowered and "event_object_table" in lowered:
            return _ResultCursor([("ratings",)])
        if "from information_schema.triggers" in lowered and "trigger_name" in lowered:
            return _ResultCursor([("contract_rating_trigger",)])
        if "returning" in lowered:
            return _ResultCursor([(42,)])
        if "select count(*)" in lowered:
            return _ResultCursor([(0,)])
        if "order by random()" in lowered:
            return _ResultCursor([(9, "random-label", 90)])
        if "select distinct" in lowered:
            return _ResultCursor([("alpha",), ("beta",)])
        if "select max(" in lowered:
            return _ResultCursor([(90,)])
        if "select min(" in lowered:
            return _ResultCursor([(1,)])
        if 'where "rating_id" > %s' in lowered:
            start_id = int((values or (0,))[0])
            if start_id < 2:
                return _ResultCursor([(1, "iter-one", 10), (2, "iter-two", 20)])
            return _ResultCursor([])
        if " where " in lowered:
            return _ResultCursor([(3, "alpha", 1)])
        return _ResultCursor([(1,)])

    def executemany(self, sql: str, values):
        self.calls.append((sql, tuple(values)))
        return _ResultCursor([])

    def close(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_postgresql_driver_connects_and_introspects(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import databasedriver as pg_driver

    raw_connections: list[_FakeDriverConnection] = []

    def fake_connect(metadata=None, url=None, **kwargs):
        conn = _FakeDriverConnection()
        raw_connections.append(conn)
        return conn

    monkeypatch.setattr(pg_driver, "connect_postgres", fake_connect)

    drv = pg_driver.DatabaseDriver({"postgres_url": "postgresql://liuxin:secret@example.invalid/library"}, set_conn=False)

    assert drv.redacted_database_url == "postgresql://liuxin:***@example.invalid/library"
    assert drv.exists() is True
    assert drv.direct_get_tables() == ["database_metadata", "stores", "digital_assets"]
    assert drv.direct_get_tables_and_columns()["stores"] == ["store_id", "store_kind"]
    assert drv.direct_get_declared_types_for_table("digital_assets")["digital_asset_size_bytes"] == "bigint"
    assert drv.direct_get_declared_column_datatype("digital_assets", "digital_asset_size_bytes") == "bigint"
    assert drv._get_unique_column_groups("digital_assets") == (
        ("digital_asset_id", "digital_asset_size_bytes"),
    )
    with pytest.raises(InputIntegrityError, match="column"):
        drv.direct_get_declared_column_datatype("digital_assets", "missing_column")
    with pytest.raises(InputIntegrityError, match="table"):
        drv.direct_get_declared_column_datatype("missing_table", "missing_column")
    assert raw_connections
    assert raw_connections[0].closed is True
    assert any('set search_path to "public"' in cursor.sql for cursor in raw_connections[0].cursors)
    assert any(
        "index_info.indpred is null" in cursor.sql
        and "index_info.indexprs is null" in cursor.sql
        for connection in raw_connections
        for cursor in connection.cursors
    )


def test_postgresql_driver_inherits_link_capability_introspection(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import (
        databasedriver as pg_driver,
    )

    drv = pg_driver.DatabaseDriver(
        {"postgres_url": "postgresql://liuxin:secret@example.invalid/library"},
        set_conn=False,
    )
    catalog = {
        "agents": ["agent_id"],
        "works": ["work_id"],
        "agent_work_links": [
            "agent_work_link_agent_id",
            "agent_work_link_work_id",
            "agent_work_link_type",
            "agent_work_link_priority",
        ],
    }
    monkeypatch.setattr(
        drv,
        "direct_get_tables_and_columns",
        lambda force_refresh=False: catalog,
    )

    capabilities = drv.direct_get_link_capabilities("agents", "works")

    assert capabilities is not None
    assert capabilities.kind is LinkKind.TYPED_PRIORITY
    assert capabilities.link_table == "agent_work_links"
    assert drv.direct_is_link_typed("agents", "works") is True
    assert drv.direct_is_link_priority("agents", "works") is True


def test_postgresql_driver_basic_insert_and_update_sql(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import databasedriver as pg_driver

    drv = pg_driver.DatabaseDriver(
        {"postgres_url": "postgresql://liuxin:secret@example.invalid/library", "schema": "liuxin_test"},
        set_conn=False,
    )
    conn = _RecordingDriverConnection()
    drv.conn = conn
    monkeypatch.setattr(drv, "direct_identify_table_from_row", lambda row: "ratings")
    monkeypatch.setattr(drv, "direct_get_id_column", lambda table: "rating_id")
    monkeypatch.setattr(drv, "_zero_prop_cache", lambda: None)

    new_id = drv.direct_add_simple_row_dict({"rating": 4.5})
    updated = drv.direct_update_row_dict({"rating_id": 42, "rating": 5.0})

    assert new_id == 42
    assert updated is True
    assert any(
        'insert into "liuxin_test"."ratings" ("rating") values (%s) returning "rating_id"' in sql
        for sql, _ in conn.calls
    )
    assert any('update "liuxin_test"."ratings" set "rating" = %s where "rating_id" = %s' in sql for sql, _ in conn.calls)


def test_postgresql_driver_delete_sql_is_native_and_schema_qualified(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import databasedriver as pg_driver

    drv = pg_driver.DatabaseDriver(
        {"postgres_url": "postgresql://liuxin:secret@example.invalid/library", "schema": "liuxin_test"},
        set_conn=False,
    )
    conn = _RecordingDriverConnection()
    drv.conn = conn
    monkeypatch.setattr(drv, "_assert_existing_table", lambda table: None)
    monkeypatch.setattr(drv, "_assert_existing_column", lambda table, column: None)
    monkeypatch.setattr(drv, "direct_get_id_column", lambda table: "rating_id")
    monkeypatch.setattr(drv, "_zero_prop_cache", lambda: None)

    assert drv.direct_delete_row_by_id("ratings", 42) is True
    assert drv.direct_delete("ratings", "rating", 5.0) is True
    assert drv.direct_delete("ratings", "rating", None) is True
    assert drv.direct_delete_many("ratings", "rating", [1.0, 2.0]) is True
    assert drv.direct_delete_many_by_ids("ratings", [7, 8]) is True
    assert drv.direct_clear_table("ratings") is True

    assert (
        'delete from "liuxin_test"."ratings" where "rating_id" = %s',
        (42,),
    ) in conn.calls
    assert (
        'delete from "liuxin_test"."ratings" where "rating" = %s',
        (5.0,),
    ) in conn.calls
    assert (
        'delete from "liuxin_test"."ratings" where "rating" is null',
        None,
    ) in conn.calls
    assert (
        'delete from "liuxin_test"."ratings" where "rating" = %s',
        ((1.0,), (2.0,)),
    ) in conn.calls
    assert (
        'delete from "liuxin_test"."ratings" where "rating_id" = %s',
        ((7,), (8,)),
    ) in conn.calls
    assert ('delete from "liuxin_test"."ratings"', None) in conn.calls
    assert ('select count(*) from "liuxin_test"."ratings"', None) in conn.calls


def test_postgresql_driver_query_helpers_are_native_and_schema_qualified(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import databasedriver as pg_driver

    drv = pg_driver.DatabaseDriver(
        {"postgres_url": "postgresql://liuxin:secret@example.invalid/library", "schema": "liuxin_test"},
        set_conn=False,
    )
    conn = _RecordingDriverConnection()
    drv.conn = conn
    headings = ["rating_id", "rating_label", "rating"]
    declared = {"rating_id": "bigint", "rating_label": "text", "rating": "bigint"}
    monkeypatch.setattr(drv, "_short_connection", lambda: conn)
    monkeypatch.setattr(drv, "direct_get_column_headings", lambda table: headings)
    monkeypatch.setattr(drv, "direct_get_declared_types_for_table", lambda table: declared)
    monkeypatch.setattr(drv, "direct_identify_table_from_column", lambda column: "ratings")
    monkeypatch.setattr(drv, "direct_get_id_column", lambda table: "rating_id")
    monkeypatch.setattr(drv, "_assert_existing_column", lambda table, column: None)

    random_row = drv.direct_get_random_row_dict("ratings")
    unique_values = drv.direct_get_unique_values_set("rating_label")
    max_rating = drv.direct_get_max("rating")
    min_rating = drv.direct_get_min("rating")
    multi_rows = drv.direct_multi_column_search(
        [
            ("rating_label", "=", "alpha"),
            ("rating", "IN", [1, 2]),
        ]
    )
    iter_rows = list(drv.direct_get_row_dict_iterator("ratings"))

    assert random_row == {"rating_id": 9, "rating_label": "random-label", "rating": 90}
    assert unique_values == {"alpha", "beta"}
    assert max_rating == 90
    assert min_rating == 1
    assert multi_rows == [{"rating_id": 3, "rating_label": "alpha", "rating": 1}]
    assert iter_rows == [
        {"rating_id": 1, "rating_label": "iter-one", "rating": 10},
        {"rating_id": 2, "rating_label": "iter-two", "rating": 20},
    ]

    assert ('select * from "liuxin_test"."ratings" order by random() limit 1', None) in conn.calls
    assert ('select distinct "rating_label" from "liuxin_test"."ratings"', None) in conn.calls
    assert ('select max("rating") from "liuxin_test"."ratings"', None) in conn.calls
    assert ('select min("rating") from "liuxin_test"."ratings"', None) in conn.calls
    assert any(
        sql == 'select * from "liuxin_test"."ratings" where "rating_label" = %s and "rating" in (%s, %s)'
        and values == ("alpha", 1, 2)
        for sql, values in conn.calls
    )
    assert any(
        sql == 'select * from "liuxin_test"."ratings" where "rating_id" > %s order by "rating_id" limit 10'
        and values == (0,)
        for sql, values in conn.calls
    )


def test_postgresql_column_case_sensitivity_uses_schema_catalog(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import databasedriver as pg_driver

    drv = pg_driver.DatabaseDriver(
        {
            "postgres_url": "postgresql://liuxin:secret@example.invalid/library",
            "schema": "liuxin_test",
        },
        set_conn=False,
    )
    conn = _RecordingDriverConnection()
    drv.conn = conn
    monkeypatch.setattr(drv, "_short_connection", lambda: conn)
    monkeypatch.setattr(drv, "direct_get_tables", lambda force_refresh=False: ["works", "column_metadata"])
    metadata_columns = [
        "column_metadata_formatting_options_json",
        "column_metadata_display_options_json",
    ]
    monkeypatch.setattr(
        drv,
        "direct_get_column_headings",
        lambda table, normalize=False: (
            metadata_columns if table == "column_metadata" else ["work_title"]
        ),
    )

    metadata = drv.direct_get_column_metadata("works", "work_title")
    assert metadata.case_sensitive is True
    assert metadata.semantic_role is ColumnSemanticRole.TITLE
    assert (
        metadata.normalization_profile
        is ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD
    )
    assert drv.direct_get_case_sensitivity("works", "work_title") is True
    assert drv.direct_is_column_case_sensitive("works", "work_title") is True
    assert (
        drv.direct_get_semantic_role("works", "work_title")
        is ColumnSemanticRole.TITLE
    )
    assert (
        drv.direct_get_normalization_profile("works", "work_title")
        is ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD
    )
    assert drv.direct_get_comparison_column("works", "work_title") is None
    assert (
        drv.direct_get_empty_value_policy("works", "work_title")
        is ColumnEmptyValuePolicy.NULL_OR_BLANK_IS_MISSING
    )
    assert (
        drv.direct_get_merge_policy("works", "work_title")
        is ColumnMergePolicy.REPLACE
    )
    assert (
        drv.direct_get_validation_profile("works", "work_title")
        is ColumnValidationProfile.DISPLAY_TEXT
    )
    assert drv.direct_get_formatting_options("works", "work_title") == {}
    assert drv.direct_get_display_options("works", "work_title") == {}
    drv.direct_set_column_metadata(metadata)
    drv.direct_set_case_sensitivity("works", "work_title", False)

    assert any(
        'from "liuxin_test"."column_metadata"' in sql
        and values == ("works", "work_title")
        for sql, values in conn.calls
    )
    assert any(
        'insert into "liuxin_test"."column_metadata"' in sql
        and values
        == (
            "works",
            "work_title",
            1,
            "title",
            "unicode_nfc_trim_casefold",
            None,
            "null_or_blank_is_missing",
            "replace",
            "display_text",
            "{}",
            "{}",
        )
        for sql, values in conn.calls
    )
    assert any(
        'insert into "liuxin_test"."column_metadata"' in sql
        and values == ("works", "work_title", 0)
        for sql, values in conn.calls
    )


def test_postgresql_individual_column_metadata_setters_use_full_record_upsert(
    monkeypatch,
) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import databasedriver as pg_driver

    drv = pg_driver.DatabaseDriver(
        {
            "postgres_url": "postgresql://liuxin:secret@example.invalid/library",
            "schema": "liuxin_test",
        },
        set_conn=False,
    )
    conn = _RecordingDriverConnection()
    drv.conn = conn
    monkeypatch.setattr(drv, "_short_connection", lambda: conn)
    monkeypatch.setattr(
        drv,
        "direct_get_tables",
        lambda force_refresh=False: ["works", "column_metadata"],
    )
    metadata_columns = [
        "column_metadata_formatting_options_json",
        "column_metadata_display_options_json",
    ]
    monkeypatch.setattr(
        drv,
        "direct_get_column_headings",
        lambda table, normalize=False: (
            metadata_columns if table == "column_metadata" else ["work_title"]
        ),
    )

    base_values = [
        "works",
        "work_title",
        1,
        "title",
        "unicode_nfc_trim_casefold",
        None,
        "null_or_blank_is_missing",
        "replace",
        "display_text",
        "{}",
        "{}",
    ]
    cases = (
        ("direct_set_semantic_role", ColumnSemanticRole.LABEL, 3, "label"),
        (
            "direct_set_normalization_profile",
            ColumnNormalizationProfile.UNICODE_NFC,
            4,
            "unicode_nfc",
        ),
        ("direct_set_comparison_column", "work_title", 5, "work_title"),
        (
            "direct_set_empty_value_policy",
            ColumnEmptyValuePolicy.PRESERVE,
            6,
            "preserve",
        ),
        (
            "direct_set_merge_policy",
            ColumnMergePolicy.PRESERVE_EXISTING,
            7,
            "preserve_existing",
        ),
        (
            "direct_set_validation_profile",
            ColumnValidationProfile.VERBATIM_TEXT,
            8,
            "verbatim_text",
        ),
        (
            "direct_set_formatting_options",
            {"number_format": "0.00", "empty_value": "—"},
            9,
            '{"empty_value":"—","number_format":"0.00"}',
        ),
        (
            "direct_set_display_options",
            {"label": "Title", "visible": True},
            10,
            '{"label":"Title","visible":true}',
        ),
    )

    for method_name, value, value_index, expected_db_value in cases:
        getattr(drv, method_name)("works", "work_title", value)
        insert_sql, insert_values = conn.calls[-1]
        assert 'insert into "liuxin_test"."column_metadata"' in insert_sql
        expected_values = list(base_values)
        expected_values[value_index] = expected_db_value
        assert insert_values == tuple(expected_values)


def test_postgresql_driver_creates_main_tables_with_native_ddl(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import databasedriver as pg_driver

    drv = pg_driver.DatabaseDriver(
        {"postgres_url": "postgresql://liuxin:secret@example.invalid/library", "schema": "liuxin_test"},
        set_conn=False,
    )
    conn = _RecordingDriverConnection()
    drv.conn = conn
    monkeypatch.setattr(drv, "_zero_prop_cache", lambda: None)

    drv.direct_create_main_table(
        "contract_pg_books",
        column_headings={"title": {"datatype": "TEXT"}, "pages": {"datatype": "INTEGER"}},
        index_on=["title"],
    )

    create_sql = next(sql for sql, _ in conn.calls if sql.startswith("create table if not exists"))
    assert 'create table if not exists "liuxin_test"."contract_pg_books"' in create_sql
    assert '"contract_pg_book_id" bigserial primary key' in create_sql
    assert '"contract_pg_book_title" text null' in create_sql
    assert '"contract_pg_book_pages" bigint null' in create_sql
    assert '"contract_pg_book_datestamp" timestamp with time zone default current_timestamp' in create_sql
    assert (
        'create index if not exists "contract_pg_books_contract_pg_book_title_index" '
        'on "liuxin_test"."contract_pg_books" ("contract_pg_book_title")',
        None,
    ) in conn.calls


def test_postgresql_driver_links_and_unlinks_main_tables_with_native_ddl(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import databasedriver as pg_driver

    drv = pg_driver.DatabaseDriver(
        {"postgres_url": "postgresql://liuxin:secret@example.invalid/library", "schema": "liuxin_test"},
        set_conn=False,
    )
    conn = _RecordingDriverConnection()
    drv.conn = conn
    monkeypatch.setattr(drv, "_assert_existing_table", lambda table: None)
    monkeypatch.setattr(drv, "direct_get_id_column", lambda table: f"{table[:-1]}_id")
    monkeypatch.setattr(drv, "_zero_prop_cache", lambda: None)

    link_table = drv.direct_link_main_tables("contract_pg_lefts", "contract_pg_rights", requested_cols="all")
    drv.direct_unlink_main_tables("contract_pg_lefts", "contract_pg_rights")

    assert link_table == "contract_pg_left_contract_pg_right_links"
    create_sql = next(
        sql for sql, _ in conn.calls
        if sql.startswith('create table if not exists "liuxin_test"."contract_pg_left_contract_pg_right_links"')
    )
    assert '"contract_pg_left_contract_pg_right_link_id" bigserial primary key' in create_sql
    assert (
        '"contract_pg_left_contract_pg_right_link_contract_pg_left_id" bigint null '
        'references "liuxin_test"."contract_pg_lefts" ("contract_pg_left_id") '
        'on delete cascade on update cascade'
    ) in create_sql
    assert (
        '"contract_pg_left_contract_pg_right_link_contract_pg_right_id" bigint null '
        'references "liuxin_test"."contract_pg_rights" ("contract_pg_right_id") '
        'on delete cascade on update cascade'
    ) in create_sql
    assert "unique" in create_sql.lower()
    assert any(
        sql.startswith('create index if not exists "contract_pg_left_contract_pg_right_link_contract_pg_left_id_index"')
        for sql, _ in conn.calls
    )
    assert (
        'drop table if exists "liuxin_test"."contract_pg_left_contract_pg_right_links" cascade',
        None,
    ) in conn.calls


def test_postgresql_driver_view_helpers_are_native_and_schema_qualified(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import databasedriver as pg_driver

    drv = pg_driver.DatabaseDriver(
        {"postgres_url": "postgresql://liuxin:secret@example.invalid/library", "schema": "liuxin_test"},
        set_conn=False,
    )
    conn = _RecordingDriverConnection()
    drv.conn = conn
    monkeypatch.setattr(drv, "_short_connection", lambda: conn)
    monkeypatch.setattr(drv, "direct_get_relation_type", lambda name: "view")
    monkeypatch.setattr(
        drv,
        "direct_get_declared_types_for_table",
        lambda table: {"id": "bigint", "label": "text", "rating": "bigint"},
    )

    headings = drv.direct_get_view_column_headings("rating_view")
    row = drv.direct_get_view_row_dict_from_id("rating_view", 3)

    assert headings == ["id", "label", "rating"]
    assert row == {"id": 3, "label": "alpha", "rating": 1}
    assert any(
        "from information_schema.columns" in sql.lower()
        and values == ("liuxin_test", "rating_view")
        for sql, values in conn.calls
    )
    assert (
        'select * from "liuxin_test"."rating_view" where "id" = %s',
        (3,),
    ) in conn.calls


def test_postgresql_driver_trigger_helpers_are_native_and_schema_qualified(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import databasedriver as pg_driver

    drv = pg_driver.DatabaseDriver(
        {"postgres_url": "postgresql://liuxin:secret@example.invalid/library", "schema": "liuxin_test"},
        set_conn=False,
    )
    conn = _RecordingDriverConnection()
    drv.conn = conn
    monkeypatch.setattr(drv, "_short_connection", lambda: conn)
    monkeypatch.setattr(drv, "_zero_prop_cache", lambda: None)

    triggers = drv.direct_get_triggers()
    dropped = drv.direct_drop_triggers(["contract_rating_trigger"])

    assert triggers == ["contract_rating_trigger"]
    assert dropped is True
    assert any(
        "select distinct trigger_name" in sql.lower()
        and "from information_schema.triggers" in sql.lower()
        and values == ("liuxin_test",)
        for sql, values in conn.calls
    )
    assert any(
        "select distinct event_object_table" in sql.lower()
        and "from information_schema.triggers" in sql.lower()
        and values == ("liuxin_test", "contract_rating_trigger")
        for sql, values in conn.calls
    )
    assert (
        'drop trigger if exists "contract_rating_trigger" on "liuxin_test"."ratings" cascade',
        None,
    ) in conn.calls


def _complete_catalog() -> dict[str, dict[str, str]]:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.checker import (
        HELPER_REQUIRED_TABLES,
        LIUXIN_POSTGRES_REQUIRED_COLUMNS,
    )

    catalog: dict[str, dict[str, str]] = {}
    for table, columns in LIUXIN_POSTGRES_REQUIRED_COLUMNS.items():
        catalog[table] = {column: "text" for column in columns}
    for table in HELPER_REQUIRED_TABLES:
        catalog.setdefault(table, {"id": "bigint"})
    catalog["digital_assets"]["digital_asset_size_bytes"] = "bigint"
    catalog["asset_replicas"]["asset_replica_observed_size_bytes"] = "bigint"
    return catalog


class _FakeCheckerCursor:
    def __init__(self, catalog: dict[str, dict[str, str]], missing_privileges: dict[str, set[str]] | None = None):
        self.catalog = catalog
        self.missing_privileges = missing_privileges or {}
        self.rows: list[dict[str, Any]] = []
        self.information_schema_schemas: list[str] = []
        self.privilege_relations: list[str] = []
        self.count_queries: list[str] = []
        self.table_regclasses: list[str] = []

    def execute(self, sql: str, values: tuple[Any, ...] | None = None):
        lowered = " ".join(sql.lower().split())
        values = values or ()
        if lowered.startswith("set local"):
            self.rows = []
        elif "has_table_privilege" in lowered:
            relation = str(values[0])
            self.privilege_relations.append(relation)
            table = _fake_relation_table_name(relation)
            privilege = str(values[1]).upper()
            self.rows = [{"ok": privilege not in self.missing_privileges.get(table, set())}]
        elif "current_database()" in lowered:
            self.rows = [{"current_database": "liuxin"}]
        elif "current_user" in lowered:
            self.rows = [{"current_user": "liuxin_runtime"}]
        elif "to_regclass" in lowered:
            relation = str(values[0])
            self.table_regclasses.append(relation)
            table = _fake_relation_table_name(relation)
            self.rows = [{"exists": table in self.catalog}]
        elif "from information_schema.columns" in lowered and "data_type" in lowered:
            self.information_schema_schemas.append(str(values[0]))
            table = str(values[1])
            self.rows = [
                {"column_name": column, "data_type": data_type}
                for column, data_type in self.catalog.get(table, {}).items()
            ]
        elif "from information_schema.columns" in lowered:
            self.information_schema_schemas.append(str(values[0]))
            table = str(values[1])
            self.rows = [{"column_name": column} for column in self.catalog.get(table, {})]
        elif lowered.startswith("select count(*)"):
            self.count_queries.append(sql)
            self.rows = [{"count": 0}]
        else:
            self.rows = []
        return self

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _fake_relation_table_name(relation: str) -> str:
    return relation.split(".")[-1].strip('"')


class _FakeCheckerConnection:
    def __init__(self, cursor: _FakeCheckerCursor):
        self.cursor_obj = cursor
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_postgresql_checker_passes_complete_schema(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import checker

    cursor = _FakeCheckerCursor(_complete_catalog())
    conn = _FakeCheckerConnection(cursor)

    monkeypatch.setattr(checker.importlib.util, "find_spec", lambda name: object())
    monkeypatch.setattr(checker, "connect_postgres", lambda *args, **kwargs: conn)
    monkeypatch.setattr(checker, "postgres_cursor", lambda connection: cursor)

    result = checker.run_postgres_self_test(postgres_url="postgresql://liuxin:secret@example.invalid/library")

    assert result["ok"] is True
    assert result["url"] == "postgresql://liuxin:***@example.invalid/library"
    assert conn.closed is True


def test_postgresql_checker_honors_configured_schema(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import checker

    cursor = _FakeCheckerCursor(_complete_catalog())
    conn = _FakeCheckerConnection(cursor)

    monkeypatch.setattr(checker.importlib.util, "find_spec", lambda name: object())
    monkeypatch.setattr(checker, "connect_postgres", lambda *args, **kwargs: conn)
    monkeypatch.setattr(checker, "postgres_cursor", lambda connection: cursor)

    result = checker.run_postgres_self_test(
        metadata={"schema": "liuxin_test"},
        postgres_url="postgresql://liuxin:secret@example.invalid/library",
    )
    report = checker.format_postgres_self_test(result)

    assert result["ok"] is True
    assert result["schema"] == "liuxin_test"
    assert "Schema: liuxin_test" in report
    assert cursor.table_regclasses
    assert all(value.startswith("liuxin_test.") for value in cursor.table_regclasses)
    assert cursor.information_schema_schemas
    assert set(cursor.information_schema_schemas) == {"liuxin_test"}
    assert cursor.privilege_relations
    assert all(value.startswith('"liuxin_test".') for value in cursor.privilege_relations)
    assert cursor.count_queries
    assert all('from "liuxin_test".' in " ".join(value.split()).casefold() for value in cursor.count_queries)


def test_postgresql_checker_reports_missing_driver(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import checker

    monkeypatch.setattr(checker.importlib.util, "find_spec", lambda name: None)
    result = checker.run_postgres_self_test(postgres_url="postgresql://liuxin:secret@example.invalid/library")

    assert result["ok"] is False
    assert any(check["name"] == "driver" and not check["ok"] for check in result["checks"])
    report = checker.format_postgres_self_test(result)
    assert "secret" not in report
    assert ".[postgres]" in report


def test_postgresql_checker_reports_schema_type_and_privilege_failures(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import checker

    catalog = _complete_catalog()
    catalog["asset_replicas"].pop("asset_replica_storage_key")
    catalog["digital_assets"]["digital_asset_size_bytes"] = "integer"
    cursor = _FakeCheckerCursor(catalog, missing_privileges={"stores": {"UPDATE"}})
    conn = _FakeCheckerConnection(cursor)

    monkeypatch.setattr(checker.importlib.util, "find_spec", lambda name: object())
    monkeypatch.setattr(checker, "connect_postgres", lambda *args, **kwargs: conn)
    monkeypatch.setattr(checker, "postgres_cursor", lambda connection: cursor)

    result = checker.run_postgres_self_test(postgres_url="postgresql://liuxin:secret@example.invalid/library")
    report = checker.format_postgres_self_test(result)

    assert result["ok"] is False
    assert "asset_replica_storage_key" in report
    assert "digital_asset_size_bytes: integer expected bigint" in report
    assert "stores: UPDATE" in report
    assert "secret" not in report


def test_postgresql_checker_reports_missing_role(monkeypatch) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import checker

    monkeypatch.setattr(checker.importlib.util, "find_spec", lambda name: object())

    def fail_connect(*args, **kwargs):
        raise RuntimeError('connection failed for postgresql://liuxin:secret@example.invalid/library: role "liuxin_missing" does not exist')

    monkeypatch.setattr(checker, "connect_postgres", fail_connect)

    result = checker.run_postgres_self_test(postgres_url="postgresql://liuxin:secret@example.invalid/library")
    report = checker.format_postgres_self_test(result)

    assert result["ok"] is False
    assert "liuxin_missing" in report
    assert "secret" not in report
