"""
Check PostgreSQL driver registration, configuration, SQL generation, metadata policies, schema setup, and checker behavior with test doubles.

Connections and query results are simulated; passing these unit checks does not
establish behavior against a live PostgreSQL server. Fakes often accept unsupported
SQL and return fixed rows, and context-manager exits do not implement transaction
semantics.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py
"""
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
    """
    Check PostgreSQL is registered, pg resolves to the same driver class, and postgres resolves to a DatabaseDriver class.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_driver_is_registered


    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.database_driver_plugins.registry import (
        get_registered_database_driver_names,
        load_database_driver,
    )

    assert "PostgreSQL" in get_registered_database_driver_names()
    assert load_database_driver("PostgreSQL") is load_database_driver("pg")
    assert load_database_driver("postgres").__name__ == "DatabaseDriver"


def test_postgresql_driver_exposes_shared_column_base_contract() -> None:
    """
    Check ratings and digital_assets use the expected singular column bases.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_driver_exposes_shared_column_base_contract


    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.databasedriver import (
        DatabaseDriver,
    )

    assert DatabaseDriver.direct_get_column_base("ratings") == "rating"
    assert DatabaseDriver.direct_get_column_base("digital_assets") == (
        "digital_asset"
    )


def test_postgresql_driver_import_does_not_require_psycopg2() -> None:
    """
    Import the PostgreSQL driver module and check DatabaseDriver exists.

    This test does not itself remove or block psycopg2, so it does not independently
    prove optional-driver absence behavior.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_driver_import_does_not_require_psycopg2


    :return: None; failed expectations raise AssertionError.
    """
    mod = importlib.import_module("LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.databasedriver")

    assert mod.DatabaseDriver is not None


def test_postgresql_url_redaction() -> None:
    """
    Check URL redaction hides the password and sslpassword while preserving the username and application_name parameter.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_url_redaction


    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.config import redact_postgres_url

    url = "postgresql://liuxin:secret@example.invalid:5432/library?sslpassword=hidden&application_name=lx"

    redacted = redact_postgres_url(url)

    assert "secret" not in redacted
    assert "hidden" not in redacted
    assert "liuxin:***@" in redacted
    assert "sslpassword=%2A%2A%2A" in redacted
    assert "application_name=lx" in redacted


def test_postgresql_schema_configuration_prefers_explicit_metadata_env(monkeypatch) -> None:
    """
    Check schema selection uses explicit input before metadata and metadata before the environment.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_schema_configuration_prefers_explicit_metadata_env


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.config import configured_postgres_schema

    monkeypatch.setenv("LIUXIN_POSTGRES_SCHEMA", "env_schema")

    assert configured_postgres_schema() == "env_schema"
    assert configured_postgres_schema({"schema": "metadata_schema"}) == "metadata_schema"
    assert configured_postgres_schema({"schema": "metadata_schema"}, explicit="explicit_schema") == "explicit_schema"


def test_postgresql_service_target_configuration_prefers_explicit_metadata_env(monkeypatch) -> None:
    """
    Check service selection precedence and the kind, value, and label of a metadata-selected service target.

    Set both service environment variables and clear URL overrides for this test.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_service_target_configuration_prefers_explicit_metadata_env


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Substitute a recording psycopg2 connector and check the exact service, password, timeout, and application-name arguments.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_connect_uses_native_service_profile


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Block psycopg2 import and check connection setup raises PostgresConnectionError with installation guidance.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_connect_hints_when_python_driver_is_missing


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Check formatted errors hide a URL password and include targeted guidance for a missing database, unavailable server, and missing service profile.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_errors_hint_for_missing_database_and_unavailable_server


    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Use fake connection/cursor objects to check operation return, connection closure, configured search_path, and schema-qualified table lookup.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_shared_connection_helper_uses_configured_schema


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import connection as pg_connection

    class FakeCursor:
        """
        Record SQL calls and always report a successful table-existence result.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_shared_connection_helper_uses_configured_schema
        """
        def __init__(self) -> None:
            """
            Initialize an empty per-cursor SQL/bindings call log.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_shared_connection_helper_uses_configured_schema


            :return: None.
            """
            self.calls: list[tuple[str, tuple[Any, ...] | None]] = []

        def execute(self, sql: str, values=None):
            """
            Append SQL and bindings to the call log without copying or executing them.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_shared_connection_helper_uses_configured_schema


            :param sql: SQL string recorded by the fake; no database executes it.
            :param values: Bindings recorded without SQL validation.
            :return: This FakeCursor for chained calls.
            """
            self.calls.append((sql, values))
            return self

        def fetchone(self):
            """
            Return the fixed exists=True mapping regardless of recorded SQL.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_shared_connection_helper_uses_configured_schema


            :return: Fresh dictionary reporting existence; no cursor position is maintained.
            """
            return {"exists": True}

        def __enter__(self):
            """
            Return the fake cursor on context entry without acquiring resources.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_shared_connection_helper_uses_configured_schema


            :return: This FakeCursor.
            """
            return self

        def __exit__(self, exc_type, exc, tb):
            """
            Leave fake cursor state unchanged and allow any context-body exception to propagate.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_shared_connection_helper_uses_configured_schema


            :param exc_type: Exception type supplied by context-manager exit, ignored here.
            :param exc: Exception instance supplied by context-manager exit, ignored here.
            :param tb: Traceback supplied by context-manager exit, ignored here.
            :return: False; no cleanup is performed.
            """
            return False

    class FakeConnection:
        """
        Expose a fixed cursor attribute and a close flag for the shared-connection helper test.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_shared_connection_helper_uses_configured_schema
        """
        def __init__(self) -> None:
            """
            Initialize an open-state flag and one FakeCursor stored as an attribute.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_shared_connection_helper_uses_configured_schema


            :return: None; cursor is an object, not a cursor factory.
            """
            self.closed = False
            self.cursor = FakeCursor()

        def __enter__(self):
            """
            Return the fake connection on context entry without changing state.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_shared_connection_helper_uses_configured_schema


            :return: This FakeConnection.
            """
            return self

        def __exit__(self, exc_type, exc, tb):
            """
            Allow context-body exceptions to propagate without closing or transacting.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_shared_connection_helper_uses_configured_schema


            :param exc_type: Exception type supplied by context-manager exit, ignored here.
            :param exc: Exception instance supplied by context-manager exit, ignored here.
            :param tb: Traceback supplied by context-manager exit, ignored here.
            :return: False; the closed flag is unchanged.
            """
            return False

        def close(self) -> None:
            """
            Set the fake closed flag without enforcing it on later operations.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_shared_connection_helper_uses_configured_schema


            :return: None; sets closed to True.
            """
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
    """
    Check backtick identifiers and an unquoted question-mark placeholder are translated while a quoted question mark remains literal.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_sql_translation_is_small_and_explicit


    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import translate_sql_for_postgres

    translated = translate_sql_for_postgres("insert into `asset_replicas` (`asset_replica_storage_key`) values (?, '?')")

    assert translated == 'insert into "asset_replicas" ("asset_replica_storage_key") values (%s, \'?\')'


def test_database_init_classifies_postgres_as_server_backend() -> None:
    """
    Check PostgreSQL URL/service metadata is recognized as server-backed and selected SQLite/local metadata remains classified as local.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_database_init_classifies_postgres_as_server_backend


    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.database import _metadata_uses_server_database

    assert _metadata_uses_server_database({"database_path": "postgresql://liuxin@example.invalid/library"}, "SQLite")
    assert _metadata_uses_server_database({"postgres_url": "postgres://liuxin@example.invalid/library"}, "pg")
    assert _metadata_uses_server_database({"postgres_service": "liuxin_runtime"}, "SQLite")
    assert _metadata_uses_server_database({"database_service": "liuxin_runtime"}, "SQLite")
    assert _metadata_uses_server_database({"service": "liuxin_runtime"}, "PostgreSQL")
    assert not _metadata_uses_server_database({"database_path": "/tmp/liuxin.db"}, "SQLite")
    assert not _metadata_uses_server_database({"service": "unrelated"}, "SQLite")


def test_postgresql_schema_catalog_satisfies_checker_contract() -> None:
    """
    Check the schema catalog includes required checker tables/columns, custom-column labels, metadata policy/options fields, and workflow step codes.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_schema_catalog_satisfies_checker_contract


    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Record schema-builder SQL strings and return a fixed one-row cursor without applying DDL.

    Example:
        >>> connection = _RecordingSchemaConnection()
        >>> connection.execute('DDL').fetchone()
        (1,)
        >>> connection.statements
        ['DDL']
    """
    def __init__(self) -> None:
        """
        Initialize an empty SQL-statement list.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: None.
        """
        self.statements: list[str] = []

    def execute(self, sql: str, values=None):
        """
        Record the SQL string, ignore bindings, and create a cursor containing the tuple (1,).

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :param sql: SQL string recorded by the fake; no database executes it.
        :param values: Ignored bindings accepted for connection-call compatibility.
        :return: New _ResultCursor with one fixed row.
        """
        self.statements.append(sql)
        return _ResultCursor([(1,)])

    def __enter__(self):
        """
        Return the recorder on context entry without transaction setup.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: This _RecordingSchemaConnection.
        """
        return self

    def __exit__(self, exc_type, exc, tb):
        """
        Propagate context-body exceptions without transaction or cleanup behavior.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :param exc_type: Exception type supplied by context-manager exit, ignored here.
        :param exc: Exception instance supplied by context-manager exit, ignored here.
        :param tb: Traceback supplied by context-manager exit, ignored here.
        :return: False; recorder state remains unchanged.
        """
        return False


def test_postgresql_schema_builder_executes_core_and_storage_tables() -> None:
    """
    Record generated DDL and check selected core/storage tables, dependency ordering, foreign-key policy, seeded metadata values, and one metadata insert per catalog column.

    Statements are recorded by a fake connection and are not executed on PostgreSQL.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_schema_builder_executes_core_and_storage_tables


    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Patch connection/schema creation and check the entrypoint forwards the configured schema and closes the raw connection.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_schema_builder_entrypoint_uses_metadata_schema


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import schema as pg_schema

    class FakeRawConnection:
        """
        Track whether the schema-creation entrypoint closes its fake raw connection.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_schema_builder_entrypoint_uses_metadata_schema
        """
        def __init__(self) -> None:
            """
            Initialize the fake connection as not closed.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_schema_builder_entrypoint_uses_metadata_schema


            :return: None.
            """
            self.closed = False

        def close(self) -> None:
            """
            Mark the fake raw connection closed.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_schema_builder_entrypoint_uses_metadata_schema


            :return: None; sets closed to True.
            """
            self.closed = True

    raw = FakeRawConnection()
    calls: list[tuple[object, str]] = []

    monkeypatch.setattr(pg_schema, "connect_postgres", lambda metadata, url: raw)

    def fake_create(conn, *, schema: str):
        """
        Record the connection and requested schema instead of creating database objects.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_schema_builder_entrypoint_uses_metadata_schema


        :param conn: Connection wrapper supplied by the schema entrypoint.
        :param schema: Schema string forwarded by the entrypoint.
        :return: None; appends one tuple to the enclosing calls list.
        """
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
    """
    Check generated grants/setup sections include expected statements and reject selected invalid role names and section values.

    Inspect SQL strings only; no roles, databases, schemas, or privileges are changed.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_runtime_grant_statements_validate_identifiers


    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Remember the last SQL call and return fixed introspection rows selected by substring checks.

    The fake does not execute SQL, advance a result position, or enforce closure.

    Example:
        >>> cursor = _FakeDriverCursor()
        >>> cursor.execute('SELECT 1').fetchone()
        (1,)
        >>> cursor.fetchone()
        (1,)
    """
    def __init__(self) -> None:
        """
        Initialize empty SQL text and None bindings.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: None.
        """
        self.sql = ""
        self.values: tuple[Any, ...] | None = None

    def execute(self, sql: str, values: tuple[Any, ...] | None = None):
        """
        Replace the remembered SQL and bindings without executing or copying the bindings.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :param sql: SQL string recorded by the fake; no database executes it.
        :param values: Bindings recorded without SQL validation.
        :return: This cursor for chained calls.
        """
        self.sql = sql
        self.values = values
        return self

    def executemany(self, sql: str, values):
        """
        Replace remembered SQL and materialize the outer bindings iterable as a tuple.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :param sql: SQL string recorded by the fake; no database executes it.
        :param values: Iterable consumed once into the recorded tuple.
        :return: This cursor; inner binding objects are not copied.
        """
        self.sql = sql
        self.values = tuple(values)
        return self

    def fetchone(self):
        """
        Return a fixed SELECT 1 or schema-fingerprint row based on the last SQL string.

        SELECT 1 matching takes precedence; repeated fetches do not consume the result.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: Tuple (1,), tuple containing fingerprint, or None for other SQL.
        """
        lowered = self.sql.lower()
        if "select 1" in lowered:
            return (1,)
        if "schema_fingerprint" in lowered:
            return ("fingerprint",)
        return None

    def fetchall(self):
        """
        Return fixed index, table, column, or datatype rows based on the last SQL string.

        Datatype lookup expects the table name in values[1] and only supplies digital_assets
        types. Unsupported SQL returns an empty list; malformed expected bindings can raise.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: Fresh list of simulated introspection rows; no result position is advanced.
        """
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
        """
        Accept a close call without changing state or disabling later operations.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: None.
        """
        pass

    def __iter__(self):
        """
        Iterate a newly generated fetchall result for the last SQL string.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: New iterator over the simulated rows; repeated iteration starts again.
        """
        return iter(self.fetchall())


class _FakeDriverConnection:
    """
    Create independent recording cursors and track explicit close calls without real connection or transaction behavior.

    Example:
        >>> connection = _FakeDriverConnection()
        >>> cursor = connection.cursor()
        >>> len(connection.cursors), connection.closed
        (1, False)
        >>> connection.close()
        >>> connection.closed
        True
    """
    def __init__(self) -> None:
        """
        Initialize a false closed flag and an empty list of created cursors.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: None.
        """
        self.closed = False
        self.cursors: list[_FakeDriverCursor] = []

    def cursor(self, *args, **kwargs):
        """
        Create and record a new fake cursor, ignoring factory options and the closed flag.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :param args: Ignored positional cursor-factory options.
        :param kwargs: Ignored keyword cursor-factory options.
        :return: New _FakeDriverCursor.
        """
        cur = _FakeDriverCursor()
        self.cursors.append(cur)
        return cur

    def commit(self) -> None:
        """
        Accept commit without changing fake state or persisting anything.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: None.
        """
        pass

    def rollback(self) -> None:
        """
        Accept rollback without undoing recorded calls or changing fake state.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: None.
        """
        pass

    def close(self) -> None:
        """
        Set the closed flag while leaving existing cursors and later factory calls usable.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: None.
        """
        self.closed = True

    def __enter__(self):
        """
        Return the fake connection without changing its closed flag.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: This _FakeDriverConnection.
        """
        return self

    def __exit__(self, exc_type, exc, tb):
        """
        Allow context-body exceptions to propagate without commit, rollback, or close.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :param exc_type: Exception type supplied by context-manager exit, ignored here.
        :param exc: Exception instance supplied by context-manager exit, ignored here.
        :param tb: Traceback supplied by context-manager exit, ignored here.
        :return: False; connection state is unchanged.
        """
        return False


class _ResultCursor:
    """
    Provide repeatable, non-consuming access to a materialized row list and a None lastrowid.

    Example:
        >>> cursor = _ResultCursor([(1,), (2,)])
        >>> cursor.fetchone(), cursor.fetchone()
        ((1,), (1,))
        >>> cursor.fetchall()
        [(1,), (2,)]
    """
    def __init__(self, rows):
        """
        Materialize the supplied rows as a list and initialize lastrowid to None.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :param rows: Iterable of rows consumed once into the cursor’s list.
        :return: None; nested row objects are retained by reference.
        """
        self.rows = list(rows)
        self.lastrowid = None

    def fetchone(self):
        """
        Return the first stored row without removing it or advancing a position.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: First row object, or None when the row list is empty.
        """
        return self.rows[0] if self.rows else None

    def fetchall(self):
        """
        Return a shallow list copy of all stored rows on every call.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: New list retaining references to the stored row objects.
        """
        return list(self.rows)

    def __iter__(self):
        """
        Start a new iterator over the stored row list.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: Iterator beginning at the first row; independent of previous fetches.
        """
        return iter(self.rows)


class _RecordingDriverConnection:
    """
    Record SQL calls and return fixed cursors for recognized metadata, trigger, query, and write patterns.

    Responses are substring-driven and do not model stored state, SQL validation,
    transactions, or resource closure.

    Example:
        >>> connection = _RecordingDriverConnection()
        >>> connection.execute('select count(*) from ratings').fetchone()
        (0,)
        >>> connection.calls
        [('select count(*) from ratings', None)]
    """
    def __init__(self) -> None:
        """
        Initialize an empty SQL/bindings call log.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: None.
        """
        self.calls: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, values=None):
        """
        Log a SQL call and dispatch fixed rows by ordered lowercase substring checks.

        Metadata, view-column, and trigger patterns precede RETURNING,
        count/random/distinct/extrema, ID pagination, and generic WHERE handling. Pagination
        reads the first binding and ends at a starting ID of two; unrecognized SQL gets the
        tuple (1,). No statement is executed.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :param sql: SQL string recorded by the fake; no database executes it.
        :param values: Bindings recorded without SQL validation.
        :return: New _ResultCursor for the matched branch; original bindings are retained in
            the call log.
        """
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
        """
        Record SQL with an outer tuple copy of all binding groups and return an empty result cursor.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :param sql: SQL string recorded by the fake; no database executes it.
        :param values: Iterable of binding groups consumed once; inner objects are retained.
        :return: New _ResultCursor with no rows; no database writes occur.
        """
        self.calls.append((sql, tuple(values)))
        return _ResultCursor([])

    def close(self) -> None:
        """
        Accept close without changing state or clearing the call log.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: None.
        """
        pass

    def __enter__(self):
        """
        Return the recorder on context entry without acquiring resources.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: This _RecordingDriverConnection.
        """
        return self

    def __exit__(self, exc_type, exc, tb):
        """
        Propagate context-body exceptions without commit, rollback, or cleanup.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :param exc_type: Exception type supplied by context-manager exit, ignored here.
        :param exc: Exception instance supplied by context-manager exit, ignored here.
        :param tb: Traceback supplied by context-manager exit, ignored here.
        :return: False; the call log is retained.
        """
        return False


def test_postgresql_driver_connects_and_introspects(monkeypatch) -> None:
    """
    Use fake connections to check redaction, existence, table/column/type/index introspection, invalid-name errors, schema setup, and first-connection closure.

    Also require index-query filters excluding partial and expression indexes; no real
    server is contacted.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_driver_connects_and_introspects


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import databasedriver as pg_driver

    raw_connections: list[_FakeDriverConnection] = []

    def fake_connect(metadata=None, url=None, **kwargs):
        """
        Create and retain a fake raw connection while ignoring all requested connection settings.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_driver_connects_and_introspects


        :param metadata: Ignored optional connection metadata.
        :param url: Ignored optional PostgreSQL URL.
        :param kwargs: Ignored additional connector keyword arguments.
        :return: New _FakeDriverConnection appended to the enclosing raw_connections list.
        """
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
    """
    Supply a small synthetic catalog and check the inherited agent/work relation reports typed-priority capabilities.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_driver_inherits_link_capability_introspection


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Use a recording connection to check insert ID, update success, and schema-qualified INSERT/UPDATE SQL text.

    This case does not separately assert recorded binding values.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_driver_basic_insert_and_update_sql


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Check recorded schema-qualified delete SQL and bindings for IDs, scalar/NULL values, batches, and table clearing.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_driver_delete_sql_is_native_and_schema_qualified


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Check fake-backed random/distinct/extrema/multi-column/paged query results and their schema-qualified SQL and selected bindings.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_driver_query_helpers_are_native_and_schema_qualified


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Check decoding of fixed metadata policy/options fields and recorded schema-qualified full-record and case-only writes.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_column_case_sensitivity_uses_schema_catalog


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Call eight individual metadata setters and check each records the full baseline tuple with the selected field replaced.

    Formatting and display options are checked as compact JSON strings; the fake does
    not persist earlier updates.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_individual_column_metadata_setters_use_full_record_upsert


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Record main-table creation and check schema-qualified names, identity/text/integer/timestamp columns, and the requested index.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_driver_creates_main_tables_with_native_ddl


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Record link-table creation/removal and check generated identity/endpoint columns, cascading references, a uniqueness clause, an index, and DROP TABLE.

    The uniqueness assertion checks the presence of the keyword, not its complete
    definition.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_driver_links_and_unlinks_main_tables_with_native_ddl


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Check view headings and row decoding from fake results plus configured-schema introspection and bound ID lookup.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_driver_view_helpers_are_native_and_schema_qualified


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Check fake trigger discovery/removal results and the exact schema/table lookup bindings and DROP TRIGGER statement.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_driver_trigger_helpers_are_native_and_schema_qualified


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Build the checker’s required-column catalog with text defaults, helper-table IDs, and two bigint size fields.

    This synthetic catalog supplies the checker’s expected names; its default types do
    not model the complete production schema.

    Example:
        >>> catalog = _complete_catalog()
        >>> catalog['digital_assets']['digital_asset_size_bytes']
        'bigint'
        >>> catalog['asset_replicas']['asset_replica_observed_size_bytes']
        'bigint'


    :return: Fresh nested table/column/type mapping.
    """
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
    """
    Simulate catalog, privilege, identity, and count queries while recording schema-qualified lookup arguments.

    Example:
        >>> cursor = _FakeCheckerCursor({})
        >>> cursor.execute('SELECT current_user').fetchone()
        {'current_user': 'liuxin_runtime'}
        >>> cursor.fetchone()
        {'current_user': 'liuxin_runtime'}
    """
    def __init__(self, catalog: dict[str, dict[str, str]], missing_privileges: dict[str, set[str]] | None = None):
        """
        Retain the catalog and a truthy privilege mapping, and initialize result and query-record lists.

        A falsy privilege mapping is replaced by a new empty dictionary; no input is deeply
        copied.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :param catalog: Table/column/type mapping consulted on each simulated query.
        :param missing_privileges: Optional table-to-uppercase-privilege-set mapping;
            omitted or empty values mean no missing privileges.
        :return: None.
        """
        self.catalog = catalog
        self.missing_privileges = missing_privileges or {}
        self.rows: list[dict[str, Any]] = []
        self.information_schema_schemas: list[str] = []
        self.privilege_relations: list[str] = []
        self.count_queries: list[str] = []
        self.table_regclasses: list[str] = []

    def execute(self, sql: str, values: tuple[Any, ...] | None = None):
        """
        Normalize SQL case/whitespace and choose fixed or catalog-derived result dictionaries by ordered pattern checks.

        Record privilege relations, regclass targets, information-schema schemas, and count
        SQL in their respective lists. Falsy bindings become an empty tuple; branches
        expecting missing bindings can raise. Unsupported SQL clears rows.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :param sql: Query text used for substring matching and count-query recording.
        :param values: Optional positional bindings interpreted according to the recognized
            query pattern.
        :return: This fake cursor; no SQL is executed and each call replaces the current
            result list.
        """
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
        """
        Return the first current result dictionary without consuming it.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: First row object, or None when no rows are stored.
        """
        return self.rows[0] if self.rows else None

    def fetchall(self):
        """
        Return a shallow copy of all current result rows without advancing a cursor position.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: New list containing the same row dictionaries.
        """
        return list(self.rows)

    def __enter__(self):
        """
        Return this cursor without acquiring or resetting resources.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: This _FakeCheckerCursor.
        """
        return self

    def __exit__(self, exc_type, exc, tb):
        """
        Propagate context-body exceptions without changing cursor state.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :param exc_type: Context-body exception type, ignored by this fake.
        :param exc: Context-body exception instance, ignored by this fake.
        :param tb: Context-body traceback, ignored by this fake.
        :return: False; no cleanup occurs.
        """
        return False


def _fake_relation_table_name(relation: str) -> str:
    """
    Take the final dot-separated relation component and strip double quotes from its ends.

    This is a test helper, not a parser for quoted SQL identifiers containing dots.

    Example:
        >>> _fake_relation_table_name('"schema"."stores"')
        'stores'


    :param relation: Relation string to split and trim.
    :return: Final component string, possibly empty.
    """
    return relation.split(".")[-1].strip('"')


class _FakeCheckerConnection:
    """
    Wrap a caller-supplied checker cursor and track explicit close calls without transaction behavior.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py
    """
    def __init__(self, cursor: _FakeCheckerCursor):
        """
        Store the cursor object and initialize closed to False.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :param cursor: Fake checker cursor retained by reference as cursor_obj.
        :return: None.
        """
        self.cursor_obj = cursor
        self.closed = False

    def close(self) -> None:
        """
        Set the closed flag without disabling the retained cursor.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: None.
        """
        self.closed = True

    def __enter__(self):
        """
        Return the fake connection without changing its closed flag.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :return: This _FakeCheckerConnection.
        """
        return self

    def __exit__(self, exc_type, exc, tb):
        """
        Propagate context-body exceptions without closing or transacting.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py


        :param exc_type: Context-body exception type, ignored by this fake.
        :param exc: Context-body exception instance, ignored by this fake.
        :param tb: Context-body traceback, ignored by this fake.
        :return: False; state remains unchanged.
        """
        return False


def test_postgresql_checker_passes_complete_schema(monkeypatch) -> None:
    """
    Provide a synthetic complete catalog and check self-test success, password redaction, and explicit connection closure.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_checker_passes_complete_schema


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Check a configured schema appears in the result/report and every recorded regclass, column, privilege, and count lookup uses it.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_checker_honors_configured_schema


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Simulate missing Python driver discovery and check the failed driver result, installation hint, and password-free report.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_checker_reports_missing_driver


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import checker

    monkeypatch.setattr(checker.importlib.util, "find_spec", lambda name: None)
    result = checker.run_postgres_self_test(postgres_url="postgresql://liuxin:secret@example.invalid/library")

    assert result["ok"] is False
    assert any(check["name"] == "driver" and not check["ok"] for check in result["checks"])
    report = checker.format_postgres_self_test(result)
    assert "secret" not in report
    assert ".[postgres]" in report


def test_postgresql_checker_reports_schema_type_and_privilege_failures(monkeypatch) -> None:
    """
    Simulate a missing storage-key column, a non-bigint size field, and missing UPDATE privilege, then check each appears in a password-free failure report.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_checker_reports_schema_type_and_privilege_failures


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Simulate a connection error naming a missing role and check failure reporting preserves the role name while removing the URL password.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_checker_reports_missing_role


    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL import checker

    monkeypatch.setattr(checker.importlib.util, "find_spec", lambda name: object())

    def fail_connect(*args, **kwargs):
        """
        Raise the fixed missing-role RuntimeError instead of opening a connection.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py::test_postgresql_checker_reports_missing_role


        :param args: Ignored positional connection arguments.
        :param kwargs: Ignored keyword connection arguments.
        :return: Never returns normally.
        """
        raise RuntimeError('connection failed for postgresql://liuxin:secret@example.invalid/library: role "liuxin_missing" does not exist')

    monkeypatch.setattr(checker, "connect_postgres", fail_connect)

    result = checker.run_postgres_self_test(postgres_url="postgresql://liuxin:secret@example.invalid/library")
    report = checker.format_postgres_self_test(result)

    assert result["ok"] is False
    assert "liuxin_missing" in report
    assert "secret" not in report
