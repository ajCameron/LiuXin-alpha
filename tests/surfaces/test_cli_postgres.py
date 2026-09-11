"""
Exercise PostgreSQL CLI routing, SQL rendering, and private configuration output.

Connection, self-test, schema creation, and privilege execution are replaced with
recording fakes where used. SQL generators, environment-file publication, and the
initialization manifest writer run locally against pytest temporary paths. These
tests do not establish live-server readiness or successful privilege application.
Self-test receipt fixtures already contain redacted targets; their display tests
are not direct tests of the underlying diagnostic target sanitizer.
"""

from __future__ import annotations

import json

from LiuXin_alpha.surfaces.cli.squashfs import main as cli_main
from LiuXin_alpha.surfaces.cli import postgres as pg_cli


def test_postgres_check_json_success(monkeypatch, capsys) -> None:
    """
    Require zero status and secret-free JSON for a pre-redacted successful receipt.

    Example:
        >>> test_postgres_check_json_success(monkeypatch, capsys)  # doctest: +SKIP


    :param monkeypatch: Pytest patcher replacing the database self-test.
    :param capsys: Capture used to decode and inspect the printed receipt.
    :return: None; assert success and absence of the input URL's secret text.
    """
    def fake_self_test(*args, **kwargs):
        """
        Return a fixed successful connection check with a pre-redacted URL.

        Example:
            >>> fake_self_test()["ok"]  # doctest: +SKIP
            True


        :param args: Ignored positional self-test inputs.
        :param kwargs: Ignored keyword self-test controls.
        :return: Fresh success receipt; no connection or sanitization is performed.
        """
        return {
            "backend": "postgresql",
            "url": "postgresql://liuxin:***@example.invalid/library",
            "ok": True,
            "checks": [{"name": "connection", "ok": True, "message": "connected"}],
        }

    monkeypatch.setattr(pg_cli, "run_postgres_self_test", fake_self_test)

    rc = cli_main(
        [
            "postgres",
            "check",
            "--url",
            (
                "postgresql://liuxin:secret@example.invalid/library"
                "?sslmode=require&password=query-secret"
            ),
            "--json",
            "--no-password-prompt",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert "secret" not in json.dumps(payload)


def test_postgres_check_text_failure_returns_2(monkeypatch, capsys) -> None:
    """
    Render a supplied missing-driver failure as text with exit status two.

    Example:
        >>> test_postgres_check_text_failure_returns_2(monkeypatch, capsys)  # doctest: +SKIP


    :param monkeypatch: Patcher supplying the pre-redacted failed diagnostic.
    :param capsys: Capture for the self-test heading and secret-absence assertions.
    :return: None; assert the failure status and formatted output contract.
    """
    def fake_self_test(*args, **kwargs):
        """
        Simulate an unavailable driver without inspecting installed dependencies.

        Example:
            >>> fake_self_test()["checks"][0]["name"]  # doctest: +SKIP
            'driver'


        :param args: Ignored positional connection selectors.
        :param kwargs: Ignored diagnostic flags and credentials.
        :return: Fresh failed receipt with a pre-redacted target and driver message.
        """
        return {
            "backend": "postgresql",
            "url": "postgresql://liuxin:***@example.invalid/library",
            "ok": False,
            "checks": [{"name": "driver", "ok": False, "message": "psycopg2 is not installed"}],
        }

    monkeypatch.setattr(pg_cli, "run_postgres_self_test", fake_self_test)

    rc = cli_main(
        [
            "postgres",
            "check",
            "--url",
            "postgresql://liuxin:secret@example.invalid/library",
            "--no-password-prompt",
        ]
    )

    assert rc == 2
    output = capsys.readouterr().out
    assert "LiuXin PostgreSQL Self-Test" in output
    assert "secret" not in output


def test_postgres_check_connect_only_can_store_env_file(monkeypatch, tmp_path, capsys) -> None:
    """
    Disable deeper checks while exporting the configured target to a private file.

    The file intentionally retains credentials embedded in the URL; only console
    output is expected to be redacted. No separate password export is requested.

    Example:
        >>> test_postgres_check_connect_only_can_store_env_file(monkeypatch, tmp_path, capsys)  # doctest: +SKIP


    :param monkeypatch: Patcher installing the recording self-test stub.
    :param tmp_path: Isolated directory for the real environment-file write.
    :param capsys: Capture for redacted stdout and the stderr publication notice.
    :return: None; assert family flags, 0600 mode, export content, and console output.
    """
    calls = []

    def fake_self_test(*args, **kwargs):
        """
        Record diagnostic arguments and report configured/connected success.

        Example:
            >>> fake_self_test(check_core=False)["schema"]  # doctest: +SKIP
            'liuxin_test'


        :param args: Positional inputs appended to the enclosing calls list.
        :param kwargs: Keyword controls recorded for the connect-only assertions.
        :return: Successful receipt authorizing the caller's environment export.
        """
        calls.append((args, kwargs))
        return {
            "backend": "postgresql",
            "url": "postgresql://liuxin:***@example.invalid/library",
            "schema": "liuxin_test",
            "ok": True,
            "checks": [
                {"name": "configured", "ok": True, "message": "PostgreSQL URL is configured"},
                {"name": "connection", "ok": True, "message": "connected"},
            ],
        }

    monkeypatch.setattr(pg_cli, "run_postgres_self_test", fake_self_test)
    target = tmp_path / "liuxin-postgres.env"

    rc = cli_main(
        [
            "postgres",
            "check",
            "--url",
            "postgresql://liuxin:secret@example.invalid/library",
            "--schema",
            "liuxin_test",
            "--connect-only",
            "--store-env-file",
            str(target),
            "--no-password-prompt",
        ]
    )

    assert rc == 0
    assert calls
    assert calls[0][1]["check_core"] is False
    assert calls[0][1]["check_storage"] is False
    assert calls[0][1]["check_helpers"] is False
    assert target.stat().st_mode & 0o777 == 0o600
    content = target.read_text(encoding="utf-8")
    assert "LIUXIN_POSTGRES_URL=postgresql://liuxin:secret@example.invalid/library" in content
    assert "LIUXIN_POSTGRES_SCHEMA=liuxin_test" in content
    assert "LIUXIN_POSTGRES_PASSWORD" not in content
    captured = capsys.readouterr()
    assert "secret" not in captured.out
    assert "secret" not in captured.err
    assert str(target) in captured.err


def test_postgres_check_store_env_file_password_is_explicit(monkeypatch, tmp_path) -> None:
    """
    Export the environment-derived password when --store-password is requested.

    Example:
        >>> test_postgres_check_store_env_file_password_is_explicit(monkeypatch, tmp_path)  # doctest: +SKIP


    :param monkeypatch: Patcher setting the password and replacing the self-test.
    :param tmp_path: Temporary parent for the shell export file.
    :return: None; assert the explicit opt-in includes the resolved password export.
    """
    def fake_self_test(*args, **kwargs):
        """
        Approve configuration so the environment-password export path can run.

        Example:
            >>> fake_self_test()["checks"][0]["ok"]  # doctest: +SKIP
            True


        :param args: Ignored self-test connection inputs.
        :param kwargs: Ignored resolved password and diagnostic controls.
        :return: Configured/connected success receipt using the public schema.
        """
        return {
            "backend": "postgresql",
            "url": "postgresql://liuxin@example.invalid/library",
            "schema": "public",
            "ok": True,
            "checks": [
                {"name": "configured", "ok": True, "message": "PostgreSQL URL is configured"},
                {"name": "connection", "ok": True, "message": "connected"},
            ],
        }

    monkeypatch.setenv("LIUXIN_POSTGRES_PASSWORD", "stored-secret")
    monkeypatch.setattr(pg_cli, "run_postgres_self_test", fake_self_test)
    target = tmp_path / "liuxin-postgres-with-password.env"

    rc = cli_main(
        [
            "postgres",
            "check",
            "--url",
            "postgresql://liuxin@example.invalid/library",
            "--store-env-file",
            str(target),
            "--store-password",
            "--no-password-prompt",
        ]
    )

    assert rc == 0
    content = target.read_text(encoding="utf-8")
    assert "LIUXIN_POSTGRES_PASSWORD=stored-secret" in content


def test_postgres_check_can_use_and_store_explicit_password(monkeypatch, tmp_path, capsys) -> None:
    """
    Forward a CLI password to diagnostics and opted-in storage without echoing it.

    Example:
        >>> test_postgres_check_can_use_and_store_explicit_password(monkeypatch, tmp_path, capsys)  # doctest: +SKIP


    :param monkeypatch: Patcher clearing the password environment and recording calls.
    :param tmp_path: Temporary parent for the credential-bearing export file.
    :param capsys: Capture for checking both console streams for the test secret.
    :return: None; assert password forwarding, persistence, and console suppression.
    """
    calls = []

    def fake_self_test(*args, **kwargs):
        """
        Record the explicit password forwarded by check and permit export.

        Example:
            >>> fake_self_test(password="test-only")["ok"]  # doctest: +SKIP
            True


        :param args: Positional connection inputs retained in the enclosing list.
        :param kwargs: Keyword diagnostic inputs, including the resolved password.
        :return: Configured/connected success receipt without password text.
        """
        calls.append((args, kwargs))
        return {
            "backend": "postgresql",
            "url": "postgresql://liuxin@example.invalid/library",
            "schema": "public",
            "ok": True,
            "checks": [
                {"name": "configured", "ok": True, "message": "PostgreSQL target is configured"},
                {"name": "connection", "ok": True, "message": "connected"},
            ],
        }

    monkeypatch.delenv("LIUXIN_POSTGRES_PASSWORD", raising=False)
    monkeypatch.setattr(pg_cli, "run_postgres_self_test", fake_self_test)
    target = tmp_path / "liuxin-postgres-cli-password.env"

    rc = cli_main(
        [
            "postgres",
            "check",
            "--url",
            "postgresql://liuxin@example.invalid/library",
            "--password",
            "cli-secret",
            "--store-env-file",
            str(target),
            "--store-password",
            "--no-password-prompt",
        ]
    )

    assert rc == 0
    assert calls and calls[0][1]["password"] == "cli-secret"
    content = target.read_text(encoding="utf-8")
    assert "LIUXIN_POSTGRES_PASSWORD=cli-secret" in content
    captured = capsys.readouterr()
    assert "cli-secret" not in captured.out
    assert "cli-secret" not in captured.err


def test_postgres_check_store_env_file_without_config_does_not_raise(monkeypatch, tmp_path, capsys) -> None:
    """
    Refuse environment export when the real diagnostic finds no configured target.

    The fixture clears URL variables, not service-profile variables; the test
    expects the surrounding test environment not to provide a service selector.

    Example:
        >>> test_postgres_check_store_env_file_without_config_does_not_raise(monkeypatch, tmp_path, capsys)  # doctest: +SKIP


    :param monkeypatch: Patcher clearing both supported database URL variables.
    :param tmp_path: Directory in which the refused output must remain absent.
    :param capsys: Capture for the missing URL/service diagnostic text.
    :return: None; assert status two, no output file, and an explanatory message.
    """
    monkeypatch.delenv("LIUXIN_POSTGRES_URL", raising=False)
    monkeypatch.delenv("LIUXIN_DATABASE_URL", raising=False)
    target = tmp_path / "missing.env"

    rc = cli_main(
        [
            "postgres",
            "check",
            "--store-env-file",
            str(target),
            "--no-password-prompt",
        ]
    )

    assert rc == 2
    assert not target.exists()
    output = capsys.readouterr().out
    assert "No PostgreSQL URL or service profile configured" in output


def test_postgres_schema_sql_includes_storage_bigint(capsys) -> None:
    """
    Check generated schema SQL for schema selection, storage sizes, and custom fields.

    SQL is rendered, not executed against a PostgreSQL server.

    Example:
        >>> test_postgres_schema_sql_includes_storage_bigint(capsys)  # doctest: +SKIP


    :param capsys: Capture for the generated DDL text.
    :return: None; assert zero status and the selected schema/table/column clauses.
    """
    rc = cli_main(["postgres", "schema-sql", "--schema", "liuxin_test"])

    assert rc == 0
    output = capsys.readouterr().out
    lowered = output.casefold()
    assert 'set search_path to "liuxin_test";' in output
    assert 'create table if not exists "digital_assets"' in lowered
    assert '"digital_asset_size_bytes" bigint null' in output
    assert 'create table if not exists "custom_columns"' in lowered


class _FakeRawConnection:
    """
    Observe connection closure without implementing a database connection protocol.

    Example:
        >>> connection = _FakeRawConnection()
        >>> connection.closed
        False
        >>> connection.close()
        >>> connection.closed
        True
    """

    def __init__(self) -> None:
        """
        Start an unclosed connection-lifecycle observation.

        Example:
            >>> _FakeRawConnection().closed
            False


        :return: None; initialize closed to False without opening a resource.
        """
        self.closed = False

    def close(self) -> None:
        """
        Mark closure observed; repeated calls leave the flag true.

        Example:
            >>> connection = _FakeRawConnection()
            >>> connection.close()
            >>> connection.closed
            True


        :return: None; set the observation flag without database I/O.
        """
        self.closed = True


def test_postgres_init_uses_connection_and_schema_builder(
    tmp_path, monkeypatch, capsys
) -> None:
    """
    Route initialization through connection/schema helpers and publish a clean manifest.

    Database helpers are mocked. The real local manifest must omit the supplied
    authority/query passwords while retaining the schema and sslmode option.

    Example:
        >>> test_postgres_init_uses_connection_and_schema_builder(tmp_path, monkeypatch, capsys)  # doctest: +SKIP


    :param tmp_path: Temporary system root for the actual manifest publication.
    :param monkeypatch: Patcher installing recording connection and schema helpers.
    :param capsys: Capture for success text and secret-absence checks.
    :return: None; assert password forwarding, closure, schema routing, and manifest data.
    """
    raw = _FakeRawConnection()
    calls: list[tuple[object, str]] = []
    connect_calls = []

    def fake_connect(*args, **kwargs):
        """
        Record initialization connection inputs and return the enclosing lifecycle fake.

        Example:
            >>> fake_connect(password="test-only") is raw  # doctest: +SKIP
            True


        :param args: Positional database selectors retained in connect_calls.
        :param kwargs: Keyword connection/password controls retained in connect_calls.
        :return: The shared raw fake, without attempting a network connection.
        """
        connect_calls.append((args, kwargs))
        return raw

    monkeypatch.setattr(pg_cli, "connect_postgres", fake_connect)

    def fake_create(conn, *, schema: str):
        """
        Record the schema request without using the supplied adapted connection.

        Example:
            >>> fake_create(connection, schema="catalogue")  # doctest: +SKIP


        :param conn: Connection adapter supplied by the initialization handler.
        :param schema: Requested PostgreSQL schema name, retained unchanged.
        :return: None; append the pair to calls without executing DDL.
        """
        calls.append((conn, schema))

    monkeypatch.setattr(pg_cli, "create_postgres_schema", fake_create)
    system_root = tmp_path / "postgres-system"

    rc = cli_main(
        [
            "postgres",
            "init",
            "--url",
            (
                "postgresql://liuxin:secret@example.invalid/library"
                "?sslmode=require&password=query-secret"
            ),
            "--schema",
            "liuxin_test",
            "--password",
            "init-secret",
            "--no-password-prompt",
            "--system-root",
            str(system_root),
        ]
    )

    assert rc == 0
    assert connect_calls and connect_calls[0][1]["password"] == "init-secret"
    assert raw.closed is True
    assert calls and calls[0][1] == "liuxin_test"
    output = capsys.readouterr().out
    assert "liuxin_test" in output
    assert "secret" not in output
    assert "init-secret" not in output
    manifest = json.loads(
        (system_root / "liuxin-system.json").read_text(encoding="utf-8")
    )
    assert manifest["database"] == (
        "postgresql://liuxin@example.invalid/library?sslmode=require"
    )
    assert manifest["database_metadata"] == {"schema": "liuxin_test"}


def test_postgres_write_env_redacts_output_and_sets_private_mode(tmp_path, capsys) -> None:
    """
    Keep URL credentials in a mode-0600 export while redacting the console summary.

    Example:
        >>> test_postgres_write_env_redacts_output_and_sets_private_mode(tmp_path, capsys)  # doctest: +SKIP


    :param tmp_path: Temporary parent for the real URL export file.
    :param capsys: Capture for the public publication summary.
    :return: None; assert file mode/content and absence of the test secret on stdout.
    """
    target = tmp_path / "liuxin-postgres.env"

    rc = cli_main(
        [
            "postgres",
            "write-env",
            "--url",
            "postgresql://liuxin:secret@example.invalid/library",
            "--output",
            str(target),
        ]
    )

    assert rc == 0
    assert target.exists()
    assert target.stat().st_mode & 0o777 == 0o600
    content = target.read_text(encoding="utf-8")
    assert "LIUXIN_POSTGRES_URL=" in content
    assert "LIUXIN_POSTGRES_PASSWORD" not in content
    assert "secret" in content
    assert "secret" not in capsys.readouterr().out


def test_postgres_write_env_can_include_password(tmp_path) -> None:
    """
    Include a separate CLI password export only along the requested opt-in path.

    Example:
        >>> test_postgres_write_env_can_include_password(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary directory receiving a credential-bearing env file.
    :return: None; assert success, private mode, and the explicit password assignment.
    """
    target = tmp_path / "liuxin-postgres-password.env"

    rc = cli_main(
        [
            "postgres",
            "write-env",
            "--url",
            "postgresql://liuxin@example.invalid/library",
            "--output",
            str(target),
            "--include-password",
            "--password",
            "stored-secret",
        ]
    )

    assert rc == 0
    assert target.stat().st_mode & 0o777 == 0o600
    content = target.read_text(encoding="utf-8")
    assert "LIUXIN_POSTGRES_PASSWORD=stored-secret" in content


def test_postgres_write_env_can_include_schema(tmp_path) -> None:
    """
    Preserve an explicit schema name in the generated shell connection exports.

    Example:
        >>> test_postgres_write_env_can_include_schema(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary directory for the environment file.
    :return: None; assert zero status and the requested schema assignment.
    """
    target = tmp_path / "liuxin-postgres-schema.env"

    rc = cli_main(
        [
            "postgres",
            "write-env",
            "--url",
            "postgresql://liuxin@example.invalid/library",
            "--output",
            str(target),
            "--schema",
            "liuxin_test",
        ]
    )

    assert rc == 0
    content = target.read_text(encoding="utf-8")
    assert "LIUXIN_POSTGRES_SCHEMA=liuxin_test" in content


def test_postgres_write_env_can_export_service_profile(tmp_path, capsys) -> None:
    """
    Export a service selector and schema without synthesizing a database URL.

    Example:
        >>> test_postgres_write_env_can_export_service_profile(tmp_path, capsys)  # doctest: +SKIP


    :param tmp_path: Temporary directory for the mode-0600 service export file.
    :param capsys: Capture for the service-based target summary.
    :return: None; assert service/schema exports, absent URL export, and printed target.
    """
    target = tmp_path / "liuxin-postgres-service.env"

    rc = cli_main(
        [
            "postgres",
            "write-env",
            "--service",
            "liuxin_runtime",
            "--output",
            str(target),
            "--schema",
            "liuxin_test",
        ]
    )

    assert rc == 0
    assert target.stat().st_mode & 0o777 == 0o600
    content = target.read_text(encoding="utf-8")
    assert "LIUXIN_POSTGRES_SERVICE=liuxin_runtime" in content
    assert "LIUXIN_POSTGRES_SCHEMA=liuxin_test" in content
    assert "LIUXIN_POSTGRES_URL" not in content
    output = capsys.readouterr().out
    assert "service=liuxin_runtime" in output


def test_postgres_check_accepts_service_profile(monkeypatch) -> None:
    """
    Forward a service name in both diagnostic metadata and its explicit keyword.

    Example:
        >>> test_postgres_check_accepts_service_profile(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Patcher replacing diagnostics with a service-aware recorder.
    :return: None; assert both selector paths and successful command status.
    """
    calls = []

    def fake_self_test(*args, **kwargs):
        """
        Capture service selection and return a prebuilt service connection receipt.

        Example:
            >>> fake_self_test()["target_kind"]  # doctest: +SKIP
            'service'


        :param args: Positional diagnostic inputs, including metadata, retained in calls.
        :param kwargs: Explicit service and other self-test options retained in calls.
        :return: Successful service-kind receipt without resolving a real service file.
        """
        calls.append((args, kwargs))
        return {
            "backend": "postgresql",
            "url": "service=liuxin_runtime",
            "target_kind": "service",
            "ok": True,
            "checks": [{"name": "connection", "ok": True, "message": "connected"}],
        }

    monkeypatch.setattr(pg_cli, "run_postgres_self_test", fake_self_test)

    rc = cli_main(
        [
            "postgres",
            "check",
            "--service",
            "liuxin_runtime",
            "--no-password-prompt",
        ]
    )

    assert rc == 0
    assert calls[0][0][0]["postgres_service"] == "liuxin_runtime"
    assert calls[0][1]["postgres_service"] == "liuxin_runtime"


def test_postgres_grant_sql_prints_runtime_privileges(capsys) -> None:
    """
    Render database-connect, table-DML, and sequence grants for the runtime role.

    Example:
        >>> test_postgres_grant_sql_prints_runtime_privileges(capsys)  # doctest: +SKIP


    :param capsys: Capture for generated SQL, which is not executed.
    :return: None; assert the three privilege clauses and successful rendering status.
    """
    rc = cli_main(["postgres", "grant-sql", "--role", "liuxin_runtime", "--database", "liuxin"])

    assert rc == 0
    output = capsys.readouterr().out
    assert 'grant connect on database "liuxin" to "liuxin_runtime";' in output
    assert 'grant select, insert, update, delete on all tables in schema "public" to "liuxin_runtime";' in output
    assert 'grant usage, select on all sequences in schema "public" to "liuxin_runtime";' in output


def test_postgres_setup_sql_prints_admin_bootstrap_script(capsys) -> None:
    """
    Include roles, database, schema, runtime grants, and owner defaults in setup SQL.

    Example:
        >>> test_postgres_setup_sql_prints_admin_bootstrap_script(capsys)  # doctest: +SKIP


    :param capsys: Capture for the full rendered bootstrap script and password guidance.
    :return: None; assert selected SQL clauses without creating any server objects.
    """
    rc = cli_main(
        [
            "postgres",
            "setup-sql",
            "--database",
            "liuxin",
            "--owner-role",
            "liuxin_owner",
            "--runtime-role",
            "liuxin_runtime",
            "--schema",
            "liuxin",
        ]
    )

    assert rc == 0
    output = capsys.readouterr().out
    assert 'create role "liuxin_owner" login' in output
    assert 'create role "liuxin_runtime" login' in output
    assert 'create database "liuxin" owner "liuxin_owner";' in output
    assert 'create schema if not exists "liuxin" authorization "liuxin_owner";' in output
    assert 'grant connect on database "liuxin" to "liuxin_runtime";' in output
    assert 'grant select, insert, update, delete on all tables in schema "liuxin" to "liuxin_runtime";' in output
    assert 'alter default privileges for role "liuxin_owner" in schema "liuxin" grant select, insert, update, delete on tables to "liuxin_runtime";' in output
    assert "password" in output.casefold()


def test_postgres_setup_sql_can_skip_existing_database_and_roles(capsys) -> None:
    """
    Omit role/database creation on request while retaining grants to a shared role.

    Example:
        >>> test_postgres_setup_sql_can_skip_existing_database_and_roles(capsys)  # doctest: +SKIP


    :param capsys: Capture for the creation-suppressed bootstrap SQL.
    :return: None; assert missing creation/probe text and retained database-connect grant.
    """
    rc = cli_main(
        [
            "postgres",
            "setup-sql",
            "--database",
            "liuxin",
            "--owner-role",
            "liuxin_owner",
            "--runtime-role",
            "liuxin_owner",
            "--schema",
            "public",
            "--no-create-database",
            "--no-create-roles",
        ]
    )

    assert rc == 0
    output = capsys.readouterr().out.casefold()
    assert "create database" not in output
    assert "pg_catalog.pg_roles" not in output
    assert 'grant connect on database "liuxin" to "liuxin_owner";' in output


def test_postgres_setup_sql_can_print_server_section_only(capsys) -> None:
    """
    Restrict setup output to server-level work, excluding schema/table privileges.

    Example:
        >>> test_postgres_setup_sql_can_print_server_section_only(capsys)  # doctest: +SKIP


    :param capsys: Capture for the server-only SQL rendering.
    :return: None; assert its heading/database creation and excluded database-local clauses.
    """
    rc = cli_main(
        [
            "postgres",
            "setup-sql",
            "--database",
            "liuxin",
            "--owner-role",
            "liuxin_owner",
            "--runtime-role",
            "liuxin_runtime",
            "--schema",
            "liuxin",
            "--section",
            "server",
        ]
    )

    assert rc == 0
    output = capsys.readouterr().out
    assert "Server section" in output
    assert 'create database "liuxin" owner "liuxin_owner";' in output
    assert 'create schema if not exists "liuxin"' not in output
    assert 'grant select, insert, update, delete on all tables in schema "liuxin"' not in output


def test_postgres_setup_sql_can_print_database_section_only(capsys) -> None:
    """
    Restrict setup output to schema/grants, excluding database and role creation.

    Example:
        >>> test_postgres_setup_sql_can_print_database_section_only(capsys)  # doctest: +SKIP


    :param capsys: Capture for the database-local SQL rendering.
    :return: None; assert heading, schema/runtime/default privileges, and excluded server work.
    """
    rc = cli_main(
        [
            "postgres",
            "setup-sql",
            "--database",
            "liuxin",
            "--owner-role",
            "liuxin_owner",
            "--runtime-role",
            "liuxin_runtime",
            "--schema",
            "liuxin",
            "--section",
            "database",
        ]
    )

    assert rc == 0
    output = capsys.readouterr().out
    assert "Database section" in output
    assert 'create database "liuxin"' not in output
    assert "pg_catalog.pg_roles" not in output
    assert 'create schema if not exists "liuxin" authorization "liuxin_owner";' in output
    assert 'grant select, insert, update, delete on all tables in schema "liuxin" to "liuxin_runtime";' in output
    assert 'alter default privileges for role "liuxin_owner" in schema "liuxin"' in output


def test_postgres_grant_runtime_role_uses_helper(monkeypatch, capsys) -> None:
    """
    Forward URL credentials and grant selectors without echoing secrets in the summary.

    Example:
        >>> test_postgres_grant_runtime_role_uses_helper(monkeypatch, capsys)  # doctest: +SKIP


    :param monkeypatch: Patcher replacing privilege execution with a recording fake.
    :param capsys: Capture for the role summary and secret-absence checks.
    :return: None; assert the exact helper call and output without applying privileges.
    """
    calls = []

    def fake_grant(metadata, url, *, service, role, schema, password, prompt_for_password):
        """
        Record grant execution inputs and fabricate a runtime-privilege receipt.

        Example:
            >>> receipt = fake_grant({}, None, service=None, role="reader", schema="public", password="", prompt_for_password=False)  # doctest: +SKIP


        :param metadata: Connection metadata retained by reference in the call tuple.
        :param url: Explicit URL retained unchanged, including any credentials.
        :param service: Optional explicit service selector retained unchanged.
        :param role: Runtime role echoed in the fake receipt.
        :param schema: Target schema echoed in the fake receipt.
        :param password: Separate resolved password recorded but omitted from the receipt.
        :param prompt_for_password: Interactive-password policy recorded without prompting.
        :return: Receipt naming liuxin/owner with fixed privileges and no executed statements.
        """
        calls.append((metadata, url, service, role, schema, password, prompt_for_password))
        return {
            "role": role,
            "schema": schema,
            "database": "liuxin",
            "grantor": "owner",
            "table_privileges": ["SELECT", "INSERT", "UPDATE", "DELETE"],
            "sequence_privileges": ["USAGE", "SELECT"],
            "statements": [],
        }

    monkeypatch.setattr(pg_cli, "grant_runtime_role_privileges", fake_grant)

    rc = cli_main(
        [
            "postgres",
            "grant-runtime-role",
            "--url",
            "postgresql://owner:secret@example.invalid/library",
            "--role",
            "liuxin_runtime",
            "--schema",
            "liuxin",
            "--password",
            "grant-secret",
            "--no-password-prompt",
        ]
    )

    assert rc == 0
    assert calls == [
        (
            {
                "postgres_url": "postgresql://owner:secret@example.invalid/library",
                "schema": "liuxin",
            },
            "postgresql://owner:secret@example.invalid/library",
            None,
            "liuxin_runtime",
            "liuxin",
            "grant-secret",
            False,
        )
    ]
    output = capsys.readouterr().out
    assert "liuxin_runtime" in output
    assert "secret" not in output
    assert "grant-secret" not in output


def test_postgres_grant_runtime_role_accepts_service_profile(monkeypatch) -> None:
    """
    Route service-based grants without a URL and with the expected empty password.

    The password expectation assumes no ambient password variable is configured.

    Example:
        >>> test_postgres_grant_runtime_role_accepts_service_profile(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Patcher replacing live privilege execution with a call recorder.
    :return: None; assert exact metadata/service/schema/prompt routing and zero status.
    """
    calls = []

    def fake_grant(metadata, url, *, service, role, schema, password, prompt_for_password):
        """
        Capture service-based grant arguments without resolving a service or server.

        Example:
            >>> receipt = fake_grant({}, None, service="admin", role="reader", schema="public", password="", prompt_for_password=False)  # doctest: +SKIP


        :param metadata: Connection metadata retained by reference in the call tuple.
        :param url: Explicit URL selector, expected to be None in this scenario.
        :param service: Administrative service selector retained unchanged.
        :param role: Runtime role echoed in the fake receipt.
        :param schema: Target schema echoed in the fake receipt.
        :param password: Resolved separate password retained only in the call record.
        :param prompt_for_password: Prompt policy recorded without any interactive input.
        :return: Fixed liuxin/owner receipt describing privileges, with no executed SQL.
        """
        calls.append((metadata, url, service, role, schema, password, prompt_for_password))
        return {
            "role": role,
            "schema": schema,
            "database": "liuxin",
            "grantor": "owner",
            "table_privileges": ["SELECT", "INSERT", "UPDATE", "DELETE"],
            "sequence_privileges": ["USAGE", "SELECT"],
            "statements": [],
        }

    monkeypatch.setattr(pg_cli, "grant_runtime_role_privileges", fake_grant)

    rc = cli_main(
        [
            "postgres",
            "grant-runtime-role",
            "--service",
            "liuxin_admin",
            "--role",
            "liuxin_runtime",
            "--schema",
            "liuxin",
            "--no-password-prompt",
        ]
    )

    assert rc == 0
    assert calls == [
        (
            {
                "postgres_service": "liuxin_admin",
                "schema": "liuxin",
            },
            None,
            "liuxin_admin",
            "liuxin_runtime",
            "liuxin",
            "",
            False,
        )
    ]
