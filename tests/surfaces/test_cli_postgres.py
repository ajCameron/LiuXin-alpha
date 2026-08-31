from __future__ import annotations

import json

from LiuXin_alpha.surfaces.cli.squashfs import main as cli_main
from LiuXin_alpha.surfaces.cli import postgres as pg_cli


def test_postgres_check_json_success(monkeypatch, capsys) -> None:
    def fake_self_test(*args, **kwargs):
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
    def fake_self_test(*args, **kwargs):
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
    calls = []

    def fake_self_test(*args, **kwargs):
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
    def fake_self_test(*args, **kwargs):
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
    calls = []

    def fake_self_test(*args, **kwargs):
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
    rc = cli_main(["postgres", "schema-sql", "--schema", "liuxin_test"])

    assert rc == 0
    output = capsys.readouterr().out
    lowered = output.casefold()
    assert 'set search_path to "liuxin_test";' in output
    assert 'create table if not exists "digital_assets"' in lowered
    assert '"digital_asset_size_bytes" bigint null' in output
    assert 'create table if not exists "custom_columns"' in lowered


class _FakeRawConnection:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_postgres_init_uses_connection_and_schema_builder(
    tmp_path, monkeypatch, capsys
) -> None:
    raw = _FakeRawConnection()
    calls: list[tuple[object, str]] = []
    connect_calls = []

    def fake_connect(*args, **kwargs):
        connect_calls.append((args, kwargs))
        return raw

    monkeypatch.setattr(pg_cli, "connect_postgres", fake_connect)

    def fake_create(conn, *, schema: str):
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
    calls = []

    def fake_self_test(*args, **kwargs):
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
    rc = cli_main(["postgres", "grant-sql", "--role", "liuxin_runtime", "--database", "liuxin"])

    assert rc == 0
    output = capsys.readouterr().out
    assert 'grant connect on database "liuxin" to "liuxin_runtime";' in output
    assert 'grant select, insert, update, delete on all tables in schema "public" to "liuxin_runtime";' in output
    assert 'grant usage, select on all sequences in schema "public" to "liuxin_runtime";' in output


def test_postgres_setup_sql_prints_admin_bootstrap_script(capsys) -> None:
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
    calls = []

    def fake_grant(metadata, url, *, service, role, schema, password, prompt_for_password):
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
    calls = []

    def fake_grant(metadata, url, *, service, role, schema, password, prompt_for_password):
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
