"""Helpers for granting LiuXin PostgreSQL runtime privileges."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
    DEFAULT_POSTGRES_STATEMENT_TIMEOUT_MS,
    connect_postgres,
    postgres_cursor,
    set_statement_timeout,
)


RUNTIME_TABLE_PRIVILEGES = ("select", "insert", "update", "delete")
RUNTIME_SEQUENCE_PRIVILEGES = ("usage", "select")
DEFAULT_SCHEMA = "public"
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class PostgresRuntimePrivilegeError(RuntimeError):
    """Raised when runtime role grants cannot be built or applied."""


def grant_runtime_role_privileges(
    metadata: Mapping[str, object] | None = None,
    url: str | None = None,
    *,
    service: str | None = None,
    role: str,
    schema: str = DEFAULT_SCHEMA,
    password: str | None = None,
    prompt_for_password: bool = True,
) -> dict[str, Any]:
    """Grant the runtime role the privileges needed for LiuXin reads and writes."""

    runtime_role = _validate_identifier(role, "runtime role")
    schema_name = _validate_identifier(schema, "schema")
    conn = connect_postgres(
        metadata,
        url,
        service=service,
        password=password,
        prompt_for_password=prompt_for_password,
        application_name="liuxin-alpha-runtime-grants",
    )
    try:
        with conn:
            with postgres_cursor(conn) as cur:
                set_statement_timeout(cur, timeout_ms=DEFAULT_POSTGRES_STATEMENT_TIMEOUT_MS)
                database = _scalar_text(cur, "select current_database()")
                grantor = _scalar_text(cur, "select current_user")
                statements = build_runtime_grant_statements(
                    role=runtime_role,
                    schema=schema_name,
                    database=database,
                )
                for statement in statements:
                    cur.execute(statement)
                return {
                    "role": runtime_role,
                    "schema": schema_name,
                    "database": database,
                    "grantor": grantor,
                    "table_privileges": [privilege.upper() for privilege in RUNTIME_TABLE_PRIVILEGES],
                    "sequence_privileges": [privilege.upper() for privilege in RUNTIME_SEQUENCE_PRIVILEGES],
                    "statements": statements,
                }
    except Exception as exc:
        raise PostgresRuntimePrivilegeError(
            f"failed to grant PostgreSQL runtime privileges to role {runtime_role!r}: {exc}"
        ) from exc
    finally:
        conn.close()


def build_runtime_grant_statements(
    *,
    role: str,
    schema: str = DEFAULT_SCHEMA,
    database: str,
    default_privileges_for_role: str | None = None,
) -> list[str]:
    """Return SQL statements for granting LiuXin runtime privileges."""

    runtime_role = _validate_identifier(role, "runtime role")
    schema_name = _validate_identifier(schema, "schema")
    database_name = _validate_identifier(database, "database")
    default_owner = (
        _validate_identifier(default_privileges_for_role, "default privileges owner role")
        if default_privileges_for_role not in (None, "")
        else None
    )
    role_ident = _quote_identifier(runtime_role)
    schema_ident = _quote_identifier(schema_name)
    database_ident = _quote_identifier(database_name)
    default_owner_sql = f" for role {_quote_identifier(default_owner)}" if default_owner else ""
    return [
        f"grant connect on database {database_ident} to {role_ident}",
        f"grant usage on schema {schema_ident} to {role_ident}",
        f"grant select, insert, update, delete on all tables in schema {schema_ident} to {role_ident}",
        f"grant usage, select on all sequences in schema {schema_ident} to {role_ident}",
        f"alter default privileges{default_owner_sql} in schema {schema_ident} grant select, insert, update, delete on tables to {role_ident}",
        f"alter default privileges{default_owner_sql} in schema {schema_ident} grant usage, select on sequences to {role_ident}",
    ]


def build_postgres_setup_statements(
    *,
    database: str,
    owner_role: str,
    runtime_role: str,
    schema: str = DEFAULT_SCHEMA,
    create_database: bool = True,
    create_roles: bool = True,
    section: str = "all",
) -> list[str]:
    """Return an admin-facing SQL script for preparing a LiuXin PostgreSQL target."""

    database_name = _validate_identifier(database, "database")
    owner = _validate_identifier(owner_role, "owner role")
    runtime = _validate_identifier(runtime_role, "runtime role")
    schema_name = _validate_identifier(schema, "schema")
    setup_section = _validate_setup_section(section)

    database_ident = _quote_identifier(database_name)
    owner_ident = _quote_identifier(owner)
    schema_ident = _quote_identifier(schema_name)

    statements: list[str] = []
    if setup_section in {"all", "server"}:
        statements.extend(
            _postgres_server_setup_statements(
                database_ident=database_ident,
                owner_ident=owner_ident,
                owner=owner,
                runtime=runtime,
                create_database=create_database,
                create_roles=create_roles,
            )
        )
    if setup_section in {"all", "database"}:
        statements.extend(
            _postgres_database_setup_statements(
                database_ident=database_ident,
                owner_ident=owner_ident,
                owner=owner,
                schema_ident=schema_ident,
                runtime=runtime,
                schema_name=schema_name,
                database_name=database_name,
            )
        )
    return statements


def _postgres_server_setup_statements(
    *,
    database_ident: str,
    owner_ident: str,
    owner: str,
    runtime: str,
    create_database: bool,
    create_roles: bool,
) -> list[str]:
    statements = [
        "-- Server section: run as a PostgreSQL admin from a maintenance database such as postgres.",
        "-- Do not paste passwords into this file; set them separately with psql/createuser tooling.",
    ]
    if create_roles:
        statements.append(_create_role_if_missing_statement(owner))
        if runtime != owner:
            statements.append(_create_role_if_missing_statement(runtime))
    if create_database:
        statements.append(f"create database {database_ident} owner {owner_ident}")
    return statements


def _postgres_database_setup_statements(
    *,
    database_ident: str,
    owner_ident: str,
    owner: str,
    schema_ident: str,
    runtime: str,
    schema_name: str,
    database_name: str,
) -> list[str]:
    return [
        "-- Database section: run while connected to the target LiuXin database.",
        f"create schema if not exists {schema_ident} authorization {owner_ident}",
        f"grant usage, create on schema {schema_ident} to {owner_ident}",
        f"grant connect on database {database_ident} to {owner_ident}",
        *build_runtime_grant_statements(
            role=runtime,
            schema=schema_name,
            database=database_name,
            default_privileges_for_role=owner,
        ),
    ]


def _validate_setup_section(value: str) -> str:
    text = str(value or "").strip().casefold()
    if text not in {"all", "server", "database"}:
        raise PostgresRuntimePrivilegeError(f"Invalid PostgreSQL setup section: {value!r}")
    return text


def _create_role_if_missing_statement(role: str) -> str:
    role_ident = _quote_identifier(role)
    role_literal = _quote_literal(role)
    create_literal = _quote_literal(f"create role {role_ident} login")
    return (
        "do $$\n"
        "begin\n"
        f"  if not exists (select 1 from pg_catalog.pg_roles where rolname = {role_literal}) then\n"
        f"    execute {create_literal};\n"
        "  end if;\n"
        "end\n"
        "$$"
    )


def _scalar_text(cur: Any, statement: str) -> str:
    cur.execute(statement)
    row = cur.fetchone()
    if isinstance(row, Mapping):
        return str(next(iter(row.values())) if row else "")
    try:
        return str(row[0] if row else "")
    except Exception:
        return ""


def _validate_identifier(value: str, label: str) -> str:
    text = str(value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(text):
        raise PostgresRuntimePrivilegeError(f"Invalid PostgreSQL {label}: {value!r}")
    return text


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


__all__ = [
    "PostgresRuntimePrivilegeError",
    "RUNTIME_SEQUENCE_PRIVILEGES",
    "RUNTIME_TABLE_PRIVILEGES",
    "build_postgres_setup_statements",
    "build_runtime_grant_statements",
    "grant_runtime_role_privileges",
]
