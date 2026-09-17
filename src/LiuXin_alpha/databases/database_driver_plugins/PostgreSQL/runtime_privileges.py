"""
Build administrative PostgreSQL setup SQL and apply runtime role grants.

Pure builders return statements without executing them. The grant helper owns its
connection and transaction; it neither creates a database nor creates login roles.
"""

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
    """
    Report invalid setup inputs or failure while applying runtime grants.

    Example:
        Catch ``PostgresRuntimePrivilegeError`` when a generated identifier is
        invalid or the connected grantor lacks required privileges.
    """


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
    """
    Validate role/schema names, then apply grants in one owned transaction.

    Read database and grantor identity, set a local timeout, execute current/default
    privilege grants, and close the connection. Default privileges apply to future
    objects created by the connected grantor. Connection establishment errors propagate;
    errors inside the grant transaction are wrapped with their original message.

    Example:
        ``grant_runtime_role_privileges(service="owner", role="library_runtime")``
        grants access using the owner service and closes the connection afterwards.


    :param metadata: Optional connection configuration mapping.
    :param url: Optional explicit PostgreSQL URL.
    :param service: Optional explicit service profile.
    :param role: Runtime role receiving privileges.
    :param schema: Schema name validated against the conservative identifier allowlist.
    :param password: Optional password for the grantor connection.
    :param prompt_for_password: Whether connection authentication may prompt on a terminal.
    :return: Role/schema/database/grantor details, granted privilege lists and executed statements.
    """

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
    """
    Build six grants for database connect, schema usage and current/future objects.

    Grant CRUD on tables and USAGE/SELECT on sequences. Future-object defaults apply
    to the explicitly named owner, or the SQL executor when no owner is supplied.
    Invalid names raise PostgresRuntimePrivilegeError before statements are returned.

    Example:
        >>> len(build_runtime_grant_statements(role="reader", database="library"))
        6


    :param role: Runtime role receiving privileges.
    :param schema: Schema name validated against the conservative identifier allowlist.
    :param database: Database name validated before SQL generation.
    :param default_privileges_for_role: Optional object-creating role for ALTER DEFAULT PRIVILEGES.
    :return: Ordered list of SQL statements without trailing semicolons.
    """

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
    """
    Build server and/or target-database setup statements for an administrator.

    The server section may create roles and a database; the database section creates
    the schema and grants owner/runtime access. These sections require different
    connection contexts and must not be blindly executed as one transaction.

    Example:
        >>> build_postgres_setup_statements(database="library", owner_role="owner",
        ...     runtime_role="reader", section="server", create_database=False,
        ...     create_roles=False)[0].startswith("-- Server section:")
        True


    :param database: Database name validated before SQL generation.
    :param owner_role: Role that owns the database/schema and creates future objects.
    :param runtime_role: Role that receives runtime access.
    :param schema: Schema name validated against the conservative identifier allowlist.
    :param create_database: Whether to emit CREATE DATABASE, which is not idempotent.
    :param create_roles: Whether to emit guarded LOGIN role creation blocks.
    :param section: Case-insensitive all, server or database; surrounding whitespace is stripped.
    :return: Ordered SQL/comment strings for the requested setup sections.
    """

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
    """
    Build the maintenance-database section with optional role/database creation.

    Identical owner/runtime roles produce only one guarded role-creation block.
    The generated database creation is unconditional when enabled.

    Example:
        With both creation flags false, the server section contains only its two
        administrative comments.


    :param database_ident: Already quoted database identifier.
    :param owner_ident: Already quoted owner-role identifier.
    :param owner: Validated owner role name, before identifier quoting.
    :param runtime: Validated runtime role name, before identifier quoting.
    :param create_database: Whether to emit CREATE DATABASE, which is not idempotent.
    :param create_roles: Whether to emit guarded LOGIN role creation blocks.
    :return: Server-section comments followed by enabled setup statements.
    """
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
    """
    Build schema creation and owner/runtime grants for the target database.

    Inputs are internal, already validated names or quoted identifiers. Default object
    privileges explicitly name the owner role.

    Example:
        The returned section grants schema CREATE to the owner and table CRUD
        to the runtime role.


    :param database_ident: Already quoted database identifier.
    :param owner_ident: Already quoted owner-role identifier.
    :param owner: Validated owner role name, before identifier quoting.
    :param schema_ident: Already quoted schema identifier.
    :param runtime: Validated runtime role name, before identifier quoting.
    :param schema_name: Validated unquoted schema name passed to the grant builder.
    :param database_name: Validated unquoted database name passed to the grant builder.
    :return: Target-database comment and ordered SQL statements.
    """
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
    """
    Normalize a setup section and reject values outside all/server/database.

    Example:
        >>> _validate_setup_section(" SERVER ")
        'server'


    :param value: Section selector converted to text and stripped.
    :return: Normalized section name.
    """
    text = str(value or "").strip().casefold()
    if text not in {"all", "server", "database"}:
        raise PostgresRuntimePrivilegeError(f"Invalid PostgreSQL setup section: {value!r}")
    return text


def _create_role_if_missing_statement(role: str) -> str:
    """
    Build a DO block that creates a LOGIN role only if its name is absent.

    This helper quotes inputs but relies on its callers to validate identifiers.
    No password or elevated role attributes are included.

    Example:
        A block for reader checks pg_catalog.pg_roles before issuing
        ``create role "reader" login``.


    :param role: Runtime role receiving privileges.
    :return: Anonymous procedural block without a final statement separator.
    """
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
    """
    Execute a query and stringify the first mapping value or sequence element.

    Example:
        A one-column ``select current_database()`` row yields its database name.


    :param cur: Open cursor used to execute and fetch one row.
    :param statement: SQL expected to return a scalar result.
    :return: First value as text, or an empty string for absent/unindexable rows.
    """
    cur.execute(statement)
    row = cur.fetchone()
    if isinstance(row, Mapping):
        return str(next(iter(row.values())) if row else "")
    try:
        return str(row[0] if row else "")
    except Exception:
        return ""


def _validate_identifier(value: str, label: str) -> str:
    """
    Accept a stripped ASCII letter/underscore identifier with alphanumeric suffix.

    Reject empty values, punctuation and leading digits with a labeled
    PostgresRuntimePrivilegeError.

    Example:
        >>> _validate_identifier(" library_reader ", "role")
        'library_reader'


    :param value: Name converted to text and trimmed.
    :param label: Input description included in validation errors.
    :return: Validated identifier text.
    """
    text = str(value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(text):
        raise PostgresRuntimePrivilegeError(f"Invalid PostgreSQL {label}: {value!r}")
    return text


def _quote_identifier(value: str) -> str:
    """
    Delimit an identifier and double embedded double quotes without validation.

    Example:
        >>> _quote_identifier("reader")
        '"reader"'


    :param value: String identifier to escape.
    :return: Double-quoted SQL identifier.
    """
    return '"' + value.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    """
    Delimit text as a SQL string literal, doubling apostrophes.

    Example:
        >>> _quote_literal("reader")
        "'reader'"


    :param value: Text to escape as a string literal.
    :return: Single-quoted SQL literal.
    """
    return "'" + value.replace("'", "''") + "'"


__all__ = [
    "PostgresRuntimePrivilegeError",
    "RUNTIME_SEQUENCE_PRIVILEGES",
    "RUNTIME_TABLE_PRIVILEGES",
    "build_postgres_setup_statements",
    "build_runtime_grant_statements",
    "grant_runtime_role_privileges",
]
