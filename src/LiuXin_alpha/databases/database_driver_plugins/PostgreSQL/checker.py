"""Strict PostgreSQL readiness checks for LiuXin."""

from __future__ import annotations

import importlib.util
import re
import shlex
from collections.abc import Iterable, Mapping
from typing import Any
from urllib.parse import unquote, urlsplit

from LiuXin_alpha.databases.database.constants import (
    HELPER_TABLES,
    OPTIONAL_HELPER_TABLES,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.config import (
    DEFAULT_POSTGRES_SCHEMA,
    configured_postgres_schema,
    configured_postgres_target,
    redact_postgres_target,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
    DEFAULT_POSTGRES_STATEMENT_TIMEOUT_MS,
    POSTGRES_DRIVER_INSTALL_HINT,
    check_required_tables,
    connect_postgres,
    postgres_cursor,
    redact_postgres_error,
    set_statement_timeout,
)


CORE_REQUIRED_TABLES = (
    "database_metadata",
    "works",
    "expressions",
    "manifestations",
    "items",
    "agents",
)
STORAGE_REQUIRED_TABLES = (
    "stores",
    "digital_assets",
    "asset_replicas",
)
HELPER_REQUIRED_TABLES = tuple(
    sorted(
        set(HELPER_TABLES)
        - set(CORE_REQUIRED_TABLES)
        - set(OPTIONAL_HELPER_TABLES)
    )
)
LIUXIN_POSTGRES_REQUIRED_TABLES = CORE_REQUIRED_TABLES + STORAGE_REQUIRED_TABLES + HELPER_REQUIRED_TABLES

CORE_REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "database_metadata": ("database_metadata_id", "database_metadata_unique_id"),
    "works": ("work_id", "work_title", "work_sort_title"),
    "expressions": ("expression_id", "expression_language_id"),
    "manifestations": ("manifestation_id", "manifestation_format_detail"),
    "items": ("item_id", "item_manifestation_id"),
    "agents": ("agent_id", "agent_type", "agent_canonical_name"),
}
STORAGE_REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "stores": ("store_id", "store_kind", "store_root_uri"),
    "digital_assets": ("digital_asset_id", "digital_asset_hash_sha256", "digital_asset_size_bytes"),
    "asset_replicas": (
        "asset_replica_id",
        "asset_replica_store_id",
        "asset_replica_storage_key",
        "asset_replica_digital_asset_id",
        "asset_replica_observed_size_bytes",
    ),
}
LIUXIN_POSTGRES_REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    **CORE_REQUIRED_COLUMNS,
    **STORAGE_REQUIRED_COLUMNS,
}

LIUXIN_POSTGRES_REQUIRED_COLUMN_TYPES: dict[str, dict[str, tuple[str, ...]]] = {
    "digital_assets": {
        "digital_asset_size_bytes": ("bigint",),
    },
    "asset_replicas": {
        "asset_replica_observed_size_bytes": ("bigint",),
    },
}

RUNTIME_TABLE_PRIVILEGES = ("SELECT", "INSERT", "UPDATE", "DELETE")


def run_postgres_self_test(
    metadata: Mapping[str, object] | None = None,
    postgres_url: str | None = None,
    *,
    postgres_service: str | None = None,
    password: str | None = None,
    prompt_for_password: bool = True,
    check_core: bool = True,
    check_storage: bool = True,
    check_helpers: bool = True,
) -> dict[str, Any]:
    """Run the LiuXin PostgreSQL readiness check."""

    target = configured_postgres_target(metadata, explicit_url=postgres_url, explicit_service=postgres_service)
    result: dict[str, Any] = {
        "backend": "postgresql",
        "url": redact_postgres_target(target),
        "target_kind": target.kind,
        "schema": configured_postgres_schema(metadata),
        "ok": False,
        "checks": [],
    }
    _add_check(
        result,
        "configured",
        target.configured,
        "PostgreSQL target is configured" if target.configured else "No PostgreSQL URL or service profile configured",
    )
    if not target.configured:
        return result

    driver_present = importlib.util.find_spec("psycopg2") is not None
    _add_check(
        result,
        "driver",
        driver_present,
        (
            "psycopg2 is importable"
            if driver_present
            else POSTGRES_DRIVER_INSTALL_HINT
        ),
    )
    if not driver_present:
        return result

    conn = None
    try:
        conn = connect_postgres(
            metadata,
            postgres_url,
            service=postgres_service,
            password=password,
            prompt_for_password=prompt_for_password,
        )
        _add_check(result, "connection", True, "connected to PostgreSQL")
    except Exception as exc:
        missing_role_message = _missing_role_check_message(exc, target.value if target.kind == "url" else "")
        if missing_role_message:
            _add_check(result, "role.exists", False, missing_role_message)
        _add_check(result, "connection", False, redact_postgres_error(exc, target.label))
        return result

    try:
        with conn:
            with postgres_cursor(conn) as cur:
                set_statement_timeout(cur, timeout_ms=DEFAULT_POSTGRES_STATEMENT_TIMEOUT_MS)
                result["database"] = _scalar_text(cur, "select current_database()")
                result["user"] = _scalar_text(cur, "select current_user")
                _add_check(
                    result,
                    "identity",
                    bool(result.get("database")) and bool(result.get("user")),
                    f"database={result.get('database') or '<unknown>'}, user={result.get('user') or '<unknown>'}",
                )
                if check_core:
                    _check_table_group(
                        result,
                        cur,
                        label="core",
                        schema=str(result["schema"]),
                        required_tables=CORE_REQUIRED_TABLES,
                        required_columns=CORE_REQUIRED_COLUMNS,
                        required_column_types={},
                    )
                else:
                    _add_check(result, "core.skipped", True, "core schema checks skipped")
                if check_storage:
                    _check_table_group(
                        result,
                        cur,
                        label="storage",
                        schema=str(result["schema"]),
                        required_tables=STORAGE_REQUIRED_TABLES,
                        required_columns=STORAGE_REQUIRED_COLUMNS,
                        required_column_types=LIUXIN_POSTGRES_REQUIRED_COLUMN_TYPES,
                    )
                else:
                    _add_check(result, "storage.skipped", True, "storage schema checks skipped")
                if check_helpers:
                    _check_table_group(
                        result,
                        cur,
                        label="helpers",
                        schema=str(result["schema"]),
                        required_tables=HELPER_REQUIRED_TABLES,
                        required_columns={},
                        required_column_types={},
                    )
                else:
                    _add_check(result, "helpers.skipped", True, "helper table checks skipped")
    except Exception as exc:
        _add_check(result, "readiness", False, redact_postgres_error(exc, target.label))
    finally:
        if conn is not None:
            conn.close()
    result["ok"] = all(bool(check.get("ok")) for check in result["checks"])
    return result


def format_postgres_self_test(result: Mapping[str, Any]) -> str:
    """Return a short human-readable PostgreSQL readiness report."""

    lines = [
        "LiuXin PostgreSQL Self-Test",
        f"Target: {result.get('url') or '<not configured>'}",
        f"Schema: {result.get('schema') or DEFAULT_POSTGRES_SCHEMA}",
        "",
    ]
    for check in result.get("checks") or []:
        marker = "ok" if check.get("ok") else "FAIL"
        lines.append(f"[{marker}] {check.get('name')}: {check.get('message')}")
    lines.append("")
    lines.append(f"Result: {'OK' if result.get('ok') else 'FAILED'}")
    return "\n".join(lines)


def _check_table_group(
    result: dict[str, Any],
    cur: Any,
    *,
    label: str,
    schema: str,
    required_tables: Iterable[str],
    required_columns: Mapping[str, Iterable[str]],
    required_column_types: Mapping[str, Mapping[str, Iterable[str]]] | None = None,
) -> None:
    schema_check = check_required_tables(cur, required_tables, schema=schema)
    missing_tables = list(schema_check.missing_tables)
    _add_check(
        result,
        f"{label}.tables",
        not missing_tables,
        "all required tables exist" if not missing_tables else f"missing tables: {', '.join(missing_tables)}",
    )
    if missing_tables:
        return

    missing_columns: dict[str, list[str]] = {}
    for table_name, columns in required_columns.items():
        available = _table_columns(cur, table_name, schema=schema)
        missing = sorted(column for column in columns if column not in available)
        if missing:
            missing_columns[table_name] = missing
    _add_check(
        result,
        f"{label}.columns",
        not missing_columns,
        "required columns exist" if not missing_columns else _format_missing_columns(missing_columns),
    )

    wrong_types: dict[str, list[str]] = {}
    for table_name, column_types in (required_column_types or {}).items():
        available_types = _table_column_types(cur, table_name, schema=schema)
        for column_name, accepted_types in column_types.items():
            actual_type = available_types.get(str(column_name), "")
            accepted = {str(item).casefold() for item in accepted_types}
            if actual_type.casefold() not in accepted:
                wrong_types.setdefault(str(table_name), []).append(
                    f"{column_name}: {actual_type or '<missing>'} expected {'/'.join(sorted(accepted))}"
                )
    if required_column_types:
        _add_check(
            result,
            f"{label}.column_types",
            not wrong_types,
            "required column types match" if not wrong_types else _format_column_type_mismatches(wrong_types),
        )

    missing_privileges = _missing_table_privileges(cur, required_tables, RUNTIME_TABLE_PRIVILEGES, schema=schema)
    _add_check(
        result,
        f"{label}.privileges",
        not missing_privileges,
        (
            "runtime table privileges exist"
            if not missing_privileges
            else _format_missing_privileges(missing_privileges, str(result.get("user") or "configured role"), schema)
        ),
    )
    missing_select = sorted(table for table, privileges in missing_privileges.items() if "SELECT" in privileges)
    if missing_select:
        result[f"{label}_counts"] = {}
        _add_check(
            result,
            f"{label}.reads",
            False,
            f"skipped row counts because SELECT is missing on: {', '.join(missing_select)}",
        )
        return

    read_counts: dict[str, int] = {}
    for table_name in required_tables:
        read_counts[str(table_name)] = _table_count(cur, str(table_name), schema=schema)
    result[f"{label}_counts"] = read_counts
    _add_check(result, f"{label}.reads", True, _format_counts(read_counts))


def _add_check(result: dict[str, Any], name: str, ok: bool, message: str) -> None:
    result["checks"].append({"name": name, "ok": bool(ok), "message": str(message)})
    result["ok"] = all(bool(check.get("ok")) for check in result["checks"])


def _table_columns(cur: Any, table_name: str, *, schema: str = DEFAULT_POSTGRES_SCHEMA) -> set[str]:
    cur.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = %s and table_name = %s
        """,
        (schema, table_name),
    )
    return {str(_row_value(row, "column_name")) for row in cur.fetchall()}


def _table_column_types(cur: Any, table_name: str, *, schema: str = DEFAULT_POSTGRES_SCHEMA) -> dict[str, str]:
    cur.execute(
        """
        select column_name, data_type
        from information_schema.columns
        where table_schema = %s and table_name = %s
        """,
        (schema, table_name),
    )
    return {
        str(_row_value(row, "column_name")): str(_row_value(row, "data_type"))
        for row in cur.fetchall()
    }


def _table_count(cur: Any, table_name: str, *, schema: str = DEFAULT_POSTGRES_SCHEMA) -> int:
    cur.execute(f"select count(*) as count from {_qualified_table(schema, table_name)}")
    row = cur.fetchone()
    return int(_row_value(row, "count") or 0)


def _missing_table_privileges(
    cur: Any,
    table_names: Iterable[str],
    privileges: Iterable[str],
    *,
    schema: str = DEFAULT_POSTGRES_SCHEMA,
) -> dict[str, list[str]]:
    missing: dict[str, list[str]] = {}
    for table_name in table_names:
        table = str(table_name)
        for privilege in privileges:
            if not _has_table_privilege(cur, table, str(privilege), schema=schema):
                missing.setdefault(table, []).append(str(privilege).upper())
    return missing


def _has_table_privilege(cur: Any, table_name: str, privilege: str, *, schema: str = DEFAULT_POSTGRES_SCHEMA) -> bool:
    cur.execute(
        "select has_table_privilege(current_user, %s, %s) as ok",
        (_qualified_table(schema, table_name), privilege.upper()),
    )
    return bool(_row_value(cur.fetchone(), "ok"))


def _scalar_text(cur: Any, sql: str) -> str:
    cur.execute(sql)
    row = cur.fetchone()
    return str(next(iter(dict(row).values())) if row else "")


def _row_value(row: Any, key: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    try:
        return row[key]
    except Exception:
        return None


def _format_counts(counts: Mapping[str, int]) -> str:
    return ", ".join(f"{table}={count}" for table, count in sorted(counts.items()))


def _format_missing_columns(missing_columns: Mapping[str, Iterable[str]]) -> str:
    return "; ".join(
        f"{table}: {', '.join(columns)}"
        for table, columns in sorted((table, list(columns)) for table, columns in missing_columns.items())
    )


def _format_column_type_mismatches(wrong_types: Mapping[str, Iterable[str]]) -> str:
    return "; ".join(
        f"{table}: {', '.join(columns)}"
        for table, columns in sorted((table, list(columns)) for table, columns in wrong_types.items())
    )


def _format_missing_privileges(missing_privileges: Mapping[str, Iterable[str]], role: str, schema: str) -> str:
    missing = "; ".join(
        f"{table}: {', '.join(privileges)}"
        for table, privileges in sorted((table, list(privileges)) for table, privileges in missing_privileges.items())
    )
    quoted_role = shlex.quote(role) if role and role != "configured role" else "ROLE"
    quoted_schema = shlex.quote(schema)
    return (
        f"missing runtime privileges for {role}: {missing}. "
        "Repair as a PostgreSQL owner/admin, for example: "
        f"grant select, insert, update, delete on all tables in schema {quoted_schema} to {quoted_role}"
    )


def _q(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _qualified_table(schema: str, table_name: str) -> str:
    return f"{_q(schema)}.{_q(table_name)}"


_MISSING_ROLE_RE = re.compile(r"role\s+[\"']?(?P<role>[^\"'\s]+)[\"']?\s+does not exist", re.IGNORECASE)


def _missing_role_check_message(exc: BaseException, url: str) -> str:
    message = str(exc or "")
    match = _MISSING_ROLE_RE.search(message)
    if not match:
        return ""
    role = match.group("role") or _configured_role_name(url)
    if not role:
        return "Configured PostgreSQL login role does not exist."
    quoted_role = shlex.quote(role)
    return (
        f"PostgreSQL role {role!r} does not exist. Create it as a PostgreSQL admin, "
        f"for example: sudo -u postgres createuser --pwprompt {quoted_role}"
    )


def _configured_role_name(url: str) -> str:
    try:
        username = urlsplit(url).username
    except ValueError:
        return ""
    return unquote(username or "")
