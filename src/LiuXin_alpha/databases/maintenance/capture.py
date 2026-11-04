"""Capture subscribed physical row changes inside their source transaction.

Triggers retain row identity and foreign-key identity columns, not a second copy
of arbitrary bibliographic values or file bytes. Prompt eligibility is checked
before mutation; handlers run later at the Python write boundary.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from typing import Any

from LiuXin_alpha.databases.maintenance.schema import TABLES, quote, table_sql


# Todo: We should be able to do better that Any for connection - need a specific type?
def install_capture(
    connection: Any,
    *,
    table: str,
    id_column: str,
    columns: Iterable[str],
    schema: str | None = None,
) -> None:
    """Install versioned triggers for an inspected physical table in this transaction."""
    if table in TABLES or table.startswith(("sqlite_", "pg_", "maintenance_")):
        raise ValueError("Maintenance internals cannot subscribe to themselves")
    columns = tuple(columns)
    if id_column not in columns:
        raise ValueError("Maintenance capture requires the table's physical ID column")
    identity_columns = tuple(
        column for column in columns if column == id_column or column.endswith("_id")
    )
    token = hashlib.sha256(table.encode()).hexdigest()[:16]
    if schema:
        _install_postgres(connection, schema, table, id_column, identity_columns, token)
    else:
        _install_sqlite(connection, table, id_column, identity_columns, token)


def remove_capture(connection: Any, *, table: str, schema: str | None = None) -> None:
    """Remove only this runtime's guards after the final subscription is removed."""
    if connection.execute(
        f"SELECT 1 FROM {table_sql('maintenance_subscriptions', schema)} WHERE table_name=? LIMIT 1",
        (table,),
    ).fetchone():
        raise ValueError("Cannot remove capture while a table is still subscribed")
    token = hashlib.sha256(table.encode()).hexdigest()[:16]
    if schema:
        for suffix in ("capture_v1", "truncate_guard_v1"):
            connection.execute(
                f"DROP TRIGGER IF EXISTS {quote(f'liuxin_m_{token}_{suffix}')} ON {table_sql(table, schema)}"
            ).close()
        connection.execute(
            f"DROP FUNCTION IF EXISTS {table_sql(f'liuxin_m_{token}_capture_v1', schema)}()"
        ).close()
    else:
        for operation in ("insert", "update", "delete"):
            for kind in ("guard", "capture"):
                connection.execute(
                    f"DROP TRIGGER IF EXISTS {quote(f'liuxin_m_{token}_{operation}_{kind}_v1')}"
                ).close()


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _identity_json(prefix: str, columns: tuple[str, ...], *, postgres: bool) -> str:
    arguments = ", ".join(
        f"{_literal(column)}, {prefix}.{quote(column)}" for column in columns
    )
    return (
        f"json_build_object({arguments})::text"
        if postgres
        else f"json_object({arguments})"
    )


def _install_sqlite(connection, table, id_column, columns, token):
    source = quote(table)
    subscribed = (
        "SELECT p.plugin_id, p.version, p.mode, p.enabled "
        "FROM maintenance_plugins p JOIN maintenance_subscriptions s "
        "ON s.plugin_id = p.plugin_id "
        f"WHERE s.table_name = {_literal(table)} "
        "AND p.plugin_id <> LIUXIN_MAINTENANCE_ORIGIN()"
    )
    for operation in ("insert", "update", "delete"):
        guard_name = quote(f"liuxin_m_{token}_{operation}_guard_v1")
        capture_name = quote(f"liuxin_m_{token}_{operation}_capture_v1")
        connection.execute(f"DROP TRIGGER IF EXISTS {guard_name}")
        connection.execute(f"DROP TRIGGER IF EXISTS {capture_name}")
        connection.execute(
            f"CREATE TRIGGER {guard_name} BEFORE {operation.upper()} ON {source} BEGIN "
            f"SELECT CASE WHEN EXISTS (SELECT 1 FROM ({subscribed}) "
            "WHERE mode = 'prompt' AND (enabled = 0 OR "
            "LIUXIN_MAINTENANCE_READY(plugin_id, version) = 0)) "
            "THEN RAISE(ABORT, 'Required prompt maintenance is unavailable') END; END"
        )
        before = (
            "NULL"
            if operation == "insert"
            else _identity_json("OLD", columns, postgres=False)
        )
        after = (
            "NULL"
            if operation == "delete"
            else _identity_json("NEW", columns, postgres=False)
        )
        row = "OLD" if operation == "delete" else "NEW"
        connection.execute(
            f"CREATE TRIGGER {capture_name} AFTER {operation.upper()} ON {source} BEGIN "
            "INSERT INTO maintenance_changes "
            "(table_name, row_id, operation, before_json, after_json, ancestry_json, created_ms) "
            f"SELECT {_literal(table)}, {row}.{quote(id_column)}, {_literal(operation)}, "
            f"{before}, {after}, LIUXIN_MAINTENANCE_ANCESTRY(), "
            "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) "
            f"WHERE EXISTS ({subscribed}); "
            "INSERT INTO maintenance_obligations (change_id, plugin_id, plugin_version, mode) "
            "SELECT (SELECT MAX(change_id) FROM maintenance_changes), "
            f"plugin_id, version, mode FROM ({subscribed}); END"
        )


def _install_postgres(connection, schema, table, id_column, columns, token):
    def q(name):
        return table_sql(name, schema)

    function = q(f"liuxin_m_{token}_capture_v1")
    trigger = quote(f"liuxin_m_{token}_capture_v1")
    before = _identity_json("OLD", columns, postgres=True)
    after = _identity_json("NEW", columns, postgres=True)
    subscribed = (
        f"SELECT p.plugin_id, p.version, p.mode, p.enabled FROM {q('maintenance_plugins')} p "
        f"JOIN {q('maintenance_subscriptions')} s ON s.plugin_id = p.plugin_id "
        f"WHERE s.table_name = {_literal(table)} "
        "AND p.plugin_id <> COALESCE(current_setting('liuxin.maintenance_origin', true), '')"
    )
    connection.execute(
        f"CREATE OR REPLACE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $body$ "
        "DECLARE captured BIGINT; before_ids TEXT; after_ids TEXT; source_id BIGINT; BEGIN "
        "IF TG_OP = 'TRUNCATE' THEN "
        "RAISE EXCEPTION 'Maintenance capture cannot handle TRUNCATE; use DELETE instead'; END IF; "
        f"IF EXISTS (SELECT 1 FROM ({subscribed}) s WHERE mode = 'prompt' AND "
        "(enabled = 0 OR (COALESCE(NULLIF(current_setting('liuxin.maintenance_ready', true), ''), '{}')"
        "::jsonb ->> plugin_id) IS DISTINCT FROM version)) THEN "
        "RAISE EXCEPTION 'Required prompt maintenance is unavailable'; END IF; "
        f"IF NOT EXISTS ({subscribed}) THEN RETURN NULL; END IF; "
        f"IF TG_OP <> 'INSERT' THEN before_ids := {before}; source_id := OLD.{quote(id_column)}; END IF; "
        f"IF TG_OP <> 'DELETE' THEN after_ids := {after}; source_id := NEW.{quote(id_column)}; END IF; "
        f"INSERT INTO {q('maintenance_changes')} "
        "(table_name, row_id, operation, before_json, after_json, ancestry_json, created_ms) "
        f"VALUES ({_literal(table)}, source_id, lower(TG_OP), before_ids, after_ids, "
        "COALESCE(NULLIF(current_setting('liuxin.maintenance_ancestry', true), ''), '[]'), "
        "CAST(EXTRACT(EPOCH FROM clock_timestamp()) * 1000 AS BIGINT)) RETURNING change_id INTO captured; "
        f"INSERT INTO {q('maintenance_obligations')} (change_id, plugin_id, plugin_version, mode) "
        f"SELECT captured, plugin_id, version, mode FROM ({subscribed}) s; "
        "RETURN NULL; END $body$"
    )
    connection.execute(f"DROP TRIGGER IF EXISTS {trigger} ON {q(table)}")
    connection.execute(
        f"CREATE TRIGGER {trigger} AFTER INSERT OR UPDATE OR DELETE ON {q(table)} "
        f"FOR EACH ROW EXECUTE FUNCTION {function}()"
    )
    # TRUNCATE emits no row events, including through CASCADE. Keep the guard in
    # the database so external connections cannot silently bypass subscriptions.
    truncate_guard = quote(f"liuxin_m_{token}_truncate_guard_v1")
    connection.execute(f"DROP TRIGGER IF EXISTS {truncate_guard} ON {q(table)}")
    connection.execute(
        f"CREATE TRIGGER {truncate_guard} BEFORE TRUNCATE ON {q(table)} "
        f"FOR EACH STATEMENT EXECUTE FUNCTION {function}()"
    )
