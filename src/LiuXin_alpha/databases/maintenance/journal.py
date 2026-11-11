"""Read and acknowledge catalogue obligations without owning execution leases."""

from __future__ import annotations

import json
import time
from collections.abc import Iterable, Mapping
from typing import Any

from LiuXin_alpha.databases.maintenance.events import RowChangeEvent
from LiuXin_alpha.databases.maintenance.schema import quote, table_sql


def now_ms() -> int:
    """Return wall-clock milliseconds for persistent scheduling and diagnostics."""
    return time.time_ns() // 1_000_000


def rows(connection: Any, sql: str, parameters=()) -> list[dict[str, Any]]:
    """Materialize a bounded query with its physical column names on either backend."""
    cursor = connection.execute(sql, parameters)
    try:
        names = tuple(column[0] for column in cursor.description)
        return [
            dict(row)
            if isinstance(row, Mapping)
            else dict(zip(names, row, strict=True))
            for row in cursor.fetchall()
        ]
    finally:
        cursor.close()


def inspect_source(
    connection: Any, table: str, schema: str | None
) -> tuple[str, tuple[str, ...]]:
    """Require a physical table with one integer primary key before installing capture."""
    if schema:
        exists = connection.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = ? "
            "AND table_name = ? AND table_type = 'BASE TABLE'",
            (schema, table),
        ).fetchone()
        columns = rows(
            connection,
            "SELECT column_name AS name, data_type AS type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            (schema, table),
        )
        keys = connection.execute(
            "SELECT k.column_name FROM information_schema.table_constraints t "
            "JOIN information_schema.key_column_usage k ON k.constraint_name=t.constraint_name "
            "AND k.constraint_schema=t.constraint_schema AND k.table_name=t.table_name "
            "WHERE t.table_schema=? AND t.table_name=? AND t.constraint_type='PRIMARY KEY'",
            (schema, table),
        ).fetchall()
        keys = [row[0] for row in keys]
    else:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        columns = rows(connection, f"PRAGMA table_info({quote(table)})")
        keys = [row["name"] for row in columns if row["pk"]]
    if not exists or len(keys) != 1:
        raise ValueError(
            f"Maintenance requires a physical table with one primary key: {table!r}"
        )
    declared_type = next(row["type"] for row in columns if row["name"] == keys[0])
    if declared_type.lower() not in {"integer", "int", "bigint", "smallint"}:
        raise ValueError(
            "Maintenance capture currently requires integer row identities"
        )
    return keys[0], tuple(row["name"] for row in columns)


def pending(
    connection: Any,
    *,
    schema: str | None,
    mode: str,
    plugin_id: str | None = None,
    ids: Iterable[int] | None = None,
    limit: int = 128,
    lock: bool = False,
) -> list[dict[str, Any]]:
    """Read unacknowledged obligations and their immutable captured change facts."""
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError(
            "Maintenance batches must contain between 1 and 1000 obligations"
        )
    conditions = ["o.mode = ?", "o.completed_ms IS NULL"]
    parameters: list[Any] = [mode]
    if plugin_id is not None:
        conditions.append("o.plugin_id = ?")
        parameters.append(plugin_id)
    if ids is not None:
        ids = tuple(ids)
        if not ids:
            return []
        if len(ids) > 500 or any(type(value) is not int or value < 1 for value in ids):
            raise ValueError(
                "Delivery identities must be a bounded collection of positive integers"
            )
        conditions.append("o.obligation_id IN (" + ",".join("?" for _ in ids) + ")")
        parameters.extend(ids)
    sql = (
        f"SELECT o.*, c.table_name, c.row_id, c.operation, c.before_json, c.after_json, "
        f"c.event_json, c.ancestry_json, c.created_ms FROM {table_sql('maintenance_obligations', schema)} o "
        f"JOIN {table_sql('maintenance_changes', schema)} c ON c.change_id = o.change_id "
        "WHERE " + " AND ".join(conditions) + " ORDER BY o.obligation_id LIMIT ?"
    )
    parameters.append(limit)
    if lock and schema:
        sql += " FOR UPDATE OF o"
    return rows(connection, sql, parameters)


def event_from_record(record: dict[str, Any]) -> RowChangeEvent:
    """Reconstruct physical change identity without querying a deleted source row."""
    return RowChangeEvent(
        record["table_name"],
        record["row_id"],
        change_id=record["change_id"],
        operation=record["operation"],
        before=None
        if record["before_json"] is None
        else json.loads(record["before_json"]),
        after=None
        if record["after_json"] is None
        else json.loads(record["after_json"]),
    )


def acknowledge(
    connection: Any, records: list[dict[str, Any]], schema: str | None
) -> None:
    """Acknowledge exactly these revisions in the handler's effects transaction."""
    connection.executemany(
        f"UPDATE {table_sql('maintenance_obligations', schema)} "
        "SET completed_ms = ?, attempts = attempts + 1, last_error = NULL "
        "WHERE obligation_id = ? AND completed_ms IS NULL",
        [(now_ms(), row["obligation_id"]) for row in records],
    )
