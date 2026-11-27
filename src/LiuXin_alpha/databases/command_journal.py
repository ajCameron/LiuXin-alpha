"""Persist application outcomes in the caller's catalogue transaction.

This module owns SQL only. Application request identity, authority, effect states
and replay policy belong to Core. No connection is opened or committed here.
"""

from __future__ import annotations

import json
from typing import Any

from LiuXin_alpha.databases.db_types.driver_types import RowSQLConnection
from LiuXin_alpha.databases.maintenance.schema import table_sql


def fetchone(
    connection: RowSQLConnection, sql: str, parameters: tuple[Any, ...] = ()
) -> Any:
    """Read one row and release the driver's cursor even when fetching fails."""
    cursor = connection.execute(sql, parameters)
    try:
        return cursor.fetchone()
    finally:
        cursor.close()


TABLE = "application_command_outcomes"
VERSION = 1


def present(connection: RowSQLConnection, schema: str | None = None) -> bool:
    """Inspect the selected catalogue without creating recovery state."""
    if schema:
        row = fetchone(
            connection,
            "SELECT 1 FROM information_schema.tables WHERE table_schema=? AND table_name=?",
            (schema, TABLE),
        )
    else:
        row = fetchone(
            connection,
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (TABLE,),
        )
    return row is not None


def install(connection: RowSQLConnection, schema: str | None = None) -> None:
    """Install supporting state inside a serialized write, including on first use.

    A PostgreSQL advisory lock serializes initial DDL across processes; SQLite's
    owning write transaction already excludes competing writers. Rolling back
    the enclosing write also rolls back a first installation.
    """
    if present(connection, schema):
        return
    if schema:
        fetchone(
            connection,
            "SELECT pg_advisory_xact_lock(hashtext(?))",
            (schema + ":application_command_outcomes",),
        )
    connection.execute(
        f"CREATE TABLE IF NOT EXISTS {table_sql(TABLE, schema)} ("
        "command_id TEXT PRIMARY KEY, version INTEGER NOT NULL, "
        "operation TEXT NOT NULL, fingerprint TEXT NOT NULL, "
        "authority_json TEXT NOT NULL, outcome_json TEXT NOT NULL)"
    ).close()


def reserve(
    connection: RowSQLConnection, command_id: str, schema: str | None = None
) -> None:
    """Serialize one PostgreSQL request before checking its outcome.

    Hash collisions only serialize unrelated requests; the full identity remains
    the primary key. SQLite callers already hold the serialized macro transaction.
    """
    if schema:
        fetchone(
            connection,
            "SELECT pg_advisory_xact_lock(hashtext(?))",
            (schema + ":command:" + command_id,),
        )


def read(
    connection: RowSQLConnection, command_id: str, schema: str | None = None
) -> dict[str, Any] | None:
    """Read one outcome from the current snapshot without repairing schema."""
    if not present(connection, schema):
        return None
    row = fetchone(
        connection,
        f"SELECT version, operation, fingerprint, authority_json, outcome_json "
        f"FROM {table_sql(TABLE, schema)} WHERE command_id=?",
        (command_id,),
    )
    if row is None:
        return None
    if row[0] != VERSION:
        raise RuntimeError(
            "Unsupported command outcome version; explicit upgrade required"
        )
    return {
        "operation": row[1],
        "fingerprint": row[2],
        "authority": json.loads(row[3]),
        "outcome": json.loads(row[4]),
    }


def insert(
    connection: RowSQLConnection,
    command_id: str,
    operation: str,
    fingerprint: str,
    authority: tuple[str, ...] | None,
    outcome: dict[str, Any],
    schema: str | None = None,
) -> None:
    """Persist a new identity and finite JSON outcome without replacing history."""
    connection.execute(
        f"INSERT INTO {table_sql(TABLE, schema)} "
        "(command_id, version, operation, fingerprint, authority_json, outcome_json) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            command_id,
            VERSION,
            operation,
            fingerprint,
            json.dumps(authority, allow_nan=False),
            json.dumps(outcome, allow_nan=False),
        ),
    ).close()


def finish(
    connection: RowSQLConnection,
    command_id: str,
    outcome: dict[str, Any],
    schema: str | None = None,
) -> None:
    """Replace an accepted uncertain outcome after its sole executor reports back."""
    reserve(connection, command_id, schema)
    saved = read(connection, command_id, schema)
    if saved is None or saved["outcome"].get("state") != "in_progress":
        raise RuntimeError("Command no longer owns an unfinished outcome")
    connection.execute(
        f"UPDATE {table_sql(TABLE, schema)} SET outcome_json=? WHERE command_id=?",
        (json.dumps(outcome, allow_nan=False), command_id),
    ).close()


def prune(
    connection: RowSQLConnection, command_id: str, schema: str | None = None
) -> bool:
    """Drop a successful result while retaining identity and effect evidence forever."""
    reserve(connection, command_id, schema)
    saved = read(connection, command_id, schema)
    if saved is None or saved["outcome"].get("state") != "succeeded":
        return False
    outcome = {**saved["outcome"], "result": None, "result_pruned": True}
    connection.execute(
        f"UPDATE {table_sql(TABLE, schema)} SET outcome_json=? WHERE command_id=?",
        (json.dumps(outcome, allow_nan=False), command_id),
    ).close()
    return True
