"""Retain application job intents in the same transaction as their command receipt."""

from __future__ import annotations

import json
import time
from typing import Any

from LiuXin_alpha.databases.command_journal import fetchone, install, reserve
from LiuXin_alpha.databases.db_types.driver_types import RowSQLConnection
from LiuXin_alpha.databases.maintenance.schema import table_sql

TABLE = "application_command_outbox"


def present(connection: RowSQLConnection, schema: str | None) -> bool:
    """Inspect pending-delivery support without installing it."""
    if schema:
        return (
            fetchone(
                connection,
                "SELECT 1 FROM information_schema.tables WHERE table_schema=? AND table_name=?",
                (schema, TABLE),
            )
            is not None
        )
    return (
        fetchone(
            connection,
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (TABLE,),
        )
        is not None
    )


def add(
    connection: RowSQLConnection,
    command_id: str,
    intent: dict[str, Any],
    schema: str | None,
) -> None:
    """Store an immutable delivery intent; never commit or start its executor."""
    install(connection, schema)
    reserve(connection, "outbox-schema", schema)
    connection.execute(
        f"CREATE TABLE IF NOT EXISTS {table_sql(TABLE, schema)} (command_id TEXT PRIMARY KEY, intent_json TEXT NOT NULL, accepted_at_ns BIGINT NOT NULL, job_id TEXT)"
    ).close()
    connection.execute(
        f"INSERT INTO {table_sql(TABLE, schema)} (command_id, intent_json, accepted_at_ns) VALUES (?, ?, ?)",
        (
            command_id,
            json.dumps(intent, sort_keys=True, allow_nan=False),
            time.time_ns(),
        ),
    ).close()


def pending(
    connection: RowSQLConnection, schema: str | None, limit: int = 32
) -> list[tuple[str, dict[str, Any]]]:
    """Read a bounded page; the durable jobs request key arbitrates duplicate delivery."""
    if type(limit) is not int or not 1 <= limit <= 500:
        raise ValueError("Outbox delivery limit must be between 1 and 500")
    if not present(connection, schema):
        return []
    cursor = connection.execute(
        f"SELECT command_id, intent_json FROM {table_sql(TABLE, schema)} WHERE job_id IS NULL ORDER BY accepted_at_ns, command_id LIMIT ?",
        (limit,),
    )
    try:
        return [(row[0], json.loads(row[1])) for row in cursor.fetchall()]
    finally:
        cursor.close()


def acknowledge(
    connection: RowSQLConnection, command_id: str, job_id: str, schema: str | None
) -> None:
    """Record an already accepted ledger run; a lost acknowledgement is replayable."""
    saved = fetchone(
        connection,
        f"SELECT job_id FROM {table_sql(TABLE, schema)} WHERE command_id=?",
        (command_id,),
    )
    if saved is None or saved[0] not in (None, job_id):
        raise RuntimeError(
            "Outbox acknowledgement disagrees with retained job identity"
        )
    connection.execute(
        f"UPDATE {table_sql(TABLE, schema)} SET job_id=? WHERE command_id=? AND job_id IS NULL",
        (job_id, command_id),
    ).close()


def job_id(
    connection: RowSQLConnection, command_id: str, schema: str | None
) -> str | None:
    """Resolve delivery without treating assignment as completion of the job."""
    if not present(connection, schema):
        return None
    row = fetchone(
        connection,
        f"SELECT job_id FROM {table_sql(TABLE, schema)} WHERE command_id=?",
        (command_id,),
    )
    return None if row is None else row[0]
