"""Install the optional catalogue-local maintenance journal explicitly.

Opening a catalogue never invokes this migration. Journal rows participate in
the source transaction; execution leases and job states belong to the jobs owner.
"""

from __future__ import annotations

import re
import uuid
from typing import Any

SCHEMA_VERSION = 1
TABLES = (
    "maintenance_metadata",
    "maintenance_plugins",
    "maintenance_subscriptions",
    "maintenance_changes",
    "maintenance_obligations",
    "maintenance_projections",
)


def quote(name: str) -> str:
    """Quote an inspected SQL identifier, rejecting empty and NUL names."""
    if not isinstance(name, str) or not name or "\x00" in name:
        raise ValueError("A nonempty SQL identifier is required")
    return '"' + name.replace('"', '""') + '"'


def table_sql(name: str, schema: str | None = None) -> str:
    """Qualify an internal table for PostgreSQL or use SQLite's main catalogue."""
    return f"{quote(schema)}.{quote(name)}" if schema else quote(name)


def present(connection: Any, schema: str | None = None) -> bool:
    """Inspect migration presence without writes or probing an absent relation."""
    if schema:
        row = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = 'maintenance_metadata'",
            (schema,),
        ).fetchone()
    else:
        row = connection.execute(
            "SELECT COUNT(*) FROM sqlite_master "
            "WHERE type = 'table' AND name = 'maintenance_metadata'"
        ).fetchone()
    return bool(row[0])


def migration_status(connection: Any, schema: str | None = None) -> dict[str, Any]:
    """Return the installed version and journal identity without repairing it."""
    if not present(connection, schema):
        return {"installed": False, "version": None, "required_version": SCHEMA_VERSION}
    rows = dict(
        connection.execute(
            f"SELECT key, value FROM {table_sql('maintenance_metadata', schema)}"
        ).fetchall()
    )
    version = int(rows.get("schema_version", "0"))
    return {
        "installed": True,
        "version": version,
        "required_version": SCHEMA_VERSION,
        "journal_uuid": rows.get("journal_uuid"),
        "current": version == SCHEMA_VERSION,
    }


def migrate(connection: Any, schema: str | None = None) -> dict[str, Any]:
    """Create the journal in the caller's write unit, preserving existing history.

    This function neither commits nor opens a connection. Unknown installed
    versions are refused. The caller serializes concurrent schema migrations.
    """
    if schema is not None and not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", schema):
        raise ValueError("Unsupported maintenance schema identifier")
    current = migration_status(connection, schema)
    if current["installed"]:
        if not current.get("current") or not current.get("journal_uuid"):
            raise RuntimeError(
                "Maintenance schema requires an explicit version upgrade"
            )
        return current

    def q(name: str) -> str:
        return table_sql(name, schema)

    identity = (
        "BIGSERIAL PRIMARY KEY" if schema else "INTEGER PRIMARY KEY AUTOINCREMENT"
    )
    definitions = (
        ("maintenance_metadata", "key TEXT PRIMARY KEY, value TEXT NOT NULL"),
        (
            "maintenance_plugins",
            "plugin_id TEXT PRIMARY KEY, version TEXT NOT NULL, "
            "mode TEXT NOT NULL CHECK (mode IN ('lazy', 'prompt')), "
            "enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)), "
            "priority INTEGER NOT NULL, dependencies_json TEXT NOT NULL, "
            "definition_json TEXT NOT NULL, retry_limit INTEGER NOT NULL, "
            "retry_backoff_s REAL NOT NULL",
        ),
        (
            "maintenance_subscriptions",
            f"plugin_id TEXT NOT NULL REFERENCES {q('maintenance_plugins')}(plugin_id), "
            "table_name TEXT NOT NULL, PRIMARY KEY (plugin_id, table_name)",
        ),
        (
            "maintenance_changes",
            f"change_id {identity}, table_name TEXT NOT NULL, row_id BIGINT NOT NULL, "
            "operation TEXT NOT NULL CHECK (operation IN ('insert', 'update', 'delete', 'manual')), "
            "before_json TEXT, after_json TEXT, event_json TEXT, "
            "ancestry_json TEXT NOT NULL, created_ms BIGINT NOT NULL",
        ),
        (
            "maintenance_obligations",
            f"obligation_id {identity}, "
            f"change_id BIGINT NOT NULL REFERENCES {q('maintenance_changes')}(change_id) ON DELETE CASCADE, "
            f"plugin_id TEXT NOT NULL REFERENCES {q('maintenance_plugins')}(plugin_id), "
            "plugin_version TEXT NOT NULL, mode TEXT NOT NULL, completed_ms BIGINT, "
            "attempts INTEGER NOT NULL DEFAULT 0, available_ms BIGINT NOT NULL DEFAULT 0, "
            "retry_generation INTEGER NOT NULL DEFAULT 0, "
            "last_error TEXT, last_failure_run_id BIGINT, job_ledger_uuid TEXT, job_run_id BIGINT, "
            "UNIQUE (change_id, plugin_id)",
        ),
        (
            "maintenance_projections",
            f"plugin_id TEXT NOT NULL REFERENCES {q('maintenance_plugins')}(plugin_id), "
            "projection_key TEXT NOT NULL, value_json TEXT NOT NULL, "
            "revision BIGINT NOT NULL, PRIMARY KEY (plugin_id, projection_key)",
        ),
    )
    for name, columns in definitions:
        connection.execute(f"CREATE TABLE {q(name)} ({columns})")
    connection.execute(
        f"CREATE INDEX maintenance_pending_idx ON {q('maintenance_obligations')} "
        "(mode, completed_ms, available_ms, obligation_id)"
    )
    connection.execute(
        f"CREATE INDEX maintenance_plugin_pending_idx ON {q('maintenance_obligations')} "
        "(plugin_id, completed_ms, change_id)"
    )
    connection.executemany(
        f"INSERT INTO {q('maintenance_metadata')} (key, value) VALUES (?, ?)",
        (("schema_version", str(SCHEMA_VERSION)), ("journal_uuid", str(uuid.uuid4()))),
    )
    return migration_status(connection, schema)
