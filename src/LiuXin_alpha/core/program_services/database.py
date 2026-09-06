"""Core-owned database operations and wire translation."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services.payloads import (
    _callable,
    _database_callable,
    _payload,
    plain,
)

if TYPE_CHECKING:
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


def database_info(runtime: CoreRuntime, query: CoreQuery) -> dict[str, Any]:
    del query
    db = runtime.database

    def value(name: str) -> Any:
        try:
            return getattr(db, name, None)
        except Exception:
            return None

    metadata = value("metadata")
    return {
        "uuid": value("uuid"),
        "library_id": value("library_id"),
        "database_version": value("database_version"),
        "type": value("type"),
        "exists": (
            bool(db.check_exists())
            if callable(getattr(db, "check_exists", None))
            else None
        ),
        "metadata": dict(metadata) if isinstance(metadata, Mapping) else {},
    }


def database_summary(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    del query
    db = runtime.database
    tables = sorted(str(item) for item in db.get_tables())
    counts: dict[str, int | None] = {}
    for table in tables:
        try:
            counts[table] = int(db.get_record_count(table))
        except Exception:
            counts[table] = None
    categories: dict[str, list[str]] = {}
    categorize = getattr(db, "categorize_table", None)
    if callable(categorize):
        for table in tables:
            try:
                category = str(categorize(table))
            except Exception:
                category = "unknown"
            categories.setdefault(category, []).append(table)
    return {
        "table_count": len(tables),
        "row_total": sum(count for count in counts.values() if count is not None),
        "counts": counts,
        "categories": categories,
    }


def database_telemetry(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    del query
    db = runtime.database
    snapshot_method = getattr(db, "get_write_telemetry_snapshot", None)
    snapshot = snapshot_method() if callable(snapshot_method) else {}
    dirty_method = getattr(db, "get_dirtied_count", None)
    persisted_method = getattr(db, "get_persisted_dirtied_count", None)
    return {
        "write": plain(snapshot),
        "dirty_count": (
            int(cast(Any, dirty_method())) if callable(dirty_method) else None
        ),
        "persisted_dirty_count": (
            int(cast(Any, persisted_method())) if callable(persisted_method) else None
        ),
    }


def database_migrations_status(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    del query
    tables = set(str(value) for value in runtime.database.get_tables())
    ledger: list[Any] = []
    if "storage_schema_migrations" in tables:
        try:
            ledger = [
                plain(value)
                for value in runtime.database.get_all_rows(
                    "storage_schema_migrations",
                    iterator_return=False,
                    sort_column="storage_schema_migration_id",
                )
            ]
        except Exception:
            ledger = []
    try:
        normalized = _database_callable(
            runtime,
            "audit_normalized_identities",
            area="normalized identities",
        )()
    except Exception as error:
        normalized_value: Any = {
            "available": False,
            "error": str(error) or type(error).__name__,
        }
        normalized_clean = False
        rows_needing_update = None
    else:
        normalized_value = plain(normalized)
        collisions = tuple(getattr(normalized, "collisions", ()))
        normalized_clean = not collisions
        rows_needing_update = int(getattr(normalized, "rows_needing_update", 0))
    required_storage = {
        "storage-0001-migration-ledger",
        "storage-0002-ingest-journal",
    }
    recorded = {
        str(value.get("storage_schema_migration_id") or value.get("migration_id"))
        for value in ledger
        if isinstance(value, Mapping)
    }
    pending_storage = sorted(required_storage - recorded)
    return {
        "ok": not pending_storage and normalized_clean and not rows_needing_update,
        "storage": {
            "schema_version": 2,
            "ledger": ledger,
            "pending": pending_storage,
        },
        "normalized_identities": {
            "clean": normalized_clean,
            "rows_needing_update": rows_needing_update,
            "report": normalized_value,
        },
    }


def database_migrations_plan(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    status = database_migrations_status(runtime, query)
    actions: list[dict[str, Any]] = []
    storage = status["storage"]
    if storage["pending"]:
        actions.append(
            {
                "action": "migrate_storage_schema",
                "pending": list(storage["pending"]),
                "additive": True,
            }
        )
    identities = status["normalized_identities"]
    if identities["rows_needing_update"]:
        actions.append(
            {
                "action": "migrate_normalized_identities",
                "rows_needing_update": identities["rows_needing_update"],
                "additive": True,
            }
        )
    collision_count = len(
        identities.get("report", {}).get("collisions", [])
        if isinstance(identities.get("report"), Mapping)
        else []
    )
    return {
        "ok": collision_count == 0,
        "status": status,
        "actions": actions,
        "blocked_by_collisions": collision_count,
        "message": (
            "No migrations are pending."
            if not actions
            else "Review the additive migration actions before applying."
        ),
    }


def database_backup(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    output_path = payload.get("output_path")
    driver = getattr(runtime.database, "driver", None)
    direct = getattr(driver, "direct_backup", None)
    if callable(direct):
        result = direct(None if output_path in (None, "") else str(output_path))
    else:
        if output_path not in (None, ""):
            raise CoreDispatchError(
                "This database backend cannot select an explicit backup path.",
                code="capability_unavailable",
            )
        _callable(runtime.database, "backup", area="database")()
        result = None
    backup_path = result or output_path
    verification: dict[str, Any] | None = None
    if bool(payload.get("verify", False)):
        if backup_path in (None, ""):
            raise CoreDispatchError(
                "The backend did not report a file path that Core can verify."
            )
        path = Path(str(backup_path)).expanduser().resolve(strict=False)
        try:
            connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
            try:
                rows = connection.execute("PRAGMA quick_check").fetchall()
            finally:
                connection.close()
        except sqlite3.Error as error:
            raise CoreDispatchError(f"Backup verification failed: {error}") from error
        messages = [str(row[0]) for row in rows]
        verification = {"ok": messages == ["ok"], "messages": messages}
        if not verification["ok"]:
            raise CoreDispatchError("Backup failed SQLite quick_check.")
    return {
        "backed_up": True,
        "backup_path": None if backup_path is None else str(backup_path),
        "verification": verification,
    }


def database_vacuum(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    del command
    db = runtime.database
    vacuum = getattr(db, "vacuum", None)
    if not callable(vacuum):
        backend = getattr(db, "backend", None)
        vacuum = getattr(backend, "vacuum", None)
    _callable(
        type("_VacuumTarget", (), {"vacuum": vacuum})(),
        "vacuum",
        area="database",
    )()
    return {"vacuumed": True}


def database_migrations_apply(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    del command
    from LiuXin_alpha.storage.migrations import migrate_storage_schema

    storage_report = migrate_storage_schema(runtime.database)
    identity_report = _database_callable(
        runtime,
        "migrate_normalized_identities",
        area="normalized identities",
    )()
    runtime.services.refresh_field_metadata()
    return runtime.services.reconcile(
        {
            "migrated": True,
            "storage": plain(storage_report),
            "normalized_identities": plain(identity_report),
        }
    )
