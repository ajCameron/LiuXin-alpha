"""Capture database reopening options without transferring a live connection.

Core owns the selected target for its lifetime. Worker submissions receive fresh
copies of these options; changing the caller's mapping, working directory or
profile cannot redirect a queued operation. Job summaries expose argument names,
not these private option values; diagnostics must use configuration redaction.
PostgreSQL service files and authentication are still resolved by the driver
when a connection is opened.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any


def normalize_database_type(db_type: str) -> str:
    """Canonicalize built-in aliases while retaining third-party driver names."""
    kind = db_type.strip()
    return {
        "sqlite": "SQLite",
        "sqlite_apsw": "SQLite_apsw",
        "apsw": "SQLite_apsw",
        "postgres": "PostgreSQL",
        "postgresql": "PostgreSQL",
        "pg": "PostgreSQL",
    }.get(kind.casefold(), kind)


def is_sqlite(db_type: str) -> bool:
    """Recognize the built-in SQLite driver names and aliases."""
    return db_type.strip().casefold() in {"sqlite", "sqlite_apsw", "apsw"}


def resolve_database_connection(
    *,
    database_path: str | Path | None,
    db_type: str,
    database_metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve an absolute file path or a PostgreSQL target and schema, without opening resources.

    An explicit PostgreSQL path/selector wins over metadata target aliases; absent
    a selector, the driver's metadata/environment precedence applies. Schema and
    other metadata survive. Returned containers share no mutable input values.
    """
    metadata = deepcopy(dict(database_metadata or {}))
    kind = normalize_database_type(db_type)
    if kind.casefold() in {"postgres", "postgresql", "pg"}:
        from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.config import (
            METADATA_SERVICE_KEYS,
            METADATA_URL_KEYS,
            configured_postgres_schema,
            configured_postgres_target,
            is_postgres_url,
        )

        if database_path is not None and str(database_path).strip():
            selector = str(database_path).strip()
            # A path argument explicitly selects the target. Do not let stale
            # aliases in metadata silently select a different server.
            for key in (*METADATA_URL_KEYS, *METADATA_SERVICE_KEYS):
                metadata.pop(key, None)
            metadata[
                "postgres_url" if is_postgres_url(selector) else "postgres_service"
            ] = selector
        target = configured_postgres_target(metadata)
        if not target.configured:
            raise ValueError("A PostgreSQL connection target is required.")
        for key in (*METADATA_URL_KEYS, *METADATA_SERVICE_KEYS):
            metadata.pop(key, None)
        metadata["postgres_url" if target.kind == "url" else "postgres_service"] = (
            target.value
        )
        path = target.label
        metadata["schema"] = configured_postgres_schema(metadata)
    else:
        selected = (
            database_path
            if database_path is not None
            else metadata.get("database_path")
        )
        if selected is None or not str(selected).strip():
            raise ValueError("A database path is required.")
        path = str(Path(selected).expanduser().resolve())
    metadata["database_path"] = path
    return {"database_path": path, "db_type": kind, "database_metadata": metadata}


def capture_database_connection(database: Any) -> dict[str, Any] | None:
    """Copy an open database's effective target/schema for later worker graphs.

    Borrowed facades without connection metadata remain usable synchronously.
    Native PostgreSQL drivers supply their resolved target/schema so an environment
    change since opening cannot redirect a worker into a different namespace.
    """
    raw = getattr(database, "metadata", None)
    if not isinstance(raw, Mapping):
        return None
    metadata = deepcopy(dict(raw))
    kind = str(getattr(database, "type", None) or "SQLite")
    path = metadata.get("database_path")
    driver = getattr(database, "_driver", None)
    target = getattr(driver, "connection_target", None)
    if (
        normalize_database_type(kind) == "PostgreSQL"
        and driver is not None
        and target is not None
    ):
        path = target.label
        metadata["schema"] = driver.schema
    if not path and kind.strip().casefold() not in {"postgres", "postgresql", "pg"}:
        return None
    return resolve_database_connection(
        database_path=path, db_type=kind, database_metadata=metadata
    )
