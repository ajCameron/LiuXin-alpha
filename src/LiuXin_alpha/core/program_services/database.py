"""
Expose database inspection, migration planning/application, backup verification, and vacuum through Core.

Inspection functions have deliberately different error boundaries: some missing
or failed observations become unknown values, while present operation failures
remain visible. Mutation adapters do not wrap backend operations, later verification,
metadata refresh, and reconciliation in one transaction or supply rollback.
"""

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
    """
    Describe database identity and shallow metadata, suppressing ordinary identity-attribute access failures.

    The optional check_exists capability is different: its lookup and execution
    errors propagate, and its result is truth-tested. No backend health test is
    substituted when that callable is absent.

    Example:
        >>> from types import SimpleNamespace
        >>> database_info(SimpleNamespace(database=object()), None)["exists"] is None
        True


    :param runtime: Runtime exposing the database object to inspect.
    :param query: Ignored query envelope; no fields are consumed.
    :return: Raw identity/type values, optional exists flag, and copied Mapping metadata or an empty dictionary.
    """
    del query
    db = runtime.database

    def value(name: str) -> Any:
        """
        Read one captured database attribute, using None for absence or any ordinary access failure.

        Example:
            >>> value("uuid")  # doctest: +SKIP


        :param name: Exact attribute name requested from the enclosing database.
        :return: Attribute value or None; callable values are not invoked.
        """
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
    """
    Count advertised tables and optionally group them by category, retaining failed row counts as None.

    Table enumeration must succeed. Counts and category calls fail independently;
    failed categories become unknown. row_total sums only known counts and is not
    a completeness claim. Names are sorted but not deduplicated before table_count,
    while the counts dictionary naturally has one entry per name.

    Example:
        >>> summary = database_summary(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime supplying database table enumeration, row counts, and optional categorization.
    :param query: Ignored query envelope; no table filter or refresh option is consumed.
    :return: Advertised table_count, partial known row_total, counts, and optional category groups.
    """
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
    """
    Read optional write telemetry and dirty counters as independent database observations.

    Missing callable telemetry yields an empty mapping; missing callable counters
    yield None. Errors from present methods, attribute lookup, projection, or integer
    conversion propagate rather than being replaced with zero.

    Example:
        >>> telemetry = database_telemetry(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime exposing database telemetry and optional dirty-count methods.
    :param query: Ignored query envelope; counters are not reset by this adapter.
    :return: Projected write snapshot plus optional dirty_count and persisted_dirty_count integers.
    """
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
    """
    Compare the storage migration ledger with two required IDs and audit normalized identities.

    Ledger read/iteration/projection errors become an empty ledger; table enumeration
    errors still propagate. Identity capability/call failures become an unavailable
    report with clean=False and unknown update count. Successful report projection
    and attribute extraction occur outside that catch and may fail. Identity clean
    and row counts use report attributes, not keys on an arbitrary Mapping result.

    Example:
        >>> status = database_migrations_status(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime providing the database ledger and database-first identity audit capability.
    :param query: Ignored query envelope; the required storage schema version is fixed at two.
    :return: Pending storage IDs, identity report/clean/update state, and ok only when all three checks pass.
    """
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
    """
    Derive suggested additive migrations and collision count from a fresh status report without applying changes.

    The plan's ok means no reported collisions, not that status is healthy or its
    audit was available. Unknown update counts add no identity action; an unavailable
    audit can therefore coexist with ok=True and a no-pending-migrations message.
    Consumers must inspect the retained status as well as actions and collision count.

    Example:
        >>> plan = database_migrations_plan(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime used for the underlying migration-status inspection.
    :param query: Query forwarded to database_migrations_status without additional options.
    :return: Status, ordered storage/identity action suggestions, collision count, and a message based only on action presence.
    """
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
    """
    Invoke backend backup and optionally open the reported file read-only for SQLite quick_check.

    Prefer driver.direct_backup with optional unstripped output text; only an absent
    callable selects database.backup, which cannot accept an explicit path here.
    Direct-call failures are not retried. A truthy backend result takes precedence
    over the requested output path. Verification is SQLite-specific and does not
    compare the backup's contents with the source; it may fail after a file was
    written, with no adapter cleanup. backed_up=True otherwise means the call returned.

    Example:
        >>> receipt = database_backup(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime providing the selected database and its optional driver backup capability.
    :param command: Command with optional output_path and truth-tested verify=False.
    :return: backed_up=True, stringified backup_path or None, and optional quick_check outcome/messages.
    :raises CoreDispatchError: For unsupported explicit paths, unavailable verification paths, SQLite verification errors, or a non-ok check.
    """
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
    """
    Invoke database vacuum, falling back to backend vacuum only when the database callable is unavailable.

    A transient class carries the selected callable through the shared capability
    check, retaining Python descriptor binding. Callable execution errors propagate
    without retry or reconciliation. vacuumed=True indicates returned delegation.

    Example:
        >>> receipt = database_vacuum(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime exposing database and its optional backend vacuum operation.
    :param command: Ignored command envelope; no compaction options or confirmation fields are consumed.
    :return: Dictionary containing vacuumed=True after the selected callable returns.
    :raises CoreDispatchError: If neither object supplies a callable vacuum method.
    """
    del command
    db = runtime.database
    vacuum = getattr(db, "vacuum", None)
    if not callable(vacuum):
        backend = getattr(db, "backend", None)
        vacuum = getattr(backend, "vacuum", None)
    _callable(
        type(
            "_VacuumTarget",
            (),
            {
                "__doc__": """
                Carry the selected database vacuum callable through Core's capability checker.

                This transient adapter owns no database lifecycle or implementation
                state beyond its class-level callable. Normal descriptor binding
                applies when vacuum is retrieved from the adapter instance.

                Example:
                    >>> target.vacuum()  # doctest: +SKIP
                """,
                "vacuum": vacuum,
            },
        )(),
        "vacuum",
        area="database",
    )()
    return {"vacuumed": True}


def database_migrations_apply(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    """
    Apply storage-schema migration, then identity migration, refresh field metadata, and reconcile in sequence.

    No prior plan, collision report, confirmation, or payload switch is checked by
    this handler. Each subsystem owns its migration policy; failure at a later step
    can follow earlier applied changes without an adapter-wide rollback.

    Example:
        >>> receipt = database_migrations_apply(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime supplying the database, identity migrator, field metadata refresh, and reconciliation.
    :param command: Ignored command envelope; both migrations are always attempted in order.
    :return: Reconciled migrated=True receipt with projected storage and identity reports after all preceding steps succeed.
    """
    del command
    from LiuXin_alpha.storage.utils.migrations import migrate_storage_schema

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
