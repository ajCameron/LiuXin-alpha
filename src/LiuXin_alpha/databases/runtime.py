"""
Attach database maintenance collaborators and bootstrap configured storage.

These helpers mutate a concrete database’s runtime graph. Maintenance initialization
starts background work; storage bootstrap can construct backends and alter metadata or
registry state. Neither helper wraps its entire operation in a transaction or performs
automatic rollback of partial setup.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, TYPE_CHECKING

from LiuXin_alpha.storage.durable_manager import (
    StorageBootstrapIssue,
    StorageBootstrapReport,
    StorageManager,
)
from LiuXin_alpha.storage.api import StorageManagementError
from LiuXin_alpha.databases.maintenance.service import Maintainer
from LiuXin_alpha.utils.logging import default_log

if TYPE_CHECKING:

    from LiuXin_alpha.databases.database import Database
    from LiuXin_alpha.databases.database.dirtied_mixin import DatabaseWriteTelemetry
    from LiuXin_alpha.databases.api.maintenance_api import MaintenanceCallbackSinkAPI


def initialise_database_runtime(
    db: Database,
    *,
    callback_proxy_cls: Callable[
        [Maintainer, DatabaseWriteTelemetry | None], MaintenanceCallbackSinkAPI
    ],
    # Todo: preferences should have an API
    preferences_obj: Mapping[str, Any],
) -> None:
    """
    Start maintenance and wire runtime collaborators back to the database.

    Retain supplied preferences, then use the constructor's runtime wiring path.
    Resolve required collaborators before starting Maintainer and its engine.
    Install maintenance aliases, callback and clean hooks, and db back-references.
    This does not bootstrap storage. Repeated calls do not stop an earlier
    maintainer; a later wiring failure can leave the new engine running, so
    callers own cleanup.

    Example:
        During one-time database construction, initialise_database_runtime(db,
        callback_proxy_cls=CallbackProxy, preferences_obj=prefs) connects the
        pre-created collaborators; close the owning database at shutdown.


    :param db: Concrete database with driver, driver_wrapper, macros, metadata_sql and
        write_telemetry already available.
    :param callback_proxy_cls: Constructor accepting the new Maintainer and
        db.write_telemetry to produce the driver callback proxy.
    :param preferences_obj: Preference object retained directly as db.preferences.
    :return: None; runtime aliases and collaborator references are assigned on db.
    :raises DatabaseIntegrityError: A required live facade collaborator is unbound.
    :raises RuntimeError: The table SQL collaborator is absent.
    """

    db.preferences = preferences_obj
    # Todo: Protected method access
    db._initialise_runtime_collaborators(
        enable_maintenance=True, callback_proxy_cls=callback_proxy_cls
    )


# Todo: As it mixes db and storage, should be in library - perhaps with a dummy for when we just want a db up
def bootstrap_storage_manager(
    db: Database,
    *,
    startup_on_add: bool = False,
    include_offline: bool = False,
    clear_existing: bool = True,
    strict: bool = False,
) -> StorageBootstrapReport:
    """
    Create or refresh the storage manager and retain its bootstrap report.

    If storage is absent, construct StorageManager before the guarded load; construction
    errors propagate even in non-strict mode. Otherwise atomically rebind the manager
    and its durable repository views before applying the startup policy. Non-strict
    loading exceptions are logged and converted to one discovered/failed configuration
    with the error text; partial load effects remain. Strict loading exceptions
    propagate before a new report is stored. A returned failed report is stored before
    strict mode raises StorageManagementError. Skips alone do not make report.ok false.

    Example:
        Given an open db, report = bootstrap_storage_manager(db, startup_on_add=False)
        refreshes configured Stores without requesting candidate startup; inspect
        report.issues for reported skips or failures.


    :param db: Concrete database hosting storage and storage_bootstrap_report.
    :param startup_on_add: Default False; configure the manager and forward whether
        loading should start candidates.
    :param include_offline: Forward inclusion of offline/retired or unavailable
        configurations.
    :param clear_existing: Forward whether loading replaces live facades and removes
        absent configurations.
    :param strict: Reraise loading exceptions and reject reports with counted failures
        when True.
    :return: StorageBootstrapReport also assigned to db.storage_bootstrap_report on
        completed reporting paths.
    :raises StorageManagementError: Strict mode receives a report with counted failures,
        or manager construction rejects its configuration.
    """

    storage = db.storage
    if storage is None:
        storage = StorageManager(db=db, startup_on_add=startup_on_add)
        db.storage = storage
    else:
        bind_database = getattr(storage, "bind_database", None)
        if bind_database is None:
            # Small test doubles may model only the legacy mutable surface.
            storage.db = db
        else:
            bind_database(db)
        storage.startup_on_add = bool(startup_on_add)

    try:
        report = storage.load_from_database(
            db,
            include_offline=include_offline,
            clear_existing=clear_existing,
            startup=startup_on_add,
        )
    except Exception as exc:
        if strict:
            raise
        metadata = db.metadata
        default_log.log_exception(
            "Storage manager bootstrap failed.",
            exc,
            "WARNING",
            ("database_path", metadata.get("database_path") if metadata else None),
        )
        report = StorageBootstrapReport(
            discovered_configurations=1,
            loaded_stores=0,
            skipped_configurations=0,
            failed_configurations=1,
            issues=(
                StorageBootstrapIssue(
                    None,
                    None,
                    str(exc) or type(exc).__name__,
                ),
            ),
        )

    db.storage_bootstrap_report = report
    if strict and not report.ok:
        issue_summary = "; ".join(
            "{}: {}".format(
                issue.store_name or issue.store_ref or "unknown Store",
                issue.reason,
            )
            for issue in report.issues
        )
        detail = f" ({issue_summary})" if issue_summary else ""
        raise StorageManagementError(
            "Storage manager bootstrap failed for "
            f"{report.failed_configurations} of "
            f"{report.discovered_configurations} configured Stores{detail}."
        )
    return report


__all__ = [
    "Maintainer",
    "StorageBootstrapReport",
    "StorageManager",
    "bootstrap_storage_manager",
    "initialise_database_runtime",
]
