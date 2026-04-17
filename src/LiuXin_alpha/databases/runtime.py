"""Runtime composition helpers for the database package.

This module owns app-wiring concerns that the raw ``Database`` core should not need
to spell out inline: storage bootstrap and maintenance/runtime attachment.
"""

from __future__ import annotations

from typing import Any

from LiuXin_alpha.storage.store_manager import StorageBootstrapReport, StorageManager
from LiuXin_alpha.databases.maintenance.service import Maintainer
from LiuXin_alpha.utils.logging import default_log


def initialise_database_runtime(
    db: Any,
    *,
    callback_proxy_cls: type,
    preferences_obj: Any,
) -> None:
    """Attach runtime collaborators to a concrete database instance."""

    db.maintenance = Maintainer(db)
    db.maintainer = db.maintenance
    db._maintainer_callback_proxy = callback_proxy_cls(db.maintenance, db.write_telemetry)
    db.driver.maintainer_callback = db._maintainer_callback_proxy
    db.clean = db.maintenance.clean

    db.preferences = preferences_obj

    # Ensure all major collaborators can refer back to the live database object.
    db.driver_wrapper.db = db
    db.driver.db = db
    db.macros.db = db


def bootstrap_storage_manager(
    db: Any,
    *,
    startup_on_add: bool = False,
    include_offline: bool = False,
    clear_existing: bool = True,
    strict: bool = False,
) -> StorageBootstrapReport:
    """Build or refresh a ``StorageManager`` from rows in the ``stores`` table."""

    if getattr(db, "storage", None) is None:
        db.storage = StorageManager(db=db, startup_on_add=startup_on_add)
    else:
        # Existing managers can outlive one database instance during test/runtime
        # wiring; keep the live database bound before reading store rows.
        db.storage.db = db

    try:
        report = db.storage.load_from_database(
            db,
            include_offline=include_offline,
            clear_existing=clear_existing,
        )
    except Exception as exc:
        if strict:
            raise
        default_log.log_exception(
            "Storage manager bootstrap failed.",
            exc,
            "WARNING",
            ("database_path", db.metadata.get("database_path") if getattr(db, "metadata", None) else None),
        )
        report = StorageBootstrapReport(
            discovered_rows=0,
            loaded_stores=0,
            skipped_rows=0,
            failed_rows=1,
        )

    db.storage_bootstrap_report = report
    return report


__all__ = [
    "Maintainer",
    "StorageBootstrapReport",
    "StorageManager",
    "bootstrap_storage_manager",
    "initialise_database_runtime",
]
