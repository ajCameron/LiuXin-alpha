"""Runtime composition helpers for the database package.

This module owns app-wiring concerns that the raw ``Database`` core should not need
to spell out inline: storage bootstrap and maintenance/runtime attachment.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from LiuXin_alpha.storage.store_manager import (
    StorageBootstrapIssue,
    StorageBootstrapReport,
    StorageManager,
)
from LiuXin_alpha.databases.maintenance.service import Maintainer
from LiuXin_alpha.utils.logging import default_log

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


def initialise_database_runtime(
    db: "DatabaseAPI",
    *,
    callback_proxy_cls: type,
        # Todo: preferences should have an API
    preferences_obj: Any,
) -> None:
    """
    Attach runtime collaborators to a concrete database instance.

    :param db:
    :param callback_proxy_cls:
    :param preferences_obj:
    :return:
    """

    db.maintenance = Maintainer(db)
    db.maintainer = db.maintenance
    # Todo: The database API should have write_telemetry declared
    db._maintainer_callback_proxy = callback_proxy_cls(db.maintenance, db.write_telemetry)
    # Todo: Protected method access
    db.driver.maintainer_callback = db._maintainer_callback_proxy
    db.clean = db.maintenance.clean

    db.preferences = preferences_obj

    # Ensure all major collaborators can refer back to the live database object.
    db.driver_wrapper.catalog = db
    db.driver.catalog = db
    db.macros.db = db
    db.metadata_sql.db = db


# Todo: As it mixes db and storage, should be in library - perhaps with a dummy for when we just want a db up
def bootstrap_storage_manager(
    db: "DatabaseAPI",
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
        db.storage.startup_on_add = bool(startup_on_add)

    try:
        report = db.storage.load_from_database(
            db,
            include_offline=include_offline,
            clear_existing=clear_existing,
            startup=startup_on_add,
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
    return report


__all__ = [
    "Maintainer",
    "StorageBootstrapReport",
    "StorageManager",
    "bootstrap_storage_manager",
    "initialise_database_runtime",
]
