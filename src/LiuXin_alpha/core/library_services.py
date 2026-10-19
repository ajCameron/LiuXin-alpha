"""Own the complete resource graph for one open LiuXin library.

``LibraryServices`` is the single composition root for database-backed library
work.  It opens resources in dependency order and closes every resource it owns
in reverse order.  A failed open performs the same compensating teardown before
the original error is allowed to escape.
"""

from __future__ import annotations

import pathlib
from collections.abc import Callable, Mapping
from contextlib import ExitStack
from typing import Any, Self


class LibraryServicesCloseError(RuntimeError):
    """Report one or more cleanup failures after every closer was attempted."""

    def __init__(self, failures: tuple[tuple[str, BaseException], ...]) -> None:
        self.failures = failures
        summary = "; ".join(
            f"{name}: {type(error).__name__}: {error}" for name, error in failures
        )
        super().__init__(f"Library service cleanup failed ({summary})")


def _capture_close(
    failures: list[tuple[str, BaseException]],
    name: str,
    closer: Callable[[], Any],
) -> None:
    """Run one closer and retain its failure so later closers still run."""

    try:
        closer()
    except BaseException as error:
        failures.append((name, error))


def _stop_maintenance(maintenance: Any) -> None:
    """Stop an owned maintenance facade and briefly join its engine when present."""

    stop = getattr(maintenance, "stop", None)
    if callable(stop):
        stop()
    engine = getattr(maintenance, "maintainer", None)
    join = getattr(engine, "join", None)
    if callable(join):
        join(timeout=1)


def _close_owned_storage(database: Any, storage: Any) -> None:
    """Close an owned manager and remove its temporary database back-reference."""

    try:
        storage.close()
    finally:
        if getattr(database, "storage", None) is storage:
            database.storage = None
            database.storage_bootstrap_report = None


def _close_owned_maintenance(database: Any, maintenance: Any) -> None:
    """Stop owned maintenance and restore the database's disabled callback state."""

    try:
        _stop_maintenance(maintenance)
    finally:
        initialise = getattr(database, "_initialise_runtime_collaborators", None)
        if (
            callable(initialise)
            and getattr(database, "maintenance", None) is maintenance
        ):
            initialise(enable_maintenance=False)


class LibraryServices:
    """Own one Library and the database, storage, cache and Catalog behind it."""

    def __init__(
        self,
        *,
        library: Any,
        core_services: Any,
        database: Any,
        storage: Any | None,
        maintenance: Any | None,
        exit_stack: ExitStack,
        close_failures: list[tuple[str, BaseException]],
    ) -> None:
        self.library = library
        self.core_services = core_services
        self.database = database
        self.storage = storage
        self.maintenance = maintenance
        self._exit_stack = exit_stack
        self._close_failures = close_failures
        self._closed = False

    @classmethod
    def open(
        cls,
        *,
        database: Any | None = None,
        database_path: str | pathlib.Path | None = None,
        db_type: str = "SQLite",
        database_metadata: Mapping[str, Any] | None = None,
        create: bool = False,
        backup: bool = False,
        enable_storage_manager: bool = True,
        strict_storage_manager_bootstrap: bool = False,
        storage_startup_on_add: bool = False,
        repair_bootstrap_rows: bool = True,
        enable_maintenance: bool = True,
        catalog: Any | None = None,
        cache: Any | None = None,
        cache_type: str | None = None,
        cache_kwargs: Mapping[str, Any] | None = None,
        read_source: Any | None = None,
        cache_allow_database_fallback: bool = True,
        preferences: Any | None = None,
        library_preferences: Any | None = None,
        field_metadata: Any | None = None,
        maintenance: Any | None = None,
    ) -> Self:
        """Open one library graph and roll it back completely if any step fails."""

        if database is None and database_path is None:
            raise ValueError("Provide either `database` or `database_path`.")
        if database is not None and database_path is not None:
            raise ValueError("Provide only one of `database` or `database_path`.")

        stack = ExitStack()
        failures: list[tuple[str, BaseException]] = []

        def own(name: str, closer: Callable[[], Any]) -> None:
            stack.callback(_capture_close, failures, name, closer)

        try:
            if database is None:
                from LiuXin_alpha.databases.database import Database

                assert database_path is not None
                server_database = str(db_type).strip().casefold() in {
                    "postgres",
                    "postgresql",
                    "pg",
                }
                if server_database:
                    target = str(database_path)
                    metadata = {
                        "database_path": target,
                        **dict(database_metadata or {}),
                    }
                    if target.startswith(("postgres://", "postgresql://")):
                        metadata["postgres_url"] = target
                    else:
                        metadata["postgres_service"] = target
                else:
                    path = pathlib.Path(database_path).expanduser()
                    if create:
                        path.parent.mkdir(parents=True, exist_ok=True)
                    metadata = {"database_path": str(path)}
                database = Database(
                    metadata=metadata,
                    db_type=db_type,
                    create=create,
                    backup=backup,
                    enable_storage_manager=False,
                    enable_maintenance=False,
                    repair_bootstrap_rows=repair_bootstrap_rows,
                    application_preferences=preferences,
                )
                own("database", database.close)

            storage = getattr(database, "storage", None)
            if enable_storage_manager and storage is None:
                from LiuXin_alpha.storage.api import StorageManagementError
                from LiuXin_alpha.storage.durable_manager import (
                    StorageBootstrapIssue,
                    StorageBootstrapReport,
                    StorageManager,
                )
                from LiuXin_alpha.utils.logging import default_log

                storage = StorageManager(
                    db=database,
                    startup_on_add=storage_startup_on_add,
                )
                database.storage = storage
                own(
                    "storage",
                    lambda: _close_owned_storage(database, storage),
                )
                try:
                    report = storage.load_from_database(
                        database,
                        include_offline=False,
                        clear_existing=True,
                        startup=storage_startup_on_add,
                    )
                except Exception as error:
                    if strict_storage_manager_bootstrap:
                        raise
                    default_log.log_exception(
                        "Storage manager bootstrap failed.",
                        error,
                        "WARNING",
                        (
                            "database_path",
                            (
                                database.metadata.get("database_path")
                                if getattr(database, "metadata", None)
                                else None
                            ),
                        ),
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
                                str(error) or type(error).__name__,
                            ),
                        ),
                    )
                database.storage_bootstrap_report = report
                if strict_storage_manager_bootstrap and not report.ok:
                    details = "; ".join(
                        f"{issue.store_name or issue.store_ref or 'unknown Store'}: "
                        f"{issue.reason}"
                        for issue in report.issues
                    )
                    suffix = f" ({details})" if details else ""
                    raise StorageManagementError(
                        "Storage manager bootstrap failed for "
                        f"{report.failed_configurations} of "
                        f"{report.discovered_configurations} configured Stores{suffix}."
                    )

            active_maintenance = maintenance
            if active_maintenance is None:
                active_maintenance = getattr(database, "maintenance", None)
            if enable_maintenance and active_maintenance is None:
                initialise = getattr(
                    database, "_initialise_runtime_collaborators", None
                )
                if not callable(initialise):
                    raise TypeError(
                        "The database cannot attach an owned maintenance service."
                    )
                initialise(enable_maintenance=True)
                active_maintenance = database.maintenance
                own(
                    "maintenance",
                    lambda: _close_owned_maintenance(
                        database,
                        active_maintenance,
                    ),
                )

            from LiuXin_alpha.library import Library

            library = Library(
                database=database,
                storage=storage,
            )

            from LiuXin_alpha.core.services import CoreServices

            core_services = CoreServices(
                library=library,
                catalog=catalog,
                cache=cache,
                cache_type=cache_type,
                cache_kwargs=cache_kwargs,
                read_source=read_source,
                preferences=preferences,
                library_preferences=library_preferences,
                field_metadata=field_metadata,
                maintenance=active_maintenance,
                enable_maintenance=enable_maintenance,
                cache_allow_database_fallback=cache_allow_database_fallback,
                close_cache_on_shutdown=None,
                close_library_on_shutdown=False,
            )
            own("core services", core_services.close)

            # Catalog construction is part of opening the graph, not a later
            # request-dependent side effect.  It currently owns no close handle.
            _ = core_services.catalog

            return cls(
                library=library,
                core_services=core_services,
                database=database,
                storage=storage,
                maintenance=active_maintenance,
                exit_stack=stack.pop_all(),
                close_failures=failures,
            )
        except BaseException as startup_error:
            stack.close()
            if failures:
                cleanup_error = LibraryServicesCloseError(tuple(failures))
                raise BaseExceptionGroup(
                    "Library service startup and rollback failed",
                    [startup_error, cleanup_error],
                ) from None
            raise

    @property
    def closed(self) -> bool:
        """Return whether teardown has already been attempted."""

        return self._closed

    def close(self) -> None:
        """Close every owned resource once, even when an earlier close fails."""

        if self._closed:
            return
        self._closed = True
        self._exit_stack.close()
        if self._close_failures:
            raise LibraryServicesCloseError(tuple(self._close_failures))

    def __enter__(self) -> Self:
        """Return the already-open owner without changing its resource graph."""

        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        """Close the complete owned graph when its context exits."""

        del exc_type, exc, traceback
        self.close()


__all__ = ["LibraryServices", "LibraryServicesCloseError"]
