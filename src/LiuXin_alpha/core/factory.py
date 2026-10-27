"""
Compose a direct Core runtime through its library owner or select an HTTP client.

Runtime construction can open/create a database and bootstrap services; remote
client selection only configures a client and does not start or probe a server.
"""

from __future__ import annotations

import pathlib
from collections.abc import Mapping
from typing import Any

from LiuXin_alpha.configuration import (
    ConfigurationPaths,
    current_preferences,
    use_preferences,
)
from LiuXin_alpha.core.api import CoreClientAPI
from LiuXin_alpha.core.proxies.remote import RemoteCoreClient
from LiuXin_alpha.core.runtime import CoreRuntime


def create_core(
    *,
    library: Any | None = None,
    database: Any | None = None,
    database_path: str | pathlib.Path | None = None,
    db_type: str = "SQLite",
    database_metadata: Mapping[str, Any] | None = None,
    create: bool = False,
    backup: bool = False,
    enable_storage_manager: bool = True,
    strict_storage_manager_bootstrap: bool = False,
    storage_startup_on_add: bool = False,
    enable_maintenance: bool = True,
    repair_bootstrap_rows: bool = True,
    catalog: Any | None = None,
    cache: Any | None = None,
    cache_type: str | None = None,
    cache_kwargs: Mapping[str, Any] | None = None,
    read_source: Any | None = None,
    cache_allow_database_fallback: bool = True,
    core_uuid: str | None = None,
    core_version: str = "2.0.0",
    api_version: str = "2.0",
    job_manager: Any | None = None,
    durable_job_path: str | pathlib.Path | None = None,
    close_job_manager_on_shutdown: bool | None = None,
    preferences: Any | None = None,
    configuration_paths: ConfigurationPaths | None = None,
    library_preferences: Any | None = None,
    field_metadata: Any | None = None,
    maintenance: Any | None = None,
) -> CoreRuntime:
    """
    Build a Core runtime around a borrowed library or an owned LibraryServices graph.

    A supplied library excludes database/database-path arguments and remains
    caller-owned at shutdown. Otherwise ``LibraryServices`` opens the database,
    storage, cache and Catalog graph and closes every owned resource in reverse
    order. A supplied database remains borrowed, while resources constructed
    around it remain owned by this Core session.

    Cache ownership follows CoreServices: a cache created by type is owned, while
    an injected cache is borrowed. The default job manager is process-global and
    is not shut down unless explicitly requested. Runtime-construction failure
    closes the already-open LibraryServices graph before propagating.

    Example:
        >>> runtime = create_core(database_path="library.sqlite", create=False)  # doctest: +SKIP
        >>> runtime.shutdown()  # doctest: +SKIP


    :param library: Optional existing library facade to borrow instead of constructing one.
    :param database: Existing database object wrapped when no library is supplied.
    :param database_path: Database file path or server connection/service selector used when opening a database.
    :param db_type: Driver selector for a database opened by the library.
    :param database_metadata: Optional connection metadata forwarded to Library; its server-database path consumes it.
    :param create: Request new database creation; existing files or populated server schemas are rejected.
    :param backup: Compatibility argument; ordinary opening never implicitly replaces a database.
    :param enable_storage_manager: Storage-manager setup flag for a newly opened database.
    :param strict_storage_manager_bootstrap: Whether new-database storage bootstrap should use strict failure handling.
    :param storage_startup_on_add: Whether newly bootstrapped stores should start as they are added.
    :param enable_maintenance: Maintenance-service setup flag for a newly opened database.
    :param repair_bootstrap_rows: Whether new-database bootstrap may repair its required rows.
    :param catalog: Optional Catalog service injected into CoreServices instead of lazy composition.
    :param cache: Optional borrowed cache; mutually exclusive with ``cache_type`` in CoreServices.
    :param cache_type: Optional cache implementation selector for an owned cache.
    :param cache_kwargs: Keyword options for cache construction when a cache type is supplied.
    :param read_source: Optional explicit metadata read source instead of deriving one from database/cache.
    :param cache_allow_database_fallback: Whether the composed cache-backed read source may use database fallback.
    :param core_uuid: Advertised identity override; the runtime generates a UUID for a falsey value.
    :param core_version: Advertised runtime implementation version, converted to text without version validation.
    :param api_version: Advertised command/query contract version, converted to text.
    :param job_manager: Optional job manager; omission uses the shared process-default manager.
    :param durable_job_path: Optional local ledger path; path-opened SQLite libraries default to a .jobs.sqlite sidecar, while server databases need an explicit ledger.
    :param close_job_manager_on_shutdown: Explicit manager-close policy; ``None`` currently defaults to false.
    :param preferences: Optional application store passed to database construction and CoreServices.
    :param configuration_paths: Fixed application/component paths; exclusive with an injected store. Settings are loaded before opening database/storage resources.
    :param library_preferences: Optional per-library preference service injected into CoreServices.
    :param field_metadata: Optional field-description service injected into CoreServices.
    :param maintenance: Optional maintenance facade injected independently of database-construction flags.
    :return: Initialized in-process runtime with named handlers registered and no HTTP server started.
    :raises ValueError: For conflicting/missing construction inputs or incompatible cache-selection arguments.
    """

    if configuration_paths is not None:
        if preferences is not None:
            raise ValueError("Provide preferences or configuration_paths, not both.")
        from LiuXin_alpha.preferences import Preferences

        preferences = Preferences(paths=configuration_paths, upgrade_on_load=False)
    if preferences is None and library is None and database is None:
        preferences = current_preferences()

    supplied_library = library is not None
    if supplied_library and (database is not None or database_path is not None):
        raise ValueError("Provide `library`, or a database/database_path, not both.")
    if durable_job_path is not None and (
        database_path is None or not enable_storage_manager
    ):
        raise ValueError(
            "durable_job_path requires a database_path and storage services"
        )
    library_services = None
    if library is None:
        from LiuXin_alpha.core.library_services import LibraryServices

        with use_preferences(preferences):
            library_services = LibraryServices.open(
                database=database,
                database_path=database_path,
                db_type=db_type,
                database_metadata=database_metadata,
                create=create,
                backup=backup,
                enable_storage_manager=enable_storage_manager,
                strict_storage_manager_bootstrap=strict_storage_manager_bootstrap,
                storage_startup_on_add=storage_startup_on_add,
                enable_maintenance=enable_maintenance,
                repair_bootstrap_rows=repair_bootstrap_rows,
                catalog=catalog,
                cache=cache,
                cache_type=cache_type,
                cache_kwargs=cache_kwargs,
                read_source=read_source,
                cache_allow_database_fallback=cache_allow_database_fallback,
                preferences=preferences,
                library_preferences=library_preferences,
                field_metadata=field_metadata,
                maintenance=maintenance,
            )
        library = library_services.library
    try:
        durable_jobs = None
        if database_path is not None and enable_storage_manager:
            if durable_job_path is None and str(db_type).lower().startswith("sqlite"):
                source_path = pathlib.Path(database_path).expanduser().resolve()
                durable_job_path = source_path.with_name(
                    source_path.name + ".jobs.sqlite"
                )
            if durable_job_path is not None:
                from LiuXin_alpha.core.durable_jobs import DurableJobService
                from LiuXin_alpha.core.library_services import LibraryServices

                worker_path = database_path
                ledger_path = pathlib.Path(durable_job_path).expanduser().resolve()
                if str(db_type).lower().startswith("sqlite"):
                    worker_path = pathlib.Path(database_path).expanduser().resolve()
                    if ledger_path == worker_path or (
                        ledger_path.exists()
                        and worker_path.exists()
                        and ledger_path.samefile(worker_path)
                    ):
                        raise ValueError(
                            "The job ledger must be separate from the library database"
                        )
                worker_metadata = dict(database_metadata or {})

                def open_worker_library():
                    # Each worker owns this graph; no main connection crosses threads.
                    with use_preferences(preferences):
                        return LibraryServices.open(
                            database_path=worker_path,
                            db_type=db_type,
                            database_metadata=worker_metadata,
                            create=False,
                            backup=False,
                            enable_storage_manager=True,
                            enable_maintenance=False,
                            storage_startup_on_add=True,
                            strict_storage_manager_bootstrap=True,
                            repair_bootstrap_rows=False,
                            preferences=preferences,
                        )

                durable_jobs = DurableJobService(
                    path=ledger_path,
                    library_uuid=str(library.db.uuid),
                    open_library=open_worker_library,
                )
        return CoreRuntime(
            library=library,
            core_uuid=core_uuid,
            core_version=core_version,
            api_version=api_version,
            job_manager=job_manager,
            durable_jobs=durable_jobs,
            close_job_manager_on_shutdown=close_job_manager_on_shutdown,
            catalog=catalog,
            cache=cache,
            cache_type=cache_type,
            cache_kwargs=cache_kwargs,
            read_source=read_source,
            preferences=preferences,
            library_preferences=library_preferences,
            field_metadata=field_metadata,
            maintenance=maintenance,
            cache_allow_database_fallback=cache_allow_database_fallback,
            close_library_on_shutdown=False,
            close_cache_on_shutdown=None,
            library_services=library_services,
        )
    except BaseException:
        if library_services is not None:
            library_services.close()
        raise


def core_client(
    *,
    runtime: CoreRuntime | None = None,
    endpoint: str | None = None,
    timeout_seconds: float = 10.0,
    bearer_token: str | None = None,
) -> CoreClientAPI:
    """
    Return a supplied runtime unchanged or construct an unconnected HTTP client, requiring exactly one choice.

    Selection tests ``None``, not truthiness. The remote constructor strips the
    endpoint's trailing slash and checks its HTTP(S) prefix; it does not verify
    reachability. Selecting a client neither transfers runtime ownership nor
    starts a server. A remote client's later shutdown call targets the server
    runtime, not merely a local connection.

    Example:
        >>> client = core_client(endpoint="http://127.0.0.1:8080/")
        >>> client.endpoint
        'http://127.0.0.1:8080'
        >>> core_client()
        Traceback (most recent call last):
        ...
        ValueError: Provide exactly one of `runtime` or `endpoint`.


    :param runtime: Existing in-process runtime to return directly, with no wrapper or health check.
    :param endpoint: HTTP(S) base URL for a new remote client, exclusive with ``runtime``.
    :param bearer_token: Optional HTTP server-role credential; unused for trusted local execution.
    :param timeout_seconds: Remote request timeout converted to float; ignored for a supplied runtime.
    :return: Supplied runtime or newly configured RemoteCoreClient sharing the client protocol.
    :raises ValueError: Unless exactly one source is supplied, or if the remote endpoint prefix is invalid.
    """

    if (runtime is None) == (endpoint is None):
        raise ValueError("Provide exactly one of `runtime` or `endpoint`.")
    if runtime is not None:
        return runtime
    assert endpoint is not None
    return RemoteCoreClient(
        endpoint=endpoint,
        timeout_seconds=timeout_seconds,
        bearer_token=bearer_token,
    )


__all__ = [
    "core_client",
    "create_core",
]
