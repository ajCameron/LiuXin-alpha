"""
Compose a direct Core runtime with explicit service ownership or select an HTTP client.

Runtime construction can open/create a database and bootstrap services; remote
client selection only configures a client and does not start or probe a server.
"""

from __future__ import annotations

import pathlib

from collections.abc import Mapping
from typing import Any

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
    close_job_manager_on_shutdown: bool | None = None,
    preferences: Any | None = None,
    library_preferences: Any | None = None,
    field_metadata: Any | None = None,
    maintenance: Any | None = None,
) -> CoreRuntime:
    """
    Build a Core runtime around a borrowed library or a newly constructed library wrapper.

    A supplied library excludes database/database-path arguments and remains
    caller-owned at shutdown. Otherwise ``Library`` requires exactly one database
    object or path and owns a database it opens itself. Core closes its constructed
    library wrapper, but that wrapper does not own a caller-supplied database.
    Library-construction options are unused when a library is supplied.

    Cache ownership follows CoreServices: a cache created by type is owned, while
    an injected cache is borrowed. The default job manager is process-global and
    is not shut down unless explicitly requested. If runtime construction fails
    after library creation, this factory supplies no compensating close/rollback.

    Example:
        >>> runtime = create_core(database_path="library.sqlite", create=False)  # doctest: +SKIP
        >>> runtime.shutdown()  # doctest: +SKIP


    :param library: Optional existing library facade to borrow instead of constructing one.
    :param database: Existing database object wrapped when no library is supplied.
    :param database_path: Database file path or server connection/service selector used when opening a database.
    :param db_type: Driver selector for a database opened by the library.
    :param database_metadata: Optional connection metadata forwarded to Library; its server-database path consumes it.
    :param create: Whether library database opening should create a database, potentially creating local parent directories.
    :param backup: Whether database opening should request the driver's backup behavior.
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
    :param close_job_manager_on_shutdown: Explicit manager-close policy; ``None`` currently defaults to false.
    :param preferences: Optional global preference service injected into CoreServices.
    :param library_preferences: Optional per-library preference service injected into CoreServices.
    :param field_metadata: Optional field-description service injected into CoreServices.
    :param maintenance: Optional maintenance facade injected independently of database-construction flags.
    :return: Initialized in-process runtime with named handlers registered and no HTTP server started.
    :raises ValueError: For conflicting/missing construction inputs or incompatible cache-selection arguments.
    """

    supplied_library = library is not None
    if supplied_library and (database is not None or database_path is not None):
        raise ValueError("Provide `library`, or a database/database_path, not both.")
    if library is None:
        from LiuXin_alpha.library import Library

        library = Library(
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
        )
    return CoreRuntime(
        library=library,
        core_uuid=core_uuid,
        core_version=core_version,
        api_version=api_version,
        job_manager=job_manager,
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
        close_library_on_shutdown=not supplied_library,
        close_cache_on_shutdown=None,
    )


def core_client(
    *,
    runtime: CoreRuntime | None = None,
    endpoint: str | None = None,
    timeout_seconds: float = 10.0,
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
    )


__all__ = [
    "core_client",
    "create_core",
]
