"""Public composition and client-selection helpers for Core."""

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
    """Compose the canonical in-process Core from a library or database."""

    supplied_library = library is not None
    if supplied_library and (
        database is not None or database_path is not None
    ):
        raise ValueError(
            "Provide `library`, or a database/database_path, not both."
        )
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
    """Select the same client contract for direct or HTTP RPC access."""

    if (runtime is None) == (endpoint is None):
        raise ValueError(
            "Provide exactly one of `runtime` or `endpoint`."
        )
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
