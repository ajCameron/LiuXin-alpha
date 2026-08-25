"""Public remote-HTML ingest entrypoints.

These wrappers sit at the ingest boundary. They orchestrate remote HTML
discovery sources plus the shared HTML ingest pipeline, but do not belong to
the storage reconciliation package.
"""

from __future__ import annotations

import json
import pathlib
import time

from collections.abc import Iterable, Sequence
from typing import Callable, Optional

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.ingest.models import RemoteHtmlRegistrationReport
from LiuXin_alpha.ingest.pipelines import ingest_html_discovery_store_files
from LiuXin_alpha.ingest.sources import get_default_crawler_http_requests_per_hour
from LiuXin_alpha.ingest.sources.html_common import normalize_http_url
from LiuXin_alpha.storage.store_backend_plugins.native_html_readonly import (
    NativeHtmlBackendOptions,
    NativeHtmlReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.wget_html_readonly import (
    WgetBackendOptions,
    WgetHtmlReadOnlyStorageBackend,
)
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


ProgressCallback = Callable[[str, RemoteHtmlRegistrationReport, dict[str, object]], None]


def _now_ep_ms() -> int:
    return int(time.time() * 1000)


def _normalize_remote_root(url: str) -> str:
    normalized = normalize_http_url(url)
    if normalized is None:
        raise ValueError("Remote store URL must be a valid safe HTTP(S) URL.")
    return normalized


def _table_columns(db, table_name: str) -> set[str]:
    return set(db.get_column_headings(table_name))


def _ensure_remote_store_schema_support(db) -> set[str]:
    tables = set(db.get_tables())
    if "stores" not in tables:
        raise InputIntegrityError("Database schema missing required table for remote HTML store bootstrap: stores")

    store_columns = _table_columns(db, "stores")
    missing_store_cols = sorted({"store_root_uri"} - store_columns)
    if missing_store_cols:
        raise InputIntegrityError("stores missing columns: {}".format(", ".join(missing_store_cols)))
    return store_columns


def _upsert_remote_store_row(
    db,
    *,
    root: str,
    backend_name: str,
    store_kind: str,
    access_protocol: str,
    supports_checksums: bool,
    policy_json: str,
    store_uuid: str,
) -> Row:
    store_columns = _ensure_remote_store_schema_support(db)

    common_payload = {
        "store_uuid": store_uuid,
        "store_name": backend_name,
        "store_kind": store_kind,
        "store_access_protocol": access_protocol,
        "store_root_uri": root,
        "store_is_read_only": 1,
        "store_online_status": "online",
        "store_supports_random_read": 1,
        "store_supports_random_write": 0,
        "store_supports_delete": 0,
        "store_supports_folders": 1,
        "store_supports_hierarchical_list": 1,
        "store_supports_checksums": 1 if supports_checksums else 0,
        "store_policy_json": policy_json,
    }

    store_rows = db.search("stores", "store_root_uri", root)
    if store_rows:
        store_row = store_rows[0]
        changed = False
        for key, value in common_payload.items():
            if key not in store_row.allowed_columns:
                continue
            if store_row[key] != value:
                store_row[key] = value
                changed = True
        if changed:
            store_row.sync()
        return store_row

    now_epk = _now_ep_ms()
    payload = {
        **common_payload,
        "store_created_timestamp_ep_k": now_epk,
        "store_modified_timestamp_ep_k": now_epk,
    }
    row_dict = {key: value for key, value in payload.items() if key in store_columns}
    return Row.from_idless_row_dict(db, row_dict=row_dict, table="stores")


def ensure_wget_html_readonly_store(
    db,
    remote_url: str,
    *,
    store_name: Optional[str] = None,
    store_kind: str = "wget_html_readonly",
    max_http_requests_per_hour: float | None = None,
    wget_exe: str = "wget",
    wget_args: Optional[Sequence[str]] = None,
    timeout_s: float | None = 300.0,
    recurse: bool = True,
    max_depth: int | None = None,
    no_parent: bool = True,
    span_hosts: bool = False,
    respect_robots: bool = True,
    user_agent: str | None = None,
    no_verbose: bool = True,
    max_observed_urls: int = 100_000,
    max_output_chars: int = 8 * 1024 * 1024,
) -> tuple[Row, WgetHtmlReadOnlyStorageBackend]:
    """Create or reuse a `stores` row and wget-backed read-only store for a remote URL."""
    root = _normalize_remote_root(remote_url)
    effective_max_http_requests_per_hour = (
        get_default_crawler_http_requests_per_hour()
        if max_http_requests_per_hour is None
        else max_http_requests_per_hour
    )

    backend_name = store_name or safe_path_to_name(root)
    existing_rows = db.search("stores", "store_root_uri", root)
    persisted_uuid = (
        None
        if not existing_rows
        or "store_uuid" not in existing_rows[0].allowed_columns
        or existing_rows[0]["store_uuid"] in (None, "")
        else str(existing_rows[0]["store_uuid"])
    )
    options = WgetBackendOptions(
        wget_exe=wget_exe,
        wget_args=tuple(wget_args or ()),
        timeout_s=timeout_s,
        max_http_requests_per_hour=effective_max_http_requests_per_hour,
        recurse=bool(recurse),
        max_depth=max_depth,
        no_parent=bool(no_parent),
        span_hosts=bool(span_hosts),
        respect_robots=bool(respect_robots),
        user_agent=user_agent,
        no_verbose=bool(no_verbose),
        max_observed_urls=max_observed_urls,
        max_output_chars=max_output_chars,
    )
    backend = WgetHtmlReadOnlyStorageBackend(
        url=root,
        name=backend_name,
        uuid=persisted_uuid,
        options=options,
    )
    policy_json = json.dumps(
        {
            "backend": "wget_html_readonly",
            "wget": {
                "max_http_requests_per_hour": options.max_http_requests_per_hour,
                "wget_exe": options.wget_exe,
                "wget_args": list(options.wget_args),
                "timeout_s": options.timeout_s,
                "recurse": bool(options.recurse),
                "max_depth": options.max_depth,
                "no_parent": bool(options.no_parent),
                "span_hosts": bool(options.span_hosts),
                "respect_robots": bool(options.respect_robots),
                "user_agent": options.user_agent,
                "no_verbose": bool(options.no_verbose),
                "max_observed_urls": options.max_observed_urls,
                "max_output_chars": options.max_output_chars,
            },
        },
        sort_keys=True,
    )
    store_row = _upsert_remote_store_row(
        db,
        root=root,
        backend_name=backend.configuration.store_name,
        store_kind=store_kind,
        access_protocol="wget",
        supports_checksums=False,
        policy_json=policy_json,
        store_uuid=str(backend.store_ref),
    )
    return store_row, backend


def ensure_native_html_readonly_store(
    db,
    remote_url: str,
    *,
    store_name: Optional[str] = None,
    store_kind: str = "native_html_readonly",
    max_http_requests_per_hour: float | None = None,
    timeout_s: float | None = 30.0,
    recurse: bool = True,
    max_depth: int | None = None,
    no_parent: bool = True,
    span_hosts: bool = False,
    respect_robots: bool = True,
    user_agent: str | None = None,
    max_html_bytes: int = 2_000_000,
    max_pages: int = 10_000,
    max_observed_urls: int = 100_000,
) -> tuple[Row, NativeHtmlReadOnlyStorageBackend]:
    """Create or reuse a `stores` row and native-HTTP read-only store for a remote URL."""
    root = _normalize_remote_root(remote_url)
    effective_max_http_requests_per_hour = (
        get_default_crawler_http_requests_per_hour()
        if max_http_requests_per_hour is None
        else max_http_requests_per_hour
    )

    backend_name = store_name or safe_path_to_name(root)
    existing_rows = db.search("stores", "store_root_uri", root)
    persisted_uuid = (
        None
        if not existing_rows
        or "store_uuid" not in existing_rows[0].allowed_columns
        or existing_rows[0]["store_uuid"] in (None, "")
        else str(existing_rows[0]["store_uuid"])
    )
    options = NativeHtmlBackendOptions(
        timeout_s=timeout_s,
        max_http_requests_per_hour=effective_max_http_requests_per_hour,
        recurse=bool(recurse),
        max_depth=max_depth,
        no_parent=bool(no_parent),
        span_hosts=bool(span_hosts),
        respect_robots=bool(respect_robots),
        user_agent=user_agent,
        max_html_bytes=max(1024, int(max_html_bytes)),
        max_pages=max_pages,
        max_observed_urls=max_observed_urls,
    )
    backend = NativeHtmlReadOnlyStorageBackend(
        url=root,
        name=backend_name,
        uuid=persisted_uuid,
        options=options,
    )
    policy_json = json.dumps(
        {
            "backend": "native_html_readonly",
            "native_html": {
                "timeout_s": options.timeout_s,
                "max_http_requests_per_hour": options.max_http_requests_per_hour,
                "recurse": bool(options.recurse),
                "max_depth": options.max_depth,
                "no_parent": bool(options.no_parent),
                "span_hosts": bool(options.span_hosts),
                "respect_robots": bool(options.respect_robots),
                "user_agent": options.user_agent,
                "max_html_bytes": int(options.max_html_bytes),
                "max_pages": options.max_pages,
                "max_observed_urls": options.max_observed_urls,
            },
        },
        sort_keys=True,
    )
    store_row = _upsert_remote_store_row(
        db,
        root=root,
        backend_name=backend.configuration.store_name,
        store_kind=store_kind,
        access_protocol="native_html",
        supports_checksums=False,
        policy_json=policy_json,
        store_uuid=str(backend.store_ref),
    )
    return store_row, backend


def register_wget_html_readonly_store_files(
    db,
    remote_url: str,
    *,
    store_name: Optional[str] = None,
    store_kind: str = "wget_html_readonly",
    max_http_requests_per_hour: float | None = None,
    wget_exe: str = "wget",
    wget_args: Optional[Sequence[str]] = None,
    timeout_s: float | None = 300.0,
    recurse: bool = True,
    max_depth: int | None = None,
    no_parent: bool = True,
    span_hosts: bool = False,
    respect_robots: bool = True,
    user_agent: str | None = None,
    no_verbose: bool = True,
    max_observed_urls: int = 100_000,
    max_output_chars: int = 8 * 1024 * 1024,
    ebook_extensions: Optional[Iterable[str]] = None,
    source_label: str = "wget_html_import",
    attach_store_links: bool = True,
    refresh_storage_manager: bool = True,
    incremental_db_writes: bool = True,
    progress_callback: Optional[ProgressCallback] = None,
) -> RemoteHtmlRegistrationReport:
    """Register ebook files discovered by a wget HTML spider store into `files`."""
    store_row, backend = ensure_wget_html_readonly_store(
        db,
        remote_url=remote_url,
        store_name=store_name,
        store_kind=store_kind,
        max_http_requests_per_hour=max_http_requests_per_hour,
        wget_exe=wget_exe,
        wget_args=wget_args,
        timeout_s=timeout_s,
        recurse=recurse,
        max_depth=max_depth,
        no_parent=no_parent,
        span_hosts=span_hosts,
        respect_robots=respect_robots,
        user_agent=user_agent,
        no_verbose=no_verbose,
        max_observed_urls=max_observed_urls,
        max_output_chars=max_output_chars,
    )
    return ingest_html_discovery_store_files(
        db,
        store_row=store_row,
        store_url=str(backend.url),
        store_name_value=store_row["store_name"] if "store_name" in store_row.allowed_columns else backend.configuration.store_name,
        discovery_source=backend,
        mode="wget",
        ebook_extensions=ebook_extensions,
        source_label=source_label,
        attach_store_links=attach_store_links,
        refresh_storage_manager=refresh_storage_manager,
        incremental_db_writes=incremental_db_writes,
        progress_callback=progress_callback,
    )


def register_native_html_readonly_store_files(
    db,
    remote_url: str,
    *,
    store_name: Optional[str] = None,
    store_kind: str = "native_html_readonly",
    max_http_requests_per_hour: float | None = None,
    timeout_s: float | None = 30.0,
    recurse: bool = True,
    max_depth: int | None = None,
    no_parent: bool = True,
    span_hosts: bool = False,
    respect_robots: bool = True,
    user_agent: str | None = None,
    max_html_bytes: int = 2_000_000,
    max_pages: int = 10_000,
    max_observed_urls: int = 100_000,
    ebook_extensions: Optional[Iterable[str]] = None,
    source_label: str = "native_html_import",
    attach_store_links: bool = True,
    refresh_storage_manager: bool = True,
    incremental_db_writes: bool = True,
    progress_callback: Optional[ProgressCallback] = None,
) -> RemoteHtmlRegistrationReport:
    """Register ebook files discovered by the native HTML crawler into `files`."""
    store_row, backend = ensure_native_html_readonly_store(
        db,
        remote_url=remote_url,
        store_name=store_name,
        store_kind=store_kind,
        max_http_requests_per_hour=max_http_requests_per_hour,
        timeout_s=timeout_s,
        recurse=recurse,
        max_depth=max_depth,
        no_parent=no_parent,
        span_hosts=span_hosts,
        respect_robots=respect_robots,
        user_agent=user_agent,
        max_html_bytes=max_html_bytes,
        max_pages=max_pages,
        max_observed_urls=max_observed_urls,
    )
    return ingest_html_discovery_store_files(
        db,
        store_row=store_row,
        store_url=str(backend.url),
        store_name_value=store_row["store_name"] if "store_name" in store_row.allowed_columns else backend.configuration.store_name,
        discovery_source=backend,
        mode="native_html",
        ebook_extensions=ebook_extensions,
        source_label=source_label,
        attach_store_links=attach_store_links,
        refresh_storage_manager=refresh_storage_manager,
        incremental_db_writes=incremental_db_writes,
        progress_callback=progress_callback,
    )


def register_wget_html_readonly_with_database_path(
    *,
    database_path: str | pathlib.Path,
    remote_url: str,
    db_type: str = "SQLite",
    store_name: Optional[str] = None,
    store_kind: str = "wget_html_readonly",
    max_http_requests_per_hour: float | None = None,
    wget_exe: str = "wget",
    wget_args: Optional[Sequence[str]] = None,
    timeout_s: float | None = 300.0,
    recurse: bool = True,
    max_depth: int | None = None,
    no_parent: bool = True,
    span_hosts: bool = False,
    respect_robots: bool = True,
    user_agent: str | None = None,
    no_verbose: bool = True,
    max_observed_urls: int = 100_000,
    max_output_chars: int = 8 * 1024 * 1024,
    ebook_extensions: Optional[Iterable[str]] = None,
    source_label: str = "wget_html_import",
    attach_store_links: bool = True,
    refresh_storage_manager: bool = True,
    incremental_db_writes: bool = True,
    progress_callback: Optional[ProgressCallback] = None,
) -> RemoteHtmlRegistrationReport:
    """Open a database and ingest files from a wget HTML store."""
    from LiuXin_alpha.databases.database import Database

    metadata = {"database_path": str(pathlib.Path(database_path))}
    with Database(metadata=metadata, db_type=db_type, create=False, backup=False) as db:
        return register_wget_html_readonly_store_files(
            db,
            remote_url=remote_url,
            store_name=store_name,
            store_kind=store_kind,
            max_http_requests_per_hour=max_http_requests_per_hour,
            wget_exe=wget_exe,
            wget_args=wget_args,
            timeout_s=timeout_s,
            recurse=recurse,
            max_depth=max_depth,
            no_parent=no_parent,
            span_hosts=span_hosts,
            respect_robots=respect_robots,
            user_agent=user_agent,
            no_verbose=no_verbose,
            max_observed_urls=max_observed_urls,
            max_output_chars=max_output_chars,
            ebook_extensions=ebook_extensions,
            source_label=source_label,
            attach_store_links=attach_store_links,
            refresh_storage_manager=refresh_storage_manager,
            incremental_db_writes=incremental_db_writes,
            progress_callback=progress_callback,
        )


def register_native_html_readonly_with_database_path(
    *,
    database_path: str | pathlib.Path,
    remote_url: str,
    db_type: str = "SQLite",
    store_name: Optional[str] = None,
    store_kind: str = "native_html_readonly",
    max_http_requests_per_hour: float | None = None,
    timeout_s: float | None = 30.0,
    recurse: bool = True,
    max_depth: int | None = None,
    no_parent: bool = True,
    span_hosts: bool = False,
    respect_robots: bool = True,
    user_agent: str | None = None,
    max_html_bytes: int = 2_000_000,
    max_pages: int = 10_000,
    max_observed_urls: int = 100_000,
    ebook_extensions: Optional[Iterable[str]] = None,
    source_label: str = "native_html_import",
    attach_store_links: bool = True,
    refresh_storage_manager: bool = True,
    incremental_db_writes: bool = True,
    progress_callback: Optional[ProgressCallback] = None,
) -> RemoteHtmlRegistrationReport:
    """Open a database and ingest files from a native HTML crawler store."""
    from LiuXin_alpha.databases.database import Database

    metadata = {"database_path": str(pathlib.Path(database_path))}
    with Database(metadata=metadata, db_type=db_type, create=False, backup=False) as db:
        return register_native_html_readonly_store_files(
            db,
            remote_url=remote_url,
            store_name=store_name,
            store_kind=store_kind,
            max_http_requests_per_hour=max_http_requests_per_hour,
            timeout_s=timeout_s,
            recurse=recurse,
            max_depth=max_depth,
            no_parent=no_parent,
            span_hosts=span_hosts,
            respect_robots=respect_robots,
            user_agent=user_agent,
            max_html_bytes=max_html_bytes,
            max_pages=max_pages,
            max_observed_urls=max_observed_urls,
            ebook_extensions=ebook_extensions,
            source_label=source_label,
            attach_store_links=attach_store_links,
            refresh_storage_manager=refresh_storage_manager,
            incremental_db_writes=incremental_db_writes,
            progress_callback=progress_callback,
        )


__all__ = [
    "ensure_wget_html_readonly_store",
    "ensure_native_html_readonly_store",
    "register_wget_html_readonly_store_files",
    "register_wget_html_readonly_with_database_path",
    "register_native_html_readonly_store_files",
    "register_native_html_readonly_with_database_path",
]
