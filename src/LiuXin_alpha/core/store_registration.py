"""Compose one-shot store registration around an owned library graph.

Use these helpers with an existing catalogue path or PostgreSQL selector and
metadata. The active preference context is captured by the graph before opening.
Callers with an open graph should use the handle-based operations in ``ingest.remote_html``
or ``storage.reconcile`` instead. LibraryServices owns startup and teardown;
these helpers do not add an all-registration transaction or retry partial writes.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterable, Mapping, Sequence

from LiuXin_alpha.core.library_services import LibraryServices
from LiuXin_alpha.ingest.models import RemoteHtmlRegistrationReport
from LiuXin_alpha.ingest.native_disk import NativeDiskReport
from LiuXin_alpha.storage.reconcile.models import UnmanagedDiskRegistrationReport

type DiskProgressCallback = Callable[
    [str, UnmanagedDiskRegistrationReport, dict[str, object]], None
]
type NativeDiskProgressCallback = Callable[
    [str, NativeDiskReport, dict[str, object]], None
]
type HtmlProgressCallback = Callable[
    [str, RemoteHtmlRegistrationReport, dict[str, object]], None
]


def register_existing_disk_with_database_path(
    *,
    database_path: str | os.PathLike[str],
    disk_path: str | os.PathLike[str],
    db_type: str = "SQLite",
    database_metadata: Mapping[str, object] | None = None,
    store_name: str | None = None,
    store_kind: str = "on_disk_existing_unmanaged_drive",
    ebook_extensions: Iterable[str] | None = None,
    source_label: str = "on_disk_unmanaged_import",
    compute_hash: bool = True,
    follow_symlinks: bool = False,
    attach_store_links: bool = True,
    refresh_storage_manager: bool = True,
    progress_callback: NativeDiskProgressCallback | None = None,
) -> NativeDiskReport:
    """
    Open an existing database without backup, register the local tree, and close through its context
    manager.

    Forward registration policy unchanged. Construction, registration, and context-exit errors
    propagate; database lifetime management does not add an all-run transaction.

    Example:
        >>> report = register_existing_disk_with_database_path(database_path=catalogue, disk_path=root)  # doctest: +SKIP


    :param database_path: Existing file path or PostgreSQL selector resolved by LibraryServices.
    :param disk_path: Existing local directory used for the unmanaged Store and filesystem scan.
    :param db_type: Database adapter selector, defaulting to SQLite.
    :param database_metadata: Backend target/schema options forwarded without treating server selectors as paths.
    :param store_name: Truthy name override, otherwise a sanitized name derived from the root.
    :param store_kind: Legacy Store-kind label persisted independently of the concrete backend class.
    :param ebook_extensions: Accepted lowercase suffix policy; None chooses project defaults and an empty iterable selects none.
    :param source_label: Provenance label stored on accepted file rows.
    :param compute_hash: Must be true: native Asset identity requires SHA-256.
    :param follow_symlinks: Whether to descend linked directories; file symlinks are still handled by ordinary stat/open.
    :param attach_store_links: Must be true: native registration records a Replica location.
    :param refresh_storage_manager: Whether to call available bootstrap_storage_manager(clear_existing=True) after the scan.
    :param progress_callback: Best-effort synchronous observer receiving the live mutable report.
    :return: Registration report after successful delegated lifecycle-owner cleanup.
    """
    from LiuXin_alpha.ingest.native_disk import register_native_disk

    with LibraryServices.open(
        database_path=os.fspath(database_path),
        db_type=db_type,
        database_metadata=database_metadata,
        create=False,
        backup=False,
        enable_maintenance=False,
    ) as lifecycle:
        return register_native_disk(
            lifecycle.library,
            disk_path=disk_path,
            store_name=store_name,
            ebook_extensions=ebook_extensions,
            source_label=source_label,
            compute_hash=compute_hash,
            follow_symlinks=follow_symlinks,
            attach_store_links=attach_store_links,
            refresh_storage_manager=refresh_storage_manager,
            progress_callback=progress_callback,
        )


def register_rclone_http_readonly_with_database_path(
    *,
    database_path: str | os.PathLike[str],
    remote_url: str,
    db_type: str = "SQLite",
    database_metadata: Mapping[str, object] | None = None,
    store_name: str | None = None,
    store_kind: str = "rclone_http_readonly",
    max_http_requests_per_hour: float | None = None,
    apply_rclone_tpslimit: bool = True,
    rclone_tpslimit_burst: int = 1,
    enforce_global_rate_limit: bool = True,
    rclone_exe: str = "rclone",
    rclone_args: Sequence[str] | None = None,
    timeout_s: float | None = 60.0,
    max_inventory_entries: int = 100_000,
    max_json_token_chars: int = 8 * 1024 * 1024,
    ebook_extensions: Iterable[str] | None = None,
    source_label: str = "rclone_http_import",
    capture_hashes: bool = False,
    attach_store_links: bool = True,
    refresh_storage_manager: bool = True,
    progress_callback: DiskProgressCallback | None = None,
) -> UnmanagedDiskRegistrationReport:
    """
    Open an existing database without backup and delegate rclone inventory registration.

    LibraryServices owns the graph; this helper forwards policies and closes it before returning.
    Setup, inventory, or close failures can escape after prior writes.

    Example:
        >>> report = register_rclone_http_readonly_with_database_path(database_path=catalogue, remote_url="remote:")  # doctest: +SKIP


    :param database_path: Existing file path or PostgreSQL selector resolved by LibraryServices.
    :param remote_url: Remote selector trimmed here and parsed/validated by the concrete backend.
    :param db_type: Database adapter selector, defaulting to SQLite.
    :param database_metadata: Backend target/schema options forwarded without treating server selectors as paths.
    :param store_name: Truthy name override, otherwise a sanitized name derived from the root.
    :param store_kind: Legacy Store-kind label persisted independently of the concrete backend class.
    :param max_http_requests_per_hour: Rate ceiling; None resolves the configured default before backend construction.
    :param apply_rclone_tpslimit: Whether to add rclone TPS flags, bool-converted into options.
    :param rclone_tpslimit_burst: Burst option int-converted and clamped to at least one.
    :param enforce_global_rate_limit: Whether shared rate scheduling is enabled, bool-converted into options.
    :param rclone_exe: Executable selector recorded in backend options and policy JSON.
    :param rclone_args: Extra arguments materialized as a tuple, or an empty tuple for a falsey value.
    :param timeout_s: Per-command backend timeout in seconds, or None for no configured timeout.
    :param max_inventory_entries: Backend ceiling on inventory entries.
    :param max_json_token_chars: Backend JSON token character ceiling.
    :param ebook_extensions: Accepted lowercase suffix policy; None chooses project defaults and an empty iterable selects none.
    :param source_label: Provenance label stored on accepted file rows.
    :param capture_hashes: Whether to retain exact sha256 inventory digest claims.
    :param attach_store_links: Whether to add supported primary links for accepted file rows.
    :param refresh_storage_manager: Whether to call available bootstrap_storage_manager(clear_existing=True) after the scan.
    :param progress_callback: Best-effort synchronous observer receiving the live mutable report.
    :return: Delegated registration report after successful context exit.
    """
    from LiuXin_alpha.storage.reconcile.store_db_sync import (
        register_rclone_http_readonly_store_files,
    )

    with LibraryServices.open(
        database_path=os.fspath(database_path),
        db_type=db_type,
        database_metadata=database_metadata,
        create=False,
        backup=False,
        enable_maintenance=False,
    ) as lifecycle:
        db = lifecycle.database
        return register_rclone_http_readonly_store_files(
            db,
            remote_url=remote_url,
            store_name=store_name,
            store_kind=store_kind,
            max_http_requests_per_hour=max_http_requests_per_hour,
            apply_rclone_tpslimit=apply_rclone_tpslimit,
            rclone_tpslimit_burst=rclone_tpslimit_burst,
            enforce_global_rate_limit=enforce_global_rate_limit,
            rclone_exe=rclone_exe,
            rclone_args=rclone_args,
            timeout_s=timeout_s,
            max_inventory_entries=max_inventory_entries,
            max_json_token_chars=max_json_token_chars,
            ebook_extensions=ebook_extensions,
            source_label=source_label,
            capture_hashes=capture_hashes,
            attach_store_links=attach_store_links,
            refresh_storage_manager=refresh_storage_manager,
            progress_callback=progress_callback,
        )


def register_wget_html_readonly_with_database_path(
    *,
    database_path: str | os.PathLike[str],
    remote_url: str,
    db_type: str = "SQLite",
    database_metadata: Mapping[str, object] | None = None,
    store_name: str | None = None,
    store_kind: str = "wget_html_readonly",
    max_http_requests_per_hour: float | None = None,
    wget_exe: str = "wget",
    wget_args: Sequence[str] | None = None,
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
    ebook_extensions: Iterable[str] | None = None,
    source_label: str = "wget_html_import",
    attach_store_links: bool = True,
    refresh_storage_manager: bool = True,
    incremental_db_writes: bool = True,
    progress_callback: HtmlProgressCallback | None = None,
) -> RemoteHtmlRegistrationReport:
    """
    Open an existing catalogue context and delegate wget URL registration within it.

    LibraryServices normalizes the filesystem path, including user expansion.
    This helper does not accept general connection strings.
    Database creation and backup are disabled. Construction/body/exit failures
    propagate, and completed registration writes are not rolled back by this
    wrapper. The report is returned only after the delegated library owner closes.

    Example:
        >>> report = register_wget_html_readonly_with_database_path(database_path='catalogue.sqlite', remote_url='https://example.test/books/')  # doctest: +SKIP


    :param database_path: Existing file path or PostgreSQL selector resolved by LibraryServices.
    :param remote_url: HTTP(S) discovery root checked by the delegated ensure helper.
    :param db_type: Database driver selector, defaulting to SQLite.
    :param database_metadata: Backend target/schema options forwarded to the lifecycle owner.
    :param store_name: Store display name, or falsey for generated naming.
    :param store_kind: Persisted Store kind label.
    :param max_http_requests_per_hour: Crawl rate, or None for preference/default selection.
    :param wget_exe: Executable selector forwarded to discovery.
    :param wget_args: Extra trusted invocation tokens forwarded and persisted in policy.
    :param timeout_s: Crawl-process timeout seconds, or None.
    :param recurse: Whether recursive spider traversal is enabled.
    :param max_depth: Optional recursive wget level.
    :param no_parent: Root-parent restriction forwarded to backend options.
    :param span_hosts: Whether other same-scheme authorities are allowed.
    :param respect_robots: Whether crawler robots policy remains enabled.
    :param user_agent: Optional request-agent text.
    :param no_verbose: Request reduced wget diagnostic verbosity.
    :param max_observed_urls: Extracted URL-occurrence ceiling.
    :param max_output_chars: Retained diagnostic-character ceiling.
    :param ebook_extensions: Allowed registration suffixes, or None for the shared ebook set.
    :param source_label: File provenance label forwarded to registration.
    :param attach_store_links: Whether to add supported missing association rows.
    :param refresh_storage_manager: Whether to attempt post-write manager bootstrap.
    :param incremental_db_writes: Whether file registration occurs during crawl callbacks.
    :param progress_callback: Optional progress consumer invoked while the delegated library graph is open.
    :return: Delegated registration report after successful lifecycle-owner cleanup.
    """
    from LiuXin_alpha.ingest.remote_html import register_wget_html_readonly_store_files

    with LibraryServices.open(
        database_path=os.fspath(database_path),
        db_type=db_type,
        database_metadata=database_metadata,
        create=False,
        backup=False,
        enable_maintenance=False,
    ) as lifecycle:
        db = lifecycle.database
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
    database_path: str | os.PathLike[str],
    remote_url: str,
    db_type: str = "SQLite",
    database_metadata: Mapping[str, object] | None = None,
    store_name: str | None = None,
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
    ebook_extensions: Iterable[str] | None = None,
    source_label: str = "native_html_import",
    attach_store_links: bool = True,
    refresh_storage_manager: bool = True,
    incremental_db_writes: bool = True,
    progress_callback: HtmlProgressCallback | None = None,
) -> RemoteHtmlRegistrationReport:
    """
    Open an existing catalogue context and delegate native-crawler URL registration.

    LibraryServices normalizes the filesystem path, including user expansion.
    Creation and backup are disabled. Constructor/body/cleanup failures propagate,
    and no all-registration transaction or rollback is introduced here.

    Example:
        >>> report = register_native_html_readonly_with_database_path(database_path='catalogue.sqlite', remote_url='https://example.test/books/')  # doctest: +SKIP


    :param database_path: Existing file path or PostgreSQL selector resolved by LibraryServices.
    :param remote_url: Root normalized by the delegated native Store ensure helper.
    :param db_type: Database driver selector, defaulting to SQLite.
    :param database_metadata: Backend target/schema options forwarded to the lifecycle owner.
    :param store_name: Explicit display name, or falsey for a generated name.
    :param store_kind: Kind label stored on the configured Store row.
    :param max_http_requests_per_hour: Native request rate, or None for shared preference selection.
    :param timeout_s: Per-request timeout seconds, or None.
    :param recurse: Enable descendant page traversal.
    :param max_depth: Optional descendant depth ceiling.
    :param no_parent: Restrict same-authority results to the root path.
    :param span_hosts: Permit other same-scheme authorities.
    :param respect_robots: Enable native robots checks with their existing fail-open policy.
    :param user_agent: Optional header/robots agent string.
    :param max_html_bytes: HTML-body byte ceiling, clamped by the ensure helper.
    :param max_pages: Maximum dequeued normalized page URLs.
    :param max_observed_urls: Maximum raw parsed link occurrences.
    :param ebook_extensions: Registration suffix collection, or None for BOOK_EXTENSIONS.
    :param source_label: File provenance label for persisted rows.
    :param attach_store_links: Insert missing supported file/Store associations.
    :param refresh_storage_manager: Attempt clearing manager bootstrap after registration.
    :param incremental_db_writes: Register accepted callbacks immediately rather than after discovery.
    :param progress_callback: Optional observer receiving events while the database remains open.
    :return: Delegated report after registration and database-context cleanup succeed.
    """
    from LiuXin_alpha.ingest.remote_html import (
        register_native_html_readonly_store_files,
    )

    with LibraryServices.open(
        database_path=os.fspath(database_path),
        db_type=db_type,
        database_metadata=database_metadata,
        create=False,
        backup=False,
        enable_maintenance=False,
    ) as lifecycle:
        db = lifecycle.database
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
    "register_existing_disk_with_database_path",
    "register_rclone_http_readonly_with_database_path",
    "register_wget_html_readonly_with_database_path",
    "register_native_html_readonly_with_database_path",
]
