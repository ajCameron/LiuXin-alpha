"""
Compose remote HTML discovery, read-only Store declarations, and file registration.

Ensure helpers normalize the root, construct a backend, and upsert Store metadata
without crawling. Registration then delegates to the shared pipeline; Store rows
and incremental file writes can survive a later failure. This is catalogue
registration of discovered addresses, not copying ebook bytes or a transaction
covering the whole crawl. Database-path helpers own only their database context.
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
    """
    Read Unix wall time in truncated milliseconds for Store creation timestamps.

    Example:
        >>> isinstance(_now_ep_ms(), int)
        True


    :return: time.time() multiplied by 1000 and converted to int, not a monotonic clock.
    """
    return int(time.time() * 1000)


def _normalize_remote_root(url: str) -> str:
    """
    Require a root accepted by the shared HTTP(S) normalization policy.

    This does not probe reachability or inspect database state.

    Example:
        >>> _normalize_remote_root('HTTPS://example.test/books/#index')
        'https://example.test/books/'


    :param url: Candidate root normalized before any Store lookup/write in ensure helpers.
    :return: Accepted normalized address.
    :raises ValueError: Shared normalization rejects the root.
    """
    normalized = normalize_http_url(url)
    if normalized is None:
        raise ValueError("Remote store URL must be a valid safe HTTP(S) URL.")
    return normalized


def _table_columns(db, table_name: str) -> set[str]:
    """
    Materialize unique column headings reported by the database adapter.

    Example:
        >>> columns = _table_columns(database, 'stores')  # doctest: +SKIP


    :param db: Database exposing get_column_headings.
    :param table_name: Table whose headings are requested without normalization.
    :return: New heading set; adapter errors propagate.
    """
    return set(db.get_column_headings(table_name))


def _ensure_remote_store_schema_support(db) -> set[str]:
    """
    Require the stores table and its root-URI column before optional-field upsert.

    Other identity/policy/capability fields are not required or migrated here.

    Example:
        >>> columns = _ensure_remote_store_schema_support(database)  # doctest: +SKIP


    :param db: Database providing table and column metadata.
    :return: Available stores columns after the minimal contract passes.
    :raises InputIntegrityError: stores or store_root_uri is absent.
    """
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
    """
    Reuse the first exact-root Store row or insert supported read-only declaration fields.

    Existing rows use their allowed_columns and sync only when a common field
    differs; creation timestamps are added only for new rows. Existing modified
    time is not explicitly advanced. Matching is by root alone, so kind/name/
    policy/UUID may be overwritten, and duplicate roots are not reconciled.
    Lookup, assignment, and sync are not wrapped in a transaction or reservation.

    Example:
        >>> row, backend = ensure_native_html_readonly_store(database, root)  # doctest: +SKIP


    :param db: Row-backed database with the minimal stores schema.
    :param root: Exact normalized root used for lookup and stored URI.
    :param backend_name: Human-readable Store name to insert/update when supported.
    :param store_kind: Caller-selected persisted kind label.
    :param access_protocol: Persisted protocol label for this discovery backend.
    :param supports_checksums: Boolean coerced to an integer capability declaration.
    :param policy_json: Serialized backend settings stored without further validation/redaction.
    :param store_uuid: Backend identity text retained in supported identity columns.
    :return: First reused/synchronized row or newly inserted Row.
    """
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
    """
    Construct a wget-backed Store and upsert its declaration without running the crawler.

    Normalize the root before lookup and reuse the first matching row's UUID
    when nonempty; later matches are not searched for a usable identity.
    Generate a name for falsey store_name. Construct options/backend before
    the upsert's minimal schema check. Persist sorted policy JSON, including
    caller wget arguments without secret filtering. A custom store_kind changes
    the row label, not the actual backend class or policy backend identifier.

    Example:
        >>> row, backend = ensure_wget_html_readonly_store(database, 'https://example.test/books/')  # doctest: +SKIP


    :param db: Caller-owned database searched and updated through Row APIs.
    :param remote_url: HTTP(S) root normalized before Store selection.
    :param store_name: Explicit display name, or falsey to derive one from the root.
    :param store_kind: Persisted Store kind, defaulting to wget_html_readonly.
    :param max_http_requests_per_hour: Rate value, or None for the shared preference default.
    :param wget_exe: Executable selector recorded in options/policy without probing it.
    :param wget_args: Extra invocation tokens copied to a tuple and persisted as a JSON list.
    :param timeout_s: Crawl timeout seconds, or None without an explicit deadline.
    :param recurse: Boolean-coerced recursive spider policy.
    :param max_depth: Optional wget traversal level, interpreted when arguments are rendered.
    :param no_parent: Boolean-coerced root-parent restriction.
    :param span_hosts: Boolean-coerced permission for other same-scheme authorities.
    :param respect_robots: Boolean-coerced crawler robots policy.
    :param user_agent: Optional crawler/HTTP user-agent text.
    :param no_verbose: Boolean-coerced reduced diagnostic verbosity option.
    :param max_observed_urls: Positive extracted-observation ceiling passed to options.
    :param max_output_chars: Positive captured-character ceiling passed to options.
    :return: Persisted Store row and uncrawled backend instance.
    :raises ValueError: Root, options, or persisted backend identity is invalid.
    """
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
    """
    Construct a native-HTTP Store and upsert its declaration without fetching the root.

    Normalize before lookup, reuse a nonempty UUID from the first matching row,
    and derive a name for falsey store_name. Later rows are not searched for an
    identity. HTML bytes are int-converted and clamped to
    1024 here. Backend creation precedes the upsert's schema check. Policy JSON
    identifies native_html_readonly even if a custom row kind is requested.

    Example:
        >>> row, backend = ensure_native_html_readonly_store(database, 'https://example.test/books/')  # doctest: +SKIP


    :param db: Caller-owned database used for Store lookup and row persistence.
    :param remote_url: HTTP(S) root checked before Store database access.
    :param store_name: Display name, or falsey to derive it from the normalized root.
    :param store_kind: Persisted kind label, not a backend-class selector.
    :param max_http_requests_per_hour: Native rate value, or None for the shared preference.
    :param timeout_s: Per-request timeout seconds, or None without an explicit timeout.
    :param recurse: Boolean-coerced policy for descending discovered page links.
    :param max_depth: Optional descendant depth ceiling interpreted by the crawler.
    :param no_parent: Boolean-coerced same-authority root-path restriction.
    :param span_hosts: Boolean-coerced acceptance of other same-scheme authorities.
    :param respect_robots: Boolean-coerced robots policy, with crawler-defined failure behavior.
    :param user_agent: Optional request-header/robots-agent text.
    :param max_html_bytes: HTML-body byte limit, int-converted and clamped to at least 1024.
    :param max_pages: Positive dequeued-page ceiling passed to options.
    :param max_observed_urls: Positive raw-link observation ceiling passed to options.
    :return: Persisted Store row and native backend instance before startup/discovery.
    :raises ValueError: Root, options, numeric conversion, or persisted identity is invalid.
    """
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
    """
    Ensure a wget Store declaration, then register discovered ebook-suffix addresses.

    The Store upsert happens before file-schema/discovery validation. Pipeline
    writes are not rolled back on a later crawl failure, and this wrapper does
    not close the caller database/backend or copy remote ebook bytes. Progress
    errors follow the pipeline's best-effort callback policy.

    Example:
        >>> report = register_wget_html_readonly_store_files(database, 'https://example.test/books/')  # doctest: +SKIP


    :param db: Caller-owned catalogue database for Store/file/link updates.
    :param remote_url: Root normalized by the wget Store ensure helper.
    :param store_name: Display name, or falsey for generated naming.
    :param store_kind: Kind label persisted on the Store row.
    :param max_http_requests_per_hour: Crawl rate, or None for the shared preference default.
    :param wget_exe: Executable selector used by discovery.
    :param wget_args: Extra trusted subprocess arguments, also retained in policy JSON.
    :param timeout_s: Crawl-process timeout seconds, or None.
    :param recurse: Whether wget should recurse through discovered pages.
    :param max_depth: Optional recursive level, later clamped when rendered.
    :param no_parent: Whether root-parent traversal/results are restricted.
    :param span_hosts: Whether same-scheme foreign authorities may be traversed/accepted.
    :param respect_robots: Whether wget's robots policy remains enabled.
    :param user_agent: Optional request user-agent text.
    :param no_verbose: Whether to request reduced diagnostic verbosity.
    :param max_observed_urls: Extracted-URL occurrence ceiling for discovery.
    :param max_output_chars: Captured diagnostic-character ceiling.
    :param ebook_extensions: Registration suffix set, or None for BOOK_EXTENSIONS.
    :param source_label: Provenance label written to supported file_source columns.
    :param attach_store_links: Insert missing supported file-to-Store link rows.
    :param refresh_storage_manager: Attempt post-registration bootstrap with clear_existing=True.
    :param incremental_db_writes: Register from callbacks during crawling instead of after it returns.
    :param progress_callback: Optional event/report/detail consumer; receives mutable report state.
    :return: Registration counters/errors if discovery and finalization return normally.
    """
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
    """
    Ensure a native HTTP Store declaration and register accepted ebook-suffix addresses.

    No remote ebook bytes are copied or independently verified. Store metadata
    is persisted before file-schema validation and crawling; later errors can
    leave that row and incremental file/link effects. The caller retains database
    ownership. See the pipeline for per-file versus fatal error handling.

    Example:
        >>> report = register_native_html_readonly_store_files(database, 'https://example.test/books/')  # doctest: +SKIP


    :param db: Caller-owned catalogue database receiving Store/file/link updates.
    :param remote_url: HTTP(S) root normalized before Store lookup.
    :param store_name: Display name, or falsey to derive one from the root.
    :param store_kind: Persisted Store kind label.
    :param max_http_requests_per_hour: Rate value, or None for the shared preference.
    :param timeout_s: Per-request timeout seconds, or None.
    :param recurse: Whether discovered page-like links may be fetched recursively.
    :param max_depth: Optional descendant depth ceiling.
    :param no_parent: Restrict same-authority results to the root path.
    :param span_hosts: Permit other authorities with the same scheme.
    :param respect_robots: Consult robots rules under the native crawler's permissive failure policy.
    :param user_agent: Optional HTTP header and robots agent name.
    :param max_html_bytes: HTML-body byte ceiling, clamped to at least 1024 by the ensure helper.
    :param max_pages: Ceiling on dequeued unique normalized page URLs.
    :param max_observed_urls: Ceiling on raw parsed-link occurrences.
    :param ebook_extensions: Registration suffix collection, or None for BOOK_EXTENSIONS.
    :param source_label: File provenance label recorded where the schema supports it.
    :param attach_store_links: Whether to add missing supported file/Store association rows.
    :param refresh_storage_manager: Whether to attempt a clearing post-registration bootstrap.
    :param incremental_db_writes: Process accepted URLs during callbacks rather than after discovery.
    :param progress_callback: Optional event/report/details observer with best-effort delivery.
    :return: Mutable registration report after normal discovery/finalization completion.
    """
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
    """
    Open an existing catalogue context and delegate wget URL registration within it.

    Wrap database_path in pathlib.Path then str without expanduser/resolve; this
    is a filesystem-path convenience, not a general connection-string adapter.
    Database creation and backup are disabled. Construction/body/exit failures
    propagate, and completed registration writes are not rolled back by this
    wrapper. The report is returned only after database context exit succeeds.

    Example:
        >>> report = register_wget_html_readonly_with_database_path(database_path='catalogue.sqlite', remote_url='https://example.test/books/')  # doctest: +SKIP


    :param database_path: Existing catalogue path passed through Path/string conversion.
    :param remote_url: HTTP(S) discovery root checked by the delegated ensure helper.
    :param db_type: Database driver selector, defaulting to SQLite.
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
    :param progress_callback: Optional progress consumer invoked inside the database context.
    :return: Delegated registration report after successful database-context exit.
    """
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
    """
    Open an existing catalogue context and delegate native-crawler URL registration.

    Path conversion is lexical, without expanduser/resolve; create=False and
    backup=False are explicit. Constructor/body/cleanup exceptions propagate,
    and no all-registration transaction or rollback is introduced here.

    Example:
        >>> report = register_native_html_readonly_with_database_path(database_path='catalogue.sqlite', remote_url='https://example.test/books/')  # doctest: +SKIP


    :param database_path: Existing database path converted through pathlib.Path and str.
    :param remote_url: Root normalized by the delegated native Store ensure helper.
    :param db_type: Database driver selector, defaulting to SQLite.
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
