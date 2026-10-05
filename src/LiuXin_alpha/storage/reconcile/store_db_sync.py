"""
Register local or rclone-discovered ebook paths in the legacy catalogue tables.

Store declarations, file upserts, and optional primary links are incremental;
there is no all-run transaction or removal of entries absent from a later scan.
Local classification follows path suffixes, and remote registration follows
inventory Location keys. Neither proves ebook format validity. Callbacks observe
live reports, while ordinary callback errors are ignored.

The local hash helper currently returns SHA-512 hex plus size even though the
payload field is named file_hash_sha256. Remote digest capture trusts an inventory
claim named sha256. These existing behaviors are distinct from trusted SHA-256
verification. The standalone main entry point exposes local registration only.

Example:
    >>> report = register_existing_disk_as_unmanaged_store(db, disk_root, compute_hash=False)  # doctest: +SKIP
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import pathlib
import time

from datetime import datetime
from typing import Callable, Iterable, Optional, Sequence
from urllib.parse import parse_qs, unquote, urlparse

from LiuXin_alpha.constants.file_extensions import BOOK_EXTENSIONS
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.storage.reconcile.models import UnmanagedDiskRegistrationReport
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive import (
    OnDiskUnmanagedStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import (
    RcloneBackendOptions,
    RcloneHttpReadOnlyStorageBackend,
    get_default_rclone_http_requests_per_hour,
)
from LiuXin_alpha.utils.storage.local.file_properties import get_file_hash
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


ProgressCallback = Callable[[str, UnmanagedDiskRegistrationReport, dict[str, object]], None]
_HTML_LIKE_EXTENSIONS = {"htm", "html", "htmlz", "xhtm", "xhtml"}


def _now_ep_ms() -> int:
    """
    Read wall-clock Unix-epoch milliseconds, truncating fractional milliseconds.

    Example:
        >>> isinstance(_now_ep_ms(), int)
        True


    :return: Current epoch milliseconds, without a monotonicity guarantee.
    """
    return int(time.time() * 1000)


def _epoch_ms_from_seconds(value: float | int | None) -> Optional[int]:
    """
    Float-convert seconds to integer epoch milliseconds while preserving None.

    Conversion and nonfinite-value errors propagate; no timezone interpretation occurs.

    Example:
        >>> _epoch_ms_from_seconds(1.25)
        1250


    :param value: Seconds since the epoch, or None for missing metadata.
    :return: Milliseconds truncated toward zero, or None.
    """
    if value is None:
        return None
    return int(float(value) * 1000.0)


def _normalize_ebook_extensions(ebook_extensions: Optional[Iterable[str]]) -> set[str]:
    """
    Build a lowercase suffix set, using BOOK_EXTENSIONS only for None.

    Skip values whose string is blank after stripping, but retain whitespace on accepted strings.
    Remove leading dots without stripping surrounding spaces; an explicit empty iterable stays
    empty.

    Example:
        >>> _normalize_ebook_extensions(['.EPUB', 'txt', '']) == {'epub', 'txt'}
        True


    :param ebook_extensions: Iterable of suffix-like values, or None for the project defaults.
    :return: Normalized membership set used for extension filtering.
    """
    if ebook_extensions is None:
        ebook_extensions = BOOK_EXTENSIONS
    return {str(x).lower().lstrip(".") for x in ebook_extensions if str(x).strip()}


def _normalize_root(path: str | os.PathLike[str]) -> pathlib.Path:
    """
    Expand a local path, require an existing directory, then resolve it.

    Existence/type checks and subsequent scanning are separate filesystem observations. Missing
    paths raise FileNotFoundError and non-directories raise NotADirectoryError.

    Example:
        >>> root = _normalize_root(existing_directory)  # doctest: +SKIP


    :param path: Local directory path; URI parsing is not performed.
    :return: Resolved absolute Path after validation.
    """
    root = pathlib.Path(path).expanduser()
    if not root.exists():
        raise FileNotFoundError("Disk path does not exist: {!r}".format(str(root)))
    if not root.is_dir():
        raise NotADirectoryError("Disk path is not a directory: {!r}".format(str(root)))
    return root.resolve()


def _normalize_remote_root(url: str) -> str:
    """
    Stringify and trim the remote selector, rejecting only blank text here.

    Address syntax, credential policy, and backend support are checked by the backend constructor
    later.

    Example:
        >>> _normalize_remote_root(' remote:books ')
        'remote:books'


    :param url: Remote selector accepted as text before backend-specific parsing.
    :return: Nonblank stripped selector; blank text raises ValueError.
    """
    text = str(url).strip()
    if not text:
        raise ValueError("Remote store URL cannot be blank.")
    return text


def _coerce_datetime_ep_ms(value: object) -> Optional[int]:
    """
    Parse nonblank ISO-like text with Z replaced by +00:00, then convert to epoch milliseconds.

    Parsing Exceptions return None, while stringification or timestamp-conversion failures can
    propagate. Naive datetimes use the process local timezone.

    Example:
        >>> _coerce_datetime_ep_ms('1970-01-01T00:00:01Z')
        1000


    :param value: Timestamp-like object stringified for datetime.fromisoformat, or None.
    :return: Truncated epoch milliseconds or None for absent/unparseable text.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    return int(dt.timestamp() * 1000.0)


def _infer_remote_access_protocol(remote_url: str) -> str:
    """
    Label text as HTTPS, HTTP, or rclone using case-insensitive prefix/substring heuristics.

    Check HTTPS first; this does not parse or validate an address.

    Example:
        >>> _infer_remote_access_protocol('remote:')
        'rclone'


    :param remote_url: Remote selector examined for an HTTP scheme or url= component.
    :return: The metadata label https, http, or rclone.
    """
    lowered = remote_url.lower()
    if "url=https://" in lowered or lowered.startswith("https://"):
        return "https"
    if "url=http://" in lowered or lowered.startswith("http://"):
        return "http"
    return "rclone"


def _guess_remote_url_extension(candidate_url: str) -> str:
    """
    Select the last suffix found in the decoded URL path and query values.

    Query-value suffixes override the path suffix, regardless of parameter name. parse_qs already
    decodes values before a further unquote; this heuristic does not establish file type and is
    separate from the live inventory-key filter.

    Example:
        >>> _guess_remote_url_extension('https://example.invalid/download.html?name=book.epub')
        'epub'


    :param candidate_url: URL-like text, with a falsey value treated as empty.
    :return: Lowercase suffix without its dot, or an empty string when none is found.
    """
    parsed = urlparse(str(candidate_url or "").strip())
    suffixes: list[str] = []

    path_text = unquote(parsed.path or "")
    path_ext = pathlib.PurePosixPath(path_text).suffix.lower().lstrip(".")
    if path_ext:
        suffixes.append(path_ext)

    query_values = parse_qs(parsed.query or "", keep_blank_values=True)
    for values in query_values.values():
        for raw_value in values:
            value_text = unquote(str(raw_value or ""))
            value_ext = pathlib.PurePosixPath(value_text).suffix.lower().lstrip(".")
            if value_ext:
                suffixes.append(value_ext)

    if not suffixes:
        return ""
    return suffixes[-1]


def _table_columns(db, table_name: str) -> set[str]:
    """
    Read the database-advertised column headings as a set.

    Example:
        >>> columns = _table_columns(db, "files")  # doctest: +SKIP


    :param db: Database facade supplying get_column_headings.
    :param table_name: Table whose headings are requested without independent validation.
    :return: Unique column-name set; database failures propagate.
    """
    return set(db.get_column_headings(table_name))


def _ensure_schema_support(db) -> tuple[set[str], set[str], set[str], set[str]]:
    """
    Require stores/files tables and their minimal registration address columns.

    Read optional file_store_links headings when present. This is a partial schema check, not
    validation of every field subsequently used; missing required tables/columns raise
    InputIntegrityError.

    Example:
        >>> tables, stores, files, links = _ensure_schema_support(db)  # doctest: +SKIP


    :param db: Database facade queried for table and column names.
    :return: Table names, Store columns, file columns, and optional link columns as four sets.
    """
    tables = set(db.get_tables())
    required_tables = {"stores", "files"}
    missing_tables = sorted(required_tables - tables)
    if missing_tables:
        raise InputIntegrityError(
            "Database schema missing required tables for unmanaged ingestion: {}".format(", ".join(missing_tables))
        )

    store_columns = _table_columns(db, "stores")
    file_columns = _table_columns(db, "files")
    link_columns = _table_columns(db, "file_store_links") if "file_store_links" in tables else set()

    required_store_columns = {"store_root_uri"}
    required_file_columns = {"file_store_id", "file_storage_key"}
    missing_store_cols = sorted(required_store_columns - store_columns)
    missing_file_cols = sorted(required_file_columns - file_columns)
    if missing_store_cols or missing_file_cols:
        err_chunks = []
        if missing_store_cols:
            err_chunks.append("stores missing columns: {}".format(", ".join(missing_store_cols)))
        if missing_file_cols:
            err_chunks.append("files missing columns: {}".format(", ".join(missing_file_cols)))
        raise InputIntegrityError("; ".join(err_chunks))

    return tables, store_columns, file_columns, link_columns


def ensure_unmanaged_store_for_disk(
    db,
    disk_path: str | os.PathLike[str],
    *,
    store_name: Optional[str] = None,
    store_kind: str = "on_disk_existing_unmanaged_drive",
) -> tuple[Row, OnDiskUnmanagedStorageBackend]:
    """
    Create or refresh the first Store row matching a resolved existing disk directory.

    Reuse a nonempty UUID from that first row and update supported declaration fields, marking it
    online without an explicit health probe here. Other matching rows are ignored. Construction and
    row errors propagate; no file scan or encompassing transaction is added.

    Example:
        >>> row, backend = ensure_unmanaged_store_for_disk(db, disk_root)  # doctest: +SKIP


    :param db: Borrowed database facade receiving incremental row operations.
    :param disk_path: Existing local directory expanded/resolved before lookup.
    :param store_name: Truthy name override, otherwise a sanitized name derived from the root.
    :param store_kind: Legacy Store-kind label persisted independently of the concrete backend class.
    :return: Persisted Store row and constructed unmanaged backend.
    """
    _ensure_schema_support(db)
    root = _normalize_root(disk_path)

    backend_name = store_name or safe_path_to_name(str(root))
    backend = OnDiskUnmanagedStorageBackend(url=str(root), name=backend_name)

    store_rows = db.search("stores", "store_root_uri", str(root))
    if store_rows:
        store_row = store_rows[0]
        persisted_uuid = store_row["store_uuid"] if "store_uuid" in store_row.allowed_columns else None
        if persisted_uuid not in (None, ""):
            backend = OnDiskUnmanagedStorageBackend(
                url=str(root),
                name=backend_name,
                uuid=str(persisted_uuid),
            )
        updates = {
            "store_uuid": str(backend.store_ref),
            "store_name": backend.configuration.store_name,
            "store_kind": store_kind,
            "store_access_protocol": "file",
            "store_root_uri": str(root),
            "store_operational_role": "live",
            "store_is_read_only": 1,
            "store_online_status": "online",
            "store_supports_random_read": 1,
            "store_supports_random_write": 0,
            "store_supports_delete": 0,
            "store_supports_folders": 1,
            "store_supports_checksums": 1,
        }
        changed = False
        for key, value in updates.items():
            if key not in store_row.allowed_columns:
                continue
            if store_row[key] != value:
                store_row[key] = value
                changed = True
        if changed:
            store_row.sync()
        return store_row, backend

    store_columns = _table_columns(db, "stores")
    now_epk = _now_ep_ms()
    payload = {
        "store_uuid": str(backend.store_ref),
        "store_name": backend.configuration.store_name,
        "store_kind": store_kind,
        "store_access_protocol": "file",
        "store_root_uri": str(root),
        "store_is_read_only": 1,
        "store_online_status": "online",
        "store_supports_random_read": 1,
        "store_supports_random_write": 0,
        "store_supports_delete": 0,
        "store_supports_folders": 1,
        "store_supports_checksums": 1,
        "store_created_timestamp_ep_k": now_epk,
        "store_modified_timestamp_ep_k": now_epk,
    }
    row_dict = {key: value for key, value in payload.items() if key in store_columns}
    store_row = Row.from_idless_row_dict(db, row_dict=row_dict, table="stores")
    return store_row, backend


def ensure_rclone_http_readonly_store(
    db,
    remote_url: str,
    *,
    store_name: Optional[str] = None,
    store_kind: str = "rclone_http_readonly",
    max_http_requests_per_hour: float | None = None,
    apply_rclone_tpslimit: bool = True,
    rclone_tpslimit_burst: int = 1,
    enforce_global_rate_limit: bool = True,
    rclone_exe: str = "rclone",
    rclone_args: Optional[Sequence[str]] = None,
    timeout_s: float | None = 60.0,
    max_inventory_entries: int = 100_000,
    max_json_token_chars: int = 8 * 1024 * 1024,
) -> tuple[Row, RcloneHttpReadOnlyStorageBackend]:
    """
    Create or refresh the first Store row for a validated rclone read-only selector.

    Construct the backend before database writes and reuse a nonempty UUID from the first matching
    row. Persist supported capability fields and sorted policy JSON, including executable/arguments;
    the declaration is marked online without inventory enumeration here. Callback/rate enforcement
    belongs to later backend operations.

    Example:
        >>> row, backend = ensure_rclone_http_readonly_store(db, "remote:")  # doctest: +SKIP


    :param db: Borrowed database facade receiving incremental row operations.
    :param remote_url: Remote selector trimmed here and parsed/validated by the concrete backend.
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
    :return: Persisted Store row and rclone HTTP backend; setup failures propagate.
    """
    _ensure_schema_support(db)
    root = _normalize_remote_root(remote_url)
    effective_max_http_requests_per_hour = (
        get_default_rclone_http_requests_per_hour()
        if max_http_requests_per_hour is None
        else max_http_requests_per_hour
    )

    backend_name = store_name or safe_path_to_name(root)
    options = RcloneBackendOptions(
        rclone_exe=rclone_exe,
        rclone_args=tuple(rclone_args or ()),
        timeout_s=timeout_s,
        max_http_requests_per_hour=effective_max_http_requests_per_hour,
        apply_rclone_tpslimit=bool(apply_rclone_tpslimit),
        rclone_tpslimit_burst=max(1, int(rclone_tpslimit_burst)),
        enforce_global_rate_limit=bool(enforce_global_rate_limit),
        max_inventory_entries=max_inventory_entries,
        max_json_token_chars=max_json_token_chars,
    )
    backend = RcloneHttpReadOnlyStorageBackend(url=root, name=backend_name, options=options)

    store_columns = _table_columns(db, "stores")
    policy_payload = {
        "backend": "rclone_http_readonly",
        "rclone": {
            "max_http_requests_per_hour": options.max_http_requests_per_hour,
            "apply_rclone_tpslimit": options.apply_rclone_tpslimit,
            "rclone_tpslimit_burst": options.rclone_tpslimit_burst,
            "enforce_global_rate_limit": options.enforce_global_rate_limit,
            "rclone_exe": options.rclone_exe,
            "rclone_args": list(options.rclone_args),
            "timeout_s": options.timeout_s,
            "max_inventory_entries": options.max_inventory_entries,
            "max_json_token_chars": options.max_json_token_chars,
        },
    }
    policy_json = json.dumps(policy_payload, sort_keys=True)

    store_rows = db.search("stores", "store_root_uri", root)
    if store_rows:
        store_row = store_rows[0]
        persisted_uuid = store_row["store_uuid"] if "store_uuid" in store_row.allowed_columns else None
        if persisted_uuid not in (None, ""):
            backend = RcloneHttpReadOnlyStorageBackend(
                url=root,
                name=backend_name,
                uuid=str(persisted_uuid),
                options=options,
            )
        updates = {
            "store_uuid": str(backend.store_ref),
            "store_name": backend.configuration.store_name,
            "store_kind": store_kind,
            "store_access_protocol": _infer_remote_access_protocol(root),
            "store_root_uri": root,
            "store_is_read_only": 1,
            "store_online_status": "online",
            "store_supports_random_read": 1,
            "store_supports_random_write": 0,
            "store_supports_delete": 0,
            "store_supports_folders": 1,
            "store_supports_hierarchical_list": 1,
            "store_supports_checksums": 1,
            "store_policy_json": policy_json,
        }
        changed = False
        for key, value in updates.items():
            if key not in store_row.allowed_columns:
                continue
            if store_row[key] != value:
                store_row[key] = value
                changed = True
        if changed:
            store_row.sync()
        return store_row, backend

    now_epk = _now_ep_ms()
    payload = {
        "store_uuid": str(backend.store_ref),
        "store_name": backend.configuration.store_name,
        "store_kind": store_kind,
        "store_access_protocol": _infer_remote_access_protocol(root),
        "store_root_uri": root,
        "store_is_read_only": 1,
        "store_online_status": "online",
        "store_supports_random_read": 1,
        "store_supports_random_write": 0,
        "store_supports_delete": 0,
        "store_supports_folders": 1,
        "store_supports_hierarchical_list": 1,
        "store_supports_checksums": 1,
        "store_created_timestamp_ep_k": now_epk,
        "store_modified_timestamp_ep_k": now_epk,
        "store_policy_json": policy_json,
    }
    row_dict = {key: value for key, value in payload.items() if key in store_columns}
    store_row = Row.from_idless_row_dict(db, row_dict=row_dict, table="stores")
    return store_row, backend


def _iter_files_under_root(root: pathlib.Path, *, follow_symlinks: bool = False):
    """
    Yield os.walk filename entries with sorted directory and filename traversal.

    No independent regular-file or containment check is applied. follow_symlinks controls directory
    descent only; file symlinks can still be yielded and opened later. Default os.walk error
    handling can skip inaccessible directories.

    Example:
        >>> paths = list(_iter_files_under_root(root))  # doctest: +SKIP


    :param root: Local tree passed to os.walk.
    :param follow_symlinks: Whether os.walk descends into linked directories, with its normal cycle behavior.
    :return: Iterator of child Paths in per-directory sorted traversal order.
    """
    for dirpath, dirnames, filenames in os.walk(root, followlinks=follow_symlinks):
        dirnames.sort()
        filenames.sort()
        base = pathlib.Path(dirpath)
        for filename in filenames:
            yield base / filename


def _build_file_payload(
    path: pathlib.Path,
    *,
    root: pathlib.Path,
    store_id: int,
    now_epk: int,
    source_label: str,
    compute_hash: bool,
) -> dict[str, object]:
    """
    Read local stat/hash observations and assemble legacy ebook row metadata.

    The storage key is lexical relative_to(root); stat/open can follow file symlinks. compute_hash
    uses the existing get_file_hash helper, whose value is SHA-512 hex plus decimal byte size
    despite being assigned to file_hash_sha256. The ok label records that calculation, not a
    comparison with a trusted digest. Stat and byte reads are separate observations.

    Example:
        >>> payload = _build_file_payload(path, root=root, store_id=1, now_epk=0, source_label="import", compute_hash=False)  # doctest: +SKIP


    :param path: Discovered file path to stat and optionally read.
    :param root: Path used to derive the relative POSIX storage key.
    :param store_id: Legacy Store row identity stored on the file.
    :param now_epk: Wall-clock epoch milliseconds for registration timestamps.
    :param source_label: Provenance label stored unchanged.
    :param compute_hash: Whether to calculate the legacy hash and mark integrity ok.
    :return: Unfiltered field mapping, including None values and platform stat ctime/mtime conversions.
    """
    rel = path.relative_to(root).as_posix()
    ext = path.suffix.lower().lstrip(".")
    stat = path.stat()
    sha256 = get_file_hash(str(path)) if compute_hash else None
    mime_type, _ = mimetypes.guess_type(path.name)

    payload = {
        "file_store_id": store_id,
        "file_storage_key": rel,
        "file_name": path.name,
        "file_base_name": path.stem,
        "file_extension": ext,
        "file_mime_type": mime_type,
        "file_role": "primary",
        "file_media_category": "ebook",
        "file_size_bytes": int(stat.st_size),
        "file_hash_sha256": sha256,
        "file_integrity_status": "ok" if compute_hash else "unchecked",
        "file_last_seen_timestamp_ep_k": now_epk,
        "file_last_integrity_check_timestamp_ep_k": now_epk if compute_hash else None,
        "file_acquired_timestamp_ep_k": now_epk,
        "file_source": source_label,
        "file_original_name": path.name,
        "file_original_path": str(path),
        "file_processed": 0,
        "file_modified_timestamp_ep_k": now_epk,
        "file_source_created_datestamp_ep_k": _epoch_ms_from_seconds(getattr(stat, "st_ctime", None)),
        "file_source_modified_datestamp_ep_k": _epoch_ms_from_seconds(getattr(stat, "st_mtime", None)),
    }
    return payload


def _build_remote_file_payload(
    *,
    file_url: str,
    storage_key: str,
    size_bytes: int,
    modified_at: datetime | None,
    sha256: str | None,
    store_id: int,
    now_epk: int,
    source_label: str,
) -> dict[str, object]:
    """
    Assemble legacy ebook fields from supplied inventory observations without reading remote bytes.

    A truthy supplied SHA-256 claim sets integrity ok without validating its syntax or content.
    Name/MIME derive from the key, falling back to the URL as a POSIX path only for a falsey key. A
    naive modified_at uses local timezone timestamp rules.

    Example:
        >>> payload = _build_remote_file_payload(file_url="remote:book.epub", storage_key="book.epub", size_bytes=3, modified_at=None, sha256=None, store_id=1, now_epk=0, source_label="remote")
        >>> payload["file_integrity_status"]
        'unchecked'


    :param file_url: Constructed original remote address retained as provenance.
    :param storage_key: Backend key stored unchanged and used for filename metadata.
    :param size_bytes: Inventory-supplied size, without independent validation here.
    :param modified_at: Inventory datetime converted to epoch milliseconds, or None.
    :param sha256: Optional digest claim copied directly into the legacy hash field.
    :param store_id: Legacy Store row identity.
    :param now_epk: Registration wall-clock epoch milliseconds.
    :param source_label: Provenance label retained verbatim.
    :return: Unfiltered payload mapping; conversion errors propagate.
    """
    path_obj = pathlib.PurePosixPath(storage_key or pathlib.PurePosixPath(file_url).name)
    name = path_obj.name
    ext = path_obj.suffix.lower().lstrip(".")

    mtime_epk = (
        None if modified_at is None else int(modified_at.timestamp() * 1000.0)
    )
    mime_type, _ = mimetypes.guess_type(name)

    payload = {
        "file_store_id": store_id,
        "file_storage_key": storage_key,
        "file_name": name,
        "file_base_name": path_obj.stem,
        "file_extension": ext,
        "file_mime_type": mime_type,
        "file_role": "primary",
        "file_media_category": "ebook",
        "file_size_bytes": size_bytes,
        "file_hash_sha256": sha256,
        "file_integrity_status": "ok" if sha256 else "unchecked",
        "file_last_seen_timestamp_ep_k": now_epk,
        "file_last_integrity_check_timestamp_ep_k": now_epk if sha256 else None,
        "file_acquired_timestamp_ep_k": now_epk,
        "file_source": source_label,
        "file_original_name": name,
        "file_original_path": file_url,
        "file_processed": 0,
        "file_modified_timestamp_ep_k": now_epk,
        "file_source_created_datestamp_ep_k": None,
        "file_source_modified_datestamp_ep_k": mtime_epk,
    }
    return payload


def _emit_progress(
    progress_callback: Optional[ProgressCallback],
    *,
    event: str,
    report: UnmanagedDiskRegistrationReport,
    details: Optional[dict[str, object]] = None,
) -> None:
    """
    Notify an optional observer with the live report and a shallow details copy.

    Stringify the event inside the guard and swallow ordinary Exceptions, including observer
    failures. BaseException subclasses propagate, and observer mutations to the report remain
    visible.

    Example:
        >>> _emit_progress(None, event="scan", report=report)  # doctest: +SKIP


    :param progress_callback: Optional synchronous observer of event/report/details.
    :param event: Event value converted to text for delivery.
    :param report: Live mutable report passed by reference.
    :param details: Optional mapping shallow-copied, with a falsey value treated as empty.
    :return: None after notification or an ignored ordinary failure.
    """
    if progress_callback is None:
        return
    try:
        progress_callback(str(event), report, dict(details or {}))
    except Exception:
        # Progress callbacks are best-effort and must not break sync behavior.
        return


def _insert_file_row(db, *, payload: dict[str, object], file_columns: set[str]) -> Row:
    """
    Insert supported non-None file fields through the Row facade.

    Example:
        >>> row = _insert_file_row(db, payload=payload, file_columns=columns)  # doctest: +SKIP


    :param db: Database receiving the new legacy files row.
    :param payload: Candidate fields; unsupported keys and None values are omitted.
    :param file_columns: Advertised allowed file-column set.
    :return: Created Row; insertion failures propagate.
    """
    row_dict = {key: value for key, value in payload.items() if key in file_columns and value is not None}
    return Row.from_idless_row_dict(db, row_dict=row_dict, table="files")


def _update_file_row(row: Row, *, payload: dict[str, object], file_columns: set[str]) -> bool:
    """
    Sync supported non-None fields only after a substantive difference is found.

    Four volatile timestamps do not trigger an update by themselves, but are assigned when another
    field differs, including acquired time. None never clears existing metadata. Assignments precede
    sync and are not rolled back here after failure.

    Example:
        >>> changed = _update_file_row(row, payload=payload, file_columns=columns)  # doctest: +SKIP


    :param row: Existing file Row, mutated when a substantive change exists.
    :param payload: Candidate supported values and timestamps.
    :param file_columns: Column names eligible for comparison and assignment.
    :return: True after syncing a changed row, otherwise False.
    """
    volatile_columns = {
        "file_acquired_timestamp_ep_k",
        "file_last_seen_timestamp_ep_k",
        "file_last_integrity_check_timestamp_ep_k",
        "file_modified_timestamp_ep_k",
    }

    changed = False
    for key, value in payload.items():
        if value is None:
            continue
        if key not in file_columns:
            continue
        if key in volatile_columns:
            continue
        if row[key] != value:
            changed = True
            break

    if not changed:
        return False

    for key, value in payload.items():
        if value is None:
            continue
        if key not in file_columns:
            continue
        if row[key] != value:
            row[key] = value
    row.sync()
    return True


def _ensure_file_store_link(
    db,
    *,
    file_id: int,
    store_id: int,
    tables: set[str],
    link_columns: set[str],
    linked_file_ids: set[int],
) -> bool:
    """
    Insert a primary file/Store link when schema support and the caller cache permit.

    The cache suppresses any already-known file identity, without rechecking link type or database
    uniqueness. Add the identity to the cache only after insertion returns.

    Example:
        >>> linked = _ensure_file_store_link(db, file_id=1, store_id=2, tables=tables, link_columns=columns, linked_file_ids=known)  # doctest: +SKIP


    :param db: Database receiving the optional link insertion.
    :param file_id: Legacy file row identity to link.
    :param store_id: Target Store row identity.
    :param tables: Advertised tables, checked for file_store_links.
    :param link_columns: Advertised link columns; both foreign-key columns are required.
    :param linked_file_ids: Mutable set of already-linked file IDs for this Store.
    :return: True after insertion, or False for absent schema support/cached identity.
    """
    if "file_store_links" not in tables:
        return False
    required = {"file_store_link_file_id", "file_store_link_store_id"}
    if not required.issubset(link_columns):
        return False
    if file_id in linked_file_ids:
        return False

    payload = {
        "file_store_link_file_id": file_id,
        "file_store_link_store_id": store_id,
        "file_store_link_priority": 0,
        "file_store_link_type": "primary",
    }
    row_dict = {key: value for key, value in payload.items() if key in link_columns and value is not None}
    Row.from_idless_row_dict(db, row_dict=row_dict, table="file_store_links")
    linked_file_ids.add(file_id)
    return True


def register_existing_disk_as_unmanaged_store(
    db,
    disk_path: str | os.PathLike[str],
    *,
    store_name: Optional[str] = None,
    store_kind: str = "on_disk_existing_unmanaged_drive",
    ebook_extensions: Optional[Iterable[str]] = None,
    source_label: str = "on_disk_unmanaged_import",
    compute_hash: bool = True,
    follow_symlinks: bool = False,
    attach_store_links: bool = True,
    refresh_storage_manager: bool = True,
    progress_callback: Optional[ProgressCallback] = None,
) -> UnmanagedDiskRegistrationReport:
    """
    Upsert legacy ebook file rows and optional links from a sorted local tree scan.

    Ensure the Store first, then map existing rows by storage key with the last duplicate winning.
    Count candidates before per-file work; file/link errors become report strings and scanning
    continues. Prior writes and counters survive those errors. Do not delete absent files or add an
    all-run transaction.

    Setup/iterator failures can escape without a completed report. Optional manager bootstrap
    ignores its return value but records raised Exceptions. Progress observers receive the live
    report with ordinary failures swallowed. Hash calculation follows the legacy helper contract
    described by _build_file_payload.

    Example:
        >>> report = register_existing_disk_as_unmanaged_store(db, disk_root, compute_hash=False)  # doctest: +SKIP


    :param db: Borrowed database facade receiving incremental row operations.
    :param disk_path: Existing local directory used for the unmanaged Store and filesystem scan.
    :param store_name: Truthy name override, otherwise a sanitized name derived from the root.
    :param store_kind: Legacy Store-kind label persisted independently of the concrete backend class.
    :param ebook_extensions: Accepted lowercase suffix policy; None chooses project defaults and an empty iterable selects none.
    :param source_label: Provenance label stored on accepted file rows.
    :param compute_hash: Whether to store the legacy SHA-512-plus-size helper value in file_hash_sha256.
    :param follow_symlinks: Whether to descend linked directories; file symlinks are still handled by ordinary stat/open.
    :param attach_store_links: Whether to add supported primary links for accepted file rows.
    :param refresh_storage_manager: Whether to call available bootstrap_storage_manager(clear_existing=True) after the scan.
    :param progress_callback: Best-effort synchronous observer receiving the live mutable report.
    :return: Finished registration report after normal scan completion, possibly containing partial-write errors.
    """
    tables, _, file_columns, link_columns = _ensure_schema_support(db)
    store_row, backend = ensure_unmanaged_store_for_disk(
        db,
        disk_path=disk_path,
        store_name=store_name,
        store_kind=store_kind,
    )

    store_id = int(store_row.row_id if store_row.row_id is not None else store_row["store_id"])
    report = UnmanagedDiskRegistrationReport(
        store_row_id=store_id,
        store_root_uri=str(backend.root_path),
        store_name=store_row["store_name"] if "store_name" in store_row.allowed_columns else backend.configuration.store_name,
    )
    _emit_progress(
        progress_callback,
        event="start",
        report=report,
        details={"mode": "local", "store_id": store_id, "store_root_uri": str(backend.root_path)},
    )

    existing_rows = db.search("files", "file_store_id", store_id)
    existing_by_key: dict[str, Row] = {}
    for row in existing_rows:
        key = row["file_storage_key"]
        if key is not None:
            existing_by_key[str(key)] = row

    linked_file_ids: set[int] = set()
    if attach_store_links and "file_store_links" in tables and "file_store_link_store_id" in link_columns:
        for link_row in db.search("file_store_links", "file_store_link_store_id", store_id):
            file_id = link_row["file_store_link_file_id"]
            if file_id is not None:
                linked_file_ids.add(int(file_id))

    ebook_exts = _normalize_ebook_extensions(ebook_extensions)

    for path in _iter_files_under_root(backend.root_path, follow_symlinks=follow_symlinks):
        report.scanned_files += 1
        ext = path.suffix.lower().lstrip(".")
        if ext not in ebook_exts:
            report.skipped_non_ebook_files += 1
            _emit_progress(
                progress_callback,
                event="scan",
                report=report,
                details={"path": str(path), "is_ebook": False},
            )
            continue

        report.ebook_candidates += 1
        now_epk = _now_ep_ms()
        try:
            payload = _build_file_payload(
                path,
                root=backend.root_path,
                store_id=store_id,
                now_epk=now_epk,
                source_label=source_label,
                compute_hash=compute_hash,
            )
            storage_key = str(payload["file_storage_key"])

            existing = existing_by_key.get(storage_key)
            if existing is None:
                row = _insert_file_row(db, payload=payload, file_columns=file_columns)
                existing_by_key[storage_key] = row
                report.inserted_files += 1
            else:
                if _update_file_row(existing, payload=payload, file_columns=file_columns):
                    report.updated_files += 1
                else:
                    report.unchanged_files += 1
                row = existing

            if attach_store_links and row.row_id is not None:
                linked = _ensure_file_store_link(
                    db,
                    file_id=int(row.row_id),
                    store_id=store_id,
                    tables=tables,
                    link_columns=link_columns,
                    linked_file_ids=linked_file_ids,
                )
                if linked:
                    report.linked_files += 1

        except Exception as exc:
            report.errors.append("{} :: {}".format(str(path), repr(exc)))
            _emit_progress(
                progress_callback,
                event="error",
                report=report,
                details={"path": str(path), "error": repr(exc)},
            )
        else:
            _emit_progress(
                progress_callback,
                event="scan",
                report=report,
                details={"path": str(path), "is_ebook": True},
            )

    if refresh_storage_manager and hasattr(db, "bootstrap_storage_manager"):
        try:
            db.bootstrap_storage_manager(clear_existing=True)
        except Exception as exc:
            report.errors.append("storage_manager_bootstrap_failed :: {!r}".format(exc))

    report.finished_timestamp_ep_k = _now_ep_ms()
    _emit_progress(
        progress_callback,
        event="done",
        report=report,
        details={"mode": "local", "store_id": store_id, "store_root_uri": str(backend.root_path)},
    )
    return report


def register_rclone_http_readonly_store_files(
    db,
    remote_url: str,
    *,
    store_name: Optional[str] = None,
    store_kind: str = "rclone_http_readonly",
    max_http_requests_per_hour: float | None = None,
    apply_rclone_tpslimit: bool = True,
    rclone_tpslimit_burst: int = 1,
    enforce_global_rate_limit: bool = True,
    rclone_exe: str = "rclone",
    rclone_args: Optional[Sequence[str]] = None,
    timeout_s: float | None = 60.0,
    max_inventory_entries: int = 100_000,
    max_json_token_chars: int = 8 * 1024 * 1024,
    ebook_extensions: Optional[Iterable[str]] = None,
    source_label: str = "rclone_http_import",
    capture_hashes: bool = False,
    attach_store_links: bool = True,
    refresh_storage_manager: bool = True,
    progress_callback: Optional[ProgressCallback] = None,
) -> UnmanagedDiskRegistrationReport:
    """
    Upsert legacy ebook rows from a streaming rclone inventory and optionally bootstrap the manager.

    Persist the Store before iterating. Filter the raw Location key suffix, not the URL-extension
    heuristic. capture_hashes accepts only an exactly named sha256 inventory claim and does not
    download bytes for verification. Construct original URLs by text concatenation.

    Last matching duplicate keys win in the initial row map. Per-entry processing failures become
    report errors after any prior writes/counts, while inventory iteration/location access can fail
    outside that guard. No absent-file deletion or all-run transaction is provided. Ordinary
    progress failures are swallowed and bootstrap return values are ignored.

    Example:
        >>> report = register_rclone_http_readonly_store_files(db, "remote:")  # doctest: +SKIP


    :param db: Borrowed database facade receiving incremental row operations.
    :param remote_url: Remote selector trimmed here and parsed/validated by the concrete backend.
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
    :param capture_hashes: Whether to retain exactly named sha256 inventory digest claims.
    :param attach_store_links: Whether to add supported primary links for accepted file rows.
    :param refresh_storage_manager: Whether to call available bootstrap_storage_manager(clear_existing=True) after the scan.
    :param progress_callback: Best-effort synchronous observer receiving the live mutable report.
    :return: Finished report after normal inventory completion, possibly with per-entry/bootstrap errors.
    """
    tables, _, file_columns, link_columns = _ensure_schema_support(db)
    store_row, backend = ensure_rclone_http_readonly_store(
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
    )

    store_id = int(store_row.row_id if store_row.row_id is not None else store_row["store_id"])
    report = UnmanagedDiskRegistrationReport(
        store_row_id=store_id,
        store_root_uri=str(backend.url),
        store_name=store_row["store_name"] if "store_name" in store_row.allowed_columns else backend.configuration.store_name,
    )
    _emit_progress(
        progress_callback,
        event="start",
        report=report,
        details={"mode": "rclone", "store_id": store_id, "store_root_uri": str(backend.url)},
    )

    existing_rows = db.search("files", "file_store_id", store_id)
    existing_by_key: dict[str, Row] = {}
    for row in existing_rows:
        key = row["file_storage_key"]
        if key is not None:
            existing_by_key[str(key)] = row

    linked_file_ids: set[int] = set()
    if attach_store_links and "file_store_links" in tables and "file_store_link_store_id" in link_columns:
        for link_row in db.search("file_store_links", "file_store_link_store_id", store_id):
            file_id = link_row["file_store_link_file_id"]
            if file_id is not None:
                linked_file_ids.add(int(file_id))

    ebook_exts = _normalize_ebook_extensions(ebook_extensions)

    for info in backend.iter_file_infos():
        location = info.location
        report.scanned_files += 1
        try:
            storage_key = location.key
            ext = pathlib.PurePosixPath(storage_key).suffix.lower().lstrip(".")
            if ext not in ebook_exts:
                report.skipped_non_ebook_files += 1
                _emit_progress(
                    progress_callback,
                    event="scan",
                    report=report,
                    details={"path": storage_key, "is_ebook": False},
                )
                continue

            report.ebook_candidates += 1
            now_epk = _now_ep_ms()
            sha256: str | None = None
            if capture_hashes and info.digest is not None:
                if info.digest.algorithm == "sha256":
                    sha256 = info.digest.value
            root_uri = backend.configuration.store_root_uri
            file_url = (
                root_uri + storage_key
                if root_uri.endswith(":")
                else root_uri.rstrip("/") + "/" + storage_key
            )

            payload = _build_remote_file_payload(
                file_url=file_url,
                storage_key=storage_key,
                size_bytes=info.size,
                modified_at=info.modified_at,
                sha256=sha256,
                store_id=store_id,
                now_epk=now_epk,
                source_label=source_label,
            )

            existing = existing_by_key.get(storage_key)
            if existing is None:
                row = _insert_file_row(db, payload=payload, file_columns=file_columns)
                existing_by_key[storage_key] = row
                report.inserted_files += 1
            else:
                if _update_file_row(existing, payload=payload, file_columns=file_columns):
                    report.updated_files += 1
                else:
                    report.unchanged_files += 1
                row = existing

            if attach_store_links and row.row_id is not None:
                linked = _ensure_file_store_link(
                    db,
                    file_id=int(row.row_id),
                    store_id=store_id,
                    tables=tables,
                    link_columns=link_columns,
                    linked_file_ids=linked_file_ids,
                )
                if linked:
                    report.linked_files += 1

        except Exception as exc:
            marker = getattr(location, "key", "<unknown>")
            report.errors.append("{} :: {}".format(marker, repr(exc)))
            _emit_progress(
                progress_callback,
                event="error",
                report=report,
                details={"path": marker, "error": repr(exc)},
            )
        else:
            _emit_progress(
                progress_callback,
                event="scan",
                report=report,
                details={"path": storage_key, "is_ebook": True},
            )

    if refresh_storage_manager and hasattr(db, "bootstrap_storage_manager"):
        try:
            db.bootstrap_storage_manager(clear_existing=True)
        except Exception as exc:
            report.errors.append("storage_manager_bootstrap_failed :: {!r}".format(exc))

    report.finished_timestamp_ep_k = _now_ep_ms()
    _emit_progress(
        progress_callback,
        event="done",
        report=report,
        details={"mode": "rclone", "store_id": store_id, "store_root_uri": str(backend.url)},
    )
    return report


def register_existing_disk_with_database_path(
    *,
    database_path: str | os.PathLike[str],
    disk_path: str | os.PathLike[str],
    db_type: str = "SQLite",
    store_name: Optional[str] = None,
    store_kind: str = "on_disk_existing_unmanaged_drive",
    ebook_extensions: Optional[Iterable[str]] = None,
    source_label: str = "on_disk_unmanaged_import",
    compute_hash: bool = True,
    follow_symlinks: bool = False,
    attach_store_links: bool = True,
    refresh_storage_manager: bool = True,
    progress_callback: Optional[ProgressCallback] = None,
) -> UnmanagedDiskRegistrationReport:
    """
    Open an existing database without backup, register the local tree, and close through its context
    manager.

    Forward registration policy unchanged. Construction, registration, and context-exit errors
    propagate; database lifetime management does not add an all-run transaction.

    Example:
        >>> report = register_existing_disk_with_database_path(database_path=catalogue, disk_path=root)  # doctest: +SKIP


    :param database_path: Database path stringified through Path without expanduser or resolve.
    :param disk_path: Existing local directory used for the unmanaged Store and filesystem scan.
    :param db_type: Database adapter selector, defaulting to SQLite.
    :param store_name: Truthy name override, otherwise a sanitized name derived from the root.
    :param store_kind: Legacy Store-kind label persisted independently of the concrete backend class.
    :param ebook_extensions: Accepted lowercase suffix policy; None chooses project defaults and an empty iterable selects none.
    :param source_label: Provenance label stored on accepted file rows.
    :param compute_hash: Whether to store the legacy SHA-512-plus-size helper value in file_hash_sha256.
    :param follow_symlinks: Whether to descend linked directories; file symlinks are still handled by ordinary stat/open.
    :param attach_store_links: Whether to add supported primary links for accepted file rows.
    :param refresh_storage_manager: Whether to call available bootstrap_storage_manager(clear_existing=True) after the scan.
    :param progress_callback: Best-effort synchronous observer receiving the live mutable report.
    :return: Registration report after successful database context exit.
    """
    from LiuXin_alpha.databases.database import Database

    metadata = {"database_path": str(pathlib.Path(database_path))}
    with Database(metadata=metadata, db_type=db_type, create=False, backup=False) as db:
        return register_existing_disk_as_unmanaged_store(
            db,
            disk_path=disk_path,
            store_name=store_name,
            store_kind=store_kind,
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
    store_name: Optional[str] = None,
    store_kind: str = "rclone_http_readonly",
    max_http_requests_per_hour: float | None = None,
    apply_rclone_tpslimit: bool = True,
    rclone_tpslimit_burst: int = 1,
    enforce_global_rate_limit: bool = True,
    rclone_exe: str = "rclone",
    rclone_args: Optional[Sequence[str]] = None,
    timeout_s: float | None = 60.0,
    max_inventory_entries: int = 100_000,
    max_json_token_chars: int = 8 * 1024 * 1024,
    ebook_extensions: Optional[Iterable[str]] = None,
    source_label: str = "rclone_http_import",
    capture_hashes: bool = False,
    attach_store_links: bool = True,
    refresh_storage_manager: bool = True,
    progress_callback: Optional[ProgressCallback] = None,
) -> UnmanagedDiskRegistrationReport:
    """
    Open an existing database without backup and delegate rclone inventory registration.

    The wrapper owns the Database context, forwards all policies, and closes it before returning.
    Setup, inventory, or close failures can escape after prior writes.

    Example:
        >>> report = register_rclone_http_readonly_with_database_path(database_path=catalogue, remote_url="remote:")  # doctest: +SKIP


    :param database_path: Database path stringified through Path without expanduser or resolve.
    :param remote_url: Remote selector trimmed here and parsed/validated by the concrete backend.
    :param db_type: Database adapter selector, defaulting to SQLite.
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
    from LiuXin_alpha.databases.database import Database

    metadata = {"database_path": str(pathlib.Path(database_path))}
    with Database(metadata=metadata, db_type=db_type, create=False, backup=False) as db:
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


def _build_arg_parser() -> argparse.ArgumentParser:
    """
    Construct the standalone local-disk registration grammar without opening a database.

    The fixed help labels describe historical hash behavior; parsing does not perform the operation.
    This entry point does not expose the rclone registration options.

    Example:
        >>> _build_arg_parser().parse_args(["--database", "catalogue.sqlite", "--disk", "/books"]).no_hash
        False


    :return: Fresh ArgumentParser with required database/disk paths and output/failure flags.
    """
    parser = argparse.ArgumentParser(description="Register ebook files from an unmanaged disk into LiuXin files table.")
    parser.add_argument("--database", required=True, help="Path to the target LiuXin database file.")
    parser.add_argument("--disk", required=True, help="Root path of the existing disk/folder to index.")
    parser.add_argument("--db-type", default="SQLite", help="Database driver type (default: SQLite).")
    parser.add_argument("--store-name", default=None, help="Optional store name override.")
    parser.add_argument(
        "--no-hash",
        action="store_true",
        help="Skip SHA256 hashing while ingesting files (faster, less integrity data).",
    )
    parser.add_argument(
        "--follow-symlinks",
        action="store_true",
        help="Follow symlinked directories while walking the disk.",
    )
    parser.add_argument(
        "--no-store-links",
        action="store_true",
        help="Do not create `file_store_links` rows (only set files.file_store_id).",
    )
    parser.add_argument("--json", action="store_true", help="Print report as JSON.")
    parser.add_argument(
        "--fail-on-errors",
        action="store_true",
        help="Exit non-zero if any file-level errors occurred.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    Parse local-disk registration options, run the database-path wrapper, and print its report.

    Emit either sorted JSON or fixed text counters. Report errors affect the return code only with
    --fail-on-errors. Parsing can raise SystemExit and setup/output exceptions propagate after any
    prior effects.

    Example:
        >>> status = main(["--database", str(catalogue), "--disk", str(root), "--json"])  # doctest: +SKIP


    :param argv: Argument sequence, or None to read the process command line.
    :return: 2 when fail-on-errors is selected and report.errors is nonempty; otherwise 0.
    """
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    report = register_existing_disk_with_database_path(
        database_path=args.database,
        disk_path=args.disk,
        db_type=args.db_type,
        store_name=args.store_name,
        compute_hash=not args.no_hash,
        follow_symlinks=args.follow_symlinks,
        attach_store_links=not args.no_store_links,
    )

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    else:
        print("Store: {} ({})".format(report.store_name, report.store_root_uri))
        print("Store row id: {}".format(report.store_row_id))
        print("Scanned files: {}".format(report.scanned_files))
        print("Ebook candidates: {}".format(report.ebook_candidates))
        print("Inserted files: {}".format(report.inserted_files))
        print("Updated files: {}".format(report.updated_files))
        print("Unchanged files: {}".format(report.unchanged_files))
        print("Linked files: {}".format(report.linked_files))
        print("Skipped non-ebook files: {}".format(report.skipped_non_ebook_files))
        print("Errors: {}".format(len(report.errors)))
        if report.duration_seconds is not None:
            print("Duration (seconds): {:.3f}".format(report.duration_seconds))

    if args.fail_on_errors and report.errors:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "UnmanagedDiskRegistrationReport",
    "ensure_unmanaged_store_for_disk",
    "ensure_rclone_http_readonly_store",
    "register_existing_disk_as_unmanaged_store",
    "register_existing_disk_with_database_path",
    "register_rclone_http_readonly_store_files",
    "register_rclone_http_readonly_with_database_path",
    "main",
]
