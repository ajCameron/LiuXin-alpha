"""
Store <-> database reconciliation helpers.

Current focus:
- treat an existing local disk tree as an unmanaged/read-only store
- register discovered ebook files into the database `files` table
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
from LiuXin_alpha.storage.store_backend_plugins.wget_html_readonly import (
    WgetBackendOptions,
    WgetHtmlReadOnlyStorageBackend,
    get_default_wget_http_requests_per_hour,
)
from LiuXin_alpha.utils.storage.local.file_properties import get_file_hash
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


ProgressCallback = Callable[[str, UnmanagedDiskRegistrationReport, dict[str, object]], None]


def _now_ep_ms() -> int:
    return int(time.time() * 1000)


def _epoch_ms_from_seconds(value: float | int | None) -> Optional[int]:
    if value is None:
        return None
    return int(float(value) * 1000.0)


def _normalize_ebook_extensions(ebook_extensions: Optional[Iterable[str]]) -> set[str]:
    if ebook_extensions is None:
        ebook_extensions = BOOK_EXTENSIONS
    return {str(x).lower().lstrip(".") for x in ebook_extensions if str(x).strip()}


def _normalize_root(path: str | os.PathLike[str]) -> pathlib.Path:
    root = pathlib.Path(path).expanduser()
    if not root.exists():
        raise FileNotFoundError("Disk path does not exist: {!r}".format(str(root)))
    if not root.is_dir():
        raise NotADirectoryError("Disk path is not a directory: {!r}".format(str(root)))
    return root.resolve()


def _normalize_remote_root(url: str) -> str:
    text = str(url).strip()
    if not text:
        raise ValueError("Remote store URL cannot be blank.")
    return text


def _coerce_datetime_ep_ms(value: object) -> Optional[int]:
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
    lowered = remote_url.lower()
    if "url=https://" in lowered or lowered.startswith("https://"):
        return "https"
    if "url=http://" in lowered or lowered.startswith("http://"):
        return "http"
    return "rclone"


def _storage_key_from_store_url(*, store_url: str, file_url: str) -> str:
    root = str(store_url).strip()
    target = str(file_url).strip()
    if not root:
        return target.lstrip("/")
    if root.endswith(":"):
        if target.startswith(root):
            return target[len(root) :].lstrip("/")
        return target
    rooted = root.rstrip("/") + "/"
    if target.startswith(rooted):
        return target[len(rooted) :]
    if target.startswith(root):
        return target[len(root) :].lstrip("/")
    return target


def _extract_preferred_hash(hashes: object) -> str | None:
    if not isinstance(hashes, dict):
        return None
    preferred = ("sha256", "sha1", "md5", "crc32")
    for key in preferred:
        value = hashes.get(key)
        if value:
            return str(value)
    for value in hashes.values():
        if value:
            return str(value)
    return None


def _table_columns(db, table_name: str) -> set[str]:
    return set(db.get_column_headings(table_name))


def _ensure_schema_support(db) -> tuple[set[str], set[str], set[str], set[str]]:
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
    Create/reuse a `stores` row and unmanaged store backend for an existing disk path.
    """
    _ensure_schema_support(db)
    root = _normalize_root(disk_path)

    backend_name = store_name or safe_path_to_name(str(root))
    backend = OnDiskUnmanagedStorageBackend(url=str(root), name=backend_name)

    store_rows = db.search("stores", "store_root_uri", str(root))
    if store_rows:
        store_row = store_rows[0]
        updates = {
            "store_name": backend.name,
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
        }
        changed = False
        for key, value in updates.items():
            if key not in store_row.allowed_columns:
                return
            if store_row[key] != value:
                store_row[key] = value
                changed = True
        if changed:
            store_row.sync()
        return store_row, backend

    store_columns = _table_columns(db, "stores")
    now_epk = _now_ep_ms()
    payload = {
        "store_name": backend.name,
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
) -> tuple[Row, RcloneHttpReadOnlyStorageBackend]:
    """
    Create/reuse a `stores` row and rclone-backed read-only store for a remote URL.
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
        },
    }
    policy_json = json.dumps(policy_payload, sort_keys=True)

    store_rows = db.search("stores", "store_root_uri", root)
    if store_rows:
        store_row = store_rows[0]
        updates = {
            "store_name": backend.name,
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
        "store_name": backend.name,
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
) -> tuple[Row, WgetHtmlReadOnlyStorageBackend]:
    """
    Create/reuse a `stores` row and wget-backed read-only store for a remote URL.
    """
    _ensure_schema_support(db)
    root = _normalize_remote_root(remote_url)
    effective_max_http_requests_per_hour = (
        get_default_wget_http_requests_per_hour()
        if max_http_requests_per_hour is None
        else max_http_requests_per_hour
    )

    backend_name = store_name or safe_path_to_name(root)
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
    )
    backend = WgetHtmlReadOnlyStorageBackend(url=root, name=backend_name, options=options)

    store_columns = _table_columns(db, "stores")
    policy_payload = {
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
        },
    }
    policy_json = json.dumps(policy_payload, sort_keys=True)

    store_rows = db.search("stores", "store_root_uri", root)
    if store_rows:
        store_row = store_rows[0]
        updates = {
            "store_name": backend.name,
            "store_kind": store_kind,
            "store_access_protocol": "wget",
            "store_root_uri": root,
            "store_is_read_only": 1,
            "store_online_status": "online",
            "store_supports_random_read": 1,
            "store_supports_random_write": 0,
            "store_supports_delete": 0,
            "store_supports_folders": 1,
            "store_supports_hierarchical_list": 1,
            "store_supports_checksums": 0,
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
        "store_name": backend.name,
        "store_kind": store_kind,
        "store_access_protocol": "wget",
        "store_root_uri": root,
        "store_is_read_only": 1,
        "store_online_status": "online",
        "store_supports_random_read": 1,
        "store_supports_random_write": 0,
        "store_supports_delete": 0,
        "store_supports_folders": 1,
        "store_supports_hierarchical_list": 1,
        "store_supports_checksums": 0,
        "store_created_timestamp_ep_k": now_epk,
        "store_modified_timestamp_ep_k": now_epk,
        "store_policy_json": policy_json,
    }
    row_dict = {key: value for key, value in payload.items() if key in store_columns}
    store_row = Row.from_idless_row_dict(db, row_dict=row_dict, table="stores")
    return store_row, backend


def _iter_files_under_root(root: pathlib.Path, *, follow_symlinks: bool = False):
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
    stat_blob: dict[str, object] | None,
    store_id: int,
    now_epk: int,
    source_label: str,
    capture_hashes: bool,
) -> dict[str, object]:
    path_obj = pathlib.PurePosixPath(storage_key or pathlib.PurePosixPath(file_url).name)
    name = path_obj.name
    ext = path_obj.suffix.lower().lstrip(".")

    size_raw = (stat_blob or {}).get("Size")
    try:
        size_bytes = int(size_raw) if size_raw is not None else 0
    except Exception:
        size_bytes = 0

    mtime_epk = _coerce_datetime_ep_ms((stat_blob or {}).get("ModTime"))
    mime_type, _ = mimetypes.guess_type(name)

    chosen_hash = None
    if capture_hashes:
        chosen_hash = _extract_preferred_hash((stat_blob or {}).get("Hashes"))

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
        "file_hash_sha256": chosen_hash,
        "file_integrity_status": "ok" if chosen_hash else "unchecked",
        "file_last_seen_timestamp_ep_k": now_epk,
        "file_last_integrity_check_timestamp_ep_k": now_epk if chosen_hash else None,
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
    if progress_callback is None:
        return
    try:
        progress_callback(str(event), report, dict(details or {}))
    except Exception:
        # Progress callbacks are best-effort and must not break sync behavior.
        return


def _insert_file_row(db, *, payload: dict[str, object], file_columns: set[str]) -> Row:
    row_dict = {key: value for key, value in payload.items() if key in file_columns and value is not None}
    return Row.from_idless_row_dict(db, row_dict=row_dict, table="files")


def _update_file_row(row: Row, *, payload: dict[str, object], file_columns: set[str]) -> bool:
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
    Register ebook files under a disk path into `files` using one unmanaged store row.
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
        store_name=store_row["store_name"] if "store_name" in store_row.allowed_columns else backend.name,
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
    ebook_extensions: Optional[Iterable[str]] = None,
    source_label: str = "rclone_http_import",
    capture_hashes: bool = False,
    attach_store_links: bool = True,
    refresh_storage_manager: bool = True,
    progress_callback: Optional[ProgressCallback] = None,
) -> UnmanagedDiskRegistrationReport:
    """
    Register ebook files discovered by a read-only rclone HTTP store into `files`.
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
    )

    store_id = int(store_row.row_id if store_row.row_id is not None else store_row["store_id"])
    report = UnmanagedDiskRegistrationReport(
        store_row_id=store_id,
        store_root_uri=str(backend.url),
        store_name=store_row["store_name"] if "store_name" in store_row.allowed_columns else backend.name,
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

    for remote_file in backend.true_files():
        report.scanned_files += 1
        try:
            storage_key = _storage_key_from_store_url(store_url=backend.url, file_url=remote_file.file_url)
            ext = pathlib.PurePosixPath(storage_key).suffix.lower().lstrip(".")
            if ext not in ebook_exts:
                report.skipped_non_ebook_files += 1
                _emit_progress(
                    progress_callback,
                    event="scan",
                    report=report,
                    details={"path": storage_key, "is_ebook": False},
                )
                return

            report.ebook_candidates += 1
            now_epk = _now_ep_ms()
            stat_blob: dict[str, object] | None = None
            stat_fn = getattr(remote_file, "_stat_blob", None)
            if callable(stat_fn):
                maybe_blob = stat_fn()
                if isinstance(maybe_blob, dict):
                    stat_blob = maybe_blob

            payload = _build_remote_file_payload(
                file_url=remote_file.file_url,
                storage_key=storage_key,
                stat_blob=stat_blob,
                store_id=store_id,
                now_epk=now_epk,
                source_label=source_label,
                capture_hashes=bool(capture_hashes),
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
            marker = getattr(remote_file, "file_url", "<unknown>")
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
    ebook_extensions: Optional[Iterable[str]] = None,
    source_label: str = "wget_html_import",
    attach_store_links: bool = True,
    refresh_storage_manager: bool = True,
    incremental_db_writes: bool = True,
    progress_callback: Optional[ProgressCallback] = None,
) -> UnmanagedDiskRegistrationReport:
    """
    Register ebook files discovered by a wget HTML spider store into `files`.
    """
    tables, _, file_columns, link_columns = _ensure_schema_support(db)
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
    )

    store_id = int(store_row.row_id if store_row.row_id is not None else store_row["store_id"])
    report = UnmanagedDiskRegistrationReport(
        store_row_id=store_id,
        store_root_uri=str(backend.url),
        store_name=store_row["store_name"] if "store_name" in store_row.allowed_columns else backend.name,
    )
    _emit_progress(
        progress_callback,
        event="start",
        report=report,
        details={"mode": "wget", "store_id": store_id, "store_root_uri": str(backend.url)},
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

    def _on_crawl_log_line(raw_line: str) -> None:
        line = str(raw_line or "").strip()
        if not line:
            return
        _emit_progress(
            progress_callback,
            event="crawl-log",
            report=report,
            details={"line": line, "mode": "wget"},
        )

    def _process_crawled_url(crawled_url: str) -> None:
        remote_file = backend.get_file(crawled_url)
        report.scanned_files += 1
        try:
            storage_key = _storage_key_from_store_url(store_url=backend.url, file_url=remote_file.file_url)
            ext = pathlib.PurePosixPath(storage_key).suffix.lower().lstrip(".")
            if ext not in ebook_exts:
                report.skipped_non_ebook_files += 1
                _emit_progress(
                    progress_callback,
                    event="scan",
                    report=report,
                    details={"path": storage_key, "is_ebook": False},
                )
                return

            report.ebook_candidates += 1
            now_epk = _now_ep_ms()
            payload = _build_remote_file_payload(
                file_url=remote_file.file_url,
                storage_key=storage_key,
                stat_blob=None,
                store_id=store_id,
                now_epk=now_epk,
                source_label=source_label,
                capture_hashes=False,
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
            marker = getattr(remote_file, "file_url", "<unknown>")
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

    if incremental_db_writes:
        backend.crawl_urls(
            force=False,
            log_line_callback=_on_crawl_log_line,
            discovered_url_callback=_process_crawled_url,
        )
    else:
        crawled_urls = backend.crawl_urls(force=False, log_line_callback=_on_crawl_log_line)
        for crawled_url in crawled_urls:
            _process_crawled_url(crawled_url)

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
        details={"mode": "wget", "store_id": store_id, "store_root_uri": str(backend.url)},
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
    Convenience wrapper that opens a Database instance from a path.
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
    ebook_extensions: Optional[Iterable[str]] = None,
    source_label: str = "rclone_http_import",
    capture_hashes: bool = False,
    attach_store_links: bool = True,
    refresh_storage_manager: bool = True,
    progress_callback: Optional[ProgressCallback] = None,
) -> UnmanagedDiskRegistrationReport:
    """
    Convenience wrapper that opens a Database instance and ingests files from an rclone HTTP store.
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
    ebook_extensions: Optional[Iterable[str]] = None,
    source_label: str = "wget_html_import",
    attach_store_links: bool = True,
    refresh_storage_manager: bool = True,
    incremental_db_writes: bool = True,
    progress_callback: Optional[ProgressCallback] = None,
) -> UnmanagedDiskRegistrationReport:
    """
    Convenience wrapper that opens a Database instance and ingests files from a wget HTML store.
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
            ebook_extensions=ebook_extensions,
            source_label=source_label,
            attach_store_links=attach_store_links,
            refresh_storage_manager=refresh_storage_manager,
            incremental_db_writes=incremental_db_writes,
            progress_callback=progress_callback,
        )


def _build_arg_parser() -> argparse.ArgumentParser:
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
    "ensure_wget_html_readonly_store",
    "register_existing_disk_as_unmanaged_store",
    "register_existing_disk_with_database_path",
    "register_rclone_http_readonly_store_files",
    "register_rclone_http_readonly_with_database_path",
    "register_wget_html_readonly_store_files",
    "register_wget_html_readonly_with_database_path",
    "main",
]
