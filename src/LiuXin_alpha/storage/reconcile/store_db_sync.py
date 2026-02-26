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

from typing import Iterable, Optional, Sequence

from LiuXin_alpha.constants.file_extensions import BOOK_EXTENSIONS
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.storage.reconcile.models import UnmanagedDiskRegistrationReport
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive import (
    OnDiskUnmanagedStorageBackend,
)
from LiuXin_alpha.utils.storage.local.file_properties import get_file_hash
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


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
        "store_created_timestamp_ep_k": now_epk,
        "store_modified_timestamp_ep_k": now_epk,
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
    ebook_extensions: Optional[Iterable[str]] = None,
    source_label: str = "on_disk_unmanaged_import",
    compute_hash: bool = True,
    follow_symlinks: bool = False,
    attach_store_links: bool = True,
) -> UnmanagedDiskRegistrationReport:
    """
    Register ebook files under a disk path into `files` using one unmanaged store row.
    """
    tables, _, file_columns, link_columns = _ensure_schema_support(db)
    store_row, backend = ensure_unmanaged_store_for_disk(db, disk_path=disk_path, store_name=store_name)

    store_id = int(store_row.row_id if store_row.row_id is not None else store_row["store_id"])
    report = UnmanagedDiskRegistrationReport(
        store_row_id=store_id,
        store_root_uri=str(backend.root_path),
        store_name=store_row["store_name"] if "store_name" in store_row.allowed_columns else backend.name,
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

    report.finished_timestamp_ep_k = _now_ep_ms()
    return report


def register_existing_disk_with_database_path(
    *,
    database_path: str | os.PathLike[str],
    disk_path: str | os.PathLike[str],
    db_type: str = "SQLite",
    store_name: Optional[str] = None,
    ebook_extensions: Optional[Iterable[str]] = None,
    source_label: str = "on_disk_unmanaged_import",
    compute_hash: bool = True,
    follow_symlinks: bool = False,
    attach_store_links: bool = True,
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
            ebook_extensions=ebook_extensions,
            source_label=source_label,
            compute_hash=compute_hash,
            follow_symlinks=follow_symlinks,
            attach_store_links=attach_store_links,
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
    "register_existing_disk_as_unmanaged_store",
    "register_existing_disk_with_database_path",
    "main",
]
