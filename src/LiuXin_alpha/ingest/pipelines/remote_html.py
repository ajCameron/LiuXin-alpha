from __future__ import annotations

import mimetypes
import pathlib
import time

from datetime import datetime
from typing import Callable, Iterable, Optional
from urllib.parse import parse_qs, unquote, urlparse

from LiuXin_alpha.constants.file_extensions import BOOK_EXTENSIONS
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.storage.reconcile.models import UnmanagedDiskRegistrationReport


ProgressCallback = Callable[[str, UnmanagedDiskRegistrationReport, dict[str, object]], None]
_HTML_LIKE_EXTENSIONS = {"htm", "html", "htmlz", "xhtm", "xhtml"}


def _now_ep_ms() -> int:
    return int(time.time() * 1000)


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


def _normalize_ebook_extensions(ebook_extensions: Optional[Iterable[str]]) -> set[str]:
    if ebook_extensions is None:
        ebook_extensions = BOOK_EXTENSIONS
    return {str(x).lower().lstrip(".") for x in ebook_extensions if str(x).strip()}


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


def _guess_remote_url_extension(candidate_url: str) -> str:
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

    return {
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


def ingest_html_discovery_store_files(
    db,
    *,
    store_row: Row,
    store_url: str,
    store_name_value: str,
    discovery_source,
    mode: str,
    ebook_extensions: Optional[Iterable[str]],
    source_label: str,
    attach_store_links: bool,
    refresh_storage_manager: bool,
    incremental_db_writes: bool,
    progress_callback: Optional[ProgressCallback],
) -> UnmanagedDiskRegistrationReport:
    tables, _, file_columns, link_columns = _ensure_schema_support(db)
    store_id = int(store_row.row_id if store_row.row_id is not None else store_row["store_id"])
    report = UnmanagedDiskRegistrationReport(
        store_row_id=store_id,
        store_root_uri=str(store_url),
        store_name=str(store_name_value),
    )
    _emit_progress(
        progress_callback,
        event="start",
        report=report,
        details={"mode": mode, "store_id": store_id, "store_root_uri": str(store_url)},
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
            details={"line": line, "mode": mode},
        )

    def _on_crawl_observation(details: dict[str, object]) -> None:
        candidate_url = str(details.get("url", "") or "").strip()
        if not candidate_url:
            return
        accepted = bool(details.get("accepted", False))
        reason = str(details.get("reason", "") or "").strip() or "unknown"
        extension = _guess_remote_url_extension(candidate_url)
        html_like = extension in _HTML_LIKE_EXTENSIONS
        book_like = extension in ebook_exts

        report.crawler_urls_observed += 1
        if html_like:
            report.crawler_html_seen += 1
            if not accepted:
                report.crawler_html_rejected += 1
        if book_like:
            report.crawler_book_like_found += 1
        if not accepted:
            report.crawler_rejection_counts[reason] = int(report.crawler_rejection_counts.get(reason, 0)) + 1

        _emit_progress(
            progress_callback,
            event="crawl-observation",
            report=report,
            details={
                "url": candidate_url,
                "accepted": accepted,
                "reason": reason,
                "extension": extension,
                "html_like": html_like,
                "book_like": book_like,
            },
        )

    def _process_discovered_url(crawled_url: str) -> None:
        marker = str(crawled_url or "").strip() or "<unknown>"
        report.scanned_files += 1
        try:
            storage_key = _storage_key_from_store_url(store_url=store_url, file_url=marker)
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
            payload = _build_remote_file_payload(
                file_url=marker,
                storage_key=storage_key,
                stat_blob=None,
                store_id=store_id,
                now_epk=_now_ep_ms(),
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
        discovery_source.discover_urls(
            force=False,
            log_line_callback=_on_crawl_log_line,
            discovered_url_callback=_process_discovered_url,
            observed_url_callback=_on_crawl_observation,
        )
    else:
        crawled_urls = discovery_source.discover_urls(
            force=False,
            log_line_callback=_on_crawl_log_line,
            observed_url_callback=_on_crawl_observation,
        )
        for crawled_url in crawled_urls:
            _process_discovered_url(crawled_url)

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
        details={"mode": mode, "store_id": store_id, "store_root_uri": str(store_url)},
    )
    return report


__all__ = ["ingest_html_discovery_store_files"]
