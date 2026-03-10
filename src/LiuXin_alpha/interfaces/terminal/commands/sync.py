"""`sync` command group for store/database reconciliation."""

from __future__ import annotations

import dataclasses
import json

from typing import Optional

from LiuXin_alpha.interfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import get_default_rclone_http_requests_per_hour
from LiuXin_alpha.storage.reconcile import (
    register_existing_disk_as_unmanaged_store,
    register_rclone_http_readonly_store_files,
)


@dataclasses.dataclass(frozen=True)
class _SyncStoreOptions:
    store_ref: str
    source_label: str
    ebook_extensions: Optional[list[str]]
    compute_hash: bool
    capture_hashes: bool
    follow_symlinks: bool
    refresh_storage_manager: bool
    attach_store_links: bool
    max_http_requests_per_hour: Optional[float]
    json_output: bool


def _split_extensions(raw: Optional[str]) -> Optional[list[str]]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    for separator in (";", " ", "\t", "\n"):
        text = text.replace(separator, ",")
    parts = [part.strip().lstrip(".").lower() for part in text.split(",")]
    values = [part for part in parts if part]
    if not values:
        return None
    deduped: list[str] = []
    for value in values:
        if value not in deduped:
            deduped.append(value)
    return deduped


def _read_option_value(args: list[str], idx: int, *, option_name: str) -> tuple[str, int]:
    token = args[idx]
    if "=" in token:
        _, value = token.split("=", 1)
        if value.strip() == "":
            raise ValueError("Option {} requires a non-blank value.".format(option_name))
        return value, idx + 1
    if idx + 1 >= len(args):
        raise ValueError("Option {} requires a value.".format(option_name))
    value = args[idx + 1]
    if str(value).strip() == "":
        raise ValueError("Option {} requires a non-blank value.".format(option_name))
    return value, idx + 2


def _parse_sync_store_options(args: list[str], *, usage: str) -> _SyncStoreOptions:
    if not args:
        raise ValueError("Usage: {}".format(usage))

    store_ref: Optional[str] = None
    source_label = "on_disk_unmanaged_import"
    extensions_raw: Optional[str] = None
    compute_hash = True
    capture_hashes = False
    follow_symlinks = False
    refresh_storage_manager = True
    attach_store_links = True
    max_http_requests_per_hour: Optional[float] = None
    json_output = False

    idx = 0
    while idx < len(args):
        token = str(args[idx]).strip()

        if token.lower() in {"to-db", "to_db", "todb"}:
            idx += 1
            continue

        if token == "--source" or token.startswith("--source="):
            value, idx = _read_option_value(args, idx, option_name="--source")
            source_label = value.strip()
            if not source_label:
                raise ValueError("Option --source requires a non-blank value.")
            continue

        if token == "--extensions" or token.startswith("--extensions="):
            value, idx = _read_option_value(args, idx, option_name="--extensions")
            extensions_raw = value
            continue

        if token == "--max-http-requests-per-hour" or token.startswith("--max-http-requests-per-hour="):
            value, idx = _read_option_value(args, idx, option_name="--max-http-requests-per-hour")
            text = str(value).strip().lower()
            if text in {"none", "off", "disable", "disabled"}:
                max_http_requests_per_hour = 0.0
                continue
            try:
                max_http_requests_per_hour = float(text)
            except Exception:
                raise ValueError("Option --max-http-requests-per-hour requires a numeric value or 'none'.")
            continue

        if token == "--no-hash":
            compute_hash = False
            idx += 1
            continue
        if token == "--hash":
            compute_hash = True
            idx += 1
            continue
        if token == "--follow-symlinks":
            follow_symlinks = True
            idx += 1
            continue
        if token == "--no-follow-symlinks":
            follow_symlinks = False
            idx += 1
            continue
        if token == "--no-refresh":
            refresh_storage_manager = False
            idx += 1
            continue
        if token == "--refresh":
            refresh_storage_manager = True
            idx += 1
            continue
        if token == "--no-links":
            attach_store_links = False
            idx += 1
            continue
        if token == "--links":
            attach_store_links = True
            idx += 1
            continue
        if token == "--capture-hashes":
            capture_hashes = True
            idx += 1
            continue
        if token == "--no-capture-hashes":
            capture_hashes = False
            idx += 1
            continue
        if token == "--json":
            json_output = True
            idx += 1
            continue

        if token.startswith("-"):
            raise ValueError("Unknown option: {!r}".format(token))

        if store_ref is not None:
            raise ValueError("Unexpected extra argument {!r}. Usage: {}".format(token, usage))
        store_ref = token
        idx += 1

    if store_ref is None:
        raise ValueError("Usage: {}".format(usage))

    return _SyncStoreOptions(
        store_ref=store_ref,
        source_label=source_label,
        ebook_extensions=_split_extensions(extensions_raw),
        compute_hash=compute_hash,
        capture_hashes=capture_hashes,
        follow_symlinks=follow_symlinks,
        refresh_storage_manager=refresh_storage_manager,
        attach_store_links=attach_store_links,
        max_http_requests_per_hour=max_http_requests_per_hour,
        json_output=json_output,
    )


def _safe_int(value: str) -> Optional[int]:
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _resolve_store_row(browser, store_ref: str):
    if "stores" not in set(browser.db.get_tables()):
        raise ValueError("Database schema does not contain `stores` table.")

    store_id = _safe_int(store_ref)
    if store_id is not None:
        row = browser.db.get_row_from_id("stores", store_id)
        if row is None:
            raise ValueError("No store found for id {}.".format(store_id))
        return row

    rows = browser.db.search("stores", "store_name", str(store_ref))
    if not rows:
        raise ValueError("No store found for name {!r}.".format(store_ref))
    if len(rows) > 1:
        raise ValueError("Multiple stores found for name {!r}; use store id instead.".format(store_ref))
    return rows[0]


def _is_rclone_http_store(store_row) -> bool:
    kind = str(store_row["store_kind"] or "").strip().lower()
    protocol = str(store_row["store_access_protocol"] or "").strip().lower()
    if kind in {"rclone_http_readonly", "rclone_http_ro", "http_ro"}:
        return True
    return protocol in {"http", "https", "rclone"}


class SyncStoreCommand(TerminalCommandAPI):
    """Reconcile one existing store with the files table."""

    group = "sync"
    group_aliases = ("reconcile",)
    name = "store"
    aliases = ("stores",)
    summary = "Sync one store: sync store <store_id|store_name> [to-db] [options]"
    usage = (
        "sync store <store_id|store_name> [to-db] [--extensions epub,mobi] [--source <label>] "
        "[--no-hash] [--capture-hashes] [--max-http-requests-per-hour <prefs-default>] "
        "[--follow-symlinks] [--no-refresh] [--no-links] [--json]"
    )
    expose_direct = False

    def execute(self, browser, args: list[str]) -> bool:
        options = _parse_sync_store_options(args, usage=self.usage)
        store_row = _resolve_store_row(browser, options.store_ref)

        store_root_uri = str(store_row["store_root_uri"] or "").strip()
        if not store_root_uri:
            raise ValueError("Store {} has no `store_root_uri`.".format(store_row["store_id"]))

        store_name_raw = str(store_row["store_name"] or "").strip()
        store_name = store_name_raw or None
        store_kind_raw = str(store_row["store_kind"] or "").strip()
        store_kind = store_kind_raw or "on_disk_existing_unmanaged_drive"

        if _is_rclone_http_store(store_row):
            effective_max_http_requests_per_hour = (
                get_default_rclone_http_requests_per_hour()
                if options.max_http_requests_per_hour is None
                else options.max_http_requests_per_hour
            )
            source_label = options.source_label
            if source_label == "on_disk_unmanaged_import":
                source_label = "rclone_http_import"
            report = register_rclone_http_readonly_store_files(
                browser.db,
                remote_url=store_root_uri,
                store_name=store_name,
                store_kind=store_kind,
                max_http_requests_per_hour=effective_max_http_requests_per_hour,
                ebook_extensions=options.ebook_extensions,
                source_label=source_label,
                capture_hashes=options.capture_hashes,
                attach_store_links=options.attach_store_links,
                refresh_storage_manager=options.refresh_storage_manager,
            )
        else:
            report = register_existing_disk_as_unmanaged_store(
                browser.db,
                disk_path=store_root_uri,
                store_name=store_name,
                store_kind=store_kind,
                ebook_extensions=options.ebook_extensions,
                source_label=options.source_label,
                compute_hash=options.compute_hash,
                follow_symlinks=options.follow_symlinks,
                attach_store_links=options.attach_store_links,
                refresh_storage_manager=options.refresh_storage_manager,
            )

        if options.json_output:
            browser.emit(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True, indent=2))
            return True

        browser.emit("Sync completed:")
        browser.emit("  store_id: {}".format(report.store_row_id))
        browser.emit("  store_name: {}".format(report.store_name))
        browser.emit("  store_root_uri: {}".format(report.store_root_uri))
        browser.emit("  scanned_files: {}".format(report.scanned_files))
        browser.emit("  ebook_candidates: {}".format(report.ebook_candidates))
        browser.emit("  skipped_non_ebook_files: {}".format(report.skipped_non_ebook_files))
        browser.emit("  inserted_files: {}".format(report.inserted_files))
        browser.emit("  updated_files: {}".format(report.updated_files))
        browser.emit("  unchanged_files: {}".format(report.unchanged_files))
        browser.emit("  linked_files: {}".format(report.linked_files))
        browser.emit("  errors: {}".format(len(report.errors)))
        if _is_rclone_http_store(store_row):
            browser.emit(
                "  max_http_requests_per_hour: {}".format(
                    get_default_rclone_http_requests_per_hour()
                    if options.max_http_requests_per_hour is None
                    else options.max_http_requests_per_hour
                )
            )
        if report.errors:
            preview_count = min(5, len(report.errors))
            browser.emit("  error_preview:")
            for error in report.errors[:preview_count]:
                browser.emit("    - {}".format(error))
            if len(report.errors) > preview_count:
                browser.emit("    ... {} more".format(len(report.errors) - preview_count))
        return True


__all__ = [
    "SyncStoreCommand",
]
