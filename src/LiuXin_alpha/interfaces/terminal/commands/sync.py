"""`sync` command group for store/database reconciliation."""

from __future__ import annotations

import dataclasses
import json
import time

from typing import Optional

from LiuXin_alpha.interfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.utils.jobs import JobRequest
from LiuXin_alpha.storage.store_backend_plugins.native_html_readonly import get_default_native_html_requests_per_hour
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import get_default_rclone_http_requests_per_hour
from LiuXin_alpha.storage.store_backend_plugins.wget_html_readonly import get_default_wget_http_requests_per_hour
from LiuXin_alpha.storage.reconcile import (
    register_existing_disk_as_unmanaged_store,
    register_existing_disk_with_database_path,
    register_native_html_readonly_store_files,
    register_native_html_readonly_with_database_path,
    register_rclone_http_readonly_store_files,
    register_rclone_http_readonly_with_database_path,
    register_wget_html_readonly_store_files,
    register_wget_html_readonly_with_database_path,
)


def _crawler_progress_suffix(report) -> str:
    observed = int(getattr(report, "crawler_urls_observed", 0) or 0)
    if observed <= 0:
        return ""
    return (
        " crawl_urls={} html_seen={} book_like={} rejected_html={}".format(
            observed,
            int(getattr(report, "crawler_html_seen", 0) or 0),
            int(getattr(report, "crawler_book_like_found", 0) or 0),
            int(getattr(report, "crawler_html_rejected", 0) or 0),
        )
    )


def _crawler_rejection_summary(report) -> str:
    counts = dict(getattr(report, "crawler_rejection_counts", {}) or {})
    if not counts:
        return ""
    parts = ["{}={}".format(key, counts[key]) for key in sorted(counts.keys())]
    return " | ".join(parts)


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
    rclone_http_no_slash: bool
    rclone_http_no_head: bool
    wget_recurse: bool
    wget_max_depth: Optional[int]
    wget_timeout_s: Optional[float]
    wget_no_parent: bool
    wget_span_hosts: bool
    wget_respect_robots: bool
    wget_user_agent: Optional[str]
    wget_no_verbose: bool
    wget_args: tuple[str, ...]
    wget_incremental_db_writes: bool
    background: bool
    job_backend: Optional[str]
    job_timeout_s: Optional[float]
    job_no_output: bool
    job_panel: bool
    show_progress: bool
    progress_every: int
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
    rclone_http_no_slash = False
    rclone_http_no_head = False
    wget_recurse = True
    wget_max_depth: Optional[int] = None
    wget_timeout_s: Optional[float] = None
    wget_no_parent = True
    wget_span_hosts = False
    wget_respect_robots = True
    wget_user_agent: Optional[str] = None
    wget_no_verbose = False
    wget_args: list[str] = []
    wget_incremental_db_writes = True
    background = False
    job_backend: Optional[str] = None
    job_timeout_s: Optional[float] = None
    job_no_output = False
    job_panel = False
    show_progress = True
    progress_every = 100
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
        if token == "--progress-every" or token.startswith("--progress-every="):
            value, idx = _read_option_value(args, idx, option_name="--progress-every")
            try:
                parsed = int(str(value).strip())
            except Exception:
                raise ValueError("Option --progress-every requires an integer value.")
            if parsed <= 0:
                raise ValueError("Option --progress-every must be >= 1.")
            progress_every = parsed
            continue
        if token == "--rclone-http-no-slash":
            rclone_http_no_slash = True
            idx += 1
            continue
        if token == "--rclone-http-no-head":
            rclone_http_no_head = True
            idx += 1
            continue
        if token == "--wget-recurse":
            wget_recurse = True
            idx += 1
            continue
        if token == "--wget-no-recurse":
            wget_recurse = False
            idx += 1
            continue
        if token == "--wget-max-depth" or token.startswith("--wget-max-depth="):
            value, idx = _read_option_value(args, idx, option_name="--wget-max-depth")
            text = str(value).strip().lower()
            if text in {"none", "inf", "infinite", "unbounded", "off", "disable", "disabled"}:
                wget_max_depth = None
                continue
            try:
                parsed_depth = int(text)
            except Exception:
                raise ValueError("Option --wget-max-depth requires an integer value or 'none'.")
            if parsed_depth <= 0:
                raise ValueError("Option --wget-max-depth must be >= 1.")
            wget_max_depth = parsed_depth
            continue
        if token == "--wget-timeout-s" or token.startswith("--wget-timeout-s="):
            value, idx = _read_option_value(args, idx, option_name="--wget-timeout-s")
            text = str(value).strip().lower()
            if text in {"none", "off", "disable", "disabled", "inf", "infinite"}:
                wget_timeout_s = None
                continue
            try:
                parsed_timeout = float(text)
            except Exception:
                raise ValueError("Option --wget-timeout-s requires a numeric value or 'none'.")
            if parsed_timeout <= 0:
                raise ValueError("Option --wget-timeout-s must be > 0, or use 'none'.")
            wget_timeout_s = parsed_timeout
            continue
        if token == "--wget-no-parent":
            wget_no_parent = True
            idx += 1
            continue
        if token == "--wget-parent":
            wget_no_parent = False
            idx += 1
            continue
        if token == "--wget-span-hosts":
            wget_span_hosts = True
            idx += 1
            continue
        if token == "--wget-no-span-hosts":
            wget_span_hosts = False
            idx += 1
            continue
        if token == "--wget-ignore-robots":
            wget_respect_robots = False
            idx += 1
            continue
        if token == "--wget-respect-robots":
            wget_respect_robots = True
            idx += 1
            continue
        if token == "--wget-user-agent" or token.startswith("--wget-user-agent="):
            value, idx = _read_option_value(args, idx, option_name="--wget-user-agent")
            wget_user_agent = str(value).strip()
            if not wget_user_agent:
                raise ValueError("Option --wget-user-agent requires a non-blank value.")
            continue
        if token == "--wget-verbose":
            wget_no_verbose = False
            idx += 1
            continue
        if token == "--wget-no-verbose":
            wget_no_verbose = True
            idx += 1
            continue
        if token == "--wget-arg" or token.startswith("--wget-arg="):
            value, idx = _read_option_value(args, idx, option_name="--wget-arg")
            wget_arg = str(value).strip()
            if not wget_arg:
                raise ValueError("Option --wget-arg requires a non-blank value.")
            wget_args.append(wget_arg)
            continue
        if token == "--wget-incremental-db-writes":
            wget_incremental_db_writes = True
            idx += 1
            continue
        if token == "--wget-no-incremental-db-writes":
            wget_incremental_db_writes = False
            idx += 1
            continue
        if token == "--background":
            background = True
            idx += 1
            continue
        if token == "--foreground":
            background = False
            idx += 1
            continue
        if token == "--job-backend" or token.startswith("--job-backend="):
            value, idx = _read_option_value(args, idx, option_name="--job-backend")
            text = str(value).strip()
            if not text:
                raise ValueError("Option --job-backend requires a non-blank value.")
            job_backend = text
            continue
        if token == "--job-timeout-s" or token.startswith("--job-timeout-s="):
            value, idx = _read_option_value(args, idx, option_name="--job-timeout-s")
            text = str(value).strip().lower()
            if text in {"none", "off", "disable", "disabled", "inf", "infinite"}:
                job_timeout_s = None
                continue
            try:
                parsed = float(text)
            except Exception:
                raise ValueError("Option --job-timeout-s requires a numeric value or 'none'.")
            if parsed <= 0:
                raise ValueError("Option --job-timeout-s must be > 0, or use 'none'.")
            job_timeout_s = parsed
            continue
        if token == "--job-no-output":
            job_no_output = True
            idx += 1
            continue
        if token == "--job-output":
            job_no_output = False
            idx += 1
            continue
        if token == "--job-panel":
            job_panel = True
            idx += 1
            continue
        if token == "--no-job-panel":
            job_panel = False
            idx += 1
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
        if token == "--progress":
            show_progress = True
            idx += 1
            continue
        if token == "--no-progress":
            show_progress = False
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
        rclone_http_no_slash=rclone_http_no_slash,
        rclone_http_no_head=rclone_http_no_head,
        wget_recurse=wget_recurse,
        wget_max_depth=wget_max_depth,
        wget_timeout_s=wget_timeout_s,
        wget_no_parent=wget_no_parent,
        wget_span_hosts=wget_span_hosts,
        wget_respect_robots=wget_respect_robots,
        wget_user_agent=wget_user_agent,
        wget_no_verbose=wget_no_verbose,
        wget_args=tuple(wget_args),
        wget_incremental_db_writes=wget_incremental_db_writes,
        background=background,
        job_backend=job_backend,
        job_timeout_s=job_timeout_s,
        job_no_output=job_no_output,
        job_panel=job_panel,
        show_progress=show_progress,
        progress_every=progress_every,
        json_output=json_output,
    )


def _sync_job_progress_logger(event: str, report, details: dict[str, object], *, progress_every: int) -> None:
    scanned = int(getattr(report, "scanned_files", 0) or 0)
    observed = int(getattr(report, "crawler_urls_observed", 0) or 0)
    if event == "start":
        print(
            "JOB sync started: mode={} store_id={} root={}".format(
                details.get("mode", "unknown"),
                getattr(report, "store_row_id", "<unknown>"),
                getattr(report, "store_root_uri", "<unknown>"),
            ),
            flush=True,
        )
        return
    if event == "crawl-log":
        line = str(details.get("line", "")).strip()
        if line:
            print("JOB wget: {}".format(line), flush=True)
        return
    if event == "error":
        print(
            "JOB sync warning: {} (errors={})".format(
                details.get("path", "<unknown>"),
                len(getattr(report, "errors", []) or []),
            ),
            flush=True,
        )
        return
    if event in {"scan", "crawl-observation"}:
        n = max(1, int(progress_every))
        tick = max(scanned, observed)
        if tick == 1 or (tick % n == 0):
            print(
                "JOB sync progress: scanned={} candidates={} inserted={} updated={} unchanged={} linked={} errors={}{}".format(
                    getattr(report, "scanned_files", 0),
                    getattr(report, "ebook_candidates", 0),
                    getattr(report, "inserted_files", 0),
                    getattr(report, "updated_files", 0),
                    getattr(report, "unchanged_files", 0),
                    getattr(report, "linked_files", 0),
                    len(getattr(report, "errors", []) or []),
                    _crawler_progress_suffix(report),
                ),
                flush=True,
            )
        return
    if event == "done":
        print(
            "JOB sync completed: scanned={} candidates={} inserted={} updated={} unchanged={} linked={} errors={}{}".format(
                getattr(report, "scanned_files", 0),
                getattr(report, "ebook_candidates", 0),
                getattr(report, "inserted_files", 0),
                getattr(report, "updated_files", 0),
                getattr(report, "unchanged_files", 0),
                getattr(report, "linked_files", 0),
                len(getattr(report, "errors", []) or []),
                _crawler_progress_suffix(report),
            ),
            flush=True,
        )
        return


def run_sync_store_job(
    *,
    database_path: str,
    db_type: str,
    mode: str,
    store_root_uri: str,
    store_name: Optional[str],
    store_kind: str,
    source_label: str,
    ebook_extensions: Optional[list[str]],
    compute_hash: bool,
    capture_hashes: bool,
    follow_symlinks: bool,
    attach_store_links: bool,
    refresh_storage_manager: bool,
    max_http_requests_per_hour: Optional[float],
    rclone_args: tuple[str, ...],
    wget_recurse: bool,
    wget_max_depth: Optional[int],
    wget_timeout_s: Optional[float],
    wget_no_parent: bool,
    wget_span_hosts: bool,
    wget_respect_robots: bool,
    wget_user_agent: Optional[str],
    wget_no_verbose: bool,
    wget_args: tuple[str, ...],
    wget_incremental_db_writes: bool = True,
    progress_output: bool = True,
    progress_every: int = 100,
) -> dict[str, object]:
    """
    Background-safe sync entrypoint for `sync store --background`.

    This function is intentionally module-level so the jobs backend can import
    and execute it in either a process or serial backend.
    """
    mode_norm = str(mode or "").strip().lower()
    callback = None
    if progress_output:
        callback = lambda event, report, details: _sync_job_progress_logger(  # noqa: E731
            event,
            report,
            details,
            progress_every=progress_every,
        )

    if mode_norm == "rclone":
        report = register_rclone_http_readonly_with_database_path(
            database_path=database_path,
            remote_url=store_root_uri,
            db_type=db_type,
            store_name=store_name,
            store_kind=store_kind,
            max_http_requests_per_hour=max_http_requests_per_hour,
            rclone_args=rclone_args,
            ebook_extensions=ebook_extensions,
            source_label=source_label,
            capture_hashes=capture_hashes,
            attach_store_links=attach_store_links,
            refresh_storage_manager=refresh_storage_manager,
            progress_callback=callback,
        )
        return report.to_dict()

    if mode_norm == "wget":
        report = register_wget_html_readonly_with_database_path(
            database_path=database_path,
            remote_url=store_root_uri,
            db_type=db_type,
            store_name=store_name,
            store_kind=store_kind,
            max_http_requests_per_hour=max_http_requests_per_hour,
            wget_args=wget_args,
            timeout_s=wget_timeout_s,
            recurse=wget_recurse,
            max_depth=wget_max_depth,
            no_parent=wget_no_parent,
            span_hosts=wget_span_hosts,
            respect_robots=wget_respect_robots,
            user_agent=wget_user_agent,
            no_verbose=wget_no_verbose,
            ebook_extensions=ebook_extensions,
            source_label=source_label,
            attach_store_links=attach_store_links,
            refresh_storage_manager=refresh_storage_manager,
            incremental_db_writes=bool(wget_incremental_db_writes),
            progress_callback=callback,
        )
        return report.to_dict()

    if mode_norm == "native":
        report = register_native_html_readonly_with_database_path(
            database_path=database_path,
            remote_url=store_root_uri,
            db_type=db_type,
            store_name=store_name,
            store_kind=store_kind,
            max_http_requests_per_hour=max_http_requests_per_hour,
            timeout_s=wget_timeout_s,
            recurse=wget_recurse,
            max_depth=wget_max_depth,
            no_parent=wget_no_parent,
            span_hosts=wget_span_hosts,
            respect_robots=wget_respect_robots,
            user_agent=wget_user_agent,
            ebook_extensions=ebook_extensions,
            source_label=source_label,
            attach_store_links=attach_store_links,
            refresh_storage_manager=refresh_storage_manager,
            incremental_db_writes=bool(wget_incremental_db_writes),
            progress_callback=callback,
        )
        return report.to_dict()

    if mode_norm == "local":
        report = register_existing_disk_with_database_path(
            database_path=database_path,
            disk_path=store_root_uri,
            db_type=db_type,
            store_name=store_name,
            store_kind=store_kind,
            ebook_extensions=ebook_extensions,
            source_label=source_label,
            compute_hash=compute_hash,
            follow_symlinks=follow_symlinks,
            attach_store_links=attach_store_links,
            refresh_storage_manager=refresh_storage_manager,
            progress_callback=callback,
        )
        return report.to_dict()

    raise ValueError("Unknown sync mode: {!r}".format(mode))


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
    if _is_wget_html_store(store_row):
        return False
    if _is_native_html_store(store_row):
        return False
    if kind in {"rclone_http_readonly", "rclone_http_ro", "http_ro"}:
        return True
    return protocol in {"http", "https", "rclone"}


def _is_wget_html_store(store_row) -> bool:
    kind = str(store_row["store_kind"] or "").strip().lower()
    protocol = str(store_row["store_access_protocol"] or "").strip().lower()
    if kind in {"wget_html_readonly", "wget_http_ro", "http_spider_ro"}:
        return True
    return protocol == "wget"


def _is_native_html_store(store_row) -> bool:
    kind = str(store_row["store_kind"] or "").strip().lower()
    protocol = str(store_row["store_access_protocol"] or "").strip().lower()
    if kind in {"native_html_readonly", "native_http_ro", "http_native_ro"}:
        return True
    return protocol in {"native", "native_html"}


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
        "[--rclone-http-no-slash] [--rclone-http-no-head] "
        "[--wget-no-recurse] [--wget-max-depth <n|none>] [--wget-timeout-s <sec|none>] "
        "[--wget-parent|--wget-no-parent] "
        "[--wget-span-hosts|--wget-no-span-hosts] [--wget-ignore-robots|--wget-respect-robots] "
        "[--wget-user-agent <ua>] [--wget-verbose|--wget-no-verbose] [--wget-arg <arg> ...] "
        "[--wget-incremental-db-writes|--wget-no-incremental-db-writes] "
        "[--background] [--job-backend process|serial] [--job-timeout-s <sec|none>] [--job-output|--job-no-output] "
        "[--job-panel|--no-job-panel] "
        "[--follow-symlinks] [--no-refresh] [--no-links] "
        "[--progress|--no-progress] [--progress-every <n>] [--json]"
    )
    expose_direct = False

    def execute(self, browser, args: list[str]) -> bool:
        options = _parse_sync_store_options(args, usage=self.usage)
        if options.json_output and options.show_progress:
            options = dataclasses.replace(options, show_progress=False)
        store_row = _resolve_store_row(browser, options.store_ref)

        store_root_uri = str(store_row["store_root_uri"] or "").strip()
        if not store_root_uri:
            raise ValueError("Store {} has no `store_root_uri`.".format(store_row["store_id"]))

        store_name_raw = str(store_row["store_name"] or "").strip()
        store_name = store_name_raw or None
        store_kind_raw = str(store_row["store_kind"] or "").strip()
        store_kind = store_kind_raw or "on_disk_existing_unmanaged_drive"
        is_wget = _is_wget_html_store(store_row)
        is_native = _is_native_html_store(store_row)
        is_rclone = _is_rclone_http_store(store_row)

        start_monotonic = time.monotonic()
        progress_state = {
            "last_scanned": 0,
            "last_observed": 0,
            "last_emit_monotonic": 0.0,
        }

        def _emit_progress_line(event: str, report, details: dict[str, object]) -> None:
            if not options.show_progress:
                return
            now = time.monotonic()
            if event == "start":
                browser.emit(
                    "Sync started: store_id={} mode={} root={}".format(
                        report.store_row_id,
                        details.get("mode", "unknown"),
                        report.store_root_uri,
                    )
                )
                progress_state["last_emit_monotonic"] = now
                return
            if event == "done":
                browser.emit(
                    "Sync progress: scanned={} candidates={} inserted={} updated={} unchanged={} linked={} errors={}{}".format(
                        report.scanned_files,
                        report.ebook_candidates,
                        report.inserted_files,
                        report.updated_files,
                        report.unchanged_files,
                        report.linked_files,
                        len(report.errors),
                        _crawler_progress_suffix(report),
                    )
                )
                return
            if event == "error":
                browser.emit(
                    "Sync warning: {} (errors={})".format(
                        details.get("path", "<unknown>"),
                        len(report.errors),
                    )
                )
                progress_state["last_emit_monotonic"] = now
                return
            if event == "crawl-log":
                line = str(details.get("line", "")).strip()
                if line:
                    mode_label = str(details.get("mode", "") or "").strip()
                    prefix = "Crawler"
                    if mode_label == "wget":
                        prefix = "Wget"
                    elif mode_label == "native_html":
                        prefix = "Native"
                    browser.emit("{}: {}".format(prefix, line))
                progress_state["last_emit_monotonic"] = now
                return
            observed = int(getattr(report, "crawler_urls_observed", 0) or 0)
            scanned = int(report.scanned_files)
            delta = max(scanned, observed) - max(
                int(progress_state.get("last_scanned", 0)),
                int(progress_state.get("last_observed", 0)),
            )
            last_emit = float(progress_state.get("last_emit_monotonic", 0.0))
            if delta < options.progress_every and (now - last_emit) < 2.0:
                return
            elapsed = max(0.001, now - start_monotonic)
            rate = float(max(scanned, observed)) / elapsed
            browser.emit(
                "Sync progress: scanned={} candidates={} inserted={} updated={} unchanged={} linked={} errors={} rate={:.1f}/s{}".format(
                    report.scanned_files,
                    report.ebook_candidates,
                    report.inserted_files,
                    report.updated_files,
                    report.unchanged_files,
                    report.linked_files,
                    len(report.errors),
                    rate,
                    _crawler_progress_suffix(report),
                )
            )
            progress_state["last_scanned"] = scanned
            progress_state["last_observed"] = observed
            progress_state["last_emit_monotonic"] = now

        rclone_args: list[str] = []
        if options.rclone_http_no_slash:
            rclone_args.append("--http-no-slash")
        if options.rclone_http_no_head:
            rclone_args.append("--http-no-head")

        effective_max_http_requests_per_hour: Optional[float]
        if is_rclone:
            effective_max_http_requests_per_hour = (
                get_default_rclone_http_requests_per_hour()
                if options.max_http_requests_per_hour is None
                else options.max_http_requests_per_hour
            )
        elif is_wget:
            effective_max_http_requests_per_hour = (
                get_default_wget_http_requests_per_hour()
                if options.max_http_requests_per_hour is None
                else options.max_http_requests_per_hour
            )
        elif is_native:
            effective_max_http_requests_per_hour = (
                get_default_native_html_requests_per_hour()
                if options.max_http_requests_per_hour is None
                else options.max_http_requests_per_hour
            )
        else:
            effective_max_http_requests_per_hour = options.max_http_requests_per_hour

        source_label = options.source_label
        if is_rclone and source_label == "on_disk_unmanaged_import":
            source_label = "rclone_http_import"
        elif is_wget and source_label == "on_disk_unmanaged_import":
            source_label = "wget_html_import"
        elif is_native and source_label == "on_disk_unmanaged_import":
            source_label = "native_html_import"

        if options.background:
            if options.json_output:
                raise ValueError("--json is not supported with --background. Use `jobs show <id>` for details.")
            database_path = str(browser.database_path or "").strip()
            if not database_path:
                raise ValueError("Cannot submit background sync without a resolvable database path.")
            db_type = str(getattr(browser.db, "type", "") or "SQLite")
            mode = "rclone" if is_rclone else ("wget" if is_wget else ("native" if is_native else "local"))
            store_id_value = int(store_row["store_id"])
            sync_kwargs = {
                "database_path": database_path,
                "db_type": db_type,
                "mode": mode,
                "store_root_uri": store_root_uri,
                "store_name": store_name,
                "store_kind": store_kind,
                "source_label": source_label,
                "ebook_extensions": options.ebook_extensions,
                "compute_hash": options.compute_hash,
                "capture_hashes": options.capture_hashes,
                "follow_symlinks": options.follow_symlinks,
                "attach_store_links": options.attach_store_links,
                "refresh_storage_manager": options.refresh_storage_manager,
                "max_http_requests_per_hour": effective_max_http_requests_per_hour,
                "rclone_args": tuple(rclone_args),
                "wget_recurse": options.wget_recurse,
                "wget_max_depth": options.wget_max_depth,
                "wget_timeout_s": options.wget_timeout_s,
                "wget_no_parent": options.wget_no_parent,
                "wget_span_hosts": options.wget_span_hosts,
                "wget_respect_robots": options.wget_respect_robots,
                "wget_user_agent": options.wget_user_agent,
                "wget_no_verbose": options.wget_no_verbose,
                "wget_args": options.wget_args,
                "wget_incremental_db_writes": options.wget_incremental_db_writes,
                "progress_output": not options.job_no_output,
                "progress_every": options.progress_every,
            }
            label = "sync:{}:{}".format(mode, store_id_value)
            job_id = ""
            if hasattr(browser, "supports_core_commands") and bool(browser.supports_core_commands()):
                result = browser.execute_core_command(
                    "sync.store.start",
                    payload={
                        "sync_kwargs": sync_kwargs,
                        "job_backend": options.job_backend,
                        "job_timeout_s": options.job_timeout_s,
                        "job_no_output": options.job_no_output,
                        "label": label,
                    },
                )
                job_id = str((result or {}).get("job_id", "")).strip()
                if not job_id:
                    raise RuntimeError("Core command `sync.store.start` did not return a job id.")
            else:
                request = JobRequest(
                    module_name="LiuXin_alpha.interfaces.terminal.commands.sync",
                    function_name="run_sync_store_job",
                    kwargs=sync_kwargs,
                )
                job_timeout = -1.0 if options.job_timeout_s is None else float(options.job_timeout_s)
                job_id = browser.job_manager.submit(
                    request,
                    timeout=job_timeout,
                    no_output=options.job_no_output,
                    backend=options.job_backend,
                    label=label,
                )
            browser.emit_detail_sections(
                [
                    (
                        "Submission",
                        [
                            ("mode", mode),
                            ("store_id", store_id_value),
                            ("backend", options.job_backend or "default"),
                            ("timeout_s", "none" if options.job_timeout_s is None else options.job_timeout_s),
                            ("output_capture", "disabled" if options.job_no_output else "enabled"),
                        ],
                    )
                ],
                title="Sync job submitted: {}".format(job_id),
                max_cell_width=120,
            )
            browser.emit("  Use `jobs show {} --wait` to inspect completion.".format(job_id))
            if options.job_panel:
                panel_rows: list[tuple[str, object]] = []
                if browser.supports_job_output_panel():
                    browser.attach_job_output_panel(job_id)
                    panel_rows.append(("output_panel", "attached to job {}".format(job_id)))
                    if options.job_no_output:
                        panel_rows.append(("output_panel_note", "job output capture is disabled (--job-no-output)"))
                else:
                    panel_rows.append(("output_panel", "unavailable in this UI mode"))
                browser.emit_detail_sections([("", panel_rows)], max_cell_width=120)
            return True

        if options.job_panel:
            raise ValueError("--job-panel requires --background.")

        if is_rclone:
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
                rclone_args=tuple(rclone_args),
                progress_callback=_emit_progress_line,
            )
        elif is_wget:
            if options.capture_hashes:
                browser.emit("Note: --capture-hashes is ignored for wget spider stores.")
            report = register_wget_html_readonly_store_files(
                browser.db,
                remote_url=store_root_uri,
                store_name=store_name,
                store_kind=store_kind,
                max_http_requests_per_hour=effective_max_http_requests_per_hour,
                wget_args=options.wget_args,
                recurse=options.wget_recurse,
                max_depth=options.wget_max_depth,
                timeout_s=options.wget_timeout_s,
                no_parent=options.wget_no_parent,
                span_hosts=options.wget_span_hosts,
                respect_robots=options.wget_respect_robots,
                user_agent=options.wget_user_agent,
                no_verbose=options.wget_no_verbose,
                ebook_extensions=options.ebook_extensions,
                source_label=source_label,
                attach_store_links=options.attach_store_links,
                refresh_storage_manager=options.refresh_storage_manager,
                incremental_db_writes=options.wget_incremental_db_writes,
                progress_callback=_emit_progress_line,
            )
        elif is_native:
            if options.capture_hashes:
                browser.emit("Note: --capture-hashes is ignored for native HTML crawler stores.")
            report = register_native_html_readonly_store_files(
                browser.db,
                remote_url=store_root_uri,
                store_name=store_name,
                store_kind=store_kind,
                max_http_requests_per_hour=effective_max_http_requests_per_hour,
                timeout_s=options.wget_timeout_s,
                recurse=options.wget_recurse,
                max_depth=options.wget_max_depth,
                no_parent=options.wget_no_parent,
                span_hosts=options.wget_span_hosts,
                respect_robots=options.wget_respect_robots,
                user_agent=options.wget_user_agent,
                ebook_extensions=options.ebook_extensions,
                source_label=source_label,
                attach_store_links=options.attach_store_links,
                refresh_storage_manager=options.refresh_storage_manager,
                incremental_db_writes=options.wget_incremental_db_writes,
                progress_callback=_emit_progress_line,
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
                progress_callback=_emit_progress_line,
            )

        if options.json_output:
            browser.emit(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True, indent=2))
            return True

        sections: list[tuple[str, list[tuple[str, object]]]] = [
            (
                "Store",
                [
                    ("store_id", report.store_row_id),
                    ("store_name", report.store_name),
                    ("store_root_uri", report.store_root_uri),
                ],
            ),
            (
                "Results",
                [
                    ("scanned_files", report.scanned_files),
                    ("ebook_candidates", report.ebook_candidates),
                    ("skipped_non_ebook_files", report.skipped_non_ebook_files),
                    ("inserted_files", report.inserted_files),
                    ("updated_files", report.updated_files),
                    ("unchanged_files", report.unchanged_files),
                    ("linked_files", report.linked_files),
                    ("errors", len(report.errors)),
                ],
            ),
        ]
        store_row_after = browser.db.get_row_from_id("stores", report.store_row_id)
        capabilities_rows: list[tuple[str, object]] = []
        if store_row_after is not None and "store_supports_checksums" in set(store_row_after.allowed_columns):
            supports_checksums = bool(int(store_row_after["store_supports_checksums"] or 0))
            capabilities_rows.append(("store_supports_checksums", "yes" if supports_checksums else "no"))
        transport_rows: list[tuple[str, object]] = []
        if is_rclone:
            transport_rows.append(
                (
                    "max_http_requests_per_hour",
                    get_default_rclone_http_requests_per_hour()
                    if options.max_http_requests_per_hour is None
                    else options.max_http_requests_per_hour,
                )
            )
        elif is_wget:
            transport_rows.append(
                (
                    "max_http_requests_per_hour",
                    get_default_wget_http_requests_per_hour()
                    if options.max_http_requests_per_hour is None
                    else options.max_http_requests_per_hour,
                )
            )
        elif is_native:
            transport_rows.append(
                (
                    "max_http_requests_per_hour",
                    get_default_native_html_requests_per_hour()
                    if options.max_http_requests_per_hour is None
                    else options.max_http_requests_per_hour,
                )
            )
        if capabilities_rows:
            sections.append(("Capabilities", capabilities_rows))
        if transport_rows:
            sections.append(("Transport", transport_rows))
        if is_wget or is_native:
            crawler_rows: list[tuple[str, object]] = [
                ("crawler_urls_observed", getattr(report, "crawler_urls_observed", 0)),
                ("crawler_html_seen", getattr(report, "crawler_html_seen", 0)),
                ("crawler_book_like_found", getattr(report, "crawler_book_like_found", 0)),
                ("crawler_html_rejected", getattr(report, "crawler_html_rejected", 0)),
            ]
            rejection_summary = _crawler_rejection_summary(report)
            if rejection_summary:
                crawler_rows.append(("crawler_rejections", rejection_summary))
            sections.append(("Crawler", crawler_rows))
        browser.emit_detail_sections(sections, title="Sync completed:", max_cell_width=120)
        if report.errors:
            preview_count = min(5, len(report.errors))
            browser.emit("")
            browser.emit("Error preview")
            browser.emit(browser.render_table(["error"], [[error] for error in report.errors[:preview_count]], max_cell_width=120))
            if len(report.errors) > preview_count:
                browser.emit("... {} more".format(len(report.errors) - preview_count))
        return True


__all__ = [
    "SyncStoreCommand",
    "run_sync_store_job",
]
