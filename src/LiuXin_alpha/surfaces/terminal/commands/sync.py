"""Core-backed ``sync store`` terminal command."""

from __future__ import annotations

import argparse
import dataclasses
import json
import time

from collections.abc import Mapping
from typing import Any, Optional

from LiuXin_alpha.core.workflow_jobs import (
    run_sync_store_job as _core_run_sync_store_job,
)
from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


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
    crawler_recurse: bool
    crawler_max_depth: Optional[int]
    crawler_timeout_s: Optional[float]
    crawler_no_parent: bool
    crawler_span_hosts: bool
    crawler_respect_robots: bool
    crawler_user_agent: Optional[str]
    wget_no_verbose: bool
    wget_args: tuple[str, ...]
    crawler_incremental_db_writes: bool
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
    for separator in (";", " ", "\t", "\n"):
        text = text.replace(separator, ",")
    values = [
        part.strip().lstrip(".").lower()
        for part in text.split(",")
        if part.strip()
    ]
    return list(dict.fromkeys(values)) or None


def _none_like(value: str) -> bool:
    return str(value).strip().lower() in {
        "none",
        "off",
        "disable",
        "disabled",
        "inf",
        "infinite",
        "unbounded",
    }


def _optional_positive_float(value: str, *, option: str) -> float | None:
    if _none_like(value):
        return None
    try:
        parsed = float(value)
    except Exception as exc:
        raise ValueError(
            "{} requires a numeric value or 'none'.".format(option)
        ) from exc
    if parsed <= 0:
        raise ValueError("{} must be > 0, or use 'none'.".format(option))
    return parsed


def _optional_positive_int(value: str, *, option: str) -> int | None:
    if _none_like(value):
        return None
    try:
        parsed = int(value)
    except Exception as exc:
        raise ValueError(
            "{} requires an integer value or 'none'.".format(option)
        ) from exc
    if parsed <= 0:
        raise ValueError("{} must be >= 1.".format(option))
    return parsed


def _parse_sync_store_options(
    args: list[str],
    *,
    usage: str,
) -> _SyncStoreOptions:
    if not args:
        raise ValueError("Usage: {}".format(usage))

    raw_tokens = [
        token
        for token in args
        if str(token).strip().lower() not in {"to-db", "to_db", "todb"}
    ]
    # argparse otherwise treats a separate wget argument such as
    # ``--wget-arg --timeout=5`` as one of our own options. Preserve the
    # terminal command's established pass-through form.
    tokens: list[str] = []
    index = 0
    while index < len(raw_tokens):
        token = str(raw_tokens[index])
        if token == "--wget-arg":
            if index + 1 >= len(raw_tokens):
                raise ValueError("--wget-arg requires a value.")
            value = str(raw_tokens[index + 1]).strip()
            if not value:
                raise ValueError("--wget-arg requires a non-blank value.")
            tokens.append("--wget-arg={}".format(value))
            index += 2
            continue
        tokens.append(token)
        index += 1
    parser = argparse.ArgumentParser(add_help=False, exit_on_error=False)
    parser.add_argument("store_ref")
    parser.add_argument("--source", default="on_disk_unmanaged_import")
    parser.add_argument("--extensions")
    parser.add_argument("--max-http-requests-per-hour")
    parser.add_argument("--progress-every", type=int, default=100)
    parser.add_argument("--rclone-http-no-slash", action="store_true")
    parser.add_argument("--rclone-http-no-head", action="store_true")

    parser.set_defaults(
        compute_hash=True,
        capture_hashes=False,
        follow_symlinks=False,
        refresh_storage_manager=True,
        attach_store_links=True,
        crawler_recurse=True,
        crawler_no_parent=True,
        crawler_span_hosts=False,
        crawler_respect_robots=True,
        wget_no_verbose=False,
        crawler_incremental_db_writes=True,
        background=False,
        job_no_output=False,
        job_panel=False,
        show_progress=True,
    )
    parser.add_argument("--hash", dest="compute_hash", action="store_true")
    parser.add_argument("--no-hash", dest="compute_hash", action="store_false")
    parser.add_argument("--capture-hashes", dest="capture_hashes", action="store_true")
    parser.add_argument("--no-capture-hashes", dest="capture_hashes", action="store_false")
    parser.add_argument("--follow-symlinks", dest="follow_symlinks", action="store_true")
    parser.add_argument("--no-follow-symlinks", dest="follow_symlinks", action="store_false")
    parser.add_argument("--refresh", dest="refresh_storage_manager", action="store_true")
    parser.add_argument("--no-refresh", dest="refresh_storage_manager", action="store_false")
    parser.add_argument("--links", dest="attach_store_links", action="store_true")
    parser.add_argument("--no-links", dest="attach_store_links", action="store_false")
    parser.add_argument(
        "--crawler-recurse",
        "--wget-recurse",
        dest="crawler_recurse",
        action="store_true",
    )
    parser.add_argument(
        "--crawler-no-recurse",
        "--wget-no-recurse",
        dest="crawler_recurse",
        action="store_false",
    )
    parser.add_argument("--crawler-max-depth", "--wget-max-depth")
    parser.add_argument("--crawler-timeout-s", "--wget-timeout-s")
    parser.add_argument(
        "--crawler-no-parent",
        "--wget-no-parent",
        dest="crawler_no_parent",
        action="store_true",
    )
    parser.add_argument(
        "--crawler-parent",
        "--wget-parent",
        dest="crawler_no_parent",
        action="store_false",
    )
    parser.add_argument(
        "--crawler-span-hosts",
        "--wget-span-hosts",
        dest="crawler_span_hosts",
        action="store_true",
    )
    parser.add_argument(
        "--crawler-no-span-hosts",
        "--wget-no-span-hosts",
        dest="crawler_span_hosts",
        action="store_false",
    )
    parser.add_argument(
        "--crawler-ignore-robots",
        "--wget-ignore-robots",
        dest="crawler_respect_robots",
        action="store_false",
    )
    parser.add_argument(
        "--crawler-respect-robots",
        "--wget-respect-robots",
        dest="crawler_respect_robots",
        action="store_true",
    )
    parser.add_argument("--crawler-user-agent", "--wget-user-agent")
    parser.add_argument("--wget-verbose", dest="wget_no_verbose", action="store_false")
    parser.add_argument("--wget-no-verbose", dest="wget_no_verbose", action="store_true")
    parser.add_argument("--wget-arg", action="append", default=[])
    parser.add_argument(
        "--crawler-incremental-db-writes",
        "--wget-incremental-db-writes",
        dest="crawler_incremental_db_writes",
        action="store_true",
    )
    parser.add_argument(
        "--crawler-no-incremental-db-writes",
        "--wget-no-incremental-db-writes",
        dest="crawler_incremental_db_writes",
        action="store_false",
    )
    parser.add_argument("--background", dest="background", action="store_true")
    parser.add_argument("--foreground", dest="background", action="store_false")
    parser.add_argument("--job-backend")
    parser.add_argument("--job-timeout-s")
    parser.add_argument("--job-no-output", dest="job_no_output", action="store_true")
    parser.add_argument("--job-output", dest="job_no_output", action="store_false")
    parser.add_argument("--job-panel", dest="job_panel", action="store_true")
    parser.add_argument("--no-job-panel", dest="job_panel", action="store_false")
    parser.add_argument("--progress", dest="show_progress", action="store_true")
    parser.add_argument("--no-progress", dest="show_progress", action="store_false")
    parser.add_argument("--json", dest="json_output", action="store_true")

    try:
        values = parser.parse_args(tokens)
    except (argparse.ArgumentError, SystemExit) as exc:
        raise ValueError("Usage: {}".format(usage)) from exc

    if not str(values.source).strip():
        raise ValueError("--source requires a non-blank value.")
    if values.progress_every <= 0:
        raise ValueError("--progress-every must be >= 1.")

    max_http: float | None = None
    if values.max_http_requests_per_hour is not None:
        if _none_like(values.max_http_requests_per_hour):
            max_http = 0.0
        else:
            try:
                max_http = float(values.max_http_requests_per_hour)
            except Exception as exc:
                raise ValueError(
                    "--max-http-requests-per-hour requires a numeric value or 'none'."
                ) from exc

    return _SyncStoreOptions(
        store_ref=str(values.store_ref),
        source_label=str(values.source).strip(),
        ebook_extensions=_split_extensions(values.extensions),
        compute_hash=bool(values.compute_hash),
        capture_hashes=bool(values.capture_hashes),
        follow_symlinks=bool(values.follow_symlinks),
        refresh_storage_manager=bool(values.refresh_storage_manager),
        attach_store_links=bool(values.attach_store_links),
        max_http_requests_per_hour=max_http,
        rclone_http_no_slash=bool(values.rclone_http_no_slash),
        rclone_http_no_head=bool(values.rclone_http_no_head),
        crawler_recurse=bool(values.crawler_recurse),
        crawler_max_depth=(
            None
            if values.crawler_max_depth is None
            else _optional_positive_int(
                values.crawler_max_depth,
                option="--crawler-max-depth",
            )
        ),
        crawler_timeout_s=(
            None
            if values.crawler_timeout_s is None
            else _optional_positive_float(
                values.crawler_timeout_s,
                option="--crawler-timeout-s",
            )
        ),
        crawler_no_parent=bool(values.crawler_no_parent),
        crawler_span_hosts=bool(values.crawler_span_hosts),
        crawler_respect_robots=bool(values.crawler_respect_robots),
        crawler_user_agent=(
            None
            if values.crawler_user_agent is None
            else str(values.crawler_user_agent).strip()
        ),
        wget_no_verbose=bool(values.wget_no_verbose),
        wget_args=tuple(str(value) for value in values.wget_arg),
        crawler_incremental_db_writes=bool(
            values.crawler_incremental_db_writes
        ),
        background=bool(values.background),
        job_backend=(
            None
            if values.job_backend is None
            else str(values.job_backend).strip()
        ),
        job_timeout_s=(
            None
            if values.job_timeout_s is None
            else _optional_positive_float(
                values.job_timeout_s,
                option="--job-timeout-s",
            )
        ),
        job_no_output=bool(values.job_no_output),
        job_panel=bool(values.job_panel),
        show_progress=bool(values.show_progress),
        progress_every=int(values.progress_every),
        json_output=bool(values.json_output),
    )


def run_sync_store_job(**kwargs: Any) -> dict[str, object]:
    """Compatibility import for callers that used the former surface worker."""

    return _core_run_sync_store_job(**kwargs)


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
        raise ValueError(
            "Multiple stores found for name {!r}; use store id instead.".format(
                store_ref
            )
        )
    return rows[0]


def _sync_mode(store_row: Mapping[str, Any]) -> str:
    kind = str(store_row["store_kind"] or "").strip().lower()
    protocol = str(store_row["store_access_protocol"] or "").strip().lower()
    if kind in {"wget_html_readonly", "wget_http_ro", "http_spider_ro"} or protocol == "wget":
        return "wget"
    if kind in {"native_html_readonly", "native_http_ro", "http_native_ro"} or protocol in {
        "native",
        "native_html",
    }:
        return "native"
    if kind in {"rclone_http_readonly", "rclone_http_ro", "http_ro"} or protocol in {
        "http",
        "https",
        "rclone",
    }:
        return "rclone"
    return "local"


def _wait_for_job(browser, job_id: str) -> dict[str, Any]:
    while True:
        response = browser.execute_core_query(
            "jobs.get",
            payload={"job_id": job_id},
        )
        job = dict((response or {}).get("job", {}) or {})
        if str(job.get("state") or "") in {
            "succeeded",
            "failed",
            "cancelled",
            "timed_out",
        }:
            break
        time.sleep(0.1)
    completed = browser.execute_core_query(
        "jobs.result",
        payload={"job_id": job_id, "timeout_s": 0.0},
    )
    execution = dict((completed or {}).get("execution", {}) or {})
    if not bool(execution.get("ok", False)):
        raise RuntimeError(
            str(execution.get("traceback") or "Store sync job failed.")
        )
    report = execution.get("result")
    if not isinstance(report, Mapping):
        raise RuntimeError("Store sync job did not return a report.")
    return dict(report)


def _emit_job_log(browser, job_id: str) -> None:
    offset = 0
    while True:
        response = browser.execute_core_query(
            "jobs.log.read",
            payload={
                "job_id": job_id,
                "offset": offset,
                "max_bytes": 1024 * 1024,
            },
        )
        text = str((response or {}).get("text") or "").rstrip()
        if text:
            browser.emit(text)
        offset = int((response or {}).get("next_offset") or offset)
        if bool((response or {}).get("eof", True)):
            return


class SyncStoreCommand(TerminalCommandAPI):
    """Reconcile one existing store through the named Core job API."""

    group = "sync"
    group_aliases = ("reconcile",)
    name = "store"
    aliases = ("stores",)
    summary = "Sync one store: sync store <store_id|store_name> [to-db] [options]"
    usage = (
        "sync store <store_id|store_name> [to-db] [--extensions epub,mobi] "
        "[--source <label>] [--background] [--json]"
    )
    expose_direct = False

    def execute(self, browser, args: list[str]) -> bool:
        options = _parse_sync_store_options(args, usage=self.usage)
        if options.background and options.json_output:
            raise ValueError(
                "--json is not supported with --background. "
                "Use `jobs show <id>` for details."
            )
        if options.job_panel and not options.background:
            raise ValueError("--job-panel requires --background.")

        store_row = _resolve_store_row(browser, options.store_ref)
        store_root_uri = str(store_row["store_root_uri"] or "").strip()
        if not store_root_uri:
            raise ValueError(
                "Store {} has no `store_root_uri`.".format(
                    store_row["store_id"]
                )
            )
        mode = _sync_mode(store_row)
        source_label = options.source_label
        if source_label == "on_disk_unmanaged_import" and mode != "local":
            source_label = {
                "rclone": "rclone_http_import",
                "wget": "wget_html_import",
                "native": "native_html_import",
            }[mode]

        rclone_args: list[str] = []
        if options.rclone_http_no_slash:
            rclone_args.append("--http-no-slash")
        if options.rclone_http_no_head:
            rclone_args.append("--http-no-head")

        result = browser.execute_core_command(
            "sync.store.start",
            payload={
                "sync_kwargs": {
                    "mode": mode,
                    "store_root_uri": store_root_uri,
                    "store_name": str(store_row["store_name"] or "").strip()
                    or None,
                    "store_kind": str(store_row["store_kind"] or "").strip()
                    or "on_disk_existing_unmanaged_drive",
                    "source_label": source_label,
                    "ebook_extensions": options.ebook_extensions,
                    "compute_hash": options.compute_hash,
                    "capture_hashes": options.capture_hashes,
                    "follow_symlinks": options.follow_symlinks,
                    "attach_store_links": options.attach_store_links,
                    "refresh_storage_manager": options.refresh_storage_manager,
                    "max_http_requests_per_hour": options.max_http_requests_per_hour,
                    "rclone_args": tuple(rclone_args),
                    "crawler_recurse": options.crawler_recurse,
                    "crawler_max_depth": options.crawler_max_depth,
                    "crawler_timeout_s": options.crawler_timeout_s,
                    "crawler_no_parent": options.crawler_no_parent,
                    "crawler_span_hosts": options.crawler_span_hosts,
                    "crawler_respect_robots": options.crawler_respect_robots,
                    "crawler_user_agent": options.crawler_user_agent,
                    "wget_no_verbose": options.wget_no_verbose,
                    "wget_args": options.wget_args,
                    "crawler_incremental_db_writes": options.crawler_incremental_db_writes,
                    "progress_output": options.show_progress
                    and not options.job_no_output
                    and not options.json_output,
                    "progress_every": options.progress_every,
                },
                "job_backend": options.job_backend,
                "job_timeout_s": options.job_timeout_s,
                "job_no_output": options.job_no_output,
                "label": "sync:{}:{}".format(mode, store_row["store_id"]),
            },
        )
        job_id = str((result or {}).get("job_id") or "")
        if not job_id:
            raise RuntimeError("Core sync command did not return a job id.")

        if options.background:
            browser.emit_detail_sections(
                [
                    (
                        "Submission",
                        [
                            ("mode", mode),
                            ("store_id", store_row["store_id"]),
                            ("backend", options.job_backend or "default"),
                            (
                                "timeout_s",
                                "none"
                                if options.job_timeout_s is None
                                else options.job_timeout_s,
                            ),
                        ],
                    )
                ],
                title="Sync job submitted: {}".format(job_id),
                max_cell_width=120,
            )
            browser.emit(
                "  Use `jobs show {} --wait` to inspect completion.".format(
                    job_id
                )
            )
            if options.job_panel:
                if browser.supports_job_output_panel():
                    browser.attach_job_output_panel(job_id)
                    browser.emit("output_panel: attached to job {}".format(job_id))
                else:
                    browser.emit("output_panel: unavailable in this UI mode")
            return True

        report = _wait_for_job(browser, job_id)
        if options.show_progress and not options.json_output:
            _emit_job_log(browser, job_id)
        if options.json_output:
            browser.emit(
                json.dumps(
                    report,
                    ensure_ascii=False,
                    sort_keys=True,
                    indent=2,
                )
            )
            return True

        browser.emit_detail_sections(
            [
                (
                    "Store",
                    [
                        ("store_id", report.get("store_row_id", "")),
                        ("store_name", report.get("store_name", "")),
                        ("store_root_uri", report.get("store_root_uri", "")),
                    ],
                ),
                (
                    "Results",
                    [
                        ("scanned_files", report.get("scanned_files", 0)),
                        ("ebook_candidates", report.get("ebook_candidates", 0)),
                        (
                            "skipped_non_ebook_files",
                            report.get("skipped_non_ebook_files", 0),
                        ),
                        ("inserted_files", report.get("inserted_files", 0)),
                        ("updated_files", report.get("updated_files", 0)),
                        ("unchanged_files", report.get("unchanged_files", 0)),
                        ("linked_files", report.get("linked_files", 0)),
                        ("errors", len(report.get("errors", ()) or ())),
                    ],
                ),
            ],
            title="Sync completed:",
            max_cell_width=120,
        )
        errors = list(report.get("errors", ()) or ())
        if errors:
            browser.emit("")
            browser.emit("Error preview")
            browser.emit(
                browser.render_table(
                    ["error"],
                    [[error] for error in errors[:5]],
                    max_cell_width=120,
                )
            )
        return True


__all__ = ["SyncStoreCommand", "run_sync_store_job"]
