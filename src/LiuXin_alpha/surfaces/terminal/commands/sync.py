"""
Parse store-reconciliation policy and submit the corresponding local or remote Core job.

Background mode reports submission without waiting. Foreground mode polls job
state, then optionally replays captured logs and renders the report; it does not
stream live progress while waiting. The legacy worker name remains a forwarding
entry point to the Core-owned implementation.
"""

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
    """
    Hold store identity, scanner policy, job controls, and report-output choices.

    Scanner fields cover extension filtering, hashes, symlinks, store links and
    refresh, request rates, rclone flags, and crawler/wget behavior. Timeout/rate
    units are seconds/requests per hour respectively; ``None`` is forwarded for
    omitted limits. Job/output flags select background execution, pane attachment,
    captured output, progress cadence, and JSON rendering.

    Direct construction performs no validation. Frozen attributes do not make
    the optional extension list immutable; parser defaults and command-level
    compatibility checks are separate from this record.

    Example:
        >>> options = _parse_sync_store_options(["12", "--background"], usage="sync store")
        >>> options.store_ref, options.background, options.crawler_incremental_db_writes
        ('12', True, True)
    """

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
    """
    Split extension text into ordered unique lowercase entries with leading dots removed.

    Commas, semicolons, spaces, tabs, and newlines separate entries. Blank pieces
    are filtered before dot removal, so a dot-only piece survives as an empty
    string. No extension syntax or supported-format validation is performed.

    Example:
        >>> _split_extensions(".EPUB; mobi EPUB .")
        ['epub', 'mobi', '']


    :param raw: Optional separated extension text.
    :return: Deduplicated normalized entries, or ``None`` for absent/empty input with no pieces.
    """
    if raw is None:
        return None
    text = str(raw).strip()
    for separator in (";", " ", "\t", "\n"):
        text = text.replace(separator, ",")
    values = [
        part.strip().lstrip(".").lower() for part in text.split(",") if part.strip()
    ]
    return list(dict.fromkeys(values)) or None


def _none_like(value: str) -> bool:
    """
    Recognize textual disabled/unbounded aliases used by numeric-limit options.

    Example:
        >>> _none_like(" INFINITE "), _none_like("0")
        (True, False)


    :param value: Value converted to stripped lowercase text.
    :return: Whether text is none/off/disable/disabled/inf/infinite/unbounded.
    """
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
    """
    Parse a positive float or disabled alias, without an explicit finiteness check.

    Parsed values are rejected only when they compare less than or equal to zero;
    NaN and numeric infinity forms not caught by the alias helper can pass through.

    Example:
        >>> _optional_positive_float("0.5", option="--timeout"), _optional_positive_float("off", option="--timeout")
        (0.5, None)


    :param value: Float text or recognized disabled/unbounded alias.
    :param option: Option spelling included in conversion/range diagnostics.
    :return: Parsed float, or ``None`` for a recognized alias.
    :raises ValueError: If conversion fails or the parsed value compares as nonpositive.
    """
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
    """
    Parse a strictly positive integer or return ``None`` for a disabled/unbounded alias.

    Example:
        >>> _optional_positive_int("3", option="--depth"), _optional_positive_int("none", option="--depth")
        (3, None)


    :param value: Integer text or recognized disabled/unbounded alias.
    :param option: Option spelling used in diagnostics.
    :return: Positive integer or ``None`` for an alias.
    :raises ValueError: If integer conversion fails or the value is less than one.
    """
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
    """
    Parse reconciliation/job options while preserving separate dash-prefixed wget arguments.

    Legacy ``to-db`` spellings are removed wherever they occur as complete tokens,
    including option-value positions. Separate ``--wget-arg`` values are converted
    to equals form before argparse consumes the tokens. Repeated options generally
    use the last value; wget arguments accumulate in order.

    Source must be nonblank and progress cadence positive. Optional depth/timeouts
    use the numeric helpers. HTTP rate accepts any float without positivity or
    finiteness validation; its disabled aliases become zero rather than ``None``.
    Store existence and incompatible job/output combinations are checked later.

    Example:
        >>> options = _parse_sync_store_options(["12", "to-db", "--wget-arg", "--timeout=5"], usage="sync store")
        >>> options.store_ref, options.wget_args
        ('12', ('--timeout=5',))


    :param args: Store reference and supported reconciliation/job/output tokens.
    :param usage: Usage text for missing arguments or wrapped argparse failures.
    :return: Parsed options without submitting work or inspecting the backing store.
    :raises ValueError: If parsing, required-value checks, or configured numeric checks fail.
    """
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
    parser.add_argument(
        "--no-capture-hashes", dest="capture_hashes", action="store_false"
    )
    parser.add_argument(
        "--follow-symlinks", dest="follow_symlinks", action="store_true"
    )
    parser.add_argument(
        "--no-follow-symlinks", dest="follow_symlinks", action="store_false"
    )
    parser.add_argument(
        "--refresh", dest="refresh_storage_manager", action="store_true"
    )
    parser.add_argument(
        "--no-refresh", dest="refresh_storage_manager", action="store_false"
    )
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
    parser.add_argument(
        "--wget-no-verbose", dest="wget_no_verbose", action="store_true"
    )
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
        crawler_incremental_db_writes=bool(values.crawler_incremental_db_writes),
        background=bool(values.background),
        job_backend=(
            None if values.job_backend is None else str(values.job_backend).strip()
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
    """
    Forward legacy surface-worker calls to the Core-owned store synchronization worker.

    No arguments are rewritten or defaulted here. The Core worker owns database
    access, scanner selection, progress output, and report serialization.

    Example:
        >>> report = run_sync_store_job(**worker_options)  # doctest: +SKIP


    :param kwargs: Core worker keyword arguments, including database connection and reconciliation policy.
    :return: Core worker report unchanged; argument and execution failures propagate.
    """

    return _core_run_sync_store_job(**kwargs)


def _safe_int(value: str) -> Optional[int]:
    """
    Parse stripped text as an integer, returning ``None`` for ordinary conversion failures.

    Example:
        >>> _safe_int("12"), _safe_int("archive")
        (12, None)


    :param value: Value stringified and stripped before integer conversion.
    :return: Parsed integer without range validation, or ``None`` when conversion fails.
    """
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _resolve_store_row(browser, store_ref: str):
    """
    Resolve numeric-looking store references by ID, otherwise require one exact store-name match.

    Name queries retain the stringified reference's whitespace. Numeric names
    take the ID path and duplicate name matches must be disambiguated by ID.

    Example:
        >>> row = _resolve_store_row(browser, "12")  # doctest: +SKIP


    :param browser: Host supplying table enumeration and store-row lookup/search.
    :param store_ref: Integer ID text or exact registered store name.
    :return: Existing store row selected by ID or unique name.
    :raises ValueError: If the stores table/reference is absent or the name is ambiguous.
    """
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
    """
    Select wget, native, rclone, or local synchronization from ordered kind/protocol checks.

    Wget recognition wins before native, then rclone; unrecognized combinations
    fall back to local. Both mapping keys are required even if one alone could
    determine the mode. This selects a mode without checking installed backends.

    Example:
        >>> _sync_mode({"store_kind": "wget_html_readonly", "store_access_protocol": "https"})
        'wget'


    :param store_row: Mapping with store kind and access-protocol fields, normalized as text.
    :return: Canonical mode string used in the Core synchronization payload.
    """
    kind = str(store_row["store_kind"] or "").strip().lower()
    protocol = str(store_row["store_access_protocol"] or "").strip().lower()
    if (
        kind in {"wget_html_readonly", "wget_http_ro", "http_spider_ro"}
        or protocol == "wget"
    ):
        return "wget"
    if kind in {
        "native_html_readonly",
        "native_http_ro",
        "http_native_ro",
    } or protocol in {
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
    """
    Poll until a recognized terminal state, then retrieve and require a successful mapping report.

    Nonterminal, missing, or unrecognized states repeat indefinitely with a
    tenth-second sleep; this loop has no independent deadline or cancellation.
    Query errors propagate. Result retrieval requests a zero timeout after the
    terminal state; a failed execution raises using its traceback when available.

    Example:
        >>> from unittest.mock import Mock
        >>> host = Mock()
        >>> host.execute_core_query.side_effect = [
        ...     {"job": {"state": "succeeded"}},
        ...     {"execution": {"ok": True, "result": {"inserted_files": 2}}},
        ... ]
        >>> _wait_for_job(host, "job-1")
        {'inserted_files': 2}


    :param browser: Host providing Core job-state and execution-result queries.
    :param job_id: Identifier forwarded unchanged on every query.
    :return: Shallow dictionary copy of the successful execution's report mapping.
    :raises RuntimeError: If execution is not successful or its result is not a mapping.
    """
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
        raise RuntimeError(str(execution.get("traceback") or "Store sync job failed."))
    report = execution.get("result")
    if not isinstance(report, Mapping):
        raise RuntimeError("Store sync job did not return a report.")
    return dict(report)


def _emit_job_log(browser, job_id: str) -> None:
    """
    Read captured job output in requested one-mebibyte chunks and emit nonblank stripped text.

    Starts at offset zero and follows returned offsets until EOF, defaulting to
    EOF when the field is absent. Each chunk loses trailing whitespace, so this
    is not byte-exact replay. There is no total size bound or stalled-offset
    guard if the provider repeatedly returns non-EOF without advancing.

    Example:
        >>> from unittest.mock import Mock
        >>> host = Mock()
        >>> host.execute_core_query.return_value = {"text": "done  ", "eof": True}
        >>> _emit_job_log(host, "job-1")
        >>> host.emit.assert_called_once_with("done")


    :param browser: Host supplying Core log-read queries and text output.
    :param job_id: Identifier used for every log chunk request.
    :return: ``None`` after the provider reports EOF; query/conversion/output errors propagate.
    """
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
    """
    Reconcile a registered store through Core, either waiting for a report or returning after submission.

    Mode comes from kind/protocol metadata, not a fresh capability probe. The
    default provenance label is adapted for remote modes. Foreground progress
    is captured job-log replay after completion rather than live terminal updates.

    Example:
        >>> SyncStoreCommand().group_aliases
        ('reconcile',)
    """

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
        """
        Validate mode/output choices, resolve the store, and submit its policy to ``sync.store.start``.

        Background JSON is rejected, and a job pane requires background mode.
        Background return only confirms receipt of a job ID, not job success.
        Foreground mode waits without a local deadline, optionally replays logs,
        and prints JSON or a human-readable report with at most five error previews.
        Report-level file errors do not themselves fail a successful job execution.

        Job timeout is forwarded to Core, separate from the waiting loop. Pane,
        waiting, log, or output failures do not cancel or undo a submitted job.
        The adapter forwards scanner policy but does not itself enforce request
        rates, crawl boundaries, or rollback of database changes.

        Example:
            >>> SyncStoreCommand().execute(browser, ["12", "--background", "--job-panel"])  # doctest: +SKIP


        :param browser: Host providing store reads, Core job operations, optional pane support, and output.
        :param args: Store reference, optional legacy to-db token, and synchronization/job/output options.
        :return: ``True`` after submission reporting or successful foreground report rendering.
        :raises ValueError: If options conflict, store resolution fails, or the root URI is blank.
        :raises RuntimeError: If Core omits a job ID or foreground execution/report validation fails.
        """
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
                "Store {} has no `store_root_uri`.".format(store_row["store_id"])
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
                    "store_name": str(store_row["store_name"] or "").strip() or None,
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
                "  Use `jobs show {} --wait` to inspect completion.".format(job_id)
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

        detail_sections = [
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
        ]
        if mode in {"wget", "native"}:
            detail_sections.append(
                (
                    "Crawler",
                    [
                        (
                            "crawler_urls_observed",
                            report.get("crawler_urls_observed", 0),
                        ),
                        ("crawler_html_seen", report.get("crawler_html_seen", 0)),
                        (
                            "crawler_book_like_found",
                            report.get("crawler_book_like_found", 0),
                        ),
                        (
                            "crawler_html_rejected",
                            report.get("crawler_html_rejected", 0),
                        ),
                        (
                            "crawler_rejections",
                            json.dumps(
                                report.get("crawler_rejection_counts", {}) or {},
                                ensure_ascii=False,
                                sort_keys=True,
                            ),
                        ),
                    ],
                )
            )
        browser.emit_detail_sections(
            detail_sections,
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
