"""
Implement grouped job listing, inspection, log display, cancellation, and pane attachment.

Core-capable hosts use transport-neutral job operations; legacy hosts retain
job-manager fallbacks. Advertised Core capability does not trigger fallback after
an operation fails. Log tails limit displayed lines, not the underlying file read.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.surfaces.terminal.job_view import (
    fetch_terminal_job_view,
    read_terminal_job_log_view,
    resolve_terminal_job_log_path,
    terminal_job_view_from_object,
)


def _safe_int(value: str) -> Optional[int]:
    """
    Parse stripped text as an integer, returning ``None`` for conversion failures.

    Example:
        >>> _safe_int(" 12 "), _safe_int("1.5")
        (12, None)


    :param value: Value converted to text and stripped before integer parsing.
    :return: Parsed integer, or ``None`` if conversion raises an ``Exception``.
    """
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _safe_float(value: str) -> Optional[float]:
    """
    Parse stripped text as a float without validating range or finiteness.

    Example:
        >>> _safe_float(" 0.5 "), _safe_float("soon")
        (0.5, None)


    :param value: Value converted to text and stripped before floating-point parsing.
    :return: Parsed float, or ``None`` if conversion raises an ``Exception``.
    """
    try:
        return float(str(value).strip())
    except Exception:
        return None


def _read_option_value(
    args: list[str], idx: int, *, option_name: str
) -> tuple[str, int]:
    """
    Read a nonblank equals-form or following-token option value and advance the cursor.

    The option's spelling is not checked here; even a following option-like token
    is accepted as a value. Returned text retains its original surrounding spaces.

    Example:
        >>> _read_option_value(["--state= running "], 0, option_name="--state")
        (' running ', 1)


    :param args: Token list containing the option and any separately supplied value.
    :param idx: Valid index of the option token; invalid indices are not handled here.
    :param option_name: Human-readable option spelling used only in error messages.
    :return: Value text and the index of the first unconsumed token.
    :raises ValueError: If the value is absent or consists only of whitespace.
    """
    token = args[idx]
    if "=" in token:
        _, value = token.split("=", 1)
        if value.strip() == "":
            raise ValueError(
                "Option {} requires a non-blank value.".format(option_name)
            )
        return value, idx + 1
    if idx + 1 >= len(args):
        raise ValueError("Option {} requires a value.".format(option_name))
    value = args[idx + 1]
    if str(value).strip() == "":
        raise ValueError("Option {} requires a non-blank value.".format(option_name))
    return value, idx + 2


def _parse_states(raw: str) -> set[str]:
    """
    Split comma/semicolon-separated state names into a lowercase deduplicated set.

    Blank pieces are discarded. Names are not checked against the job system's
    supported states, and spaces alone do not separate multiple names.

    Example:
        >>> sorted(_parse_states(" RUNNING;failed,running,, "))
        ['failed', 'running']


    :param raw: Text containing state names separated by commas or semicolons.
    :return: Normalized nonblank state tokens, or an empty set when none remain.
    """
    text = str(raw).strip().lower()
    if not text:
        return set()
    values: set[str] = set()
    for part in text.replace(";", ",").split(","):
        token = part.strip().lower()
        if token:
            values.add(token)
    return values


def _format_ts(value: float | None) -> str:
    """
    Render an epoch timestamp in local time to second precision, without an offset suffix.

    Missing, out-of-range, or otherwise unconvertible values display as blank.

    Example:
        >>> _format_ts(None), _format_ts(float("nan"))
        ('', '')


    :param value: Epoch seconds accepted by ``float`` and local ``datetime.fromtimestamp``.
    :return: Local ISO timestamp text, or an empty string for absence or conversion failure.
    """
    if value is None:
        return ""
    try:
        return datetime.fromtimestamp(float(value)).isoformat(timespec="seconds")
    except Exception:
        return ""


def _format_duration(value: float | None) -> str:
    """
    Format a duration with two decimal places, leaving absence or conversion errors blank.

    Negative and non-finite floats are not rejected; they use normal float formatting.

    Example:
        >>> _format_duration(1.234), _format_duration(None)
        ('1.23', '')


    :param value: Duration in seconds, converted to float before formatting.
    :return: Formatted seconds without a unit suffix, or empty text for absence or failure.
    """
    if value is None:
        return ""
    try:
        return "{:.2f}".format(float(value))
    except Exception:
        return ""


def _browser_jobs_list(
    browser, *, states: Optional[set[str]], limit: int, offset: int
) -> tuple[list[dict[str, object]], int]:
    """
    Retrieve a job window through Core queries or the legacy in-process manager.

    Core receives integer paging values and sorted states when supplied. Missing
    or unconvertible totals fall back to the returned job count. The legacy branch
    lists all matching jobs before slicing. Both normalize entries through the
    shared terminal view; capability/query/normalization failures propagate.

    Example:
        >>> from unittest.mock import Mock
        >>> host = Mock()
        >>> host.supports_core_queries.return_value = True
        >>> host.execute_core_query.return_value = {"jobs": [], "total": 7}
        >>> _browser_jobs_list(host, states=None, limit=5, offset=10)
        ([], 7)


    :param browser: Core-query-capable host or legacy host exposing ``job_manager.list``.
    :param states: Optional state filter; ``None`` omits the Core payload field.
    :param limit: Requested page length, forwarded without clamping here.
    :param offset: Requested starting position, forwarded without clamping here.
    :return: Normalized job dictionaries for the page and the reported matching total.
    """
    if hasattr(browser, "supports_core_queries") and bool(
        browser.supports_core_queries()
    ):
        payload: dict[str, object] = {
            "limit": int(limit),
            "offset": int(offset),
        }
        if states is not None:
            payload["states"] = sorted(states)
        result = browser.execute_core_query("jobs.list", payload=payload)
        jobs_raw = list((result or {}).get("jobs", ()) or ())
        total_raw = (result or {}).get("total", len(jobs_raw))
        try:
            total = int(total_raw)
        except Exception:
            total = len(jobs_raw)
        return [terminal_job_view_from_object(one).__dict__ for one in jobs_raw], total

    all_jobs = browser.job_manager.list(states=states)
    total = len(all_jobs)
    window = all_jobs[offset : offset + limit]
    return [terminal_job_view_from_object(one).__dict__ for one in window], total


def _browser_job_cancel(browser, *, job_id: str) -> dict[str, object]:
    """
    Request cancellation through Core commands or the legacy job manager.

    The legacy branch reads the state afterward and substitutes ``unknown`` if
    that read fails. Core results are copied without filling or validating fields.
    Cancellation-request failures propagate, and success does not await termination.

    Example:
        >>> from unittest.mock import Mock
        >>> host = Mock()
        >>> host.supports_core_commands.return_value = True
        >>> host.execute_core_command.return_value = {"cancelled": False}
        >>> _browser_job_cancel(host, job_id="job-1")
        {'cancelled': False}


    :param browser: Core-command-capable host or legacy host with cancellation and state access.
    :param job_id: Job identifier forwarded to the selected cancellation operation.
    :return: Cancellation response mapping, with normalized job/state fields only in the legacy branch.
    """
    if hasattr(browser, "supports_core_commands") and bool(
        browser.supports_core_commands()
    ):
        result = browser.execute_core_command(
            "jobs.cancel", payload={"job_id": str(job_id)}
        )
        return dict(result or {})

    cancelled = bool(browser.job_manager.cancel(job_id))
    state = "unknown"
    try:
        state = str(browser.job_manager.get(job_id).state)
    except Exception:
        state = "unknown"
    return {"job_id": str(job_id), "cancelled": cancelled, "state": state}


class JobsListCommand(TerminalCommandAPI):
    """
    Expose paged job state and timing listings through ``jobs list`` or ``jobs ls``.

    Declares the ``job`` group alias and keeps ``list`` out of direct command
    registration. Optional state names filter jobs before the display window.

    Example:
        >>> JobsListCommand().usage
        'jobs list [limit] [offset] [--state running,failed,...]'
    """

    group = "jobs"
    group_aliases = ("job",)
    expose_direct = False
    name = "list"
    aliases = ("ls",)
    summary = "List submitted jobs."
    usage = "jobs list [limit] [offset] [--state running,failed,...]"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Parse job filters and paging, then print the matching window and total summary.

        Defaults are the host page size, clamped to one, and offset zero. Repeated
        state options replace the previous filter. Tokens starting with a dash
        are parsed as options, so negative positional numbers are rejected before
        numeric clamping. State names themselves are not validated here.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock(page_size=20)
            >>> host.supports_core_queries.return_value = True
            >>> host.execute_core_query.return_value = {"jobs": [], "total": 0}
            >>> JobsListCommand().execute(host, ["5", "--state=running"])
            True
            >>> host.render_table.assert_not_called()


        :param browser: Host supplying page-size defaults, job access, and table/output methods.
        :param args: Up to two positional paging values plus optional ``--state`` values.
        :return: ``True`` after list rendering, including an empty matching window.
        :raises ValueError: If options, paging syntax, or argument counts are invalid.
        """
        limit = max(1, int(browser.page_size))
        offset = 0
        states: Optional[set[str]] = None
        positional: list[str] = []

        idx = 0
        while idx < len(args):
            token = str(args[idx]).strip()
            if token == "--state" or token.startswith("--state="):
                value, idx = _read_option_value(args, idx, option_name="--state")
                parsed = _parse_states(value)
                if not parsed:
                    raise ValueError("Option --state requires at least one value.")
                states = parsed
                continue
            if token.startswith("-"):
                raise ValueError("Unknown option: {!r}".format(token))
            positional.append(token)
            idx += 1

        if len(positional) >= 1:
            maybe_limit = _safe_int(positional[0])
            if maybe_limit is None:
                raise ValueError("limit must be an integer")
            limit = max(1, maybe_limit)
        if len(positional) >= 2:
            maybe_offset = _safe_int(positional[1])
            if maybe_offset is None:
                raise ValueError("offset must be an integer")
            offset = max(0, maybe_offset)
        if len(positional) > 2:
            raise ValueError("Usage: {}".format(self.usage))

        window, total = _browser_jobs_list(
            browser, states=states, limit=limit, offset=offset
        )

        if not window:
            browser.emit("No jobs matched.")
            browser.emit(
                "Summary: total={} shown=0 offset={} limit={}".format(
                    total, offset, limit
                )
            )
            return True

        headers = [
            "job_id",
            "label",
            "state",
            "backend",
            "submitted",
            "started",
            "finished",
            "dur_s",
        ]
        rows = []
        for one in window:
            rows.append(
                [
                    str(one.get("job_id", "") or ""),
                    str(one.get("label", "") or ""),
                    str(one.get("state", "") or ""),
                    str(one.get("backend_name", "") or ""),
                    _format_ts(one.get("submitted_at", None)),
                    _format_ts(one.get("started_at", None)),
                    _format_ts(one.get("finished_at", None)),
                    _format_duration(one.get("duration_s", None)),
                ]
            )
        browser.emit(browser.render_table(headers, rows, max_cell_width=44))
        browser.emit(
            "Summary: total={} shown={} offset={} limit={}".format(
                total,
                len(window),
                offset,
                limit,
            )
        )
        return True


class JobsShowCommand(TerminalCommandAPI):
    """
    Expose job status, timing, output location, and available execution details.

    ``jobs show`` can wait for an execution result and optionally display all
    traceback lines instead of the first-line preview.

    Example:
        >>> JobsShowCommand().usage
        'jobs show <job_id> [--wait[=<sec|none>]] [--traceback]'
    """

    group = "jobs"
    expose_direct = False
    name = "show"
    aliases = ()
    summary = "Show one job."
    usage = "jobs show <job_id> [--wait[=<sec|none>]] [--traceback]"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Parse one job ID and optional waiting/traceback choices, then render a shared job view.

        Bare ``--wait`` uses thirty seconds unless followed by a non-option token,
        which is consumed as its value. ``none``, ``off``, and disable aliases set
        an unbounded timeout while still enabling waiting. Numeric timeouts are
        forwarded without range/finiteness checks; repeated options use the last
        value. Put the job ID before a bare wait option to avoid it becoming a value.

        Missing execution data is displayed as not finished. An unsuccessful job
        result is rendered, not raised as a command failure. Fetching, waiting,
        and output errors still propagate.

        Example:
            >>> JobsShowCommand().execute(browser, ["job-1", "--wait=5", "--traceback"])  # doctest: +SKIP


        :param browser: Host supplying shared job retrieval and detail/table output methods.
        :param args: One job ID, optional wait value, and optional ``--traceback`` flag.
        :return: ``True`` after rendering the available job information.
        :raises ValueError: If required arguments, options, or wait-value syntax are invalid.
        """
        if not args:
            raise ValueError("Usage: {}".format(self.usage))

        job_id: Optional[str] = None
        wait_timeout: Optional[float] = None
        do_wait = False
        show_traceback = False

        idx = 0
        while idx < len(args):
            token = str(args[idx]).strip()
            if token == "--wait":
                do_wait = True
                # Optional value form: --wait 10
                if idx + 1 < len(args) and not str(args[idx + 1]).strip().startswith(
                    "-"
                ):
                    raw = str(args[idx + 1]).strip().lower()
                    if raw in {"none", "off", "disable", "disabled"}:
                        wait_timeout = None
                    else:
                        parsed = _safe_float(raw)
                        if parsed is None:
                            raise ValueError(
                                "Option --wait expects a numeric value or 'none'."
                            )
                        wait_timeout = parsed
                    idx += 2
                    continue
                wait_timeout = 30.0
                idx += 1
                continue
            if token.startswith("--wait="):
                do_wait = True
                raw = token.split("=", 1)[1].strip().lower()
                if raw in {"none", "off", "disable", "disabled"}:
                    wait_timeout = None
                else:
                    parsed = _safe_float(raw)
                    if parsed is None:
                        raise ValueError(
                            "Option --wait expects a numeric value or 'none'."
                        )
                    wait_timeout = parsed
                idx += 1
                continue
            if token == "--traceback":
                show_traceback = True
                idx += 1
                continue
            if token.startswith("-"):
                raise ValueError("Unknown option: {!r}".format(token))
            if job_id is None:
                job_id = token
                idx += 1
                continue
            raise ValueError(
                "Unexpected extra argument {!r}. Usage: {}".format(token, self.usage)
            )

        if not job_id:
            raise ValueError("Usage: {}".format(self.usage))

        info = fetch_terminal_job_view(
            browser,
            job_id=job_id,
            do_wait=do_wait,
            wait_timeout=wait_timeout,
        )

        browser.emit_detail_sections(
            [
                (
                    "Overview",
                    [
                        ("label", str(info.label or "")),
                        ("state", str(info.state or "")),
                        ("backend", str(info.backend_name or "")),
                    ],
                ),
                (
                    "Timing",
                    [
                        ("submitted_at", _format_ts(info.submitted_at)),
                        ("started_at", _format_ts(info.started_at)),
                        ("finished_at", _format_ts(info.finished_at)),
                        ("duration_s", _format_duration(info.duration_s)),
                        ("timeout_s", info.timeout_s),
                    ],
                ),
                (
                    "Output",
                    [
                        ("no_output", "yes" if bool(info.no_output) else "no"),
                        ("log_path", resolve_terminal_job_log_path(info) or ""),
                    ],
                ),
            ],
            title="Job {}".format(str(info.job_id or "")),
            max_cell_width=120,
        )

        execution = info.execution
        if execution is None:
            browser.emit("")
            browser.emit(
                browser.render_detail_sections(
                    [("Execution", [("status", "<not finished>")])], max_cell_width=120
                )
            )
            return True

        browser.emit("")
        browser.emit(
            browser.render_detail_sections(
                [
                    (
                        "Execution",
                        [
                            (
                                "execution_ok",
                                "yes" if bool(execution.get("ok", False)) else "no",
                            ),
                            (
                                "timed_out",
                                "yes"
                                if bool(execution.get("timed_out", False))
                                else "no",
                            ),
                            (
                                "aborted",
                                "yes"
                                if bool(execution.get("aborted", False))
                                else "no",
                            ),
                            (
                                "result_preview",
                                str(execution.get("result_preview", "")),
                            ),
                        ],
                    )
                ],
                max_cell_width=120,
            )
        )

        tb = str(execution.get("traceback", "") or "").strip()
        if not tb:
            return True
        if show_traceback:
            browser.emit("")
            browser.emit("Traceback")
            browser.emit(
                browser.render_table(
                    ["line"], [[line] for line in tb.splitlines()], max_cell_width=120
                )
            )
        else:
            first_line = tb.splitlines()[0]
            browser.emit("")
            browser.emit(
                browser.render_detail_sections(
                    [
                        (
                            "Traceback",
                            [
                                ("traceback_preview", first_line),
                                ("hint", "use --traceback for full traceback"),
                            ],
                        )
                    ],
                    max_cell_width=120,
                )
            )
        return True


class JobsTailCommand(TerminalCommandAPI):
    """
    Expose a single recent-log snapshot through ``jobs tail`` or ``jobs log``.

    The reader loads the whole advertised log before selecting lines. This is not
    a streaming follow operation and the display limit does not bound file I/O.

    Example:
        >>> JobsTailCommand().usage
        'jobs tail <job_id> [lines] [--wait[=<sec|none>]]'
    """

    group = "jobs"
    expose_direct = False
    name = "tail"
    aliases = ("log",)
    summary = "Show recent log output for one job."
    usage = "jobs tail <job_id> [lines] [--wait[=<sec|none>]]"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Optionally wait for a job, read its complete log view, and display the requested final lines.

        Defaults to twenty lines; extra numeric positional tokens repeatedly replace
        the line limit, clamped to one. Dash-prefixed positionals are rejected as
        options. Wait parsing matches ``jobs show``: a bare wait defaults to thirty
        seconds, and the ``none``/off aliases request waiting without a timeout.

        Absent, missing, empty, or unreadable logs produce the shared reader's status
        message. Paths are not confined here and control characters are not generally
        sanitized. The optional wait concerns job completion, not following new log lines.

        Example:
            >>> JobsTailCommand().execute(browser, ["job-1", "10", "--wait=5"])  # doctest: +SKIP


        :param browser: Host supplying job access and detail/table output methods.
        :param args: Job ID followed by optional line limits and wait options.
        :return: ``True`` after rendering log status or the selected tail and count summary.
        :raises ValueError: If a job ID is missing or line/wait/option syntax is invalid.
        """
        if not args:
            raise ValueError("Usage: {}".format(self.usage))

        job_id: Optional[str] = None
        wait_timeout: Optional[float] = None
        do_wait = False
        line_limit = 20

        idx = 0
        while idx < len(args):
            token = str(args[idx]).strip()
            if token == "--wait":
                do_wait = True
                if idx + 1 < len(args) and not str(args[idx + 1]).strip().startswith(
                    "-"
                ):
                    raw = str(args[idx + 1]).strip().lower()
                    if raw in {"none", "off", "disable", "disabled"}:
                        wait_timeout = None
                    else:
                        parsed = _safe_float(raw)
                        if parsed is None:
                            raise ValueError(
                                "Option --wait expects a numeric value or 'none'."
                            )
                        wait_timeout = parsed
                    idx += 2
                    continue
                wait_timeout = 30.0
                idx += 1
                continue
            if token.startswith("--wait="):
                do_wait = True
                raw = token.split("=", 1)[1].strip().lower()
                if raw in {"none", "off", "disable", "disabled"}:
                    wait_timeout = None
                else:
                    parsed = _safe_float(raw)
                    if parsed is None:
                        raise ValueError(
                            "Option --wait expects a numeric value or 'none'."
                        )
                    wait_timeout = parsed
                idx += 1
                continue
            if token.startswith("-"):
                raise ValueError("Unknown option: {!r}".format(token))
            if job_id is None:
                job_id = token
                idx += 1
                continue
            maybe_lines = _safe_int(token)
            if maybe_lines is None:
                raise ValueError("lines must be an integer")
            line_limit = max(1, maybe_lines)
            idx += 1

        if not job_id:
            raise ValueError("Usage: {}".format(self.usage))

        info = fetch_terminal_job_view(
            browser,
            job_id=job_id,
            do_wait=do_wait,
            wait_timeout=wait_timeout,
        )
        log_view = read_terminal_job_log_view(info)

        browser.emit_detail_sections(
            [
                (
                    "Job",
                    [
                        ("job_id", info.job_id),
                        ("state", info.state),
                        ("log_path", log_view.log_path or "<none>"),
                        ("status", log_view.status),
                    ],
                )
            ],
            title="Job tail {}".format(info.job_id),
            max_cell_width=120,
        )
        browser.emit("")
        if not log_view.lines:
            browser.emit(
                browser.render_detail_sections(
                    [("Output", [("message", log_view.message)])], max_cell_width=120
                )
            )
            return True

        tail = list(log_view.lines[-line_limit:])
        browser.emit(
            browser.render_table(
                ["line"], [[line] for line in tail], max_cell_width=120
            )
        )
        browser.emit(
            "Summary: total_lines={} shown={}".format(len(log_view.lines), len(tail))
        )
        return True


class JobsCancelCommand(TerminalCommandAPI):
    """
    Expose cancellation requests through ``jobs cancel``, ``jobs abort``, or ``jobs stop``.

    The command reports whether a request was accepted; it does not wait for the
    job to stop or make cancellation rollback guarantees.

    Example:
        >>> JobsCancelCommand().aliases
        ('abort', 'stop')
    """

    group = "jobs"
    expose_direct = False
    name = "cancel"
    aliases = ("abort", "stop")
    summary = "Cancel one running/pending job."
    usage = "jobs cancel <job_id>"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Validate one job ID, request cancellation, and report acceptance or absence of a cancellable job.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock()
            >>> host.supports_core_commands.return_value = True
            >>> host.execute_core_command.return_value = {"cancelled": False}
            >>> JobsCancelCommand().execute(host, ["job-1"])
            True
            >>> host.emit.assert_called_once_with("No cancellable job found for job-1.")


        :param browser: Host supplying Core or legacy cancellation and output methods.
        :param args: Exactly one nonblank job identifier, stripped before cancellation.
        :return: ``True`` after reporting, even when cancellation was not accepted.
        :raises ValueError: If the identifier is blank or the argument count is not one.
        """
        if len(args) != 1:
            raise ValueError("Usage: {}".format(self.usage))
        job_id = str(args[0]).strip()
        if not job_id:
            raise ValueError("Usage: {}".format(self.usage))

        result = _browser_job_cancel(browser, job_id=job_id)
        cancelled = bool(result.get("cancelled", False))
        if not cancelled:
            browser.emit("No cancellable job found for {}.".format(job_id))
            return True
        state = str(result.get("state", "unknown") or "unknown")
        browser.emit("Cancel requested for {} (state={}).".format(job_id, state))
        return True


class JobsPanelCommand(TerminalCommandAPI):
    """
    Expose attachment or detachment of a job's output view in a supporting host pane.

    This command selects the job; the windowed driver owns subsequent refreshes.
    Unsupported hosts receive a message suggesting ``jobs show`` instead.

    Example:
        >>> JobsPanelCommand().usage
        'jobs panel <job_id|off>'
    """

    group = "jobs"
    expose_direct = False
    name = "panel"
    aliases = ("pane",)
    summary = "Attach one job log to the windowed output panel."
    usage = "jobs panel <job_id|off>"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Detach on an off token, or fetch a job and ask a pane-capable host to attach it.

        Detachment is attempted before checking capabilities. Attach requests on
        unsupported hosts return without job lookup. Fetch exceptions are wrapped
        as unknown-job ``ValueError`` messages, including failures unrelated to
        absence. The attachment method's return value is ignored.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock()
            >>> host.detach_job_output_panel.return_value = False
            >>> JobsPanelCommand().execute(host, ["off"])
            True
            >>> host.emit.assert_called_once_with("No active job output panel.")


        :param browser: Host supplying pane support, job lookup, attachment, and output methods.
        :param args: Exactly one job ID or case-insensitive off/none/disable/disabled token.
        :return: ``True`` after the request or an unsupported/no-active-pane message.
        :raises ValueError: If arguments are invalid or fetching the requested job raises an exception.
        """
        if len(args) != 1:
            raise ValueError("Usage: {}".format(self.usage))

        token = str(args[0]).strip()
        if not token:
            raise ValueError("Usage: {}".format(self.usage))

        if token.lower() in {"off", "none", "disable", "disabled"}:
            if browser.detach_job_output_panel():
                browser.emit("Job output panel detached.")
            else:
                browser.emit("No active job output panel.")
            return True

        if not browser.supports_job_output_panel():
            browser.emit("Current UI does not support a dedicated job output panel.")
            browser.emit(
                "Use `jobs show {} --wait` to inspect progress/completion.".format(
                    token
                )
            )
            return True

        try:
            info = fetch_terminal_job_view(
                browser, job_id=token, do_wait=False, wait_timeout=None
            )
        except Exception as exc:
            raise ValueError("Unknown job id {!r}: {}".format(token, exc))

        info_job_id = str(info.job_id or token)
        browser.attach_job_output_panel(info_job_id)
        info_log_path = resolve_terminal_job_log_path(info)
        browser.emit_detail_sections(
            [
                (
                    "",
                    [
                        ("log_path", info_log_path or "<none>"),
                    ],
                )
            ],
            title="Job output panel attached to {}.".format(info_job_id),
            max_cell_width=120,
        )
        return True


__all__ = [
    "JobsListCommand",
    "JobsShowCommand",
    "JobsTailCommand",
    "JobsCancelCommand",
    "JobsPanelCommand",
]
