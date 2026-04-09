"""`jobs` command group for background job inspection/control."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from LiuXin_alpha.interfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.interfaces.terminal.job_view import (
    fetch_terminal_job_view,
    read_terminal_job_log_view,
    resolve_terminal_job_log_path,
    terminal_job_view_from_object,
)


def _safe_int(value: str) -> Optional[int]:
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _safe_float(value: str) -> Optional[float]:
    try:
        return float(str(value).strip())
    except Exception:
        return None


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


def _parse_states(raw: str) -> set[str]:
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
    if value is None:
        return ""
    try:
        return datetime.fromtimestamp(float(value)).isoformat(timespec="seconds")
    except Exception:
        return ""


def _format_duration(value: float | None) -> str:
    if value is None:
        return ""
    try:
        return "{:.2f}".format(float(value))
    except Exception:
        return ""


def _browser_jobs_list(browser, *, states: Optional[set[str]], limit: int, offset: int) -> tuple[list[dict[str, object]], int]:
    if hasattr(browser, "supports_core_queries") and bool(browser.supports_core_queries()):
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
    if hasattr(browser, "supports_core_commands") and bool(browser.supports_core_commands()):
        result = browser.execute_core_command("jobs.cancel", payload={"job_id": str(job_id)})
        return dict(result or {})

    cancelled = bool(browser.job_manager.cancel(job_id))
    state = "unknown"
    try:
        state = str(browser.job_manager.get(job_id).state)
    except Exception:
        state = "unknown"
    return {"job_id": str(job_id), "cancelled": cancelled, "state": state}


class JobsListCommand(TerminalCommandAPI):
    """List submitted jobs and their states."""

    group = "jobs"
    group_aliases = ("job",)
    expose_direct = False
    name = "list"
    aliases = ("ls",)
    summary = "List submitted jobs."
    usage = "jobs list [limit] [offset] [--state running,failed,...]"

    def execute(self, browser, args: list[str]) -> bool:
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

        window, total = _browser_jobs_list(browser, states=states, limit=limit, offset=offset)

        if not window:
            browser.emit("No jobs matched.")
            browser.emit("Summary: total={} shown=0 offset={} limit={}".format(total, offset, limit))
            return True

        headers = ["job_id", "label", "state", "backend", "submitted", "started", "finished", "dur_s"]
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
    """Show detailed job information."""

    group = "jobs"
    expose_direct = False
    name = "show"
    aliases = ()
    summary = "Show one job."
    usage = "jobs show <job_id> [--wait[=<sec|none>]] [--traceback]"

    def execute(self, browser, args: list[str]) -> bool:
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
                if idx + 1 < len(args) and not str(args[idx + 1]).strip().startswith("-"):
                    raw = str(args[idx + 1]).strip().lower()
                    if raw in {"none", "off", "disable", "disabled"}:
                        wait_timeout = None
                    else:
                        parsed = _safe_float(raw)
                        if parsed is None:
                            raise ValueError("Option --wait expects a numeric value or 'none'.")
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
                        raise ValueError("Option --wait expects a numeric value or 'none'.")
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
            raise ValueError("Unexpected extra argument {!r}. Usage: {}".format(token, self.usage))

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
            browser.emit(browser.render_detail_sections([("Execution", [("status", "<not finished>")])], max_cell_width=120))
            return True

        browser.emit("")
        browser.emit(
            browser.render_detail_sections(
                [
                    (
                        "Execution",
                        [
                            ("execution_ok", "yes" if bool(execution.get("ok", False)) else "no"),
                            ("timed_out", "yes" if bool(execution.get("timed_out", False)) else "no"),
                            ("aborted", "yes" if bool(execution.get("aborted", False)) else "no"),
                            ("result_preview", str(execution.get("result_preview", ""))),
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
            browser.emit(browser.render_table(["line"], [[line] for line in tb.splitlines()], max_cell_width=120))
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
    """Show recent log output for one job."""

    group = "jobs"
    expose_direct = False
    name = "tail"
    aliases = ("log",)
    summary = "Show recent log output for one job."
    usage = "jobs tail <job_id> [lines] [--wait[=<sec|none>]]"

    def execute(self, browser, args: list[str]) -> bool:
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
                if idx + 1 < len(args) and not str(args[idx + 1]).strip().startswith("-"):
                    raw = str(args[idx + 1]).strip().lower()
                    if raw in {"none", "off", "disable", "disabled"}:
                        wait_timeout = None
                    else:
                        parsed = _safe_float(raw)
                        if parsed is None:
                            raise ValueError("Option --wait expects a numeric value or 'none'.")
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
                        raise ValueError("Option --wait expects a numeric value or 'none'.")
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
            browser.emit(browser.render_detail_sections([("Output", [("message", log_view.message)])], max_cell_width=120))
            return True

        tail = list(log_view.lines[-line_limit:])
        browser.emit(browser.render_table(["line"], [[line] for line in tail], max_cell_width=120))
        browser.emit("Summary: total_lines={} shown={}".format(len(log_view.lines), len(tail)))
        return True


class JobsCancelCommand(TerminalCommandAPI):
    """Cancel one job by id."""

    group = "jobs"
    expose_direct = False
    name = "cancel"
    aliases = ("abort", "stop")
    summary = "Cancel one running/pending job."
    usage = "jobs cancel <job_id>"

    def execute(self, browser, args: list[str]) -> bool:
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
    """Attach/detach a job log stream to the windowed auxiliary panel."""

    group = "jobs"
    expose_direct = False
    name = "panel"
    aliases = ("pane",)
    summary = "Attach one job log to the windowed output panel."
    usage = "jobs panel <job_id|off>"

    def execute(self, browser, args: list[str]) -> bool:
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
            browser.emit("Use `jobs show {} --wait` to inspect progress/completion.".format(token))
            return True

        try:
            info = fetch_terminal_job_view(browser, job_id=token, do_wait=False, wait_timeout=None)
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
