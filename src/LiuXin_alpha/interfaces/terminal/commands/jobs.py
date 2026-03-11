"""`jobs` command group for background job inspection/control."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from LiuXin_alpha.interfaces.terminal.commands.base import TerminalCommandAPI


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


def _preview(value: object, *, max_len: int = 200) -> str:
    text = repr(value)
    if len(text) <= max_len:
        return text
    return text[: max(0, max_len - 3)] + "..."


def _get_job_field(info: object, key: str, default=None):
    if isinstance(info, dict):
        return info.get(key, default)
    return getattr(info, key, default)


def _as_job_execution_dict(execution: object | None) -> dict[str, object] | None:
    if execution is None:
        return None
    if isinstance(execution, dict):
        payload = dict(execution)
        if "result_preview" not in payload and "result" in payload:
            payload["result_preview"] = _preview(payload.get("result"))
        return payload
    result = getattr(execution, "result", None)
    return {
        "ok": bool(getattr(execution, "ok", False)),
        "timed_out": bool(getattr(execution, "timed_out", False)),
        "aborted": bool(getattr(execution, "aborted", False)),
        "traceback": str(getattr(execution, "traceback", "") or ""),
        "log_path": str(getattr(execution, "log_path", "") or ""),
        "result_preview": _preview(result),
    }


def _as_job_dict(info: object) -> dict[str, object]:
    if isinstance(info, dict):
        payload = dict(info)
        payload["execution"] = _as_job_execution_dict(payload.get("execution"))
        return payload
    return {
        "job_id": str(getattr(info, "job_id", "") or ""),
        "label": str(getattr(info, "label", "") or ""),
        "state": str(getattr(info, "state", "") or ""),
        "backend_name": str(getattr(info, "backend_name", "") or ""),
        "submitted_at": getattr(info, "submitted_at", None),
        "started_at": getattr(info, "started_at", None),
        "finished_at": getattr(info, "finished_at", None),
        "duration_s": getattr(info, "duration_s", None),
        "timeout_s": getattr(info, "timeout_s", None),
        "no_output": bool(getattr(info, "no_output", False)),
        "log_path": str(getattr(info, "log_path", "") or ""),
        "execution": _as_job_execution_dict(getattr(info, "execution", None)),
    }


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
        return [_as_job_dict(one) for one in jobs_raw], total

    all_jobs = browser.job_manager.list(states=states)
    total = len(all_jobs)
    window = all_jobs[offset : offset + limit]
    return [_as_job_dict(one) for one in window], total


def _browser_job_get(
    browser,
    *,
    job_id: str,
    do_wait: bool,
    wait_timeout: Optional[float],
) -> dict[str, object]:
    if hasattr(browser, "supports_core_queries") and bool(browser.supports_core_queries()):
        payload: dict[str, object] = {"job_id": str(job_id)}
        if do_wait:
            payload["timeout_s"] = wait_timeout
        query_name = "jobs.wait" if do_wait else "jobs.get"
        result = browser.execute_core_query(query_name, payload=payload)
        return _as_job_dict((result or {}).get("job", {}))

    if do_wait:
        info = browser.job_manager.wait(job_id, timeout=wait_timeout)
    else:
        info = browser.job_manager.get(job_id)
    return _as_job_dict(info)


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
                    str(_get_job_field(one, "job_id", "") or ""),
                    str(_get_job_field(one, "label", "") or ""),
                    str(_get_job_field(one, "state", "") or ""),
                    str(_get_job_field(one, "backend_name", "") or ""),
                    _format_ts(_get_job_field(one, "submitted_at", None)),
                    _format_ts(_get_job_field(one, "started_at", None)),
                    _format_ts(_get_job_field(one, "finished_at", None)),
                    _format_duration(_get_job_field(one, "duration_s", None)),
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

        info = _browser_job_get(
            browser,
            job_id=job_id,
            do_wait=do_wait,
            wait_timeout=wait_timeout,
        )

        browser.emit("Job {}".format(str(_get_job_field(info, "job_id", "") or "")))
        browser.emit("  label: {}".format(str(_get_job_field(info, "label", "") or "")))
        browser.emit("  state: {}".format(str(_get_job_field(info, "state", "") or "")))
        browser.emit("  backend: {}".format(str(_get_job_field(info, "backend_name", "") or "")))
        browser.emit("  submitted_at: {}".format(_format_ts(_get_job_field(info, "submitted_at", None))))
        browser.emit("  started_at: {}".format(_format_ts(_get_job_field(info, "started_at", None))))
        browser.emit("  finished_at: {}".format(_format_ts(_get_job_field(info, "finished_at", None))))
        browser.emit("  duration_s: {}".format(_format_duration(_get_job_field(info, "duration_s", None))))
        browser.emit("  timeout_s: {}".format(_get_job_field(info, "timeout_s", None)))
        browser.emit("  no_output: {}".format("yes" if bool(_get_job_field(info, "no_output", False)) else "no"))
        browser.emit("  log_path: {}".format(str(_get_job_field(info, "log_path", "") or "")))

        execution = _as_job_execution_dict(_get_job_field(info, "execution", None))
        if execution is None:
            browser.emit("  execution: <not finished>")
            return True

        browser.emit("  execution_ok: {}".format("yes" if bool(execution.get("ok", False)) else "no"))
        browser.emit("  timed_out: {}".format("yes" if bool(execution.get("timed_out", False)) else "no"))
        browser.emit("  aborted: {}".format("yes" if bool(execution.get("aborted", False)) else "no"))
        browser.emit("  result_preview: {}".format(str(execution.get("result_preview", ""))))

        tb = str(execution.get("traceback", "") or "").strip()
        if not tb:
            return True
        if show_traceback:
            browser.emit("  traceback:")
            for line in tb.splitlines():
                browser.emit("    {}".format(line))
        else:
            first_line = tb.splitlines()[0]
            browser.emit("  traceback_preview: {}".format(first_line))
            browser.emit("  (use --traceback for full traceback)")
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
            info = _browser_job_get(browser, job_id=token, do_wait=False, wait_timeout=None)
        except Exception as exc:
            raise ValueError("Unknown job id {!r}: {}".format(token, exc))

        info_job_id = str(_get_job_field(info, "job_id", token) or token)
        browser.attach_job_output_panel(info_job_id)
        browser.emit("Job output panel attached to {}.".format(info_job_id))
        info_log_path = str(_get_job_field(info, "log_path", "") or "")
        if info_log_path:
            browser.emit("  log_path: {}".format(info_log_path))
        else:
            browser.emit("  log_path: <none>")
        return True


__all__ = [
    "JobsListCommand",
    "JobsShowCommand",
    "JobsCancelCommand",
    "JobsPanelCommand",
]
