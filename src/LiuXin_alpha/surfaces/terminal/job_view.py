"""Shared terminal-facing job snapshot and log helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


def _preview(value: object, *, max_len: int = 200) -> str:
    text = repr(value)
    if len(text) <= max_len:
        return text
    return text[: max(0, max_len - 3)] + "..."


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


@dataclass(frozen=True)
class TerminalJobView:
    job_id: str
    label: str
    state: str
    backend_name: str
    submitted_at: Optional[float]
    started_at: Optional[float]
    finished_at: Optional[float]
    duration_s: Optional[float]
    timeout_s: Optional[float]
    no_output: bool
    log_path: str
    execution: dict[str, object] | None


@dataclass(frozen=True)
class TerminalJobLogView:
    status: str
    log_path: str
    lines: tuple[str, ...]
    message: str


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


def terminal_job_view_from_object(info: object) -> TerminalJobView:
    payload = _as_job_dict(info)
    return TerminalJobView(
        job_id=str(payload.get("job_id", "") or ""),
        label=str(payload.get("label", "") or ""),
        state=str(payload.get("state", "") or ""),
        backend_name=str(payload.get("backend_name", "") or ""),
        submitted_at=payload.get("submitted_at", None),
        started_at=payload.get("started_at", None),
        finished_at=payload.get("finished_at", None),
        duration_s=payload.get("duration_s", None),
        timeout_s=payload.get("timeout_s", None),
        no_output=bool(payload.get("no_output", False)),
        log_path=str(payload.get("log_path", "") or ""),
        execution=payload.get("execution", None),
    )


def fetch_terminal_job_view(
    browser,
    *,
    job_id: str,
    do_wait: bool,
    wait_timeout: Optional[float],
) -> TerminalJobView:
    if hasattr(browser, "supports_core_queries") and bool(browser.supports_core_queries()):
        payload: dict[str, object] = {"job_id": str(job_id)}
        if do_wait:
            payload["timeout_s"] = wait_timeout
        query_name = "jobs.wait" if do_wait else "jobs.get"
        result = browser.execute_core_query(query_name, payload=payload)
        return terminal_job_view_from_object((result or {}).get("job", {}))

    if do_wait:
        info = browser.job_manager.wait(job_id, timeout=wait_timeout)
    else:
        info = browser.job_manager.get(job_id)
    return terminal_job_view_from_object(info)


def resolve_terminal_job_log_path(job: TerminalJobView) -> str:
    if str(job.log_path).strip():
        return str(job.log_path).strip()
    execution = job.execution or {}
    if isinstance(execution, dict):
        return str(execution.get("log_path", "") or "").strip()
    return ""


def read_terminal_job_log_view(job: TerminalJobView) -> TerminalJobLogView:
    log_path = resolve_terminal_job_log_path(job)
    if not log_path:
        return TerminalJobLogView(
            status="no_log_path",
            log_path="",
            lines=(),
            message="No log path is available for this job.",
        )

    path = Path(log_path)
    if not path.exists():
        return TerminalJobLogView(
            status="missing",
            log_path=log_path,
            lines=(),
            message="Log file not found yet: {}".format(log_path),
        )

    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        return TerminalJobLogView(
            status="read_error",
            log_path=log_path,
            lines=(),
            message="Failed reading log {}: {}".format(log_path, exc),
        )

    payload_lines = tuple(text.splitlines())
    if not payload_lines:
        return TerminalJobLogView(
            status="empty",
            log_path=log_path,
            lines=(),
            message="(no log output yet)",
        )

    return TerminalJobLogView(
        status="ready",
        log_path=log_path,
        lines=payload_lines,
        message="",
    )


__all__ = [
    "TerminalJobLogView",
    "TerminalJobView",
    "fetch_terminal_job_view",
    "read_terminal_job_log_view",
    "resolve_terminal_job_log_path",
    "terminal_job_view_from_object",
]
