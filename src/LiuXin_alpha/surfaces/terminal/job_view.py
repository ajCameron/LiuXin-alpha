"""
Normalize Core/local job records and read advertised log files for terminal presentation.

Normalization is shallow and permissive, not schema validation. Log reading returns
all decoded lines; callers choose their own display tail or viewport afterward.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


def _preview(value: object, *, max_len: int = 200) -> str:
    """
    Render a value with ``repr`` and truncate long representations with an ellipsis.

    The budget includes the three dots. Budgets below three can still produce
    ``...`` when truncation is needed; representation errors are not suppressed.

    Example:
        >>> _preview("abcdefgh", max_len=6)
        "'ab..."


    :param value: Object whose Python representation is used as the display preview.
    :param max_len: Desired maximum characters, including any truncation marker.
    :return: Full representation or its shortened prefix followed by ``...``.
    """
    text = repr(value)
    if len(text) <= max_len:
        return text
    return text[: max(0, max_len - 3)] + "..."


def _as_job_execution_dict(execution: object | None) -> dict[str, object] | None:
    """
    Shallow-copy execution dictionaries or collect display fields from an execution object.

    Dictionaries keep all keys and any existing ``result_preview``; a missing preview
    is derived only when ``result`` exists. Objects provide boolean status flags,
    traceback/log strings, and a result preview, with defaults for absent attributes.

    Example:
        >>> _as_job_execution_dict({"result": 42})
        {'result': 42, 'result_preview': '42'}


    :param execution: Optional dictionary or attribute-based execution result.
    :return: New top-level execution dictionary, or ``None`` for absent execution data.
    """
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
    """
    Hold normalized job identity, status, timing, output policy, and execution metadata.

    ``job_id``, ``label``, ``state``, and ``backend_name`` are display strings.
    Submitted/started/finished times are optional timestamp values; duration and
    timeout are optional seconds. The factory preserves these numeric fields without
    validation. ``no_output`` records output policy, ``log_path`` advertises a path,
    and ``execution`` holds optional execution details. Frozen attributes do not make
    the execution dictionary or its nested values immutable.

    Example:
        >>> job = terminal_job_view_from_object({"job_id": "j1", "state": "running"})
        >>> (job.job_id, job.state, job.execution)
        ('j1', 'running', None)
    """

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
    """
    Describe a complete log read or an unavailable/empty-log condition.

    ``status`` is ``ready``, ``no_log_path``, ``missing``, ``read_error``, or ``empty``.
    ``log_path`` is the advertised path, ``lines`` contains all split log lines on
    success, and ``message`` explains a nonready result. This record itself does not
    impose a byte or line bound and performs no filesystem validation at construction.

    Example:
        >>> result = TerminalJobLogView("empty", "job.log", (), "(no log output yet)")
        >>> (result.status, result.lines)
        ('empty', ())
    """

    status: str
    log_path: str
    lines: tuple[str, ...]
    message: str


def _as_job_dict(info: object) -> dict[str, object]:
    """
    Copy a job dictionary or gather known job attributes, normalizing nested execution data.

    Dictionary inputs retain arbitrary top-level keys. Attribute inputs get defaults
    for missing fields and string/boolean conversion for display values; timing values
    pass through unchanged. Attribute access and conversion errors propagate.

    Example:
        >>> _as_job_dict({"job_id": "j1", "custom": 7})
        {'job_id': 'j1', 'custom': 7, 'execution': None}


    :param info: Dictionary or attribute-based job record to adapt.
    :return: New dictionary containing job fields and normalized optional execution details.
    """
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
    """
    Build a permissive terminal snapshot from dictionary keys or object attributes.

    Falsey identity/state/path values become empty strings, ``no_output`` becomes
    a boolean, and missing timing fields become ``None``. Timing types and state
    names are not validated. Empty or missing-like records can therefore produce
    a mostly empty snapshot rather than a not-found error.

    Example:
        >>> job = terminal_job_view_from_object({"job_id": "j1", "no_output": 1})
        >>> (job.job_id, job.no_output, job.started_at)
        ('j1', True, None)


    :param info: Job dictionary or object to read through the shared normalization helper.
    :return: Frozen top-level snapshot with a shallowly normalized execution dictionary.
    """
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
    """
    Fetch or wait for a job through the browser's advertised Core capability or local manager.

    A truthy Core capability chooses ``jobs.get``/``jobs.wait`` and reads the returned
    ``job`` payload. Otherwise call the matching local-manager method. A failing
    selected backend does not fall back to the other. A missing Core job payload is
    normalized as an empty record, not rejected here.

    Example:
        >>> job = fetch_terminal_job_view(  # doctest: +SKIP
        ...     browser, job_id="j1", do_wait=False, wait_timeout=None
        ... )


    :param browser: Host exposing Core query capability and/or a local job manager.
    :param job_id: Job identifier, stringified for Core and passed directly to local methods.
    :param do_wait: Use the selected backend's wait operation instead of an immediate snapshot.
    :param wait_timeout: Optional timeout in seconds passed only for waiting operations.
    :return: Normalized job snapshot; capability, query, wait, and normalization errors propagate.
    """
    if hasattr(browser, "supports_core_queries") and bool(
        browser.supports_core_queries()
    ):
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
    """
    Select the stripped top-level log path, falling back to execution metadata when blank.

    This selects an advertised string only; it does not check existence, readability,
    containment, or whether the referenced file belongs to the current machine.

    Example:
        >>> job = terminal_job_view_from_object({"execution": {"log_path": " job.log "}})
        >>> resolve_terminal_job_log_path(job)
        'job.log'


    :param job: Job snapshot whose direct and execution log-path fields are inspected.
    :return: Nonblank direct path, execution-dictionary path, or an empty string when absent.
    """
    if str(job.log_path).strip():
        return str(job.log_path).strip()
    execution = job.execution or {}
    if isinstance(execution, dict):
        return str(execution.get("log_path", "") or "").strip()
    return ""


def read_terminal_job_log_view(job: TerminalJobView) -> TerminalJobLogView:
    """
    Read all lines of the advertised log or return an explicit absent, empty, or read-error result.

    Resolve the path, check existence, then read the complete file as UTF-8 with
    replacement for invalid bytes. Split line endings without imposing a size/tail
    limit or escaping terminal control characters. File-read exceptions become a
    ``read_error`` record; path resolution and the existence check are outside that
    handler and can still raise. Paths are trusted as advertised, not confined here.

    Example:
        >>> read_terminal_job_log_view(terminal_job_view_from_object({})).status
        'no_log_path'


    :param job: Snapshot advertising a direct or execution-level log path.
    :return: Immutable log-status record with all decoded lines or an explanatory message.
    """
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
