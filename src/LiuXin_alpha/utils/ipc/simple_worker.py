"""Compatibility worker API backed by `LiuXin_alpha.utils.jobs`."""

from __future__ import annotations

from typing import Any

from LiuXin_alpha.utils.jobs import JobExecution, JobRequest, execute_job


class WorkerError(Exception):
    def __init__(self, msg: str, orig_tb: str = "", log_path: str | None = None):
        super().__init__(msg)
        self.orig_tb = orig_tb
        self.log_path = log_path


def _raise_for_execution(exec_result: JobExecution) -> None:
    if exec_result.timed_out:
        raise WorkerError("Worker appears to have hung", exec_result.traceback or "", log_path=exec_result.log_path)
    if not exec_result.ok:
        raise WorkerError("Worker failed", exec_result.traceback or "", log_path=exec_result.log_path)


def fork_job(
    mod_name,
    func_name,
    args=(),
    kwargs=None,
    timeout=300,  # seconds
    cwd=None,
    priority="normal",
    env=None,
    no_output=False,
    heartbeat=None,
    abort=None,
    module_is_source_code=False,
    backend=None,
):
    """Run a function in the configured job backend.

    Returns a dict with:
    - `result`: function return value
    - `stdout_stderr`: log file path when `no_output=False`
    """

    del priority  # Priority handling is backend-specific and currently ignored.

    request = JobRequest(
        module_name=mod_name,
        function_name=func_name,
        args=tuple(args or ()),
        kwargs=dict(kwargs or {}),
        module_is_source_code=bool(module_is_source_code),
        cwd=cwd,
        env=dict(env or {}),
    )

    execution = execute_job(
        request,
        timeout=timeout,
        no_output=no_output,
        heartbeat=heartbeat,
        abort=abort,
        backend=backend,
    )

    if execution.aborted:
        return {"result": None, "stdout_stderr": execution.log_path}

    _raise_for_execution(execution)

    ans: dict[str, Any] = {"result": execution.result, "stdout_stderr": None}
    if not no_output:
        ans["stdout_stderr"] = execution.log_path
    return ans
