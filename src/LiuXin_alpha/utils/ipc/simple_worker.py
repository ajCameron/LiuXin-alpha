"""
Run isolated worker callables and exchange serialized results over local IPC.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise simple worker through a consuming regression::

        python -m pytest -q tests/utils/ipc/test_simple_worker.py
"""

from __future__ import annotations

from typing import Any

from LiuXin_alpha.utils.jobs import JobExecution, JobRequest, execute_job


class WorkerError(Exception):
    """
    Report the WorkerError Calibre compatibility failure.

    Example:
        Exercise WorkerError through a consuming regression::

            python -m pytest -q tests/utils/ipc/test_simple_worker.py
    """
    def __init__(self, msg: str, orig_tb: str = "", log_path: str | None = None):
        """
        Initialize and validate the WorkerError state.

        Example:
            Exercise WorkerError.  init   through a consuming regression::

                python -m pytest -q tests/utils/ipc/test_simple_worker.py


        :param msg: Value supplied for msg under the utility contract.
        :param orig_tb: Value supplied for orig tb under the utility contract.
        :param log_path: Value supplied for log path under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        super().__init__(msg)
        self.orig_tb = orig_tb
        self.log_path = log_path


def _raise_for_execution(exec_result: JobExecution) -> None:
    """
    Perform the raise for execution utility operation under explicit compatibility rules.

    Example:
        Exercise  raise for execution through a consuming regression::

            python -m pytest -q tests/utils/ipc/test_simple_worker.py


    :param exec_result: Value supplied for exec result under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Run a function in the configured job backend.

    Example:
        Exercise fork job through a consuming regression::

            python -m pytest -q tests/utils/ipc/test_simple_worker.py


    :param mod_name: Value supplied for mod name under the utility contract.
    :param func_name: Value supplied for func name under the utility contract.
    :param args: Positional values forwarded to the compatibility implementation.
    :param kwargs: Keyword values forwarded to the compatibility implementation.
    :param timeout: Maximum wait time before the operation fails.
    :param cwd: Value supplied for cwd under the utility contract.
    :param priority: Value supplied for priority under the utility contract.
    :param env: Value supplied for env under the utility contract.
    :param no_output: Value supplied for no output under the utility contract.
    :param heartbeat: Value supplied for heartbeat under the utility contract.
    :param abort: Value supplied for abort under the utility contract.
    :param module_is_source_code: Value supplied for module is source code under the
        utility contract.
    :param backend: Value supplied for backend under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
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
