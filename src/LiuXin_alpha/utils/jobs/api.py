"""
Define job state, progress, result and cancellation contracts shared by job managers.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise api through a consuming regression::

        python -m pytest -q tests/utils/jobs/test_jobs_manager.py
"""

from __future__ import annotations

import importlib
import os
import tempfile
import time
import traceback
import types
from abc import ABC, abstractmethod
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import dataclass, field
from multiprocessing import Pipe, Process
from typing import Any, Callable, Iterator, Mapping


@dataclass
class JobRequest:
    """
    Description of a callable to execute.

    Example:
        Exercise JobRequest through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_manager.py
    """

    module_name: str
    function_name: str
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    module_is_source_code: bool = False
    cwd: str | None = None
    env: dict[str, str] | None = None


@dataclass
class JobExecution:
    """
    Execution result returned by a backend.

    Example:
        Exercise JobExecution through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_manager.py
    """

    ok: bool
    result: Any = None
    traceback: str | None = None
    log_path: str | None = None
    timed_out: bool = False
    aborted: bool = False


class JobBackend(ABC):
    """
    Backend interface for running a `JobRequest`.

    Example:
        Exercise JobBackend through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_manager.py
    """

    name: str

    @abstractmethod
    def run(
        self,
        request: JobRequest,
        *,
        timeout: float,
        no_output: bool,
        heartbeat: Callable[[], bool] | None,
        abort: Any,
        log_path: str | None = None,
    ) -> JobExecution:
        """
        Perform the run utility operation under explicit compatibility rules.

        Example:
            Exercise JobBackend.run through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param request: Value supplied for request under the utility contract.
        :param timeout: Maximum wait time before the operation fails.
        :param no_output: Value supplied for no output under the utility contract.
        :param heartbeat: Value supplied for heartbeat under the utility contract.
        :param abort: Value supplied for abort under the utility contract.
        :param log_path: Value supplied for log path under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


@contextmanager
def _temporary_cwd(path: str | None) -> Iterator[None]:
    """
    Perform the temporary cwd utility operation under explicit compatibility rules.

    Example:
        Exercise  temporary cwd through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_manager.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: An iterator yielding the normalized values described above.
    """
    if not path:
        yield
        return

    old = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old)


@contextmanager
def _temporary_env(env: Mapping[str, str] | None) -> Iterator[None]:
    """
    Perform the temporary env utility operation under explicit compatibility rules.

    Example:
        Exercise  temporary env through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_manager.py


    :param env: Value supplied for env under the utility contract.
    :return: An iterator yielding the normalized values described above.
    """
    if not env:
        yield
        return

    sentinel = object()
    previous: dict[str, Any] = {}
    try:
        for key, value in env.items():
            previous[key] = os.environ.get(key, sentinel)
            os.environ[key] = str(value)
        yield
    finally:
        for key, old in previous.items():
            if old is sentinel:
                os.environ.pop(key, None)
            else:
                os.environ[key] = str(old)


def _load_job_callable(request: JobRequest) -> Callable[..., Any]:
    """
    Perform the load job callable utility operation under explicit compatibility rules.

    Example:
        Exercise  load job callable through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_manager.py


    :param request: Value supplied for request under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if request.module_is_source_code:
        module = types.ModuleType("liuxin_job_source")
        exec(request.module_name, module.__dict__)
    else:
        module = importlib.import_module(request.module_name)

    func = getattr(module, request.function_name)
    if not callable(func):
        raise TypeError(f"Target is not callable: {request.module_name}.{request.function_name}")
    return func


def _execute_request_payload(request: JobRequest) -> dict[str, Any]:
    """
    Perform the execute request payload utility operation under explicit compatibility rules.

    Example:
        Exercise  execute request payload through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_manager.py


    :param request: Value supplied for request under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        with _temporary_env(request.env), _temporary_cwd(request.cwd):
            func = _load_job_callable(request)
            result = func(*request.args, **request.kwargs)
        return {"ok": True, "result": result, "tb": None}
    except BaseException:
        return {"ok": False, "result": None, "tb": traceback.format_exc()}


def allocate_job_log_path() -> str:
    """
    Perform the allocate job log path utility operation under explicit compatibility rules.

    Example:
        Exercise allocate job log path through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_manager.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    fd, path = tempfile.mkstemp(prefix="liuxin_job_", suffix=".log")
    os.close(fd)
    return path


def _execute_with_optional_logging(request: JobRequest, log_path: str | None) -> dict[str, Any]:
    """
    Perform the execute with optional logging utility operation under explicit compatibility rules.

    Example:
        Exercise  execute with optional logging through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_manager.py


    :param request: Value supplied for request under the utility contract.
    :param log_path: Value supplied for log path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not log_path:
        return _execute_request_payload(request)

    with open(log_path, "w", encoding="utf-8", errors="replace") as log_file:
        with redirect_stdout(log_file), redirect_stderr(log_file):
            return _execute_request_payload(request)


def _process_entry(conn, request: JobRequest, log_path: str | None) -> None:
    """
    Perform the process entry utility operation under explicit compatibility rules.

    Example:
        Exercise  process entry through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_manager.py


    :param conn: SQLite connection used for schema or metadata queries.
    :param request: Value supplied for request under the utility contract.
    :param log_path: Value supplied for log path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    try:
        payload = _execute_with_optional_logging(request, log_path)
        conn.send(payload)
    except BaseException:
        conn.send({"ok": False, "result": None, "tb": traceback.format_exc()})
    finally:
        conn.close()


class SerialJobBackend(JobBackend):
    """
    Provide the SerialJobBackend utility contract with explicit state and cleanup behavior.

    Example:
        Exercise SerialJobBackend through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_manager.py
    """
    name = "serial"

    def run(
        self,
        request: JobRequest,
        *,
        timeout: float,
        no_output: bool,
        heartbeat: Callable[[], bool] | None,
        abort: Any,
        log_path: str | None = None,
    ) -> JobExecution:
        """
        Perform the run utility operation under explicit compatibility rules.

        Example:
            Exercise SerialJobBackend.run through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param request: Value supplied for request under the utility contract.
        :param timeout: Maximum wait time before the operation fails.
        :param no_output: Value supplied for no output under the utility contract.
        :param heartbeat: Value supplied for heartbeat under the utility contract.
        :param abort: Value supplied for abort under the utility contract.
        :param log_path: Value supplied for log path under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        del timeout  # Serial backend cannot preempt running work.

        if abort is not None and hasattr(abort, "is_set") and abort.is_set():
            return JobExecution(ok=False, aborted=True)
        if heartbeat is not None and not heartbeat():
            return JobExecution(ok=False, timed_out=True, traceback="Worker heartbeat reported failure")

        effective_log_path = None if no_output else (str(log_path).strip() if log_path else allocate_job_log_path())
        payload = _execute_with_optional_logging(request, effective_log_path)

        return JobExecution(
            ok=bool(payload.get("ok")),
            result=payload.get("result"),
            traceback=payload.get("tb"),
            log_path=effective_log_path,
        )


class ProcessJobBackend(JobBackend):
    """
    Provide the ProcessJobBackend utility contract with explicit state and cleanup behavior.

    Example:
        Exercise ProcessJobBackend through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_manager.py
    """
    name = "process"

    def run(
        self,
        request: JobRequest,
        *,
        timeout: float,
        no_output: bool,
        heartbeat: Callable[[], bool] | None,
        abort: Any,
        log_path: str | None = None,
    ) -> JobExecution:
        """
        Perform the run utility operation under explicit compatibility rules.

        Example:
            Exercise ProcessJobBackend.run through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param request: Value supplied for request under the utility contract.
        :param timeout: Maximum wait time before the operation fails.
        :param no_output: Value supplied for no output under the utility contract.
        :param heartbeat: Value supplied for heartbeat under the utility contract.
        :param abort: Value supplied for abort under the utility contract.
        :param log_path: Value supplied for log path under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        effective_log_path = None if no_output else (str(log_path).strip() if log_path else allocate_job_log_path())

        parent_conn, child_conn = Pipe(duplex=False)
        worker = Process(target=_process_entry, args=(child_conn, request, effective_log_path), daemon=True)
        started = time.monotonic()
        worker.start()
        child_conn.close()

        try:
            while worker.is_alive():
                if abort is not None and hasattr(abort, "is_set") and abort.is_set():
                    worker.terminate()
                    worker.join(timeout=1.0)
                    return JobExecution(ok=False, aborted=True, log_path=effective_log_path)

                if heartbeat is not None and not heartbeat():
                    worker.terminate()
                    worker.join(timeout=1.0)
                    return JobExecution(
                        ok=False,
                        timed_out=True,
                        traceback="Worker heartbeat reported failure",
                        log_path=effective_log_path,
                    )

                if timeout >= 0 and (time.monotonic() - started) > timeout:
                    worker.terminate()
                    worker.join(timeout=1.0)
                    return JobExecution(ok=False, timed_out=True, traceback="Worker timed out", log_path=effective_log_path)

                worker.join(timeout=0.05)

            if parent_conn.poll(timeout=0.2):
                payload = parent_conn.recv()
            else:
                payload = {"ok": False, "result": None, "tb": "Worker exited without returning a payload"}

            return JobExecution(
                ok=bool(payload.get("ok")),
                result=payload.get("result"),
                traceback=payload.get("tb"),
                log_path=effective_log_path,
            )
        finally:
            parent_conn.close()


_SERIAL_BACKEND = SerialJobBackend()
_PROCESS_BACKEND = ProcessJobBackend()


def available_backends() -> tuple[str, ...]:
    """
    Perform the available backends utility operation under explicit compatibility rules.

    Example:
        Exercise available backends through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_manager.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return ("process", "serial")


def get_backend(name: str | None = None) -> JobBackend:
    """
    Return backend under the documented compatibility and safety rules.

    Example:
        Exercise get backend through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_manager.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    selected = (name or os.environ.get("LIUXIN_JOB_BACKEND", "process")).strip().lower()
    if selected in {"", "auto", "default"}:
        selected = "process"

    if selected == "process":
        return _PROCESS_BACKEND
    if selected == "serial":
        return _SERIAL_BACKEND

    valid = ", ".join(available_backends())
    raise ValueError(f"Unknown job backend: {selected!r}. Expected one of: {valid}")


def execute_job(
    request: JobRequest,
    *,
    timeout: float = 300,
    no_output: bool = False,
    heartbeat: Callable[[], bool] | None = None,
    abort: Any = None,
    backend: str | JobBackend | None = None,
    log_path: str | None = None,
) -> JobExecution:
    """
    Perform the execute job utility operation under explicit compatibility rules.

    Example:
        Exercise execute job through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_manager.py


    :param request: Value supplied for request under the utility contract.
    :param timeout: Maximum wait time before the operation fails.
    :param no_output: Value supplied for no output under the utility contract.
    :param heartbeat: Value supplied for heartbeat under the utility contract.
    :param abort: Value supplied for abort under the utility contract.
    :param backend: Value supplied for backend under the utility contract.
    :param log_path: Value supplied for log path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    backend_impl: JobBackend
    if isinstance(backend, JobBackend):
        backend_impl = backend
    else:
        backend_impl = get_backend(backend)

    return backend_impl.run(
        request,
        timeout=float(timeout),
        no_output=bool(no_output),
        heartbeat=heartbeat,
        abort=abort,
        log_path=log_path,
    )
