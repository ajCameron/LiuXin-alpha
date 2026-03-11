"""Portable job execution API with swappable backends.

This module intentionally provides a small, explicit surface area so callers can
run the same job using either:
- `process` backend: child process isolation with timeout/abort support
- `serial` backend: in-process execution fallback
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
    """Description of a callable to execute."""

    module_name: str
    function_name: str
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    module_is_source_code: bool = False
    cwd: str | None = None
    env: dict[str, str] | None = None


@dataclass
class JobExecution:
    """Execution result returned by a backend."""

    ok: bool
    result: Any = None
    traceback: str | None = None
    log_path: str | None = None
    timed_out: bool = False
    aborted: bool = False


class JobBackend(ABC):
    """Backend interface for running a `JobRequest`."""

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
        raise NotImplementedError


@contextmanager
def _temporary_cwd(path: str | None) -> Iterator[None]:
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
    try:
        with _temporary_env(request.env), _temporary_cwd(request.cwd):
            func = _load_job_callable(request)
            result = func(*request.args, **request.kwargs)
        return {"ok": True, "result": result, "tb": None}
    except BaseException:
        return {"ok": False, "result": None, "tb": traceback.format_exc()}


def allocate_job_log_path() -> str:
    fd, path = tempfile.mkstemp(prefix="liuxin_job_", suffix=".log")
    os.close(fd)
    return path


def _execute_with_optional_logging(request: JobRequest, log_path: str | None) -> dict[str, Any]:
    if not log_path:
        return _execute_request_payload(request)

    with open(log_path, "w", encoding="utf-8", errors="replace") as log_file:
        with redirect_stdout(log_file), redirect_stderr(log_file):
            return _execute_request_payload(request)


def _process_entry(conn, request: JobRequest, log_path: str | None) -> None:
    try:
        payload = _execute_with_optional_logging(request, log_path)
        conn.send(payload)
    except BaseException:
        conn.send({"ok": False, "result": None, "tb": traceback.format_exc()})
    finally:
        conn.close()


class SerialJobBackend(JobBackend):
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
    return ("process", "serial")


def get_backend(name: str | None = None) -> JobBackend:
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
