"""High-level job manager built on top of the low-level jobs API.

This module provides a unified submission interface for asynchronous job
execution with:
- swappable execution backends (`process`, `serial`, or custom `JobBackend`)
- queueing and worker concurrency via `ThreadPoolExecutor`
- job status tracking, cancellation, waiting, and result retrieval
"""

from __future__ import annotations

import threading
import time
import uuid

from concurrent.futures import (
    CancelledError as FutureCancelledError,
    Future,
    ThreadPoolExecutor,
    TimeoutError as FutureTimeoutError,
)
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Literal

from .api import JobBackend, JobExecution, JobRequest, allocate_job_log_path, execute_job

JobState = Literal["pending", "running", "succeeded", "failed", "timed_out", "aborted", "cancelled"]


@dataclass
class ManagedJob:
    """Snapshot of one submitted job."""

    job_id: str
    request: JobRequest
    state: JobState
    submitted_at: float
    started_at: float | None = None
    finished_at: float | None = None
    timeout_s: float = 300.0
    no_output: bool = False
    log_path: str | None = None
    backend_name: str | None = None
    label: str | None = None
    retry_of_job_id: str | None = None
    execution: JobExecution | None = None

    @property
    def done(self) -> bool:
        return self.state in {"succeeded", "failed", "timed_out", "aborted", "cancelled"}

    @property
    def duration_s(self) -> float | None:
        if self.started_at is None:
            return None
        end = self.finished_at if self.finished_at is not None else time.time()
        return max(0.0, float(end - self.started_at))


@dataclass
class _RuntimeJob:
    info: ManagedJob
    future: Future[JobExecution] | None = None
    abort_event: threading.Event = field(default_factory=threading.Event)


class JobManagerAPI:
    """Abstract shape for job manager implementations."""

    def submit(
        self,
        request: JobRequest,
        *,
        timeout: float = 300.0,
        no_output: bool = False,
        heartbeat: Callable[[], bool] | None = None,
        backend: str | JobBackend | None = None,
        label: str | None = None,
    ) -> str:
        raise NotImplementedError

    def get(self, job_id: str) -> ManagedJob:
        raise NotImplementedError

    def list(self, *, states: Iterable[JobState] | None = None) -> list[ManagedJob]:
        raise NotImplementedError

    def wait(self, job_id: str, *, timeout: float | None = None) -> ManagedJob:
        raise NotImplementedError

    def result(
        self,
        job_id: str,
        *,
        timeout: float | None = None,
        raise_on_failure: bool = False,
    ) -> JobExecution:
        raise NotImplementedError

    def cancel(self, job_id: str) -> bool:
        raise NotImplementedError

    def retry(
        self,
        job_id: str,
        *,
        label: str | None = None,
        allow_succeeded: bool = False,
    ) -> str:
        """Submit a new run of one completed job without rewriting history."""

        raise NotImplementedError

    def shutdown(self, *, wait: bool = True, cancel_pending: bool = False) -> None:
        raise NotImplementedError


class InMemoryJobManager(JobManagerAPI):
    """Thread-safe in-memory job registry and scheduler."""

    def __init__(
        self,
        *,
        max_workers: int = 4,
        default_backend: str | JobBackend | None = None,
    ) -> None:
        self._lock = threading.RLock()
        self._jobs: dict[str, _RuntimeJob] = {}
        self._executor = ThreadPoolExecutor(max_workers=max(1, int(max_workers)), thread_name_prefix="liuxin-job")
        self._default_backend = default_backend
        self._closed = False

    def submit(
        self,
        request: JobRequest,
        *,
        timeout: float = 300.0,
        no_output: bool = False,
        heartbeat: Callable[[], bool] | None = None,
        backend: str | JobBackend | None = None,
        label: str | None = None,
        retry_of_job_id: str | None = None,
    ) -> str:
        with self._lock:
            if self._closed:
                raise RuntimeError("Job manager is shut down")

            job_id = uuid.uuid4().hex
            info = ManagedJob(
                job_id=job_id,
                request=request,
                state="pending",
                submitted_at=time.time(),
                timeout_s=float(timeout),
                no_output=bool(no_output),
                log_path=(None if no_output else allocate_job_log_path()),
                backend_name=self._backend_name(backend),
                label=label,
                retry_of_job_id=retry_of_job_id,
            )
            runtime = _RuntimeJob(info=info)
            self._jobs[job_id] = runtime
            runtime.future = self._executor.submit(
                self._run_one,
                job_id,
                heartbeat=heartbeat,
                backend=backend,
            )
            return job_id

    def _backend_name(self, backend: str | JobBackend | None) -> str:
        selected = self._default_backend if backend is None else backend
        if selected is None:
            return "auto"
        if isinstance(selected, JobBackend):
            return str(getattr(selected, "name", selected.__class__.__name__))
        return str(selected)

    def _run_one(
        self,
        job_id: str,
        *,
        heartbeat: Callable[[], bool] | None,
        backend: str | JobBackend | None,
    ) -> JobExecution:
        with self._lock:
            runtime = self._jobs.get(job_id)
            if runtime is None:
                return JobExecution(ok=False, aborted=True, traceback="Job not found")
            if runtime.abort_event.is_set():
                runtime.info.state = "cancelled"
                runtime.info.started_at = time.time()
                runtime.info.finished_at = runtime.info.started_at
                exec_result = JobExecution(ok=False, aborted=True, traceback="Job cancelled before start")
                runtime.info.execution = exec_result
                return exec_result

            runtime.info.state = "running"
            runtime.info.started_at = time.time()
            request = runtime.info.request
            timeout_s = runtime.info.timeout_s
            no_output = runtime.info.no_output
            log_path = runtime.info.log_path

        selected_backend = self._default_backend if backend is None else backend
        execution = execute_job(
            request,
            timeout=timeout_s,
            no_output=no_output,
            heartbeat=heartbeat,
            abort=runtime.abort_event,
            backend=selected_backend,
            log_path=log_path,
        )

        with self._lock:
            runtime2 = self._jobs.get(job_id)
            if runtime2 is None:
                return execution
            runtime2.info.execution = execution
            if runtime2.info.log_path is None and execution.log_path:
                runtime2.info.log_path = execution.log_path
            runtime2.info.finished_at = time.time()
            runtime2.info.state = self._state_from_execution(execution)
        return execution

    @staticmethod
    def _state_from_execution(execution: JobExecution) -> JobState:
        if execution.timed_out:
            return "timed_out"
        if execution.aborted:
            return "aborted"
        if execution.ok:
            return "succeeded"
        return "failed"

    def get(self, job_id: str) -> ManagedJob:
        with self._lock:
            runtime = self._jobs.get(str(job_id))
            if runtime is None:
                raise KeyError("Unknown job id: {!r}".format(job_id))
            return self._clone_info(runtime.info)

    def list(self, *, states: Iterable[JobState] | None = None) -> list[ManagedJob]:
        allowed = set(states) if states is not None else None
        with self._lock:
            snapshots: list[ManagedJob] = []
            for runtime in self._jobs.values():
                if allowed is not None and runtime.info.state not in allowed:
                    continue
                snapshots.append(self._clone_info(runtime.info))
        snapshots.sort(key=lambda one: one.submitted_at)
        return snapshots

    def wait(self, job_id: str, *, timeout: float | None = None) -> ManagedJob:
        future = self._future_for(job_id)
        if future is not None:
            try:
                future.result(timeout=timeout)
            except FutureCancelledError:
                pass
            except FutureTimeoutError:
                pass
        return self.get(job_id)

    def result(
        self,
        job_id: str,
        *,
        timeout: float | None = None,
        raise_on_failure: bool = False,
    ) -> JobExecution:
        info = self.wait(job_id, timeout=timeout)
        execution = info.execution
        if execution is None:
            raise RuntimeError("Job {!r} has no execution payload yet (state={}).".format(job_id, info.state))

        if raise_on_failure and not execution.ok:
            if execution.timed_out:
                raise TimeoutError("Job {!r} timed out.".format(job_id))
            if execution.aborted:
                raise RuntimeError("Job {!r} was aborted.".format(job_id))
            raise RuntimeError(execution.traceback or "Job {!r} failed.".format(job_id))
        return execution

    def cancel(self, job_id: str) -> bool:
        with self._lock:
            runtime = self._jobs.get(str(job_id))
            if runtime is None:
                return False
            if runtime.info.done:
                return False
            runtime.abort_event.set()
            future = runtime.future
            if future is not None and future.cancel():
                runtime.info.state = "cancelled"
                runtime.info.finished_at = time.time()
                runtime.info.execution = JobExecution(ok=False, aborted=True, traceback="Job cancelled before start")
                return True
            return True

    def retry(
        self,
        job_id: str,
        *,
        label: str | None = None,
        allow_succeeded: bool = False,
    ) -> str:
        """Replay one terminal request as a new, linked managed job."""

        original = self.get(job_id)
        if not original.done:
            raise ValueError(
                "Job {!r} is still {}; only terminal jobs can be retried."
                .format(job_id, original.state)
            )
        if original.state == "succeeded" and not allow_succeeded:
            raise ValueError(
                "Job {!r} succeeded; pass allow_succeeded=True to replay it."
                .format(job_id)
            )
        backend: str | None = original.backend_name
        if backend in {None, "", "auto"}:
            backend = None
        return self.submit(
            original.request,
            timeout=original.timeout_s,
            no_output=original.no_output,
            backend=backend,
            label=label if label is not None else original.label,
            retry_of_job_id=original.job_id,
        )

    def shutdown(self, *, wait: bool = True, cancel_pending: bool = False) -> None:
        with self._lock:
            self._closed = True
            job_ids = list(self._jobs.keys())
        if cancel_pending:
            for job_id in job_ids:
                self.cancel(job_id)
        self._executor.shutdown(wait=bool(wait), cancel_futures=bool(cancel_pending))

    def _future_for(self, job_id: str) -> Future[JobExecution] | None:
        with self._lock:
            runtime = self._jobs.get(str(job_id))
            if runtime is None:
                raise KeyError("Unknown job id: {!r}".format(job_id))
            return runtime.future

    @staticmethod
    def _clone_info(info: ManagedJob) -> ManagedJob:
        return ManagedJob(
            job_id=info.job_id,
            request=info.request,
            state=info.state,
            submitted_at=info.submitted_at,
            started_at=info.started_at,
            finished_at=info.finished_at,
            timeout_s=info.timeout_s,
            no_output=info.no_output,
            log_path=info.log_path,
            backend_name=info.backend_name,
            label=info.label,
            retry_of_job_id=info.retry_of_job_id,
            execution=info.execution,
        )


__all__ = [
    "JobState",
    "ManagedJob",
    "JobManagerAPI",
    "InMemoryJobManager",
]
