"""
Schedule and supervise in-memory thread or process jobs with cancellation and timeouts.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise manager through a consuming regression::

        python -m pytest -q tests/utils/jobs/test_jobs_manager.py
"""

from __future__ import annotations

import builtins
import threading
import time
import traceback
import uuid
from collections.abc import Callable, Iterable
from concurrent.futures import (
    CancelledError as FutureCancelledError,
)
from concurrent.futures import (
    Future,
    ThreadPoolExecutor,
)
from concurrent.futures import (
    TimeoutError as FutureTimeoutError,
)
from contextvars import copy_context
from dataclasses import dataclass, field
from typing import Any, Literal

from LiuXin_alpha.configuration import capture_preferences, use_preferences

from .api import (
    JobBackend,
    JobExecution,
    JobRequest,
    allocate_job_log_path,
    execute_job,
)

JobState = Literal[
    "pending", "running", "succeeded", "failed", "timed_out", "aborted", "cancelled"
]


@dataclass
class ManagedJob:
    """
    Snapshot of one submitted job.

    Example:
        Exercise ManagedJob through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_manager.py
    """

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
        """
        Perform the done utility operation under explicit compatibility rules.

        Example:
            Exercise ManagedJob.done through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.state in {
            "succeeded",
            "failed",
            "timed_out",
            "aborted",
            "cancelled",
        }

    @property
    def duration_s(self) -> float | None:
        """
        Perform the duration s utility operation under explicit compatibility rules.

        Example:
            Exercise ManagedJob.duration s through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.started_at is None:
            return None
        end = self.finished_at if self.finished_at is not None else time.time()
        return max(0.0, float(end - self.started_at))


@dataclass
class _RuntimeJob:
    """
    Carry normalized RuntimeJob data across the Calibre compatibility boundary.

    Example:
        Exercise  RuntimeJob through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_manager.py
    """

    preferences: Any
    info: ManagedJob
    future: Future[JobExecution] | None = None
    abort_event: threading.Event = field(default_factory=threading.Event)


class JobManagerAPI:
    """
    Abstract shape for job manager implementations.

    Implementations must support concurrent calls: Core waits/results can overlap
    status, cancellation and submission. Protect manager state independently and
    never hold its state lock throughout a blocking wait. Core's shared database
    handler lock is not a synchronization mechanism for this interface.

    Example:
        Exercise JobManagerAPI through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_manager.py
    """

    def list_page(
        self,
        *,
        states: Iterable[JobState] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[ManagedJob], int]:
        """Return a bounded window; custom managers may override the list fallback."""
        if (
            type(limit) is not int
            or not 1 <= limit <= 1000
            or type(offset) is not int
            or offset < 0
        ):
            raise ValueError("Job pages require limit 1..1000 and a nonnegative offset")
        jobs = self.list(states=states)
        return jobs[offset : offset + limit], len(jobs)

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
        """
        Submit a callable and capture its concrete application store now.

        Queued tasks retain the submitting selection even when the process default
        changes. Serial execution shares that store; process execution copies its
        values when spawning. Backend/startup errors become terminal failed jobs.

        Example:
            Exercise JobManagerAPI.submit through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param request: Value supplied for request under the utility contract.
        :param timeout: Maximum wait time before the operation fails.
        :param no_output: Value supplied for no output under the utility contract.
        :param heartbeat: Value supplied for heartbeat under the utility contract.
        :param backend: Value supplied for backend under the utility contract.
        :param label: Value supplied for label under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def get(self, job_id: str) -> ManagedJob:
        """
        Perform the get utility operation under explicit compatibility rules.

        Example:
            Exercise JobManagerAPI.get through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param job_id: Value supplied for job id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def list(self, *, states: Iterable[JobState] | None = None) -> list[ManagedJob]:
        """
        Perform the list utility operation under explicit compatibility rules.

        Example:
            Exercise JobManagerAPI.list through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param states: Value supplied for states under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def wait(self, job_id: str, *, timeout: float | None = None) -> ManagedJob:
        """
        Perform the wait utility operation under explicit compatibility rules.

        Example:
            Exercise JobManagerAPI.wait through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param job_id: Value supplied for job id under the utility contract.
        :param timeout: Maximum wait time before the operation fails.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def result(
        self,
        job_id: str,
        *,
        timeout: float | None = None,
        raise_on_failure: bool = False,
    ) -> JobExecution:
        """
        Perform the result utility operation under explicit compatibility rules.

        Example:
            Exercise JobManagerAPI.result through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param job_id: Value supplied for job id under the utility contract.
        :param timeout: Maximum wait time before the operation fails.
        :param raise_on_failure: Value supplied for raise on failure under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def cancel(self, job_id: str) -> bool:
        """
        Request cancellation for the selected job and report whether the request was accepted.

        Example:
            Exercise JobManagerAPI.cancel through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param job_id: Value supplied for job id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def retry(
        self,
        job_id: str,
        *,
        label: str | None = None,
        allow_succeeded: bool = False,
    ) -> str:
        """
        Submit a new run of one completed job without rewriting history.

        Example:
            Exercise JobManagerAPI.retry through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param job_id: Value supplied for job id under the utility contract.
        :param label: Value supplied for label under the utility contract.
        :param allow_succeeded: Value supplied for allow succeeded under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        raise NotImplementedError

    def shutdown(self, *, wait: bool = True, cancel_pending: bool = False) -> None:
        """
        Perform the shutdown utility operation under explicit compatibility rules.

        Example:
            Exercise JobManagerAPI.shutdown through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param wait: Value supplied for wait under the utility contract.
        :param cancel_pending: Value supplied for cancel pending under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


class InMemoryJobManager(JobManagerAPI):
    """
    Thread-safe in-memory job registry and scheduler.

    Example:
        Exercise InMemoryJobManager through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_manager.py
    """

    def __init__(
        self,
        *,
        max_workers: int = 4,
        default_backend: str | JobBackend | None = None,
    ) -> None:
        """
        Initialize and validate the InMemoryJobManager state.

        Example:
            Exercise InMemoryJobManager.  init   through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param max_workers: Value supplied for max workers under the utility contract.
        :param default_backend: Value supplied for default backend under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        self._lock = threading.RLock()
        self._jobs: dict[str, _RuntimeJob] = {}
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, int(max_workers)), thread_name_prefix="liuxin-job"
        )
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
        """
        Submit a callable and capture its concrete application store now.

        The executor and retries retain this store. Serial work shares its values;
        a process worker copies them when spawning, rather than reloading a profile.

        Example:
            Exercise InMemoryJobManager.submit through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param request: Value supplied for request under the utility contract.
        :param timeout: Maximum wait time before the operation fails.
        :param no_output: Value supplied for no output under the utility contract.
        :param heartbeat: Value supplied for heartbeat under the utility contract.
        :param backend: Value supplied for backend under the utility contract.
        :param label: Value supplied for label under the utility contract.
        :param retry_of_job_id: Value supplied for retry of job id under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
            runtime = _RuntimeJob(info=info, preferences=capture_preferences())
            self._jobs[job_id] = runtime
            # Each job captures its submitting Core's selection, including when
            # it waits in the executor queue behind a different profile.
            runtime.future = self._executor.submit(
                copy_context().run,
                self._run_one,
                job_id,
                heartbeat=heartbeat,
                backend=backend,
            )
            return job_id

    def _backend_name(self, backend: str | JobBackend | None) -> str:
        """
        Perform the backend name utility operation under explicit compatibility rules.

        Example:
            Exercise InMemoryJobManager. backend name through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param backend: Value supplied for backend under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the run one utility operation under explicit compatibility rules.

        Example:
            Exercise InMemoryJobManager. run one through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param job_id: Value supplied for job id under the utility contract.
        :param heartbeat: Value supplied for heartbeat under the utility contract.
        :param backend: Value supplied for backend under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        with self._lock:
            runtime = self._jobs.get(job_id)
            if runtime is None:
                return JobExecution(ok=False, aborted=True, traceback="Job not found")
            if runtime.abort_event.is_set():
                runtime.info.state = "cancelled"
                runtime.info.started_at = time.time()
                runtime.info.finished_at = runtime.info.started_at
                exec_result = JobExecution(
                    ok=False, aborted=True, traceback="Job cancelled before start"
                )
                runtime.info.execution = exec_result
                return exec_result

            runtime.info.state = "running"
            runtime.info.started_at = time.time()
            request = runtime.info.request
            timeout_s = runtime.info.timeout_s
            no_output = runtime.info.no_output
            log_path = runtime.info.log_path

        selected_backend = self._default_backend if backend is None else backend
        try:
            with use_preferences(runtime.preferences):
                execution = execute_job(
                    request,
                    timeout=timeout_s,
                    no_output=no_output,
                    heartbeat=heartbeat,
                    abort=runtime.abort_event,
                    backend=selected_backend,
                    log_path=log_path,
                )
        except BaseException:
            # Startup/pickling/backend failures are terminal jobs too. Otherwise
            # the Future fails while the public job remains "running" forever.
            execution = JobExecution(
                ok=False, traceback=traceback.format_exc(), log_path=log_path
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
        """
        Perform the state from execution utility operation under explicit compatibility rules.

        Example:
            Exercise InMemoryJobManager. state from execution through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param execution: Value supplied for execution under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if execution.timed_out:
            return "timed_out"
        if execution.aborted:
            return "aborted"
        if execution.ok:
            return "succeeded"
        return "failed"

    def get(self, job_id: str) -> ManagedJob:
        """
        Perform the get utility operation under explicit compatibility rules.

        Example:
            Exercise InMemoryJobManager.get through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param job_id: Value supplied for job id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        with self._lock:
            runtime = self._jobs.get(str(job_id))
            if runtime is None:
                raise KeyError(f"Unknown job id: {job_id!r}")
            return self._clone_info(runtime.info)

    def list(self, *, states: Iterable[JobState] | None = None) -> list[ManagedJob]:
        """
        Perform the list utility operation under explicit compatibility rules.

        Example:
            Exercise InMemoryJobManager.list through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param states: Value supplied for states under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        allowed = set(states) if states is not None else None
        with self._lock:
            snapshots: list[ManagedJob] = []
            for runtime in self._jobs.values():
                if allowed is not None and runtime.info.state not in allowed:
                    continue
                snapshots.append(self._clone_info(runtime.info))
        snapshots.sort(key=lambda one: one.submitted_at)
        return snapshots

    def list_page(
        self,
        *,
        states: Iterable[JobState] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[builtins.list[ManagedJob], int]:
        """Count existing history but clone payloads only for the requested window."""
        if (
            type(limit) is not int
            or not 1 <= limit <= 1000
            or type(offset) is not int
            or offset < 0
        ):
            raise ValueError("Job pages require limit 1..1000 and a nonnegative offset")
        allowed = None if states is None else set(states)
        with self._lock:
            selected = sorted(
                (
                    runtime.info
                    for runtime in self._jobs.values()
                    if allowed is None or runtime.info.state in allowed
                ),
                key=lambda info: info.submitted_at,
            )
            return [
                self._clone_info(info) for info in selected[offset : offset + limit]
            ], len(selected)

    def wait(self, job_id: str, *, timeout: float | None = None) -> ManagedJob:
        """
        Perform the wait utility operation under explicit compatibility rules.

        Example:
            Exercise InMemoryJobManager.wait through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param job_id: Value supplied for job id under the utility contract.
        :param timeout: Maximum wait time before the operation fails.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the result utility operation under explicit compatibility rules.

        Example:
            Exercise InMemoryJobManager.result through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param job_id: Value supplied for job id under the utility contract.
        :param timeout: Maximum wait time before the operation fails.
        :param raise_on_failure: Value supplied for raise on failure under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        info = self.wait(job_id, timeout=timeout)
        execution = info.execution
        if execution is None:
            raise RuntimeError(
                f"Job {job_id!r} has no execution payload yet (state={info.state})."
            )

        if raise_on_failure and not execution.ok:
            if execution.timed_out:
                raise TimeoutError(f"Job {job_id!r} timed out.")
            if execution.aborted:
                raise RuntimeError(f"Job {job_id!r} was aborted.")
            raise RuntimeError(execution.traceback or f"Job {job_id!r} failed.")
        return execution

    def cancel(self, job_id: str) -> bool:
        """
        Request cancellation for the selected job and report whether the request was accepted.

        Example:
            Exercise InMemoryJobManager.cancel through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param job_id: Value supplied for job id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
                runtime.info.execution = JobExecution(
                    ok=False, aborted=True, traceback="Job cancelled before start"
                )
                return True
            return True

    def retry(
        self,
        job_id: str,
        *,
        label: str | None = None,
        allow_succeeded: bool = False,
    ) -> str:
        """
        Replay a terminal request using its original selected application store.

        The caller's active profile cannot redirect a retry. The store object is
        retained, not a historical snapshot of values or a file reload request.

        Example:
            Exercise InMemoryJobManager.retry through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param job_id: Value supplied for job id under the utility contract.
        :param label: Value supplied for label under the utility contract.
        :param allow_succeeded: Value supplied for allow succeeded under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        original = self.get(job_id)
        if not original.done:
            raise ValueError(
                f"Job {job_id!r} is still {original.state}; only terminal jobs can be retried."
            )
        if original.state == "succeeded" and not allow_succeeded:
            raise ValueError(
                f"Job {job_id!r} succeeded; pass allow_succeeded=True to replay it."
            )
        backend: str | None = original.backend_name
        if backend in {None, "", "auto"}:
            backend = None
        with self._lock:
            selected_preferences = self._jobs[str(job_id)].preferences
        with use_preferences(selected_preferences):
            return self.submit(
                original.request,
                timeout=original.timeout_s,
                no_output=original.no_output,
                backend=backend,
                label=label if label is not None else original.label,
                retry_of_job_id=original.job_id,
            )

    def shutdown(self, *, wait: bool = True, cancel_pending: bool = False) -> None:
        """
        Perform the shutdown utility operation under explicit compatibility rules.

        Example:
            Exercise InMemoryJobManager.shutdown through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param wait: Value supplied for wait under the utility contract.
        :param cancel_pending: Value supplied for cancel pending under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        with self._lock:
            self._closed = True
            job_ids = list(self._jobs.keys())
        if cancel_pending:
            for job_id in job_ids:
                self.cancel(job_id)
        self._executor.shutdown(wait=bool(wait), cancel_futures=bool(cancel_pending))

    def _future_for(self, job_id: str) -> Future[JobExecution] | None:
        """
        Perform the future for utility operation under explicit compatibility rules.

        Example:
            Exercise InMemoryJobManager. future for through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param job_id: Value supplied for job id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        with self._lock:
            runtime = self._jobs.get(str(job_id))
            if runtime is None:
                raise KeyError(f"Unknown job id: {job_id!r}")
            return runtime.future

    @staticmethod
    def _clone_info(info: ManagedJob) -> ManagedJob:
        """
        Perform the clone info utility operation under explicit compatibility rules.

        Example:
            Exercise InMemoryJobManager. clone info through a consuming regression::

                python -m pytest -q tests/utils/jobs/test_jobs_manager.py


        :param info: Value supplied for info under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
