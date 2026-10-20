"""Adapt the durable ledger to Core's existing job views and utility execution.

Only the selected storage audit is persisted here. Utility jobs retain their
existing IDs and manager. All durable status comes from JobRepository; the
adapter owns a polling worker, not another run-state registry.
"""

from __future__ import annotations

import json
import math
import threading
import time
import uuid
from collections.abc import Callable, Iterable
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from LiuXin_alpha.core.storage_audit_jobs import AUDIT_JOB_KIND, StorageAuditHandler
from LiuXin_alpha.core.wire import to_wire
from LiuXin_alpha.jobs.handler_api import JobHandlerRegistry
from LiuXin_alpha.jobs.models import JobDefinition, JobResultPolicy, JobRun, JobRunState
from LiuXin_alpha.jobs.repository import JobRepository
from LiuXin_alpha.jobs.worker import JobWorker
from LiuXin_alpha.utils.jobs.api import JobBackend, JobExecution, JobRequest
from LiuXin_alpha.utils.jobs.manager import JobManagerAPI, JobState, ManagedJob


@dataclass
class DurableJobView(ManagedJob):
    """Carry ledger evidence alongside the existing managed-job wire view."""

    durable: dict[str, Any] = field(default_factory=dict)


class DurableJobService:
    """Own one library's lazy ledger and worker with independently opened services.

    Construction opens no files or threads. Submission starts polling. Reopening
    and inspection recover expired attempts explicitly; a pending run resumes when
    its delivery is resubmitted or another audit starts the worker. Shutdown stops
    polling without cancelling queued work or preempting an active audit.
    """

    def __init__(
        self,
        *,
        path: str | Path,
        library_uuid: str,
        open_library: Callable[[], AbstractContextManager[Any]],
        lease_for_s: float = 60.0,
    ) -> None:
        if not library_uuid or library_uuid.lower() == "none":
            raise ValueError("Durable work requires a persistent library UUID")
        self.path = Path(path)
        self.library_uuid = library_uuid
        self._handler = StorageAuditHandler(
            library_uuid=library_uuid, open_library=open_library
        )
        self._lease_for_s = lease_for_s
        self._lock = threading.RLock()
        self._repository: JobRepository | None = None
        self._worker: JobWorker | None = None
        self._thread: threading.Thread | None = None
        self._closed = False

    @property
    def repository(self) -> JobRepository:
        """Open and bind the ledger on first use, independently of Core's database."""
        with self._lock:
            if self._repository is None:
                self._check_open()
                repository = JobRepository(self.path)
                repository.bind_owner(f"core.library:{self.library_uuid}")
                self._repository = repository
            return self._repository

    @staticmethod
    def handles(job_id: str) -> bool:
        return job_id.startswith("durable:")

    def _job_id(self, run_id: int | None) -> str:
        return f"durable:{self.repository.ledger_uuid}:{run_id}"

    def _run_id(self, job_id: str) -> int:
        try:
            prefix, ledger, run = job_id.split(":")
            run_id = int(run)
            if prefix != "durable" or run_id < 1:
                raise ValueError
        except (ValueError, TypeError) as error:
            raise KeyError(job_id) from error
        if ledger != self.repository.ledger_uuid:
            raise KeyError(job_id)
        return run_id

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("Durable job service is closed")

    def _start_worker(self) -> None:
        """Start one owned polling thread; failed startup leaves accepted work queued."""
        self._check_open()
        if self._thread is not None and self._thread.is_alive():
            return
        registry = JobHandlerRegistry()
        registry.register(self._handler)
        worker = JobWorker(
            repository=self.repository, handlers=registry, lease_for_s=self._lease_for_s
        )
        thread = threading.Thread(
            target=worker.run_forever,
            kwargs={"poll_interval_s": 0.1},
            name="core-durable-jobs",
            daemon=True,
        )
        thread.start()
        self._worker, self._thread = worker, thread

    def submit_audit(
        self,
        *,
        limit: int,
        offset: int,
        calculate_digests: bool,
        request_key: str,
        label: str = "Storage audit",
    ) -> str:
        """Persist normalized selection intent; the worker selects its page at execution."""
        payload = json.dumps(
            {
                "library_uuid": self.library_uuid,
                "limit": limit,
                "offset": offset,
                "calculate_digests": calculate_digests,
            },
            sort_keys=True,
            allow_nan=False,
        )
        self._handler.validate_payload(payload)
        with self._lock:
            self._check_open()
            run = self.repository.submit_job(
                JobDefinition(
                    job_kind=AUDIT_JOB_KIND,
                    job_name=label,
                    payload_json=payload,
                    result_policy=JobResultPolicy.KEEP_ALL,
                ),
                request_key=request_key,
            )
            self._start_worker()
            return self._job_id(run.job_run_id)

    def _view(self, run: JobRun) -> DurableJobView:
        state = (
            "pending"
            if run.state in (JobRunState.QUEUED, JobRunState.LEASED)
            else run.state.value
        )
        terminal = state not in ("pending", "running")
        execution = None
        if terminal:
            execution = JobExecution(
                ok=run.state is JobRunState.SUCCEEDED,
                result=None if run.result_json is None else json.loads(run.result_json),
                traceback=run.error_text,
                timed_out=run.state is JobRunState.TIMED_OUT,
                aborted=run.state in (JobRunState.ABORTED, JobRunState.CANCELLED),
            )
        return DurableJobView(
            job_id=self._job_id(run.job_run_id),
            request=JobRequest(
                module_name="LiuXin_alpha.core.storage_audit_jobs",
                function_name="run_storage_audit",
            ),
            state=cast(JobState, state),
            label=run.job_name,
            submitted_at=(run.queued_timestamp_ep_k or 0) / 1000,
            started_at=None
            if run.started_timestamp_ep_k is None
            else run.started_timestamp_ep_k / 1000,
            finished_at=None
            if run.finished_timestamp_ep_k is None
            else run.finished_timestamp_ep_k / 1000,
            timeout_s=-1,
            backend_name="durable",
            execution=execution,
            retry_of_job_id=None
            if run.retry_of_job_run_id is None
            else self._job_id(run.retry_of_job_run_id),
            durable={
                "version": 1,
                "ledger_uuid": self.repository.ledger_uuid,
                "run_id": run.job_run_id,
                "state": run.state.value,
                "attempt_number": run.attempt_number,
                "cancel_requested": run.cancel_requested,
                "progress": {
                    "current": run.progress_current,
                    "total": run.progress_total,
                    "unit": run.progress_unit,
                    "message": run.progress_message,
                },
            },
        )

    def get(self, job_id: str) -> DurableJobView:
        run_id = self._run_id(job_id)
        self.repository.recover_expired_runs()
        return self._view(self.repository.get_run(run_id))

    def list(self, *, states: Iterable[JobState] | None = None) -> list[ManagedJob]:
        self.repository.recover_expired_runs()
        selected = None if states is None else set(states)
        jobs = [self._view(run) for run in self.repository.list_runs()]
        return [job for job in jobs if not selected or job.state in selected]

    def wait(self, job_id: str, *, timeout: float | None = None) -> ManagedJob:
        if timeout is not None and (not math.isfinite(timeout) or timeout < 0):
            raise ValueError("Wait timeout must be finite and nonnegative")
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            job = self.get(job_id)
            if job.done or (deadline is not None and time.monotonic() >= deadline):
                return job
            time.sleep(
                0.05
                if deadline is None
                else min(0.05, max(0, deadline - time.monotonic()))
            )

    def result(
        self,
        job_id: str,
        *,
        timeout: float | None = None,
        raise_on_failure: bool = False,
    ) -> JobExecution:
        job = self.wait(job_id, timeout=timeout)
        if not job.done or job.execution is None:
            raise TimeoutError(f"Job {job_id} has not completed")
        if raise_on_failure and not job.execution.ok:
            raise RuntimeError(job.execution.traceback or f"Job ended as {job.state}")
        return job.execution

    def cancel(self, job_id: str) -> bool:
        return self.repository.request_cancel(self._run_id(job_id))

    def retry(
        self,
        job_id: str,
        *,
        request_key: str,
        acknowledge_possible_effects: bool = False,
    ) -> str:
        with self._lock:
            self._check_open()
            run = self.repository.retry_run(
                self._run_id(job_id),
                request_key=request_key,
                acknowledge_possible_effects=acknowledge_possible_effects,
            )
            self._start_worker()
            return self._job_id(run.job_run_id)

    def events(
        self, job_id: str, *, after_event_id: int = 0, limit: int = 100
    ) -> dict[str, Any]:
        """Return a bounded stable cursor including progress and per-replica receipts."""
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("Event limit must be an integer between 1 and 1000")
        if type(after_event_id) is not int or after_event_id < 0:
            raise ValueError("Event cursor must be a nonnegative integer")
        run_id = self._run_id(job_id)
        self.get(job_id)
        page = self.repository.list_events(
            run_id, after_event_id=after_event_id, limit=limit + 1
        )
        selected = page[:limit]
        return {
            "job_id": job_id,
            "events": to_wire(selected),
            "next_cursor": selected[-1].event_id if selected else after_event_id,
            "has_more": len(page) > limit,
        }

    def stop(self, *, wait: bool = False) -> None:
        """Leave queued work durable; active work owns its independent library graph."""
        with self._lock:
            self._closed = True
            if self._worker is not None:
                self._worker.stop()
            thread = self._thread
        if wait and thread is not None:
            thread.join()


class RoutedJobManager(JobManagerAPI):
    """Route namespaced durable IDs while preserving borrowed utility jobs."""

    def __init__(self, utility: JobManagerAPI, durable: DurableJobService) -> None:
        self.utility = utility
        self.durable = durable

    def _owner(self, job_id: str) -> Any:
        return self.durable if self.durable.handles(job_id) else self.utility

    def submit(
        self,
        request: JobRequest,
        *,
        timeout: float = 300,
        no_output: bool = False,
        heartbeat: Callable[[], bool] | None = None,
        backend: str | JobBackend | None = None,
        label: str | None = None,
    ) -> str:
        return self.utility.submit(
            request,
            timeout=timeout,
            no_output=no_output,
            heartbeat=heartbeat,
            backend=backend,
            label=label,
        )

    def get(self, job_id: str) -> ManagedJob:
        return self._owner(job_id).get(job_id)

    def list(self, *, states: Iterable[JobState] | None = None) -> list[ManagedJob]:
        selected = None if states is None else tuple(states)
        return self.utility.list(states=selected) + self.durable.list(states=selected)

    def wait(self, job_id: str, *, timeout: float | None = None) -> ManagedJob:
        return self._owner(job_id).wait(job_id, timeout=timeout)

    def result(
        self,
        job_id: str,
        *,
        timeout: float | None = None,
        raise_on_failure: bool = False,
    ) -> JobExecution:
        return self._owner(job_id).result(
            job_id, timeout=timeout, raise_on_failure=raise_on_failure
        )

    def cancel(self, job_id: str) -> bool:
        return self._owner(job_id).cancel(job_id)

    def retry(
        self, job_id: str, *, label: str | None = None, allow_succeeded: bool = False
    ) -> str:
        if self.durable.handles(job_id):
            if label is not None or allow_succeeded:
                raise ValueError(
                    "Durable retries preserve their label and refuse successful runs"
                )
            return self.durable.retry(job_id, request_key=uuid.uuid4().hex)
        return self.utility.retry(job_id, label=label, allow_succeeded=allow_succeeded)

    def shutdown(self, *, wait: bool = True, cancel_pending: bool = False) -> None:
        try:
            self.durable.stop(wait=wait)
        finally:
            self.utility.shutdown(wait=wait, cancel_pending=cancel_pending)
