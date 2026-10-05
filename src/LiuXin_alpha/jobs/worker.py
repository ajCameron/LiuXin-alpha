"""
Execute managed jobs and report their progress and results.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise worker through a consuming regression::

        python -m pytest -q tests/jobs/test_jobs_worker.py
"""

from __future__ import annotations

import json
import socket
import time

from LiuXin_alpha.jobs.handler_api import JobHandlerRegistry, JobRunContext
from LiuXin_alpha.jobs.models import JobProgressUpdate, JobRunState, now_ep_k
from LiuXin_alpha.jobs.repository import JobRepository


class JobWorker:
    """
    Lease queued runs, execute handlers, and persist progress/results.

    Example:
        Exercise JobWorker through a consuming regression::

            python -m pytest -q tests/jobs/test_jobs_worker.py
    """

    def __init__(
        self,
        *,
        repository: JobRepository,
        handlers: JobHandlerRegistry,
        worker_id: str | None = None,
        lease_for_s: float = 60.0,
    ) -> None:
        """
        Initialize and validate the jobworker state.

        Example:
            Exercise JobWorker.  init   through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_worker.py


        :param repository: Value supplied for repository under the utility contract.
        :param handlers: Value supplied for handlers under the utility contract.
        :param worker_id: Value supplied for worker id under the utility contract.
        :param lease_for_s: Value supplied for lease for s under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.repository = repository
        self.handlers = handlers
        self.worker_id = worker_id or f"{socket.gethostname()}:{id(self)}"
        self.lease_for_s = float(lease_for_s)
        self._stop = False

    def stop(self) -> None:
        """
        Perform the stop operation under explicit file-format and conversion rules.

        Example:
            Exercise JobWorker.stop through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_worker.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._stop = True

    def run_once(self) -> bool:
        """
        Perform the run once operation under explicit file-format and conversion rules.

        Example:
            Exercise JobWorker.run once through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_worker.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        leased = self.repository.lease_next_run(worker_id=self.worker_id, lease_for_s=self.lease_for_s)
        if leased is None:
            return False
        self.repository.mark_running(int(leased.job_run_id), worker_id=self.worker_id)
        run = self.repository.get_run(int(leased.job_run_id))
        definition = self.repository.get_definition(int(run.job_definition_id))
        handler = self.handlers.get(definition.job_kind)
        context = JobRunContext(
            repository=self.repository,
            job_definition_id=int(definition.job_definition_id or 0),
            job_run_id=int(run.job_run_id or 0),
            worker_id=self.worker_id,
            started_timestamp_ep_k=now_ep_k(),
        )
        try:
            handler.validate_payload(definition.payload_json)
            context.update_progress(JobProgressUpdate(progress_current=0, progress_total=None, progress_unit="steps", progress_message="Starting"))
            result = handler.run(payload_json=definition.payload_json, run_context=context)
            if context.is_cancel_requested():
                self.repository.mark_cancelled(int(run.job_run_id), error_text="Cancelled by request")
            else:
                self.repository.mark_succeeded(int(run.job_run_id), result_json=json.dumps(result, sort_keys=True, default=str))
        except Exception as exc:
            self.repository.mark_failed(int(run.job_run_id), error_text=str(exc))
        return True

    def run_forever(self, *, poll_interval_s: float = 5.0) -> None:
        """
        Perform the run forever operation under explicit file-format and conversion rules.

        Example:
            Exercise JobWorker.run forever through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_worker.py


        :param poll_interval_s: Value supplied for poll interval s under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        while not self._stop:
            did_work = self.run_once()
            if not did_work:
                time.sleep(max(0.0, float(poll_interval_s)))


__all__ = ["JobWorker"]
