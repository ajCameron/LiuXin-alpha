"""Execute durable claims with renewal, fenced reporting and explicit interruption.

A lease protects ledger transitions, not arbitrary external side effects. Handlers
must cooperate at domain checkpoints; a lost process is never replayed implicitly.
"""

from __future__ import annotations

import json
import socket
import threading
import uuid

from LiuXin_alpha.jobs.handler_api import (
    JobCancelled,
    JobHandlerRegistry,
    JobRunContext,
)
from LiuXin_alpha.jobs.models import JobProgressUpdate, now_ep_k
from LiuXin_alpha.jobs.repository import JobLeaseLost, JobRepository


class JobWorker:
    """Lease one run at a time and persist only results owned by that exact claim."""

    def __init__(
        self,
        *,
        repository: JobRepository,
        handlers: JobHandlerRegistry,
        worker_id: str | None = None,
        lease_for_s: float = 60.0,
    ) -> None:
        self.repository = repository
        self.handlers = handlers
        self.worker_id = worker_id or f"{socket.gethostname()}:{uuid.uuid4().hex}"
        self.lease_for_s = repository._lease_milliseconds(lease_for_s) / 1000
        self._stop = threading.Event()

    def stop(self) -> None:
        """Stop polling after current work; this does not cancel an active handler."""
        self._stop.set()

    def run_once(self) -> bool:
        """Run at most one claim, including durable setup/serialization failures.

        A renewal thread keeps a long handler's claim alive independently of
        progress callbacks. Loss of that claim forbids all subsequent reporting;
        recovery records uncertain effects rather than replaying the handler.
        Cancellation is checked before execution and when accepting the receipt.
        """
        leased = self.repository.lease_next_run(
            worker_id=self.worker_id, lease_for_s=self.lease_for_s
        )
        if leased is None:
            return False
        run_id = int(leased.job_run_id)
        claim = {"worker_id": self.worker_id, "lease_token": str(leased.lease_token)}
        try:
            run = self.repository.mark_running(run_id, **claim)
        except JobLeaseLost:
            # Cancellation or recovery may win between leasing and starting.
            return True
        context = JobRunContext(
            repository=self.repository,
            job_definition_id=run.job_definition_id,
            job_run_id=run_id,
            started_timestamp_ep_k=now_ep_k(),
            lease_for_s=self.lease_for_s,
            **claim,
        )
        renewal_stop = threading.Event()
        lease_lost = threading.Event()

        def renew() -> None:
            while not renewal_stop.wait(self.lease_for_s / 3):
                try:
                    self.repository.heartbeat(
                        run_id,
                        **claim,
                        lease_for_s=self.lease_for_s,
                        message="Worker lease renewal",
                    )
                except Exception:
                    # Fail closed if ownership cannot be established. Recovery
                    # will expose the expired run, even when this process survives.
                    lease_lost.set()
                    return

        renewal = threading.Thread(
            target=renew, name=f"job-lease-{run_id}", daemon=True
        )
        try:
            renewal.start()
            context.raise_if_cancel_requested()
            handler = self.handlers.get(run.job_kind)
            payload = run.payload_json
            if payload is None:
                raise ValueError("Run has no captured payload")
            handler.validate_payload(payload)
            context.raise_if_cancel_requested()
            context.update_progress(
                JobProgressUpdate(
                    progress_current=0,
                    progress_unit="steps",
                    progress_message="Starting",
                )
            )
            result = handler.run(payload_json=payload, run_context=context)
            if lease_lost.is_set():
                raise JobLeaseLost(f"Lease renewal failed for run {run_id}")
            result_json = json.dumps(result, sort_keys=True, allow_nan=False)
            self.repository.mark_succeeded(run_id, **claim, result_json=result_json)
        except JobLeaseLost:
            pass  # The old owner cannot overwrite the current run or its events.
        except JobCancelled as exc:
            self._finish_if_owned(run_id, claim, str(exc), cancelled=True)
        except Exception as exc:
            self._finish_if_owned(run_id, claim, str(exc))
        finally:
            renewal_stop.set()
            if renewal.ident is not None:
                renewal.join()
        return True

    def _finish_if_owned(
        self, run_id: int, claim: dict[str, str], error: str, *, cancelled: bool = False
    ) -> None:
        """Persist handler failure only if this worker still owns the live claim."""
        try:
            finish = (
                self.repository.mark_cancelled
                if cancelled
                else self.repository.mark_failed
            )
            finish(run_id, **claim, error_text=error)
        except JobLeaseLost:
            pass

    def run_forever(self, *, poll_interval_s: float = 5.0) -> None:
        """Poll responsively until stopped; explicit stop does not interrupt work."""
        while not self._stop.is_set():
            if not self.run_once():
                self._stop.wait(max(0.0, float(poll_interval_s)))


__all__ = ["JobWorker"]
