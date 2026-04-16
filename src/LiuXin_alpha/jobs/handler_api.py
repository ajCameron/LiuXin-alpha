"""Job handler contracts and registry helpers."""

from __future__ import annotations

import dataclasses

from abc import ABC, abstractmethod
from typing import Any

from LiuXin_alpha.jobs.models import JobProgressUpdate


class JobHandlerAPI(ABC):
    """Runtime handler for one durable job kind."""

    job_kind: str

    @abstractmethod
    def validate_payload(self, payload_json: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def run(self, *, payload_json: str, run_context: "JobRunContext") -> dict[str, Any]:
        raise NotImplementedError


@dataclasses.dataclass(slots=True)
class JobRunContext:
    """Mutable execution context exposed to a running job handler."""

    repository: Any
    job_definition_id: int
    job_run_id: int
    worker_id: str
    started_timestamp_ep_k: int

    def heartbeat(self, message: str | None = None) -> None:
        self.repository.heartbeat(self.job_run_id, worker_id=self.worker_id, message=message)

    def update_progress(self, update: JobProgressUpdate) -> None:
        self.repository.update_progress(self.job_run_id, update)

    def is_cancel_requested(self) -> bool:
        return bool(self.repository.get_run(self.job_run_id).cancel_requested)

    def log(self, message: str, *, event_json: str | None = None) -> None:
        self.repository.append_log(self.job_run_id, message=message, event_json=event_json)


class JobHandlerRegistry:
    """Map job kinds to handler instances."""

    def __init__(self) -> None:
        self._handlers: dict[str, JobHandlerAPI] = {}

    def register(self, handler: JobHandlerAPI) -> None:
        kind = str(getattr(handler, "job_kind", "")).strip()
        if not kind:
            raise ValueError("Handler must declare a non-blank job_kind")
        self._handlers[kind] = handler

    def get(self, job_kind: str) -> JobHandlerAPI:
        kind = str(job_kind).strip()
        if kind not in self._handlers:
            raise KeyError(f"Unknown job kind: {job_kind!r}")
        return self._handlers[kind]

    def knows(self, job_kind: str) -> bool:
        return str(job_kind).strip() in self._handlers

    def job_kinds(self) -> tuple[str, ...]:
        return tuple(sorted(self._handlers))


__all__ = ["JobHandlerAPI", "JobRunContext", "JobHandlerRegistry"]
