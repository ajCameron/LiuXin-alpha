"""Simple scheduler for materialising interval-based job runs."""

from __future__ import annotations

from LiuXin_alpha.jobs.models import JobTriggerKind
from LiuXin_alpha.jobs.repository import JobRepository


class JobScheduler:
    """Queue due runs from enabled job definitions."""

    def __init__(self, repository: JobRepository) -> None:
        self.repository = repository

    def tick(self, *, now_timestamp_ep_k: int | None = None) -> int:
        due = self.repository.get_due_definitions_for_scheduling(now_timestamp_ep_k=now_timestamp_ep_k)
        queued = 0
        for definition in due:
            try:
                self.repository.enqueue_run(
                    job_definition_id=int(definition.job_definition_id or 0),
                    trigger_kind=JobTriggerKind.SCHEDULED,
                    not_before_timestamp_ep_k=now_timestamp_ep_k,
                )
            except RuntimeError:
                continue
            queued += 1
        return queued


__all__ = ["JobScheduler"]
