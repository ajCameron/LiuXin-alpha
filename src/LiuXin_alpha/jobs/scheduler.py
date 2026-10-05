"""
Schedule runnable managed jobs under concurrency and retry rules.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise scheduler through a consuming regression::

        python -m pytest -q tests/jobs/test_jobs_repository.py
"""

from __future__ import annotations

from LiuXin_alpha.jobs.models import JobTriggerKind
from LiuXin_alpha.jobs.repository import JobRepository


class JobScheduler:
    """
    Queue due runs from enabled job definitions.

    Example:
        Exercise JobScheduler through a consuming regression::

            python -m pytest -q tests/jobs/test_jobs_repository.py
    """

    def __init__(self, repository: JobRepository) -> None:
        """
        Initialize and validate the jobscheduler state.

        Example:
            Exercise JobScheduler.  init   through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param repository: Value supplied for repository under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.repository = repository

    def tick(self, *, now_timestamp_ep_k: int | None = None) -> int:
        """
        Perform the tick operation under explicit file-format and conversion rules.

        Example:
            Exercise JobScheduler.tick through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param now_timestamp_ep_k: Value supplied for now timestamp ep k under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
