"""
Define the worker-handler protocol for managed jobs.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise handler api through a consuming regression::

        python -m pytest -q tests/jobs/test_jobs_repository.py
"""

from __future__ import annotations

import dataclasses

from abc import ABC, abstractmethod
from typing import Any

from LiuXin_alpha.jobs.models import JobProgressUpdate


class JobHandlerAPI(ABC):
    """
    Runtime handler for one durable job kind.

    Example:
        Exercise JobHandlerAPI through a consuming regression::

            python -m pytest -q tests/jobs/test_jobs_repository.py
    """

    job_kind: str

    @abstractmethod
    def validate_payload(self, payload_json: str) -> None:
        """
        Validate payload under the format's safety and compatibility rules.

        Example:
            Exercise JobHandlerAPI.validate payload through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param payload_json: Value supplied for payload json under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @abstractmethod
    def run(self, *, payload_json: str, run_context: "JobRunContext") -> dict[str, Any]:
        """
        Execute the configured conversion stage and return its primary result.

        Example:
            Exercise JobHandlerAPI.run through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param payload_json: Value supplied for payload json under the utility contract.
        :param run_context: Value supplied for run context under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


@dataclasses.dataclass(slots=True)
class JobRunContext:
    """
    Mutable execution context exposed to a running job handler.

    Example:
        Exercise JobRunContext through a consuming regression::

            python -m pytest -q tests/jobs/test_jobs_repository.py
    """

    repository: Any
    job_definition_id: int
    job_run_id: int
    worker_id: str
    started_timestamp_ep_k: int

    def heartbeat(self, message: str | None = None) -> None:
        """
        Perform the heartbeat operation under explicit file-format and conversion rules.

        Example:
            Exercise JobRunContext.heartbeat through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param message: Value supplied for message under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.repository.heartbeat(self.job_run_id, worker_id=self.worker_id, message=message)

    def update_progress(self, update: JobProgressUpdate) -> None:
        """
        Perform the update progress operation under explicit file-format and conversion rules.

        Example:
            Exercise JobRunContext.update progress through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param update: Value supplied for update under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.repository.update_progress(self.job_run_id, update)

    def is_cancel_requested(self) -> bool:
        """
        Return whether is cancel requested holds for the supplied ebook data.

        Example:
            Exercise JobRunContext.is cancel requested through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :return: True when the documented condition holds; otherwise False.
        """
        return bool(self.repository.get_run(self.job_run_id).cancel_requested)

    def log(self, message: str, *, event_json: str | None = None) -> None:
        """
        Perform the log operation under explicit file-format and conversion rules.

        Example:
            Exercise JobRunContext.log through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param message: Value supplied for message under the utility contract.
        :param event_json: Value supplied for event json under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.repository.append_log(self.job_run_id, message=message, event_json=event_json)


class JobHandlerRegistry:
    """
    Map job kinds to handler instances.

    Example:
        Exercise JobHandlerRegistry through a consuming regression::

            python -m pytest -q tests/jobs/test_jobs_repository.py
    """

    def __init__(self) -> None:
        """
        Initialize and validate the jobhandlerregistry state.

        Example:
            Exercise JobHandlerRegistry.  init   through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :return: None; validated state is stored on the receiving object.
        """
        self._handlers: dict[str, JobHandlerAPI] = {}

    def register(self, handler: JobHandlerAPI) -> None:
        """
        Perform the register operation under explicit file-format and conversion rules.

        Example:
            Exercise JobHandlerRegistry.register through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param handler: Value supplied for handler under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        kind = str(getattr(handler, "job_kind", "")).strip()
        if not kind:
            raise ValueError("Handler must declare a non-blank job_kind")
        self._handlers[kind] = handler

    def get(self, job_kind: str) -> JobHandlerAPI:
        """
        Perform the get operation under explicit file-format and conversion rules.

        Example:
            Exercise JobHandlerRegistry.get through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param job_kind: Value supplied for job kind under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        kind = str(job_kind).strip()
        if kind not in self._handlers:
            raise KeyError(f"Unknown job kind: {job_kind!r}")
        return self._handlers[kind]

    def knows(self, job_kind: str) -> bool:
        """
        Perform the knows operation under explicit file-format and conversion rules.

        Example:
            Exercise JobHandlerRegistry.knows through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param job_kind: Value supplied for job kind under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return str(job_kind).strip() in self._handlers

    def job_kinds(self) -> tuple[str, ...]:
        """
        Perform the job kinds operation under explicit file-format and conversion rules.

        Example:
            Exercise JobHandlerRegistry.job kinds through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return tuple(sorted(self._handlers))


__all__ = ["JobHandlerAPI", "JobRunContext", "JobHandlerRegistry"]
