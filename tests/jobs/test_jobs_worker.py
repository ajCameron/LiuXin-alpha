"""
Provide test jobs worker utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test jobs worker through a consuming regression::

        python -m pytest -q tests/jobs/test_jobs_worker.py
"""
from __future__ import annotations

import dataclasses
import json

from typing import Any

from LiuXin_alpha.jobs.api import JobDefinition, JobHandlerAPI, JobHandlerRegistry, JobProgressUpdate, JobRepository, JobWorker


class _FakeHandler(JobHandlerAPI):
    """
    Provide the fakehandler contract for validated ebook processing.

    Example:
        Exercise  FakeHandler through a consuming regression::

            python -m pytest -q tests/jobs/test_jobs_worker.py
    """
    job_kind = "fake"

    def validate_payload(self, payload_json: str) -> None:
        """
        Validate payload under the format's safety and compatibility rules.

        Example:
            Exercise  FakeHandler.validate payload through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_worker.py


        :param payload_json: Value supplied for payload json under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        loaded = json.loads(payload_json)
        if "value" not in loaded:
            raise ValueError("Missing value")

    def run(self, *, payload_json: str, run_context) -> dict[str, Any]:
        """
        Execute the configured conversion stage and return its primary result.

        Example:
            Exercise  FakeHandler.run through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_worker.py


        :param payload_json: Value supplied for payload json under the utility contract.
        :param run_context: Value supplied for run context under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        loaded = json.loads(payload_json)
        run_context.log("handler starting")
        run_context.update_progress(JobProgressUpdate(progress_current=1, progress_total=1, progress_message="done"))
        return {"echo": loaded["value"]}


class _CancellableHandler(JobHandlerAPI):
    """
    Provide the cancellablehandler contract for validated ebook processing.

    Example:
        Exercise  CancellableHandler through a consuming regression::

            python -m pytest -q tests/jobs/test_jobs_worker.py
    """
    job_kind = "cancellable"

    def validate_payload(self, payload_json: str) -> None:
        """
        Validate payload under the format's safety and compatibility rules.

        Example:
            Exercise  CancellableHandler.validate payload through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_worker.py


        :param payload_json: Value supplied for payload json under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        json.loads(payload_json)

    def run(self, *, payload_json: str, run_context) -> dict[str, Any]:
        """
        Execute the configured conversion stage and return its primary result.

        Example:
            Exercise  CancellableHandler.run through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_worker.py


        :param payload_json: Value supplied for payload json under the utility contract.
        :param run_context: Value supplied for run context under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        del payload_json
        run_context.repository.request_cancel(run_context.job_run_id)
        if run_context.is_cancel_requested():
            return {"cancelled": True}
        raise AssertionError("Expected cancellation to be visible")


def test_job_worker_executes_handler_and_persists_result(tmp_path) -> None:
    """
    Perform the test job worker executes handler and persists result operation under explicit file-format and conversion rules.

    Example:
        Exercise test job worker executes handler and persists result through a consuming regression::

            python -m pytest -q tests/jobs/test_jobs_worker.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    repo = JobRepository(tmp_path / "jobs.sqlite")
    registry = JobHandlerRegistry()
    registry.register(_FakeHandler())
    worker = JobWorker(repository=repo, handlers=registry, worker_id="worker-1")

    definition = repo.create_definition(JobDefinition(job_kind="fake", job_name="Fake Job", payload_json=json.dumps({"value": 42})))
    queued = repo.enqueue_run(job_definition_id=int(definition.job_definition_id))

    assert worker.run_once() is True
    run = repo.get_run(int(queued.job_run_id))
    assert run.state.value == "succeeded"
    assert json.loads(str(run.result_json or "{}")) == {"echo": 42}
    events = repo.list_events(int(queued.job_run_id))
    assert any((one.event_message or "") == "handler starting" for one in events)


def test_job_worker_marks_cancelled_when_requested(tmp_path) -> None:
    """
    Perform the test job worker marks cancelled when requested operation under explicit file-format and conversion rules.

    Example:
        Exercise test job worker marks cancelled when requested through a consuming regression::

            python -m pytest -q tests/jobs/test_jobs_worker.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    repo = JobRepository(tmp_path / "jobs.sqlite")
    registry = JobHandlerRegistry()
    registry.register(_CancellableHandler())
    worker = JobWorker(repository=repo, handlers=registry, worker_id="worker-2")

    definition = repo.create_definition(JobDefinition(job_kind="cancellable", job_name="Cancel Job", payload_json="{}"))
    queued = repo.enqueue_run(job_definition_id=int(definition.job_definition_id))

    assert worker.run_once() is True
    run = repo.get_run(int(queued.job_run_id))
    assert run.state.value == "cancelled"
