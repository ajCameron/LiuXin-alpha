"""Exercise captured cooperative deadlines without treating timeout as rollback."""

import json
from dataclasses import replace

import pytest

from LiuXin_alpha.jobs.handler_api import JobHandlerAPI, JobHandlerRegistry
from LiuXin_alpha.jobs.models import JobDefinition, JobRunState
from LiuXin_alpha.jobs.repository import JobRepository
from LiuXin_alpha.jobs.worker import JobWorker


class DeadlineHandler(JobHandlerAPI):
    job_kind = "deadline-test"

    def validate_payload(self, payload_json):
        pass

    def run(self, *, payload_json, run_context):
        # Inject elapsed time at a real executor checkpoint, not a slow sleep.
        assert run_context.deadline_monotonic is not None
        run_context.deadline_monotonic = 0
        run_context.raise_if_cancel_requested()
        raise AssertionError("deadline allowed a later effect")


def test_timeout_is_captured_retained_on_retry_and_acknowledged(tmp_path):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    definition = repository.create_definition(
        JobDefinition(
            job_kind="deadline-test",
            job_name="Timed action",
            payload_json="{}",
            timeout_s=12,
        )
    )
    run = repository.enqueue_run(job_definition_id=definition.job_definition_id)
    repository.update_definition(replace(definition, timeout_s=500))
    registry = JobHandlerRegistry()
    registry.register(DeadlineHandler())
    worker = JobWorker(repository=repository, handlers=registry)
    assert worker.run_once()
    ended = repository.get_run(run.job_run_id)
    assert ended.state is JobRunState.TIMED_OUT
    assert ended.timeout_s == 12
    assert "prior effects" in ended.error_text
    retry = repository.retry_run(
        run.job_run_id, request_key="explicit-retry", acknowledge_possible_effects=True
    )
    assert retry.timeout_s == 12
    assert repository.latest_attempt(run.job_run_id).job_run_id == retry.job_run_id


@pytest.mark.parametrize("expired", [False, True])
@pytest.mark.parametrize("cancelled", [False, True])
def test_handler_return_checks_deadline_and_retains_completed_effects(
    tmp_path, expired, cancelled
):
    path = tmp_path / "jobs.sqlite"
    repository = JobRepository(path)
    published = tmp_path / "published"

    class ReturningHandler(DeadlineHandler):
        def run(self, *, payload_json, run_context):
            published.write_bytes(b"completed effect")
            if expired:
                run_context.deadline_monotonic = 0
            if cancelled:
                repository.cancel_action(run_context.job_run_id)
            # No explicit checkpoint after the final external operation.
            return {"published": str(published)}

    run = repository.submit_job(
        JobDefinition(
            job_kind="deadline-test",
            job_name="Return after effect",
            payload_json="{}",
            timeout_s=12,
        ),
        request_key="return-boundary",
    )
    registry = JobHandlerRegistry()
    registry.register(ReturningHandler())
    assert JobWorker(repository=repository, handlers=registry).run_once()
    ended = JobRepository(path).get_run(run.job_run_id)
    expected = (
        JobRunState.TIMED_OUT
        if expired
        else JobRunState.CANCELLED
        if cancelled
        else JobRunState.SUCCEEDED
    )
    assert ended.state is expected
    assert json.loads(ended.result_json) == {"published": str(published)}
    assert published.read_bytes() == b"completed effect"
