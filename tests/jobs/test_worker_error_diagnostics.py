"""A broken handler diagnostic must not strand a claim or stop later jobs."""

import pytest

from LiuXin_alpha.jobs.api import (
    JobDefinition,
    JobHandlerAPI,
    JobHandlerRegistry,
    JobRepository,
    JobWorker,
)


class BrokenText(Exception):
    def __str__(self):
        raise RuntimeError("formatter failed")


@pytest.mark.parametrize(
    "error", [BrokenText(), ValueError("bad\x00text\ud800"), ValueError("x" * 5000)]
)
def test_worker_records_bounded_printable_failure_and_continues(tmp_path, error):
    class Handler(JobHandlerAPI):
        job_kind = "diagnostic-test"

        def validate_payload(self, payload_json):
            pass

        def run(self, *, payload_json, run_context):
            if payload_json == '{"fail":true}':
                raise error
            return {"ok": True}

    repository = JobRepository(tmp_path / "jobs.sqlite")
    runs = [
        repository.submit_job(
            JobDefinition(
                job_kind=Handler.job_kind, job_name="Diagnostic", payload_json=payload
            ),
            request_key=payload,
        )
        for payload in ('{"fail":true}', "{}")
    ]
    handlers = JobHandlerRegistry()
    handlers.register(Handler())
    worker = JobWorker(repository=repository, handlers=handlers)
    assert worker.run_once()
    assert worker.run_once()
    failed, succeeded = [repository.get_run(run.job_run_id) for run in runs]
    assert failed.state.value == "failed"
    assert succeeded.state.value == "succeeded"
    assert 0 < len(failed.error_text) <= 2000
    assert "\x00" not in failed.error_text
    failed.error_text.encode("utf-8", "strict")
    if isinstance(error, BrokenText):
        assert "BrokenText" in failed.error_text
