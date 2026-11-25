"""Keep durable history paging independent of total retained payload volume."""

import pytest

from LiuXin_alpha.jobs.models import JobDefinition, JobRunState
from LiuXin_alpha.jobs.repository import JobRepository


def test_pages_keep_counts_order_and_empty_filters(tmp_path, monkeypatch):
    repository = JobRepository(tmp_path / "jobs.sqlite")
    for index in range(27):
        run = repository.submit_job(
            JobDefinition(job_name=f"Work {index}", job_kind="test", payload_json="{}"),
            request_key=str(index),
        )
        if index % 2:
            repository.request_cancel(run.job_run_id)
    hydrated = []
    original = repository._run_from_row

    def capture(row):
        hydrated.append(row["job_run_id"])
        return original(row)

    monkeypatch.setattr(repository, "_run_from_row", capture)
    rows, total = repository.list_runs_page(
        states=[JobRunState.QUEUED], limit=3, offset=4
    )
    assert total == 14
    assert [row.job_run_id for row in rows] == [9, 11, 13]
    assert hydrated == [9, 11, 13]
    assert repository.list_runs_page(states=[], limit=3) == ([], 0)
    assert repository.list_runs_page(offset=1000) == ([], 27)
    with pytest.raises(ValueError):
        repository.list_runs_page(limit=1001)
