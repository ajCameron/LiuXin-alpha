from __future__ import annotations

import time

from dataclasses import dataclass, field

import pytest

from LiuXin_alpha.core import CoreCommand, CoreQuery, CoreRuntime
from LiuXin_alpha.core.errors import CoreHandlerError
from LiuXin_alpha.utils.jobs import JobRequest
from LiuXin_alpha.utils.jobs.manager import InMemoryJobManager


@dataclass
class _FakeDatabase:
    metadata: dict[str, str] = field(default_factory=lambda: {"database_path": "/tmp/jobs_phase2.sqlite"})
    type: str = "SQLite"


@dataclass
class _FakeStorage:
    pass


@dataclass
class _FakeLibrary:
    database: _FakeDatabase
    storage: _FakeStorage


def _build_runtime_with_manager(manager: InMemoryJobManager) -> CoreRuntime:
    library = _FakeLibrary(database=_FakeDatabase(), storage=_FakeStorage())
    return CoreRuntime(library=library, core_version="test-phase2", job_manager=manager)


def test_core_runtime_jobs_list_get_wait_queries() -> None:
    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    try:
        runtime = _build_runtime_with_manager(manager)
        job_id = manager.submit(
            JobRequest(module_name="math", function_name="sqrt", args=(49,)),
            no_output=True,
            label="sqrt49",
        )

        listed = runtime.execute_query(
            CoreQuery(
                name="jobs.list",
                payload={"limit": 20, "offset": 0},
            )
        ).result
        assert int(listed["total"]) >= 1
        listed_ids = {str(one.get("job_id", "")) for one in listed.get("jobs", ())}
        assert job_id in listed_ids

        got = runtime.execute_query(
            CoreQuery(
                name="jobs.get",
                payload={"job_id": job_id},
            )
        ).result["job"]
        assert str(got["job_id"]) == job_id
        assert str(got["label"]) == "sqrt49"

        waited = runtime.execute_query(
            CoreQuery(
                name="jobs.wait",
                payload={"job_id": job_id, "timeout_s": 2.0},
            )
        ).result["job"]
        assert str(waited["state"]) == "succeeded"
        execution = dict(waited.get("execution") or {})
        assert bool(execution.get("ok", False)) is True
        assert "7.0" in str(execution.get("result_preview", ""))
    finally:
        manager.shutdown(wait=True, cancel_pending=True)


def test_core_runtime_jobs_cancel_command() -> None:
    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    try:
        runtime = _build_runtime_with_manager(manager)
        source = """
import time

def run(seconds):
    time.sleep(seconds)
    return seconds
"""
        _first = manager.submit(
            JobRequest(module_name=source, function_name="run", args=(0.4,), module_is_source_code=True),
            no_output=True,
            label="blocker",
        )
        second = manager.submit(
            JobRequest(module_name=source, function_name="run", args=(0.05,), module_is_source_code=True),
            no_output=True,
            label="to-cancel",
        )

        cancelled = runtime.execute_command(
            CoreCommand(
                name="jobs.cancel",
                payload={"job_id": second},
            )
        ).result
        assert str(cancelled["job_id"]) == second
        assert bool(cancelled["cancelled"]) is True

        # Allow state transition to settle before asserting.
        deadline = time.time() + 2.0
        state = str(cancelled.get("state", "") or "")
        while state in {"pending", "running"} and time.time() < deadline:
            state = runtime.execute_query(CoreQuery(name="jobs.get", payload={"job_id": second})).result["job"]["state"]
            if state in {"pending", "running"}:
                time.sleep(0.05)
        assert state in {"cancelled", "aborted", "succeeded", "failed"}
    finally:
        manager.shutdown(wait=True, cancel_pending=True)


def test_core_runtime_jobs_expose_result_and_bounded_log_content() -> None:
    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    try:
        runtime = _build_runtime_with_manager(manager)
        source = """
def run():
    print("core-program-job-log")
    return {"answer": 42}
"""
        job_id = manager.submit(
            JobRequest(
                module_name=source,
                function_name="run",
                module_is_source_code=True,
            ),
            no_output=False,
            label="result-and-log",
        )
        result = runtime.query(
            "jobs.result",
            {"job_id": job_id, "timeout_s": 2.0},
        )
        assert result["execution"]["ok"] is True
        assert result["execution"]["result"] == {"answer": 42}

        log = runtime.query(
            "jobs.log.read",
            {"job_id": job_id, "offset": 0, "max_bytes": 1024},
        )
        assert log["available"] is True
        assert log["eof"] is True
        assert "core-program-job-log" in log["text"]
    finally:
        manager.shutdown(wait=True, cancel_pending=True)


def test_core_runtime_jobs_get_unknown_job_raises_dispatch_error() -> None:
    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    try:
        runtime = _build_runtime_with_manager(manager)
        with pytest.raises(CoreHandlerError):
            runtime.execute_query(CoreQuery(name="jobs.get", payload={"job_id": "does-not-exist"}))
    finally:
        manager.shutdown(wait=True, cancel_pending=True)


def test_core_runtime_jobs_list_rejects_unknown_state() -> None:
    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    try:
        runtime = _build_runtime_with_manager(manager)
        with pytest.raises(CoreHandlerError) as caught:
            runtime.execute_query(
                CoreQuery(
                    name="jobs.list",
                    payload={"states": ["running", "not-a-state"]},
                )
            )
        assert caught.value.code == "dispatch_error"
        assert "not-a-state" in str(caught.value)
    finally:
        manager.shutdown(wait=True, cancel_pending=True)
