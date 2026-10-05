"""
Exercise named Core job inspection, waiting, cancellation, retry, and result/log access with a serial backend.

Lightweight library facades avoid opening a database. Each test owns and shuts
down its in-memory job manager; worker execution and log capture remain real.
Embedded source strings are deliberate job inputs, not imported project modules.
"""

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
    """
    Advertise SQLite identity and a database-path hint without opening or implementing a database.

    Each instance receives its own metadata dictionary, used for Core composition
    and worker payload context rather than filesystem access in these tests.

    Example:
        >>> _FakeDatabase().type
        'SQLite'
    """

    metadata: dict[str, str] = field(
        default_factory=lambda: {"database_path": "/tmp/jobs_phase2.sqlite"}
    )
    type: str = "SQLite"


@dataclass
class _FakeStorage:
    """
    Supply an empty storage target so generic Core service discovery can find one.

    No storage operations are implemented or needed by this job-endpoint suite.

    Example:
        >>> _FakeStorage()
        _FakeStorage()
    """

    pass


@dataclass
class _FakeLibrary:
    """
    Group fake database/storage targets for runtime construction without resource ownership.

    Example:
        >>> _FakeLibrary(_FakeDatabase(), _FakeStorage()).database.type
        'SQLite'
    """

    database: _FakeDatabase
    storage: _FakeStorage


def _build_runtime_with_manager(manager: InMemoryJobManager) -> CoreRuntime:
    """
    Construct a phase-2 runtime around lightweight targets and the caller's job manager.

    The supplied manager remains caller-owned; tests shut it down explicitly in
    finally blocks. The fake database path is descriptive metadata, not opened here.

    Example:
        >>> runtime = _build_runtime_with_manager(manager)  # doctest: +SKIP


    :param manager: Existing in-memory manager to expose through the runtime's named job endpoints.
    :return: Runtime with the fake library and phase-2 implementation-version label.
    """
    library = _FakeLibrary(database=_FakeDatabase(), storage=_FakeStorage())
    return CoreRuntime(library=library, core_version="test-phase2", job_manager=manager)


def test_core_runtime_jobs_list_get_wait_queries() -> None:
    """
    Run a serial square-root job and check list/get/wait envelopes, labels, success state, and result preview.

    Example:
        >>> test_core_runtime_jobs_list_get_wait_queries()  # doctest: +SKIP


    :return: None after assertions pass and the owned manager is shut down with pending work cancelled.
    """
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
    """
    Queue work behind a blocker, request cancellation through Core, and poll briefly for a nonrunning state.

    The final assertion permits cancelled, aborted, succeeded, or failed, reflecting
    cancellation races rather than requiring that the target never executed.

    Example:
        >>> test_core_runtime_jobs_cancel_command()  # doctest: +SKIP


    :return: None if cancellation is acknowledged and the observed state settles within the allowed outcomes.
    """
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
            JobRequest(
                module_name=source,
                function_name="run",
                args=(0.4,),
                module_is_source_code=True,
            ),
            no_output=True,
            label="blocker",
        )
        second = manager.submit(
            JobRequest(
                module_name=source,
                function_name="run",
                args=(0.05,),
                module_is_source_code=True,
            ),
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
            state = runtime.execute_query(
                CoreQuery(name="jobs.get", payload={"job_id": second})
            ).result["job"]["state"]
            if state in {"pending", "running"}:
                time.sleep(0.05)
        assert state in {"cancelled", "aborted", "succeeded", "failed"}
    finally:
        manager.shutdown(wait=True, cancel_pending=True)


def test_core_runtime_jobs_retry_creates_a_linked_run() -> None:
    """
    Retry a finished successful job with explicit permission and verify its new label and original-job linkage.

    The assertions check linkage and labeling, not the retried calculation's value
    or an explicit success-state assertion after the second wait.

    Example:
        >>> test_core_runtime_jobs_retry_creates_a_linked_run()  # doctest: +SKIP


    :return: None if manager snapshots and Core serialization retain the requested retry metadata.
    """
    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    try:
        runtime = _build_runtime_with_manager(manager)
        original_id = manager.submit(
            JobRequest(module_name="math", function_name="sqrt", args=(100,)),
            no_output=True,
            label="sqrt100",
        )
        manager.wait(original_id, timeout=2.0)

        retried = runtime.command(
            "jobs.retry",
            {
                "job_id": original_id,
                "allow_succeeded": True,
                "label": "sqrt100-retry",
            },
        )
        retry_id = str(retried["job_id"])
        finished = manager.wait(retry_id, timeout=2.0)

        assert retried["retry_of_job_id"] == original_id
        assert finished.retry_of_job_id == original_id
        assert finished.label == "sqrt100-retry"
        shown = runtime.query("jobs.get", {"job_id": retry_id})["job"]
        assert shown["retry_of_job_id"] == original_id
    finally:
        manager.shutdown(wait=True, cancel_pending=True)


def test_core_runtime_jobs_expose_result_and_bounded_log_content() -> None:
    """
    Execute a printing job, retrieve its full result, and read its short captured log through a 1,024-byte request.

    The log is shorter than the requested cap, so this verifies content and EOF,
    not truncation behavior for oversized output.

    Example:
        >>> test_core_runtime_jobs_expose_result_and_bounded_log_content()  # doctest: +SKIP


    :return: None if result content, log availability, EOF, and printed text match the assertions.
    """
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
    """
    Require the outer CoreHandlerError when the jobs.get handler cannot find the requested job.

    The test does not inspect the nested dispatch exception or its code despite
    the historical test-name wording.

    Example:
        >>> test_core_runtime_jobs_get_unknown_job_raises_dispatch_error()  # doctest: +SKIP


    :return: None if execution raises the expected handler-boundary exception and manager cleanup completes.
    """
    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    try:
        runtime = _build_runtime_with_manager(manager)
        with pytest.raises(CoreHandlerError):
            runtime.execute_query(
                CoreQuery(name="jobs.get", payload={"job_id": "does-not-exist"})
            )
    finally:
        manager.shutdown(wait=True, cancel_pending=True)


def test_core_runtime_jobs_list_rejects_unknown_state() -> None:
    """
    Reject an unknown state token while preserving dispatch_error classification through the handler wrapper.

    Example:
        >>> test_core_runtime_jobs_list_rejects_unknown_state()  # doctest: +SKIP


    :return: None if the wrapped error retains its stable code and identifies the invalid state token.
    """
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
