"""
Check local job-proxy execution and remote named-RPC routing without a live HTTP server.

Local tests own serial job managers and execute real workers against lightweight
library targets. Remote tests replace HTTP boundaries with recording fakes, so
their assertions cover request construction and response handling, not networking.
Embedded worker-source strings are intentional job inputs.
"""

from __future__ import annotations

import json
import time

from dataclasses import dataclass, field
from typing import Any

import pytest

from LiuXin_alpha.core import CoreRuntime
from LiuXin_alpha.core.proxies import (
    JobsProxyABC,
    JobsProxyAPI,
    LocalJobsProxy,
    LocalLibraryProxy,
    RemoteJobsProxy,
    RemoteLibraryProxy,
)
import LiuXin_alpha.core.proxies.remote as remote_proxy_module
from LiuXin_alpha.utils.jobs import JobRequest
from LiuXin_alpha.utils.jobs.manager import InMemoryJobManager


@dataclass
class _FakeDatabase:
    """
    Supply SQLite identity and a per-instance path hint without opening a database.

    Example:
        >>> _FakeDatabase().type
        'SQLite'
    """

    metadata: dict[str, str] = field(
        default_factory=lambda: {"database_path": "/tmp/core_proxy_jobs_phase3.sqlite"}
    )
    type: str = "SQLite"


@dataclass
class _FakeStorage:
    """
    Make an empty storage target discoverable during Core service composition.

    Example:
        >>> _FakeStorage()
        _FakeStorage()
    """

    pass


@dataclass
class _FakeLibrary:
    """
    Hold the fake database and storage targets borrowed by each test runtime.

    Example:
        >>> _FakeLibrary(_FakeDatabase(), _FakeStorage()).database.type
        'SQLite'
    """

    database: _FakeDatabase
    storage: _FakeStorage


def _build_runtime_with_manager(manager: InMemoryJobManager) -> CoreRuntime:
    """
    Compose a phase-3 runtime with fake library targets and a caller-owned job manager.

    Tests close the manager explicitly; the advertised database path is not opened.

    Example:
        >>> runtime = _build_runtime_with_manager(manager)  # doctest: +SKIP


    :param manager: Existing in-memory manager exposed by the runtime's job endpoints.
    :return: Runtime carrying the test-phase3 implementation-version label.
    """
    return CoreRuntime(
        library=_FakeLibrary(database=_FakeDatabase(), storage=_FakeStorage()),
        core_version="test-phase3",
        job_manager=manager,
    )


def test_local_library_proxy_jobs_list_get_wait() -> None:
    """
    Execute a square-root job and verify local proxy listing, identity, label, and successful result preview.

    Example:
        >>> test_local_library_proxy_jobs_list_get_wait()  # doctest: +SKIP


    :return: None if all proxy receipts match the submitted job and manager cleanup completes.
    """
    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    try:
        runtime = _build_runtime_with_manager(manager)
        proxy = LocalLibraryProxy(runtime)

        job_id = manager.submit(
            JobRequest(module_name="math", function_name="sqrt", args=(64,)),
            no_output=True,
            label="sqrt64",
        )

        listed = proxy.jobs.list(limit=20, offset=0)
        listed_jobs = list(listed.get("jobs", ()) or ())
        assert any(str(one.get("job_id", "")) == job_id for one in listed_jobs)

        got = proxy.jobs.get(job_id)
        got_job = dict(got.get("job", {}) or {})
        assert str(got_job.get("job_id", "")) == job_id
        assert str(got_job.get("label", "")) == "sqrt64"

        waited = proxy.jobs.wait(job_id, timeout_s=2.0)
        waited_job = dict(waited.get("job", {}) or {})
        assert str(waited_job.get("state", "")) == "succeeded"
        execution = dict(waited_job.get("execution", {}) or {})
        assert bool(execution.get("ok", False)) is True
        assert "8.0" in str(execution.get("result_preview", ""))
    finally:
        manager.shutdown(wait=True, cancel_pending=True)


def test_jobs_proxy_contract_types_are_explicit() -> None:
    """
    Require local/remote job proxies to satisfy the shared ABC and runtime-checkable protocol.

    Constructing the remote proxy does not contact its example endpoint.

    Example:
        >>> test_jobs_proxy_contract_types_are_explicit()  # doctest: +SKIP


    :return: None after checking subclass and instance contracts and closing the local manager.
    """
    assert issubclass(LocalJobsProxy, JobsProxyABC)
    assert issubclass(RemoteJobsProxy, JobsProxyABC)

    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    try:
        runtime = _build_runtime_with_manager(manager)
        local_proxy = LocalLibraryProxy(runtime)
        assert isinstance(local_proxy.jobs, JobsProxyAPI)
    finally:
        manager.shutdown(wait=True, cancel_pending=True)

    remote_proxy = RemoteLibraryProxy(
        endpoint="http://example.test", timeout_seconds=1.0
    )
    assert isinstance(remote_proxy.jobs, JobsProxyAPI)


def test_local_library_proxy_jobs_cancel() -> None:
    """
    Cancel queued local-proxy work and accept the terminal outcomes allowed by execution races.

    Acknowledgment is required, but the allowed final states include success and
    failure as well as cancellation/abort; this does not prove the job never ran.

    Example:
        >>> test_local_library_proxy_jobs_cancel()  # doctest: +SKIP


    :return: None if cancellation is acknowledged and brief polling observes an allowed state.
    """
    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    try:
        runtime = _build_runtime_with_manager(manager)
        proxy = LocalLibraryProxy(runtime)

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

        cancelled = proxy.jobs.cancel(second)
        assert str(cancelled.get("job_id", "")) == second
        assert bool(cancelled.get("cancelled", False)) is True

        deadline = time.time() + 2.0
        state = str(cancelled.get("state", "") or "")
        while state in {"pending", "running"} and time.time() < deadline:
            state = str(proxy.jobs.get(second).get("job", {}).get("state", "") or "")
            if state in {"pending", "running"}:
                time.sleep(0.05)
        assert state in {"cancelled", "aborted", "succeeded", "failed"}
    finally:
        manager.shutdown(wait=True, cancel_pending=True)


def test_remote_library_proxy_jobs_methods_use_named_rpc(monkeypatch) -> None:
    """
    Record remote list/get/wait/cancel RPCs and check named routes plus state-filter serialization.

    The fake returns canned successful envelopes; no job manager or server runs.

    Example:
        >>> with pytest.MonkeyPatch.context() as patch:
        ...     test_remote_library_proxy_jobs_methods_use_named_rpc(patch)


    :param monkeypatch: Pytest fixture replacing only this job proxy's HTTP helper.
    :return: None if all four calls produce the expected results and inspected request fields.
    """
    proxy = RemoteLibraryProxy(endpoint="http://example.test", timeout_seconds=1.0)
    calls: list[tuple[str, str, dict[str, Any] | None]] = []

    def _fake_http_json(*, method: str, url: str, payload=None):
        """
        Record a job RPC and return the canned envelope selected by its operation name.

        Example:
            >>> response = _fake_http_json(method="POST", url="/rpc/query", payload={"name": "jobs.list"})  # doctest: +SKIP


        :param method: HTTP verb copied into the enclosing test's call log as text.
        :param url: Request URL recorded without contacting it.
        :param payload: Optional request mapping whose name selects one of four supported responses.
        :return: Successful list/get/wait/cancel envelope with fixed job-1 data.
        :raises AssertionError: If the operation name is outside this fake's supported set.
        """
        payload_dict = dict(payload or {})
        calls.append((str(method), str(url), payload_dict))
        name = str(payload_dict.get("name", ""))
        if name == "jobs.list":
            return {
                "ok": True,
                "result": {
                    "jobs": [{"job_id": "job-1", "state": "running"}],
                    "total": 1,
                    "offset": 0,
                    "limit": 10,
                },
            }
        if name == "jobs.get":
            return {
                "ok": True,
                "result": {"job": {"job_id": "job-1", "state": "running"}},
            }
        if name == "jobs.wait":
            return {
                "ok": True,
                "result": {"job": {"job_id": "job-1", "state": "succeeded"}},
            }
        if name == "jobs.cancel":
            return {
                "ok": True,
                "result": {"job_id": "job-1", "cancelled": True, "state": "cancelled"},
            }
        raise AssertionError("Unexpected RPC name: {}".format(name))

    monkeypatch.setattr(proxy.jobs, "_http_json", _fake_http_json)

    listed = proxy.jobs.list(limit=10, offset=0, states={"running"})
    assert int(listed.get("total", 0)) == 1

    got = proxy.jobs.get("job-1")
    assert str(got.get("job", {}).get("job_id", "")) == "job-1"

    waited = proxy.jobs.wait("job-1", timeout_s=5.0)
    assert str(waited.get("job", {}).get("state", "")) == "succeeded"

    cancelled = proxy.jobs.cancel("job-1")
    assert bool(cancelled.get("cancelled", False)) is True

    assert len(calls) == 4
    assert calls[0][0] == "POST"
    assert calls[0][1].endswith("/rpc/query")
    assert str(calls[0][2].get("name", "")) == "jobs.list"
    assert calls[0][2].get("payload", {}).get("states") == ["running"]
    assert calls[3][1].endswith("/rpc/command")
    assert str(calls[3][2].get("name", "")) == "jobs.cancel"


def test_remote_library_proxy_jobs_rejects_blank_job_id() -> None:
    """
    Reject a whitespace-only job ID before attempting a remote get request.

    Example:
        >>> test_remote_library_proxy_jobs_rejects_blank_job_id()


    :return: None if client-side validation raises ValueError for the blank identifier.
    """
    proxy = RemoteLibraryProxy(endpoint="http://example.test", timeout_seconds=1.0)
    with pytest.raises(ValueError):
        proxy.jobs.get("  ")


def test_remote_jobs_proxy_list_serializes_state_sets_for_http(monkeypatch) -> None:
    """
    Inspect an actual urllib request and require sorted JSON state values in the named list payload.

    Only urlopen is replaced, retaining request construction and response decoding.
    Timeout is recorded by the fake but not asserted by this test.

    Example:
        >>> with pytest.MonkeyPatch.context() as patch:
        ...     test_remote_jobs_proxy_list_serializes_state_sets_for_http(patch)


    :param monkeypatch: Pytest fixture replacing urlopen with a request-recording fake.
    :return: None if the decoded result and inspected method, URL, name, and state array match.
    """
    proxy = RemoteLibraryProxy(endpoint="http://example.test", timeout_seconds=1.0)
    observed: dict[str, Any] = {}

    class _FakeResponse:
        """
        Act as a no-resource HTTP response carrying a successful empty job-list envelope.

        Example:
            >>> response = _FakeResponse()  # doctest: +SKIP
            >>> json.loads(response.read())["result"]["jobs"]  # doctest: +SKIP
            []
        """

        def __enter__(self):
            """
            Return this response for the proxy's with statement without acquiring a resource.

            Example:
                >>> response.__enter__() is response  # doctest: +SKIP
                True


            :return: This same fake response instance.
            """
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            """
            Leave the fake context without cleanup or suppression of an active exception.

            Example:
                >>> response.__exit__(None, None, None)  # doctest: +SKIP
                False


            :param exc_type: Active exception type, ignored.
            :param exc: Active exception instance, ignored.
            :param tb: Active traceback, ignored.
            :return: False so exceptions from the with body remain visible.
            """
            del exc_type, exc, tb
            return False

        def read(self) -> bytes:
            """
            Encode the same successful empty-list envelope on every read.

            No stream cursor is maintained; repeated reads return the full body.

            Example:
                >>> json.loads(response.read())["result"]["total"]  # doctest: +SKIP
                0


            :return: UTF-8 JSON bytes containing zero jobs with offset zero and limit ten.
            """
            return json.dumps(
                {
                    "ok": True,
                    "result": {"jobs": [], "total": 0, "offset": 0, "limit": 10},
                }
            ).encode("utf-8")

    def _fake_urlopen(request, timeout=0):
        """
        Capture request metadata and decoded JSON without performing network I/O.

        Example:
            >>> response = _fake_urlopen(request, timeout=1.0)  # doctest: +SKIP


        :param request: urllib request exposing method, full URL, and optional UTF-8 JSON body.
        :param timeout: Timeout value converted to float for the enclosing observation dictionary.
        :return: New fake response with a successful empty-list envelope.
        """
        observed["timeout"] = float(timeout)
        observed["method"] = request.get_method()
        observed["url"] = request.full_url
        observed["body"] = json.loads((request.data or b"{}").decode("utf-8"))
        return _FakeResponse()

    monkeypatch.setattr(remote_proxy_module.urllib.request, "urlopen", _fake_urlopen)

    listed = proxy.jobs.list(limit=10, offset=0, states={"running", "failed"})

    assert int(listed.get("total", -1)) == 0
    assert observed["method"] == "POST"
    assert str(observed["url"]).endswith("/rpc/query")
    assert observed["body"]["name"] == "jobs.list"
    assert observed["body"]["payload"]["states"] == ["failed", "running"]


def test_remote_library_proxy_bootstrap_storage_manager_uses_command_endpoint(
    monkeypatch,
) -> None:
    """
    Require database storage bootstrap to use the generic invoke command route, not the query route.

    The response is canned; the test inspects target/method routing, not real
    bootstrap behavior or the forwarded clear_existing value.

    Example:
        >>> with pytest.MonkeyPatch.context() as patch:
        ...     test_remote_library_proxy_bootstrap_storage_manager_uses_command_endpoint(patch)


    :param monkeypatch: Pytest fixture replacing this database proxy's HTTP helper.
    :return: None if the result and single POST invoke-command request match the assertions.
    """
    proxy = RemoteLibraryProxy(endpoint="http://example.test", timeout_seconds=1.0)
    calls: list[tuple[str, str, dict[str, Any] | None]] = []

    def _fake_http_json(*, method: str, url: str, payload=None):
        """
        Record the bootstrap RPC and return a fixed success receipt without interpreting its payload.

        Example:
            >>> _fake_http_json(method="POST", url="/rpc/command")["result"]  # doctest: +SKIP
            {'bootstrapped': 1}


        :param method: HTTP verb stored as text in the enclosing call list.
        :param url: Request URL recorded without network access.
        :param payload: Optional request mapping shallow-copied into the call record.
        :return: Successful envelope whose result reports one bootstrapped object.
        """
        payload_dict = dict(payload or {})
        calls.append((str(method), str(url), payload_dict))
        return {"ok": True, "result": {"bootstrapped": 1}}

    monkeypatch.setattr(proxy.database, "_http_json", _fake_http_json)

    result = proxy.database.bootstrap_storage_manager(clear_existing=True)

    assert result == {"bootstrapped": 1}
    assert len(calls) == 1
    assert calls[0][0] == "POST"
    assert calls[0][1].endswith("/rpc/command")
    assert calls[0][2]["name"] == "invoke"
    assert calls[0][2]["payload"]["target"] == "database"
    assert calls[0][2]["payload"]["method"] == "bootstrap_storage_manager"
