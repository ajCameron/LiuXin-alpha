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
    metadata: dict[str, str] = field(default_factory=lambda: {"database_path": "/tmp/core_proxy_jobs_phase3.sqlite"})
    type: str = "SQLite"


@dataclass
class _FakeStorage:
    pass


@dataclass
class _FakeLibrary:
    database: _FakeDatabase
    storage: _FakeStorage


def _build_runtime_with_manager(manager: InMemoryJobManager) -> CoreRuntime:
    return CoreRuntime(
        library=_FakeLibrary(database=_FakeDatabase(), storage=_FakeStorage()),
        core_version="test-phase3",
        job_manager=manager,
    )


def test_local_library_proxy_jobs_list_get_wait() -> None:
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
    assert issubclass(LocalJobsProxy, JobsProxyABC)
    assert issubclass(RemoteJobsProxy, JobsProxyABC)

    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    try:
        runtime = _build_runtime_with_manager(manager)
        local_proxy = LocalLibraryProxy(runtime)
        assert isinstance(local_proxy.jobs, JobsProxyAPI)
    finally:
        manager.shutdown(wait=True, cancel_pending=True)

    remote_proxy = RemoteLibraryProxy(endpoint="http://example.test", timeout_seconds=1.0)
    assert isinstance(remote_proxy.jobs, JobsProxyAPI)


def test_local_library_proxy_jobs_cancel() -> None:
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
            JobRequest(module_name=source, function_name="run", args=(0.4,), module_is_source_code=True),
            no_output=True,
            label="blocker",
        )
        second = manager.submit(
            JobRequest(module_name=source, function_name="run", args=(0.05,), module_is_source_code=True),
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
    proxy = RemoteLibraryProxy(endpoint="http://example.test", timeout_seconds=1.0)
    calls: list[tuple[str, str, dict[str, Any] | None]] = []

    def _fake_http_json(*, method: str, url: str, payload=None):
        payload_dict = dict(payload or {})
        calls.append((str(method), str(url), payload_dict))
        name = str(payload_dict.get("name", ""))
        if name == "jobs.list":
            return {
                "ok": True,
                "result": {"jobs": [{"job_id": "job-1", "state": "running"}], "total": 1, "offset": 0, "limit": 10},
            }
        if name == "jobs.get":
            return {"ok": True, "result": {"job": {"job_id": "job-1", "state": "running"}}}
        if name == "jobs.wait":
            return {"ok": True, "result": {"job": {"job_id": "job-1", "state": "succeeded"}}}
        if name == "jobs.cancel":
            return {"ok": True, "result": {"job_id": "job-1", "cancelled": True, "state": "cancelled"}}
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
    proxy = RemoteLibraryProxy(endpoint="http://example.test", timeout_seconds=1.0)
    with pytest.raises(ValueError):
        proxy.jobs.get("  ")


def test_remote_jobs_proxy_list_serializes_state_sets_for_http(monkeypatch) -> None:
    proxy = RemoteLibraryProxy(endpoint="http://example.test", timeout_seconds=1.0)
    observed: dict[str, Any] = {}

    class _FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            del exc_type, exc, tb
            return False

        def read(self) -> bytes:
            return json.dumps({"ok": True, "result": {"jobs": [], "total": 0, "offset": 0, "limit": 10}}).encode(
                "utf-8"
            )

    def _fake_urlopen(request, timeout=0):
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


def test_remote_library_proxy_bootstrap_storage_manager_uses_command_endpoint(monkeypatch) -> None:
    proxy = RemoteLibraryProxy(endpoint="http://example.test", timeout_seconds=1.0)
    calls: list[tuple[str, str, dict[str, Any] | None]] = []

    def _fake_http_json(*, method: str, url: str, payload=None):
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
