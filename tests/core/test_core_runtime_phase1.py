from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from LiuXin_alpha.core import CoreCommand, CoreQuery, CoreRuntime
from LiuXin_alpha.core.proxies import LocalLibraryProxy
from LiuXin_alpha.interfaces.terminal.commands import sync as sync_command_module
from LiuXin_alpha.utils.jobs.manager import InMemoryJobManager


def test_core_runtime_health_and_invoke_paths(core_runtime_factory: Callable[..., CoreRuntime]) -> None:
    runtime = core_runtime_factory(core_version="test-phase1")

    health = runtime.execute_query(CoreQuery(name="health")).result
    assert health["core_version"] == "test-phase1"
    assert health["shutdown"] is False

    result = runtime.invoke_query(target="library", method="echo", args=("hello",))
    assert result == "echo:hello"

    write_result = runtime.invoke_command(target="database", method="set_value", args=(7,))
    assert write_result == 7
    assert runtime.invoke_query(target="database", method="get_value") == 7


def test_core_runtime_emits_command_lifecycle_events(core_runtime_factory: Callable[..., CoreRuntime]) -> None:
    runtime = core_runtime_factory(core_version="test-phase1")
    events = []
    runtime.subscribe(events.append)

    command = CoreCommand(
        name="invoke",
        payload={
            "target": "database",
            "method": "set_value",
            "args": (11,),
            "kwargs": {},
        },
    )
    result = runtime.execute_command(command)
    assert result.ok is True

    event_types = [event.event_type for event in events]
    assert "command.started" in event_types
    assert "command.finished" in event_types


def test_local_proxy_auto_dispatches_read_and_write(core_runtime_factory: Callable[..., CoreRuntime]) -> None:
    runtime = core_runtime_factory(core_version="test-phase1")
    proxy = LocalLibraryProxy(runtime)

    assert proxy.database.get_value() == 0
    assert proxy.database.set_value(19) == 19
    assert proxy.database.get_value() == 19
    assert proxy.storage.ping() == "pong"
    assert proxy.health()["core_version"] == "test-phase1"
    jobs_payload = proxy.jobs.list(limit=5, offset=0)
    assert "jobs" in jobs_payload


def test_core_runtime_sync_store_start_submits_job(monkeypatch) -> None:
    @dataclass
    class _FakeDatabase:
        value: int = 0

    @dataclass
    class _FakeStorage:
        ping_count: int = 0

    @dataclass
    class _FakeLibrary:
        database: _FakeDatabase
        storage: _FakeStorage

    captured: dict[str, object] = {}

    def _fake_run_sync_store_job(**kwargs):
        captured.update(kwargs)
        return {"ok": True, "inserted_files": 1}

    monkeypatch.setattr(sync_command_module, "run_sync_store_job", _fake_run_sync_store_job)

    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    try:
        library = _FakeLibrary(
            database=_FakeDatabase(value=0),
            storage=_FakeStorage(),
        )
        runtime = CoreRuntime(library=library, core_version="test-phase1", job_manager=manager)
        result = runtime.execute_command(
            CoreCommand(
                name="sync.store.start",
                payload={
                    "sync_kwargs": {
                        "database_path": "/tmp/fake.sqlite",
                        "db_type": "SQLite",
                        "mode": "local",
                        "store_root_uri": "/tmp/store",
                        "store_name": "fake_store",
                        "store_kind": "on_disk_existing_unmanaged_drive",
                        "source_label": "on_disk_unmanaged_import",
                        "ebook_extensions": None,
                        "compute_hash": False,
                        "capture_hashes": False,
                        "follow_symlinks": False,
                        "attach_store_links": False,
                        "refresh_storage_manager": False,
                        "max_http_requests_per_hour": None,
                        "rclone_args": (),
                        "wget_recurse": True,
                        "wget_max_depth": None,
                        "wget_timeout_s": None,
                        "wget_no_parent": True,
                        "wget_span_hosts": False,
                        "wget_respect_robots": True,
                        "wget_user_agent": None,
                        "wget_no_verbose": True,
                        "wget_args": (),
                        "wget_incremental_db_writes": True,
                        "progress_output": False,
                        "progress_every": 100,
                    },
                    "job_backend": "serial",
                    "job_timeout_s": 5.0,
                    "job_no_output": True,
                    "label": "sync:local:1",
                },
            )
        ).result
        job_id = str(result["job_id"])
        info = manager.wait(job_id, timeout=2.0)
        assert info.state == "succeeded"
        assert captured.get("mode") == "local"
        assert captured.get("database_path") == "/tmp/fake.sqlite"
    finally:
        manager.shutdown(wait=True, cancel_pending=True)


def test_core_runtime_sync_store_cancel_reports_unknown_job(core_runtime_factory: Callable[..., CoreRuntime]) -> None:
    runtime = core_runtime_factory(core_version="test-phase1")
    result = runtime.execute_command(
        CoreCommand(
            name="sync.store.cancel",
            payload={"job_id": "no-such-job"},
        )
    ).result
    assert result["job_id"] == "no-such-job"
    assert result["cancelled"] is False
    assert result["state"] == "unknown"
