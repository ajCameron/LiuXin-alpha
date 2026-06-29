from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pytest

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.core import CoreCommand, CoreQuery, CoreRuntime
from LiuXin_alpha.core.proxies import LocalLibraryProxy
from LiuXin_alpha.core.proxies.local import looks_like_write_method
from LiuXin_alpha.library.library import Library
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
    assert "write.completed" in event_types
    assert "command.finished" in event_types
    write_events = [event for event in events if event.event_type == "write.completed"]
    assert len(write_events) == 1
    assert write_events[0].payload["target"] == "database"
    assert write_events[0].payload["method"] == "set_value"
    assert write_events[0].payload["command_id"] == command.command_id


def test_core_runtime_api_describe_exposes_named_handlers_and_targets(
    core_runtime_factory: Callable[..., CoreRuntime],
) -> None:
    runtime = core_runtime_factory(core_version="test-phase1")

    described = runtime.describe_api()

    assert described["core_version"] == "test-phase1"
    command_names = {entry["name"] for entry in described["commands"]}
    query_names = {entry["name"] for entry in described["queries"]}
    assert "invoke" in command_names
    assert "sync.store.start" in command_names
    assert "health" in query_names
    assert "api.describe" in query_names

    targets = {entry["name"]: entry for entry in described["targets"]}
    assert set(targets) >= {"library", "database", "storage"}

    library_methods = {method["name"]: method for method in targets["library"]["methods"]}
    database_methods = {method["name"]: method for method in targets["database"]["methods"]}
    storage_methods = {method["name"]: method for method in targets["storage"]["methods"]}

    assert library_methods["echo"]["write"] is False
    assert library_methods["echo"]["parameters"][0]["name"] == "text"
    assert database_methods["set_value"]["write"] is True
    assert database_methods["get_value"]["write"] is False
    assert storage_methods["ping"]["write"] is False

    db_only = runtime.execute_query(CoreQuery(name="api.describe", payload={"target": "db"})).result
    assert [entry["name"] for entry in db_only["targets"]] == ["database"]


def test_local_proxy_auto_dispatches_read_and_write(core_runtime_factory: Callable[..., CoreRuntime]) -> None:
    runtime = core_runtime_factory(core_version="test-phase1")
    proxy = LocalLibraryProxy(runtime)

    assert proxy.database.get_value() == 0
    assert proxy.database.set_value(19) == 19
    assert proxy.database.get_value() == 19
    assert proxy.storage.ping() == "pong"
    assert proxy.health()["core_version"] == "test-phase1"
    assert proxy.describe_api(target="library")["targets"][0]["name"] == "library"
    jobs_payload = proxy.jobs.list(limit=5, offset=0)
    assert "jobs" in jobs_payload


def test_local_proxy_bootstrap_storage_manager_routes_via_command() -> None:
    @dataclass
    class _FakeDatabase:
        bootstrapped: int = 0

        def bootstrap_storage_manager(self, *, clear_existing: bool = False):
            self.bootstrapped += 1
            return {"bootstrapped": self.bootstrapped, "clear_existing": bool(clear_existing)}

    @dataclass
    class _FakeStorage:
        pass

    @dataclass
    class _FakeLibrary:
        database: _FakeDatabase
        storage: _FakeStorage

    runtime = CoreRuntime(
        library=_FakeLibrary(database=_FakeDatabase(), storage=_FakeStorage()),
        core_version="test-phase1",
    )
    events = []
    runtime.subscribe(events.append)
    proxy = LocalLibraryProxy(runtime)

    result = proxy.database.bootstrap_storage_manager(clear_existing=True)

    assert looks_like_write_method("bootstrap_storage_manager") is True
    assert result["bootstrapped"] == 1
    assert result["clear_existing"] is True
    event_types = [event.event_type for event in events]
    assert "command.started" in event_types
    assert "command.finished" in event_types


def test_core_runtime_library_store_save_and_lookup_round_trip(tmp_path) -> None:
    db_path = tmp_path / "core_runtime_store_save.sqlite"
    store_root = tmp_path / "runtime-store"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type="SQLite",
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        runtime = CoreRuntime(
            library=Library(database=db, close_database_on_close=False),
            core_version="test-phase1",
        )
        saved = runtime.invoke_command(
            target="library",
            method="save_store_row",
            kwargs={
                "store_payload": {
                    "store_name": "runtime-store",
                    "store_kind": "on_disk_existing_managed_drive",
                    "store_access_protocol": "file",
                    "store_root_uri": str(store_root.resolve()),
                    "store_is_read_only": 0,
                    "store_online_status": "online",
                    "store_supports_folders": 1,
                    "store_supports_hierarchical_list": 1,
                    "store_supports_random_read": 1,
                    "store_supports_random_write": 1,
                    "store_supports_delete": 1,
                    "store_supports_checksums": 1,
                    "store_supports_immutable_objects": 0,
                    "store_modified_timestamp_ep_k": 123,
                    "store_created_timestamp_ep_k": 123,
                }
            },
        )

        assert saved["store_name"] == "runtime-store"
        found = runtime.invoke_query(
            target="library",
            method="find_existing_store",
            kwargs={
                "root_uri": str(store_root.resolve()),
                "store_name": "runtime-store",
            },
        )
        assert found is not None
        assert int(found["store_id"]) == int(saved["store_id"])
        assert found["store_kind"] == "on_disk_existing_managed_drive"


def test_core_runtime_library_row_get_and_update_round_trip(tmp_path) -> None:
    db_path = tmp_path / "core_runtime_row_update.sqlite"
    store_root = tmp_path / "runtime-row-store"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type="SQLite",
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        runtime = CoreRuntime(
            library=Library(database=db, close_database_on_close=False),
            core_version="test-phase1",
        )
        saved = runtime.invoke_command(
            target="library",
            method="save_store_row",
            kwargs={
                "store_payload": {
                    "store_name": "runtime-row-store",
                    "store_kind": "on_disk_existing_managed_drive",
                    "store_access_protocol": "file",
                    "store_root_uri": str(store_root.resolve()),
                    "store_is_read_only": 0,
                    "store_online_status": "online",
                }
            },
        )

        row_id = int(saved["store_id"])
        fetched = runtime.invoke_query(
            target="library",
            method="get_row",
            kwargs={"table": "stores", "row_id": row_id},
        )
        assert fetched is not None
        assert fetched["store_name"] == "runtime-row-store"

        updated = runtime.invoke_command(
            target="library",
            method="update_row_fields",
            kwargs={
                "table": "stores",
                "row_id": row_id,
                "updates": {
                    "store_name": "runtime-row-store-updated",
                    "store_online_status": "offline",
                },
            },
        )
        assert updated["store_name"] == "runtime-row-store-updated"
        assert updated["store_online_status"] == "offline"


def test_core_runtime_library_delete_row_round_trip(tmp_path) -> None:
    db_path = tmp_path / "core_runtime_row_delete.sqlite"
    store_root = tmp_path / "runtime-delete-store"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type="SQLite",
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        runtime = CoreRuntime(
            library=Library(database=db, close_database_on_close=False),
            core_version="test-phase1",
        )
        saved = runtime.invoke_command(
            target="library",
            method="save_store_row",
            kwargs={
                "store_payload": {
                    "store_name": "runtime-delete-store",
                    "store_kind": "on_disk_existing_managed_drive",
                    "store_access_protocol": "file",
                    "store_root_uri": str(store_root.resolve()),
                    "store_is_read_only": 0,
                    "store_online_status": "online",
                }
            },
        )

        row_id = int(saved["store_id"])
        deleted = runtime.invoke_command(
            target="library",
            method="delete_row",
            kwargs={"table": "stores", "row_id": row_id},
        )
        assert deleted["store_name"] == "runtime-delete-store"
        assert db.get_row_from_id("stores", row_id) is None


def test_core_runtime_library_delete_impact_reports_direct_references(tmp_path) -> None:
    db_path = tmp_path / "core_runtime_delete_impact.sqlite"
    store_root = tmp_path / "runtime-impact-store"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type="SQLite",
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        runtime = CoreRuntime(
            library=Library(database=db, close_database_on_close=False),
            core_version="test-phase1",
        )
        saved = runtime.invoke_command(
            target="library",
            method="save_store_row",
            kwargs={
                "store_payload": {
                    "store_name": "runtime-impact-store",
                    "store_kind": "on_disk_existing_managed_drive",
                    "store_access_protocol": "file",
                    "store_root_uri": str(store_root.resolve()),
                    "store_is_read_only": 0,
                    "store_online_status": "online",
                }
            },
        )
        row_id = int(saved["store_id"])
        folder = db.get_blank_row("folders")
        folder["folder_store_id"] = row_id
        folder["folder_name"] = "root"
        folder["folder_relpath"] = "root"
        folder.sync()

        impact = runtime.invoke_query(
            target="library",
            method="describe_row_delete_impact",
            kwargs={"table": "stores", "row_id": row_id},
        )

        assert impact["table"] == "stores"
        assert impact["row_id"] == row_id
        assert impact["reference_total"] >= 1
        assert any(
            item["table"] == "folders" and item["column"] == "folder_store_id" and int(item["count"]) >= 1
            for item in impact["reference_counts"]
        )
        assert any(
            item["table"] == "folders"
            and item["column"] == "folder_store_id"
            and item["sample_rows"]
            and item["sample_rows"][0]["folder_relpath"] == "root"
            for item in impact["reference_counts"]
        )
        assert impact["warning"] == "Delete may fail or cascade depending on schema constraints."


def test_core_runtime_sync_store_start_submits_job(monkeypatch) -> None:
    sync_command_module = pytest.importorskip(
        "LiuXin_alpha.surfaces.terminal.commands.sync",
        reason="Terminal command package is not exposed under surfaces/ in this checkout.",
    )

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
                        "crawler_recurse": True,
                        "crawler_max_depth": None,
                        "crawler_timeout_s": None,
                        "crawler_no_parent": True,
                        "crawler_span_hosts": False,
                        "crawler_respect_robots": True,
                        "crawler_user_agent": None,
                        "wget_no_verbose": True,
                        "wget_args": (),
                        "crawler_incremental_db_writes": True,
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
