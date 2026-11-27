"""
Provide test core runtime phase1 utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test core runtime phase1 through a consuming regression::

        python -m pytest -q tests/core/test_core_runtime_phase1.py
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pytest

from LiuXin_alpha.core import CoreCommand, CoreQuery, CoreRuntime
from LiuXin_alpha.core.proxies import LocalLibraryProxy
from LiuXin_alpha.core.proxies.local import looks_like_write_method
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.library.library import Library


def test_core_runtime_health_and_invoke_paths(
    core_runtime_factory: Callable[..., CoreRuntime],
) -> None:
    """
    Perform the test core runtime health and invoke paths operation under explicit file-format and conversion rules.

    Example:
        Exercise test core runtime health and invoke paths through a consuming regression::

            python -m pytest -q tests/core/test_core_runtime_phase1.py


    :param core_runtime_factory: Value supplied for core runtime factory under the
        utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    runtime = core_runtime_factory(core_version="test-phase1")

    health = runtime.execute_query(CoreQuery(name="health")).result
    assert health["core_version"] == "test-phase1"
    assert health["shutdown"] is False

    result = runtime.invoke_query(target="library", method="echo", args=("hello",))
    assert result == "echo:hello"

    write_result = runtime.invoke_command(
        target="database", method="set_value", args=(7,)
    )
    assert write_result == 7
    assert runtime.invoke_query(target="database", method="get_value") == 7


def test_core_runtime_emits_command_lifecycle_events(
    core_runtime_factory: Callable[..., CoreRuntime],
) -> None:
    """
    Perform the test core runtime emits command lifecycle events operation under explicit file-format and conversion rules.

    Example:
        Exercise test core runtime emits command lifecycle events through a consuming regression::

            python -m pytest -q tests/core/test_core_runtime_phase1.py


    :param core_runtime_factory: Value supplied for core runtime factory under the
        utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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

    assert runtime.wait_for_events(2)
    event_types = [event.event_type for event in events]
    assert "command.started" in event_types
    assert "write.completed" in event_types
    assert "command.finished" in event_types
    assert runtime.wait_for_events(2)
    write_events = [event for event in events if event.event_type == "write.completed"]
    assert len(write_events) == 1
    assert write_events[0].payload["target"] == "database"
    assert write_events[0].payload["method"] == "set_value"
    assert write_events[0].payload["command_id"] == command.command_id


def test_core_runtime_api_describe_exposes_named_handlers_and_targets(
    core_runtime_factory: Callable[..., CoreRuntime],
) -> None:
    """
    Perform the test core runtime api describe exposes named handlers and targets operation under explicit file-format and conversion rules.

    Example:
        Exercise test core runtime api describe exposes named handlers and targets through a consuming regression::

            python -m pytest -q tests/core/test_core_runtime_phase1.py


    :param core_runtime_factory: Value supplied for core runtime factory under the
        utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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

    library_methods = {
        method["name"]: method for method in targets["library"]["methods"]
    }
    database_methods = {
        method["name"]: method for method in targets["database"]["methods"]
    }
    storage_methods = {
        method["name"]: method for method in targets["storage"]["methods"]
    }

    assert library_methods["echo"]["write"] is False
    assert library_methods["echo"]["parameters"][0]["name"] == "text"
    assert database_methods["set_value"]["write"] is True
    assert database_methods["get_value"]["write"] is False
    assert storage_methods["ping"]["write"] is False

    db_only = runtime.execute_query(
        CoreQuery(name="api.describe", payload={"target": "db"})
    ).result
    assert [entry["name"] for entry in db_only["targets"]] == ["database"]


def test_local_proxy_auto_dispatches_read_and_write(
    core_runtime_factory: Callable[..., CoreRuntime],
) -> None:
    """
    Perform the test local proxy auto dispatches read and write operation under explicit file-format and conversion rules.

    Example:
        Exercise test local proxy auto dispatches read and write through a consuming regression::

            python -m pytest -q tests/core/test_core_runtime_phase1.py


    :param core_runtime_factory: Value supplied for core runtime factory under the
        utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test local proxy bootstrap storage manager routes via command operation under explicit file-format and conversion rules.

    Example:
        Exercise test local proxy bootstrap storage manager routes via command through a consuming regression::

            python -m pytest -q tests/core/test_core_runtime_phase1.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    @dataclass
    class _FakeDatabase:
        """
        Provide the fakedatabase contract for validated ebook processing.

        Example:
            Exercise test local proxy bootstrap storage manager routes via command. FakeDatabase through a consuming regression::

                python -m pytest -q tests/core/test_core_runtime_phase1.py
        """

        bootstrapped: int = 0

        def bootstrap_storage_manager(self, *, clear_existing: bool = False):
            """
            Perform the bootstrap storage manager operation under explicit file-format and conversion rules.

            Example:
                Exercise test local proxy bootstrap storage manager routes via command. FakeDatabase.bootstrap storage manager through a consuming regression::

                    python -m pytest -q tests/core/test_core_runtime_phase1.py


            :param clear_existing: Value supplied for clear existing under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.bootstrapped += 1
            return {
                "bootstrapped": self.bootstrapped,
                "clear_existing": bool(clear_existing),
            }

    @dataclass
    class _FakeStorage:
        """
        Provide the fakestorage contract for validated ebook processing.

        Example:
            Exercise test local proxy bootstrap storage manager routes via command. FakeStorage through a consuming regression::

                python -m pytest -q tests/core/test_core_runtime_phase1.py
        """

        pass

    @dataclass
    class _FakeLibrary:
        """
        Provide the fakelibrary contract for validated ebook processing.

        Example:
            Exercise test local proxy bootstrap storage manager routes via command. FakeLibrary through a consuming regression::

                python -m pytest -q tests/core/test_core_runtime_phase1.py
        """

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
    assert runtime.wait_for_events(2)
    event_types = [event.event_type for event in events]
    assert "command.started" in event_types
    assert "command.finished" in event_types


def test_core_runtime_library_store_save_and_lookup_round_trip(tmp_path) -> None:
    """
    Perform the test core runtime library store save and lookup round trip operation under explicit file-format and conversion rules.

    Example:
        Exercise test core runtime library store save and lookup round trip through a consuming regression::

            python -m pytest -q tests/core/test_core_runtime_phase1.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
            library=Library(database=db),
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
    """
    Perform the test core runtime library row get and update round trip operation under explicit file-format and conversion rules.

    Example:
        Exercise test core runtime library row get and update round trip through a consuming regression::

            python -m pytest -q tests/core/test_core_runtime_phase1.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
            library=Library(database=db),
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
    """
    Perform the test core runtime library delete row round trip operation under explicit file-format and conversion rules.

    Example:
        Exercise test core runtime library delete row round trip through a consuming regression::

            python -m pytest -q tests/core/test_core_runtime_phase1.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
            library=Library(database=db),
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
    """
    Perform the test core runtime library delete impact reports direct references operation under explicit file-format and conversion rules.

    Example:
        Exercise test core runtime library delete impact reports direct references through a consuming regression::

            python -m pytest -q tests/core/test_core_runtime_phase1.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
            library=Library(database=db),
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
            item["table"] == "folders"
            and item["column"] == "folder_store_id"
            and int(item["count"]) >= 1
            for item in impact["reference_counts"]
        )
        assert any(
            item["table"] == "folders"
            and item["column"] == "folder_store_id"
            and item["sample_rows"]
            and item["sample_rows"][0]["folder_relpath"] == "root"
            for item in impact["reference_counts"]
        )
        assert (
            impact["warning"]
            == "Delete may fail or cascade depending on schema constraints."
        )


def test_core_runtime_sync_store_requires_durable_owner(core_runtime_factory) -> None:
    from LiuXin_alpha.core.errors import CoreError

    runtime = core_runtime_factory()
    with pytest.raises(CoreError) as failure:
        runtime.command(
            "sync.store.start",
            {"sync_kwargs": {"database_path": "/tmp/fake.sqlite", "db_type": "SQLite"}},
        )
    assert failure.value.code == "capability_unavailable"


def test_core_runtime_sync_store_cancel_reports_unknown_job(
    core_runtime_factory: Callable[..., CoreRuntime],
) -> None:
    """
    Perform the test core runtime sync store cancel reports unknown job operation under explicit file-format and conversion rules.

    Example:
        Exercise test core runtime sync store cancel reports unknown job through a consuming regression::

            python -m pytest -q tests/core/test_core_runtime_phase1.py


    :param core_runtime_factory: Value supplied for core runtime factory under the
        utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
