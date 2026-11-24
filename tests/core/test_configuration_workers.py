"""Stress profile boundaries with queued, retried and spawned Core work."""

from __future__ import annotations

import os
import threading

import pytest

from LiuXin_alpha.configuration import (
    ConfigurationPaths,
    current_preferences,
    use_preferences,
)
from LiuXin_alpha.core.factory import create_core
from LiuXin_alpha.core.library_services import LibraryServices
from LiuXin_alpha.preferences import Preferences
from LiuXin_alpha.utils.jobs.api import JobRequest
from LiuXin_alpha.utils.jobs.manager import InMemoryJobManager

READ_PROFILE = """
def read():
    from LiuXin_alpha.configuration import current_preferences, configuration_paths
    from LiuXin_alpha.utils.config.config_tools import JSONConfig
    component = JSONConfig("worker")
    return (current_preferences()["network_timeout"], str(configuration_paths().preferences), component.file_path)
"""

OPEN_LIBRARY = """
def run(connection, title, fail=False):
    from LiuXin_alpha.core.workflow_jobs import _open_library_services
    from LiuXin_alpha.configuration import current_preferences, configuration_paths
    with _open_library_services(**connection) as owner:
        assert owner.database.preferences is current_preferences()
        assert owner.core_services.preferences is current_preferences()
        if fail:
            raise RuntimeError("worker failure after opening")
        identity = owner.core_services.catalog.works.create({"title": title})
        return (identity, str(configuration_paths().preferences), owner.database.uuid)
"""


def make_store(root):
    return Preferences(
        paths=ConfigurationPaths.resolve(system_root=root, environment={}),
        upgrade_on_load=False,
    )


@pytest.mark.parametrize("backend", ["serial", "process"])
def test_queued_work_retains_store_and_component_paths_after_profile_switch(
    tmp_path, monkeypatch, backend
):
    from LiuXin_alpha import configuration

    stores = [make_store(tmp_path / str(i)) for i in range(2)]
    for i, store in enumerate(stores):
        store["network_timeout"] = 10 + i
    gate = threading.Event()
    manager = InMemoryJobManager(max_workers=1, default_backend=backend)
    manager._executor.submit(gate.wait, 20)
    ids = []
    try:
        for i in range(12):
            with use_preferences(stores[i % 2]):
                ids.append(
                    manager.submit(
                        JobRequest(
                            module_name=READ_PROFILE,
                            function_name="read",
                            module_is_source_code=True,
                        ),
                        no_output=True,
                    )
                )
        monkeypatch.setattr(configuration, "_default", make_store(tmp_path / "wrong"))
        gate.set()
        for i, identity in enumerate(ids):
            result = manager.result(identity, timeout=30, raise_on_failure=True).result
            selected = stores[i % 2]
            assert result == (
                10 + i % 2,
                str(selected.paths.preferences),
                str(selected.paths.components / "worker.json"),
            )
    finally:
        gate.set()
        manager.shutdown(wait=True, cancel_pending=True)


@pytest.mark.parametrize("backend", ["serial", "process"])
def test_unbound_jobs_pin_process_default_at_submission(tmp_path, monkeypatch, backend):
    from LiuXin_alpha import configuration

    first, second = make_store(tmp_path / "first"), make_store(tmp_path / "second")
    first["network_timeout"], second["network_timeout"] = 16, 90
    gate = threading.Event()
    manager = InMemoryJobManager(max_workers=1, default_backend=backend)
    manager._executor.submit(gate.wait, 20)
    try:
        with use_preferences(None):
            monkeypatch.setattr(configuration, "_default", first)
            identity = manager.submit(
                JobRequest(
                    module_name=READ_PROFILE,
                    function_name="read",
                    module_is_source_code=True,
                ),
                no_output=True,
            )
            monkeypatch.setattr(configuration, "_default", second)
        gate.set()
        assert (
            manager.result(identity, timeout=30, raise_on_failure=True).result[0] == 16
        )
    finally:
        gate.set()
        manager.shutdown(wait=True, cancel_pending=True)


@pytest.mark.parametrize("driver", ["SQLite", "SQLite_apsw"])
@pytest.mark.parametrize("backend", ["serial", "process"])
def test_real_worker_graphs_keep_database_and_profile_together(
    tmp_path, monkeypatch, driver, backend
):
    stores = [make_store(tmp_path / name) for name in ("first", "second")]
    runtimes = []
    manager = InMemoryJobManager(max_workers=1, default_backend=backend)
    try:
        monkeypatch.chdir(tmp_path)
        for i, store in enumerate(stores):
            runtimes.append(
                create_core(
                    database_path=f"library-{i}.sqlite",
                    db_type=driver,
                    create=True,
                    preferences=store,
                    enable_storage_manager=False,
                    enable_maintenance=False,
                    job_manager=manager,
                )
            )
        monkeypatch.chdir(tmp_path.parent)
        ids = []
        for i, core in enumerate(runtimes):
            with use_preferences(core.services.preferences):
                ids.append(
                    manager.submit(
                        JobRequest(
                            module_name=OPEN_LIBRARY,
                            function_name="run",
                            module_is_source_code=True,
                            kwargs={
                                "connection": core.services.database_connection,
                                "title": f"selected-{i}",
                            },
                        ),
                        no_output=True,
                    )
                )
        for i, identity in enumerate(ids):
            execution = manager.result(identity, timeout=60, raise_on_failure=True)
            work_id, path, database_uuid = execution.result
            assert path == str(stores[i].paths.preferences)
            assert str(database_uuid) == str(runtimes[i].database.uuid)
            rows = list(runtimes[i].database.get("SELECT work_title FROM works"))
            assert rows == [(f"selected-{i}",)]
            assert work_id > 0
        # A failing reopened graph must close and leave the next job usable.
        with use_preferences(stores[0]):
            failed = manager.submit(
                JobRequest(
                    module_name=OPEN_LIBRARY,
                    function_name="run",
                    module_is_source_code=True,
                    kwargs={
                        "connection": runtimes[0].services.database_connection,
                        "title": "unused",
                        "fail": True,
                    },
                ),
                no_output=True,
            )
        failure = manager.result(failed, timeout=60)
        assert not failure.ok and "worker failure after opening" in failure.traceback
        with use_preferences(stores[1]):
            healthy = manager.submit(
                JobRequest(
                    module_name=READ_PROFILE,
                    function_name="read",
                    module_is_source_code=True,
                ),
                no_output=True,
            )
        assert manager.result(healthy, timeout=30, raise_on_failure=True).result[
            1
        ] == str(stores[1].paths.preferences)
        # The durable-worker factory must also retain the selected graph.
        with use_preferences(stores[1]):
            with runtimes[0]._durable_jobs._open_library() as graph:
                assert current_preferences() is stores[0]
                assert graph.database.preferences is stores[0]
                assert str(graph.database.uuid) == str(runtimes[0].database.uuid)
            assert current_preferences() is stores[1]
    finally:
        manager.shutdown(wait=True, cancel_pending=True)
        for core in reversed(runtimes):
            core.shutdown()


@pytest.mark.parametrize("failure", ["body", "close"])
def test_library_context_restores_selection_even_when_cleanup_fails(
    tmp_path, monkeypatch, failure
):
    first, second = make_store(tmp_path / "first"), make_store(tmp_path / "second")
    owner = LibraryServices.open(
        database_path=tmp_path / "graph.sqlite",
        create=True,
        preferences=first,
        enable_storage_manager=False,
        enable_maintenance=False,
    )
    seen = []

    def observe_close():
        seen.append(current_preferences())
        if failure == "close":
            raise RuntimeError("close failed")

    owner._exit_stack.callback(observe_close)
    with use_preferences(second):
        with pytest.raises(RuntimeError, match="failed"):
            with owner:
                assert current_preferences() is first
                if failure == "body":
                    raise RuntimeError("body failed")
        assert current_preferences() is second
    assert seen == [first]
    assert owner.closed


def test_unserializable_store_fails_one_job_without_poisoning_worker_pool(tmp_path):
    manager = InMemoryJobManager(max_workers=1, default_backend="process")
    try:
        with use_preferences({"unserializable": lambda: None}):
            failed = manager.submit(
                JobRequest(module_name="math", function_name="sqrt", args=(9,)),
                no_output=True,
            )
        failure = manager.result(failed, timeout=20)
        assert not failure.ok
        with use_preferences(make_store(tmp_path)):
            good = manager.submit(
                JobRequest(module_name="math", function_name="sqrt", args=(16,)),
                no_output=True,
            )
        assert manager.result(good, timeout=20, raise_on_failure=True).result == 4
    finally:
        manager.shutdown(wait=True, cancel_pending=True)


@pytest.mark.parametrize("selector", ["url_metadata", "service"])
def test_live_postgres_profiles_reopen_their_original_schemas(
    tmp_path, monkeypatch, selector
):
    """Use disposable schemas; exercise real spawned connections and durable reopen."""
    from uuid import uuid4

    import psycopg2
    from psycopg2 import sql
    from psycopg2.extensions import parse_dsn

    url = os.environ.get("LIUXIN_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("LIUXIN_TEST_POSTGRES_URL is not configured")
    service_name = "core_configuration_" + uuid4().hex
    service_file = tmp_path / "pg_service.conf"
    service_file.write_text(
        "["
        + service_name
        + "]\n"
        + "".join(f"{key}={value}\n" for key, value in parse_dsn(url).items())
    )
    monkeypatch.setenv("PGSERVICEFILE", str(service_file))
    cleanup = psycopg2.connect(url)
    schemas = ["core_configuration_" + uuid4().hex for _ in range(2)]
    stores = [make_store(tmp_path / name) for name in ("first", "second")]
    runtimes = []
    manager = InMemoryJobManager(max_workers=1, default_backend="process")
    try:
        for i, schema in enumerate(schemas):
            metadata = {
                "schema": schema,
                "dsn" if selector == "url_metadata" else "service": url
                if selector == "url_metadata"
                else service_name,
            }
            runtimes.append(
                create_core(
                    db_type="PostgreSQL",
                    database_metadata=metadata,
                    create=True,
                    preferences=stores[i],
                    enable_storage_manager=False,
                    enable_maintenance=False,
                    durable_job_path=tmp_path / f"jobs-{i}.sqlite",
                    job_manager=manager,
                )
            )
        monkeypatch.setenv("LIUXIN_POSTGRES_SCHEMA", "wrong_schema_after_open")
        monkeypatch.setenv("LIUXIN_POSTGRES_URL", "postgresql://127.0.0.1:1/wrong")
        for i, core in enumerate(runtimes):
            with use_preferences(stores[i]):
                identity = manager.submit(
                    JobRequest(
                        module_name=OPEN_LIBRARY,
                        function_name="run",
                        module_is_source_code=True,
                        kwargs={
                            "connection": core.services.database_connection,
                            "title": f"profile-{i}",
                        },
                    ),
                    no_output=True,
                )
            result = manager.result(identity, timeout=90, raise_on_failure=True).result
            assert result[1] == str(stores[i].paths.preferences)
            assert str(result[2]) == str(core.database.uuid)
            assert list(core.database.get("SELECT work_title FROM works")) == [
                (f"profile-{i}",)
            ]
            # Exercise the supported named API -> durable worker -> graph path.
            core.command("maintenance.run", {"action": "install"})
            core.catalog.works.create({"title": f"maintenance-{i}"})
            core.command("maintenance.run", {"action": "drain", "max_batches": 20})
            assert core.query("maintenance.status")["pending"] == []
            with use_preferences(stores[1 - i]):
                with core._durable_jobs._open_library() as graph:
                    assert current_preferences() is stores[i]
                    assert graph.database._driver.schema == schemas[i]
                    assert str(graph.database.uuid) == str(core.database.uuid)
                assert current_preferences() is stores[1 - i]
    finally:
        manager.shutdown(wait=True, cancel_pending=True)
        for core in reversed(runtimes):
            core.shutdown()
        with cleanup:
            with cleanup.cursor() as cursor:
                for schema in schemas:
                    cursor.execute(
                        sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                            sql.Identifier(schema)
                        )
                    )
        cleanup.close()


def test_process_start_failure_closes_both_pipe_endpoints(monkeypatch):
    from LiuXin_alpha.utils.jobs import api

    opened = []
    original_pipe = api.PROCESS_CONTEXT.Pipe

    def pipe(*args, **kwargs):
        endpoints = original_pipe(*args, **kwargs)
        opened.extend(endpoints)
        return endpoints

    monkeypatch.setattr(api.PROCESS_CONTEXT, "Pipe", pipe)
    with use_preferences({"unserializable": lambda: None}):
        with pytest.raises(Exception, match="pickle"):
            api.execute_job(
                JobRequest(module_name="math", function_name="sqrt", args=(9,)),
                no_output=True,
                backend="process",
            )
    assert len(opened) == 2
    assert all(endpoint.closed for endpoint in opened)
