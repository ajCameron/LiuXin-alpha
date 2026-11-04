"""Exercise operator controls across the catalogue and jobs transaction boundary."""

import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from LiuXin_alpha.core.durable_jobs import DurableJobService
from LiuXin_alpha.core.maintenance_jobs import (
    MaintenanceJobHandler,
    publish_obligations,
)
from LiuXin_alpha.databases.db_types.maintenance_types import MaintenanceMode
from LiuXin_alpha.jobs.handler_api import JobHandlerRegistry
from LiuXin_alpha.jobs.repository import JobRepository
from LiuXin_alpha.jobs.worker import JobWorker
from tests.databases.test_maintenance_runtime import Projection
from tests.databases.test_maintenance_runtime import runtime_factory as runtime_factory


def _setup(runtime_factory, tmp_path, plugins):
    runtime = runtime_factory(plugins)
    runtime.install()
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'operator controls')")
    repository = JobRepository(tmp_path / "jobs.sqlite")

    @contextmanager
    def open_library():
        yield SimpleNamespace(library=SimpleNamespace(db=runtime_factory(plugins).db))

    handlers = JobHandlerRegistry()
    handlers.register(
        MaintenanceJobHandler(library_uuid="test-library", open_library=open_library)
    )
    worker = JobWorker(repository=repository, handlers=handlers)
    return runtime, repository, worker, open_library


def _publish(runtime, repository, **kwargs):
    return publish_obligations(
        runtime, repository, library_uuid="test-library", **kwargs
    )


@pytest.mark.parametrize("previous_failures", [0, 1])
def test_pause_preserves_last_attempt_and_resume_republishes(
    runtime_factory, tmp_path, previous_failures
):
    plugin = Projection()
    plugin.mode = MaintenanceMode.LAZY
    plugin.retry_limit = previous_failures
    runtime, repository, worker, _ = _setup(runtime_factory, tmp_path, [plugin])
    _publish(runtime, repository)
    if previous_failures:
        plugin.fail = True
        assert worker.run_once()
        plugin.fail = False
        _publish(runtime, repository)
    runtime.set_enabled(plugin.name, False)
    before = runtime.status()["pending"][0]
    assert worker.run_once()
    paused = runtime.status()["pending"][0]
    assert paused["attempts"] == before["attempts"]
    assert paused["last_error"] == before["last_error"]
    assert paused["job_run_id"] is None
    assert paused["retry_generation"] == before["retry_generation"] + 1
    assert _publish(runtime, repository)["published"] == 0
    runtime.set_enabled(plugin.name, True)
    assert _publish(runtime, repository)["published"] == 1
    assert worker.run_once()
    assert runtime.status()["pending"] == []
    with runtime.unit() as connection:
        assert connection.execute("SELECT row_id FROM effects").fetchall() == [(1,)]


@pytest.mark.parametrize("successor_ran", [False, True])
def test_external_retry_does_not_wedge_publication(
    runtime_factory, tmp_path, successor_ran
):
    plugin = Projection()
    plugin.mode = MaintenanceMode.LAZY
    runtime, repository, worker, _ = _setup(runtime_factory, tmp_path, [plugin])
    parent = _publish(runtime, repository)["run_ids"][0]
    plugin.fail = True
    assert worker.run_once()
    plugin.fail = False
    repository.retry_run(
        parent, request_key="external-retry", acknowledge_possible_effects=True
    )
    if successor_ran:
        assert worker.run_once()
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (2, 'later work')")
    for _ in range(6):
        _publish(runtime, repository)
        while worker.run_once():
            pass
        if not runtime.status()["pending"]:
            break
    assert runtime.status()["pending"] == []
    with runtime.unit() as connection:
        assert connection.execute(
            "SELECT row_id FROM effects ORDER BY row_id"
        ).fetchall() == [(1,), (2,)]


def test_core_generic_retry_refuses_before_creating_successor(
    runtime_factory, tmp_path
):
    plugin = Projection()
    plugin.mode = MaintenanceMode.LAZY
    runtime, repository, worker, open_library = _setup(
        runtime_factory, tmp_path, [plugin]
    )
    parent = _publish(runtime, repository)["run_ids"][0]
    plugin.fail = True
    assert worker.run_once()
    service = DurableJobService(
        path=repository.sqlite_path,
        library_uuid="test-library",
        open_library=open_library,
    )
    service._repository = repository
    with pytest.raises(ValueError, match="maintenance retry"):
        service.retry(
            service._job_id(parent),
            request_key="operator",
            acknowledge_possible_effects=True,
        )
    assert len(repository.list_runs()) == 1
    assert service._thread is None


def test_publication_prioritizes_ready_plugins_with_bounded_pages(
    runtime_factory, tmp_path
):
    low, high = Projection(), Projection()
    low.name, low.mode, low.priority = "a.low", MaintenanceMode.LAZY, -100
    high.name, high.mode, high.priority = "z.high", MaintenanceMode.LAZY, 200
    runtime, repository, _, _ = _setup(runtime_factory, tmp_path, [low, high])
    first = repository.get_run(_publish(runtime, repository, limit=1)["run_ids"][0])
    assert json.loads(first.payload_json)["plugin_id"] == high.name
    assert repository.get_definition(first.job_definition_id).priority == high.priority


def test_interrupted_old_priority_handoff_retains_its_captured_job(
    runtime_factory, tmp_path
):
    from LiuXin_alpha.core.maintenance_jobs import MAINTENANCE_JOB_KIND
    from LiuXin_alpha.jobs.models import JobDefinition

    plugin = Projection()
    plugin.mode = MaintenanceMode.LAZY
    runtime, repository, worker, _ = _setup(runtime_factory, tmp_path, [plugin])
    state = runtime.status()
    row = state["pending"][0]
    payload = json.dumps(
        dict(
            library_uuid="test-library",
            journal_uuid=state["journal_uuid"],
            plugin_id=plugin.name,
            plugin_version=plugin.version,
            obligation_id=row["obligation_id"],
        )
    )
    run = repository.submit_job(
        JobDefinition(
            job_kind=MAINTENANCE_JOB_KIND,
            job_name=f"Maintenance: {plugin.name}",
            payload_json=payload,
        ),
        request_key=f"maintenance:{state['journal_uuid']}:{row['obligation_id']}:0",
    )
    assert repository.get_definition(run.job_definition_id).priority == 100
    assert _publish(runtime, repository)["run_ids"] == [run.job_run_id]
    assert worker.run_once()
    assert runtime.status()["pending"] == []
