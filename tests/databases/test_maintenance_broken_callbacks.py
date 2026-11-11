"""Verify failures, deferred hooks and borrowed transaction misuse on real backends."""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from LiuXin_alpha.databases.db_types.maintenance_types import MaintenanceMode
from LiuXin_alpha.databases.maintenance.bound_connection import (
    MaintenanceTransactionError,
)
from LiuXin_alpha.databases.maintenance.plugins import MaintenancePluginUpgrade
from LiuXin_alpha.databases.maintenance.runtime import (
    MaintenanceFailure,
    MaintenanceRuntime,
)
from tests.databases.test_maintenance_malformed_plugins import (
    async_hook,
    generator_hook,
)
from tests.databases.test_maintenance_runtime import Projection
from tests.databases.test_maintenance_runtime import runtime_factory as runtime_factory


def write(runtime, identity=1):
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (?, 'source')", (identity,))


def assert_no_effects(runtime):
    connection = runtime.driver.get_connection()
    try:
        assert connection.execute("SELECT * FROM effects").fetchall() == []
    finally:
        connection.close()


@pytest.mark.parametrize("mode", [MaintenanceMode.PROMPT, MaintenanceMode.LAZY])
@pytest.mark.parametrize("skip_all", [False, True])
def test_explicit_selection_and_coalescing_acknowledge_the_original_batch(
    runtime_factory, mode, skip_all
):
    class Selective(Projection):
        def wants_event(self, event):
            return not skip_all and event.row_id % 2 == 1

        def coalesce_key(self, event):
            return "one-summary"

    plugin = Selective()
    plugin.mode = mode
    runtime = runtime_factory([plugin])
    runtime.install()
    with runtime.unit() as connection:
        for identity in range(1, 5):
            connection.execute("INSERT INTO works VALUES (?, 'source')", (identity,))
    if mode is MaintenanceMode.LAZY:
        identities = [row["obligation_id"] for row in runtime.status()["pending"]]
        assert runtime.deliver(plugin.name, identities) == {
            "handled": 0 if skip_all else 1
        }
    check = runtime.driver.get_connection()
    assert check.execute("SELECT row_id FROM effects").fetchall() == (
        [] if skip_all else [(3,)]
    )
    assert (
        check.execute(
            "SELECT COUNT(*) FROM maintenance_obligations WHERE completed_ms IS NULL"
        ).fetchone()[0]
        == 0
    )


@pytest.mark.parametrize("hook", ["wants_event", "coalesce_key", "handle_events"])
@pytest.mark.parametrize("factory", [async_hook, generator_hook])
@pytest.mark.parametrize("mode", [MaintenanceMode.PROMPT, MaintenanceMode.LAZY])
def test_sync_wrappers_cannot_publish_deferred_hook_work(
    runtime_factory, hook, factory, mode
):
    plugin = Projection()
    plugin.mode = mode
    setattr(plugin, hook, lambda *args: factory())
    runtime = runtime_factory([plugin])
    runtime.install()
    if mode is MaintenanceMode.PROMPT:
        with pytest.raises(MaintenanceFailure, match="deferred work"):
            write(runtime)
    else:
        write(runtime)
        identity = runtime.status()["pending"][0]["obligation_id"]
        with pytest.raises(MaintenanceFailure, match="deferred work"):
            runtime.deliver(plugin.name, [identity])
        assert runtime.status()["pending"][0]["attempts"] == 1
    assert_no_effects(runtime)


@pytest.mark.parametrize(
    "failure",
    ["selection", "coalesce", "unhashable", "hash", "handler", "interrupt", "exit"],
)
@pytest.mark.parametrize("mode", [MaintenanceMode.PROMPT, MaintenanceMode.LAZY])
def test_callback_failures_preserve_recoverable_state(runtime_factory, failure, mode):
    class BadHash:
        def __hash__(self):
            raise RuntimeError("broken hash")

    class Broken(Projection):
        broken = True

        def wants_event(self, event):
            if self.broken and failure == "selection":
                raise RuntimeError("selection")
            return True

        def coalesce_key(self, event):
            if self.broken:
                if failure == "coalesce":
                    raise RuntimeError("coalesce")
                if failure == "unhashable":
                    return []
                if failure == "hash":
                    return BadHash()
            return None

        def handle_events(self, context, events):
            result = super().handle_events(context, events)
            if self.broken:
                if failure == "interrupt":
                    raise KeyboardInterrupt("interrupted plugin")
                if failure == "exit":
                    raise SystemExit("exiting plugin")
                raise RuntimeError("handler")
            return result

    plugin = Broken()
    plugin.mode = mode
    runtime = runtime_factory([plugin])
    runtime.install()
    expected = {
        "unhashable": TypeError,
        "interrupt": KeyboardInterrupt,
        "exit": SystemExit,
    }.get(failure, RuntimeError)
    if mode is MaintenanceMode.PROMPT:
        with pytest.raises(expected):
            write(runtime)
    else:
        write(runtime)
        identity = runtime.status()["pending"][0]["obligation_id"]
        with pytest.raises(expected):
            runtime.deliver(plugin.name, [identity])
        pending = runtime.status()["pending"]
        assert len(pending) == 1
        assert pending[0]["attempts"] == (0 if failure in {"interrupt", "exit"} else 1)
    assert not runtime.in_handler
    assert_no_effects(runtime)
    plugin.broken = False
    if mode is MaintenanceMode.PROMPT:
        write(runtime)
    else:
        runtime.retry(plugin.name)
        assert runtime.deliver(plugin.name, [identity]) == {"handled": 1}


@pytest.mark.parametrize("surface", ["execute", "cursor", "executemany"])
@pytest.mark.parametrize(
    "sql",
    [
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT escaped",
        "CREATE TABLE escaped (id INTEGER)",
        "DROP TABLE works",
        "PRAGMA user_version=99",
        "SELECT 1; COMMIT",
        "/* comment */ END",
    ],
)
def test_borrowed_row_sql_refuses_ownership_and_schema_changes(
    runtime_factory, surface, sql
):
    class Broken(Projection):
        def handle_events(self, context, events):
            super().handle_events(context, events)
            self.cursor = context.connection.cursor()
            if surface == "executemany":
                context.connection.executemany(sql, [()])
            elif surface == "cursor":
                self.cursor.execute(sql)
            else:
                context.connection.execute(sql)

    plugin = Broken()
    runtime = runtime_factory([plugin])
    runtime.install()
    with pytest.raises(MaintenanceTransactionError):
        write(runtime)
    assert_no_effects(runtime)
    for action in (
        lambda: plugin.saved.execute("SELECT 1"),
        lambda: plugin.saved.cursor(),
        lambda: plugin.cursor.execute("SELECT 1"),
        lambda: plugin.cursor.fetchone(),
        lambda: plugin.cursor.fetchmany(),
        lambda: plugin.cursor.fetchall(),
        lambda: list(plugin.cursor),
    ):
        with pytest.raises(MaintenanceTransactionError, match="no longer active"):
            action()
    plugin.handle_events = Projection.handle_events.__get__(plugin)
    write(runtime)


@pytest.mark.parametrize(
    "policies",
    [
        None,
        "1",
        [None],
        [MaintenancePluginUpgrade("")],
        [MaintenancePluginUpgrade("1", replay_pending=1)],
        [MaintenancePluginUpgrade("1", migrate=42)],
        [MaintenancePluginUpgrade("1"), MaintenancePluginUpgrade("1")],
    ],
)
def test_malformed_upgrade_policies_fail_before_attachment(policies):
    plugin = Projection()
    plugin.upgrades = policies
    driver = SimpleNamespace(_open_connections=[])
    with pytest.raises((ValueError, TypeError)):
        MaintenanceRuntime(SimpleNamespace(driver=driver), [plugin])
    assert "_maintenance_runtime" not in vars(driver)


@pytest.mark.parametrize("returned", ["coroutine", "generator", "value"])
def test_migration_wrapper_return_rolls_back_its_earlier_writes(
    runtime_factory, returned
):
    old = Projection()
    old.mode = MaintenanceMode.LAZY
    original = runtime_factory([old])
    original.install()
    write(original)
    before = original.status()
    borrowed = []

    def migration(context, previous):
        borrowed.append(context.connection)
        context.connection.execute("INSERT INTO effects VALUES (99,99)")
        return {
            "coroutine": async_hook,
            "generator": generator_hook,
            "value": lambda: False,
        }[returned]()

    plugin = Projection()
    plugin.mode = MaintenanceMode.LAZY
    plugin.version = "2"
    plugin.upgrades = (
        MaintenancePluginUpgrade("1", replay_pending=True, migrate=migration),
    )
    runtime = runtime_factory([plugin])
    with pytest.raises(MaintenanceFailure):
        runtime.upgrade(plugin.name, expected_version="1", dry_run=False)
    after = runtime.status()
    assert after["plugins"] == before["plugins"]
    assert after["pending"][0]["retry_generation"] == 0
    assert after["pending"][0]["plugin_version"] == "1"
    assert after["upgrades"] == []
    assert_no_effects(runtime)
    with pytest.raises(MaintenanceTransactionError):
        borrowed[0].execute("SELECT 1")


class UnprintableError(Exception):
    def __str__(self):
        raise RuntimeError("broken exception formatter")


@pytest.mark.parametrize(
    "error",
    [UnprintableError(), ValueError("bad\x00text\ud800"), ValueError("x" * 5000)],
)
def test_unprintable_plugin_error_retains_retry_evidence(runtime_factory, error):
    class Broken(Projection):
        mode = MaintenanceMode.LAZY

        def handle_events(self, context, events):
            super().handle_events(context, events)
            raise error

    runtime = runtime_factory([Broken()])
    runtime.install()
    write(runtime)
    identity = runtime.status()["pending"][0]["obligation_id"]
    with pytest.raises(type(error)):
        runtime.deliver("projection", [identity])
    pending = runtime.status()["pending"][0]
    assert pending["attempts"] == 1
    assert type(error).__name__ in pending["last_error"]
    assert len(pending["last_error"]) <= 2000
    assert "\x00" not in pending["last_error"]
    pending["last_error"].encode("utf-8", "strict")
    assert_no_effects(runtime)


def test_broken_lazy_plugin_does_not_starve_independent_jobs(runtime_factory, tmp_path):
    from LiuXin_alpha.core.maintenance_jobs import (
        MaintenanceJobHandler,
        publish_obligations,
    )
    from LiuXin_alpha.jobs.handler_api import JobHandlerRegistry
    from LiuXin_alpha.jobs.repository import JobRepository
    from LiuXin_alpha.jobs.worker import JobWorker

    class Broken(Projection):
        name = "broken"
        mode = MaintenanceMode.LAZY
        retry_limit = 0

        def handle_events(self, context, events):
            super().handle_events(context, events)
            raise UnprintableError()

    good = Projection()
    good.mode = MaintenanceMode.LAZY
    plugins = [Broken(), good]
    runtime = runtime_factory(plugins)
    runtime.install()
    write(runtime)
    repository = JobRepository(tmp_path / "jobs.sqlite")

    @contextmanager
    def open_library():
        yield SimpleNamespace(library=SimpleNamespace(db=runtime_factory(plugins).db))

    handlers = JobHandlerRegistry()
    handlers.register(
        MaintenanceJobHandler(library_uuid="test-library", open_library=open_library)
    )
    worker = JobWorker(repository=repository, handlers=handlers)
    assert (
        publish_obligations(runtime, repository, library_uuid="test-library")[
            "published"
        ]
        == 2
    )
    assert worker.run_once()
    assert worker.run_once()
    runs = repository.list_runs()
    assert {run.state.value for run in runs} == {"failed", "succeeded"}
    failed = next(run for run in runs if run.state.value == "failed")
    assert "UnprintableError" in failed.error_text
    pending = runtime.status()["pending"]
    assert len(pending) == 1 and pending[0]["plugin_id"] == "broken"
    assert pending[0]["attempts"] == 1
    assert (
        publish_obligations(runtime, repository, library_uuid="test-library")[
            "published"
        ]
        == 0
    )


@pytest.mark.parametrize("failure_owner", ["runtime", "jobs"])
def test_zero_backoff_remains_immediate_after_backward_wall_clock_step(
    runtime_factory, tmp_path, monkeypatch, failure_owner
):
    from LiuXin_alpha.core import maintenance_jobs
    from LiuXin_alpha.databases.maintenance import runtime as runtime_module
    from LiuXin_alpha.jobs.repository import JobRepository

    clock = [100_000]
    monkeypatch.setattr(runtime_module, "now_ms", lambda: clock[0])
    monkeypatch.setattr(maintenance_jobs, "now_ms", lambda: clock[0])
    plugin = Projection()
    plugin.mode = MaintenanceMode.LAZY
    runtime = runtime_factory([plugin])
    runtime.install()
    write(runtime)
    repository = JobRepository(tmp_path / "jobs.sqlite")
    old_run = maintenance_jobs.publish_obligations(
        runtime, repository, library_uuid="test-library"
    )["run_ids"][0]
    identity = runtime.status()["pending"][0]["obligation_id"]
    if failure_owner == "runtime":
        plugin.fail = True
        with pytest.raises(RuntimeError, match="projection failed"):
            runtime.deliver(
                plugin.name,
                [identity],
                job_run_id=old_run,
                job_ledger_uuid=repository.ledger_uuid,
            )
        plugin.fail = False
        repository = JobRepository(tmp_path / "replacement-jobs.sqlite")
    else:
        leased = repository.lease_next_run(worker_id="broken", lease_for_s=60)
        assert leased is not None
        owner = {"worker_id": "broken", "lease_token": leased.lease_token}
        repository.mark_running(old_run, **owner)
        repository.mark_failed(old_run, error_text="worker failed", **owner)
        # Reconcile a jobs failure which has no catalogue failure receipt yet.
        maintenance_jobs.publish_obligations(
            runtime, repository, library_uuid="test-library"
        )
    assert runtime.status()["pending"][0]["attempts"] == 1
    assert_no_effects(runtime)

    clock[0] -= 1_000
    published = maintenance_jobs.publish_obligations(
        runtime, repository, library_uuid="test-library"
    )
    assert published["published"] == 1
    assert runtime.deliver(
        plugin.name,
        [identity],
        job_run_id=published["run_ids"][0],
        job_ledger_uuid=repository.ledger_uuid,
    ) == {"handled": 1}
    assert runtime.status()["pending"] == []
