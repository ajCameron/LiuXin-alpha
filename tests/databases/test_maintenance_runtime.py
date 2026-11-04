"""Test prompt rollback, durable revisions, and explicit runtime configuration."""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace
from uuid import uuid4

import pytest

from LiuXin_alpha.databases.connection_hooks import SQLiteCompletionConnection
from LiuXin_alpha.databases.db_types.maintenance_types import MaintenanceMode
from LiuXin_alpha.databases.maintenance.bound_connection import (
    MaintenanceTransactionError,
)
from LiuXin_alpha.databases.maintenance.plugins import (
    MaintenancePluginBase,
    MaintenancePluginResult,
)
from LiuXin_alpha.databases.maintenance.runtime import (
    MaintenanceFailure,
    MaintenanceRuntime,
)
from LiuXin_alpha.databases.transactions import transaction_scope


class Projection(MaintenancePluginBase):
    name = "projection"
    tables = ("works",)
    mode = MaintenanceMode.PROMPT
    retry_backoff_s = 0

    def __init__(self):
        self.fail = False
        self.saved = None

    def handle_events(self, context, events):
        self.saved = context.connection
        for event in events:
            context.connection.execute(
                "INSERT INTO effects VALUES (?, ?)", (event.row_id, event.change_id)
            )
        if self.fail:
            raise RuntimeError("projection failed")
        return MaintenancePluginResult(handled=len(events))


@pytest.fixture(params=["sqlite", "postgres"])
def runtime_factory(tmp_path, request):
    path = tmp_path / "catalog.sqlite"
    connections = []
    schema = None
    metadata = None
    if request.param == "postgres":
        url = os.environ.get("LIUXIN_TEST_POSTGRES_URL")
        if not url:
            pytest.skip("LIUXIN_TEST_POSTGRES_URL is not configured")
        from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
            PostgresConnectionAdapter,
            connect_postgres,
        )

        schema = "maintenance_" + uuid4().hex
        metadata = {"postgres_url": url}
        cleanup = connect_postgres(metadata, prompt_for_password=False)
        with cleanup:
            with cleanup.cursor() as cursor:
                cursor.execute(f'CREATE SCHEMA "{schema}"')

    def make(plugins):
        driver = SimpleNamespace(_open_connections=[], schema=schema)

        def connect():
            if schema:
                connection = PostgresConnectionAdapter(
                    connect_postgres(metadata, prompt_for_password=False)
                )
                connection.execute(f'SET search_path TO "{schema}"')
                connection.commit()
            else:
                connection = SQLiteCompletionConnection(path, check_same_thread=False)
                connection.execute("PRAGMA foreign_keys=ON")
                connection.create_function(
                    "LIUXIN_MAINTENANCE_READY", 2, lambda *_: False
                )
                connection.create_function("LIUXIN_MAINTENANCE_ORIGIN", 0, lambda: "")
                connection.create_function(
                    "LIUXIN_MAINTENANCE_ANCESTRY", 0, lambda: "[]"
                )
            connections.append(connection)
            driver._open_connections.append(connection)
            if getattr(driver, "_maintenance_runtime", None):
                driver._maintenance_runtime.bind(connection)
            return connection

        driver.get_connection = connect
        connection = connect()
        connection.execute(
            "CREATE TABLE IF NOT EXISTS works(work_id INTEGER PRIMARY KEY, title TEXT)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS effects(row_id INTEGER, revision INTEGER)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS expression_work_links (expression_work_link_id INTEGER PRIMARY KEY, "
            "expression_work_link_work_id INTEGER REFERENCES works(work_id) ON DELETE CASCADE, "
            "expression_work_link_expression_id INTEGER)"
        )
        connection.commit()
        return MaintenanceRuntime(
            SimpleNamespace(driver=driver, uuid="test-library"), plugins
        )

    yield make
    for connection in connections:
        connection.close()
    if schema:
        with cleanup:
            with cleanup.cursor() as cursor:
                cursor.execute(f'DROP SCHEMA "{schema}" CASCADE')
        cleanup.close()


@pytest.mark.parametrize(
    "boundary", ["unit", "native", "commit", "sql_commit", "cursor_commit"]
)
def test_prompt_failure_rolls_back_source_and_effects(runtime_factory, boundary):
    plugin = Projection()
    runtime = runtime_factory([plugin])
    runtime.install()
    connection = runtime.driver.get_connection()
    plugin.fail = True
    with pytest.raises(RuntimeError, match="projection failed"):
        if boundary in {"unit", "native"}:
            scope = (
                transaction_scope(connection, owns_transaction=True)
                if boundary == "unit"
                else connection
            )
            with scope:
                connection.execute("INSERT INTO works VALUES (1, 'source')")
        else:
            connection.execute("INSERT INTO works VALUES (1, 'source')")
            if boundary == "commit":
                connection.commit()
            elif boundary == "sql_commit":
                connection.execute("COMMIT")
            else:
                connection.cursor().execute("COMMIT")
    # SQL completion raises before publishing; the explicit caller still owns rollback.
    connection.rollback()
    assert connection.execute("SELECT * FROM works").fetchall() == []
    assert connection.execute("SELECT * FROM effects").fetchall() == []
    assert runtime.status()["pending"] == []


def test_prompt_nested_failure_preserves_independent_outer_work(runtime_factory):
    plugin = Projection()
    runtime = runtime_factory([plugin])
    runtime.install()
    connection = runtime.driver.get_connection()
    with transaction_scope(connection, owns_transaction=True):
        connection.execute("INSERT INTO effects VALUES (99, 0)")
        plugin.fail = True
        with pytest.raises(RuntimeError, match="projection failed"):
            with transaction_scope(connection, owns_transaction=False):
                connection.execute("INSERT INTO works VALUES (1, 'nested')")
        plugin.fail = False
    assert connection.execute("SELECT * FROM works").fetchall() == []
    assert connection.execute("SELECT * FROM effects").fetchall() == [(99, 0)]


def test_dependent_read_flushes_and_bound_context_expires(runtime_factory):
    plugin = Projection()
    runtime = runtime_factory([plugin])
    runtime.install()
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'source')")
        assert connection.execute("SELECT row_id FROM effects").fetchall() == [(1,)]
    with pytest.raises(MaintenanceTransactionError, match="no longer active"):
        plugin.saved.execute("SELECT * FROM works")


@pytest.mark.parametrize(
    "failure",
    [
        "selection",
        "coalesce",
        "deferred",
        "errors",
        "commit",
        "sql_commit",
        "chained_commit",
        "replace",
        "close",
    ],
)
def test_prompt_contract_failures_abort_the_unit(runtime_factory, failure):
    class Broken(Projection):
        def wants_event(self, event):
            if failure == "selection":
                raise RuntimeError("selection failed")
            return True

        def coalesce_key(self, event):
            if failure == "coalesce":
                raise RuntimeError("coalesce failed")
            return None

        def handle_events(self, context, events):
            if failure == "deferred":
                return MaintenancePluginResult(deferred=1)
            if failure == "errors":
                return MaintenancePluginResult(errors=1)
            if failure == "sql_commit":
                context.connection.execute("COMMIT")
            elif failure == "chained_commit":
                context.connection.execute("INSERT INTO effects VALUES (99, 0); COMMIT")
            elif failure == "replace":
                context.connection.execute(
                    "INSERT OR REPLACE INTO works VALUES (1, 'lost')"
                )
            else:
                getattr(context.connection, failure)()

    runtime = runtime_factory([Broken()])
    runtime.install()
    with pytest.raises((RuntimeError, MaintenanceFailure)):
        with runtime.unit() as connection:
            connection.execute("INSERT INTO works VALUES (1, 'source')")
    connection = runtime.driver.get_connection()
    assert connection.execute("SELECT * FROM works").fetchall() == []


def test_lazy_restart_replay_new_revision_deletion_and_retry(runtime_factory):
    plugin = Projection()
    plugin.mode = MaintenanceMode.LAZY
    runtime = runtime_factory([plugin])
    runtime.install()
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'source')")
    first = runtime.status()["pending"][0]["obligation_id"]
    restarted = runtime_factory([plugin])
    plugin.fail = True
    with pytest.raises(RuntimeError, match="projection failed"):
        restarted.deliver(plugin.name, [first])
    pending = restarted.status()["pending"]
    assert (
        pending[0]["attempts"] == 1 and "projection failed" in pending[0]["last_error"]
    )
    plugin.fail = False
    with restarted.unit() as connection:
        connection.execute("UPDATE works SET title='new' WHERE work_id=1")
    assert restarted.deliver(plugin.name, [first]) == {"handled": 1}
    assert restarted.deliver(plugin.name, [first]) == {"handled": 0}
    remaining = restarted.status()["pending"]
    assert len(remaining) == 1 and remaining[0]["obligation_id"] != first
    with restarted.unit() as connection:
        connection.execute("DELETE FROM works WHERE work_id=1")
    assert json.loads(restarted.status()["pending"][-1]["before_json"]) == {
        "work_id": 1
    }


def test_missing_and_disabled_plugins_preserve_obligations(runtime_factory):
    plugin = Projection()
    plugin.mode = MaintenanceMode.LAZY
    runtime = runtime_factory([plugin])
    runtime.install()
    missing = runtime_factory([])
    with missing.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'source')")
    assert not missing.status()["pending"][0]["implementation_available"]
    runtime.set_enabled(plugin.name, False)
    row = runtime.status()["pending"][0]
    with pytest.raises(MaintenanceFailure, match="unavailable"):
        runtime.deliver(plugin.name, [row["obligation_id"]])
    assert len(runtime.status()["pending"]) == 1


def test_missing_prompt_code_blocks_source_write_after_restart(runtime_factory):
    runtime = runtime_factory([Projection()])
    runtime.install()
    missing = runtime_factory([])
    with pytest.raises(Exception, match="prompt maintenance is unavailable"):
        with missing.unit() as connection:
            connection.execute("INSERT INTO works VALUES (1, 'source')")


def test_readonly_status_does_not_install_schema(runtime_factory):
    runtime = runtime_factory([])
    assert runtime.status()["installed"] is False


def test_declarations_refuse_cycles_and_prompt_dependency_on_lazy(runtime_factory):
    first, second = Projection(), Projection()
    second.name = "second"
    first.dependencies = (second.name,)
    second.dependencies = (first.name,)
    with pytest.raises(ValueError, match="cycle"):
        runtime_factory([first, second])
    second.dependencies = ()
    second.mode = MaintenanceMode.LAZY
    with pytest.raises(ValueError, match="cannot depend on lazy"):
        runtime_factory([first, second])


def test_catalog_revision_summary_and_deleted_relationship(runtime_factory):
    from LiuXin_alpha.catalog.maintenance import (
        WorkRevisionPlugin,
        WorkSummaryPlugin,
        work_summary,
    )

    runtime = runtime_factory([WorkRevisionPlugin(), WorkSummaryPlugin()])
    runtime.install()
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'one'), (2, 'two')")
        connection.execute(
            "INSERT INTO expression_work_links VALUES (1, 1, 10), (2, 1, 20)"
        )
    assert not work_summary(runtime.db, 1)["current"]
    ids = [row["obligation_id"] for row in runtime.status()["pending"]]
    runtime.deliver(WorkSummaryPlugin.name, ids)
    summary = work_summary(runtime.db, 1)
    assert summary["current"] and summary["summary"]["expression_count"] == 2
    with runtime.unit() as connection:
        connection.execute(
            "UPDATE expression_work_links SET expression_work_link_work_id=2 WHERE expression_work_link_id=1"
        )
        connection.execute(
            "DELETE FROM expression_work_links WHERE expression_work_link_id=2"
        )
    assert not work_summary(runtime.db, 1)["current"]
    assert not work_summary(runtime.db, 2)["current"]
    runtime.deliver(
        WorkSummaryPlugin.name,
        [row["obligation_id"] for row in runtime.status()["pending"]],
    )
    assert work_summary(runtime.db, 1)["summary"]["expression_count"] == 0
    assert work_summary(runtime.db, 2)["summary"]["expression_count"] == 1
    with runtime.unit() as connection:
        connection.execute("DELETE FROM works WHERE work_id=2")
    runtime.deliver(
        WorkSummaryPlugin.name,
        [row["obligation_id"] for row in runtime.status()["pending"]],
    )
    assert work_summary(runtime.db, 2)["summary"] == {
        "deleted": True,
        "expression_count": 0,
    }


def test_dependency_order_and_generated_change_cycle(runtime_factory):
    class First(Projection):
        name = "first"
        priority = -100

        def handle_events(self, context, events):
            context.connection.execute("INSERT INTO effects VALUES (10, 0)")
            return MaintenancePluginResult(handled=len(events))

    class Second(Projection):
        name = "second"
        dependencies = ("first",)
        priority = 100

        def handle_events(self, context, events):
            assert context.connection.execute("SELECT row_id FROM effects").fetchall()[
                0
            ] == (10,)
            return super().handle_events(context, events)

    runtime = runtime_factory([Second(), First()])
    runtime.install()
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'source')")
    assert connection is not None


def test_runtime_write_cycle_is_bounded_and_rolled_back(runtime_factory):
    class Changes(Projection):
        def handle_events(self, context, events):
            context.connection.execute("UPDATE works SET title=title || 'x'")
            return MaintenancePluginResult(handled=len(events))

    first, second = Changes(), Changes()
    first.name, second.name = "first", "second"
    runtime = runtime_factory([first, second])
    runtime.install()
    with pytest.raises(MaintenanceFailure, match="cycle"):
        with runtime.unit() as connection:
            connection.execute("INSERT INTO works VALUES (1, 'source')")
    connection = runtime.driver.get_connection()
    assert connection.execute("SELECT * FROM works").fetchall() == []


def test_job_handoff_restart_and_failure_retry(runtime_factory, tmp_path, monkeypatch):
    from LiuXin_alpha.core.maintenance_jobs import (
        MaintenanceJobHandler,
        publish_obligations,
    )
    from LiuXin_alpha.jobs.handler_api import JobHandlerRegistry
    from LiuXin_alpha.jobs.repository import JobRepository
    from LiuXin_alpha.jobs.worker import JobWorker

    plugin = Projection()
    plugin.mode = MaintenanceMode.LAZY
    runtime = runtime_factory([plugin])
    runtime.install()
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'source')")
    repository = JobRepository(tmp_path / "jobs.sqlite")

    @contextmanager
    def open_library():
        yield SimpleNamespace(library=SimpleNamespace(db=runtime_factory([plugin]).db))

    registry = JobHandlerRegistry()
    registry.register(
        MaintenanceJobHandler(library_uuid="test-library", open_library=open_library)
    )
    worker = JobWorker(repository=repository, handlers=registry)
    submit = repository.submit_job

    def crash_after_publish(*args, **kwargs):
        submit(*args, **kwargs)
        raise RuntimeError("crash after ledger commit")

    monkeypatch.setattr(repository, "submit_job", crash_after_publish)
    with pytest.raises(RuntimeError, match="crash after ledger"):
        publish_obligations(runtime, repository, library_uuid="test-library")
    monkeypatch.setattr(repository, "submit_job", submit)
    publish_obligations(runtime, repository, library_uuid="test-library")
    assert len(repository.list_runs()) == 1
    plugin.fail = True
    assert worker.run_once()
    assert len(runtime.status()["pending"]) == 1
    plugin.fail = False
    publish_obligations(runtime, repository, library_uuid="test-library")
    assert worker.run_once()
    assert runtime.status()["pending"] == []
    assert runtime.driver.get_connection().execute(
        "SELECT row_id FROM effects"
    ).fetchall() == [(1,)]


def test_job_receipt_crash_replays_without_repeating_effects(
    runtime_factory, tmp_path, monkeypatch
):
    from LiuXin_alpha.core.maintenance_jobs import (
        MaintenanceJobHandler,
        publish_obligations,
    )
    from LiuXin_alpha.jobs.handler_api import JobHandlerRegistry
    from LiuXin_alpha.jobs.repository import JobRepository
    from LiuXin_alpha.jobs.worker import JobWorker

    plugin = Projection()
    plugin.mode = MaintenanceMode.LAZY
    runtime = runtime_factory([plugin])
    runtime.install()
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'source')")
    repository = JobRepository(tmp_path / "jobs.sqlite")

    @contextmanager
    def open_library():
        yield SimpleNamespace(library=SimpleNamespace(db=runtime_factory([plugin]).db))

    registry = JobHandlerRegistry()
    registry.register(
        MaintenanceJobHandler(library_uuid="test-library", open_library=open_library)
    )
    worker = JobWorker(repository=repository, handlers=registry)
    run_id = publish_obligations(runtime, repository, library_uuid="test-library")[
        "run_ids"
    ][0]
    finish = repository.mark_succeeded

    def crash(*args, **kwargs):
        raise RuntimeError("job receipt unavailable")

    monkeypatch.setattr(repository, "mark_succeeded", crash)
    worker.run_once()
    assert runtime.status()["pending"] == []
    assert runtime.prune_completed() == 1
    repository.retry_run(
        run_id, request_key="replay", acknowledge_possible_effects=True
    )
    monkeypatch.setattr(repository, "mark_succeeded", finish)
    worker.run_once()
    assert runtime.driver.get_connection().execute(
        "SELECT row_id FROM effects"
    ).fetchall() == [(1,)]


def test_lost_claim_before_commit_rolls_back_handler_effects(runtime_factory):
    plugin = Projection()
    plugin.mode = MaintenanceMode.LAZY
    runtime = runtime_factory([plugin])
    runtime.install()
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'source')")
    calls = 0

    def check_claim():
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("lease lost")

    ids = [row["obligation_id"] for row in runtime.status()["pending"]]
    with pytest.raises(RuntimeError, match="lease lost"):
        runtime.deliver(plugin.name, ids, check_claim=check_claim)
    assert len(runtime.status()["pending"]) == 1
    assert (
        runtime.driver.get_connection().execute("SELECT * FROM effects").fetchall()
        == []
    )


def test_edit_while_delivery_runs_remains_pending(runtime_factory):
    import threading
    from concurrent.futures import ThreadPoolExecutor

    started, proceed, writer_started = (
        threading.Event(),
        threading.Event(),
        threading.Event(),
    )

    class Slow(Projection):
        mode = MaintenanceMode.LAZY

        def handle_events(self, context, events):
            started.set()
            assert proceed.wait(10)
            return super().handle_events(context, events)

    plugin = Slow()
    runtime = runtime_factory([plugin])
    runtime.install()
    writer = runtime_factory([plugin])
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'source')")
    identities = [row["obligation_id"] for row in runtime.status()["pending"]]

    def edit():
        writer_started.set()
        with writer.unit() as connection:
            connection.execute("UPDATE works SET title='newer' WHERE work_id=1")

    with ThreadPoolExecutor(max_workers=2) as pool:
        delivery = pool.submit(runtime.deliver, plugin.name, identities)
        assert started.wait(10)
        editing = pool.submit(edit)
        assert writer_started.wait(10)
        proceed.set()
        delivery.result(timeout=15)
        editing.result(timeout=15)
    remaining = runtime.status()["pending"]
    assert len(remaining) == 1
    assert remaining[0]["obligation_id"] not in identities


def test_exhausted_retry_requires_explicit_reset(runtime_factory):
    plugin = Projection()
    plugin.mode = MaintenanceMode.LAZY
    plugin.retry_limit = 0
    runtime = runtime_factory([plugin])
    runtime.install()
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'source')")
    identities = [row["obligation_id"] for row in runtime.status()["pending"]]
    plugin.fail = True
    with pytest.raises(RuntimeError, match="projection failed"):
        runtime.deliver(plugin.name, identities)
    plugin.fail = False
    with pytest.raises(MaintenanceFailure, match="exhausted"):
        runtime.deliver(plugin.name, identities)
    assert runtime.retry(plugin.name) == 1
    assert runtime.status()["pending"][0]["retry_generation"] == 1
    assert runtime.deliver(plugin.name, identities)["handled"] == 1


def test_repeated_expired_workers_consume_retry_budget(runtime_factory, tmp_path):
    from LiuXin_alpha.core.maintenance_jobs import publish_obligations
    from LiuXin_alpha.jobs.repository import JobRepository

    plugin = Projection()
    plugin.mode = MaintenanceMode.LAZY
    plugin.retry_limit = 1
    runtime = runtime_factory([plugin])
    runtime.install()
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'source')")
    repository = JobRepository(tmp_path / "jobs.sqlite")
    publish_obligations(runtime, repository, library_uuid="test-library")
    for attempt in (1, 2):
        leased = repository.lease_next_run(worker_id="lost", lease_for_s=60)
        assert leased is not None
        repository.mark_running(
            leased.job_run_id, worker_id="lost", lease_token=leased.lease_token
        )
        with repository.connect() as connection:
            connection.execute(
                "UPDATE job_runs SET lease_expires_timestamp_ep_k=0 WHERE job_run_id=?",
                (leased.job_run_id,),
            )
        publish_obligations(runtime, repository, library_uuid="test-library")
        assert runtime.status()["pending"][0]["attempts"] == attempt
        publish_obligations(runtime, repository, library_uuid="test-library")
    assert repository.lease_next_run(worker_id="replacement", lease_for_s=60) is None
    assert runtime.status()["pending"]


def test_missing_implementation_does_not_starve_other_plugins(
    runtime_factory, tmp_path
):
    from LiuXin_alpha.core.maintenance_jobs import publish_obligations
    from LiuXin_alpha.jobs.repository import JobRepository

    missing, available = Projection(), Projection()
    missing.name, available.name = "missing", "available"
    missing.mode = available.mode = MaintenanceMode.LAZY
    runtime = runtime_factory([missing, available])
    runtime.install()
    with runtime.unit() as connection:
        for row_id in range(1, 6):
            connection.execute("INSERT INTO works VALUES (?, 'source')", (row_id,))
    restarted = runtime_factory([available])
    repository = JobRepository(tmp_path / "jobs.sqlite")
    assert (
        publish_obligations(
            restarted, repository, library_uuid="test-library", limit=1
        )["published"]
        == 1
    )


def test_sqlite_scripts_do_not_commit_outer_work_and_roll_back_partial_effects(
    runtime_factory,
):
    plugin = Projection()
    runtime = runtime_factory([plugin])
    if runtime.schema:
        pytest.skip("SQLite's script pre-commit needs this dedicated regression")
    runtime.install()
    connection = runtime.driver.get_connection()
    connection.execute("INSERT INTO effects VALUES (99, 0)")
    with pytest.raises(Exception, match="no such table"):
        connection.executescript(
            "INSERT INTO works VALUES (1, 'partial'); INSERT INTO missing_table VALUES (1);"
        )
    assert connection.execute("SELECT * FROM works").fetchall() == []
    assert connection.execute("SELECT * FROM effects").fetchall() == [(99, 0)]
    connection.rollback()
    assert connection.execute("SELECT * FROM effects").fetchall() == []
    connection.executescript(
        "CREATE TABLE extra(value TEXT); INSERT INTO works VALUES (2, 'kept');"
    )
    assert connection.execute("SELECT row_id FROM effects").fetchall() == [(2,)]
    with pytest.raises(RuntimeError, match="transaction controls"):
        connection.executescript("INSERT INTO works VALUES (3, 'discarded'); COMMIT;")
    assert connection.execute("SELECT work_id FROM works").fetchall() == [(2,)]


def test_existing_uninstalled_handle_cannot_bypass_new_prompt_requirements(
    runtime_factory,
):
    plugin = Projection()
    early = runtime_factory([plugin])
    connection = early.driver.get_connection()
    connection.execute("INSERT INTO works VALUES (1, 'before installation')")
    connection.commit()
    late = runtime_factory([plugin])
    late.install()
    # Refresh another handle's registry while the old handle retains its absent-schema cache.
    assert early.status()["installed"]
    with pytest.raises(Exception, match="prompt maintenance is unavailable"):
        connection.execute("INSERT INTO works VALUES (2, 'must not bypass')")
    connection.rollback()
    assert connection.execute("SELECT work_id FROM works").fetchall() == [(1,)]


def test_readonly_transaction_does_not_process_lazy_work(runtime_factory):
    plugin = Projection()
    plugin.mode = MaintenanceMode.LAZY
    runtime = runtime_factory([plugin])
    runtime.install()
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'pending')")
    connection = runtime.driver.get_connection()
    if runtime.schema:
        connection.execute("BEGIN READ ONLY")
    else:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
    assert connection.execute("SELECT * FROM effects").fetchall() == []
    connection.commit()
    assert len(runtime.status()["pending"]) == 1


def test_lazy_backoff_and_cancelled_jobs_require_explicit_retry(
    runtime_factory, tmp_path
):
    from LiuXin_alpha.core.maintenance_jobs import publish_obligations
    from LiuXin_alpha.jobs.repository import JobRepository

    plugin = Projection()
    plugin.mode = MaintenanceMode.LAZY
    plugin.retry_backoff_s = 60
    runtime = runtime_factory([plugin])
    runtime.install()
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'source')")
    identities = [row["obligation_id"] for row in runtime.status()["pending"]]
    plugin.fail = True
    with pytest.raises(RuntimeError, match="projection failed"):
        runtime.deliver(plugin.name, identities)
    repository = JobRepository(tmp_path / "jobs.sqlite")
    assert (
        publish_obligations(runtime, repository, library_uuid="test-library")[
            "published"
        ]
        == 0
    )
    runtime.retry(plugin.name)
    first = publish_obligations(runtime, repository, library_uuid="test-library")[
        "run_ids"
    ][0]
    repository.request_cancel(first)
    publish_obligations(runtime, repository, library_uuid="test-library")
    assert "cancelled" in runtime.status()["pending"][0]["last_error"]
    assert (
        publish_obligations(runtime, repository, library_uuid="test-library")[
            "published"
        ]
        == 0
    )
    assert runtime.retry(plugin.name) == 1
    second = publish_obligations(runtime, repository, library_uuid="test-library")[
        "run_ids"
    ][0]
    assert second != first


def test_failed_installation_leaves_no_partial_journal(runtime_factory):
    plugin = Projection()
    plugin.tables = ("missing_source",)
    runtime = runtime_factory([plugin])
    with pytest.raises(ValueError, match="physical table"):
        runtime.install()
    assert runtime.status()["installed"] is False


def test_postgres_statement_chaining_cannot_commit_before_prompt_flush(runtime_factory):
    runtime = runtime_factory([Projection()])
    if not runtime.schema:
        pytest.skip("SQLite already rejects multiple commands per execute")
    runtime.install()
    with pytest.raises(MaintenanceTransactionError, match="one row statement"):
        with runtime.unit() as connection:
            connection.execute("INSERT INTO works VALUES (1, 'must roll back'); COMMIT")
    connection = runtime.driver.get_connection()
    assert connection.execute("SELECT * FROM works").fetchall() == []


def test_failed_savepoint_cleanup_does_not_dispatch_pending_outer_work(runtime_factory):
    calls = []

    class Recorder(Projection):
        def handle_events(self, context, events):
            calls.append(tuple(event.row_id for event in events))
            return super().handle_events(context, events)

    plugin = Recorder()
    runtime = runtime_factory([plugin])
    runtime.install()
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'outer')")
        plugin.fail = True
        with pytest.raises(RuntimeError, match="projection failed"):
            with transaction_scope(connection, owns_transaction=False):
                connection.execute("INSERT INTO works VALUES (2, 'inner')")
        assert calls == [(1, 2)]
        plugin.fail = False
    assert calls == [(1, 2), (1,)]
    connection = runtime.driver.get_connection()
    assert connection.execute("SELECT row_id FROM effects").fetchall() == [(1,)]


@pytest.mark.parametrize("operation", ["INSERT", "UPDATE", "DELETE"])
def test_cte_source_changes_share_prompt_rollback(runtime_factory, operation):
    plugin = Projection()
    runtime = runtime_factory([plugin])
    runtime.install()
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'before')")
    connection = runtime.driver.get_connection()
    statements = {
        "INSERT": "INSERT INTO works SELECT id, 'new' FROM source",
        "UPDATE": "UPDATE works SET title='new' WHERE work_id IN (SELECT id-1 FROM source)",
        "DELETE": "DELETE FROM works WHERE work_id IN (SELECT id-1 FROM source)",
    }
    plugin.fail = True
    with pytest.raises(RuntimeError, match="projection failed"):
        with connection:
            connection.execute(
                "/* CTE write */ WITH source(id) AS (VALUES(2)) "
                + statements[operation]
            )
    assert connection.execute("SELECT * FROM works").fetchall() == [(1, "before")]
    assert connection.execute("SELECT COUNT(*) FROM effects").fetchone()[0] == 1
    plugin.fail = False
    with connection:
        connection.execute("WITH source(id) AS (VALUES(2)) " + statements[operation])
    assert connection.execute("SELECT COUNT(*) FROM effects").fetchone()[0] == 2


def test_sqlite_cte_reads_remain_readonly_and_autocommit_writes_refuse(runtime_factory):
    runtime = runtime_factory([Projection()])
    if runtime.schema:
        pytest.skip("SQLite implicit transaction behavior")
    runtime.install()
    connection = runtime.driver.get_connection()
    connection.isolation_level = None
    assert connection.execute(
        "WITH data AS (SELECT 1) SELECT * FROM data"
    ).fetchall() == [(1,)]
    assert not connection.in_transaction
    with pytest.raises(MaintenanceTransactionError, match="explicit transaction"):
        connection.execute(
            "WITH data AS (SELECT 1) INSERT INTO works SELECT 1, 'x' FROM data"
        )
    assert connection.execute("SELECT * FROM works").fetchall() == []
    with transaction_scope(connection, owns_transaction=True):
        connection.execute(
            "WITH data AS (SELECT 1) INSERT INTO works SELECT 1, 'x' FROM data"
        )
    assert connection.execute("SELECT row_id FROM effects").fetchall() == [(1,)]


@pytest.mark.parametrize("custom", [False, True])
def test_sqlite_custom_cursor_created_before_install_keeps_completion_hooks(
    runtime_factory, custom
):
    plugin = Projection()
    runtime = runtime_factory([plugin])
    if runtime.schema:
        pytest.skip("SQLite custom cursor factories")
    calls = []

    class CustomCursor(sqlite3.Cursor):
        def execute(self, sql, parameters=()):
            calls.append(sql)
            return super().execute(sql, parameters)

    connection = runtime.driver.get_connection()
    cursor = connection.cursor(factory=CustomCursor if custom else sqlite3.Cursor)
    with pytest.raises(TypeError, match="Cursor factories"):
        connection.cursor(factory=lambda active: sqlite3.Cursor(active))
    runtime.install()
    cursor.execute("INSERT INTO works VALUES (1, 'source')")
    plugin.fail = True
    with pytest.raises(RuntimeError, match="projection failed"):
        cursor.execute("COMMIT")
    assert connection.execute("SELECT * FROM works").fetchall() == []
    if custom:
        assert calls == ["INSERT INTO works VALUES (1, 'source')"]


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT OR REPLACE INTO expression_work_links VALUES (1,2,50)",
        "REPLACE INTO expression_work_links VALUES (1,2,50)",
        "WITH moved AS (SELECT 1) INSERT /* conflict */ OR REPLACE INTO expression_work_links VALUES (1,2,50)",
        "UPDATE OR REPLACE expression_work_links SET expression_work_link_work_id=2",
    ],
)
def test_sqlite_replacement_refuses_lost_endpoint_and_upsert_captures_both(
    runtime_factory, statement
):
    from LiuXin_alpha.catalog.maintenance import (
        WorkRevisionPlugin,
        WorkSummaryPlugin,
        work_summary,
    )

    runtime = runtime_factory([WorkRevisionPlugin(), WorkSummaryPlugin()])
    if runtime.schema:
        pytest.skip("SQLite replacement conflicts")
    runtime.install()
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1,'old'),(2,'new')")
        connection.execute("INSERT INTO expression_work_links VALUES (1,1,50)")
    runtime.deliver(
        WorkSummaryPlugin.name,
        [row["obligation_id"] for row in runtime.status()["pending"]],
    )
    with pytest.raises(MaintenanceTransactionError, match="REPLACE conflict"):
        with runtime.unit() as connection:
            connection.execute(statement)
    assert work_summary(runtime.db, 1)["current"]
    assert work_summary(runtime.db, 1)["summary"]["expression_count"] == 1
    with runtime.unit() as connection:
        connection.execute(
            "INSERT INTO expression_work_links VALUES (1,2,50) ON CONFLICT(expression_work_link_id) "
            "DO UPDATE SET expression_work_link_work_id=excluded.expression_work_link_work_id"
        )
    assert not work_summary(runtime.db, 1)["current"]
    runtime.deliver(
        WorkSummaryPlugin.name,
        [row["obligation_id"] for row in runtime.status()["pending"]],
    )
    assert work_summary(runtime.db, 1)["summary"]["expression_count"] == 0
    assert work_summary(runtime.db, 2)["summary"]["expression_count"] == 1
