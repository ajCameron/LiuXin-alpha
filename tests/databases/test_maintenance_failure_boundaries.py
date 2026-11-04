"""Exercise maintenance boundaries that can otherwise publish partial effects."""

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest

from LiuXin_alpha.databases.db_types.maintenance_types import MaintenanceMode
from tests.databases.test_maintenance_runtime import Projection
from tests.databases.test_maintenance_runtime import runtime_factory as runtime_factory


@pytest.mark.parametrize("boundary", ("dependent_read", "explicit_flush"))
def test_caught_prompt_failure_does_not_commit_its_partial_effects(
    runtime_factory, boundary
):
    class FailsOnce(Projection):
        calls = 0

        def handle_events(self, context, events):
            result = super().handle_events(context, events)
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("transient projection failure")
            return result

    plugin = FailsOnce()
    runtime = runtime_factory([plugin])
    runtime.install()
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'source')")
        with pytest.raises(RuntimeError, match="transient projection failure"):
            if boundary == "dependent_read":
                connection.execute("SELECT * FROM effects")
            else:
                runtime.flush(connection)
        # The caller can recover and commit, but failed handler effects must
        # not survive alongside the successful second attempt.
    connection = runtime.driver.get_connection()
    assert connection.execute("SELECT title FROM works").fetchall() == [("source",)]
    assert len(connection.execute("SELECT * FROM effects").fetchall()) == 1
    assert plugin.calls == 2


@pytest.mark.parametrize("setting", ("isolation_level", "autocommit"))
@pytest.mark.parametrize(
    "autocommit", (sqlite3.LEGACY_TRANSACTION_CONTROL, False, True)
)
@pytest.mark.parametrize("fail", (False, True))
def test_sqlite_implicit_commit_settings_cannot_bypass_prompt_work(
    runtime_factory, setting, autocommit, fail
):
    plugin = Projection()
    runtime = runtime_factory([plugin])
    if runtime.schema:
        pytest.skip("SQLite connection setting boundary")
    runtime.install()
    connection = runtime.driver.get_connection()
    if setting == "isolation_level" and autocommit is True:
        pytest.skip("Native isolation_level setter does not commit in autocommit=True")
    connection.autocommit = autocommit
    if autocommit is True:
        connection.execute("BEGIN")
    connection.execute("INSERT INTO works VALUES (1, 'source')")
    plugin.fail = fail
    if fail:
        with pytest.raises(RuntimeError, match="projection failed"):
            setattr(connection, setting, None if setting == "isolation_level" else True)
    else:
        setattr(connection, setting, None if setting == "isolation_level" else True)
    plugin.fail = False
    observer = runtime.driver.get_connection()
    assert observer.execute("SELECT work_id FROM works").fetchall() == (
        [] if fail else [(1,)]
    )
    assert observer.execute("SELECT row_id FROM effects").fetchall() == (
        [] if fail else [(1,)]
    )
    connection.rollback()


def test_sqlite_autocommit_requires_explicit_write_unit(runtime_factory):
    from LiuXin_alpha.databases.maintenance.bound_connection import (
        MaintenanceTransactionError,
    )

    runtime = runtime_factory([Projection()])
    if runtime.schema:
        pytest.skip("SQLite autocommit boundary")
    runtime.install()
    connection = runtime.driver.get_connection()
    connection.autocommit = True
    with pytest.raises(MaintenanceTransactionError, match="explicit transaction"):
        connection.execute("INSERT INTO works VALUES (1, 'refused')")
    assert connection.execute("SELECT * FROM works").fetchall() == []


@pytest.mark.parametrize("failure", ("python", "sql"))
def test_caught_drain_failure_rolls_back_earlier_batches_and_receipts(
    runtime_factory, failure
):
    class FailsOnce(Projection):
        name = "second"
        dependencies = ("projection",)
        calls = 0

        def handle_events(self, context, events):
            result = super().handle_events(context, events)
            self.calls += 1
            if self.calls == 1:
                if failure == "python":
                    raise RuntimeError("batch failed")
                context.connection.execute("INSERT INTO works VALUES (1, 'duplicate')")
            return result

    runtime = runtime_factory([FailsOnce(), Projection()])
    runtime.install()
    error_type = RuntimeError
    if failure == "sql":
        if runtime.schema:
            from psycopg2 import IntegrityError
        else:
            IntegrityError = sqlite3.IntegrityError
        error_type = IntegrityError
    with runtime.unit() as connection:
        connection.executemany(
            "INSERT INTO works VALUES (?, ?)", [(i, str(i)) for i in range(1, 301)]
        )
        with pytest.raises(error_type):
            runtime.flush(connection)
        with runtime._internal():
            assert connection.execute("SELECT COUNT(*) FROM effects").fetchone() == (0,)
            assert connection.execute(
                "SELECT COUNT(*) FROM maintenance_obligations WHERE completed_ms IS NOT NULL"
            ).fetchone() == (0,)
            assert connection.execute("SELECT COUNT(*) FROM works").fetchone() == (300,)
    connection = runtime.driver.get_connection()
    assert connection.execute("SELECT COUNT(*) FROM effects").fetchone() == (600,)
    assert connection.execute(
        "SELECT COUNT(*) FROM maintenance_obligations WHERE completed_ms IS NULL"
    ).fetchone() == (0,)


@pytest.mark.parametrize("mode", (MaintenanceMode.PROMPT, MaintenanceMode.LAZY))
@pytest.mark.parametrize("external", (False, True))
def test_postgres_truncate_cannot_silently_skip_capture(
    runtime_factory, mode, external
):
    plugin = Projection()
    plugin.mode = mode
    runtime = runtime_factory([plugin])
    if not runtime.schema:
        pytest.skip("PostgreSQL TRUNCATE boundary")
    runtime.install()
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'kept')")
    from psycopg2.errors import RaiseException

    with pytest.raises(RaiseException, match="TRUNCATE; use DELETE"):
        with runtime.unit() as connection:
            cursor = (
                connection.raw_connection.cursor() if external else connection.cursor()
            )
            cursor.execute(
                "/* maintenance must see every deleted row */ TRUNCATE works CASCADE"
            )
    connection = runtime.driver.get_connection()
    assert connection.execute("SELECT title FROM works").fetchall() == [("kept",)]


@pytest.mark.parametrize("operation", ("install", "deliver"))
def test_same_runtime_writer_can_commit_while_maintenance_waits(
    runtime_factory, monkeypatch, operation
):
    plugin = Projection()
    plugin.mode = MaintenanceMode.LAZY
    runtime = runtime_factory([plugin])
    runtime.install()
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'initial')")
    ids = [row["obligation_id"] for row in runtime.status()["pending"]]
    source = runtime.driver.get_connection()
    source.execute("INSERT INTO works VALUES (2, 'concurrent')")
    waiting = threading.Event()
    connect, unit = runtime.driver.get_connection, runtime.unit

    def bounded_connect():
        connection = connect()
        with runtime._internal():
            # The timeout makes the regression fail instead of hanging. Success
            # requires the writer to finish before the worker acquires its unit.
            connection.execute(
                "SET statement_timeout='1s'"
                if runtime.schema
                else "PRAGMA busy_timeout=1000"
            )
            connection.commit()
        return connection

    @contextmanager
    def observed_unit():
        waiting.set()
        with unit() as connection:
            yield connection

    monkeypatch.setattr(runtime.driver, "get_connection", bounded_connect)
    monkeypatch.setattr(runtime, "unit", observed_unit)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = (
            pool.submit(runtime.install)
            if operation == "install"
            else pool.submit(runtime.deliver, plugin.name, ids)
        )
        assert waiting.wait(10)
        source.commit()
        future.result(timeout=10)
    assert source.execute("SELECT work_id FROM works ORDER BY work_id").fetchall() == [
        (1,),
        (2,),
    ]


@pytest.mark.parametrize("replacement", ("retry", "ledger"))
@pytest.mark.parametrize("claim_lost", (False, True))
def test_stale_job_failure_cannot_spend_successors_retry_budget(
    runtime_factory, tmp_path, replacement, claim_lost
):
    from LiuXin_alpha.core.maintenance_jobs import publish_obligations
    from LiuXin_alpha.jobs.repository import JobRepository

    plugin = Projection()
    plugin.mode = MaintenanceMode.LAZY
    runtime = runtime_factory([plugin])
    runtime.install()
    with runtime.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'source')")
    repository = JobRepository(tmp_path / "failure-jobs.sqlite")
    old_run = publish_obligations(runtime, repository, library_uuid="test-library")[
        "run_ids"
    ][0]
    row = runtime.status()["pending"][0]
    plugin.fail = True
    with pytest.raises(RuntimeError, match="projection failed"):
        runtime.deliver(plugin.name, [row["obligation_id"]], job_run_id=old_run)
    assert runtime.status()["pending"][0]["attempts"] == 1

    # Operator retry assigns a fresh job generation. The old process may still
    # wake up and notice that its claim has been revoked.
    old_ledger = repository.ledger_uuid
    if replacement == "retry":
        runtime.retry(plugin.name)
    else:
        repository = JobRepository(tmp_path / "replacement-jobs.sqlite")
    new_run = publish_obligations(runtime, repository, library_uuid="test-library")[
        "run_ids"
    ][0]
    assert (new_run != old_run) == (replacement == "retry")
    before = runtime.status()["pending"][0]

    def expired_claim():
        raise RuntimeError("old claim expired")

    message = "old claim expired" if claim_lost else "delivery was superseded"
    with pytest.raises(RuntimeError, match=message):
        runtime.deliver(
            plugin.name,
            [row["obligation_id"]],
            job_run_id=old_run,
            job_ledger_uuid=old_ledger,
            check_claim=expired_claim if claim_lost else lambda: None,
        )
    remaining = runtime.status()["pending"][0]
    assert remaining["job_run_id"] == new_run
    assert remaining["attempts"] == before["attempts"]
    assert remaining["last_error"] == before["last_error"]
    assert remaining["last_failure_run_id"] is None
    for _ in range(2):
        with pytest.raises(RuntimeError, match="projection failed"):
            runtime.deliver(
                plugin.name,
                [row["obligation_id"]],
                job_run_id=new_run,
                job_ledger_uuid=repository.ledger_uuid,
            )
    assert runtime.status()["pending"][0]["attempts"] == before["attempts"] + 1
    plugin.fail = False
    runtime.deliver(
        plugin.name,
        [row["obligation_id"]],
        job_run_id=new_run,
        job_ledger_uuid=repository.ledger_uuid,
    )
    assert runtime.status()["pending"] == []
