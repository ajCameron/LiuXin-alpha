"""Coordinate independent writers without relying on facade mutexes or scheduling."""

import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from threading import Event

import pytest

from LiuXin_alpha.databases.custom_columns import (
    CustomFieldError,
    CustomFieldService,
    OwnerRef,
    StalePlan,
)
from LiuXin_alpha.databases.database import Database
from tests.databases.test_transaction_ownership_postgres_live import (
    pg_database as pg_database,
)


def wait_for_advisory_lock(reader, pid, future):
    """Observe the real backend waiting before releasing the protected first write."""
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if future.done():
            pytest.fail(
                f"Competing native operation escaped its writer lock: {future.result()!r}"
            )
        reader.rollback()
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT wait_event_type,wait_event FROM pg_stat_activity WHERE pid=%s",
                (pid,),
            )
            state = cursor.fetchone()
        if state == ("Lock", "advisory"):
            return
        time.sleep(0.01)
    pytest.fail("Competing backend did not reach the advisory writer lock")


def competing_operations(
    pg_database,
    monkeypatch,
    *,
    outer,
    same_owner,
    migration=False,
    rollback_before=False,
):
    """Pause after the first revision read and inspect the second physical backend."""
    first_db, metadata, reader = pg_database
    with Database(
        metadata=metadata, db_type="PostgreSQL", repair_bootstrap_rows=False
    ) as second_db:
        first, second = CustomFieldService(first_db), CustomFieldService(second_db)
        field = first.create_field("works", "concurrent", datatype="comments")
        owners = [
            OwnerRef(
                "works", first_db.macros.insert_row("works", {"work_title": label})
            )
            for label in ("first", "second")
        ]
        if same_owner:
            owners[1] = owners[0]
        plan = (
            second.plan_field_migration(field.id, {"mode": "list"})
            if migration
            else None
        )
        ready, release, connected = Event(), Event(), Event()
        second_pid = []
        original_write = first.repository.write_entries
        original_connect = second_db.driver.get_connection
        staging_rollback = [rollback_before]
        first_receipts, second_receipts = [], []
        first.subscribe(first_receipts.append)
        second.subscribe(second_receipts.append)

        def pause_first(*args, **kwargs):
            if not staging_rollback[0]:
                ready.set()
                assert release.wait(20), "First writer was not released"
            return original_write(*args, **kwargs)

        def capture_connection():
            connection = original_connect()
            second_pid.append(connection.raw_connection.get_backend_pid())
            connected.set()
            return connection

        monkeypatch.setattr(first.repository, "write_entries", pause_first)
        monkeypatch.setattr(second_db.driver, "get_connection", capture_connection)

        def operation(service, owner, value, is_first):
            context = (
                nullcontext()
                if outer == "standalone"
                else service.db.macros.transaction(serialized=outer == "serialized")
            )
            try:
                with context:
                    if is_first and rollback_before:
                        with pytest.raises(RuntimeError, match="discard inner write"):
                            with service.write():
                                service.set_value(owner, field.id, "discarded")
                                raise RuntimeError("discard inner write")
                        staging_rollback[0] = False
                    if migration and not is_first:
                        service.apply_field_migration(plan)
                    else:
                        service.set_value(owner, field.id, value, expected_revision=0)
                return "committed"
            except CustomFieldError as error:
                return error

        with ThreadPoolExecutor(max_workers=2) as workers:
            a = workers.submit(operation, first, owners[0], "first write", True)
            try:
                assert ready.wait(15)
                b = workers.submit(operation, second, owners[1], "second write", False)
                assert connected.wait(15)
                wait_for_advisory_lock(reader, second_pid[0], b)
            finally:
                release.set()
            assert a.result(timeout=20) == "committed"
            error = b.result(timeout=20)
        assert isinstance(error, StalePlan if migration else CustomFieldError)
        if not migration:
            assert str(error) == "Field revision conflict"
        definition = first.get_field(field.id)
        assert definition.revision == 1
        assert definition.mode == "scalar"
        assert len(first_receipts) == 1
        assert second_receipts == []
        reader.rollback()
        with reader.cursor() as cursor:
            cursor.execute(
                f'SELECT custom_column_{field.id}_value FROM "{metadata["schema"]}".custom_column_{field.id}'
            )
            assert cursor.fetchall() == [("first write",)]


@pytest.mark.parametrize("outer", ["standalone", "ordinary", "serialized"])
@pytest.mark.parametrize("same_owner", [False, True])
def test_native_revision_check_serializes_competing_writers(
    pg_database, monkeypatch, outer, same_owner
):
    """Exactly one expected-revision edit can commit, even inside ordinary units."""
    competing_operations(pg_database, monkeypatch, outer=outer, same_owner=same_owner)


def test_nested_migration_checks_fingerprint_after_writer_reservation(
    pg_database, monkeypatch
):
    """A pending migration observes its competitor's committed revision before DDL."""
    competing_operations(
        pg_database, monkeypatch, outer="ordinary", same_owner=True, migration=True
    )


def test_native_writer_reacquires_reservation_after_savepoint_rollback(
    pg_database, monkeypatch
):
    """A rolled-back inner write cannot leave an assumed but released reservation."""
    competing_operations(
        pg_database,
        monkeypatch,
        outer="ordinary",
        same_owner=True,
        rollback_before=True,
    )
