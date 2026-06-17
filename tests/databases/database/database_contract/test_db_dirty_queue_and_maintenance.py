"""Database contract: dirtied-record queues + maintenance bot integration.

This chunk tests two related but distinct mechanisms:

1) ``Database.dirty_records_queue``
   - A plain Queue that the Database exposes for "dirtied" records.
   - ``Database.dirty_record()`` should enqueue ``(table, row_id, reason)`` only
     for tables in ``db.dirtiable_tables``.

2) The background maintenance bot integration
   - Drivers register SQLite functions (e.g. ``DIRTY_RECORD``) that call back
     into the maintainer.
   - The maintainer enqueues "dirtied" events into its own queues.

These tests are written to be stable:
* We explicitly stop the maintainer thread inside tests that inspect its queues,
  to avoid races where the background thread consumes events.
* We create our own tiny table + trigger to validate the callback plumbing,
  rather than depending on whatever triggers exist in a particular fixture DB.
"""

from __future__ import annotations

import queue
import threading
import time
from typing import Any

import pytest


def _drain(q: queue.Queue) -> list[Any]:
    out: list[Any] = []
    while True:
        try:
            out.append(q.get_nowait())
        except queue.Empty:
            return out


def _stop_maintainer_thread(db) -> None:
    """Stop and briefly join the background maintenance thread (best-effort)."""

    maint = getattr(db, "maintenance", None)
    th = getattr(maint, "maintainer", None)
    if th is None:
        return
    if hasattr(th, "stop"):
        try:
            th.stop()
        except Exception:
            pass
    try:
        th.join(timeout=1)
    except Exception:
        pass


@pytest.fixture
def stopped_db(open_db):
    """A Database where the maintainer thread is stopped to avoid queue races."""

    _stop_maintainer_thread(open_db)
    # Drain any stale events (some fixtures do schema checks on startup).
    _drain(open_db.maintenance.main_table_dirtied_queue)
    _drain(open_db.maintenance.interlink_dirtied_queue)
    yield open_db


def test_dirty_records_queue_is_shared_with_driver_and_wrapper(open_db):
    assert open_db.dirty_records_queue is not None
    assert isinstance(open_db.dirty_records_queue, queue.Queue)

    assert getattr(open_db.driver, "dirty_records_queue", None) is open_db.dirty_records_queue
    assert getattr(open_db.driver_wrapper, "dirty_records_queue", None) is open_db.dirty_records_queue


def test_get_dirtied_count_tracks_queue_size(open_db):
    start = open_db.get_dirtied_count()
    # Use a valid dirtiable table if possible.
    table = "books" if "books" in open_db.dirtiable_tables else sorted(open_db.dirtiable_tables)[0]

    for i in range(10):
        open_db.direct_dirty_record(table, i + 1, reason="unit-test")

    end = open_db.get_dirtied_count()
    assert end >= start + 10


def test_database_dirty_record_enqueues_for_dirtiable_table(open_db):
    # Choose a stable table name.
    table = "books" if "books" in open_db.dirtiable_tables else sorted(open_db.dirtiable_tables)[0]
    before = open_db.get_dirtied_count()

    open_db.direct_dirty_record(table, 123, reason="update")

    assert open_db.get_dirtied_count() >= before + 1
    got = open_db.dirty_records_queue.get_nowait()
    assert got == (table, 123, "update")


def test_database_dirty_record_warns_and_does_not_enqueue_for_unknown_table(open_db, monkeypatch):
    calls: list[tuple] = []

    from LiuXin_alpha.utils.logging import default_log

    def _fake_log_variables(*args, **kwargs):
        calls.append((args, kwargs))
        return args[0] if args else ""

    monkeypatch.setattr(default_log, "log_variables", _fake_log_variables)

    before = open_db.get_dirtied_count()
    open_db.direct_dirty_record("definitely_not_a_real_table", 1, reason="nope")
    after = open_db.get_dirtied_count()

    assert after == before
    assert calls, "Expected a warning log when dirtying a non-dirtiable table"


def test_database_dirty_record_is_threadsafe(open_db):
    table = "books" if "books" in open_db.dirtiable_tables else sorted(open_db.dirtiable_tables)[0]
    start = open_db.get_dirtied_count()

    def worker(tid: int) -> None:
        for j in range(200):
            open_db.direct_dirty_record(table, tid * 1_000_000 + j, reason="thread")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=2)

    end = open_db.get_dirtied_count()
    assert end >= start + (5 * 200)


def test_database_dirty_record_does_not_enqueue_to_maintainer_queues(stopped_db):
    """Database.dirty_record() uses Database.dirty_records_queue, not the maintainer queues."""

    table = "books" if "books" in stopped_db.dirtiable_tables else sorted(stopped_db.dirtiable_tables)[0]
    _drain(stopped_db.maintenance.main_table_dirtied_queue)

    stopped_db.direct_dirty_record(table, 77, reason="db-level")
    assert stopped_db.dirty_records_queue.get_nowait() == (table, 77, "db-level")

    # The maintainer queue should remain empty.
    with pytest.raises(queue.Empty):
        stopped_db.maintenance.main_table_dirtied_queue.get_nowait()


def test_write_telemetry_snapshot_observes_dirty_queue(stopped_db):
    table = "books" if "books" in stopped_db.dirtiable_tables else sorted(stopped_db.dirtiable_tables)[0]
    before_total = int(stopped_db.get_write_telemetry_snapshot(recent_limit=5).get("observed_total", 0))

    stopped_db.direct_dirty_record(table, 321, reason="telemetry-test")

    snapshot = stopped_db.get_write_telemetry_snapshot(recent_limit=5)
    assert int(snapshot.get("observed_total", 0)) >= before_total + 1
    recent = list(snapshot.get("recent_events", ()) or ())
    assert recent
    assert recent[-1]["source"] == "dirty_queue"
    assert recent[-1]["table"] == table
    assert recent[-1]["row_id"] == 321
    assert recent[-1]["reason"] == "telemetry-test"


def test_write_telemetry_snapshot_observes_trigger_callback_proxy(stopped_db):
    table = "books" if "books" in stopped_db.main_tables else sorted(stopped_db.main_tables)[0]
    _drain(stopped_db.maintenance.main_table_dirtied_queue)

    stopped_db.driver.maintainer_callback.direct_dirty_record(table, 654)

    got = stopped_db.maintenance.main_table_dirtied_queue.get_nowait()
    assert got == (table, 654)
    snapshot = stopped_db.get_write_telemetry_snapshot(recent_limit=5)
    recent = list(snapshot.get("recent_events", ()) or ())
    assert recent
    assert any(event["source"] == "trigger_dirty_record" and event["table"] == table for event in recent)


def test_dirty_record_sql_function_is_registered_and_enqueues(stopped_db):
    """Calling the SQLite UDF should call Maintainer.dirty_record and enqueue an event."""

    table = "books" if "books" in stopped_db.main_tables else sorted(stopped_db.main_tables)[0]
    _drain(stopped_db.maintenance.main_table_dirtied_queue)

    # The function returns NULL, but should enqueue.
    stopped_db.execute("SELECT DIRTY_RECORD(?, ?)", (table, 999))

    got = stopped_db.maintenance.main_table_dirtied_queue.get_nowait()
    assert got == (table, 999)


def test_dirty_record_trigger_enqueues_via_callback_plumbing(stopped_db, pick_payload):
    """A trigger that calls DIRTY_RECORD should enqueue a maintainer event."""

    _drain(stopped_db.maintenance.main_table_dirtied_queue)

    payload = pick_payload(10)
    stopped_db.direct_execute_sql_script(
        """
        DROP TABLE IF EXISTS contract_dirty_table;
        CREATE TABLE contract_dirty_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT
        );
        DROP TRIGGER IF EXISTS contract_dirty_table_dirty;
        CREATE TRIGGER contract_dirty_table_dirty
        AFTER INSERT ON contract_dirty_table
        BEGIN
            SELECT DIRTY_RECORD('contract_dirty_table', NEW.id);
        END;
        """
    )

    stopped_db.execute("INSERT INTO contract_dirty_table(payload) VALUES (?)", (payload,))

    table, row_id = stopped_db.maintenance.main_table_dirtied_queue.get_nowait()
    assert table == "contract_dirty_table"
    assert isinstance(row_id, int)
    assert row_id >= 1


def test_maintenance_thread_stops_on_close(driver_spec, db_metadata):
    from LiuXin_alpha.databases.database import Database

    db = Database(metadata=db_metadata, db_type=driver_spec.db_type, create=False, backup=False)
    th = getattr(getattr(db, "maintenance", None), "maintainer", None)
    assert th is not None

    # Give the thread a moment to start.
    for _ in range(20):
        if th.is_alive():
            break
        time.sleep(0.01)

    db.close()

    # After close, the thread should be told to stop and should not remain alive.
    # (It is daemon=True, but we still want a clean stop to avoid test flakiness.)
    assert not th.is_alive()


@pytest.mark.xfail(reason="Bug: DIRTY_INTERLINK_RECORD registered with 4 args but maintainer expects 5")
def test_dirty_interlink_record_udf_enqueues_interlink_queue(stopped_db):
    """Desired: calling the UDF should enqueue an interlink dirty record."""

    _drain(stopped_db.maintenance.interlink_dirtied_queue)

    # Desired: the UDF should accept 5 args (update_type, table1, table2, table1_id, table2_id)
    # and enqueue a corresponding event.
    stopped_db.execute("SELECT DIRTY_INTERLINK_RECORD(?, ?, ?, ?, ?)", ("update", "a", "b", 1, 2))

    got = stopped_db.maintenance.interlink_dirtied_queue.get_nowait()
    assert got == ("update", "a", "b", 1, 2)


def test_close_breaks_cycles_including_dirty_records_queue(driver_spec, db_metadata):
    from LiuXin_alpha.databases.database import Database

    db = Database(metadata=db_metadata, db_type=driver_spec.db_type, create=False, backup=False)
    assert db.dirty_records_queue is not None
    db.close()
    assert getattr(db, "dirty_records_queue", None) is None
