"""
Check Database dirty queues, maintainer callbacks, telemetry, and close behavior across selected backends.

Queue-inspection fixtures request a maintainer stop and briefly join it, but
suppress errors and do not verify that the stop succeeded. The interlink callback
case retains its existing expected-failure marker.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database/database_contract/test_db_dirty_queue_and_maintenance.py
"""

from __future__ import annotations

import queue
import threading
import time
from typing import Any

import pytest


def _drain(q: queue.Queue) -> list[Any]:
    """
    Remove available queue items without blocking until get_nowait raises Empty.

    Example:
        >>> q = queue.Queue()
        >>> q.put('a')
        >>> _drain(q)
        ['a']
        >>> _drain(q)
        []


    :param q: Queue to consume; concurrent producers can extend or repopulate it.
    :return: Items removed in retrieval order; does not call task_done or wait for
        producers.
    """
    out: list[Any] = []
    while True:
        try:
            out.append(q.get_nowait())
        except queue.Empty:
            return out


def _stop_maintainer_thread(db) -> None:
    """
    Attempt to stop and join the maintenance thread for at most one second.

    Return if no thread exists. Suppress ordinary stop/join exceptions and do not assert
    termination.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_dirty_queue_and_maintenance.py


    :param db: Caller-owned Database; this helper does not close the Database itself.
    :return: None; successful return does not establish that the thread stopped.
    """

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
    """
    Request maintainer shutdown, drain both maintainer queues, and yield the existing database.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_dirty_queue_and_maintenance.py


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: Iterator yielding open_db; cleanup remains with its owning fixture.
    """

    _stop_maintainer_thread(open_db)
    # Drain any stale events (some fixtures do schema checks on startup).
    _drain(open_db.maintenance.main_table_dirtied_queue)
    _drain(open_db.maintenance.interlink_dirtied_queue)
    yield open_db


def test_dirty_records_queue_is_shared_with_driver_and_wrapper(open_db):
    """
    Check Database exposes a Queue shared by identity with its driver and wrapper.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_dirty_queue_and_maintenance.py::test_dirty_records_queue_is_shared_with_driver_and_wrapper


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """
    assert open_db.dirty_records_queue is not None
    assert isinstance(open_db.dirty_records_queue, queue.Queue)

    assert getattr(open_db.driver, "dirty_records_queue", None) is open_db.dirty_records_queue
    assert getattr(open_db.driver_wrapper, "dirty_records_queue", None) is open_db.dirty_records_queue


def test_get_dirtied_count_tracks_queue_size(open_db):
    """
    Enqueue ten dirty events and check the reported size grows by at least ten.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_dirty_queue_and_maintenance.py::test_get_dirtied_count_tracks_queue_size


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """
    start = open_db.get_dirtied_count()
    # Use a valid dirtiable table if possible.
    table = "books" if "books" in open_db.dirtiable_tables else sorted(open_db.dirtiable_tables)[0]

    for i in range(10):
        open_db.dirty_record(table, i + 1, reason="unit-test")

    end = open_db.get_dirtied_count()
    assert end >= start + 10


def test_database_dirty_record_enqueues_for_dirtiable_table(open_db):
    # Choose a stable table name.
    """
    Enqueue a dirtiable-table event and check count growth and the next queue tuple.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_dirty_queue_and_maintenance.py::test_database_dirty_record_enqueues_for_dirtiable_table


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """
    table = "books" if "books" in open_db.dirtiable_tables else sorted(open_db.dirtiable_tables)[0]
    before = open_db.get_dirtied_count()

    open_db.dirty_record(table, 123, reason="update")

    assert open_db.get_dirtied_count() >= before + 1
    got = open_db.dirty_records_queue.get_nowait()
    assert got == (table, 123, "update")


def test_database_dirty_record_warns_and_does_not_enqueue_for_unknown_table(open_db, monkeypatch):
    """
    Check an unknown table leaves queue size unchanged and calls the patched logging helper.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_dirty_queue_and_maintenance.py::test_database_dirty_record_warns_and_does_not_enqueue_for_unknown_table


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param monkeypatch: Pytest patch fixture; restores replaced attributes after the
        test.
    :return: None; failed expectations raise AssertionError.
    """
    calls: list[tuple] = []

    from LiuXin_alpha.utils.logging import default_log

    def _fake_log_variables(*args, **kwargs):
        """
        Record logging arguments and return the first positional value or an empty string.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database/database_contract/test_db_dirty_queue_and_maintenance.py::test_database_dirty_record_warns_and_does_not_enqueue_for_unknown_table


        :param args: Positional logging arguments appended to the enclosing call log.
        :param kwargs: Keyword logging arguments appended to the enclosing call log.
        :return: First positional argument, or an empty string when absent.
        """
        calls.append((args, kwargs))
        return args[0] if args else ""

    monkeypatch.setattr(default_log, "log_variables", _fake_log_variables)

    before = open_db.get_dirtied_count()
    open_db.dirty_record("definitely_not_a_real_table", 1, reason="nope")
    after = open_db.get_dirtied_count()

    assert after == before
    assert calls, "Expected a warning log when dirtying a non-dirtiable table"


def test_database_dirty_record_is_threadsafe(open_db):
    """
    Start five producers of two hundred events each and check queue growth after timed joins.

    The test checks the resulting count; it does not separately assert thread
    termination.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_dirty_queue_and_maintenance.py::test_database_dirty_record_is_threadsafe


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """
    table = "books" if "books" in open_db.dirtiable_tables else sorted(open_db.dirtiable_tables)[0]
    start = open_db.get_dirtied_count()

    def worker(tid: int) -> None:
        """
        Enqueue two hundred dirty events using the worker ID to separate row-ID ranges.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database/database_contract/test_db_dirty_queue_and_maintenance.py::test_database_dirty_record_is_threadsafe


        :param tid: Worker number used in the million-offset row-ID calculation.
        :return: None; mutates the enclosing Database dirty queue.
        """
        for j in range(200):
            open_db.dirty_record(table, tid * 1_000_000 + j, reason="thread")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=2)

    end = open_db.get_dirtied_count()
    assert end >= start + (5 * 200)


def test_database_dirty_record_does_not_enqueue_to_maintainer_queues(stopped_db):
    """
    Check Database dirty_record supplies its own queue tuple and leaves the main-table maintainer queue empty.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_dirty_queue_and_maintenance.py::test_database_dirty_record_does_not_enqueue_to_maintainer_queues


    :param stopped_db: Open Database after a best-effort maintainer stop and initial
        queue drains.
    :return: None; failed expectations raise AssertionError.
    """

    table = "books" if "books" in stopped_db.dirtiable_tables else sorted(stopped_db.dirtiable_tables)[0]
    _drain(stopped_db.maintenance.main_table_dirtied_queue)

    stopped_db.dirty_record(table, 77, reason="db-level")
    assert stopped_db.dirty_records_queue.get_nowait() == (table, 77, "db-level")

    # The maintainer queue should remain empty.
    with pytest.raises(queue.Empty):
        stopped_db.maintenance.main_table_dirtied_queue.get_nowait()


def test_write_telemetry_snapshot_observes_dirty_queue(stopped_db):
    """
    Check dirty-queue telemetry advances its total and records the expected source, table, ID, and reason.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_dirty_queue_and_maintenance.py::test_write_telemetry_snapshot_observes_dirty_queue


    :param stopped_db: Open Database after a best-effort maintainer stop and initial
        queue drains.
    :return: None; failed expectations raise AssertionError.
    """
    table = "books" if "books" in stopped_db.dirtiable_tables else sorted(stopped_db.dirtiable_tables)[0]
    before_total = int(stopped_db.get_write_telemetry_snapshot(recent_limit=5).get("observed_total", 0))

    stopped_db.dirty_record(table, 321, reason="telemetry-test")

    snapshot = stopped_db.get_write_telemetry_snapshot(recent_limit=5)
    assert int(snapshot.get("observed_total", 0)) >= before_total + 1
    recent = list(snapshot.get("recent_events", ()) or ())
    assert recent
    assert recent[-1]["source"] == "dirty_queue"
    assert recent[-1]["table"] == table
    assert recent[-1]["row_id"] == 321
    assert recent[-1]["reason"] == "telemetry-test"


def test_write_telemetry_snapshot_observes_trigger_callback_proxy(stopped_db):
    """
    Call the driver maintainer proxy and check its queue tuple and a matching telemetry source/table event.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_dirty_queue_and_maintenance.py::test_write_telemetry_snapshot_observes_trigger_callback_proxy


    :param stopped_db: Open Database after a best-effort maintainer stop and initial
        queue drains.
    :return: None; failed expectations raise AssertionError.
    """
    table = "books" if "books" in stopped_db.main_tables else sorted(stopped_db.main_tables)[0]
    _drain(stopped_db.maintenance.main_table_dirtied_queue)

    stopped_db.driver.maintainer_callback.dirty_record(table, 654)

    got = stopped_db.maintenance.main_table_dirtied_queue.get_nowait()
    assert got == (table, 654)
    snapshot = stopped_db.get_write_telemetry_snapshot(recent_limit=5)
    recent = list(snapshot.get("recent_events", ()) or ())
    assert recent
    assert any(event["source"] == "trigger_dirty_record" and event["table"] == table for event in recent)


def test_dirty_record_sql_function_is_registered_and_enqueues(stopped_db):
    """
    Call DIRTY_RECORD through SQL and check the maintainer receives the expected table/ID tuple.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_dirty_queue_and_maintenance.py::test_dirty_record_sql_function_is_registered_and_enqueues


    :param stopped_db: Open Database after a best-effort maintainer stop and initial
        queue drains.
    :return: None; failed expectations raise AssertionError.
    """

    table = "books" if "books" in stopped_db.main_tables else sorted(stopped_db.main_tables)[0]
    _drain(stopped_db.maintenance.main_table_dirtied_queue)

    # The function returns NULL, but should enqueue.
    stopped_db.execute("SELECT DIRTY_RECORD(?, ?)", (table, 999))

    got = stopped_db.maintenance.main_table_dirtied_queue.get_nowait()
    assert got == (table, 999)


def test_dirty_record_trigger_enqueues_via_callback_plumbing(stopped_db, pick_payload):
    """
    Create a dedicated insert trigger and check its callback enqueues the test table and a positive integer ID.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_dirty_queue_and_maintenance.py::test_dirty_record_trigger_enqueues_via_callback_plumbing


    :param stopped_db: Open Database after a best-effort maintainer stop and initial
        queue drains.
    :param pick_payload: Fixture callable selecting a corpus payload by index.
    :return: None; failed expectations raise AssertionError.
    """

    _drain(stopped_db.maintenance.main_table_dirtied_queue)

    payload = pick_payload(10)
    stopped_db.executescript(
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
    """
    Open a Database, briefly allow its thread to start, close it, and check the retained thread is not alive.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_dirty_queue_and_maintenance.py::test_maintenance_thread_stops_on_close


    :param driver_spec: Selected database driver specification, including its ID and
        Database db_type.
    :param db_metadata: Constructor metadata for the provisioned database path.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Express the expected five-argument interlink callback tuple under the existing arity-bug xfail marker.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_dirty_queue_and_maintenance.py::test_dirty_interlink_record_udf_enqueues_interlink_queue


    :param stopped_db: Open Database after a best-effort maintainer stop and initial
        queue drains.
    :return: None; failed expectations raise AssertionError.
    """

    _drain(stopped_db.maintenance.interlink_dirtied_queue)

    # Desired: the UDF should accept 5 args (update_type, table1, table2, table1_id, table2_id)
    # and enqueue a corresponding event.
    stopped_db.execute("SELECT DIRTY_INTERLINK_RECORD(?, ?, ?, ?, ?)", ("update", "a", "b", 1, 2))

    got = stopped_db.maintenance.interlink_dirtied_queue.get_nowait()
    assert got == ("update", "a", "b", 1, 2)


def test_close_breaks_cycles_including_dirty_records_queue(driver_spec, db_metadata):
    """
    Check Database close removes its dirty_records_queue reference.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_dirty_queue_and_maintenance.py::test_close_breaks_cycles_including_dirty_records_queue


    :param driver_spec: Selected database driver specification, including its ID and
        Database db_type.
    :param db_metadata: Constructor metadata for the provisioned database path.
    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.database import Database

    db = Database(metadata=db_metadata, db_type=driver_spec.db_type, create=False, backup=False)
    assert db.dirty_records_queue is not None
    db.close()
    assert getattr(db, "dirty_records_queue", None) is None
