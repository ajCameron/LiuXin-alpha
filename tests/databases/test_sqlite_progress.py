"""Verify optional progress hooks on the real connections of both SQLite drivers."""

from __future__ import annotations

import importlib
import sqlite3

import pytest

QUERY = """WITH RECURSIVE numbers(n) AS (
    VALUES(1) UNION ALL SELECT n + 1 FROM numbers WHERE n < 1000
) SELECT SUM(n) FROM numbers"""


@pytest.fixture(params=("SQLite", "SQLite_apsw"))
def driver_module(request):
    """Load each supported SQLite connection factory."""
    if request.param == "SQLite_apsw":
        pytest.importorskip("apsw")
    return importlib.import_module(
        f"LiuXin_alpha.databases.database_driver_plugins.{request.param}.databasedriver"
    )


@pytest.fixture
def driver(driver_module):
    """Own an in-memory driver and every connection it creates."""
    value = driver_module.DatabaseDriver({"database_path": ":memory:"})
    try:
        yield value
    finally:
        value.close()


def test_default_does_not_install_any_progress_handler(driver_module, monkeypatch):
    """Default queries cannot dispatch even an empty Python callback wrapper."""
    installed = []

    class Connection(driver_module.SQLite_Connection):
        def set_progress_handler(self, callback, n):
            installed.append(callback)
            return super().set_progress_handler(callback, n)

    monkeypatch.setattr(driver_module, "SQLite_Connection", Connection)
    driver = driver_module.DatabaseDriver({"database_path": ":memory:"})
    try:
        assert driver.conn.execute(QUERY).fetchone()[0] == 500500
        assert driver.get_connection().execute(QUERY).fetchone()[0] == 500500
        assert installed == []
        assert driver.event_count == 0
    finally:
        driver.close()


def test_callbacks_cover_existing_future_connections_and_can_be_removed(driver):
    """Replacing/clearing hooks affects live handles and survives connection renewal."""
    previous = driver.get_connection()
    driver.get_connection().close()  # Closed tracked handles must not block updates.
    calls = []
    driver.set_progress_callbacks(
        (lambda: calls.append("first"), lambda: calls.append("second")),
        instruction_interval=100,
    )
    future = driver.get_connection()
    assert calls == []  # Initialization SQL is outside callback observation.
    for connection in (driver.conn, previous, future):
        calls.clear()
        assert connection.execute(QUERY).fetchone()[0] == 500500
        assert calls and calls == ["first", "second"] * (len(calls) // 2)
    driver.set_progress_callbacks()
    calls.clear()
    for connection in (driver.conn, previous, future, driver.get_connection()):
        assert connection.execute(QUERY).fetchone()[0] == 500500
    assert calls == []


def test_callback_cancellation_and_invalid_replacement(driver):
    """Cancellation interrupts a statement; invalid settings preserve active hooks."""
    driver.set_progress_callbacks((lambda: True,), instruction_interval=1)
    for interval in (0, -1, True, 1.5, 2**31):
        with pytest.raises(ValueError, match="interval"):
            driver.set_progress_callbacks((), instruction_interval=interval)
    with pytest.raises(TypeError, match="callable"):
        driver.set_progress_callbacks((None,))
    for connection in (driver.conn, driver.get_connection()):
        with pytest.raises(sqlite3.OperationalError, match="interrupt"):
            connection.execute(QUERY).fetchone()
    driver.set_progress_callbacks()
    assert driver.conn.execute(QUERY).fetchone()[0] == 500500


def test_callbacks_do_not_leak_to_another_driver(driver, driver_module):
    """A callback belongs to its driver rather than a shared class registry."""
    driver.set_progress_callbacks((lambda: True,), instruction_interval=1)
    other = driver_module.DatabaseDriver({"database_path": ":memory:"})
    try:
        assert other.conn.execute(QUERY).fetchone()[0] == 500500
    finally:
        other.close()


def test_existing_counter_can_be_enabled_and_disabled(driver):
    """Retain the diagnostic observer without making ordinary queries run it."""
    driver.set_progress_callbacks(
        (driver.simple_print_progress_handler,), instruction_interval=100
    )
    assert driver.conn.execute(QUERY).fetchone()[0] == 500500
    observed = driver.event_count
    assert observed > 0
    driver.set_progress_callbacks()
    assert driver.conn.execute(QUERY).fetchone()[0] == 500500
    assert driver.event_count == observed
