"""Keep failed physical-handle cleanup observable and retryable at its owner."""

import pytest

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.database_driver_plugins.registry import load_database_driver
from LiuXin_alpha.errors import DatabaseIntegrityError
from LiuXin_alpha.storage.durable_manager import StorageManager
from tests.databases.test_row_write_safety import db as db
from tests.databases.test_row_write_safety import template as template


@pytest.mark.parametrize("primary", [False, True])
@pytest.mark.parametrize("replacement", [False, True])
def test_failed_handle_close_keeps_facade_owner_until_retry(
    db, monkeypatch, primary, replacement
):
    """Both close and published replacement retain their failed driver obligations."""
    old = db.driver
    failed = old.conn if primary else old.get_connection()
    healthy = old.get_connection()
    original = failed.close
    failure = OSError("injected close failure")
    attempts = []

    def fail():
        attempts.append(1)
        raise failure

    with monkeypatch.context() as patch:
        patch.setattr(failed, "close", fail)
        candidate = (
            load_database_driver(db.db_type)(dict(db.metadata), db=None)
            if replacement
            else None
        )
        with pytest.raises(ExceptionGroup) as caught:
            if replacement:
                db.set_driver(candidate)
            else:
                db.close()
        assert caught.value.exceptions == (failure,)
        assert db.lifecycle_state == "closing"
        assert old._open_connections == [failed]
        assert old.db is old.macros.db is db
        assert old.conn is (failed if primary else None)
        assert failed.execute("SELECT 1").fetchone() == (1,)
        with pytest.raises(Exception, match="closed"):
            healthy.execute("SELECT 1")
        if replacement:
            assert db.driver is candidate
            assert db._retired_drivers == [old]
            assert "binding is installed" in " ".join(caught.value.__notes__)
            assert candidate.conn.execute("SELECT 1").fetchone() == (1,)
        else:
            assert db.driver is old
        with pytest.raises(DatabaseIntegrityError, match="closing"):
            db.set_driver(db.driver)
        with pytest.raises(ExceptionGroup):
            db.close()
        assert len(attempts) == 2
        assert failed.execute("SELECT 1").fetchone() == (1,)
    assert failed.close == original
    db.close()
    assert db.lifecycle_state == "closed"
    assert db._retired_drivers == []
    assert old._open_connections == []
    if replacement:
        assert old.db is old.macros.db is None
        assert candidate._open_connections == []
    with pytest.raises(Exception, match="closed"):
        failed.execute("SELECT 1")
    db.close()


def test_driver_close_attempts_every_handle_and_reports_all_errors(db, monkeypatch):
    """One failing close must neither hide another error nor prevent healthy cleanup."""
    driver = db.driver
    failed = (driver.conn, driver.get_connection())
    healthy = driver.get_connection()
    errors = (OSError("primary close"), RuntimeError("auxiliary close"))
    with monkeypatch.context() as patch:
        for connection, error in zip(failed, errors, strict=True):

            def fail(error=error):
                raise error

            patch.setattr(connection, "close", fail)
        with pytest.raises(ExceptionGroup) as caught:
            db.close()
        assert caught.value.exceptions == errors
        assert driver._open_connections == list(failed)
        for connection in failed:
            assert connection.execute("SELECT 1").fetchone() == (1,)
        with pytest.raises(Exception, match="closed"):
            healthy.execute("SELECT 1")
    db.close()
    for connection in failed:
        with pytest.raises(Exception, match="closed"):
            connection.execute("SELECT 1")


def test_constructor_retains_failed_handle_and_preserves_bootstrap_error(
    db, monkeypatch
):
    """Construction reports its original failure and retains a retryable cleanup graph."""
    captured = {}
    bootstrap_error = ValueError("bootstrap rejected")

    def fail_close():
        raise OSError("handle shutdown failed")

    def fail_load(manager, database, **kwargs):
        captured.update(
            database=database, connection=database.conn, close=database.conn.close
        )
        monkeypatch.setattr(database.conn, "close", fail_close)
        raise bootstrap_error

    monkeypatch.setattr(StorageManager, "load_from_database", fail_load)
    try:
        with pytest.raises(ValueError) as caught:
            Database(
                metadata=dict(db.metadata),
                db_type=db.db_type,
                repair_bootstrap_rows=False,
                enable_storage_manager=True,
                strict_storage_manager_bootstrap=True,
            )
        assert caught.value is bootstrap_error
        assert "handle shutdown failed" in " ".join(bootstrap_error.__notes__)
        assert captured["database"].lifecycle_state == "closing"
        assert captured["connection"].execute("SELECT 1").fetchone() == (1,)
    finally:
        if captured:
            monkeypatch.setattr(captured["connection"], "close", captured["close"])
            captured["database"].close()
    assert captured["database"].lifecycle_state == "closed"


def test_context_body_error_survives_failed_connection_close(db, monkeypatch):
    """A body error stays primary while its failed connection remains owned for retry."""
    connection = db.conn
    body_error = ValueError("body failed")

    def fail():
        raise OSError("connection cleanup failed")

    with monkeypatch.context() as patch:
        patch.setattr(connection, "close", fail)
        with pytest.raises(ValueError) as caught:
            with db:
                raise body_error
        assert caught.value is body_error
        assert "connection cleanup failed" in " ".join(body_error.__notes__)
        assert db.lifecycle_state == "closing"
        assert connection.execute("SELECT 1").fetchone() == (1,)
    db.close()
    assert db.lifecycle_state == "closed"
