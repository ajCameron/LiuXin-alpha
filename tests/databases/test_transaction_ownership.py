"""Transactions own durability; closing resources must not make pending writes durable."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.portable_macros_mixin import (
    SQLPortableMacrosMixin,
)
from LiuXin_alpha.databases.transactions import transaction_scope


@pytest.fixture(params=["SQLite", "SQLite_apsw"])
def db_type(request: pytest.FixtureRequest) -> str:
    """Exercise each driver only when its optional import dependency is installed."""
    name = str(request.param)
    if name == "SQLite_apsw":
        pytest.importorskip(
            "apsw", reason="SQLite_apsw requires the optional APSW package"
        )
    return name


@pytest.fixture
def sql_path(tmp_path: Path) -> Path:
    """Use an independent reader to distinguish local state from committed rows."""
    path = tmp_path / "transactions.sqlite"
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("CREATE TABLE probe (value TEXT)")
        conn.commit()
    return path


def _rows(path: Path) -> list[str]:
    """Read only changes visible to a new connection."""
    with closing(sqlite3.connect(path)) as conn:
        return [
            row[0] for row in conn.execute("SELECT value FROM probe ORDER BY rowid")
        ]


def _macros(driver: object) -> SQLPortableMacrosMixin:
    """Attach the real macro boundary to a minimal connection-owning driver."""
    macros = SQLPortableMacrosMixin()
    macros.db = SimpleNamespace(driver=driver)
    return macros


def test_borrowed_success_does_not_commit_callers_pending_work(sql_path: Path) -> None:
    """A macro savepoint cannot commit writes made before its entry."""
    with closing(sqlite3.connect(sql_path)) as conn:
        conn.execute("INSERT INTO probe VALUES ('caller')")
        macros = _macros(SimpleNamespace(conn=conn))
        with macros.transaction() as active:
            active.execute("INSERT INTO probe VALUES ('macro')")
        assert conn.in_transaction
        assert _rows(sql_path) == []
        conn.rollback()
    assert _rows(sql_path) == []


def test_borrowed_failure_preserves_callers_pending_work(sql_path: Path) -> None:
    """Rollback is bounded by the macro's savepoint, leaving its caller in control."""
    with closing(sqlite3.connect(sql_path)) as conn:
        conn.execute("INSERT INTO probe VALUES ('caller')")
        macros = _macros(SimpleNamespace(conn=conn))
        with pytest.raises(RuntimeError, match="late failure"):
            with macros.transaction() as active:
                active.execute("INSERT INTO probe VALUES ('macro')")
                raise RuntimeError("late failure")
        assert conn.in_transaction
        assert conn.execute("SELECT value FROM probe").fetchall() == [("caller",)]
        assert _rows(sql_path) == []
        conn.commit()
    assert _rows(sql_path) == ["caller"]


def test_caught_nested_failure_rolls_back_just_the_inner_unit(sql_path: Path) -> None:
    """Catching an inner failure must not silently persist its partial writes."""
    macros = _macros(SimpleNamespace(get_connection=lambda: sqlite3.connect(sql_path)))
    with macros.transaction() as outer:
        outer.execute("INSERT INTO probe VALUES ('before')")
        with pytest.raises(RuntimeError, match="inner failure"):
            with macros.transaction() as inner:
                assert inner is outer
                inner.execute("INSERT INTO probe VALUES ('partial')")
                raise RuntimeError("inner failure")
        outer.execute("INSERT INTO probe VALUES ('after')")
        assert _rows(sql_path) == []
    assert _rows(sql_path) == ["before", "after"]


def test_failed_commit_rolls_back_the_owned_unit(sql_path: Path) -> None:
    """Releasing an outer savepoint must not publish before the owner's commit."""

    class CommitFailure:
        """Inject a pre-commit failure while retaining real SQLite execution."""

        def __init__(self) -> None:
            """Open the real database for the injected commit failure."""
            self.connection = sqlite3.connect(sql_path)

        def __getattr__(self, name: str) -> Any:
            """Delegate transaction state and SQL calls to SQLite."""
            return getattr(self.connection, name)

        def commit(self) -> None:
            """Fail before SQLite publishes the writes."""
            raise RuntimeError("commit failed")

    macros = _macros(SimpleNamespace(get_connection=CommitFailure))
    with pytest.raises(RuntimeError, match="commit failed"):
        with macros.transaction() as active:
            active.execute("INSERT INTO probe VALUES ('partial')")
    assert _rows(sql_path) == []


def test_idle_persistent_connection_is_committed_but_not_closed(sql_path: Path) -> None:
    """Owning a new transaction does not confer ownership of its connection."""
    with closing(sqlite3.connect(sql_path)) as conn:
        macros = _macros(SimpleNamespace(conn=conn))
        with macros.transaction() as active:
            active.execute("INSERT INTO probe VALUES ('macro')")
        assert not conn.in_transaction
        assert conn.execute("SELECT value FROM probe").fetchall() == [("macro",)]
    assert _rows(sql_path) == ["macro"]


def test_legacy_connection_lock_cannot_commit_caller_work(sql_path: Path) -> None:
    """A legacy lock alias must not enter a SQL connection's committing context."""
    with closing(sqlite3.connect(sql_path)) as conn:
        conn.execute("INSERT INTO probe VALUES ('caller')")
        macros = _macros(SimpleNamespace(conn=conn))
        macros.db.lock = conn
        with macros.transaction() as active:
            active.execute("INSERT INTO probe VALUES ('macro')")
        assert _rows(sql_path) == []
        conn.rollback()


def test_borrowed_scope_rejects_idle_connection(sql_path: Path) -> None:
    """A root SAVEPOINT must never accidentally publish borrowed writes."""
    with closing(sqlite3.connect(sql_path)) as conn:
        with pytest.raises(RuntimeError, match="requires an active transaction"):
            with transaction_scope(conn, owns_transaction=False):
                pytest.fail("An idle connection was borrowed")
        assert not conn.in_transaction


@pytest.mark.parametrize("exceptional", [False, True])
def test_temporary_table_preserves_borrowed_transaction(
    sql_path: Path, exceptional: bool
) -> None:
    """Temporary table cleanup neither commits nor discards earlier caller writes."""
    with closing(sqlite3.connect(sql_path)) as conn:
        conn.execute("INSERT INTO probe VALUES ('caller')")
        macros = _macros(SimpleNamespace(conn=conn))
        try:
            with macros.temporary_value_table(("x", "y")) as table:
                assert (
                    conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0] == 2
                )
                if exceptional:
                    raise RuntimeError("temporary body failed")
        except RuntimeError as error:
            assert exceptional and str(error) == "temporary body failed"
        assert conn.in_transaction
        assert conn.execute("SELECT name FROM sqlite_temp_master").fetchall() == []
        assert _rows(sql_path) == []
        conn.commit()
    assert _rows(sql_path) == ["caller"]


def test_temporary_table_cannot_commit_surrounding_macro(sql_path: Path) -> None:
    """A late aggregate failure also rolls back writes before temporary-table use."""
    macros = _macros(SimpleNamespace(get_connection=lambda: sqlite3.connect(sql_path)))
    with pytest.raises(RuntimeError, match="late failure"):
        with macros.transaction() as active:
            active.execute("INSERT INTO probe VALUES ('partial')")
            with macros.temporary_value_table(("x",)) as table:
                assert (
                    active.execute(f'SELECT value FROM "{table}"').fetchone()[0] == "x"
                )
            assert _rows(sql_path) == []
            raise RuntimeError("late failure")
    assert _rows(sql_path) == []


@pytest.mark.parametrize("exceptional", [False, True])
def test_resource_exit_discards_uncommitted_writes(
    tmp_path: Path, db_type: str, exceptional: bool
) -> None:
    """A resource context closes handles on either exit without committing SQL."""
    path = tmp_path / "facade.sqlite"
    db = Database(
        metadata={"database_path": str(path)},
        db_type=db_type,
        create=True,
        enable_storage_manager=False,
        enable_maintenance=False,
    )
    db.driver.conn.execute("CREATE TABLE probe (value TEXT)")
    db.driver.conn.commit()
    try:
        with db:
            db.driver.conn.execute("BEGIN")
            db.driver.conn.execute("INSERT INTO probe VALUES ('partial')")
            if exceptional:
                raise RuntimeError("body failed")
    except RuntimeError as error:
        assert exceptional and str(error) == "body failed"
    finally:
        db.close()
    assert _rows(path) == []


def test_driver_replacement_discards_uncommitted_writes(
    tmp_path: Path, db_type: str
) -> None:
    """Replacing a resource cannot implicitly make its pending SQL durable."""
    from LiuXin_alpha.databases.database_driver_plugins.registry import (
        load_database_driver,
    )

    path = tmp_path / "replace.sqlite"
    metadata = {"database_path": str(path)}
    with Database(
        metadata=metadata,
        db_type=db_type,
        create=True,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as db:
        db.driver.conn.execute("CREATE TABLE probe (value TEXT)")
        db.driver.conn.commit()
        replacement = load_database_driver(db_type)(metadata, set_conn=True)
        db.driver.conn.execute("BEGIN")
        db.driver.conn.execute("INSERT INTO probe VALUES ('partial')")
        db.set_driver(replacement)
        assert _rows(path) == []


def test_resource_cleanup_preserves_explicitly_committed_work(
    tmp_path: Path, db_type: str
) -> None:
    """Closing a facade discards only pending work; completed units remain durable."""
    path = tmp_path / "committed.sqlite"
    with pytest.raises(RuntimeError, match="after commit"):
        with Database(
            metadata={"database_path": str(path)},
            db_type=db_type,
            create=True,
            enable_storage_manager=False,
            enable_maintenance=False,
        ) as db:
            with db.macros.transaction() as active:
                active.execute("CREATE TABLE probe (value TEXT)")
                active.execute("INSERT INTO probe VALUES ('committed')")
            raise RuntimeError("after commit")
    assert _rows(path) == ["committed"]


def test_facade_lock_is_a_reentrant_mutex(tmp_path: Path) -> None:
    """The shared lock serializes threads and has no hidden SQL-commit behaviour."""
    from concurrent.futures import ThreadPoolExecutor

    with Database(
        metadata={"database_path": str(tmp_path / "mutex.sqlite")},
        create=True,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as db:
        assert db.lock is db.driver_wrapper.lock
        assert not hasattr(db.lock, "execute")

        def try_lock() -> bool:
            """A competing thread observes the mutex owner's exclusion."""
            acquired = db.lock.acquire(blocking=False)
            if acquired:
                db.lock.release()
            return acquired

        with ThreadPoolExecutor(max_workers=1) as executor:
            with db.lock:
                with db.lock:
                    assert executor.submit(try_lock).result(timeout=5) is False
            assert executor.submit(try_lock).result(timeout=5) is True


def test_serialized_units_reserve_writer_before_reading(sql_path: Path) -> None:
    """Different macro hosts cannot both read pre-decision state as serialized owners."""
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    first = _macros(SimpleNamespace(get_connection=lambda: sqlite3.connect(sql_path)))
    second = _macros(SimpleNamespace(get_connection=lambda: sqlite3.connect(sql_path)))
    started, entered = Event(), Event()

    def competing_writer() -> list[str]:
        """Read only after the first unit has released the database writer reservation."""
        started.set()
        with second.transaction(serialized=True) as conn:
            entered.set()
            seen = [row[0] for row in conn.execute("SELECT value FROM probe")]
            conn.execute("INSERT INTO probe VALUES ('second')")
            return seen

    with ThreadPoolExecutor(max_workers=1) as executor:
        with first.transaction(serialized=True) as conn:
            future = executor.submit(competing_writer)
            assert started.wait(5)
            assert not entered.wait(0.2)
            conn.execute("INSERT INTO probe VALUES ('first')")
        assert future.result(timeout=5) == ["first"]
    assert _rows(sql_path) == ["first", "second"]


def test_serialized_unit_refuses_borrowed_pending_work(sql_path: Path) -> None:
    """A caller's already active transaction cannot be silently promoted or committed."""
    with closing(sqlite3.connect(sql_path)) as conn:
        conn.execute("INSERT INTO probe VALUES ('caller')")
        macros = _macros(SimpleNamespace(conn=conn))
        with pytest.raises(RuntimeError, match="ownership of the outer transaction"):
            with macros.transaction(serialized=True):
                pytest.fail("Borrowed work was promoted")
        assert conn.in_transaction
        assert _rows(sql_path) == []
        conn.commit()
    assert _rows(sql_path) == ["caller"]


def test_serialized_nesting_requires_serialized_outer_owner(sql_path: Path) -> None:
    """Savepoints preserve an existing reservation but cannot upgrade an ordinary unit."""
    macros = _macros(SimpleNamespace(get_connection=lambda: sqlite3.connect(sql_path)))
    with macros.transaction():
        with pytest.raises(RuntimeError, match="ordinary transaction"):
            with macros.transaction(serialized=True):
                pytest.fail("Ordinary outer unit was upgraded")
    with macros.transaction(serialized=True) as outer:
        with macros.transaction(serialized=True) as inner:
            assert inner is outer
            inner.execute("INSERT INTO probe VALUES ('nested')")
        assert _rows(sql_path) == []
    assert _rows(sql_path) == ["nested"]
