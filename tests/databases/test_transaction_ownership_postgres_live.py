"""Validate transaction ownership against PostgreSQL in disposable schemas."""

from __future__ import annotations

import os
from contextlib import closing
from types import SimpleNamespace
from uuid import uuid4

import pytest

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
    connect_postgres,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.portable_macros_mixin import (
    SQLPortableMacrosMixin,
)
from LiuXin_alpha.databases.transactions import transaction_active


@pytest.fixture
def pg_database():
    """Create a private schema and remove it through an independent connection."""
    url = os.environ.get("LIUXIN_TEST_POSTGRES_URL", "").strip()
    if not url:
        pytest.skip("LIUXIN_TEST_POSTGRES_URL is not configured.")
    metadata = {"postgres_url": url, "schema": f"liuxin_transactions_{uuid4().hex}"}
    with closing(connect_postgres(metadata, prompt_for_password=False)) as cleanup:
        db = None
        try:
            db = Database(
                metadata=metadata,
                db_type="PostgreSQL",
                create=True,
                enable_storage_manager=False,
                enable_maintenance=False,
            )
            db.driver.conn.execute("CREATE TABLE probe (value TEXT UNIQUE)")
            db.driver.conn.commit()
            yield db, metadata, cleanup
        finally:
            if db is not None:
                db.close()
            cleanup.rollback()
            with cleanup:
                with cleanup.cursor() as cursor:
                    cursor.execute(
                        f'DROP SCHEMA IF EXISTS "{metadata["schema"]}" CASCADE'
                    )


def _committed_rows(target):
    """Inspect durability independently of the facade's active connection."""
    _, metadata, raw = target
    raw.rollback()
    with raw.cursor() as cursor:
        cursor.execute(f'SELECT value FROM "{metadata["schema"]}".probe ORDER BY value')
        return [row[0] for row in cursor.fetchall()]


def test_postgres_caught_constraint_failure_leaves_outer_unit_usable(pg_database):
    """A savepoint recovers an aborted inner transaction without publishing outer work."""
    import psycopg2

    db, _, _ = pg_database
    with db.macros.transaction() as outer:
        outer.execute("INSERT INTO probe VALUES ('before')")
        with pytest.raises(psycopg2.IntegrityError):
            with db.macros.transaction() as inner:
                assert inner is outer
                inner.execute("INSERT INTO probe VALUES ('partial')")
                inner.execute("INSERT INTO probe VALUES ('before')")
        outer.execute("INSERT INTO probe VALUES ('after')")
        assert _committed_rows(pg_database) == []
    assert _committed_rows(pg_database) == ["after", "before"]


@pytest.mark.parametrize("exceptional", [False, True])
def test_postgres_borrowed_transaction_keeps_caller_in_control(
    pg_database, exceptional
):
    """Success and failure affect only the borrowed savepoint, including temporary SQL."""
    db, _, _ = pg_database
    conn = db.driver.conn
    conn.execute("INSERT INTO probe VALUES ('caller')")
    macros = SQLPortableMacrosMixin()
    macros.db = SimpleNamespace(
        driver=SimpleNamespace(conn=conn, schema=db.driver.schema)
    )
    try:
        with macros.transaction() as active:
            active.execute("INSERT INTO probe VALUES ('macro')")
            with macros.temporary_value_table(("x", "y")) as table:
                assert (
                    active.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0] == 2
                )
            if exceptional:
                raise RuntimeError("late failure")
    except RuntimeError as error:
        assert exceptional and str(error) == "late failure"
    assert transaction_active(conn)
    assert _committed_rows(pg_database) == []
    conn.commit()
    expected = ["caller"] if exceptional else ["caller", "macro"]
    assert _committed_rows(pg_database) == expected


def test_postgres_late_failure_rolls_back_whole_macro(pg_database):
    """Nested success and temporary tables cannot publish before the outer owner."""
    db, _, _ = pg_database
    with pytest.raises(RuntimeError, match="late failure"):
        with db.macros.transaction() as active:
            active.execute("INSERT INTO probe VALUES ('before')")
            with db.macros.transaction() as inner:
                inner.execute("INSERT INTO probe VALUES ('nested')")
            with db.macros.temporary_value_table(("x",)):
                pass
            raise RuntimeError("late failure")
    assert _committed_rows(pg_database) == []


@pytest.mark.parametrize("exceptional", [False, True])
def test_postgres_resource_exit_discards_only_pending_work(pg_database, exceptional):
    """Resource cleanup preserves an explicit commit and rolls back the next write."""
    db, _, _ = pg_database
    try:
        with db:
            db.driver.conn.execute("INSERT INTO probe VALUES ('committed')")
            db.driver.conn.commit()
            db.driver.conn.execute("INSERT INTO probe VALUES ('pending')")
            if exceptional:
                raise RuntimeError("resource failed")
    except RuntimeError as error:
        assert exceptional and str(error) == "resource failed"
    assert _committed_rows(pg_database) == ["committed"]


def test_postgres_serialized_units_coordinate_independent_macro_owners(pg_database):
    """The database reservation works without a shared facade/thread mutex."""
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    db, _, _ = pg_database
    first, second = SQLPortableMacrosMixin(), SQLPortableMacrosMixin()
    first.db = SimpleNamespace(driver=db.driver)
    second.db = SimpleNamespace(driver=db.driver)
    started, entered = Event(), Event()

    def competing_writer():
        """Observe committed state only after the first owner's reservation ends."""
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
    assert _committed_rows(pg_database) == ["first", "second"]
