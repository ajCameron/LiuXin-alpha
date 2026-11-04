"""Exercise transaction completion and durable change capture before worker delivery."""

from __future__ import annotations

import sqlite3

import pytest

from LiuXin_alpha.databases.connection_hooks import SQLiteCompletionConnection
from LiuXin_alpha.databases.maintenance.capture import install_capture
from LiuXin_alpha.databases.maintenance.schema import migrate
from LiuXin_alpha.databases.transactions import transaction_scope


@pytest.fixture
def journal():
    """Own a real SQLite catalogue with two subscriptions to one physical table."""
    connection = SQLiteCompletionConnection(":memory:")
    ready = {}
    connection.execute("PRAGMA foreign_keys = ON")
    connection.create_function("LIUXIN_MAINTENANCE_ORIGIN", 0, lambda: "")
    connection.create_function("LIUXIN_MAINTENANCE_ANCESTRY", 0, lambda: "[]")
    connection.create_function(
        "LIUXIN_MAINTENANCE_READY",
        2,
        lambda name, version: int(ready.get(name) == version),
    )
    with transaction_scope(connection, owns_transaction=True):
        migrate(connection)
        connection.execute(
            "CREATE TABLE works(work_id INTEGER PRIMARY KEY, title TEXT)"
        )
        for name in ("summary", "count"):
            connection.execute(
                "INSERT INTO maintenance_plugins VALUES (?, '1', 'lazy', 1, 0, '[]', '{}', 5, 1)",
                (name,),
            )
            connection.execute(
                "INSERT INTO maintenance_subscriptions VALUES (?, 'works')", (name,)
            )
        install_capture(
            connection, table="works", id_column="work_id", columns=("work_id", "title")
        )
    try:
        yield connection, ready
    finally:
        connection.close()


def test_capture_and_obligations_follow_source_savepoints_and_commit(journal):
    connection, _ = journal
    with transaction_scope(connection, owns_transaction=True):
        connection.execute("INSERT INTO works VALUES (1, 'kept')")
        with pytest.raises(RuntimeError):
            with transaction_scope(connection, owns_transaction=False):
                connection.execute("INSERT INTO works VALUES (2, 'discarded')")
                raise RuntimeError("rollback inner")
        assert connection.execute(
            "SELECT row_id FROM maintenance_changes"
        ).fetchall() == [(1,)]
    changes = connection.execute("SELECT change_id FROM maintenance_changes").fetchall()
    obligations = connection.execute(
        "SELECT change_id FROM maintenance_obligations"
    ).fetchall()
    assert len(changes) == 1 and obligations == changes * 2
    assert connection.execute("SELECT work_id FROM works").fetchall() == [(1,)]
    with pytest.raises(RuntimeError):
        with transaction_scope(connection, owns_transaction=True):
            connection.execute("UPDATE works SET title = 'discarded'")
            raise RuntimeError("rollback outer")
    assert connection.execute("SELECT title FROM works").fetchone() == ("kept",)
    assert connection.execute(
        "SELECT COUNT(*) FROM maintenance_changes"
    ).fetchone() == (1,)


def test_missing_prompt_implementation_prevents_source_mutation(journal):
    connection, _ = journal
    with connection:
        connection.execute(
            "UPDATE maintenance_plugins SET mode = 'prompt' WHERE plugin_id = 'summary'"
        )
    with pytest.raises(
        sqlite3.IntegrityError, match="prompt maintenance is unavailable"
    ):
        with connection:
            connection.execute("INSERT INTO works VALUES (1, 'unavailable')")
    assert connection.execute("SELECT COUNT(*) FROM works").fetchone() == (0,)
    assert connection.execute(
        "SELECT COUNT(*) FROM maintenance_changes"
    ).fetchone() == (0,)


@pytest.mark.parametrize("boundary", ("commit", "native_context", "savepoint"))
def test_failed_completion_hook_rolls_back_its_write_unit(boundary):
    connection = SQLiteCompletionConnection(":memory:")
    connection.execute("CREATE TABLE entries(value INTEGER)")

    def fail(active):
        active.execute("INSERT INTO entries VALUES (99)")
        raise RuntimeError("required effect failed")

    try:
        if boundary == "savepoint":
            connection.execute("INSERT INTO entries VALUES (1)")
        connection._liuxin_before_completion = fail
        with pytest.raises(RuntimeError, match="required effect failed"):
            if boundary == "commit":
                connection.execute("INSERT INTO entries VALUES (2)")
                connection.commit()
            elif boundary == "native_context":
                with connection:
                    connection.execute("INSERT INTO entries VALUES (2)")
            else:
                with transaction_scope(connection, owns_transaction=False):
                    connection.execute("INSERT INTO entries VALUES (2)")
        expected = [(1,)] if boundary == "savepoint" else []
        assert connection.execute("SELECT value FROM entries").fetchall() == expected
    finally:
        connection.close()
