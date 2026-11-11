"""Keep read-only protection active through completion, including empty inspections."""

import sqlite3
from types import SimpleNamespace

import pytest

from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.portable_macros_mixin import (
    SQLPortableMacrosMixin,
)


class HookConnection(sqlite3.Connection):
    """Allow the same completion-hook attribute used by production connections."""


@pytest.mark.parametrize("owned", [False, True])
def test_read_transaction_rejects_completion_hook_writes(tmp_path, owned):
    path = tmp_path / "snapshot.sqlite"
    connection = sqlite3.connect(path, factory=HookConnection)
    connection.execute("CREATE TABLE records (value TEXT)")
    connection.commit()
    driver = (
        SimpleNamespace(get_connection=lambda: connection)
        if owned
        else SimpleNamespace(conn=connection)
    )
    macros = SQLPortableMacrosMixin()
    macros.db = SimpleNamespace(driver=driver)

    def write_on_completion(active):
        active.execute("INSERT INTO records VALUES ('forbidden')")

    connection._liuxin_before_completion = write_on_completion
    try:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            # No body read can flush/reject the hook early: completion itself
            # must stay read-only until the owning transaction has finished.
            with macros.read_transaction():
                pass
        with sqlite3.connect(path) as check:
            assert check.execute("SELECT * FROM records").fetchall() == []
        if not owned:
            assert not connection.in_transaction
            assert connection.execute("PRAGMA query_only").fetchone()[0] == 0
    finally:
        connection.close()


def test_read_transaction_restores_a_preexisting_read_only_fallback():
    connection = sqlite3.connect(":memory:", factory=HookConnection)
    macros = SQLPortableMacrosMixin()
    macros.db = SimpleNamespace(driver=SimpleNamespace(conn=connection))
    connection.execute("PRAGMA query_only=ON")
    try:
        with macros.read_transaction():
            assert connection.execute("SELECT 1").fetchone()[0] == 1
        assert connection.execute("PRAGMA query_only").fetchone()[0] == 1
    finally:
        connection.close()
