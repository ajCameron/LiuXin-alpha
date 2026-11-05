"""Exercise runtime boundaries made explicit by the macro typing pass."""

from __future__ import annotations

from contextlib import closing

import pytest

from tests.databases.test_row_write_safety import db as db, template as template


@pytest.mark.parametrize("payload", ["[]", "null", "7", '"receipt"'])
def test_repair_receipt_requires_a_json_object(db, payload):
    with closing(db.driver.get_connection()) as connection:
        connection.execute(
            "CREATE TABLE catalog_repair_receipts ("
            "receipt_id TEXT PRIMARY KEY, receipt_version INTEGER, receipt_json TEXT)"
        )
        connection.execute(
            "INSERT INTO catalog_repair_receipts VALUES (?, 1, ?)",
            ("bad-shape", payload),
        )
        connection.commit()
    with pytest.raises(ValueError, match="JSON object"):
        db.macros.get_catalog_repair_receipt("bad-shape")


@pytest.mark.parametrize("project_connection", [False, True])
def test_custom_column_readers_accept_query_connections(project_connection):
    import sqlite3
    from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver import (
        SQLite_Connection,
    )
    from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.cc_macros_mixin import (
        SQLiteDatabaseCustomColumnMacros,
    )

    factory = SQLite_Connection if project_connection else sqlite3.Connection
    with closing(sqlite3.connect(":memory:", factory=factory)) as connection:
        connection.executescript(
            "CREATE TABLE custom_column_1 (custom_column_1_id INTEGER PRIMARY KEY, custom_column_1_value TEXT);"
            'INSERT INTO custom_column_1 VALUES (1, "A"), (2, "A"), (3, NULL);'
        )
        macros = SQLiteDatabaseCustomColumnMacros()
        assert macros.get_cc_id_and_value_from_id(
            "custom_column_1", 1, conn=connection
        ) == (1, "A")
        assert macros.get_cc_id_from_value("custom_column_1", "A", conn=connection) == 1
        assert (
            macros.get_cc_id_from_value("custom_column_1", "missing", conn=connection)
            is None
        )
        assert macros.get_cc_id_from_value(
            "custom_column_1", "A", all=True, conn=connection
        ) == [(1,), (2,)]
        assert macros.get_all_cc_id_val_pairs("custom_column_1", conn=connection) == [
            (1, "A"),
            (2, "A"),
            (3, None),
        ]
        assert macros.get_all_cc_custom_values(
            "custom_column_1", distinct=True, conn=connection
        ) == [("A",), (None,)]
        assert macros.direct_get_custom_tables(conn=connection) == {"custom_column_1"}
        with pytest.raises(IndexError):
            macros.get_cc_id_and_value_from_id("custom_column_1", 99, conn=connection)
        # A read neither commits nor rolls back the caller's pending changes.
        connection.execute('INSERT INTO custom_column_1 VALUES (4, "pending")')
        assert (
            macros.get_cc_id_from_value("custom_column_1", "pending", conn=connection)
            == 4
        )
        assert connection.in_transaction
        connection.rollback()
        assert (
            macros.get_cc_id_from_value("custom_column_1", "pending", conn=connection)
            is None
        )


@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("nested", [False, True])
def test_legacy_owner_batch_delete_accepts_scalar_and_parameter_rows(explicit, nested):
    import sqlite3
    from types import SimpleNamespace
    from LiuXin_alpha.databases.database_driver_plugins.SQL.macros import (
        SQLiteDatabaseMacros,
    )

    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute("CREATE TABLE links (link_book INTEGER, link_value INTEGER)")
        connection.executemany(
            "INSERT INTO links VALUES (?, ?)", [(1, 10), (2, 20), (3, 30)]
        )
        connection.commit()
        host = SimpleNamespace(
            driver=SimpleNamespace(
                conn=connection, direct_executemany=connection.executemany
            )
        )
        macros = SQLiteDatabaseMacros(host)
        ids = iter([(1,), (2,)] if nested else [1, 2])
        macros.break_cc_links_by_book_id(
            "links", (value for value in ids), conn=connection if explicit else None
        )
        assert connection.execute("SELECT link_book FROM links").fetchall() == [(3,)]
        assert connection.in_transaction
        connection.rollback()
        assert connection.execute("SELECT link_book FROM links").fetchall() == [
            (1,),
            (2,),
            (3,),
        ]


@pytest.mark.parametrize("scalar", [False, True])
@pytest.mark.parametrize("fail", [False, True])
def test_native_custom_read_closes_only_its_cursor(scalar, fail):
    from types import SimpleNamespace
    from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.cc_macros_mixin._reads import (
        read_rows,
        read_scalar,
    )

    class Cursor:
        closed = False

        def fetchone(self):
            if fail:
                raise RuntimeError("fetch failed")
            return {"value": 7}

        def __iter__(self):
            yield self.fetchone()

        def close(self):
            self.closed = True

    cursor = Cursor()
    connection = SimpleNamespace(execute=lambda sql, params=(): cursor)
    reader = read_scalar if scalar else read_rows
    if fail:
        with pytest.raises(RuntimeError, match="fetch failed"):
            reader(connection, "SELECT value")
    else:
        assert reader(connection, "SELECT value") == (7 if scalar else [(7,)])
    assert cursor.closed


def test_concurrent_macro_transactions_keep_thread_connections_separate():
    import sqlite3
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from types import SimpleNamespace
    from LiuXin_alpha.databases.database_driver_plugins.SQL.macros import (
        SQLiteDatabaseMacros,
    )

    barrier = Barrier(2, timeout=10)
    macros = SQLiteDatabaseMacros(
        SimpleNamespace(
            driver=SimpleNamespace(get_connection=lambda: sqlite3.connect(":memory:"))
        )
    )

    def run(value):
        assert macros.current_transaction_connection() is None
        with macros.transaction() as connection:
            connection.execute("CREATE TABLE entries (value INTEGER)")
            connection.execute("INSERT INTO entries VALUES (?)", (value,))
            barrier.wait()
            assert macros.current_transaction_connection() is connection
            with macros.transaction() as nested:
                assert nested is connection
                assert nested.execute("SELECT value FROM entries").fetchone() == (
                    value,
                )
            identity = id(connection)
            barrier.wait()
        assert macros.current_transaction_connection() is None
        return identity

    with ThreadPoolExecutor(max_workers=2) as pool:
        connections = tuple(pool.map(run, (7, 9)))
    assert len(set(connections)) == 2
    assert macros.current_transaction_connection() is None
