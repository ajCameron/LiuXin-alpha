"""Exercise runtime boundaries made explicit by the macro typing pass."""

from __future__ import annotations

from LiuXin_alpha.utils.calibre_compat.custom_columns.macros import CustomColumnMacros

from contextlib import closing

import pytest

from tests.databases.custom_field_fixtures import sqlite_db as sqlite_db
from tests.databases.custom_field_fixtures import sqlite_template as sqlite_template
from tests.databases.test_row_write_safety import db as db
from tests.databases.test_row_write_safety import template as template


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
def test_custom_column_readers_accept_query_connections(sqlite_db, project_connection):
    db = sqlite_db
    from LiuXin_alpha.databases.custom_columns import CustomFieldService

    service = CustomFieldService(db)
    field = service.create_field("works", "readers")
    first = service.ensure_value(field.id, "A")
    second = service.ensure_value(field.id, "B")
    table = f"custom_column_{field.id}"
    with db.macros.transaction() as connection:
        kwargs = {"conn": connection} if project_connection else {}
        assert CustomColumnMacros(db).get_cc_id_and_value_from_id(table, first, **kwargs) == (
            first,
            "A",
        )
        assert CustomColumnMacros(db).get_cc_id_from_value(table, "a", **kwargs) == first
        assert CustomColumnMacros(db).get_cc_id_from_value(table, "missing", **kwargs) is None
        assert CustomColumnMacros(db).get_cc_id_from_value(table, "A", all=True, **kwargs) == [
            (first,)
        ]
        assert CustomColumnMacros(db).get_all_cc_id_val_pairs(table, **kwargs) == [
            (first, "A"),
            (second, "B"),
        ]
        assert CustomColumnMacros(db).get_all_cc_custom_values(table, distinct=True, **kwargs) == [
            ("A",),
            ("B",),
        ]
        assert table in CustomColumnMacros(db).direct_get_custom_tables(**kwargs)
        with pytest.raises(IndexError):
            CustomColumnMacros(db).get_cc_id_and_value_from_id(table, 99, **kwargs)
    with pytest.raises(RuntimeError, match="rollback"):
        with db.macros.transaction() as connection:
            pending = service.ensure_value(field.id, "pending")
            assert (
                CustomColumnMacros(db).get_cc_id_from_value(table, "pending", conn=connection)
                == pending
            )
            raise RuntimeError("rollback")
    assert CustomColumnMacros(db).get_cc_id_from_value(table, "pending") is None


@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("nested", [False, True])
def test_legacy_owner_batch_delete_accepts_scalar_and_parameter_rows(
    sqlite_db, explicit, nested
):
    db = sqlite_db
    from LiuXin_alpha.databases.custom_columns import CustomFieldService, OwnerRef

    service = CustomFieldService(db)
    field = service.create_field("works", "batch", mode="set")
    owners = [db.macros.insert_row("works", {"work_title": str(i)}) for i in range(3)]
    for owner in owners:
        service.set_value(OwnerRef("works", owner), field.id, ["kept"])
    with pytest.raises(RuntimeError, match="rollback"):
        with db.macros.transaction() as connection:
            selected = ((owner,) if nested else owner for owner in owners[:2])
            CustomColumnMacros(db).break_cc_links_by_book_id(
                f"works_custom_column_{field.id}_link",
                selected,
                conn=connection if explicit else None,
            )
            assert connection.in_transaction
            assert not service.get_value(OwnerRef("works", owners[0]), field.id)
            assert not service.get_value(OwnerRef("works", owners[1]), field.id)
            assert service.get_value(OwnerRef("works", owners[2]), field.id)
            raise RuntimeError("rollback")
    assert all(
        service.get_value(OwnerRef("works", owner), field.id) for owner in owners
    )


@pytest.mark.parametrize("scalar", [False, True])
@pytest.mark.parametrize("fail", [False, True])
def test_native_custom_read_closes_only_its_cursor(scalar, fail):
    from types import SimpleNamespace

    from LiuXin_alpha.utils.calibre_compat.custom_columns.reads import (
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
