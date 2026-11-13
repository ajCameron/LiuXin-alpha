"""Protect custom-column transactions and wrapper schema contracts."""

import sqlite3
from types import SimpleNamespace

import pytest

from LiuXin_alpha.databases.custom_columns import (
    CustomFieldService,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.value_casting_mixin import (
    ValueCastingMixin,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.view_mixin import (
    ViewMixin,
)
from LiuXin_alpha.databases.transactions import transaction_active
from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.utils.calibre_compat.custom_columns.adapter import CustomColumnAdapter
from LiuXin_alpha.utils.calibre_compat.custom_columns.errors import (
    CompatibilityError,
)
from LiuXin_alpha.utils.calibre_compat.custom_columns.macros import CustomColumnMacros
from tests.databases import test_row_write_safety as fixtures

db = fixtures.db
template = fixtures.template
stored = fixtures.stored


def column(db, label="review_column", datatype="text"):
    return CustomColumnAdapter(db).create_custom_column(
        "Review" if label == "review_column" else label,
        datatype=datatype,
        label=label,
        in_table="works",
    )


def test_rename_preserves_custom_column_attachment(db):
    number = column(db)
    CustomColumnAdapter(db).set_custom_column_metadata(number, name="Renamed")
    assert stored(db, "SELECT custom_column_in_table FROM custom_columns") == [
        ("works",)
    ]


def test_attachment_change_rejected_before_metadata_edit(db):
    number = column(db)
    with pytest.raises(ValueError, match="native migration"):
        CustomColumnAdapter(db).set_custom_column_metadata(
            number, name="Must not persist", in_table="manifestations"
        )
    assert stored(
        db, "SELECT custom_column_name, custom_column_in_table FROM custom_columns"
    ) == [("Review", "works")]


@pytest.mark.parametrize("operation", ["metadata", "cleanup", "mark"])
def test_custom_columns_join_the_active_macro_transaction(db, operation):
    number = column(db)
    if operation == "cleanup":
        CustomColumnAdapter(db).delete_custom_column(number)
    with pytest.raises(RuntimeError, match="undo outer"):
        with db.macros.transaction() as conn:
            conn.execute("INSERT INTO works(work_title) VALUES ('pending')")
            if operation == "metadata":
                CustomColumnAdapter(db).set_custom_column_metadata(
                    number, name="Renamed"
                )
                assert conn.execute(
                    "SELECT custom_column_name FROM custom_columns"
                ).fetchone() == ("Renamed",)
            elif operation == "cleanup":
                CustomColumnAdapter(db).deleted_marked_custom_columns()
                assert len(conn.execute("SELECT * FROM custom_columns").fetchall()) == 1
            else:
                CustomColumnAdapter(db).delete_custom_column(number)
                assert conn.execute(
                    "SELECT custom_column_mark_for_delete FROM custom_columns"
                ).fetchone() == (1,)
            raise RuntimeError("undo outer")
    assert stored(db, "SELECT work_title FROM works") == []
    assert stored(db, "SELECT custom_column_name FROM custom_columns") == [("Review",)]
    assert db.driver_wrapper.get_relation_type(f"custom_column_{number}") == "table"


def test_failed_cleanup_restores_earlier_ddl_and_preserves_outer_write(db):
    first = column(db, "first_column")
    second = column(db, "second_column")
    service = CustomFieldService(db)
    for number in (first, second):
        service.retire_field(number)
    before = stored(db, "SELECT type, name, sql FROM sqlite_master ORDER BY type, name")
    with pytest.raises(RuntimeError, match="undo outer"):
        with db.macros.transaction() as conn:
            conn.execute("INSERT INTO works(work_title) VALUES ('pending')")

            def deny_second_drop(action, name, *unused):
                if (
                    action == sqlite3.SQLITE_DROP_TABLE
                    and name == f"custom_column_{second}"
                ):
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            conn.set_authorizer(deny_second_drop)
            try:
                with pytest.raises(Exception, match="not authorized"):
                    with db.macros.transaction():
                        service.apply_purge(service.plan_purge(first))
                        service.apply_purge(service.plan_purge(second))
            finally:
                conn.set_authorizer(None)
            assert transaction_active(conn)
            assert conn.execute("SELECT work_title FROM works").fetchall() == [
                ("pending",)
            ]
            assert (
                conn.execute(
                    "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
                ).fetchall()
                == before
            )
            assert len(conn.execute("SELECT * FROM custom_columns").fetchall()) == 2
            raise RuntimeError("undo outer")
    assert stored(db, "SELECT work_title FROM works") == []


def test_cleanup_map_only_deletes_its_own_marked_definitions(db):
    first = column(db, "first_column")
    second = column(db, "second_column")
    for number in (first, second):
        CustomColumnAdapter(db).delete_custom_column(number)
    with pytest.raises(CompatibilityError, match="explicit native purge plan"):
        CustomColumnMacros(db).preform_cc_column_delete_from_map(
            {first: CustomColumnAdapter(db).custom_table_names(first, in_table="works")}
        )
    service = CustomFieldService(db)
    service.apply_purge(service.plan_purge(first))
    assert stored(db, "SELECT custom_column_id FROM custom_columns") == [(second,)]
    assert db.driver_wrapper.get_relation_type(f"custom_column_{second}") == "table"


@pytest.mark.parametrize("operation", ["metadata", "cleanup", "mark"])
def test_custom_column_operations_preserve_borrowed_transaction(db, operation):
    number = column(db)
    if operation == "cleanup":
        CustomColumnAdapter(db).delete_custom_column(number)
    conn = db.driver.conn
    conn.execute("BEGIN")
    conn.execute("INSERT INTO works(work_title) VALUES ('pending unrelated write')")
    try:
        if operation == "cleanup":
            CustomColumnAdapter(db).deleted_marked_custom_columns()
        else:
            with pytest.raises(RuntimeError, match="owned or managed transaction"):
                if operation == "metadata":
                    CustomColumnAdapter(db).set_custom_column_metadata(
                        number, name="Renamed", in_table=None
                    )
                else:
                    CustomColumnAdapter(db).delete_custom_column(number)
        assert transaction_active(conn)
    finally:
        conn.rollback()
    with pytest.raises(ValueError, match="No such relation"):
        db.driver_wrapper.get_table_spec("review_pending_types", force_refresh=True)


@pytest.mark.parametrize("method", ["walk", "get_linear_row_list", "complete_row"])
def test_empty_rows_are_rejected_before_schema_lookup(db, method):
    with pytest.raises(InputIntegrityError, match="empty"):
        getattr(db.driver_wrapper, method)({})


def test_raw_query_get_has_consistent_row_and_missing_results(db):
    wrapper = db.driver_wrapper
    assert wrapper.get("SELECT ?", (42,), all=False) == (42,)
    assert wrapper.get("SELECT ?", (42,)) == [(42,)]
    assert wrapper.get("SELECT 1 WHERE 0", all=False) is None


def test_view_headings_quote_identifiers_and_preserve_primary_handle(db):
    conn = db.driver.conn
    conn.execute('CREATE VIEW "review quoted view" AS SELECT 42 AS value')
    assert db.driver_wrapper.get_view_column_headings("review quoted view") == ["value"]
    assert db.driver.conn is conn


def test_view_heading_inspection_closes_owned_connections(db, monkeypatch):
    opened = []
    acquire = db.driver.get_connection

    def record():
        connection = acquire()
        opened.append(connection)
        return connection

    monkeypatch.setattr(db.driver, "get_connection", record)
    for _ in range(3):
        db.driver_wrapper.get_view_column_headings("works")
    alive = 0
    for connection in opened:
        try:
            connection.execute("SELECT 1")
        except Exception:
            pass
        else:
            alive += 1
        finally:
            connection.close()
    assert alive == 0


@pytest.mark.parametrize(
    "lookup",
    [
        ViewMixin.direct_get_view_column_headings,
        ValueCastingMixin.direct_get_declared_types_for_table,
    ],
)
@pytest.mark.parametrize("fail", [False, True])
def test_schema_inspection_closes_temporary_connection_even_on_error(lookup, fail):
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE sample(value TEXT)").close()
    host = SimpleNamespace(
        conn=None,
        get_connection=lambda: connection,
        _DECLARED_TYPES_CACHE_ATTR="_declared_types",
    )
    if fail:
        connection.set_authorizer(lambda *args: sqlite3.SQLITE_DENY)
        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            lookup(host, "sample")
        assert "sample" not in getattr(host, "_declared_types", {})
    else:
        assert lookup(host, "sample")
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connection.execute("SELECT 1")
