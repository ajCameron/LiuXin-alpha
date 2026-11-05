"""Prove legacy cached writes publish all SQL or none on both SQLite drivers."""

from contextlib import closing
from copy import deepcopy

import pytest

from tests.databases.test_custom_column_service_contracts import facade_with_cache
from tests.databases.test_row_write_safety import db as db, template as template, stored


@pytest.fixture
def custom(db):
    facade = facade_with_cache()
    facade.db = db
    facade.notify = lambda *args: None
    facade.dirtied = lambda *args, **kwargs: None
    facade.custom_data_adapters = {"text": lambda value, data: value}
    facade.custom_column_label_map = {"shelf": facade.custom_column_num_map[3]}
    with closing(db.driver.get_connection()) as conn:
        conn.executescript("""
            CREATE TABLE custom_column_3 (custom_column_3_id INTEGER PRIMARY KEY,
                custom_column_3_value TEXT COLLATE NOCASE UNIQUE);
            CREATE TABLE books_custom_column_3_link (
                books_custom_column_3_link_id INTEGER PRIMARY KEY,
                books_custom_column_3_link_book INTEGER,
                books_custom_column_3_link_value INTEGER,
                UNIQUE (books_custom_column_3_link_book, books_custom_column_3_link_value));
            INSERT INTO custom_column_3 VALUES (1, 'old');
            INSERT INTO books_custom_column_3_link VALUES (1, 7, 1), (2, 8, 1);
            CREATE VIEW meta2 AS SELECT books_custom_column_3_link_book AS id,
                group_concat(custom_column_3_value, '|') AS custom_3
                FROM books_custom_column_3_link JOIN custom_column_3
                ON books_custom_column_3_link_value=custom_column_3_id
                GROUP BY books_custom_column_3_link_book;
        """)
    return facade


def sql_snapshot(db):
    return (
        stored(db, "SELECT * FROM custom_column_3 ORDER BY custom_column_3_id"),
        stored(db, "SELECT * FROM books_custom_column_3_link ORDER BY 1, 2"),
    )


def mutate(custom, route):
    if route == "single":
        custom.set_custom(7, ["new"], num=3)
    elif route == "bulk":
        custom.set_custom_bulk([7, 8], ["new"], num=3)
    else:
        custom.set_custom_bulk_multiple(
            iter([7, 8]), add=["new"], remove=["old"], num=3, notify=True
        )


@pytest.mark.parametrize("route", ["single", "bulk", "multiple"])
@pytest.mark.parametrize("failure", ["sql", "cache", "dirty", "commit"])
def test_write_failure_restores_sql_cache_and_suppresses_notifications(
    custom, monkeypatch, route, failure
):
    before_sql = sql_snapshot(custom.db)
    cache = custom.data
    before_cache = deepcopy(cache._data)
    notices = []
    custom.notify = lambda event, ids: notices.extend(ids)

    def fail(*args, **kwargs):
        raise RuntimeError("injected mutation failure")

    if failure == "cache":
        real_set = cache.set

        def fail_after_cache_write(*args, **kwargs):
            real_set(*args, **kwargs)
            fail()

        monkeypatch.setattr(cache, "set", fail_after_cache_write)
    elif failure == "dirty":
        custom.dirtied = fail
    elif failure == "commit":
        monkeypatch.setattr(custom.conn, "commit", fail)
    elif route == "multiple":
        original = custom.db.macros.do_cc_db_bulk_addition

        def fail_after_sql(*args, **kwargs):
            original(*args, **kwargs)
            fail()

        monkeypatch.setattr(custom.db.macros, "do_cc_db_bulk_addition", fail_after_sql)
    else:
        # Fails after removing the old link, before its replacement is written.
        monkeypatch.setattr(custom.db.macros, "add_cc_table_value", fail)
    with pytest.raises(RuntimeError, match="injected"):
        mutate(custom, route)
    assert sql_snapshot(custom.db) == before_sql
    assert cache._data == before_cache
    assert custom.data is cache
    assert notices == []
    assert not custom.conn.in_transaction
    assert (
        custom.conn.execute(
            "SELECT name FROM sqlite_temp_master WHERE name LIKE 'temp_bulk_tag_edit_%'"
        ).fetchall()
        == []
    )


@pytest.mark.parametrize("route", ["single", "bulk", "multiple"])
def test_notifications_observe_committed_values(custom, route):
    notices = []

    def notify(event, ids):
        values = dict(stored(custom.db, "SELECT id, custom_3 FROM meta2"))
        assert all(values[row] == "new" for row in ids)
        assert all(custom.data._data[row][1] == "new" for row in ids)
        notices.extend(ids)

    custom.notify = notify
    mutate(custom, route)
    assert sorted(notices) == ([7] if route == "single" else [7, 8])


@pytest.mark.parametrize("route", ["single", "bulk", "multiple"])
def test_borrowed_transaction_is_rejected_without_committing_prior_work(custom, route):
    connection = custom.conn
    connection.execute("BEGIN")
    connection.execute("INSERT INTO review_items(review_item_text) VALUES ('pending')")
    before = sql_snapshot(custom.db)
    with pytest.raises(RuntimeError, match="own transaction"):
        mutate(custom, route)
    assert connection.in_transaction
    assert sql_snapshot(custom.db) == before
    assert stored(custom.db, "SELECT * FROM review_items") == []
    connection.rollback()


def test_commit_false_is_rejected_before_any_effect(custom):
    before = sql_snapshot(custom.db)
    with pytest.raises(ValueError, match="commit=False"):
        custom.set_custom(7, ["new"], num=3, commit=False)
    assert sql_snapshot(custom.db) == before


def test_notification_failure_reports_that_sql_has_already_committed(custom):
    def notify(*args):
        raise RuntimeError("listener unavailable")

    custom.notify = notify
    with pytest.raises(RuntimeError, match="listener unavailable") as error:
        custom.set_custom(7, ["new"], num=3)
    assert "committed" in " ".join(error.value.__notes__)
    assert dict(stored(custom.db, "SELECT id, custom_3 FROM meta2"))[7] == "new"
    assert custom.data._data[7][1] == "new"


def test_temp_table_macros_never_commit_a_borrowed_transaction(custom):
    connection = custom.conn
    connection.execute("BEGIN")
    connection.execute("INSERT INTO review_items(review_item_text) VALUES ('pending')")
    custom.db.macros.create_cc_temp_tables(iter(["selected_ids"]), conn=connection)
    custom.db.macros.destroy_cc_temp_tables(iter(["selected_ids"]), conn=connection)
    assert connection.in_transaction
    assert stored(custom.db, "SELECT * FROM review_items") == []
    connection.rollback()


@pytest.mark.parametrize("route", ["delete_id", "delete_multiple", "rename", "merge"])
@pytest.mark.parametrize("failure", [None, "cache", "commit"])
def test_delete_and_rename_share_sql_and_cache_failure_boundary(
    custom, monkeypatch, route, failure
):
    custom.field_metadata = {
        "#shelf": {"table": "custom_column_3", "link_column": "value"}
    }
    if route == "merge":
        custom.conn.execute("INSERT INTO custom_column_3 VALUES (2, 'new')")
        custom.conn.execute("INSERT INTO books_custom_column_3_link VALUES (3, 7, 2)")
        custom.conn.commit()
        custom.data._data[7][1] = "old|new"
    before = sql_snapshot(custom.db)
    cache = deepcopy(custom.data._data)

    def fail(*args, **kwargs):
        raise RuntimeError("injected mutation failure")

    if failure == "cache":
        monkeypatch.setattr(custom.data, "set", fail)
    elif failure == "commit":
        monkeypatch.setattr(custom.conn, "commit", fail)

    def apply():
        if route == "delete_id":
            custom.delete_custom_item_using_id(1, num=3)
        elif route == "delete_multiple":
            assert custom.delete_item_from_multiple("OLD", num=3) == [7, 8]
        else:
            custom.rename_custom_item(1, "new", num=3)

    if failure:
        with pytest.raises(RuntimeError, match="injected"):
            apply()
        assert sql_snapshot(custom.db) == before
        assert custom.data._data == cache
    else:
        apply()
        expected = None if route.startswith("delete") else "new"
        values = dict(stored(custom.db, "SELECT id, custom_3 FROM meta2"))
        for owner in (7, 8):
            assert values.get(owner) == custom.data._data[owner][1] == expected


def test_delete_macro_preserves_caller_transaction(custom):
    connection = custom.conn
    before = sql_snapshot(custom.db)
    connection.execute("BEGIN")
    custom.db.macros.delete_cc_item(
        "custom_column_3", "books_custom_column_3_link", 1, conn=connection
    )
    assert connection.in_transaction
    assert sql_snapshot(custom.db) == before
    connection.rollback()
    assert sql_snapshot(custom.db) == before


def test_partial_bulk_failure_restores_first_owner_as_well(custom, monkeypatch):
    before = sql_snapshot(custom.db)
    cache = deepcopy(custom.data._data)
    original = custom._set_custom

    def second_fails(owner, *args, **kwargs):
        if owner == 8:
            raise RuntimeError("second owner failed")
        return original(owner, *args, **kwargs)

    monkeypatch.setattr(custom, "_set_custom", second_fails)
    with pytest.raises(RuntimeError, match="second owner"):
        custom.set_custom_bulk([7, 8], ["new"], num=3)
    assert sql_snapshot(custom.db) == before
    assert custom.data._data == cache


@pytest.mark.parametrize("route", ["delete", "rename"])
def test_shared_value_changes_preserve_other_cached_choices(custom, route):
    custom.field_metadata = {
        "#shelf": {"table": "custom_column_3", "link_column": "value"}
    }
    custom.conn.execute("INSERT INTO custom_column_3 VALUES (2, 'keep')")
    custom.conn.execute("INSERT INTO books_custom_column_3_link VALUES (3, 7, 2)")
    custom.conn.commit()
    custom.data._data[7][1] = "old|keep"
    if route == "delete":
        custom.delete_custom_item_using_id(1, num=3)
    else:
        custom.rename_custom_item(1, "new", num=3)
    assert set(custom.data._data[7][1].split("|")) == (
        {"keep"} if route == "delete" else {"new", "keep"}
    )


def test_owner_iteration_failure_never_publishes_a_prefix(custom):
    before = sql_snapshot(custom.db)

    def owners():
        yield 7
        raise RuntimeError("owner source failed")

    with pytest.raises(RuntimeError, match="owner source"):
        custom.set_custom_bulk_multiple(owners(), add=["new"], num=3)
    assert sql_snapshot(custom.db) == before
    assert not custom.conn.in_transaction


def test_case_change_refreshes_and_notifies_all_shared_owners(custom):
    dirty, notices = [], []
    custom.dirtied = lambda ids, **kwargs: dirty.extend(ids)
    custom.notify = lambda event, ids: notices.extend(ids)
    assert custom.set_custom(
        7, ["OLD"], num=3, append=True, allow_case_change=True
    ) == {7, 8}
    assert sorted(dirty) == sorted(notices) == [7, 8]
    assert [custom.data._data[owner][1] for owner in (7, 8)] == ["OLD", "OLD"]
