"""Exercise physical helper failures exposed by the complete database typing pass."""

from __future__ import annotations

from contextlib import closing

import pytest

from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError
from tests.databases.test_row_write_safety import db as db, template as template


@pytest.fixture
def tree(db):
    """Use a real physical parent-pointer table with unambiguous display naming."""
    with closing(db.driver.get_connection()) as connection:
        connection.executescript(
            "CREATE TABLE review_nodes (review_node_id INTEGER PRIMARY KEY, "
            "review_node TEXT, review_node_parent_id INTEGER, review_node_full TEXT, "
            "review_node_tree_id TEXT);"
            "INSERT INTO review_nodes VALUES (1, 'Root', NULL, NULL, NULL), "
            "(2, 'Branch', 1, NULL, NULL), (3, 'Leaf', 2, NULL, NULL);"
        )
    db.driver.refresh()
    return db.driver


def test_tree_aggregation_uses_the_existing_chain_helper(tree):
    assert (
        tree.direct_get_tree_aggregation_str("review_nodes", "review_node", 3)
        == "Root: Branch: Leaf"
    )
    with pytest.raises(InputIntegrityError, match="not found"):
        tree.direct_get_tree_aggregation_str("review_nodes", "review_node", 999)


def test_full_paths_use_physical_column_discovery(tree):
    assert tree.direct_set_full_column("review_nodes") is True
    with closing(tree.get_connection()) as connection:
        assert connection.execute(
            "SELECT review_node_full FROM review_nodes ORDER BY review_node_id"
        ).fetchall() == [("Root",), ("Root: Branch",), ("Root: Branch: Leaf",)]


def test_descendant_collection_uses_the_actual_identity_column(tree):
    leaf = tree.direct_get_row_dict_from_id("review_nodes", 3)
    assert {row["review_node_id"] for row in tree.direct_get_all_tree_rows(leaf)} == {
        1,
        2,
        3,
    }


@pytest.mark.parametrize("parent", [999, 3])
def test_ancestor_walk_rejects_missing_parents_and_cycles(tree, monkeypatch, parent):
    with closing(tree.get_connection()) as connection:
        connection.execute(
            "UPDATE review_nodes SET review_node_parent_id=? WHERE review_node_id=1",
            (parent,),
        )
        connection.commit()
    read = tree.direct_get_row_dict_from_id
    calls = 0

    def bounded_read(table, row_id):
        nonlocal calls
        calls += 1
        if calls > 6:
            pytest.fail("The parent traversal repeated rows instead of terminating")
        return read(table, row_id)

    leaf = read("review_nodes", 3)
    monkeypatch.setattr(tree, "direct_get_row_dict_from_id", bounded_read)
    with pytest.raises(DatabaseIntegrityError):
        tree.get_linear_row_index(leaf)


def test_dirty_record_uses_the_queue_wired_by_the_facade(db):
    queue = db.dirty_records_queue
    while not queue.empty():
        queue.get_nowait()
    db.driver.direct_dirty_record("review_items", 7, "typed notification")
    assert queue.get_nowait() == ("review_items", 7, "typed notification")
    assert queue.empty()


def test_registered_sql_collation_accepts_database_text(db):
    with closing(db.driver.get_connection()) as connection:
        assert connection.execute(
            "SELECT value FROM (SELECT 'Beta' AS value UNION ALL SELECT 'alpha') "
            "ORDER BY value COLLATE icucollate"
        ).fetchall() == [("alpha",), ("Beta",)]
        assert connection.execute("SELECT 'alphabet' REGEXP '^alpha'").fetchone() == (
            1,
        )


@pytest.mark.parametrize(
    ("expression", "expected"),
    [("concat(value)", "B,A"), ("identifiers_concat(position, value)", "2:B,1:A")],
)
def test_registered_aggregate_factories_support_sqlite_objects(
    db, expression, expected
):
    with closing(db.driver.get_connection()) as connection:
        assert connection.execute(
            f"SELECT {expression} FROM "
            "(SELECT 2 AS position, 'B' AS value UNION ALL SELECT 1, 'A')"
        ).fetchone() == (expected,)


def test_row_iteration_materializes_one_shot_headings(db):
    rows = db.driver.direct_iterator_return(
        "SELECT 1 AS number UNION ALL SELECT 2", iter(["number"])
    )
    assert list(rows) == [{"number": 1}, {"number": 2}]


def test_table_creation_honors_nested_column_specs(db):
    db.driver.direct_create_main_table(
        "review_measurements", {"label": {}, "amount": {"datatype": "INTEGER"}}
    )
    with closing(db.driver.get_connection()) as connection:
        declared = {
            row[1]: row[2]
            for row in connection.execute("PRAGMA table_info(review_measurements)")
        }
    assert declared["review_measurement_label"] == "TEXT"
    assert declared["review_measurement_amount"] == "INTEGER"
    assert declared["review_measurement_datestamp"] == "DATETIME"


@pytest.mark.parametrize("helper", ["get", "get_row"])
def test_native_sqlite_fetch_helpers_preserve_operational_errors(db, helper):
    import sqlite3

    with closing(db.driver.get_connection()) as connection:
        with pytest.raises(sqlite3.OperationalError, match="no such table") as error:
            getattr(connection, helper)("SELECT * FROM missing_review_table")
        assert isinstance(error.value.__cause__, sqlite3.OperationalError)


def test_apsw_native_connection_scalar_and_aggregate_callbacks():
    pytest.importorskip("apsw")
    from LiuXin_alpha.databases.database_driver_plugins.SQLite_apsw.databasedriver import (
        Connection,
    )

    with closing(Connection(":memory:")) as connection:
        assert connection.get("SELECT 7", all=False) == 7
        assert connection.get("SELECT 7 WHERE 0", all=False) is None
        assert connection.get("SELECT identifiers_concat('isbn', '123')") == [
            ("isbn:123",)
        ]


def test_macro_get_uses_the_bound_wrapper(db):
    assert db.macros.get("SELECT ? AS value", (7,)) == [(7,)]
    assert db.macros.get("SELECT ? AS value", (7,), all=False) == (7,)


def test_macro_reader_preserves_a_facade_override(db, monkeypatch):
    observed = []

    def reader(sql, values=None, *, all=True):
        observed.append((sql, values, all))
        return [("overridden",)]

    monkeypatch.setattr(db, "get", reader)
    assert db.macros.get("SELECT ?", (1,), all=False) == [("overridden",)]
    assert observed == [("SELECT ?", (1,), False)]


def test_detached_macros_reject_database_operations():
    from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver import (
        DatabaseDriver,
    )

    driver = DatabaseDriver({"database_path": ":memory:"}, set_conn=False)
    try:
        assert driver.macros.db is None
        with pytest.raises(DatabaseIntegrityError, match="not attached"):
            driver.macros.get("SELECT 1")
    finally:
        driver.close()


def test_native_apsw_batch_rollback_and_binding_keywords():
    apsw = pytest.importorskip("apsw")
    from LiuXin_alpha.databases.database_driver_plugins.SQLite_apsw.databasedriver import (
        Connection,
    )

    with closing(Connection(":memory:")) as connection:
        connection.execute(
            statements="CREATE TABLE sample (value INTEGER UNIQUE)"
        ).close()
        with pytest.raises(apsw.ConstraintError):
            connection.executemany(
                statements="INSERT INTO sample VALUES (?)",
                sequenceofbindings=iter([(1,), (1,)]),
                can_cache=False,
            )
        assert connection.get("SELECT COUNT(*) FROM sample", all=False) == 0
        connection.executemany(
            sql="INSERT INTO sample VALUES (:value)",
            sequence_of_bindings=iter([{"value": 2}, {"value": 3}]),
        ).close()
        with closing(
            connection.execute(sql="SELECT SUM(value) FROM sample", can_cache=False)
        ) as cursor:
            assert cursor.fetchone() == (5,)
