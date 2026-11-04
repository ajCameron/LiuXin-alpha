"""Exercise wrapper fixes against a disposable live PostgreSQL schema."""

from contextlib import closing
from unittest.mock import Mock

import pytest

from LiuXin_alpha.databases.api.macros_api import MacrosAPI
from LiuXin_alpha.databases.schema_specs import RelationKind
from LiuXin_alpha.databases.transactions import transaction_active
from LiuXin_alpha.errors import DatabaseDriverError
from tests.databases import test_row_write_safety_postgres_live as fixtures

pg_db = fixtures.pg_db
stored = fixtures.stored


@pytest.mark.parametrize(
    "lookup", ["relation", "table_spec", "tables", "columns", "views"]
)
def test_postgres_schema_inspection_keeps_primary_writes_pending(pg_db, lookup):
    db, _ = pg_db
    connection = db.driver.conn
    connection.execute("INSERT INTO works(work_title) VALUES ('pending')")
    wrapper = db.driver_wrapper
    calls = {
        "relation": lambda: wrapper.get_relation_type("works"),
        "table_spec": lambda: wrapper.get_table_spec("works", force_refresh=True),
        "tables": lambda: wrapper.get_tables(force_refresh=True),
        "columns": lambda: db.driver.direct_get_tables_and_columns(force_refresh=True),
        "views": lambda: wrapper.get_views(force_refresh=True),
    }
    calls[lookup]()
    assert db.driver.conn is connection
    assert transaction_active(connection)
    assert stored(pg_db, "works", "work_title", "work_id") == []
    connection.rollback()
    assert stored(pg_db, "works", "work_title", "work_id") == []


def test_postgres_single_column_write_preserves_another_writers_field(
    pg_db, monkeypatch
):
    db, _ = pg_db
    wrapper = db.driver_wrapper
    row_id = wrapper.add_row({"work_title": "before", "work_scratch": "before"})
    original = wrapper.get_row_from_id

    def interleaved(*args, **kwargs):
        stale = original(*args, **kwargs)
        with closing(db.driver.get_connection()) as connection, connection:
            connection.execute(
                "UPDATE works SET work_scratch=? WHERE work_id=?",
                ("other edit", row_id),
            )
        return stale

    monkeypatch.setattr(wrapper, "get_row_from_id", interleaved)
    wrapper.update_column("works", row_id, "work_title", "requested edit")
    assert stored(pg_db, "works", "work_title, work_scratch", "work_id") == [
        ("requested edit", "other edit")
    ]


def test_postgres_custom_definition_and_supplied_ddl_roll_back_together(
    pg_db, monkeypatch
):
    """Verify the shared transaction boundary independently of SQLite-only DDL."""
    db, _ = pg_db
    wrapper = db.driver_wrapper
    known = set(wrapper.custom_tables)

    def fail(*, conn, **kwargs):
        assert conn.execute("SELECT COUNT(*) FROM custom_columns").fetchone()[0] == 1
        conn.execute("CREATE TABLE wrapper_partial(value TEXT)").close()
        raise RuntimeError("injected DDL failure")

    # This backend deliberately lacks the SQLite compatibility DDL helper.
    # Supply an explicitly declared legacy capability whose common operations
    # still use the real PostgreSQL provider and transaction. One injected DDL
    # failure exercises rollback without pretending PostgreSQL implements it.
    provider = Mock(spec=MacrosAPI, wraps=db.macros)
    provider.create_cc_table = fail
    monkeypatch.setattr(db, "_macros", provider)
    with pytest.raises(RuntimeError, match="injected DDL failure"):
        wrapper.create_custom_column("Failure", label="failure", in_table="works")
    assert stored(pg_db, "custom_columns", "custom_column_id", "custom_column_id") == []
    assert wrapper.get_relation_type("wrapper_partial") is None
    assert wrapper.custom_tables == known


def test_postgres_unsupported_custom_ddl_does_not_leave_a_definition(pg_db):
    """A pre-existing backend limitation must fail without publishing metadata."""
    db, _ = pg_db
    with pytest.raises(DatabaseDriverError, match="requires legacy SQLite macros"):
        db.driver_wrapper.create_custom_column(
            "Unsupported", label="unsupported", in_table="works"
        )
    assert stored(pg_db, "custom_columns", "custom_column_id", "custom_column_id") == []


def test_postgres_current_folder_hierarchy_and_view_filter(pg_db):
    db, _ = pg_db
    wrapper = db.driver_wrapper
    with db.driver.conn:
        db.driver.conn.execute(
            "CREATE VIEW wrapper_review_view AS SELECT work_id FROM works"
        )
    wrapper.get_tables(force_refresh=True)
    root = wrapper.add_row({"folder_name": "root"})
    child = wrapper.add_row({"folder_name": "child", "folder_parent_id": root})
    assert wrapper.get_parent_column("folders") == "folder_parent_id"
    assert {
        row["folder_id"]
        for row in wrapper.walk(wrapper.get_row_from_id("folders", root))
    } == {root, child}
    physical = list(wrapper.iter_table_specs(include_views=False))
    all_specs = list(wrapper.iter_table_specs(include_views=True))
    assert physical
    assert all(spec.relation_kind == RelationKind.TABLE for spec in physical)
    assert any(spec.relation_kind == RelationKind.VIEW for spec in all_specs)
    assert len(all_specs) == len({spec.name for spec in all_specs})


def test_postgres_raw_query_get_and_missing_row_contracts(pg_db):
    db, _ = pg_db
    wrapper = db.driver_wrapper
    assert wrapper.get("SELECT ?", (42,), all=False) == (42,)
    assert wrapper.get("SELECT ?", (42,)) == [(42,)]
    assert wrapper.get("SELECT 1 WHERE FALSE", all=False) is None
    assert wrapper.get_row_from_id("works", -1) is False
    spec = wrapper.get_table_spec("works", force_refresh=True)
    title = next(column for column in spec.columns if column.name == "work_title")
    assert title.declared_type.lower() == "text"
