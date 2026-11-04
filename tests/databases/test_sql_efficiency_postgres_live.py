"""Check the efficiency helpers on real PostgreSQL in disposable schemas."""

from __future__ import annotations

import pytest

from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases.db_types.macro_types import LinkValue
from LiuXin_alpha.databases.query_indexes import QUERY_INDEXES
from LiuXin_alpha.errors import DatabaseIntegrityError
from tests.databases.test_transaction_ownership_postgres_live import (
    pg_database as pg_database,
)


def test_postgres_pages_aggregates_and_batched_graphs(pg_database):
    db, _, _ = pg_database
    macros = db.macros
    macros.insert_rows("works", ({"work_title": f"Owner {i}"} for i in range(1001)))
    owners = tuple(
        row["work_id"] for row in macros.get_rows("works", order_by=("work_id",))
    )
    expression = macros.insert_row("expressions", {"expression_label": "Shared"})
    spec = db.driver_wrapper.get_link_spec("works", "expressions")
    macros.replace_links_bulk(
        spec, {owner: (LinkValue(expression, priority=1),) for owner in owners}
    )
    linked = macros.get_linked_rows_bulk(spec, owners)
    assert len(linked) == 1001
    assert all(rows[0].row["expression_id"] == expression for rows in linked.values())
    linked[owners[0]][0].row["expression_label"] = "local copy"
    assert linked[owners[-1]][0].row["expression_label"] == "Shared"
    counts = macros.get_link_counts_bulk(spec, owners)
    assert all(
        count == {"links": 1, "distinct": 1, "missing": 0} for count in counts.values()
    )
    page, complete = macros.get_rows_page("works", after_id=owners[500], limit=20)
    assert not complete
    assert [row["work_id"] for row in page] == list(owners[501:521])
    assert macros.aggregate_rows("works")["count"] == 1001
    assert macros.has_rows("works", where={"work_id": owners[-1]})
    with pytest.raises(RuntimeError, match="rollback batch"):
        with macros.transaction():
            macros.delete_rows_where("works", where={"work_id": owners[-1]})
            assert not macros.has_rows("works", where={"work_id": owners[-1]})
            raise RuntimeError("rollback batch")
    assert macros.has_rows("works", where={"work_id": owners[-1]})
    # The PostgreSQL adapter must preserve search semantics via bounded pages.
    assert not macros.supports_normalized_search()
    assert (
        Catalog(db).search.find('work.title:"=owner 1000"')[0]["work_id"] == owners[-1]
    )


def test_postgres_late_staged_failure_restores_links_and_releases_temporary_tables(
    pg_database,
):
    import psycopg2

    db, _, _ = pg_database
    macros = db.macros
    owner = macros.insert_row("works", {"work_title": "Rollback owner"})
    targets = [
        macros.insert_row("expressions", {"expression_label": str(i)}) for i in range(3)
    ]
    spec = db.driver_wrapper.get_link_spec("works", "expressions")
    initial = macros.replace_links(spec, owner, [LinkValue(i) for i in targets[:2]])
    with macros.transaction() as conn:
        with pytest.raises((psycopg2.IntegrityError, DatabaseIntegrityError)):
            macros.replace_links_bulk(
                spec, {owner: (LinkValue(targets[2]), LinkValue(999999))}
            )
        assert macros.get_link_rows(spec, owner) == initial
        assert (
            conn.execute(
                "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'liuxin_links_%'"
            ).fetchone()[0]
            == 0
        )
        # The failed inner savepoint must not leave the enclosing unit aborted.
        macros.upsert_links(spec, owner, (LinkValue(targets[0], priority=2),))
    assert len(macros.get_link_rows(spec, owner)) == 2


def test_postgres_query_index_migration_checks_definitions_and_obeys_outer_rollback(
    pg_database,
):
    db, _, _ = pg_database
    macros = db.macros
    with macros.transaction() as conn:
        for index in QUERY_INDEXES:
            conn.execute(f'DROP INDEX IF EXISTS "{db.driver.schema}"."{index.name}"')
    wanted = tuple(index.name for index in QUERY_INDEXES)
    assert macros.audit_query_indexes().missing == wanted
    with pytest.raises(RuntimeError, match="rollback migration"):
        with macros.transaction():
            assert macros.migrate_query_indexes().created == wanted
            raise RuntimeError("rollback migration")
    assert macros.audit_query_indexes().missing == wanted
    assert macros.migrate_query_indexes().created == wanted
    assert macros.migrate_query_indexes().clean
    assert macros.migrate_query_indexes().created == ()
    with macros.transaction() as conn:
        index = QUERY_INDEXES[0]
        conn.execute(f'DROP INDEX "{db.driver.schema}"."{index.name}"')
        conn.execute(
            f'CREATE INDEX "{index.name}" ON "{db.driver.schema}".annotations (annotation_kind)'
        )
    assert macros.audit_query_indexes().conflicts == (index.name,)
    with pytest.raises(DatabaseIntegrityError, match="Conflicting query indexes"):
        macros.migrate_query_indexes()
