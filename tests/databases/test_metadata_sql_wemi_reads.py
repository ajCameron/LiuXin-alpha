"""Check physical WEMI batching through its real Catalog callers."""

import re
from dataclasses import replace
from pathlib import Path

import pytest

from LiuXin_alpha.catalog.api.common import CatalogMutationError, CatalogNotFoundError
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases.db_types.macro_types import LinkValue
from LiuXin_alpha.errors import InputIntegrityError

DIRECTIONS = (
    (
        "works",
        "expressions",
        "get_expressions_for_work",
        "expressions",
        "list_for_work",
    ),
    ("expressions", "works", "get_works_for_expression", "expressions", "list_works"),
    (
        "expressions",
        "manifestations",
        "get_manifestations_for_expression",
        "manifestations",
        "list_for_expression",
    ),
    (
        "manifestations",
        "expressions",
        "get_expressions_for_manifestation",
        "manifestations",
        "list_expressions",
    ),
)
DISPLAY = {
    "works": "work_title",
    "expressions": "expression_label",
    "manifestations": "manifestation_edition_statement",
}


def _entity(db, table, text):
    return db.macros.insert_row(table, {DISPLAY[table]: text})


def test_worked_example_keeps_novel_and_introduction_separate(db):
    guide = (
        Path(__file__).resolve().parents[2]
        / "dev-docs/databases/table-sql-ownership.md"
    )
    example = guide.read_text().split("```python\n", 1)[1].split("```", 1)[0]
    exec(compile(example, str(guide), "exec"), {"db": db})


def _count_reads(db, monkeypatch):
    """Record data SELECTs through the shared connection without changing it."""
    original = db.macros._macro_connection
    reads = []

    class Connection:
        def __init__(self, connection):
            self.connection = connection

        def execute(self, sql, bindings=()):
            match = re.search(r'\bFROM "([^"]+)"', sql)
            if sql.startswith("SELECT") and match:
                reads.append((match[1], len(bindings)))
            return self.connection.execute(sql, bindings)

        def __getattr__(self, name):
            return getattr(self.connection, name)

    monkeypatch.setattr(db.macros, "_macro_connection", lambda: Connection(original()))
    return reads


@pytest.mark.parametrize("primary,secondary,helper,repo_name,method", DIRECTIONS)
def test_wemi_readers_preserve_catalog_rows_order_and_link_metadata(
    db, primary, secondary, helper, repo_name, method
):
    source = _entity(db, primary, "Source")
    destinations = [_entity(db, secondary, label) for label in ("First", "Second")]
    spec = db.driver_wrapper.get_link_spec(primary, secondary)
    extra_column = next(
        c.name for c in spec.extra_link_columns if c.name.endswith("_source")
    )
    for index, destination in enumerate(destinations):
        db.macros.upsert_link(
            spec,
            source,
            LinkValue(
                destination, priority=index + 1, extra={extra_column: "test provenance"}
            ),
        )
    links = db.macros.get_link_rows(spec, source)
    assert [link.secondary_id for link in links] == destinations[::-1]
    reader = getattr(db.metadata_sql, helper)
    records = reader(source)
    assert tuple(record.link for record in records) == links
    assert [record.row[spec.secondary_id_col] for record in records] == destinations[
        ::-1
    ]
    catalog = Catalog(db)
    rows = getattr(getattr(catalog, repo_name), method)(source)
    assert rows == tuple(
        {
            **record.row,
            "_catalog_link": {
                "primary_id": record.link.primary_id,
                "secondary_id": record.link.secondary_id,
                "type": record.link.link_type,
                "priority": record.link.priority,
                "extra": dict(record.link.extra),
            },
        }
        for record in records
    )
    rows[0][DISPLAY[secondary]] = "local edit"
    rows[0]["_catalog_link"]["extra"][extra_column] = "local provenance"
    reread = getattr(getattr(catalog, repo_name), method)(source)
    assert reread[0][DISPLAY[secondary]] == "Second"
    assert reread[0]["_catalog_link"]["extra"][extra_column] == "test provenance"
    # Current core WEMI links are untyped: explicit None is not an all-types alias.
    assert not spec.typed
    with pytest.raises(InputIntegrityError, match="untyped"):
        reader(source, link_type=None)


@pytest.mark.parametrize("primary,secondary,helper,repo_name,method", DIRECTIONS)
def test_wemi_readers_keep_empty_and_missing_source_contracts(
    db, monkeypatch, primary, secondary, helper, repo_name, method
):
    source = _entity(db, primary, "Unlinked")
    catalog = Catalog(db)
    read = getattr(getattr(catalog, repo_name), method)
    counts = _count_reads(db, monkeypatch)
    assert read(source) == ()
    assert not [table for table, _ in counts if table == secondary]
    assert getattr(db.metadata_sql, helper)(-1) == ()
    with pytest.raises(CatalogNotFoundError, match=f"{primary} row not found"):
        read(987654321)
    for bad_id, error in ((True, TypeError), ("1", TypeError), (-1, ValueError)):
        with pytest.raises(error):
            read(bad_id)
    with pytest.raises(InputIntegrityError, match="integers"):
        getattr(db.metadata_sql, helper)(True)


def test_1001_linked_expressions_use_three_destination_reads(db, monkeypatch):
    source = _entity(db, "works", "Many expressions")
    spec = db.driver_wrapper.get_link_spec("works", "expressions")
    ids = tuple(range(10000, 11001))
    with db.macros.transaction() as conn:
        conn.executemany(
            "INSERT INTO expressions (expression_id, expression_label) VALUES (?, ?)",
            [(value, f"Expression {value}") for value in ids],
        )
        conn.executemany(
            f'INSERT INTO "{spec.link_table}" ("{spec.primary_link_col}", "{spec.secondary_link_col}", "{spec.priority_link_col}") VALUES (?, ?, ?)',
            [(source, value, index) for index, value in enumerate(ids)],
        )
    catalog = Catalog(db)
    counts = _count_reads(db, monkeypatch)
    rows = catalog.expressions.list_for_work(source)
    assert [row["expression_id"] for row in rows] == list(reversed(ids))
    assert counts == [
        ("works", 1),
        (spec.link_table, 1),
        ("expressions", 500),
        ("expressions", 500),
        ("expressions", 1),
    ]


def test_id_batches_deduplicate_omit_missing_and_materialize_before_reads(
    db, monkeypatch
):
    first = _entity(db, "works", "First")
    second = _entity(db, "works", "Second")
    counts = _count_reads(db, monkeypatch)
    rows = db.macros.get_rows_by_ids(
        "works", (value for value in [second, first, second, -1])
    )
    assert tuple(rows) == (second, first)
    assert rows[first]["work_title"] == "First"
    assert counts == [("works", 3)]
    counts.clear()
    assert db.macros.get_rows_by_ids("works", []) == {}
    for invalid in (True, "1", ("work", 1), 1.0):
        with pytest.raises(InputIntegrityError, match="integers"):
            db.macros.get_rows_by_ids("works", [first, invalid])

    def broken_input():
        yield first
        raise RuntimeError("input failed")

    with pytest.raises(RuntimeError, match="input failed"):
        db.macros.get_rows_by_ids("works", broken_input())
    assert counts == []


def test_batched_reads_observe_pending_writes_without_committing(db):
    source = _entity(db, "works", "Committed")
    catalog = Catalog(db)
    spec = db.driver_wrapper.get_link_spec("works", "expressions")
    with pytest.raises(RuntimeError, match="rollback"):
        with db.macros.transaction():
            destination = _entity(db, "expressions", "Pending")
            db.macros.upsert_link(spec, source, LinkValue(destination, priority=1))
            assert (
                db.macros.get_rows_by_ids("expressions", [destination])[destination][
                    "expression_label"
                ]
                == "Pending"
            )
            assert (
                catalog.expressions.list_for_work(source)[0]["expression_id"]
                == destination
            )
            raise RuntimeError("rollback")
    assert catalog.expressions.list_for_work(source) == ()
    assert db.macros.get_rows_by_ids("expressions", [destination]) == {}


def test_null_destination_is_retained_and_catalog_raises_original_error(db):
    source = _entity(db, "works", "Dangling")
    spec = db.driver_wrapper.get_link_spec("works", "expressions")
    with db.macros.transaction() as conn:
        conn.execute(
            f'INSERT INTO "{spec.link_table}" ("{spec.primary_link_col}", "{spec.secondary_link_col}", "{spec.priority_link_col}") VALUES (?, NULL, 1)',
            (source,),
        )
    records = db.metadata_sql.get_expressions_for_work(source)
    assert len(records) == 1
    assert records[0].row is None
    assert records[0].link.secondary_id is None
    with pytest.raises(
        CatalogNotFoundError,
        match=f"expressions row linked from works:{source} is missing: None",
    ):
        Catalog(db).expressions.list_for_work(source)


def test_catalog_keeps_schema_validation_errors_and_batches_tags(db, monkeypatch):
    source = _entity(db, "works", "Source")
    catalog = Catalog(db)
    tag = catalog.tags.create({"tag": "History"})
    catalog.tags.link_to_work(entity_id=tag, work_id=source)
    counts = _count_reads(db, monkeypatch)
    assert catalog.tags.list_for_work(source)[0]["tag_id"] == tag
    assert [entry for entry in counts if entry[0] == "tags"] == [("tags", 1)]
    monkeypatch.setattr(db.driver_wrapper, "get_link_spec", lambda *args: None)
    with pytest.raises(CatalogMutationError, match="no catalog link exists"):
        catalog.expressions.list_for_work(source)


def test_row_batches_reject_views_and_invalid_endpoint_id_specs(db):
    with db.macros.transaction() as conn:
        conn.execute("CREATE VIEW batch_work_view AS SELECT * FROM works")
    for table in ("batch_work_view", "absent_table"):
        with pytest.raises(InputIntegrityError, match="physical table"):
            db.macros.get_rows_by_ids(table, [])
    spec = db.driver_wrapper.get_link_spec("works", "expressions")
    with pytest.raises(InputIntegrityError, match="physical ID column"):
        db.macros.get_linked_rows(replace(spec, secondary_id_col="expression_label"), 1)
    with pytest.raises(InputIntegrityError, match="physical table"):
        db.macros.get_linked_rows(replace(spec, primary_table="batch_work_view"), 1)
