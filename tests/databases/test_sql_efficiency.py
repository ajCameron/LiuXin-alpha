"""Verify bounded physical SQL operations and their production callers."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.catalog.retrieval.hydration.read_sources import (
    DatabaseMetadataReadSource,
)
from LiuXin_alpha.core.browse_api import _work_summaries
from LiuXin_alpha.core.storage_graph_api import CoreStorageGraphAPI
from LiuXin_alpha.databases.db_types.macro_types import LinkValue
from LiuXin_alpha.databases.query_indexes import QUERY_INDEXES
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError
from tests.databases.test_metadata_sql_wemi_reads import _count_reads


def test_catalog_pages_and_cursor_traversal_do_not_read_whole_table(db, monkeypatch):
    repo = Catalog(db).works
    ids = [repo.create({"title": f"Cursor example {i}"}) for i in range(7)]
    repo.delete(ids[2])
    expected = [row["work_id"] for row in repo._all_rows()]

    def no_full_read(*args, **kwargs):
        raise AssertionError("Pagination attempted a whole-table read")

    monkeypatch.setattr(repo, "_all_rows", no_full_read)
    assert [row["work_id"] for row in repo.list(limit=3, offset=1)] == expected[1:4]
    assert [row["work_id"] for row in repo.list_after(ids[2], limit=2)] == ids[3:5]
    assert [row["work_id"] for row in repo.iter_rows(batch_size=2)] == expected
    assert repo.list_after(ids[-1]) == ()
    counts = _count_reads(db, monkeypatch)
    assert repo.list(limit=0) == repo.list_after(limit=0) == ()
    for cursor in (True, "1"):
        with pytest.raises(TypeError):
            repo.list_after(cursor, limit=0)
    with pytest.raises(ValueError):
        repo.list_after(-1, limit=0)
    with pytest.raises(ValueError):
        tuple(repo.iter_rows(batch_size=0))
    assert counts == []


def test_cursor_filters_and_current_transaction(db):
    macros = db.macros
    with pytest.raises(RuntimeError, match="undo"):
        with macros.transaction():
            first = macros.insert_row("annotations", {"annotation_item_id": 7123})
            second = macros.insert_row("annotations", {"annotation_item_id": 7123})
            macros.insert_row("annotations", {"annotation_item_id": 7124})
            rows, complete = macros.get_rows_page(
                "annotations",
                after_id=first,
                limit=1,
                where={"annotation_item_id": 7123, "annotation_kind": None},
            )
            assert [row["annotation_id"] for row in rows] == [second]
            assert complete
            assert (
                macros.get_rows_page("annotations", after_id=first, limit=0)[1] is False
            )
            with pytest.raises(InputIntegrityError):
                macros.get_rows_page("annotations", after_id=first, offset=1)
            with pytest.raises(InputIntegrityError):
                macros.get_rows_page("annotations", after_id=True)
            raise RuntimeError("undo")
    assert macros.get_row("annotations", first) is None


def test_graph_batch_bounds_queries_and_shares_destination_fetches(db, monkeypatch):
    macros = db.macros
    first = macros.insert_row("works", {"work_title": "First batch owner"})
    last = macros.insert_row("works", {"work_title": "Last batch owner"})
    expression = macros.insert_row("expressions", {"expression_label": "Shared"})
    spec = db.driver_wrapper.get_link_spec("works", "expressions")
    for owner in (first, last):
        macros.upsert_link(spec, owner, LinkValue(expression, priority=1))
    requested = [first, *range(900000, 900999), last, first]
    counts = _count_reads(db, monkeypatch)
    groups = macros.get_linked_rows_bulk(spec, requested)
    assert tuple(groups) == tuple(dict.fromkeys(requested))
    assert groups[900000] == ()
    assert groups[first][0].row["expression_id"] == expression
    assert groups[last][0].row == groups[first][0].row
    groups[first][0].row["expression_label"] = "local"
    assert groups[last][0].row["expression_label"] == "Shared"
    assert counts == [
        (spec.link_table, 500),
        (spec.link_table, 500),
        (spec.link_table, 1),
        ("expressions", 1),
    ]
    counts.clear()
    with pytest.raises(InputIntegrityError):
        macros.get_linked_rows_bulk(spec, [first, True])
    assert not counts
    source = DatabaseMetadataReadSource(db)
    groups = source.get_interlinked_rows_bulk("works", [first, last], "expressions")
    assert groups[first] == groups[last]
    assert counts == [(spec.link_table, 2), ("expressions", 1)]


def test_legacy_row_traversal_batches_destinations(db, monkeypatch):
    macros = db.macros
    work = macros.insert_row("works", {"work_title": "Legacy batched traversal"})
    expressions = [
        macros.insert_row("expressions", {"expression_label": str(i)}) for i in range(3)
    ]
    spec = db.driver_wrapper.get_link_spec("works", "expressions")
    for i, expression in enumerate(expressions):
        macros.upsert_link(spec, work, LinkValue(expression, priority=i + 1))
    source = db.get_row_from_id("works", work)
    monkeypatch.setattr(
        db, "get_row_from_id", lambda *a, **k: pytest.fail("scalar endpoint read")
    )
    counts = _count_reads(db, monkeypatch)
    rows = db.get_interlinked_rows(source, "expressions")
    assert [row.row_id for row in rows] == expressions[::-1]
    assert counts == [("expressions", 3)]


def test_query_index_migration_plans_and_is_idempotent(db):
    macros = db.macros
    with macros.transaction() as conn:
        for index in QUERY_INDEXES:
            conn.execute(f'DROP INDEX IF EXISTS "{index.name}"')
    before = macros.audit_query_indexes()
    assert before.missing == tuple(index.name for index in QUERY_INDEXES)
    report = macros.migrate_query_indexes()
    assert report.created == before.missing
    assert report.clean and macros.migrate_query_indexes().created == ()
    with macros.transaction() as conn:
        conn.executemany(
            "INSERT INTO annotations (annotation_item_id, annotation_user_id, annotation_kind) VALUES (?, ?, ?)",
            ((i % 50, i % 7, "note") for i in range(1000)),
        )
        conn.executemany(
            "INSERT INTO backup_workflows (backup_workflow_name, backup_workflow_kind, backup_workflow_output_url, backup_workflow_status) VALUES (?, ?, ?, ?)",
            (
                (
                    f"plan-{i}",
                    "squashfs_pack",
                    f"/tmp/plan-{i}.sqsh",
                    "running" if i % 50 == 0 else "complete",
                )
                for i in range(1000)
            ),
        )
        for sql, bindings, wanted in (
            (
                "SELECT * FROM annotations WHERE annotation_item_id=? ORDER BY annotation_id",
                (4,),
                "idx_annotations_item_id",
            ),
            (
                "SELECT * FROM annotations WHERE annotation_item_id=? AND annotation_user_id=? AND annotation_kind=? ORDER BY annotation_id",
                (4, 2, "note"),
                "idx_annotations_item_user_kind",
            ),
            (
                "SELECT * FROM backup_workflows WHERE backup_workflow_status=? ORDER BY backup_workflow_id",
                ("running",),
                "idx_backup_workflows_status",
            ),
        ):
            plan = " ".join(
                str(row[3])
                for row in conn.execute("EXPLAIN QUERY PLAN " + sql, bindings)
            )
            assert wanted in plan and "SEARCH" in plan


def test_query_index_migration_rejects_conflicts_and_obeys_outer_rollback(db):
    macros = db.macros
    with macros.transaction() as conn:
        for index in QUERY_INDEXES:
            conn.execute(f'DROP INDEX IF EXISTS "{index.name}"')
    with pytest.raises(RuntimeError, match="undo"):
        with macros.transaction():
            assert macros.migrate_query_indexes().clean
            raise RuntimeError("undo")
    assert len(macros.audit_query_indexes().missing) == len(QUERY_INDEXES)
    with macros.transaction() as conn:
        conn.execute(
            "CREATE INDEX idx_annotations_item_user_kind ON annotations (annotation_kind)"
        )
    with pytest.raises(DatabaseIntegrityError, match="Conflicting query indexes"):
        macros.migrate_query_indexes()
    report = macros.audit_query_indexes()
    assert report.conflicts == ("idx_annotations_item_user_kind",)
    assert len(report.missing) == 2


def test_new_schema_declares_the_query_indexes():
    root = (
        Path(__file__).resolve().parents[2]
        / "src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql"
    )
    with sqlite3.connect(":memory:") as conn:
        for path in (
            "metadata_additional/annotations.sql",
            "storage_tables/1b-backup-workflows.sql",
        ):
            conn.executescript((root / path).read_text())
        for index in QUERY_INDEXES:
            assert (
                tuple(
                    row[2] for row in conn.execute(f'PRAGMA index_info("{index.name}")')
                )
                == index.columns
            )


def test_aggregates_filter_nulls_and_observe_current_transaction(db):
    macros = db.macros
    owner = 74125
    with pytest.raises(RuntimeError, match="undo"):
        with macros.transaction():
            for user, kind in (
                (1, "note"),
                (1, "highlight"),
                (2, None),
                (None, "note"),
            ):
                macros.insert_row(
                    "annotations",
                    {
                        "annotation_item_id": owner,
                        "annotation_user_id": user,
                        "annotation_kind": kind,
                    },
                )
            result = macros.aggregate_rows(
                "annotations",
                where={"annotation_item_id": owner},
                sum_columns=["annotation_user_id"],
                counts={
                    "notes": {"annotation_kind": "note"},
                    "anonymous": {"annotation_user_id": None},
                },
            )
            assert result == {
                "count": 4,
                "sums": {"annotation_user_id": 4},
                "counts": {"notes": 2, "anonymous": 1},
            }
            assert macros.has_rows("annotations", where={"annotation_item_id": owner})
            groups = macros.get_grouped_counts_for_ids(
                "annotations",
                "annotation_item_id",
                [owner, owner + 1],
                group_by=["annotation_kind"],
                where={"annotation_user_id": 1},
            )
            assert groups[owner + 1] == ()
            assert {
                row["annotation_kind"]: row["row_count"] for row in groups[owner]
            } == {
                "note": 1,
                "highlight": 1,
            }
            raise RuntimeError("undo")
    assert not macros.has_rows("annotations", where={"annotation_item_id": owner})
    assert macros.aggregate_rows(
        "annotations",
        where={"annotation_item_id": owner},
        sum_columns=["annotation_user_id"],
    ) == {
        "count": 0,
        "sums": {"annotation_user_id": None},
        "counts": {},
    }


def test_grouped_counts_bound_owner_bindings_including_filters(db, monkeypatch):
    db.macros.insert_rows(
        "annotations",
        [
            {
                "annotation_item_id": owner,
                "annotation_user_id": user,
                "annotation_kind": "note",
            }
            for owner, user in ((10000, 1), (10000, 1), (11000, 1), (10000, 2))
        ],
    )
    reads = _count_reads(db, monkeypatch)
    groups = db.macros.get_grouped_counts_for_ids(
        "annotations",
        "annotation_item_id",
        range(10000, 11001),
        group_by=["annotation_kind"],
        where={"annotation_user_id": 1},
    )
    assert reads == [("annotations", 500), ("annotations", 500), ("annotations", 4)]
    assert groups[10000][0]["row_count"] == 2
    assert groups[11000][0]["row_count"] == 1
    assert groups[10001] == ()


def test_grouped_replica_counts_preserve_core_policy_labels(db, monkeypatch):
    macros = db.macros
    asset = macros.insert_row(
        "digital_assets", {"digital_asset_name": "Aggregate policy"}
    )
    labels = [
        (None, None, "active"),
        ("ONLINE", None, "backup"),
        ("OFFLINE", None, "active"),
        (None, "bad", "active"),
        (None, "unknown", "archive"),
        (" offline ", None, "active"),
    ]
    with macros.transaction():
        for _ in range(4):
            for presence, integrity, mode in labels:
                macros.insert_row(
                    "asset_replicas",
                    {
                        "asset_replica_digital_asset_id": asset,
                        "asset_replica_presence_status": presence,
                        "asset_replica_integrity_status": integrity,
                        "asset_replica_mode": mode,
                    },
                )
    asset_row = macros.get_row("digital_assets", asset)
    rows = macros.get_rows(
        "asset_replicas", where={"asset_replica_digital_asset_id": asset}
    )
    expected = CoreStorageGraphAPI._assess_rows(asset_row, rows, None, None)
    groups = db.metadata_sql.count_asset_replicas_for_assets([asset, 9876543])
    assert groups[9876543] == () and len(groups[asset]) == len(labels)
    assert (
        CoreStorageGraphAPI._assess_rows(
            asset_row, groups[asset], None, None, grouped=True
        )
        == expected
    )
    monkeypatch.setattr(
        macros, "get_rows", lambda *a, **k: pytest.fail("Replica payload read")
    )
    assert CoreStorageGraphAPI._assess(SimpleNamespace(database=db), asset) == expected


def test_link_counts_distinguish_roles_and_prefetch_work_page(db, monkeypatch):
    macros = db.macros
    catalog = Catalog(db)
    works = [catalog.works.create({"title": f"Counted work {i}"}) for i in range(3)]
    agent = catalog.agents.create_person({"name": "Shared contributor"})
    spec = db.driver_wrapper.get_link_spec("agents", "works")
    macros.upsert_link(spec, agent, LinkValue(works[0], link_type="aut", priority=1))
    macros.upsert_link(spec, agent, LinkValue(works[0], link_type="edt", priority=1))
    macros.upsert_link(spec, agent, LinkValue(works[1], link_type="aut", priority=2))
    counts = db.metadata_sql.count_works_linked_from("agents", [agent, 999999])
    assert counts == {
        agent: {"links": 3, "distinct": 2, "missing": 0},
        999999: {"links": 0, "distinct": 0, "missing": 0},
    }
    assert (
        db.metadata_sql.count_works_linked_from("agents", [agent], link_type="aut")[
            agent
        ]["links"]
        == 2
    )
    source = DatabaseMetadataReadSource(db)
    assert source.get_interlinked_counts_bulk("agents", [agent], "works") == {agent: 3}
    # A page's Work->Agent links and unique Agent payloads are each read once.
    runtime = SimpleNamespace(database=db, services=SimpleNamespace(read_source=source))
    rows = [catalog.works.require(work) for work in works]
    queries = _count_reads(db, monkeypatch)
    summaries = _work_summaries(runtime, rows)
    assert [len(row["authors"]) for row in summaries] == [2, 1, 0]
    assert [table for table, _ in queries].count("agents") == 1
    assert [table for table, _ in queries].count(spec.link_table) == 1


def test_bulk_inserts_normalize_bound_statements_and_rollback_late_conflicts(
    db, monkeypatch
):
    macros = db.macros
    statements = []
    original = macros._macro_transaction

    class Connection:
        def __init__(self, conn):
            self.conn = conn

        def execute(self, sql, bindings=()):
            if sql.startswith('INSERT INTO "tags"'):
                statements.append(len(bindings))
            return self.conn.execute(sql, bindings)

        def __getattr__(self, name):
            return getattr(self.conn, name)

    @contextmanager
    def transaction(*args, **kwargs):
        with original(*args, **kwargs) as conn:
            yield Connection(conn)

    monkeypatch.setattr(macros, "_macro_transaction", transaction)
    macros.insert_rows("tags", ({"tag": f"Efficient tag {i}"} for i in range(1001)))
    assert len(statements) == 5 and max(statements) <= 500
    assert (
        macros.find_table_values("tags", "tag", [" EFFICIENT   TAG 1000 "])[
            " EFFICIENT   TAG 1000 "
        ]
        is not None
    )
    before = macros.aggregate_rows("tags")["count"]
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
        macros.insert_rows(
            "tags",
            [
                *({"tag": f"Undo batch {i}"} for i in range(600)),
                {"tag": "efficient tag 0"},
            ],
        )
    assert macros.aggregate_rows("tags")["count"] == before
    assert (
        macros.find_table_values("tags", "tag", ["Undo batch 0"])["Undo batch 0"]
        is None
    )
    with pytest.raises(InputIntegrityError):
        macros.delete_rows_where("tags", where={})
    macros.delete_rows_where("tags", where={"tag": "Efficient tag 0"})
    assert macros.aggregate_rows("tags")["count"] == before - 1


def test_staged_link_replacement_keeps_survivor_ids_extras_and_rolls_back(
    db, monkeypatch
):
    macros = db.macros
    work = macros.insert_row("works", {"work_title": "Staged relationships"})
    expressions = [
        macros.insert_row("expressions", {"expression_label": f"Stage {i}"})
        for i in range(4)
    ]
    spec = db.driver_wrapper.get_link_spec("works", "expressions")
    source_column = next(
        c.name for c in spec.extra_link_columns if c.name.endswith("_source")
    )
    before = macros.replace_links(
        spec,
        work,
        [
            LinkValue(value, extra={source_column: "original"})
            for value in expressions[:3]
        ],
    )
    link_id = db.driver_wrapper.get_id_column(spec.link_table)
    old_ids = {row.secondary_id: row.extra[link_id] for row in before}
    reads = _count_reads(db, monkeypatch)
    after = macros.replace_links_bulk(
        spec,
        {
            work: [
                LinkValue(expressions[2]),
                LinkValue(expressions[0]),
                LinkValue(expressions[3]),
            ],
            **{owner: [] for owner in range(900000, 901001)},
        },
    )[work]
    assert [row.secondary_id for row in after] == [
        expressions[2],
        expressions[0],
        expressions[3],
    ]
    assert [row.priority for row in after] == [3, 2, 1]
    for row in after[:2]:
        assert row.extra[link_id] == old_ids[row.secondary_id]
        assert row.extra[source_column] == "original"
    assert len(reads) == 6 and max(size for _, size in reads) <= 500
    with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
        macros.replace_links_bulk(
            spec,
            {work: [LinkValue(expressions[1])], 99999999: [LinkValue(expressions[0])]},
        )
    assert macros.get_link_rows(spec, work) == after
    assert not tuple(
        macros._macro_connection().execute(
            "SELECT name FROM sqlite_temp_master WHERE name LIKE 'liuxin_links_%'"
        )
    )


def test_link_upserts_batch_inserts_and_reads_in_input_order(db, monkeypatch):
    macros = db.macros
    work = macros.insert_row("works", {"work_title": "Upsert batch"})
    destinations = list(range(910000, 911001))
    macros.insert_rows(
        "expressions",
        [
            {"expression_id": value, "expression_label": f"Batched {value}"}
            for value in destinations
        ],
    )
    spec = db.driver_wrapper.get_link_spec("works", "expressions")
    reads = _count_reads(db, monkeypatch)
    rows = macros.upsert_links(
        spec,
        work,
        [LinkValue(value, priority=i + 1) for i, value in enumerate(destinations)],
    )
    assert [row.secondary_id for row in rows] == destinations
    assert reads == [(spec.link_table, 1), (spec.link_table, 1)]
    reads.clear()
    revised = macros.upsert_links(
        spec, work, [LinkValue(value) for value in reversed(destinations)]
    )
    assert revised == rows[::-1]
    assert reads == [(spec.link_table, 1), (spec.link_table, 1)]
    extra = macros.insert_row(
        "expressions", {"expression_label": "Conflicting priority"}
    )
    with pytest.raises(DatabaseIntegrityError, match="Could not upsert"):
        macros.upsert_links(spec, work, [LinkValue(extra, priority=1)])
    assert macros.get_link_rows(spec, work) == rows[::-1]


def test_backup_source_batch_preserves_previous_sources_after_insert_failure(db):
    macros = db.macros
    workflow = macros.insert_row(
        "backup_workflows",
        {
            "backup_workflow_name": "Atomic source batch",
            "backup_workflow_kind": "squashfs_pack",
            "backup_workflow_output_url": "/tmp/example.sqsh",
        },
    )

    def source(ordinal, path):
        return {
            "backup_workflow_source_ordinal": ordinal,
            "backup_workflow_source_kind": "local_path",
            "backup_workflow_source_identifier": path,
            "backup_workflow_source_archive_path": path,
        }

    replace = db.metadata_sql.replace_backup_workflow_sources
    replace(workflow, [source(0, "original.epub")])
    before = db.metadata_sql.get_backup_workflow_sources_for_workflows([workflow])
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
        replace(
            workflow,
            [source(i, f"file-{i}.epub") for i in range(600)]
            + [source(0, "collision.epub")],
        )
    assert (
        db.metadata_sql.get_backup_workflow_sources_for_workflows([workflow]) == before
    )
