"""Exercise facade repairs against current generated schemas and durable writes."""

from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError
from tests.databases import test_row_write_safety as fixtures

db = fixtures.db
template = fixtures.template
stored = fixtures.stored


def row(db, table, **values):
    row_id = db.macros.insert_row(table, values)
    return db.get_row_from_id(table, row_id)


def priority_links(db):
    tag = row(db, "tags", tag="priority source")
    first = row(db, "works", work_title="first")
    second = row(db, "works", work_title="second")
    for target, priority in ((first, 10), (second, 20)):
        db.macros.insert_row(
            "tag_work_links",
            {
                "tag_work_link_tag_id": tag.row_id,
                "tag_work_link_work_id": target.row_id,
                "tag_work_link_priority": priority,
            },
        )
    return tag, first, second


def priorities(db):
    return stored(
        db,
        "SELECT tag_work_link_priority FROM tag_work_links ORDER BY tag_work_link_id",
    )


def test_priority_swap_restores_both_values_and_endpoint_order(db):
    source, first, second = priority_links(db)
    db.swap_priorities(source, first, second)
    assert priorities(db) == [(20,), (10,)]
    assert [r.row_id for r in db.get_interlinked_rows(source, "works")] == [
        first.row_id,
        second.row_id,
    ]
    db.swap_priorities(source, first, first)
    assert priorities(db) == [(20,), (10,)]


@pytest.mark.parametrize("failure_at", [2, 3])
def test_priority_swap_failure_restores_both_links(db, monkeypatch, failure_at):
    endpoints = priority_links(db)
    update = db.macros.update_row
    calls = 0

    def fail_late(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == failure_at:
            raise RuntimeError("injected update failure")
        return update(*args, **kwargs)

    monkeypatch.setattr(db.macros, "update_row", fail_late)
    with pytest.raises(RuntimeError, match="injected"):
        db.swap_priorities(*endpoints)
    assert priorities(db) == [(10,), (20,)]


def test_priority_swap_joins_outer_transaction(db):
    endpoints = priority_links(db)
    with pytest.raises(RuntimeError, match="caller rollback"):
        with db.macros.transaction():
            db.swap_priorities(*endpoints)
            assert priorities(db) == [(10,), (20,)]
            assert [
                r["tag_work_link_priority"]
                for r in db.macros.get_rows(
                    "tag_work_links", order_by=("tag_work_link_id",)
                )
            ] == [20, 10]
            raise RuntimeError("caller rollback")
    assert priorities(db) == [(10,), (20,)]


@pytest.mark.parametrize("invalid", ["bad_type", "duplicate"])
def test_rejected_intralink_does_not_allocate_placeholder(db, invalid):
    first = row(db, "works", work_title="first")
    second = row(db, "works", work_title="second")
    if invalid == "duplicate":
        db.intralink_rows(first, second, "inspired_by")
    before = stored(db, "SELECT * FROM work_work_intralinks")
    with pytest.raises(DatabaseIntegrityError) as caught:
        db.intralink_rows(
            first,
            second,
            "inspired_by" if invalid == "duplicate" else "unsupported_type",
        )
    assert isinstance(caught.value.__cause__, sqlite3.IntegrityError)
    assert stored(db, "SELECT * FROM work_work_intralinks") == before


def test_intralink_creation_joins_outer_transaction_and_reads_endpoints(db):
    first = row(db, "works", work_title="first")
    second = row(db, "works", work_title="second")
    with pytest.raises(RuntimeError, match="caller rollback"):
        with db.macros.transaction():
            link = db.intralink_rows(first, second, " INSPIRED_BY ")
            assert link.table == "work_work_intralinks"
            assert link["work_work_intralink_type"] == "inspired_by"
            assert db.get_intralinked_rows(first, None) == [second]
            assert db.get_intralinked_rows(None, second) == [first]
            assert stored(db, "SELECT * FROM work_work_intralinks") == []
            raise RuntimeError("caller rollback")
    assert db.get_intralinked_rows(first, None) == []


@pytest.mark.parametrize("direction", ["outgoing", "incoming"])
def test_intralink_query_rejects_missing_endpoint(db, direction):
    seed = row(db, "works", work_title="seed")
    source_col, target_col = (
        ("primary_id", "secondary_id")
        if direction == "outgoing"
        else ("secondary_id", "primary_id")
    )
    db.macros.insert_row(
        "work_work_intralinks",
        {
            "work_work_intralink_" + source_col: seed.row_id,
            "work_work_intralink_" + target_col: None,
            "work_work_intralink_type": "inspired_by",
        },
    )
    with pytest.raises(DatabaseIntegrityError, match="missing endpoint"):
        db.get_intralinked_rows(
            seed if direction == "outgoing" else None,
            seed if direction == "incoming" else None,
        )


def typed_links(db):
    agent = row(db, "agents", agent_type="person", agent_canonical_name="writer")
    work = row(db, "works", work_title="work")
    other = row(db, "agents", agent_type="person", agent_canonical_name="other")
    for source, role in ((agent, "aut"), (agent, "edt"), (other, "aut")):
        db.macros.insert_row(
            "agent_work_links",
            {
                "agent_work_link_agent_id": source.row_id,
                "agent_work_link_work_id": work.row_id,
                "agent_work_link_type": role,
                "agent_work_link_priority": 1,
            },
        )
    return agent, work, other


@pytest.mark.parametrize("filter_", [None, "aut", "absent"])
def test_unlink_all_handles_typed_duplicates_and_retains_endpoints(db, filter_):
    agent, work, other = typed_links(db)
    db.unlink_all(agent, "works", filter_)
    remaining = db.macros.get_rows(
        "agent_work_links", where={"agent_work_link_agent_id": agent.row_id}
    )
    assert {r["agent_work_link_type"] for r in remaining} == (
        {"edt"} if filter_ == "aut" else {"aut", "edt"} if filter_ else set()
    )
    assert (
        len(
            db.macros.get_rows(
                "agent_work_links", where={"agent_work_link_agent_id": other.row_id}
            )
        )
        == 1
    )
    for endpoint in (agent, work, other):
        assert db.macros.get_row(endpoint.table, endpoint.row_id) is not None


def test_unlink_all_trigger_failure_rolls_back_every_deletion(db):
    agent, _, _ = typed_links(db)
    before = stored(db, "SELECT * FROM agent_work_links")
    with closing(sqlite3.connect(db.driver.database_path)) as conn:
        conn.execute(
            "CREATE TRIGGER fail_unlink BEFORE DELETE ON agent_work_links WHEN OLD.agent_work_link_type = 'edt' BEGIN SELECT RAISE(ABORT, 'protected role'); END"
        )
    with pytest.raises(DatabaseIntegrityError, match="protected role"):
        db.unlink_all(agent, "works")
    assert stored(db, "SELECT * FROM agent_work_links") == before


def tree(db):
    root = row(db, "series", series="root")
    child = row(db, "series", series="child", series_parent_id=root.row_id)
    leaf = row(db, "series", series="leaf", series_parent_id=child.row_id)
    other = row(db, "series", series="unrelated")
    return root, child, leaf, other


def test_delete_tree_removes_complete_subtree_with_set_null_foreign_key(db):
    root, child, leaf, other = tree(db)
    db.delete_tree(child)
    assert {r["series_id"] for r in db.macros.get_rows("series")} == {
        root.row_id,
        other.row_id,
    }
    db.delete_tree(child)  # A stale persisted root is harmless.


@pytest.mark.parametrize("rollback", ["failure", "outer"])
def test_delete_tree_rolls_back_descendants(db, monkeypatch, rollback):
    root, child, leaf, other = tree(db)
    before = stored(db, "SELECT * FROM series")
    delete = db.macros.delete_row

    def fail_parent(table, row_id, **kwargs):
        if row_id == child.row_id:
            raise RuntimeError("injected delete failure")
        return delete(table, row_id, **kwargs)

    if rollback == "failure":
        monkeypatch.setattr(db.macros, "delete_row", fail_parent)
        with pytest.raises(RuntimeError, match="injected"):
            db.delete_tree(root)
    else:
        with pytest.raises(RuntimeError, match="caller rollback"):
            with db.macros.transaction():
                db.delete_tree(root)
                assert [r["series_id"] for r in db.macros.get_rows("series")] == [
                    other.row_id
                ]
                raise RuntimeError("caller rollback")
    assert stored(db, "SELECT * FROM series") == before


@pytest.mark.parametrize(
    "container", [list, iter, lambda values: (value for value in values)]
)
@pytest.mark.parametrize("empty", [False, True])
def test_search_tree_accepts_one_shot_iterables(db, container, empty):
    root, child, leaf, _ = tree(db)
    wanted = [] if empty else [leaf.row_id, child.row_id]
    assert db.search_tree(root, container(wanted)) == set(wanted)


def test_tree_mutations_reject_drafts(db):
    with pytest.raises(InputIntegrityError):
        db.delete_tree(Row(database=db, row_dict={"series": "draft"}))


def test_priority_swap_supports_nonnullable_priorities(db):
    with closing(sqlite3.connect(db.driver.database_path)) as conn:
        conn.executescript("""
            DROP TABLE tag_work_links;
            CREATE TABLE tag_work_links (
                tag_work_link_id INTEGER PRIMARY KEY,
                tag_work_link_tag_id INTEGER REFERENCES tags(tag_id),
                tag_work_link_work_id INTEGER REFERENCES works(work_id),
                tag_work_link_priority INTEGER NOT NULL,
                UNIQUE(tag_work_link_tag_id, tag_work_link_priority),
                UNIQUE(tag_work_link_tag_id, tag_work_link_work_id)
            );
        """)
    db.driver_wrapper.get_tables(force_refresh=True)
    source, first, second = priority_links(db)
    db.swap_priorities(source, first, second)
    assert priorities(db) == [(20,), (10,)]


@pytest.mark.parametrize("action", ["CASCADE", "RESTRICT"])
def test_delete_tree_supports_other_parent_foreign_key_actions(db, action):
    with closing(sqlite3.connect(db.driver.database_path)) as conn:
        conn.execute(
            f"CREATE TABLE review_nodes (review_node_id INTEGER PRIMARY KEY, review_node TEXT, review_node_parent_id INTEGER REFERENCES review_nodes(review_node_id) ON DELETE {action})"
        )
    db.driver_wrapper.get_tables(force_refresh=True)
    root = row(db, "review_nodes", review_node="root")
    child = row(
        db, "review_nodes", review_node="child", review_node_parent_id=root.row_id
    )
    row(db, "review_nodes", review_node="leaf", review_node_parent_id=child.row_id)
    survivor = row(db, "review_nodes", review_node="unrelated")
    db.delete_tree(root)
    assert stored(db, "SELECT review_node_id FROM review_nodes") == [(survivor.row_id,)]


def test_delete_tree_rejects_a_cycle_before_deleting_anything(db):
    with closing(sqlite3.connect(db.driver.database_path)) as conn:
        conn.executescript("""
            CREATE TABLE review_nodes (review_node_id INTEGER PRIMARY KEY, review_node TEXT, review_node_parent_id INTEGER REFERENCES review_nodes(review_node_id));
            INSERT INTO review_nodes VALUES (1, 'first', 2), (2, 'second', 1);
        """)
    db.driver_wrapper.get_tables(force_refresh=True)
    root = db.get_row_from_id("review_nodes", 1)
    with pytest.raises(InputIntegrityError, match="cycle|Cycle|repeated"):
        db.delete_tree(root)
    assert stored(
        db, "SELECT review_node_id FROM review_nodes ORDER BY review_node_id"
    ) == [(1,), (2,)]


def test_unlink_all_joins_outer_transaction(db):
    agent, _, _ = typed_links(db)
    before = stored(db, "SELECT * FROM agent_work_links")
    with pytest.raises(RuntimeError, match="caller rollback"):
        with db.macros.transaction():
            db.unlink_all(agent, "works")
            assert len(db.macros.get_rows("agent_work_links")) == 1
            raise RuntimeError("caller rollback")
    assert stored(db, "SELECT * FROM agent_work_links") == before
