"""Check atomic facade helpers, NULL queries and traversal on real catalogues."""

import sqlite3
from contextlib import closing

import pytest

from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
    connect_postgres,
)
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError
from tests.databases import test_database_facade_repairs as fixtures

db = fixtures.db
template = fixtures.template
row = fixtures.row


def stored(db, table):
    """Read durable state through an independent connection on either backend."""
    if db.type == "PostgreSQL":
        connection = connect_postgres(db.metadata, prompt_for_password=False)
        table_sql = f'"{db.driver.schema}"."{table}"'
    else:
        connection = sqlite3.connect(db.driver.database_path)
        table_sql = f'"{table}"'
    with closing(connection) as reader, closing(reader.cursor()) as cursor:
        cursor.execute(f"SELECT * FROM {table_sql} ORDER BY 1")
        return cursor.fetchall()


def ddl(db, sql):
    with db.macros.transaction() as connection:
        connection.execute(sql).close()
    db.driver_wrapper.get_tables(force_refresh=True)


def intralinks(db, direction):
    seed, first, second = [
        row(db, "works", work_title=name) for name in ("seed", "first", "second")
    ]
    for target in (first, second):
        db.intralink_rows(
            *((seed, target) if direction == "outgoing" else (target, seed)),
            "inspired_by",
        )
    db.intralink_rows(first, second, "inspired_by")
    return (seed, None) if direction == "outgoing" else (None, seed)


@pytest.mark.parametrize("direction", ["outgoing", "incoming", "pair"])
def test_unlink_intralinks_joins_caller_and_keeps_other_links(db, direction):
    seeds = intralinks(db, "outgoing" if direction == "pair" else direction)
    if direction == "pair":
        seeds = (seeds[0], db.get_intralinked_rows(*seeds)[0])
    before = stored(db, "work_work_intralinks")
    expected = 2 if direction == "pair" else 1
    with pytest.raises(RuntimeError, match="caller rollback"):
        with db.macros.transaction():
            db.unlinked_intralink(*seeds)
            assert len(db.macros.get_rows("work_work_intralinks")) == expected
            assert stored(db, "work_work_intralinks") == before
            raise RuntimeError("caller rollback")
    assert stored(db, "work_work_intralinks") == before
    db.unlinked_intralink(*seeds)
    assert len(stored(db, "work_work_intralinks")) == expected
    assert len(db.macros.get_rows("works")) == 3


@pytest.mark.parametrize("direction", ["outgoing", "incoming"])
def test_unlink_intralinks_constraint_failure_restores_all_links(db, direction):
    seeds = intralinks(db, direction)
    ddl(
        db,
        "CREATE TABLE review_protected (review_protected_id INTEGER PRIMARY KEY, review_protected_link INTEGER REFERENCES work_work_intralinks(work_work_intralink_id))",
    )
    db.macros.insert_row(
        "review_protected", {"review_protected_id": 1, "review_protected_link": 2}
    )
    before = stored(db, "work_work_intralinks")
    with pytest.raises(DatabaseIntegrityError):
        db.unlinked_intralink(*seeds)
    assert stored(db, "work_work_intralinks") == before


@pytest.mark.parametrize("swap", [False, True])
@pytest.mark.parametrize("failure", ["late", "outer", "success"])
def test_dupe_interlinks_is_atomic_including_priority_swaps(
    db, monkeypatch, swap, failure
):
    source = row(db, "works", work_title="source")
    destination = row(db, "works", work_title="destination")
    for name in ("first", "second"):
        tag = row(db, "tags", tag=name)
        db.interlink_rows(source, tag, priority=10)
    before = stored(db, "tag_work_links")
    original, calls = db.interlink_rows, 0

    def fail_late(*args, **kwargs):
        nonlocal calls
        calls += 1
        result = original(*args, **kwargs)
        if calls == 2:
            raise RuntimeError("injected late failure")
        return result

    def duplicate():
        db.dupe_interlinks(
            source,
            destination,
            swap_priorities=swap,
            restrict_to_tables=iter(["tags"]),
            force_priority=20,
        )

    if failure == "late":
        monkeypatch.setattr(db, "interlink_rows", fail_late)
        with pytest.raises(RuntimeError, match="injected"):
            duplicate()
    elif failure == "outer":
        with pytest.raises(RuntimeError, match="caller rollback"):
            with db.macros.transaction():
                duplicate()
                assert len(db.macros.get_rows("tag_work_links")) == 4
                assert stored(db, "tag_work_links") == before
                raise RuntimeError("caller rollback")
    else:
        duplicate()
        links = db.macros.get_rows("tag_work_links")
        assert len(links) == 4
        assert {
            link["tag_work_link_priority"]
            for link in links
            if link["tag_work_link_work_id"] == destination.row_id
        } == ({10} if swap else {20})
        return
    assert stored(db, "tag_work_links") == before


@pytest.mark.parametrize("failure", ["late", "outer", "success"])
def test_nest_rows_is_atomic_and_preserves_unrelated_staged_values(
    db, monkeypatch, failure
):
    parent, first, second = [
        row(db, "series", series=name) for name in ("parent", "first", "second")
    ]
    first["series"] = "staged only"
    snapshots = [dict(child._require_row_dict()) for child in (first, second)]
    before = stored(db, "series")
    original, calls = db.macros.update_row, 0

    def fail_late(*args, **kwargs):
        nonlocal calls
        calls += 1
        result = original(*args, **kwargs)
        if calls == 2:
            raise RuntimeError("injected late failure")
        return result

    def nest():
        db.nest_rows(parent, (child for child in (first, second)))

    if failure == "late":
        monkeypatch.setattr(db.macros, "update_row", fail_late)
        with pytest.raises(RuntimeError, match="injected"):
            nest()
        assert [child._require_row_dict() for child in (first, second)] == snapshots
    elif failure == "outer":
        with pytest.raises(RuntimeError, match="caller rollback"):
            with db.macros.transaction():
                nest()
                assert {r.row_id for r in db.get_children(parent)} == {
                    first.row_id,
                    second.row_id,
                }
                assert stored(db, "series") == before
                raise RuntimeError("caller rollback")
    else:
        nest()
        assert first["series"] == "staged only"
        assert db.macros.get_row("series", first.row_id)["series"] == "first"
        assert all(
            child["series_parent_id"] == parent.row_id for child in (first, second)
        )
        assert {r.row_id for r in db.get_children(parent)} == {
            first.row_id,
            second.row_id,
        }
        return
    assert stored(db, "series") == before


@pytest.mark.parametrize(
    "invalid", ["self", "ancestor", "wrong_table", "draft", "missing"]
)
def test_nest_rows_rejects_invalid_children_before_any_write(db, invalid):
    root, parent, child, other = fixtures.tree(db)
    bad = {
        "self": parent,
        "ancestor": root,
        "wrong_table": row(db, "works", work_title="wrong"),
        "draft": Row(database=db, row_dict={"series": "draft"}),
        "missing": other,
    }[invalid]
    if invalid == "missing":
        db.delete(other)
    before = stored(db, "series")
    with pytest.raises(InputIntegrityError):
        db.nest_rows(parent, [child, bad])
    assert stored(db, "series") == before


@pytest.mark.parametrize("back_iterate", [False, True])
@pytest.mark.parametrize("self_cycle", [False, True])
def test_tree_collection_rejects_legacy_cycles(db, back_iterate, self_cycle):
    ddl(
        db,
        "CREATE TABLE review_nodes (review_node_id INTEGER PRIMARY KEY, review_node TEXT, review_node_parent_id INTEGER REFERENCES review_nodes(review_node_id))",
    )
    first = row(db, "review_nodes", review_node_id=1, review_node="first")
    second = row(
        db,
        "review_nodes",
        review_node_id=2,
        review_node="second",
        review_node_parent_id=first.row_id,
    )
    db.macros.update_row(
        "review_nodes",
        first.row_id,
        {"review_node_parent_id": first.row_id if self_cycle else second.row_id},
    )
    first = db.get_row_from_id("review_nodes", first.row_id)
    with pytest.raises(InputIntegrityError, match="[Cc]ycle|repeated"):
        db.get_all_tree_rows(first, back_iterate=back_iterate)


def test_tree_collection_respects_subtree_boundary(db):
    root, child, leaf, other = fixtures.tree(db)
    assert db.get_all_tree_rows(child, back_iterate=False) == {child, leaf}
    assert db.get_all_tree_rows(leaf) == {root, child, leaf}


def test_null_search_and_grouping_distinguish_literal_none(db):
    rows = [
        row(db, "works", work_title=title)
        for title in (None, None, "None", "", "ordinary")
    ]
    null_ids = {r.row_id for r in rows[:2]}
    assert {r.row_id for r in db.search("works", "work_title", None)} == null_ids
    assert {
        r["work_id"] for r in db.driver.direct_search_table("works", "work_title", None)
    } == null_ids
    assert db.search("works", "work_title", "None") == [rows[2]]
    chunks = list(db.chunk_iterator("work_title"))
    assert {frozenset(r.row_id for r in chunk) for chunk in chunks} == {
        frozenset(null_ids),
        *(frozenset([r.row_id]) for r in rows[2:]),
    }
    with db.macros.transaction():
        added = row(db, "works", work_title=None)
        assert {
            r.row_id for r in db.search("works", "work_title", None)
        } == null_ids | {added.row_id}


def test_get_row_and_scalar_contracts_are_distinct(db):
    wrapper = db.driver_wrapper
    assert tuple(wrapper.get("SELECT 7, 8", all=False)) == (7, 8)
    assert tuple(wrapper.get("SELECT 7", all=False)) == (7,)
    assert wrapper.get("SELECT 7 WHERE 1=0", all=False) is None
    assert wrapper.fetch_scalar("SELECT 7") == 7
    assert wrapper.fetch_scalar("SELECT NULL") is None
    with pytest.raises(InputIntegrityError):
        wrapper.fetch_scalar("SELECT 7, 8")


@pytest.mark.parametrize("priority", ["highest", "not_set"])
def test_interlink_creation_joins_caller_and_recovers_from_constraint_failure(
    db, priority
):
    tag = row(db, "tags", tag="common")
    works = [row(db, "works", work_title=name) for name in ("first", "second")]
    with pytest.raises(RuntimeError, match="caller rollback"):
        with db.macros.transaction():
            first = db.interlink_rows(tag, works[0], priority=priority)
            with pytest.raises(DatabaseIntegrityError):
                db.interlink_rows(tag, works[0], priority=priority)
            second = db.interlink_rows(tag, works[1], priority=priority)
            first_priority = first["tag_work_link_priority"]
            if first_priority is not None:
                assert second["tag_work_link_priority"] > first_priority
            assert len(db.macros.get_rows("tag_work_links")) == 2
            assert stored(db, "tag_work_links") == []
            raise RuntimeError("caller rollback")
    assert stored(db, "tag_work_links") == []


def test_failed_interlink_readback_cannot_commit_a_relationship(db, monkeypatch):
    tag = row(db, "tags", tag="common")
    work = row(db, "works", work_title="target")
    original = db.macros.get_row

    def fail_readback(table, *args, **kwargs):
        if table == "tag_work_links":
            raise RuntimeError("injected readback failure")
        return original(table, *args, **kwargs)

    monkeypatch.setattr(db.macros, "get_row", fail_readback)
    with pytest.raises(RuntimeError, match="readback failure"):
        db.interlink_rows(tag, work, priority=1)
    assert stored(db, "tag_work_links") == []
