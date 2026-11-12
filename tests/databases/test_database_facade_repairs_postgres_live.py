"""Verify repaired facade operations against a real PostgreSQL catalogue."""

import pytest

from LiuXin_alpha.databases.database_driver_plugins.registry import load_database_driver
from LiuXin_alpha.errors import DatabaseIntegrityError
from tests.databases import test_database_facade_repairs as cases
from tests.databases import test_row_write_safety_postgres_live as fixtures

pg_db = fixtures.pg_db
stored = fixtures.stored


def test_postgres_swap_and_unlink_preserve_endpoint_contracts(pg_db):
    db, _ = pg_db
    endpoints = cases.priority_links(db)
    db.swap_priorities(*endpoints)
    assert stored(
        pg_db, "tag_work_links", "tag_work_link_priority", "tag_work_link_id"
    ) == [(20,), (10,)]
    # PostgreSQL's generated link table has one relationship per endpoint pair,
    # so exercise role filtering across two Works. SQLite covers multi-role pairs.
    agent = cases.row(db, "agents", agent_type="person", agent_canonical_name="writer")
    other = cases.row(db, "agents", agent_type="person", agent_canonical_name="other")
    work = cases.row(db, "works", work_title="authored")
    edited = cases.row(db, "works", work_title="edited")
    for source, target, role, priority in (
        (agent, work, "aut", 1),
        (agent, edited, "edt", 2),
        (other, work, "aut", 1),
    ):
        db.macros.insert_row(
            "agent_work_links",
            {
                "agent_work_link_agent_id": source.row_id,
                "agent_work_link_work_id": target.row_id,
                "agent_work_link_type": role,
                "agent_work_link_priority": priority,
            },
        )
    db.unlink_all(agent, "works", "aut")
    assert [
        r["agent_work_link_type"]
        for r in db.macros.get_rows(
            "agent_work_links", where={"agent_work_link_agent_id": agent.row_id}
        )
    ] == ["edt"]
    db.unlink_all(agent, "works")
    assert len(db.macros.get_rows("agent_work_links")) == 1
    assert db.macros.get_row("works", work.row_id) is not None
    assert db.macros.get_row("agents", other.row_id) is not None


@pytest.mark.parametrize("failure_at", [2, 3])
def test_postgres_swap_failure_rolls_back(pg_db, monkeypatch, failure_at):
    db, _ = pg_db
    endpoints = cases.priority_links(db)
    update, calls = db.macros.update_row, 0

    def fail_late(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == failure_at:
            raise RuntimeError("injected update failure")
        return update(*args, **kwargs)

    monkeypatch.setattr(db.macros, "update_row", fail_late)
    with pytest.raises(RuntimeError, match="injected"):
        db.swap_priorities(*endpoints)
    assert stored(
        pg_db, "tag_work_links", "tag_work_link_priority", "tag_work_link_id"
    ) == [(10,), (20,)]


def test_postgres_intralinks_leave_no_placeholder_and_return_endpoints(pg_db):
    db, _ = pg_db
    first = cases.row(db, "works", work_title="first")
    second = cases.row(db, "works", work_title="second")
    link = db.intralink_rows(first, second, "inspired_by")
    assert link.table == "work_work_intralinks"
    for link_type in ("inspired_by", "unsupported_type"):
        with pytest.raises(DatabaseIntegrityError) as caught:
            db.intralink_rows(first, second, link_type)
        assert caught.value.__cause__.pgcode.startswith("23")
    assert (
        len(
            stored(
                pg_db,
                "work_work_intralinks",
                "work_work_intralink_id",
                "work_work_intralink_id",
            )
        )
        == 1
    )
    assert db.get_intralinked_rows(first, None) == [second]
    assert db.get_intralinked_rows(None, second) == [first]


def test_postgres_subtree_deletion_and_generator_search(pg_db):
    db, _ = pg_db
    root, child, leaf, other = cases.tree(db)
    assert db.search_tree(root, (r.row_id for r in (leaf, child))) == {
        leaf.row_id,
        child.row_id,
    }
    with pytest.raises(RuntimeError, match="caller rollback"):
        with db.macros.transaction():
            db.delete_tree(root)
            assert len(db.macros.get_rows("series")) == 1
            raise RuntimeError("caller rollback")
    assert len(stored(pg_db, "series", "series_id", "series_id")) == 4
    db.delete_tree(root)
    assert stored(pg_db, "series", "series_id", "series_id") == [(other.row_id,)]


@pytest.mark.parametrize("same_driver", [False, True])
def test_postgres_driver_replacement_is_fully_usable(pg_db, same_driver):
    db, _ = pg_db
    old = db.driver
    driver = (
        old if same_driver else load_database_driver("PostgreSQL")(db.metadata, db=None)
    )
    db.set_driver(driver)
    assert db.driver.db is db.driver_wrapper.db is db.macros.db is db
    assert db.driver_wrapper.dirty_records_queue is db.dirty_records_queue
    row = cases.row(db, "works", work_title="after replacement")
    assert stored(pg_db, "works", "work_id, work_title", "work_id") == [
        (row.row_id, "after replacement")
    ]
    with pytest.raises(RuntimeError, match="caller rollback"):
        with db.macros.transaction():
            db.macros.insert_row("works", {"work_title": "pending"})
            raise RuntimeError("caller rollback")
    assert len(stored(pg_db, "works", "work_id", "work_id")) == 1
