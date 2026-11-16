"""DBR transaction, preference, pruning and physical-index contracts on PostgreSQL."""

from concurrent.futures import ThreadPoolExecutor, TimeoutError
from contextlib import closing

import pytest

from LiuXin_alpha.databases.dbprefs import DBPrefs
from LiuXin_alpha.databases.schema_specs import StorageLinkSpec
from LiuXin_alpha.databases.transactions import transaction_scope
from tests.databases.test_transaction_ownership_postgres_live import (
    pg_database as pg_database,
)

psycopg2 = pytest.importorskip("psycopg2")


def test_owned_scope_cannot_commit_existing_postgres_work(pg_database):
    db, _, _ = pg_database
    connection = db.driver.conn
    connection.execute("INSERT INTO probe VALUES ('caller')").close()
    with pytest.raises(RuntimeError, match="idle connection"):
        with transaction_scope(connection, owns_transaction=True):
            pytest.fail("owned BEGIN accepted active PostgreSQL work")
    assert connection.execute("SELECT value FROM probe").fetchall() == [("caller",)]
    connection.rollback()
    assert connection.execute("SELECT value FROM probe").fetchall() == []


@pytest.mark.parametrize("id_only", [True, False])
def test_postgres_identified_update_requires_existing_target(pg_database, id_only):
    db, _, _ = pg_database
    payload = {"work_id": 424242, "table": "works"}
    if not id_only:
        payload["work_title"] = "Missing"
    with pytest.raises(Exception, match="No row"):
        db.driver_wrapper.update_row(payload)


def test_postgres_preferences_unique_and_transactional(pg_database):
    db, _, _ = pg_database
    prefs = DBPrefs(db)
    prefs["binary"] = b"007"
    assert DBPrefs(db)["binary"] == b"007"
    with pytest.raises(RuntimeError, match="abort"):
        with db.macros.transaction():
            del prefs["binary"]
            raise RuntimeError("abort")
    assert prefs["binary"] == DBPrefs(db)["binary"] == b"007"
    with closing(db.driver.get_connection()) as connection:
        with pytest.raises(psycopg2.IntegrityError):
            connection.execute(
                "INSERT INTO preferences(preference_key,preference_value) VALUES ('binary','null')"
            )
        connection.rollback()


@pytest.mark.parametrize("kind", ["descending", "collation", "operator_class"])
def test_postgres_index_audit_rejects_changed_key_semantics(pg_database, kind):
    db, _, _ = pg_database
    assert db.macros.audit_query_indexes().clean
    keys = {
        "descending": "annotation_item_id DESC, annotation_user_id, annotation_kind, annotation_id",
        "collation": 'annotation_item_id, annotation_user_id, annotation_kind COLLATE "C", annotation_id',
        "operator_class": "annotation_item_id, annotation_user_id, annotation_kind text_pattern_ops, annotation_id",
    }[kind]
    with closing(db.driver.get_connection()) as connection:
        connection.execute("DROP INDEX idx_annotations_item_user_kind").close()
        connection.execute(
            f"CREATE INDEX idx_annotations_item_user_kind ON annotations({keys})"
        ).close()
        connection.commit()
    assert "idx_annotations_item_user_kind" in db.macros.audit_query_indexes().conflicts


def test_postgres_pruning_protects_omitted_physical_reference(pg_database):
    db, _, _ = pg_database
    tag = db.macros.ensure_table_value("tags", "tag", "Item-only")
    with closing(db.driver.get_connection()) as connection:
        connection.execute("INSERT INTO items(item_id) VALUES (5001)").close()
        connection.execute(
            "INSERT INTO item_tag_links(item_tag_link_item_id,item_tag_link_tag_id) VALUES (?,?)",
            (5001, tag),
        ).close()
        connection.commit()
    spec = StorageLinkSpec(
        "works",
        "tags",
        "tag_work_links",
        primary_id_col="work_id",
        secondary_id_col="tag_id",
        primary_link_col="tag_work_link_work_id",
        secondary_link_col="tag_work_link_tag_id",
    )
    assert db.macros.delete_unreferenced_rows("tags", (spec,)) == ()
    assert db.macros.get_row("tags", tag)["tag_id"] == tag


def test_postgres_pruning_waits_for_inflight_reference_insert(pg_database):
    """A link committed during pruning's lock acquisition must protect its endpoint."""
    db, _, _ = pg_database
    tag = db.macros.ensure_table_value("tags", "tag", "Concurrent link")
    spec = StorageLinkSpec(
        "works",
        "tags",
        "tag_work_links",
        primary_id_col="work_id",
        secondary_id_col="tag_id",
        primary_link_col="tag_work_link_work_id",
        secondary_link_col="tag_work_link_tag_id",
    )
    with closing(db.driver.get_connection()) as writer:
        writer.execute("INSERT INTO items(item_id) VALUES (5001)").close()
        writer.execute(
            "INSERT INTO item_tag_links(item_tag_link_item_id,item_tag_link_tag_id) VALUES (?,?)",
            (5001, tag),
        ).close()
        with ThreadPoolExecutor(max_workers=1) as workers:
            pending = workers.submit(
                db.macros.delete_unreferenced_rows, "tags", (spec,)
            )
            try:
                with pytest.raises(TimeoutError):
                    pending.result(timeout=0.3)
            finally:
                writer.commit()
            assert pending.result(timeout=20) == ()
    assert db.macros.get_row("tags", tag)["tag_id"] == tag
