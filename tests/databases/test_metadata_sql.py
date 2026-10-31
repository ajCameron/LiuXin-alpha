"""Verify physical-table SQL helpers, rollback and database lifecycle."""

import inspect

import pytest

from LiuXin_alpha.databases.api import MetadataSQLAPI
from LiuXin_alpha.databases.metadata_sql import MetadataSQL


def test_file_updates_use_current_columns_and_rollback(db, tmp_path):
    physical = tmp_path / "original.epub"
    physical.write_bytes(b"source bytes")
    file_id = db.macros.insert_row("files", {"file_name": physical.name})
    sql = db.metadata_sql
    sql.set_file_name(file_id, "first.epub")
    sql.set_file_size(file_id, 12)
    row = db.macros.get_row("files", file_id)
    assert (row["file_name"], row["file_size_bytes"]) == ("first.epub", 12)
    sql.set_file_size_and_name(file_id, 99, "second.epub")
    with pytest.raises(RuntimeError, match="rollback"):
        with db.macros.transaction():
            sql.set_file_name(file_id, "pending.epub")
            sql.set_file_size(file_id, 111)
            sql.set_file_size_and_name(file_id, 222, "pending-again.epub")
            sql.delete_file_by_id(file_id)
            assert db.macros.get_row("files", file_id) is None
            raise RuntimeError("rollback")
    row = db.macros.get_row("files", file_id)
    assert (row["file_name"], row["file_size_bytes"]) == ("second.epub", 99)
    sql.delete_file_by_id(file_id)
    assert db.macros.get_row("files", file_id) is None
    assert physical.read_bytes() == b"source bytes"


@pytest.mark.parametrize("table", ["files", "feeds"])
@pytest.mark.parametrize("shape", ["integers", "bindings", "generator"])
def test_table_batch_deletion_accepts_both_shapes_and_rolls_back(db, table, shape):
    column = "file_name" if table == "files" else "feed_title"
    ids = [
        db.macros.insert_row(table, {column: name}) for name in ("one", "two", "keep")
    ]
    delete = (
        db.metadata_sql.delete_files_by_id
        if table == "files"
        else db.metadata_sql.delete_feed
    )

    def values():
        if shape == "bindings":
            return [(value,) for value in ids[:2]]
        if shape == "generator":
            return (value for value in ids[:2])
        return ids[:2]

    with pytest.raises(RuntimeError, match="rollback"):
        with db.macros.transaction():
            delete(values())
            assert all(db.macros.get_row(table, value) is None for value in ids[:2])
            raise RuntimeError("rollback")
    assert all(db.macros.get_row(table, value) is not None for value in ids)
    delete(values())
    assert all(db.macros.get_row(table, value) is None for value in ids[:2])
    assert db.macros.get_row(table, ids[2]) is not None


@pytest.mark.parametrize("table", ["files", "feeds"])
def test_invalid_batch_binding_does_not_partly_delete(db, table):
    column = "file_name" if table == "files" else "feed_title"
    row_id = db.macros.insert_row(table, {column: "keep"})
    delete = (
        db.metadata_sql.delete_files_by_id
        if table == "files"
        else db.metadata_sql.delete_feed
    )
    with pytest.raises(ValueError, match="binding width"):
        delete([row_id, (123, 456)])
    assert db.macros.get_row(table, row_id) is not None


def test_feed_crud_and_enclosing_rollback(db):
    sql = db.metadata_sql
    assert sql.add_feed("Today's news", "stored recipe") is None
    row = db.macros.get_rows("feeds")[0]
    feed_id = row["feed_id"]
    sql.update_feed(feed_id, "new recipe", "New title")
    before = db.macros.get_rows("feeds")
    assert before[0]["feed_title"] == "New title"
    assert before[0]["feed_script"] == "new recipe"
    with pytest.raises(RuntimeError, match="rollback"):
        with db.macros.transaction():
            sql.add_feed("Pending", "pending recipe")
            sql.update_feed(feed_id, "pending recipe", "Pending title")
            sql.delete_feed(feed_id)
            raise RuntimeError("rollback")
    assert db.macros.get_rows("feeds") == before
    sql.delete_feed(feed_id)
    assert not db.macros.get_rows("feeds")


def test_feed_replacement_rolls_back_partial_insertion_and_outer_transaction(
    db, monkeypatch
):
    sql = db.metadata_sql
    sql.add_feed("Keep", "keep recipe")
    before = db.macros.get_rows("feeds")
    original_insert = db.macros.insert_row

    def fail_second(table, values, **kwargs):
        if table == "feeds" and values["feed_title"] == "Fail":
            raise RuntimeError("insertion failed")
        return original_insert(table, values, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(db.macros, "insert_row", fail_second)
        with pytest.raises(RuntimeError, match="insertion failed"):
            sql.set_feeds([("First", "first recipe"), ("Fail", "second recipe")])
    assert db.macros.get_rows("feeds") == before
    with pytest.raises(RuntimeError, match="rollback"):
        with db.macros.transaction():
            sql.set_feeds([("New", "new recipe")])
            assert db.macros.get_rows("feeds")[0]["feed_title"] == "New"
            raise RuntimeError("rollback")
    assert db.macros.get_rows("feeds") == before
    sql.set_feeds([("New", "new recipe")])
    assert [
        (r["feed_title"], r["feed_script"]) for r in db.macros.get_rows("feeds")
    ] == [("New", "new recipe")]
    sql.set_feeds([])
    assert not db.macros.get_rows("feeds")


def test_feed_replacement_materializes_input_before_deleting(db):
    db.metadata_sql.add_feed("Keep", "recipe")
    before = db.macros.get_rows("feeds")

    def broken_input():
        yield "First", "recipe"
        raise RuntimeError("input failed")

    with pytest.raises(RuntimeError, match="input failed"):
        db.metadata_sql.set_feeds(broken_input())
    assert db.macros.get_rows("feeds") == before


def test_tag_and_series_lookup_share_canonical_identity_and_tag_delete_rolls_back(db):
    sql = db.metadata_sql
    tag_id = sql.add_tag("Café History")
    assert sql.get_tag_id_from_value("  CAFÉ   HISTORY  ") == tag_id
    assert sql.add_tag("CAFE\u0301 HISTORY") == tag_id
    with pytest.raises(RuntimeError, match="rollback"):
        with db.macros.transaction():
            sql.delete_tag_by_value("café history")
            assert sql.get_tag_id_from_value("Café History") is None
            raise RuntimeError("rollback")
    assert sql.get_tag_id_from_value("Café History") == tag_id
    sql.delete_tag_by_value("café history")
    sql.delete_tag_by_value("missing")
    assert sql.get_tag_id_from_value("Café History") is None
    series_id = db.macros.ensure_table_value("series", "series", "Café History")
    assert sql.get_series_id_from_value("  CAFE\u0301   HISTORY ") == series_id
    assert sql.get_series_id_from_value("missing") is None


def test_helpers_target_physical_tables_and_current_columns(db):
    schema = dict(db.get("SELECT name, type FROM sqlite_master"))
    columns = {
        "files": {"file_id", "file_name", "file_size_bytes"},
        "feeds": {"feed_id", "feed_title", "feed_script"},
        "tags": {"tag_id", "tag"},
        "series": {"series_id", "series", "series_name_norm"},
    }
    for table, expected in columns.items():
        assert schema[table] == "table", table
        actual = {row[1] for row in db.get(f'PRAGMA table_info("{table}")')}
        assert expected <= actual, (table, expected - actual)
    assert isinstance(db.metadata_sql, MetadataSQLAPI)
    assert db.metadata_sql.db is db
    assert not inspect.isabstract(MetadataSQL)
    for name in MetadataSQLAPI.__abstractmethods__:
        assert inspect.signature(getattr(MetadataSQL, name)) == inspect.signature(
            getattr(MetadataSQLAPI, name)
        ), name


def test_metadata_sql_rebinds_and_is_cleared_on_close(db):
    file_id = db.macros.insert_row("files", {"file_name": "before.epub"})
    before = MetadataSQL(None)
    db.set_metadata_sql(before)
    assert before.db is None
    db._initialise_runtime_collaborators(enable_maintenance=False)
    assert before.db is db
    db.set_driver(db.driver)
    assert db.metadata_sql is not before
    assert db.metadata_sql.db is db
    db.metadata_sql.set_file_name(file_id, "after.epub")
    assert db.macros.get_row("files", file_id)["file_name"] == "after.epub"
    db.close()
    assert db.metadata_sql is None
