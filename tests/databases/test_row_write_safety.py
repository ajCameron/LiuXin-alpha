"""Exercise database-review failures against disposable SQLite catalogues."""

from __future__ import annotations

import shutil
import sqlite3
from contextlib import closing
from copy import deepcopy

import pytest

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import DatabaseIntegrityError


@pytest.fixture(scope="module", params=["SQLite", "SQLite_apsw"])
def template(request, tmp_path_factory):
    """Build each backend's schema once; every test receives an independent copy."""
    backend = request.param
    if backend == "SQLite_apsw":
        pytest.importorskip("apsw")
    path = tmp_path_factory.mktemp("row-write-template") / "template.sqlite"
    with Database(
        metadata={"database_path": str(path)},
        db_type=backend,
        create=True,
        repair_bootstrap_rows=False,
        enable_maintenance=False,
    ):
        pass
    with closing(sqlite3.connect(path)) as conn:
        conn.executescript(
            "CREATE TABLE review_items (review_item_id INTEGER PRIMARY KEY, "
            "review_item_text TEXT UNIQUE, review_item_other TEXT);"
            "CREATE TABLE review_references (review_reference_id INTEGER PRIMARY KEY, "
            "review_reference_item INTEGER REFERENCES review_items(review_item_id));"
        )
    return backend, path


@pytest.fixture
def db(template, tmp_path):
    """Keep both mutation and independent durability checks local to this test."""
    backend, source = template
    path = tmp_path / "library.sqlite"
    shutil.copyfile(source, path)
    with Database(
        metadata={"database_path": str(path)},
        db_type=backend,
        repair_bootstrap_rows=False,
        enable_maintenance=False,
    ) as database:
        yield database


def stored(db, sql, values=()):
    """Read committed results without borrowing the writer's connection."""
    with closing(sqlite3.connect(db.driver.database_path)) as conn:
        return conn.execute(sql, values).fetchall()


def test_bulk_insert_binds_columns_independently_of_mapping_order(db):
    """Different construction orders cannot silently swap stored values."""
    rows = [
        {"review_item_text": "first", "review_item_other": "one"},
        {"review_item_other": "two", "review_item_text": "second"},
    ]
    original = deepcopy(rows)
    db.driver_wrapper.add_multiple_rows(rows)
    assert stored(
        db,
        "SELECT review_item_text, review_item_other FROM review_items "
        "ORDER BY review_item_id",
    ) == [("first", "one"), ("second", "two")]
    assert rows == original


def test_failed_bulk_insert_rolls_back_earlier_rows(db):
    """A late uniqueness violation cannot publish the successful batch prefix."""
    with pytest.raises(DatabaseIntegrityError):
        db.driver_wrapper.add_multiple_rows(
            [
                {"review_item_text": "duplicate", "review_item_other": "first"},
                {"review_item_text": "duplicate", "review_item_other": "second"},
            ]
        )
    assert stored(db, "SELECT * FROM review_items") == []


def test_failed_bulk_update_rolls_back_earlier_rows(db):
    """A later uniqueness violation restores earlier updates in the same call."""
    db.driver_wrapper.add_multiple_rows(
        [
            {"review_item_text": "first"},
            {"review_item_text": "second"},
        ]
    )
    original = stored(db, "SELECT * FROM review_items ORDER BY review_item_id")
    with pytest.raises(DatabaseIntegrityError):
        db.driver_wrapper.update_columns(
            {row[0]: "duplicate" for row in original}, field="review_item_text"
        )
    assert stored(db, "SELECT * FROM review_items ORDER BY review_item_id") == original


@pytest.mark.parametrize("route", ["ids", "values", "wrapper_ids", "wrapper_values"])
def test_failed_bulk_delete_rolls_back_and_preserves_integrity_error(db, route):
    """A protected second row keeps both rows and the original failure category."""
    db.driver_wrapper.add_multiple_rows(
        [
            {"review_item_text": "first", "review_item_other": "one"},
            {"review_item_text": "second", "review_item_other": "two"},
        ]
    )
    ids = [
        row[0]
        for row in stored(
            db, "SELECT review_item_id FROM review_items ORDER BY review_item_id"
        )
    ]
    db.driver_wrapper.add_row({"review_reference_item": ids[1]})
    routes = {
        "ids": lambda: db.driver.direct_delete_many_by_ids("review_items", ids),
        "values": lambda: db.driver.direct_delete_many(
            "review_items", "review_item_id", ids
        ),
        "wrapper_ids": lambda: db.driver_wrapper.delete_by_id("review_items", ids),
        "wrapper_values": lambda: db.driver_wrapper.delete(
            "review_items", "review_item_id", ids
        ),
    }
    with pytest.raises(DatabaseIntegrityError) as caught:
        routes[route]()
    assert isinstance(caught.value.__cause__, sqlite3.IntegrityError)
    assert stored(
        db, "SELECT review_item_id FROM review_items ORDER BY review_item_id"
    ) == [(i,) for i in ids]


@pytest.mark.parametrize("fails", [False, True])
def test_single_delete_closes_its_connection_on_success_and_failure(
    db, monkeypatch, fails
):
    """No acquired handle remains usable until eventual facade shutdown."""
    row_id = db.driver_wrapper.add_row({"review_item_text": "target"})
    if fails:
        db.driver_wrapper.add_row({"review_reference_item": row_id})
    opened = []
    acquire = db.driver.get_connection

    def record_connection():
        connection = acquire()
        opened.append(connection)
        return connection

    monkeypatch.setattr(db.driver, "get_connection", record_connection)
    if fails:
        with pytest.raises(DatabaseIntegrityError):
            db.driver_wrapper.delete_by_id("review_items", row_id)
    else:
        for _ in range(25):
            db.driver_wrapper.delete_by_id("review_items", row_id)
    assert opened
    for connection in opened:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            connection.execute("SELECT 1")


def test_unchanged_row_sync_preserves_literal_none(db):
    """SQL NULL and the ordinary text 'None' remain distinct after a row round trip."""
    row_id = db.macros.insert_row("works", {"work_title": "None"})
    row = db.get_row_from_id("works", row_id)
    row.sync()
    assert stored(db, "SELECT work_title FROM works WHERE work_id=?", (row_id,)) == [
        ("None",)
    ]
    row["work_title"] = None
    row.sync()
    assert stored(db, "SELECT work_title FROM works WHERE work_id=?", (row_id,)) == [
        (None,)
    ]


@pytest.mark.parametrize("route", ["row_dict", "columns"])
def test_identity_only_updates_rederive_from_stored_display_and_keep_uniqueness(
    db, route
):
    """Omitting the display field cannot clear its key or open a uniqueness hole."""
    row_id = db.macros.insert_row("tags", {"tag": "Space Opera"})
    if route == "row_dict":
        db.driver_wrapper.update_row({"tag_id": row_id, "tag_phash": "wrong"})
    else:
        db.driver_wrapper.update_columns({row_id: "wrong"}, field="tag_phash")
    assert stored(db, "SELECT tag, tag_phash FROM tags WHERE tag_id=?", (row_id,)) == [
        ("Space Opera", "space opera")
    ]
    with pytest.raises(sqlite3.IntegrityError):
        db.macros.insert_row("tags", {"tag": "SPACE OPERA"})


def test_unsaved_rows_have_object_identity_and_cannot_be_hashed(db):
    """Distinct drafts never compare equal merely because both IDs are absent."""
    first = Row(db, {"work_title": "First draft"})
    second = Row(db, {"work_title": "Second draft"})
    assert first == first
    assert first != second
    with pytest.raises(TypeError, match="unsaved"):
        hash(first)
    with pytest.raises(TypeError, match="unsaved"):
        hash(Row(db))


def test_row_equality_compares_identity_instead_of_hashes(db):
    """Python's colliding -1/-2 integer hashes cannot conflate different row IDs."""
    first = Row(db, {"work_id": -1, "work_title": "First"})
    second = Row(db, {"work_id": -2, "work_title": "Second"})
    assert hash(first) == hash(second)
    assert first != second
    assert first != hash(first)
    assert first != []
    duplicate = Row(db, {"work_id": -1, "work_title": "Different local text"})
    assert first == duplicate
    assert {first: "same identity"}[duplicate] == "same identity"


def test_row_string_is_unicode_text(db):
    """Printing a row is valid Python 3 and preserves non-ASCII values."""
    row = Row(db, {"work_title": "雪の本"})
    assert isinstance(str(row), str)
    assert "雪の本" in str(row)
