"""Exercise custom-column lifecycle and values on generated FRBR storage."""

import json
from contextlib import closing
from copy import deepcopy
from datetime import datetime

import pytest

from LiuXin_alpha.catalog.field_metadata import CalibreFieldMetadata, FieldMetadata
from LiuXin_alpha.utils.calibre_compat.custom_columns import (
    CustomColumns,
    CustomColumnsManager,
)
from tests.databases.test_custom_value_transactions import inject_commit_failure
from tests.databases.test_row_write_safety import db as db
from tests.databases.test_row_write_safety import stored
from tests.databases.test_row_write_safety import template as template


class Cache:
    def __init__(self, owners, *, series=False):
        self._data = {
            owner: [owner, None, None] if series else [owner, None] for owner in owners
        }

    def __getitem__(self, position):
        return list(self._data.values())[position]

    def set(self, row, col, val, row_is_id=False):
        assert row_is_id
        self._data[row][col] = val


def service(
    db, *, datatype="text", multiple=False, display=None, cached=False, table="works"
):
    title = {"works": "work_title", "manifestations": "manifestation_subtitle"}[table]
    owners = [
        db.macros.insert_row(table, {title: name}) for name in ("First", "Second")
    ]
    facade = CustomColumns(db, table=table)
    number = facade.create_custom_column(
        "Review",
        label="review",
        datatype=datatype,
        is_multiple=multiple,
        display=display,
    )
    if cached:
        facade.data = Cache(owners, series=datatype == "series")
        facade.FIELD_MAP = {"id": 0, number: 1}
        if datatype == "series":
            facade.FIELD_MAP[str(number) + "_index"] = 2
    return facade, number, owners


@pytest.mark.parametrize("datatype,value", [("text", "Shared"), ("int", 42)])
@pytest.mark.parametrize("route", ["primary", "macro", "wrapper", "connection"])
def test_parent_delete_cleans_custom_storage_on_every_connection(
    db, datatype, value, route
):
    facade, number, owners = service(db, datatype=datatype)
    for owner in owners:
        facade.set_custom(owner, value, num=number)
    table, link = facade.custom_table_names(number, in_table="works")
    storage = link if datatype == "text" else table
    assert (
        facade.conn.execute(
            "SELECT name FROM sqlite_temp_master WHERE type='trigger' AND name LIKE 'custom_%'"
        ).fetchall()
        == []
    )
    assert stored(
        db,
        "SELECT name FROM sqlite_master WHERE type='trigger' AND name=?",
        ("custom_owner_delete_" + storage,),
    )
    if route == "primary":
        facade.conn.execute("DELETE FROM works WHERE work_id=?", (owners[0],))
        facade.conn.commit()
    elif route == "macro":
        db.macros.delete_rows_by_id("works", [owners[0]])
    elif route == "wrapper":
        db.delete(db.get_row_from_id("works", owners[0]))
    else:
        with closing(db.driver.get_connection()) as connection:
            connection.execute("DELETE FROM works WHERE work_id=?", (owners[0],))
            connection.commit()
    assert stored(db, f"SELECT {storage}_book FROM {storage}") == [(owners[1],)]
    assert facade.get_custom(owners[1], num=number, index_is_id=True) == value


@pytest.mark.parametrize("active", [False, True])
def test_loading_storage_leaves_repairs_explicit_without_committing_caller(db, active):
    facade, number, owners = service(db)
    _, link = facade.custom_table_names(number, in_table="works")
    connection = db.driver.conn
    connection.execute(f"DROP TRIGGER custom_owner_delete_{link}")
    connection.execute(
        f"CREATE TEMP TRIGGER custom_works_delete_trg AFTER DELETE ON works BEGIN DELETE FROM {link} WHERE work=OLD.work_id; END"
    )
    if active:
        connection.execute("BEGIN")
        connection.execute(
            "INSERT INTO review_items(review_item_text) VALUES ('pending')"
        )
    before = connection.execute(
        "SELECT name, sql FROM sqlite_master ORDER BY name"
    ).fetchall()
    loaded = CustomColumns(db, table="works")
    assert (
        connection.execute(
            "SELECT name, sql FROM sqlite_master ORDER BY name"
        ).fetchall()
        == before
    )
    assert connection.in_transaction is active
    assert stored(db, "SELECT review_item_text FROM review_items") == []
    if active:
        connection.rollback()
        # Loading did not repair or commit the caller's transaction.
        loaded = CustomColumns(db, table="works")
    loaded.native.apply_repair(loaded.native.plan_repair(actions=("cleanup_trigger",)))
    loaded.set_custom(owners[0], "value", num=number)
    db.macros.delete_rows_by_id("works", [owners[0]])
    assert stored(db, f"SELECT * FROM {link}") == []


def test_retirement_keeps_storage_until_explicit_purge_and_hides_fields(db):
    facade, number, owners = service(db)
    facade.set_custom(owners[0], "retire", num=number)
    facade.delete_custom_column(number)
    loaded = CustomColumns(db, table="works", field_metadata=facade.field_metadata)
    assert loaded.custom_column_num_map == {}
    assert "#review" not in loaded.field_metadata
    native = facade.native
    assert native.get_field(number, include_retired=True).retired
    assert stored(
        db,
        "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'custom_owner_delete_%'",
    )
    native.apply_purge(native.plan_purge(number))
    assert (
        stored(
            db,
            "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'custom_owner_delete_%'",
        )
        == []
    )
    db.macros.delete_rows_by_id("works", owners)


def test_series_metadata_rename_moves_descriptors_aliases_and_slots(db):
    facade, number, owners = service(db, datatype="series")
    metadata = facade.field_metadata
    metadata.set_field_record_index("review", 30, prefer_custom=True)
    metadata.set_field_record_index("review_index", 31, prefer_custom=True)
    metadata.add_grouped_search_terms({"review_group": ["#review", "title"]})
    facade.set_custom_column_metadata(
        number, label="renamed", is_editable=False, display={"description": "New"}
    )
    assert facade.field_metadata is metadata
    assert "#review" not in metadata and "#review_index" not in metadata
    assert metadata.custom_label_to_key_map["renamed"] == "#renamed"
    assert metadata.custom_label_to_key_map["renamed_index"] == "#renamed_index"
    assert metadata["#renamed"]["rec_index"] == 30
    assert metadata["#renamed_index"]["rec_index"] == 31
    assert metadata["#renamed"]["is_editable"] is False
    assert metadata["#renamed"]["display"] == {"description": "New"}
    assert metadata.search_term_to_field_key("review_group") == ["#renamed", "title"]


@pytest.mark.parametrize("failure", ["descriptor", "commit"])
def test_failed_definition_edit_preserves_sql_maps_and_descriptors(
    db, monkeypatch, failure
):
    facade, number, owners = service(db, datatype="series")
    other = facade.create_custom_column("Other", label="other")
    before_maps = deepcopy(facade.custom_column_num_map)
    before_fields = deepcopy(facade.field_metadata.copy())
    notices = []
    facade.notify = lambda *args: notices.append(args)
    if failure == "commit":

        def fail():
            raise RuntimeError("commit failed")

        inject_commit_failure(db, monkeypatch, fail)
        label = "changed"
        expected = RuntimeError
    else:
        label = "review_index"
        expected = ValueError
    with pytest.raises(expected):
        facade.set_custom_column_metadata(other, label=label)
    assert facade.custom_column_num_map == before_maps
    assert facade.field_metadata.copy() == before_fields
    assert stored(
        db,
        "SELECT custom_column_label FROM custom_columns WHERE custom_column_id=?",
        (other,),
    ) == [("other",)]
    assert notices == []


@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize(
    "datatype,value,expected",
    [
        ("text", "Name", "Name"),
        ("int", 42, 42),
        ("float", 1.25, 1.25),
        ("bool", True, True),
        ("rating", 4, 4),
        ("comments", "A note", "A note"),
        ("datetime", datetime(2020, 2, 3, 4, 5), datetime(2020, 2, 3, 4, 5)),
        ("series", "Cycle [2.5]", "Cycle"),
        ("enumeration", "Ready", "Ready"),
    ],
)
def test_generated_frbr_value_roundtrip_without_meta2(
    db, datatype, value, expected, cached
):
    facade, number, owners = service(
        db, datatype=datatype, display={"enum_values": ["Ready"]}, cached=cached
    )
    assert stored(db, "SELECT name FROM sqlite_master WHERE name='meta2'") == []
    facade.set_custom(owners[0], value, num=number)
    assert facade.get_custom(owners[0], num=number, index_is_id=True) == expected
    if datatype == "series":
        assert facade.get_custom_extra(owners[0], num=number, index_is_id=True) == 2.5
    facade.set_custom(owners[0], None, num=number)
    assert facade.get_custom(owners[0], num=number, index_is_id=True) is None
    if cached and datatype == "series":
        assert facade.data._data[owners[0]][2] is None


@pytest.mark.parametrize("table", ["works", "manifestations"])
@pytest.mark.parametrize("cached", [False, True])
def test_multiple_values_rename_merge_delete_and_projection_use_attachment(
    db, table, cached
):
    facade, number, owners = service(db, multiple=True, cached=cached, table=table)
    facade.set_custom_bulk(owners, ["First", "Second"], num=number)
    facade.set_custom_bulk_multiple(
        iter(owners), add=["Third"], remove=["Second"], num=number
    )
    assert facade.get_custom(owners[0], num=number, index_is_id=True) == [
        "First",
        "Third",
    ]
    values = {value: idx for idx, value in facade.get_custom_items_with_ids(num=number)}
    facade.rename_custom_item(values["First"], "Third", num=number)
    assert facade.get_custom(owners[0], num=number, index_is_id=True) == ["Third"]
    projection = facade.custom_columns_in_meta()[number]
    assert facade.conn.execute(f"SELECT {projection} FROM {table}").fetchall() == [
        ("Third",),
        ("Third",),
    ]
    assert sorted(facade.delete_item_from_multiple("THIRD", num=number)) == owners
    assert facade.get_custom(owners[0], num=number, index_is_id=True) == []


@pytest.mark.parametrize("merge", [False, True])
@pytest.mark.parametrize("cached", [False, True])
def test_enum_rename_persists_choices_and_survives_reload(db, merge, cached):
    facade, number, owners = service(
        db,
        datatype="enumeration",
        display={"enum_values": ["Old", "New"]},
        cached=cached,
    )
    facade.set_custom(owners[0], "Old", num=number)
    if merge:
        facade.set_custom(owners[1], "New", num=number)
    values = {value: idx for idx, value in facade.get_custom_items_with_ids(num=number)}
    facade.rename_custom_item(values["Old"], "New", num=number)
    persisted = json.loads(
        stored(
            db,
            "SELECT custom_column_display FROM custom_columns WHERE custom_column_id=?",
            (number,),
        )[0][0]
    )
    assert persisted["enum_values"] == ["New"]
    assert facade.field_metadata["#review"]["display"]["enum_values"] == ["New"]
    loaded = CustomColumns(db, table="works")
    loaded.set_custom(owners[1], "New", num=number)
    assert loaded.get_custom(owners[1], num=number, index_is_id=True) == "New"


def test_failed_enum_commit_restores_values_choices_and_cache(db, monkeypatch):
    facade, number, owners = service(
        db, datatype="enumeration", display={"enum_values": ["Old"]}, cached=True
    )
    facade.set_custom(owners[0], "Old", num=number)
    old_id = facade.get_custom_items_with_ids(num=number)[0][0]
    before = deepcopy(facade.data._data)
    allowed = facade.custom_field_metadata(num=number)["display"]["enum_values"]

    def fail():
        raise RuntimeError("commit failed")

    inject_commit_failure(db, monkeypatch, fail)
    with pytest.raises(RuntimeError, match="commit failed"):
        facade.rename_custom_item(old_id, "New", num=number)
    assert allowed == ["Old"]
    assert facade.data._data == before
    assert facade.all_custom(num=number) == {"Old"}
    persisted = json.loads(
        stored(
            db,
            "SELECT custom_column_display FROM custom_columns WHERE custom_column_id=?",
            (number,),
        )[0][0]
    )
    assert persisted["enum_values"] == ["Old"]


@pytest.mark.parametrize("metadata_class", [FieldMetadata, CalibreFieldMetadata])
def test_manager_refresh_removes_deleted_keys_without_touching_other_attachments(
    db, metadata_class
):
    shared = metadata_class()
    manager = CustomColumnsManager(
        db, field_metadata_by_table={"works": shared, "manifestations": shared}
    )
    work = manager.get("works")
    manifestation = manager.get("manifestations")
    first = work.create_custom_column("Work field", label="work_field")
    manifestation.create_custom_column("Edition field", label="edition_field")
    work.set_custom_column_metadata(first, label="renamed_work")
    manager.refresh()
    assert "#work_field" not in shared
    assert "#renamed_work" in shared and "#edition_field" in shared


def test_invalid_field_registration_rolls_back_column_creation(db):
    facade, number, owners = service(db, datatype="series")
    before = stored(db, "SELECT name, sql FROM sqlite_master ORDER BY name")
    before_fields = deepcopy(facade.field_metadata.copy())
    with pytest.raises(ValueError, match="Series companion"):
        facade.create_custom_column("Collision", label="review_index")
    assert stored(db, "SELECT name, sql FROM sqlite_master ORDER BY name") == before
    assert stored(db, "SELECT custom_column_id FROM custom_columns") == [(number,)]
    assert facade.field_metadata.copy() == before_fields


def test_constructor_inside_macro_transaction_does_not_publish_outer_work(db):
    facade, number, owners = service(db)
    with pytest.raises(RuntimeError, match="abort outer"):
        with db.macros.transaction():
            db.macros.insert_row("review_items", {"review_item_text": "pending"})
            loaded = CustomColumns(db, table="works")
            assert number in loaded.custom_column_num_map
            assert stored(db, "SELECT * FROM review_items") == []
            raise RuntimeError("abort outer")
    assert stored(db, "SELECT * FROM review_items") == []


@pytest.mark.parametrize("operation", ["value", "metadata", "creation"])
@pytest.mark.parametrize("connection_kind", ["primary", "macro"])
def test_service_writes_reject_macro_nesting_before_effects(
    db, operation, connection_kind
):
    facade, number, owners = service(db)
    context = (
        db.driver.conn if connection_kind == "primary" else db.macros.transaction()
    )
    with context:
        if connection_kind == "primary":
            db.driver.conn.execute("BEGIN")
        with pytest.raises(RuntimeError, match="own transaction"):
            if operation == "value":
                facade.set_custom(owners[0], "forbidden", num=number)
            elif operation == "metadata":
                facade.set_custom_column_metadata(number, label="forbidden")
            else:
                facade.create_custom_column("Forbidden", label="forbidden")
    assert facade.all_custom(num=number) == set()
    assert facade.custom_field_metadata(num=number)["label"] == "review"
    assert stored(db, "SELECT custom_column_id FROM custom_columns") == [(number,)]


def test_parent_delete_rollback_restores_owner_and_custom_values(db):
    facade, number, owners = service(db)
    facade.set_custom(owners[0], "Keep", num=number)
    with pytest.raises(RuntimeError, match="abort delete"):
        with db.macros.transaction():
            db.macros.delete_rows_by_id("works", [owners[0]])
            assert facade.get_custom(owners[0], num=number, index_is_id=True) is None
            raise RuntimeError("abort delete")
    assert facade.get_custom(owners[0], num=number, index_is_id=True) == "Keep"
    assert stored(db, "SELECT work_id FROM works WHERE work_id=?", (owners[0],)) == [
        (owners[0],)
    ]


def test_invalid_link_retarget_is_rejected_by_generated_integrity_trigger(db):
    facade, number, owners = service(db)
    facade.set_custom(owners[0], "Keep", num=number)
    _, link = facade.custom_table_names(number, in_table="works")
    with pytest.raises(Exception, match="Custom field reference violation"):
        facade.conn.execute(f"UPDATE {link} SET {link}_value=987654")
    facade.conn.rollback()
    assert facade.get_custom(owners[0], num=number, index_is_id=True) == "Keep"
