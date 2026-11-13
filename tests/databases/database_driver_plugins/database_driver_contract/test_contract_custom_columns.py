"""Protect custom-field storage through its sole native owner."""

from LiuXin_alpha.utils.calibre_compat.custom_columns.adapter import CustomColumnAdapter

import pytest

from LiuXin_alpha.databases.custom_columns import (
    CustomFieldError,
    CustomFieldService,
)
from LiuXin_alpha.databases.custom_columns.native_repository import names
from tests.databases.test_native_custom_fields import native as native
from tests.databases.test_row_write_safety import db as db
from tests.databases.test_row_write_safety import template as template


def test_one_to_one_custom_column_uniqueness_and_cascade(native):
    service, (first, second) = native
    field = service.create_field("works", "inline", datatype="comments")
    payload = "x'); DROP TABLE works; -- 日本語"
    service.set_value(first, field.id, payload)
    service.set_value(first, field.id, "replaced")
    service.set_value(second, field.id, payload)
    assert service.get_value(first, field.id) == "replaced"
    assert service.get_value(second, field.id) == payload
    service.db.macros.delete_rows_by_id("works", [first.id])
    with service.read() as connection:
        assert set(service.repository.read_entries(connection, field)) == {second.id}
    assert service.audit() == ()


def test_one_many_custom_column_enforces_secondary_exclusivity_and_cleanup(native):
    service, (first, second) = native
    field = service.create_field("works", "owned", datatype="comments", mode="list")
    for owner in (first, second):
        service.set_value(owner, field.id, ["same", "same"])
    first_entries = service.get_value(first, field.id)
    second_entries = service.get_value(second, field.id)
    assert {entry.id for entry in first_entries}.isdisjoint(
        entry.id for entry in second_entries
    )
    with pytest.raises(CustomFieldError, match="belong"):
        service.set_value(second, field.id, [first_entries[0]])
    service.db.macros.delete_rows_by_id("works", [first.id])
    assert len(service.get_value(second, field.id)) == 2
    with service.read() as connection:
        assert set(service.repository.read_entries(connection, field)) == {second.id}


def test_many_one_custom_column_allows_shared_secondary_but_limits_primary(native):
    service, (first, second) = native
    field = service.create_field("works", "shared")
    for owner in (first, second):
        service.set_value(owner, field.id, "shared")
    assert service.inventory(field.id).items[0]["owners"] == 2
    service.set_value(first, field.id, "replacement")
    assert service.get_value(first, field.id) == "replacement"
    assert service.get_value(second, field.id) == "shared"
    assert service.audit() == ()


def test_many_many_custom_column_via_direct_method_roundtrips(native):
    service, (first, second) = native
    field = service.create_field("works", "shared_set", mode="set")
    for owner in (first, second):
        service.set_value(owner, field.id, ["日本語", "SQL'; --", "日本語"])
    assert [entry.value for entry in service.get_value(first, field.id)] == [
        "日本語",
        "SQL'; --",
    ]
    assert all(item["owners"] == 2 for item in service.inventory(field.id).items)
    assert service.audit() == ()


def test_direct_create_custom_column_many_many_returns_table_name(db):
    """Raw driver creation cannot introduce another custom-column storage layout."""
    for cls in type(db.driver).__mro__:
        assert "direct_create_custom_column" not in vars(cls)
        assert "direct_create_many_many_custom_column" not in vars(cls)
    number = CustomColumnAdapter(db).create_custom_column(
        "Native", label="native", in_table="works", is_multiple=True
    )
    service = CustomFieldService(db)
    field = service.get_field(number)
    assert field.mode == "set"
    with service.read() as connection:
        assert service.repository.version(connection, field) == 2
        assert all(
            service.repository.table_exists(connection, table) for table in names(field)
        )
