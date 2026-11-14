"""Field evolution must never recycle a deleted membership or vocabulary handle."""

from contextlib import closing

import pytest

from LiuXin_alpha.databases.custom_columns import CustomFieldError
from tests.databases.test_native_custom_fields import native as native
from tests.databases.test_row_write_safety import db as db
from tests.databases.test_row_write_safety import template as template


def prepared_field(native, empty):
    """Leave holes in both identity spaces, including unused vocabulary history."""
    fields, (owner, _) = native
    field = fields.create_field("works", "identities", mode="list")
    fields.set_value(owner, field.id, ["retained", "removed"])
    retained, removed = fields.get_value(owner, field.id)
    fields.delete_value(field.id, removed.value_id)
    inventory_ids = [fields.ensure_value(field.id, f"unused-{i}") for i in range(4)]
    fields.delete_value(field.id, inventory_ids[-1])
    if empty:
        fields.delete_value(field.id, retained.value_id)
        for value_id in inventory_ids[:-1]:
            fields.delete_value(field.id, value_id)
    return fields, owner, field, removed, inventory_ids[-1]


@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("migration", ["metadata", "mode", "datatype-roundtrip"])
def test_migration_never_reuses_deleted_identities(native, empty, migration):
    """History survives empty storage and transitions between one and two tables."""
    fields, owner, field, removed, deleted_vocabulary = prepared_field(native, empty)
    changes = {
        "metadata": [{"name": "New display name"}],
        "mode": [{"mode": "set"}],
        "datatype-roundtrip": [{"datatype": "comments"}, {"datatype": "text"}],
    }[migration]
    for change in changes:
        plan = fields.plan_field_migration(field.id, change)
        assert not plan.issues
        fields.apply_field_migration(plan)
    fields.edit_entries(owner, field.id, add=["new membership"])
    new = fields.get_value(owner, field.id)[-1]
    assert new.id > removed.id
    assert new.value_id > deleted_vocabulary
    before = fields.get_value(owner, field.id)
    with pytest.raises(CustomFieldError, match="missing entries"):
        fields.edit_entries(owner, field.id, update={removed.id: "stale edit"})
    for stale in (removed.value_id, deleted_vocabulary):
        with pytest.raises(CustomFieldError, match="Unknown inventory"):
            fields.rename_value(field.id, stale, "stale vocabulary rename")
    assert fields.get_value(owner, field.id) == before
    # Verify the same durable values without using the service's read connection.
    table = f"custom_column_{field.id}"
    with closing(fields.db.driver.get_connection()) as reader:
        values = reader.execute(f'SELECT "{table}_value" FROM "{table}"').fetchall()
    assert ("new membership",) in values
    assert not any(v[0].startswith("stale") for v in values)


def test_metadata_only_edit_preserves_physical_indexes(native):
    """A descriptive edit keeps table-local state instead of dropping/recreating it."""
    fields, (_, _) = native
    field = fields.create_field("works", "heading", mode="list")
    table = f"custom_column_{field.id}"
    with fields.write() as connection:
        connection.execute(
            f'CREATE INDEX review_identity_index ON "{table}" ("{table}_value")'
        ).close()
    fields.apply_field_migration(
        fields.plan_field_migration(
            field.id,
            {"name": "Renamed heading", "label": "new_heading", "editable": False},
        )
    )
    with fields.read() as connection:
        if fields.repository.postgres:
            found = connection.execute(
                "SELECT indexname FROM pg_indexes WHERE schemaname=current_schema() AND indexname='review_identity_index'"
            ).fetchall()
        else:
            found = connection.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name='review_identity_index'"
            ).fetchall()
    assert found == [("review_identity_index",)]
    assert fields.get_field(("works", "new_heading")).editable is False


def test_failed_rebuild_preserves_values_and_identity_history(native, monkeypatch):
    """DDL/value rollback restores the old field, including its deleted-ID bound."""
    fields, owner, field, removed, deleted_vocabulary = prepared_field(native, False)
    before = fields.get_value(owner, field.id)
    plan = fields.plan_field_migration(field.id, {"mode": "set"})
    original = fields.repository.write_entries

    def fail_after_write(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("migration interrupted")

    with monkeypatch.context() as patch:
        patch.setattr(fields.repository, "write_entries", fail_after_write)
        with pytest.raises(RuntimeError, match="migration interrupted"):
            fields.apply_field_migration(plan)
    assert fields.get_field(field.id).mode == "list"
    assert fields.get_value(owner, field.id) == before
    fields.edit_entries(owner, field.id, add=["after rollback"])
    added = fields.get_value(owner, field.id)[-1]
    assert added.id > removed.id
    assert added.value_id > deleted_vocabulary
