"""Native field snapshots stay independent of Calibre layouts and live mutations."""

from dataclasses import replace

import pytest

from LiuXin_alpha.caches.field_metadata import FieldMetadata
from LiuXin_alpha.databases.custom_columns.native_types import FieldDefinition
from LiuXin_alpha.databases.schema_specs import (
    RelationKind,
    StorageColumnSpec,
    StorageSchemaSpec,
    StorageTableSpec,
)


def test_schema_fields_use_physical_qualified_keys():
    table = StorageTableSpec(
        "works",
        RelationKind.TABLE,
        (
            StorageColumnSpec("work_id", 0, affinity="INTEGER", is_primary_key=True),
            StorageColumnSpec("work_title", 1, affinity="TEXT"),
        ),
        id_column="work_id",
        is_main_table=True,
    )
    fields = FieldMetadata(
        StorageSchemaSpec(tables={"works": table}, interlinks=(), intralinks=())
    )
    assert fields.standard_field_keys() == ("works.work_id", "works.work_title")
    assert fields["works.work_title"]["datatype"] == "TEXT"
    assert (
        "title" not in fields
        and "authors" not in fields
        and "series_index" not in fields
    )
    fields.prepare_custom_fields(
        [FieldDefinition(1, "works", "series", "Series", datatype="series")]
    )()
    assert fields["#works.series"]["collection_mode"] == "scalar"
    assert "#works.series_index" not in fields
    assert "works.work_title" in fields


def test_snapshot_records_and_iteration_survive_later_publications():
    fields = FieldMetadata()
    display = {"choices": ["one", "two"]}
    definition = FieldDefinition(1, "works", "state", "State", display=display)
    fields.prepare_custom_fields([definition])()
    old = fields.snapshot()
    items = fields.items()
    display["choices"].append("external mutation")
    with pytest.raises(TypeError):
        fields[definition.key]["display"]["choices"] = ()
    fields.prepare_custom_fields([replace(definition, label="renamed", revision=1)])()
    assert old.generation == 1 and fields.generation == 2
    assert tuple(items) == ((definition.key, old.fields[definition.key]),)
    assert old.fields[definition.key]["display"]["choices"] == ("one", "two")
    assert definition.key not in fields


def test_alias_ambiguity_and_retirement_are_published_together():
    fields = FieldMetadata()
    first = FieldDefinition(1, "works", "status", "Status")
    other = FieldDefinition(2, "manifestations", "status", "Status")
    fields.prepare_custom_fields([first])()
    assert fields.search_term_to_field_key("#status") == first.key
    fields.prepare_custom_fields([first, other])()
    assert fields.search_term_to_field_key("#status") is None
    assert fields.search_term_to_field_key(other.key) == other.key
    fields.prepare_custom_fields([replace(first, retired=True), other])()
    assert fields.custom_field_keys() == (other.key,)
    assert fields.search_term_to_field_key("#status") == other.key


def test_prepared_older_snapshot_cannot_restore_retired_fields():
    fields = FieldMetadata()
    definition = FieldDefinition(1, "works", "old", "Old")
    older = fields.prepare_custom_fields([definition])
    retirement = fields.prepare_custom_fields([])
    assert fields.generation == 0
    retirement()
    older()
    assert fields.generation == 1 and not fields
    assert not fields.snapshot().aliases


def test_failed_or_discarded_preparation_does_not_block_valid_publication():
    fields = FieldMetadata()
    definition = FieldDefinition(1, "works", "valid", "Valid")
    valid = fields.prepare_custom_fields([definition])
    fields.prepare_custom_fields([])  # Simulate a callback discarded on rollback.
    with pytest.raises(ValueError, match="Duplicate"):
        fields.prepare_custom_fields([definition, definition])
    valid()
    assert fields.generation == 1 and definition.key in fields
