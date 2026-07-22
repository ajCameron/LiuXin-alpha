"""Tests for catalog-owned one-to-one destination-row writes."""

from __future__ import annotations

from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any

import pytest

from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.catalog.write import (
    CatalogOwnedRowOneToOneWriter,
    CatalogOwnedRowUpdate,
)
from LiuXin_alpha.databases.macro_types import LinkRow
from LiuXin_alpha.databases.schema_specs import (
    LinkCardinality,
    RelationKind,
    StorageColumnSpec,
    StorageLinkSpec,
    StorageTableSpec,
)


def _column(
    name: str,
    ordinal: int,
    *,
    primary_key: bool = False,
) -> StorageColumnSpec:
    return StorageColumnSpec(
        name=name,
        ordinal=ordinal,
        affinity="INTEGER" if primary_key else "TEXT",
        is_primary_key=primary_key,
    )


def _target(
    cardinality: LinkCardinality = LinkCardinality.ONE_TO_ONE,
) -> tuple[StorageLinkSpec, StorageTableSpec, StorageColumnSpec]:
    value_id = _column("owned_value_id", 0, primary_key=True)
    value = _column("owned_value", 1)
    destination = StorageTableSpec(
        name="owned_values",
        relation_kind=RelationKind.TABLE,
        columns=(value_id, value),
        id_column=value_id.name,
        is_main_table=True,
    )
    link_spec = StorageLinkSpec(
        primary_table="owned_sources",
        secondary_table=destination.name,
        link_table="owned_links",
        cardinality=cardinality,
        primary_id_col="owned_source_id",
        secondary_id_col=value_id.name,
        primary_link_col="owned_source_id",
        secondary_link_col=value_id.name,
    )
    return link_spec, destination, value


class _Macros:
    def __init__(self) -> None:
        self.calls: list[
            tuple[StorageLinkSpec, str, dict[int, Any | None]]
        ] = []

    def replace_owned_one_to_one_values_bulk(
        self,
        link_spec: StorageLinkSpec,
        value_column: str,
        replacements: Mapping[int, Any | None],
    ) -> dict[int, tuple[LinkRow, ...]]:
        materialized = dict(replacements)
        self.calls.append((link_spec, value_column, materialized))
        return {
            source_id: (
                ()
                if value is None
                else (LinkRow(source_id, 100 + source_id),)
            )
            for source_id, value in materialized.items()
        }


class _Catalog:
    def __init__(self) -> None:
        self.db = SimpleNamespace(macros=_Macros())
        self.updates: list[CatalogOwnedRowUpdate[Any]] = []

    def write_owned_row_update(
        self,
        update: CatalogOwnedRowUpdate[Any],
    ) -> Mapping[int, tuple[LinkRow, ...]]:
        self.updates.append(update)
        return update.write(self.db.macros)


class _NormalizingWriter(CatalogOwnedRowOneToOneWriter[str, str]):
    def __init__(self, *args: Any) -> None:
        self.adapted: list[str] = []
        self.validated: list[str] = []
        super().__init__(*args)

    def adapt(self, raw_value: str) -> str:
        self.adapted.append(raw_value)
        return raw_value.strip().upper()

    def validate(self, value: str) -> None:
        self.validated.append(value)
        if value == "INVALID":
            raise ValueError("invalid owned value")


def test_owned_row_update_snapshots_values_and_delegates_once() -> None:
    link_spec, table, column = _target()
    supplied = {1: "first", 2: None}
    update = CatalogOwnedRowUpdate(link_spec, table, column, supplied)
    supplied[1] = "mutated"
    macros = _Macros()

    result = update.write(macros)  # type: ignore[arg-type]

    assert update.values == {1: "first", 2: None}
    with pytest.raises(TypeError):
        update.values[3] = "blocked"  # type: ignore[index]
    assert macros.calls == [
        (link_spec, "owned_value", {1: "first", 2: None})
    ]
    assert result == {1: (LinkRow(1, 101),), 2: ()}


def test_empty_owned_row_update_is_a_no_op() -> None:
    link_spec, table, column = _target()
    macros = _Macros()

    assert CatalogOwnedRowUpdate(link_spec, table, column).write(
        macros  # type: ignore[arg-type]
    ) == {}
    assert macros.calls == []


def test_owned_row_writer_prepares_values_but_preserves_unlink_instruction() -> None:
    link_spec, table, column = _target()
    catalog = _Catalog()
    writer = _NormalizingWriter(catalog, link_spec, table, column)

    update = writer.build_update({1: " first ", 2: None})
    result = writer.write_one(3, " third ")
    unlinked = writer.write_one(4, None)

    assert update.values == {1: "FIRST", 2: None}
    assert writer.adapted == [" first ", " third "]
    assert writer.validated == ["FIRST", "THIRD"]
    assert catalog.updates[0].values == {3: "THIRD"}
    assert catalog.updates[1].values == {4: None}
    assert result == {3: (LinkRow(3, 103),)}
    assert unlinked == {4: ()}

    with pytest.raises(TypeError, match="unexpected option"):
        writer.write_one(5, "fifth", unsupported=True)


def test_owned_row_writer_rejects_invalid_values_and_update_targets() -> None:
    link_spec, table, column = _target()
    catalog = _Catalog()
    writer = _NormalizingWriter(catalog, link_spec, table, column)

    with pytest.raises(ValueError, match="invalid owned value"):
        writer.build_update({1: "invalid"})
    with pytest.raises(TypeError, match="CatalogOwnedRowUpdate"):
        writer.apply_update(object())  # type: ignore[arg-type]

    other_column = _column("other_value", 2)
    other_table = StorageTableSpec(
        name=table.name,
        relation_kind=table.relation_kind,
        columns=(*table.columns, other_column),
        id_column=table.id_column,
        is_main_table=True,
    )
    other_update = CatalogOwnedRowUpdate(
        link_spec,
        other_table,
        other_column,
        {1: "other"},
    )
    with pytest.raises(ValueError, match="target"):
        writer.apply_update(other_update)


def test_owned_row_configuration_requires_one_to_one_writable_target() -> None:
    link_spec, table, column = _target(LinkCardinality.MANY_TO_ONE)
    with pytest.raises(ValueError, match="one-to-one"):
        CatalogOwnedRowUpdate(link_spec, table, column)

    one_to_one, table, _column_spec = _target()
    with pytest.raises(ValueError, match="primary key"):
        CatalogOwnedRowUpdate(one_to_one, table, table.columns[0])
    with pytest.raises(TypeError, match="write_owned_row_update"):
        CatalogOwnedRowOneToOneWriter(
            object(),  # type: ignore[arg-type]
            one_to_one,
            table,
            column,
        )


def test_catalog_facade_applies_and_type_checks_owned_row_updates() -> None:
    link_spec, table, column = _target()
    macros = _Macros()
    catalog = Catalog(SimpleNamespace(macros=macros))
    update = CatalogOwnedRowUpdate(link_spec, table, column, {1: "value"})

    assert catalog.write_owned_row_update(update) == {
        1: (LinkRow(1, 101),),
    }
    with pytest.raises(TypeError, match="CatalogOwnedRowUpdate"):
        catalog.write_owned_row_update(object())  # type: ignore[arg-type]
