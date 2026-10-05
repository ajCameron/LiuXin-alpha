"""
Verify test owned row writer behavior against the public catalog contracts.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test owned row writer through its owning regression module::

        python -m pytest -q tests/catalog/test_owned_row_writer.py
"""

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
    """
    Perform the column test-helper operation with deterministic inputs.

    Example:
        Exercise column through its owning regression module::

            python -m pytest -q tests/catalog/test_owned_row_writer.py


    :param name: Value supplied for name under the catalog contract.
    :param ordinal: Value supplied for ordinal under the catalog contract.
    :param primary_key: Value supplied for primary key under the catalog contract.
    :return: The deterministic value, row, identity or collection described above.
    """
    return StorageColumnSpec(
        name=name,
        ordinal=ordinal,
        affinity="INTEGER" if primary_key else "TEXT",
        is_primary_key=primary_key,
    )


def _target(
    cardinality: LinkCardinality = LinkCardinality.ONE_TO_ONE,
) -> tuple[StorageLinkSpec, StorageTableSpec, StorageColumnSpec]:
    """
    Perform the target test-helper operation with deterministic inputs.

    Example:
        Exercise target through its owning regression module::

            python -m pytest -q tests/catalog/test_owned_row_writer.py


    :param cardinality: Value supplied for cardinality under the catalog contract.
    :return: The deterministic value, row, identity or collection described above.
    """
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
    """
    Provide the Macros test fixture or double with explicit deterministic behavior.

    Example:
        Exercise Macros through its owning regression module::

            python -m pytest -q tests/catalog/test_owned_row_writer.py
    """
    def __init__(self) -> None:
        """
        Initialize the Macros test double.

        Example:
            Exercise Macros.init through its owning regression module::

                python -m pytest -q tests/catalog/test_owned_row_writer.py


        :return: None; the function records state or raises through its assertions.
        """
        self.calls: list[
            tuple[StorageLinkSpec, str, dict[int, Any | None]]
        ] = []

    def replace_owned_one_to_one_values_bulk(
        self,
        link_spec: StorageLinkSpec,
        value_column: str,
        replacements: Mapping[int, Any | None],
    ) -> dict[int, tuple[LinkRow, ...]]:
        """
        Perform the replace owned one to one values bulk test-helper operation with deterministic inputs.

        Example:
            Exercise Macros.replace owned one to one values bulk through its owning regression module::

                python -m pytest -q tests/catalog/test_owned_row_writer.py


        :param link_spec: Value supplied for link spec under the catalog contract.
        :param value_column: Value supplied for value column under the catalog contract.
        :param replacements: Value supplied for replacements under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """
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
    """
    Provide the Catalog test fixture or double with explicit deterministic behavior.

    Example:
        Exercise Catalog through its owning regression module::

            python -m pytest -q tests/catalog/test_owned_row_writer.py
    """
    def __init__(self) -> None:
        """
        Initialize the Catalog test double.

        Example:
            Exercise Catalog.init through its owning regression module::

                python -m pytest -q tests/catalog/test_owned_row_writer.py


        :return: None; the function records state or raises through its assertions.
        """
        self.db = SimpleNamespace(macros=_Macros())
        self.updates: list[CatalogOwnedRowUpdate[Any]] = []

    def write_owned_row_update(
        self,
        update: CatalogOwnedRowUpdate[Any],
    ) -> Mapping[int, tuple[LinkRow, ...]]:
        """
        Perform the write owned row update test-helper operation with deterministic inputs.

        Example:
            Exercise Catalog.write owned row update through its owning regression module::

                python -m pytest -q tests/catalog/test_owned_row_writer.py


        :param update: Prepared catalog update to validate or apply.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.updates.append(update)
        return update.write(self.db.macros)


class _NormalizingWriter(CatalogOwnedRowOneToOneWriter[str, str]):
    """
    Provide the NormalizingWriter test fixture or double with explicit deterministic behavior.

    Example:
        Exercise NormalizingWriter through its owning regression module::

            python -m pytest -q tests/catalog/test_owned_row_writer.py
    """
    def __init__(self, *args: Any) -> None:
        """
        Initialize the NormalizingWriter test double.

        Example:
            Exercise NormalizingWriter.init through its owning regression module::

                python -m pytest -q tests/catalog/test_owned_row_writer.py


        :param args: Value supplied for args under the catalog contract.
        :return: None; the function records state or raises through its assertions.
        """
        self.adapted: list[str] = []
        self.validated: list[str] = []
        super().__init__(*args)

    def adapt(self, raw_value: str) -> str:
        """
        Perform the adapt test-helper operation with deterministic inputs.

        Example:
            Exercise NormalizingWriter.adapt through its owning regression module::

                python -m pytest -q tests/catalog/test_owned_row_writer.py


        :param raw_value: Value supplied for raw value under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.adapted.append(raw_value)
        return raw_value.strip().upper()

    def validate(self, value: str) -> None:
        """
        Perform the validate test-helper operation with deterministic inputs.

        Example:
            Exercise NormalizingWriter.validate through its owning regression module::

                python -m pytest -q tests/catalog/test_owned_row_writer.py


        :param value: Public or stored value to normalize, compare or write.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.validated.append(value)
        if value == "INVALID":
            raise ValueError("invalid owned value")


def test_owned_row_update_snapshots_values_and_delegates_once() -> None:
    """
    Verify owned row update snapshots values and delegates once.

    Example:
        Exercise test owned row update snapshots values and delegates once through its owning regression module::

            python -m pytest -q tests/catalog/test_owned_row_writer.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify empty owned row update remains a no op.

    Example:
        Exercise test empty owned row update is a no op through its owning regression module::

            python -m pytest -q tests/catalog/test_owned_row_writer.py


    :return: None; the function records state or raises through its assertions.
    """
    link_spec, table, column = _target()
    macros = _Macros()

    assert CatalogOwnedRowUpdate(link_spec, table, column).write(
        macros  # type: ignore[arg-type]
    ) == {}
    assert macros.calls == []


def test_owned_row_writer_prepares_values_but_preserves_unlink_instruction() -> None:
    """
    Verify owned row writer prepares values but preserves unlink instruction.

    Example:
        Exercise test owned row writer prepares values but preserves unlink instruction through its owning regression module::

            python -m pytest -q tests/catalog/test_owned_row_writer.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify owned row writer rejects invalid values and update targets.

    Example:
        Exercise test owned row writer rejects invalid values and update targets through its owning regression module::

            python -m pytest -q tests/catalog/test_owned_row_writer.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify owned row configuration requires one to one writable target.

    Example:
        Exercise test owned row configuration requires one to one writable target through its owning regression module::

            python -m pytest -q tests/catalog/test_owned_row_writer.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify catalog facade applies and type checks owned row updates.

    Example:
        Exercise test catalog facade applies and type checks owned row updates through its owning regression module::

            python -m pytest -q tests/catalog/test_owned_row_writer.py


    :return: None; the function records state or raises through its assertions.
    """
    link_spec, table, column = _target()
    macros = _Macros()
    catalog = Catalog(SimpleNamespace(macros=macros))
    update = CatalogOwnedRowUpdate(link_spec, table, column, {1: "value"})

    assert catalog.write_owned_row_update(update) == {
        1: (LinkRow(1, 101),),
    }
    with pytest.raises(TypeError, match="CatalogOwnedRowUpdate"):
        catalog.write_owned_row_update(object())  # type: ignore[arg-type]
