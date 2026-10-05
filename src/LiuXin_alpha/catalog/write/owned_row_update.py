"""
Represent one mutation of a row owned through a catalog relation.

The module keeps validation, normalization and host mutation boundaries explicit.

Example:
    Exercise owned row update through its owning regression module::

        python -m pytest -q tests/catalog/test_owned_row_writer.py
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, cast

from LiuXin_alpha.databases.db_types import SrcTableID
from LiuXin_alpha.databases.macro_types import LinkRow
from LiuXin_alpha.databases.schema_specs import (
    LinkCardinality,
    StorageColumnSpec,
    StorageLinkSpec,
    StorageTableSpec,
)

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import PortableMacrosAPI


def _empty_values[ValueT]() -> dict[SrcTableID, ValueT | None]:
    """
    Return whether an update payload explicitly clears its target value.

    Example:
        Exercise empty values through its owning regression module::

            python -m pytest -q tests/catalog/test_owned_row_writer.py


    :return: The deterministic value, row, identity or collection described above.
    """
    return {}


@dataclass(frozen=True, slots=True)
class CatalogOwnedRowUpdate[ValueT]:
    """
    Describe value replacements for destination rows owned one-to-one.

    Example:
        Exercise CatalogOwnedRowUpdate through its owning regression module::

            python -m pytest -q tests/catalog/test_owned_row_writer.py
    """

    link_spec: StorageLinkSpec
    destination_table: StorageTableSpec
    destination_column: StorageColumnSpec
    values: Mapping[SrcTableID, ValueT | None] = field(
        default_factory=_empty_values
    )

    def __post_init__(self) -> None:
        """
        Validate and materialize the owned-row update.

        Example:
            Exercise CatalogOwnedRowUpdate.post init through its owning regression module::

                python -m pytest -q tests/catalog/test_owned_row_writer.py


        :return: None; the function records state or raises through its assertions.
        """

        if not isinstance(self.link_spec, StorageLinkSpec):
            raise TypeError("link_spec must be a StorageLinkSpec")
        if self.link_spec.cardinality is not LinkCardinality.ONE_TO_ONE:
            raise ValueError("owned-row updates require a one-to-one link spec")
        if not isinstance(self.destination_table, StorageTableSpec):
            raise TypeError("destination_table must be a StorageTableSpec")
        if not isinstance(self.destination_column, StorageColumnSpec):
            raise TypeError("destination_column must be a StorageColumnSpec")
        if self.destination_table.name != self.link_spec.secondary_table:
            raise ValueError(
                "destination_table must match link_spec.secondary_table"
            )
        if self.destination_column not in self.destination_table.columns:
            raise ValueError(
                "destination_column must belong to destination_table"
            )
        if (
            self.destination_column.is_primary_key
            or self.destination_column.name == self.link_spec.secondary_id_col
        ):
            raise ValueError("owned-row value column cannot be a primary key")
        if not isinstance(self.values, Mapping):
            raise TypeError("values must be a mapping")
        object.__setattr__(
            self,
            "values",
            MappingProxyType(dict(self.values)),
        )

    def write(
        self,
        macros: PortableMacrosAPI,
    ) -> Mapping[SrcTableID, tuple[LinkRow, ...]]:
        """
        Apply this update through one portable atomic database operation.

        Example:
            Exercise CatalogOwnedRowUpdate.write through its owning regression module::

                python -m pytest -q tests/catalog/test_owned_row_writer.py


        :param macros: Value supplied for macros under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        if not self.values:
            return {}
        return cast(
            Mapping[SrcTableID, tuple[LinkRow, ...]],
            macros.replace_owned_one_to_one_values_bulk(
                self.link_spec,
                self.destination_column.name,
                self.values,
            ),
        )


__all__ = ["CatalogOwnedRowUpdate"]
