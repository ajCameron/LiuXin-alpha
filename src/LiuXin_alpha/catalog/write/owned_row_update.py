"""Normalized updates for values stored in owned one-to-one rows."""

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
    return {}


@dataclass(frozen=True, slots=True)
class CatalogOwnedRowUpdate[ValueT]:
    """
    Describe value replacements for destination rows owned one-to-one.

    A non-null value updates the source's linked destination row in place. If
    the source has no link, application creates and links a destination row in
    the same transaction. ``None`` removes the link but deliberately leaves
    the destination row for explicit cleanup policy.

    :param link_spec: Directed one-to-one storage route.
    :param destination_table: Table containing each owned destination row.
    :param destination_column: Value column to update or populate.
    :param values: Replacement values keyed by source-table ID.
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

        :return: None.
        :raises TypeError: If specifications or values have invalid types.
        :raises ValueError: If the specifications do not describe a writable
            one-to-one destination value.
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

        :param macros: Portable macro layer for the catalog database.
        :return: Complete link rows keyed by affected source-table ID.
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
