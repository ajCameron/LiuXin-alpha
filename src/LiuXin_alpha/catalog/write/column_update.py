"""
Normalized same-table column updates for catalog writers.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING

from LiuXin_alpha.databases.db_types import SrcTableID
from LiuXin_alpha.databases.schema_specs import (
    StorageColumnSpec,
    StorageTableSpec,
)

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


def _empty_values[ValueT]() -> dict[SrcTableID, ValueT]:
    return {}


@dataclass(frozen=True, slots=True)
class CatalogColumnUpdate[ValueT]:
    """
    Describe one bulk update to a column stored on its source table.

    The caller-owned value mapping is copied during construction. Applying an
    empty update is a no-op; otherwise the database performs one bulk column
    update and the stable value mapping is returned.

    :param table_spec: Table containing both source IDs and destination column.
    :param column_spec: Destination column to update.
    :param values: New column values keyed by source-table ID.
    """

    table_spec: StorageTableSpec
    column_spec: StorageColumnSpec
    values: Mapping[SrcTableID, ValueT] = field(default_factory=_empty_values)

    def __post_init__(self) -> None:
        """
        Validate and materialize the update request.

        :return: None.
        :raises TypeError: If specifications or values have invalid types.
        :raises ValueError: If the column does not belong to the table or is
            its primary key.
        """

        if not isinstance(self.table_spec, StorageTableSpec):
            raise TypeError("table_spec must be a StorageTableSpec")
        if not isinstance(self.column_spec, StorageColumnSpec):
            raise TypeError("column_spec must be a StorageColumnSpec")
        if self.column_spec not in self.table_spec.columns:
            raise ValueError("column_spec must belong to table_spec")
        if self.column_spec.is_primary_key:
            raise ValueError("catalog column updates cannot target a primary key")
        if not isinstance(self.values, Mapping):
            raise TypeError("values must be a mapping")
        object.__setattr__(
            self,
            "values",
            MappingProxyType(dict(self.values)),
        )

    def write(
        self,
        database: DatabaseAPI,
    ) -> Mapping[SrcTableID, ValueT]:
        """
        Apply this update through the database's bulk-column operation.

        :param database: Catalog database handle.
        :return: Stable written values keyed by source-table ID.
        """

        if not self.values:
            return self.values
        database.update_columns(
            values_map=dict(self.values),
            field=self.column_spec.name,
            table=self.table_spec.name,
        )
        return self.values


__all__ = ["CatalogColumnUpdate"]
