"""
Represent one immutable catalog column mutation.

The module keeps validation, normalization and host mutation boundaries explicit.

Example:
    Exercise column update through its owning regression module::

        python -m pytest -q tests/catalog/test_writer_factory.py
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
    """
    Return whether an update payload explicitly clears its target value.

    Example:
        Exercise empty values through its owning regression module::

            python -m pytest -q tests/catalog/test_writer_factory.py


    :return: The deterministic value, row, identity or collection described above.
    """
    return {}


@dataclass(frozen=True, slots=True)
class CatalogColumnUpdate[ValueT]:
    """
    Describe one bulk update to a column stored on its source table.

    Example:
        Exercise CatalogColumnUpdate through its owning regression module::

            python -m pytest -q tests/catalog/test_writer_factory.py
    """

    table_spec: StorageTableSpec
    column_spec: StorageColumnSpec
    values: Mapping[SrcTableID, ValueT] = field(default_factory=_empty_values)

    def __post_init__(self) -> None:
        """
        Validate and materialize the update request.

        Example:
            Exercise CatalogColumnUpdate.post init through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :return: None; the function records state or raises through its assertions.
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

        Example:
            Exercise CatalogColumnUpdate.write through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param database: Value supplied for database under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
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
