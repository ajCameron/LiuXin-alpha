"""
Writer for values stored directly on their source-table row.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from LiuXin_alpha.catalog.write.base_writer import CatalogValueWriter
from LiuXin_alpha.catalog.write.column_update import CatalogColumnUpdate
from LiuXin_alpha.catalog.write.host_api import CatalogWriterHostAPI
from LiuXin_alpha.databases.db_types import SrcTableID
from LiuXin_alpha.databases.schema_specs import (
    StorageColumnSpec,
    StorageTableSpec,
)

class CatalogColumnWriter[RawValueT, ValueT](
    CatalogValueWriter[
        RawValueT,
        ValueT,
        CatalogColumnUpdate[ValueT],
        Mapping[SrcTableID, ValueT],
    ]
):
    """
    Write values to a column stored directly on the source table.

    The default adapter preserves caller values. Field-specific subclasses may
    override :meth:`adapt` and :meth:`validate` without changing persistence.

    :param catalog: Catalog facade used to apply normalized column updates.
    :param table_spec: Source table containing the destination column.
    :param column_spec: Destination column to update.
    """

    def __init__(
        self,
        catalog: CatalogWriterHostAPI,
        table_spec: StorageTableSpec,
        column_spec: StorageColumnSpec,
    ) -> None:
        """
        Validate and store the column-writer configuration.

        :param catalog: Catalog facade used to apply normalized column updates.
        :param table_spec: Source table containing the destination column.
        :param column_spec: Destination column to update.
        :return: None.
        """

        if not callable(getattr(catalog, "write_column_update", None)):
            raise TypeError("catalog must provide write_column_update")
        # Reuse the value object's structural validation without retaining a
        # second representation of the target.
        CatalogColumnUpdate(table_spec, column_spec)
        super().__init__(catalog)
        self._table_spec = table_spec
        self._column_spec = column_spec

    @property
    def table_spec(self) -> StorageTableSpec:
        """
        Return the source-table specification.

        :return: Configured source-table specification.
        """

        return self._table_spec

    @property
    def column_spec(self) -> StorageColumnSpec:
        """
        Return the destination-column specification.

        :return: Configured destination-column specification.
        """

        return self._column_spec

    def adapt(self, raw_value: RawValueT) -> ValueT:
        """
        Preserve one raw value by default.

        :param raw_value: Raw metadata value supplied by the caller.
        :return: Unchanged value, typed for the concrete writer.
        """

        return cast(ValueT, raw_value)

    def build_update(
        self,
        values: Mapping[SrcTableID, RawValueT],
    ) -> CatalogColumnUpdate[ValueT]:
        """
        Build an immutable normalized column update.

        :param values: Raw values keyed by source-table ID.
        :return: Immutable normalized column update.
        """

        if not isinstance(values, Mapping):
            raise TypeError("values must be a mapping")
        return CatalogColumnUpdate(
            self.table_spec,
            self.column_spec,
            {
                source_id: self.prepare_value(raw_value)
                for source_id, raw_value in values.items()
            },
        )

    def build_one_update(
        self,
        src_id: SrcTableID,
        dst_value: RawValueT,
        **kwargs: object,
    ) -> CatalogColumnUpdate[ValueT]:
        """
        Build one same-table column replacement.

        :param src_id: Source-table ID whose column should change.
        :param dst_value: Raw replacement value.
        :param kwargs: Unsupported additional update options.
        :return: Immutable normalized column update.
        :raises TypeError: If additional update options are supplied.
        """

        if kwargs:
            names = ", ".join(sorted(kwargs))
            raise TypeError(
                f"column write_one received unexpected option(s): {names}"
            )
        return self.build_update({src_id: dst_value})

    def apply_update(
        self,
        update: CatalogColumnUpdate[ValueT],
    ) -> Mapping[SrcTableID, ValueT]:
        """
        Apply one normalized update through the catalog.

        :param update: Immutable normalized column update.
        :return: Stable written values keyed by source-table ID.
        :raises TypeError: If ``update`` is not a column update.
        :raises ValueError: If the update targets another table or column.
        """

        if not isinstance(update, CatalogColumnUpdate):
            raise TypeError("update must be a CatalogColumnUpdate")
        if (
            update.table_spec != self.table_spec
            or update.column_spec != self.column_spec
        ):
            raise ValueError("update target does not match writer target")
        return cast(
            Mapping[SrcTableID, ValueT],
            self.catalog.write_column_update(update),
        )


__all__ = ["CatalogColumnWriter"]
