"""Writer for values stored in destination rows owned one-to-one."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

from LiuXin_alpha.catalog.write.base_writer import CatalogValueWriter
from LiuXin_alpha.catalog.write.owned_row_update import CatalogOwnedRowUpdate
from LiuXin_alpha.databases.db_types import SrcTableID
from LiuXin_alpha.databases.macro_types import LinkRow
from LiuXin_alpha.databases.schema_specs import (
    StorageColumnSpec,
    StorageLinkSpec,
    StorageTableSpec,
)

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api import CatalogAPI


class CatalogOwnedRowOneToOneWriter[RawValueT, ValueT](
    CatalogValueWriter[
        RawValueT,
        ValueT,
        CatalogOwnedRowUpdate[ValueT],
        Mapping[SrcTableID, tuple[LinkRow, ...]],
    ]
):
    """
    Write values to destination rows owned by one source row each.

    This writer models a distinct one-to-one storage policy rather than a
    cardinality-only link operation. Existing destination rows retain their
    identity while their value changes. Missing rows are created and linked
    atomically, and ``None`` unlinks without performing implicit cleanup.

    :param catalog: Catalog facade used to apply normalized owned-row updates.
    :param link_spec: Directed one-to-one link specification.
    :param destination_table: Table containing each owned destination row.
    :param destination_column: Value column to update or populate.
    """

    def __init__(
        self,
        catalog: CatalogAPI,
        link_spec: StorageLinkSpec,
        destination_table: StorageTableSpec,
        destination_column: StorageColumnSpec,
    ) -> None:
        """
        Validate and store the owned-row writer configuration.

        :param catalog: Catalog facade used to apply normalized updates.
        :param link_spec: Directed one-to-one link specification.
        :param destination_table: Table containing each owned destination row.
        :param destination_column: Value column to update or populate.
        :return: None.
        """

        if not callable(getattr(catalog, "write_owned_row_update", None)):
            raise TypeError("catalog must provide write_owned_row_update")
        CatalogOwnedRowUpdate(
            link_spec,
            destination_table,
            destination_column,
        )
        super().__init__(catalog)
        self._link_spec = link_spec
        self._destination_table = destination_table
        self._destination_column = destination_column

    @property
    def link_spec(self) -> StorageLinkSpec:
        """
        Return the directed one-to-one storage route.

        :return: Configured link specification.
        """

        return self._link_spec

    @property
    def destination_table(self) -> StorageTableSpec:
        """
        Return the owned destination-table specification.

        :return: Configured destination-table specification.
        """

        return self._destination_table

    @property
    def destination_column(self) -> StorageColumnSpec:
        """
        Return the owned destination value-column specification.

        :return: Configured destination-column specification.
        """

        return self._destination_column

    def adapt(self, raw_value: RawValueT) -> ValueT:
        """
        Preserve one raw value by default.

        :param raw_value: Raw metadata value supplied by the caller.
        :return: Unchanged value, typed for the concrete writer.
        """

        return cast(ValueT, raw_value)

    def build_update(
        self,
        values: Mapping[SrcTableID, RawValueT | None],
    ) -> CatalogOwnedRowUpdate[ValueT]:
        """
        Build an immutable normalized owned-row update.

        ``None`` is the explicit unlink instruction and therefore bypasses
        field adaptation and validation.

        :param values: Raw replacement values keyed by source-table ID.
        :return: Immutable normalized owned-row update.
        """

        if not isinstance(values, Mapping):
            raise TypeError("values must be a mapping")
        return CatalogOwnedRowUpdate(
            self.link_spec,
            self.destination_table,
            self.destination_column,
            {
                source_id: (
                    None
                    if raw_value is None
                    else self.prepare_value(raw_value)
                )
                for source_id, raw_value in values.items()
            },
        )

    def build_one_update(
        self,
        src_id: SrcTableID,
        dst_value: RawValueT | None,
        **kwargs: object,
    ) -> CatalogOwnedRowUpdate[ValueT]:
        """
        Build one owned-row replacement or unlink instruction.

        :param src_id: Source-table ID whose owned value should change.
        :param dst_value: Raw replacement value, or ``None`` to unlink.
        :param kwargs: Unsupported additional update options.
        :return: Immutable normalized owned-row update.
        :raises TypeError: If additional update options are supplied.
        """

        if kwargs:
            names = ", ".join(sorted(kwargs))
            raise TypeError(
                f"owned-row write_one received unexpected option(s): {names}"
            )
        return self.build_update({src_id: dst_value})

    def apply_update(
        self,
        update: CatalogOwnedRowUpdate[ValueT],
    ) -> Mapping[SrcTableID, tuple[LinkRow, ...]]:
        """
        Apply one normalized owned-row update through the catalog.

        :param update: Immutable normalized owned-row update.
        :return: Complete written link rows keyed by source-table ID.
        :raises TypeError: If ``update`` is not an owned-row update.
        :raises ValueError: If the update targets another storage route.
        """

        if not isinstance(update, CatalogOwnedRowUpdate):
            raise TypeError("update must be a CatalogOwnedRowUpdate")
        if (
            update.link_spec != self.link_spec
            or update.destination_table != self.destination_table
            or update.destination_column != self.destination_column
        ):
            raise ValueError("update target does not match writer target")
        return self.catalog.write_owned_row_update(update)


__all__ = ["CatalogOwnedRowOneToOneWriter"]
