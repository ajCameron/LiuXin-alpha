"""
Build and apply one-to-one owned-row catalog updates.

The module keeps validation, normalization and host mutation boundaries explicit.

Example:
    Exercise owned row writer through its owning regression module::

        python -m pytest -q tests/catalog/test_owned_row_writer.py
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from LiuXin_alpha.catalog.write.base_writer import CatalogValueWriter
from LiuXin_alpha.catalog.write.host_api import CatalogWriterHostAPI
from LiuXin_alpha.catalog.write.owned_row_update import CatalogOwnedRowUpdate
from LiuXin_alpha.databases.db_types import SrcTableID
from LiuXin_alpha.databases.macro_types import LinkRow
from LiuXin_alpha.databases.schema_specs import (
    StorageColumnSpec,
    StorageLinkSpec,
    StorageTableSpec,
)

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

    Example:
        Exercise CatalogOwnedRowOneToOneWriter through its owning regression module::

            python -m pytest -q tests/catalog/test_owned_row_writer.py
    """

    def __init__(
        self,
        catalog: CatalogWriterHostAPI,
        link_spec: StorageLinkSpec,
        destination_table: StorageTableSpec,
        destination_column: StorageColumnSpec,
    ) -> None:
        """
        Validate and store the owned-row writer configuration.

        Example:
            Exercise CatalogOwnedRowOneToOneWriter.init through its owning regression module::

                python -m pytest -q tests/catalog/test_owned_row_writer.py


        :param catalog: Catalog host or facade supplying metadata and mutation services.
        :param link_spec: Value supplied for link spec under the catalog contract.
        :param destination_table: Value supplied for destination table under the catalog
            contract.
        :param destination_column: Value supplied for destination column under the catalog
            contract.
        :return: None; the function records state or raises through its assertions.
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

        Example:
            Exercise CatalogOwnedRowOneToOneWriter.link spec through its owning regression module::

                python -m pytest -q tests/catalog/test_owned_row_writer.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return self._link_spec

    @property
    def destination_table(self) -> StorageTableSpec:
        """
        Return the owned destination-table specification.

        Example:
            Exercise CatalogOwnedRowOneToOneWriter.destination table through its owning regression module::

                python -m pytest -q tests/catalog/test_owned_row_writer.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return self._destination_table

    @property
    def destination_column(self) -> StorageColumnSpec:
        """
        Return the owned destination value-column specification.

        Example:
            Exercise CatalogOwnedRowOneToOneWriter.destination column through its owning regression module::

                python -m pytest -q tests/catalog/test_owned_row_writer.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return self._destination_column

    def adapt(self, raw_value: RawValueT) -> ValueT:
        """
        Preserve one raw value by default.

        Example:
            Exercise CatalogOwnedRowOneToOneWriter.adapt through its owning regression module::

                python -m pytest -q tests/catalog/test_owned_row_writer.py


        :param raw_value: Value supplied for raw value under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        return cast(ValueT, raw_value)

    def build_update(
        self,
        values: Mapping[SrcTableID, RawValueT | None],
    ) -> CatalogOwnedRowUpdate[ValueT]:
        """
        Build an immutable normalized owned-row update.

        Example:
            Exercise CatalogOwnedRowOneToOneWriter.build update through its owning regression module::

                python -m pytest -q tests/catalog/test_owned_row_writer.py


        :param values: Values to normalize, compare or write in stable order.
        :return: The deterministic value, row, identity or collection described above.
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

        Example:
            Exercise CatalogOwnedRowOneToOneWriter.build one update through its owning regression module::

                python -m pytest -q tests/catalog/test_owned_row_writer.py


        :param src_id: Value supplied for src id under the catalog contract.
        :param dst_value: Value supplied for dst value under the catalog contract.
        :param kwargs: Value supplied for kwargs under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
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

        Example:
            Exercise CatalogOwnedRowOneToOneWriter.apply update through its owning regression module::

                python -m pytest -q tests/catalog/test_owned_row_writer.py


        :param update: Prepared catalog update to validate or apply.
        :return: The deterministic value, row, identity or collection described above.
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
