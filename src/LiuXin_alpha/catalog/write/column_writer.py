"""
Build and apply validated direct-column catalog updates.

The module keeps validation, normalization and host mutation boundaries explicit.

Example:
    Exercise column writer through its owning regression module::

        python -m pytest -q tests/catalog/test_writer_factory.py
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

    Example:
        Exercise CatalogColumnWriter through its owning regression module::

            python -m pytest -q tests/catalog/test_writer_factory.py
    """

    def __init__(
        self,
        catalog: CatalogWriterHostAPI,
        table_spec: StorageTableSpec,
        column_spec: StorageColumnSpec,
    ) -> None:
        """
        Validate and store the column-writer configuration.

        Example:
            Exercise CatalogColumnWriter.init through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param catalog: Catalog host or facade supplying metadata and mutation services.
        :param table_spec: Value supplied for table spec under the catalog contract.
        :param column_spec: Value supplied for column spec under the catalog contract.
        :return: None; the function records state or raises through its assertions.
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

        Example:
            Exercise CatalogColumnWriter.table spec through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return self._table_spec

    @property
    def column_spec(self) -> StorageColumnSpec:
        """
        Return the destination-column specification.

        Example:
            Exercise CatalogColumnWriter.column spec through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return self._column_spec

    def adapt(self, raw_value: RawValueT) -> ValueT:
        """
        Preserve one raw value by default.

        Example:
            Exercise CatalogColumnWriter.adapt through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param raw_value: Value supplied for raw value under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        return cast(ValueT, raw_value)

    def build_update(
        self,
        values: Mapping[SrcTableID, RawValueT],
    ) -> CatalogColumnUpdate[ValueT]:
        """
        Build an immutable normalized column update.

        Example:
            Exercise CatalogColumnWriter.build update through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param values: Values to normalize, compare or write in stable order.
        :return: The deterministic value, row, identity or collection described above.
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

        Example:
            Exercise CatalogColumnWriter.build one update through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param src_id: Value supplied for src id under the catalog contract.
        :param dst_value: Value supplied for dst value under the catalog contract.
        :param kwargs: Value supplied for kwargs under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
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

        Example:
            Exercise CatalogColumnWriter.apply update through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param update: Prepared catalog update to validate or apply.
        :return: The deterministic value, row, identity or collection described above.
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
