"""
Select a concrete catalog writer from field and destination metadata.

The module keeps validation, normalization and host mutation boundaries explicit.

Example:
    Exercise factory through its owning regression module::

        python -m pytest -q tests/catalog/test_writer_factory.py
"""

from __future__ import annotations

from LiuXin_alpha.catalog.write.column_writer import CatalogColumnWriter
from LiuXin_alpha.catalog.write.host_api import CatalogWriterHostAPI
from LiuXin_alpha.catalog.write.owned_row_writer import (
    CatalogOwnedRowOneToOneWriter,
)
from LiuXin_alpha.catalog.write.table_value_link_writer import (
    CatalogTableValueLinkWriter,
)
from LiuXin_alpha.databases.schema_specs import (
    LinkCardinality,
    RelationKind,
    StorageColumnSpec,
    StorageSchemaSpec,
    StorageTableSpec,
)

type SchemaCatalogWriter = (
    CatalogColumnWriter[object, object]
    | CatalogOwnedRowOneToOneWriter[object, object]
    | CatalogTableValueLinkWriter
)


def _column_from(
    table_spec: StorageTableSpec,
    column_name: str,
) -> StorageColumnSpec | None:
    """
    Resolve the writable catalog column described by field metadata.

    Example:
        Exercise column from through its owning regression module::

            python -m pytest -q tests/catalog/test_writer_factory.py


    :param table_spec: Value supplied for table spec under the catalog contract.
    :param column_name: Value supplied for column name under the catalog contract.
    :return: The deterministic value, row, identity or collection described above.
    """
    return next(
        (
            column
            for column in table_spec.columns
            if column.name == column_name
        ),
        None,
    )


def _writable_destination_tables(
    schema: StorageSchemaSpec,
    column_name: str,
) -> tuple[tuple[StorageTableSpec, StorageColumnSpec], ...]:
    """
    Return destination tables eligible for catalog writer construction.

    Example:
        Exercise writable destination tables through its owning regression module::

            python -m pytest -q tests/catalog/test_writer_factory.py


    :param schema: Value supplied for schema under the catalog contract.
    :param column_name: Value supplied for column name under the catalog contract.
    :return: The deterministic value, row, identity or collection described above.
    """
    return tuple(
        (table_spec, column)
        for table_spec in schema.tables.values()
        if table_spec.relation_kind is RelationKind.TABLE
        and not table_spec.is_link_table
        and not table_spec.is_intralink_table
        and (column := _column_from(table_spec, column_name)) is not None
    )


def create_catalog_writer(
    catalog: CatalogWriterHostAPI,
    src_table: str,
    dst_column: str,
    *,
    force_refresh: bool = False,
    destination_owned: bool | None = None,
) -> SchemaCatalogWriter:
    """
    Create a schema-backed writer from source table and destination column.

    Example:
        Exercise create catalog writer through its owning regression module::

            python -m pytest -q tests/catalog/test_writer_factory.py


    :param catalog: Catalog host or facade supplying metadata and mutation services.
    :param src_table: Value supplied for src table under the catalog contract.
    :param dst_column: Value supplied for dst column under the catalog contract.
    :param force_refresh: Value supplied for force refresh under the catalog contract.
    :param destination_owned: Value supplied for destination owned under the catalog
        contract.
    :return: The deterministic value, row, identity or collection described above.
    """

    if not isinstance(src_table, str) or not src_table:
        raise TypeError("src_table must be a non-empty string")
    if not isinstance(dst_column, str) or not dst_column:
        raise TypeError("dst_column must be a non-empty string")
    if destination_owned is not None and not isinstance(destination_owned, bool):
        raise TypeError("destination_owned must be a bool or None")

    database = getattr(catalog, "db", None)
    wrapper = getattr(database, "driver_wrapper", None)
    get_schema_spec = getattr(wrapper, "get_schema_spec", None)
    get_link_spec = getattr(wrapper, "get_link_spec", None)
    if not callable(get_schema_spec) or not callable(get_link_spec):
        raise TypeError(
            "catalog database must provide schema and link-spec discovery"
        )

    schema = get_schema_spec(force_refresh=force_refresh)
    if not isinstance(schema, StorageSchemaSpec):
        raise TypeError("get_schema_spec must return a StorageSchemaSpec")

    try:
        source_spec = schema.tables[src_table]
    except KeyError:
        raise KeyError(f"unknown source table {src_table!r}") from None
    if (
        source_spec.relation_kind is not RelationKind.TABLE
        or source_spec.is_link_table
        or source_spec.is_intralink_table
    ):
        raise ValueError(f"source {src_table!r} is not a writable main table")

    source_column = _column_from(source_spec, dst_column)
    if source_column is not None:
        return CatalogColumnWriter(catalog, source_spec, source_column)

    candidates = _writable_destination_tables(schema, dst_column)
    if not candidates:
        raise KeyError(f"unknown destination column {dst_column!r}")
    if len(candidates) > 1:
        table_names = ", ".join(
            sorted(table_spec.name for table_spec, _column in candidates)
        )
        raise ValueError(
            f"destination column {dst_column!r} is ambiguous across tables: "
            f"{table_names}"
        )

    destination_spec, destination_column = candidates[0]
    link_spec = get_link_spec(
        source_spec.name,
        destination_spec.name,
        force_refresh=False,
    )
    if link_spec is None:
        raise ValueError(
            f"no link exists from {source_spec.name!r} to "
            f"{destination_spec.name!r}"
        )
    owned = (
        link_spec.destination_owned
        if destination_owned is None
        else destination_owned
    )
    if owned and link_spec.cardinality is not LinkCardinality.ONE_TO_ONE:
        raise ValueError("only a one-to-one destination can be source-owned")
    writer_type = (
        CatalogOwnedRowOneToOneWriter
        if owned
        else CatalogTableValueLinkWriter
    )
    return writer_type(
        catalog,
        link_spec,
        destination_spec,
        destination_column,
    )


__all__ = ["SchemaCatalogWriter", "create_catalog_writer"]
