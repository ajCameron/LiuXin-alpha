"""Read one compatibility-view row with bounded results and explicit ownership."""

from typing import Any, Protocol

from LiuXin_alpha.databases.db_types.driver_types import QueryConnection
from LiuXin_alpha.databases.schema_readers import (
    SchemaDriver,
    quote_identifier,
    rows,
    schema_connection,
)
from LiuXin_alpha.databases.schema_specs import PhysicalTableSpec, RelationKind
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError


class ViewDriver(SchemaDriver, Protocol):
    """Driver facilities needed to inspect and convert a view on one handle."""

    def direct_get_physical_table_spec(
        self, table: str, *, connection: QueryConnection | None = None
    ) -> PhysicalTableSpec:
        """
        Inspect a relation through the supplied handle when one is provided.

        Example:
            >>> spec = driver.direct_get_physical_table_spec(view, connection=connection)  # doctest: +SKIP


        :param table: Relation name to resolve in the selected database/schema.
        :param connection: Borrowed introspection handle, or None for the driver
            default.
        :return: Physical relation metadata, including ordered columns and relation
            kind.
        """
        ...

    def _coerce_db_value(self, value: Any, declared_type: Any) -> Any:
        """
        Decode a stored cell using the driver declared-type rules.

        Example:
            >>> value = driver._coerce_db_value(cell, column.declared_type)  # doctest: +SKIP


        :param value: Raw SQL cell to decode.
        :param declared_type: Backend type declaration for that column, when
            available.
        :return: The driver-decoded value.
        """
        ...


def read_view_row(
    driver: ViewDriver,
    view: str,
    row_id: int,
    *,
    connection: QueryConnection | None = None,
) -> dict[str, Any] | None:
    """
    Read at most one matching row from a view exposing an id column.

    Resolve view metadata and read on the same connection, fetching at most two
    rows to detect duplicate IDs. Decode cells using their declared column types.
    Close temporary cursors and only a freshly opened connection; borrowed
    handles and their transactions remain with the caller.

    Example:
        >>> row = read_view_row(driver, "books", 7, connection=connection)  # doctest: +SKIP


    :param driver: Backend providing relation metadata, decoding and connection
        selection.
    :param view: View name resolved by the driver; it must expose a column named
        id.
    :param row_id: ID matched by equality.
    :param connection: Borrowed handle, or None to prefer the primary then a
        fresh connection.
    :return: A column-name/value dictionary, or None when no row matches.
    :raises InputIntegrityError: The relation is absent, is not a view, or lacks
        id.
    :raises DatabaseIntegrityError: More than one view row has the requested ID.
    """
    with schema_connection(driver, connection) as active:
        try:
            spec = driver.direct_get_physical_table_spec(view, connection=active)
        except ValueError as error:
            raise InputIntegrityError(f"view {view!r} not found") from error
        if spec.relation_kind != RelationKind.VIEW or "id" not in {
            col.name for col in spec.columns
        }:
            raise InputIntegrityError(f"view {view!r} must expose an id column")
        table_sql = quote_identifier(spec.name)
        if spec.schema is not None:
            table_sql = quote_identifier(spec.schema) + "." + table_sql
        found = rows(
            active, f'SELECT * FROM {table_sql} WHERE "id"=? LIMIT 2', (row_id,)
        )
        if len(found) > 1:
            raise DatabaseIntegrityError(
                f"Search yielded multiple rows for view {view!r}"
            )
        if not found:
            return None
        return {
            col.name: driver._coerce_db_value(value, col.declared_type)
            for col, value in zip(spec.columns, found[0], strict=True)
        }
