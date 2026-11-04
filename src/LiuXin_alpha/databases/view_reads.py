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
    ) -> PhysicalTableSpec: ...

    def _coerce_db_value(self, value: Any, declared_type: Any) -> Any: ...


def read_view_row(
    driver: ViewDriver,
    view: str,
    row_id: int,
    *,
    connection: QueryConnection | None = None,
) -> dict[str, Any] | None:
    """Read a view exposing id; close only cursors and connections we own."""
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
