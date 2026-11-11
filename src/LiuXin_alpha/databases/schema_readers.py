"""Small resource-safe building blocks for native schema introspection."""

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any, Protocol

from LiuXin_alpha.databases.db_types.driver_types import QueryConnection, SQLParameters


class SchemaDriver(Protocol):
    """Connection owner used by schema readers; borrowed handles stay open."""

    conn: QueryConnection | None

    def get_connection(self) -> QueryConnection: ...


@contextmanager
def schema_connection(
    driver: SchemaDriver, supplied: QueryConnection | None
) -> Iterator[QueryConnection]:
    """Prefer an explicit handle, then the primary; close only a fresh handle."""
    connection = supplied if supplied is not None else driver.conn
    owned = connection is None
    if connection is None:
        connection = driver.get_connection()
    try:
        yield connection
    finally:
        if owned:
            connection.close()


def rows(
    connection: QueryConnection, sql: str, parameters: SQLParameters = ()
) -> list[Sequence[Any]]:
    """Read positional metadata rows and close the cursor even on failure."""
    cursor = (
        connection.execute(sql, parameters) if parameters else connection.execute(sql)
    )
    try:
        # Native metadata readers use positional driver cursors. Mapping cursors
        # are accepted too, retaining the SELECT projection's insertion order.
        from collections.abc import Mapping

        return [
            tuple(row.values()) if isinstance(row, Mapping) else row for row in cursor
        ]
    finally:
        cursor.close()


def quote_identifier(name: str) -> str:
    """Quote one complete identifier; qualification is a separate operation."""
    if not isinstance(name, str) or not name or "\0" in name:
        raise ValueError("Expected a nonempty SQL identifier without NUL")
    return '"' + name.replace('"', '""') + '"'
