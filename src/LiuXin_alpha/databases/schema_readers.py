"""Small resource-safe building blocks for native schema introspection."""

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any, Protocol

from LiuXin_alpha.databases.db_types.driver_types import QueryConnection, SQLParameters


class SchemaDriver(Protocol):
    """Connection owner used by schema readers; borrowed handles stay open."""

    @property
    def conn(self) -> QueryConnection | None:
        """
        Expose the primary connection available for borrowing.

        Example:
            >>> primary = driver.conn  # doctest: +SKIP


        :return: The driver-owned handle, or None when a fresh handle is needed.
        """
        ...

    def get_connection(self) -> QueryConnection:
        """
        Open a separate connection for caller-owned schema work.

        Example:
            >>> connection = driver.get_connection()  # doctest: +SKIP


        :return: A new handle that the caller must close.
        """
        ...


@contextmanager
def schema_connection(
    driver: SchemaDriver, supplied: QueryConnection | None
) -> Iterator[QueryConnection]:
    """
    Borrow an explicit or primary handle, otherwise own a fresh schema handle.

    Selection is supplied, driver.conn, then driver.get_connection(). Only the
    freshly opened handle is closed, including on failure. No commit is issued;
    this helper does not establish a consistent snapshot by itself.

    Example:
        >>> with schema_connection(driver, supplied) as connection:  # doctest: +SKIP
        ...     tables = rows(connection, "SELECT name FROM sqlite_schema")


    :param driver: Owner exposing a primary handle and a separate-connection
        factory.
    :param supplied: Explicit borrowed connection, or None to use the driver
        selection.
    :return: A context yielding the selected connection.
    """
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
    """
    Materialize positional result rows and close the temporary cursor.

    Mapping rows are converted in mapping insertion order, which must match the
    SELECT projection. Connection lifetime and transaction completion stay external.

    Example:
        >>> result = rows(connection, "SELECT ?", (7,))  # doctest: +SKIP


    :param connection: Borrowed handle used for execution.
    :param sql: Trusted SQL with the backend-supported binding syntax.
    :param parameters: Positional or named bindings; empty means execute without
        bindings.
    :return: A list of positional rows, empty when the query has no results.
    """
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
    """
    Quote one SQL identifier, treating dots as literal name characters.

    Quote schema and relation names separately when constructing a qualified name.

    Example:
        >>> quote_identifier('a"b')
        '"a""b"'
        >>> quote_identifier("schema.table")
        '"schema.table"'


    :param name: Nonempty identifier without a NUL character.
    :return: The identifier enclosed in double quotes with internal quotes
        doubled.
    :raises ValueError: name is empty, non-string or contains NUL.
    """
    if not isinstance(name, str) or not name or "\0" in name:
        raise ValueError("Expected a nonempty SQL identifier without NUL")
    return '"' + name.replace('"', '""') + '"'
