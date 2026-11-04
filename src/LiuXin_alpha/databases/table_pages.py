"""Shared bounded ID pagination used by portable macros and driver wrappers."""

from collections.abc import Mapping, Sequence
from typing import Any

from LiuXin_alpha.databases.db_types.driver_types import QueryConnection
from LiuXin_alpha.databases.schema_readers import quote_identifier
from LiuXin_alpha.errors import InputIntegrityError


def read_page(
    connection: QueryConnection,
    *,
    table_sql: str,
    columns: Sequence[str],
    id_column: str,
    where: Mapping[str, Any] | None,
    limit: int,
    offset: int = 0,
    after_id: int | None = None,
) -> tuple[tuple[dict[str, Any], ...], bool]:
    """Read at most limit+1 rows using validated physical names and bound data.

    The caller validates schema membership and supplies quoted/qualified table
    SQL. This function owns only its cursor, including on errors and empty reads.
    """
    for name, value in (("limit", limit), ("offset", offset)):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise InputIntegrityError(f"{name} must be a nonnegative integer")
    if after_id is not None:
        if isinstance(after_id, bool) or not isinstance(after_id, int):
            raise InputIntegrityError("after_id must be an integer")
        if offset:
            raise InputIntegrityError("after_id cannot be combined with offset")
    conditions: list[str] = []
    values: list[Any] = []
    if after_id is not None:
        conditions.append(f"{quote_identifier(id_column)} > ?")
        values.append(after_id)
    for column, value in (where or {}).items():
        conditions.append(
            f"{quote_identifier(column)} IS NULL"
            if value is None
            else f"{quote_identifier(column)} = ?"
        )
        if value is not None:
            values.append(value)
    sql = f"SELECT {', '.join(quote_identifier(column) for column in columns)} FROM {table_sql}"
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += f" ORDER BY {quote_identifier(id_column)} LIMIT ? OFFSET ?"
    cursor = connection.execute(sql, (*values, limit + 1, offset))
    try:
        result = tuple(
            dict(row)
            if isinstance(row, Mapping)
            else dict(zip(columns, row, strict=True))
            for row in cursor
        )
    finally:
        cursor.close()
    return result[:limit], len(result) <= limit
