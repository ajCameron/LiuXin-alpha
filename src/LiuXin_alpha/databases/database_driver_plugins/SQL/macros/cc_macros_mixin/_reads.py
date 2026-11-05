"""Adapt legacy scalar/row-list reads to ordinary borrowed query connections.

Project connections may supply a get extension. Keep that extension authoritative;
otherwise use DB-API cursors and close them without committing or closing the handle.
"""

from collections.abc import Mapping, Sequence
from contextlib import closing
from typing import Any, Literal, Protocol, overload

from LiuXin_alpha.databases.db_types.driver_types import QueryConnection, SQLParameters
from LiuXin_alpha.databases.schema_readers import rows


class _LegacyGet(Protocol):
    @overload
    def __call__(
        self, sql: str, parameters: SQLParameters = (), *, all: Literal[True] = True
    ) -> list[Sequence[Any]]: ...

    @overload
    def __call__(
        self, sql: str, parameters: SQLParameters = (), *, all: Literal[False]
    ) -> Any: ...


def read_rows(
    connection: QueryConnection, sql: str, parameters: SQLParameters = ()
) -> list[Sequence[Any]]:
    """Keep row-list results and the legacy get hook, with a native cursor fallback."""
    getter: _LegacyGet | None = getattr(connection, "get", None)
    if callable(getter):
        return getter(sql, parameters) if parameters else getter(sql)
    return rows(connection, sql, parameters)


def read_scalar(
    connection: QueryConnection, sql: str, parameters: SQLParameters = ()
) -> Any:
    """Return the first cell or None; SQL NULL and missing rows remain indistinct."""
    getter: _LegacyGet | None = getattr(connection, "get", None)
    if callable(getter):
        return getter(sql, parameters, all=False)
    with closing(connection.execute(sql, parameters)) as cursor:
        row = cursor.fetchone()
        if row is None:
            return None
        if isinstance(row, Mapping):
            return next(iter(row.values()), None)
        return row[0] if row else None
