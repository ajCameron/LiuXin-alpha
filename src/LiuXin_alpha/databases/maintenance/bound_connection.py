"""Expose a handler's borrowed transaction without transaction/lifetime controls."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from typing import Any, Never, Self

from LiuXin_alpha.databases.db_types.driver_types import (
    SQLConnection,
    SQLCursor,
    SQLDescription,
    SQLParameters,
)
from LiuXin_alpha.databases.transactions import transaction_active
from LiuXin_alpha.databases.sql_statements import sqlite_row_operation as sqlite_row_operation
from LiuXin_alpha.databases.sql_statements import (
    has_multiple_statements,
    postgres_standard_strings,
    statement_keyword as statement_keyword,
)


class MaintenanceTransactionError(RuntimeError):
    """A maintenance handler attempted to escape its supplied write unit."""


def reject_sqlite_replace(sql: str) -> None:
    """Require explicit deletion or UPSERT instead of SQLite's implicit deletes."""
    if sqlite_row_operation(sql)[1]:
        raise MaintenanceTransactionError(
            "Maintenance capture does not support REPLACE conflict handling; "
            "use ON CONFLICT DO UPDATE or explicit DELETE and INSERT in a transaction"
        )


def require_single_statement(
    sql: str, *, postgres: bool = False, standard_conforming_strings: bool = True
) -> None:
    """Reject statement chaining using the engine's comment and quote rules."""
    if has_multiple_statements(sql, postgres=postgres, standard_conforming_strings=standard_conforming_strings):
        raise MaintenanceTransactionError(
            "Maintenance handlers require one row statement per execute"
        )


class BoundConnection:
    """Permit row SQL only while the handler's originating transaction is active.

    This is an API boundary for trusted Python plugins, not a sandbox against
    arbitrary Python reflection or external side effects. Retaining it beyond
    the handler invocation is rejected.
    """

    def __init__(self, connection: SQLConnection, *, postgres: bool = False) -> None:
        """Borrow an existing connection without opening or owning a transaction."""
        self._connection = connection
        self._postgres = postgres
        self._active = True

    def invalidate(self) -> None:
        """End the handler's permission to use this borrowed view."""
        self._active = False

    def _check(self, sql: str | None = None) -> None:
        if not self._active or not transaction_active(self._connection):
            raise MaintenanceTransactionError(
                "Maintenance transaction is no longer active"
            )
        standard = postgres_standard_strings(self._connection) if self._postgres else True
        if sql is not None and statement_keyword(sql, postgres=self._postgres, standard_conforming_strings=standard) not in {
            "SELECT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "WITH",
        }:
            raise MaintenanceTransactionError(
                "Maintenance handlers may execute row SQL only"
            )
        if sql is not None:
            require_single_statement(sql, postgres=self._postgres, standard_conforming_strings=standard)
            # This portable-SQL prohibition also rejects unsupported SQLite
            # conflict syntax before it can reach a PostgreSQL handler cursor.
            reject_sqlite_replace(sql)

    def execute(self, sql: str, values: SQLParameters = ()) -> BoundCursor:
        """Execute row SQL on the borrowed connection and retain its result cursor."""
        self._check(sql)
        return BoundCursor(self, self._connection.execute(sql, values))

    def executemany(self, sql: str, values: Iterable[SQLParameters]) -> BoundCursor:
        """Execute one bounded caller-supplied batch on the same connection."""
        self._check(sql)
        return BoundCursor(self, self._connection.executemany(sql, values))

    def cursor(self) -> BoundCursor:
        """Create a cursor with the same lifetime and transaction restrictions."""
        self._check()
        return BoundCursor(self, self._connection.cursor())

    def commit(self) -> Never:
        """Refuse committing a transaction owned by the write operation."""
        raise MaintenanceTransactionError("Maintenance handlers cannot commit")

    def rollback(self) -> Never:
        """Refuse discarding work owned by the write operation."""
        raise MaintenanceTransactionError("Maintenance handlers cannot roll back")

    def close(self) -> Never:
        """Refuse closing a connection owned by the write operation."""
        raise MaintenanceTransactionError(
            "Maintenance handlers cannot close the connection"
        )


class BoundCursor:
    """Keep cursor execution and fetching inside the handler's borrowed scope."""

    def __init__(self, connection: BoundConnection, cursor: SQLCursor) -> None:
        """Retain the borrowed owner and its cursor for this invocation only."""
        self._owner = connection
        self._cursor = cursor

    @property
    def description(self) -> SQLDescription | None:
        """Return the current result's DB-API column metadata."""
        return self._cursor.description

    @property
    def lastrowid(self) -> int | None:
        """Return the backend's last inserted identity when available."""
        return self._cursor.lastrowid

    @property
    def rowcount(self) -> int:
        """Return the backend's affected-row count."""
        return self._cursor.rowcount

    def execute(self, sql: str, values: SQLParameters = ()) -> Self:
        """Execute one row statement while the borrowed scope remains valid."""
        self._owner._check(sql)
        self._cursor.execute(sql, values)
        return self

    def executemany(self, sql: str, values: Iterable[SQLParameters]) -> Self:
        """Execute a row batch without changing transaction ownership."""
        self._owner._check(sql)
        self._cursor.executemany(sql, values)
        return self

    def fetchone(self) -> Sequence[Any] | None:
        """Fetch one row inside the originating handler invocation."""
        self._owner._check()
        return self._cursor.fetchone()

    def fetchmany(self, size: int = 1) -> list[Sequence[Any]]:
        """Fetch at most size rows inside the originating invocation."""
        self._owner._check()
        return self._cursor.fetchmany(size)

    def fetchall(self) -> list[Sequence[Any]]:
        """Fetch remaining rows while the borrowed scope is active."""
        self._owner._check()
        return self._cursor.fetchall()

    def close(self) -> None:
        """Close this cursor only, preserving the originating connection."""
        self._cursor.close()

    def __iter__(self) -> Iterator[Sequence[Any]]:
        """Yield rows without permitting iteration after the handler returns."""
        while (row := self.fetchone()) is not None:
            yield row
