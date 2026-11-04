"""Expose a handler's borrowed transaction without transaction/lifetime controls."""

from __future__ import annotations

import re
from typing import Any

from LiuXin_alpha.databases.transactions import transaction_active


class MaintenanceTransactionError(RuntimeError):
    """A maintenance handler attempted to escape its supplied write unit."""


# Todo: Move this over to utils?
def statement_keyword(sql: str) -> str:
    """
    Find the first keyword after whitespace and ordinary SQL comments.

    :param sql:
    :return:
    """
    text = str(sql)
    while True:
        text = text.lstrip(" \t\r\n;")
        if text.startswith("--"):
            _, _, text = text.partition("\n")
        elif text.startswith("/*"):
            _, found, text = text.partition("*/")
            if not found:
                return ""
        else:
            match = re.match(r"[A-Za-z]+", text)
            return "" if match is None else match[0].upper()


def sqlite_row_operation(sql: str) -> tuple[str, bool]:
    """Identify a SQLite row operation, including CTEs and replacement conflicts.

    Ignore quoted values/identifiers and comments. CTE bodies are parenthesized;
    their SELECTs do not identify the outer operation. This only classifies row
    SQL for transaction ownership, rather than validating arbitrary SQL syntax.
    """
    tokens = re.finditer(
        r"--[^\n]*|/\*.*?\*/|'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\""
        r"|`(?:``|[^`])*`|\[[^\]]*\]|[A-Za-z_][A-Za-z_0-9]*|[()]",
        str(sql),
        flags=re.S,
    )
    depth, words = 0, []
    for match in tokens:
        token = match[0]
        if token == "(":
            depth += 1
        elif token == ")":
            depth -= 1
        elif depth == 0 and token[0].isalpha():
            words.append(token.upper())
    if not words:
        return "", False
    operation = words[0]
    if operation == "WITH":
        for index, word in enumerate(words[1:], 1):
            if word in {"SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE"}:
                words = words[index:]
                operation = word
                break
    replacing = operation == "REPLACE" or (
        operation in {"INSERT", "UPDATE"} and words[1:3] == ["OR", "REPLACE"]
    )
    return operation, replacing


def reject_sqlite_replace(sql: str) -> None:
    """Require explicit deletion or UPSERT instead of SQLite's implicit deletes."""
    if sqlite_row_operation(sql)[1]:
        raise MaintenanceTransactionError(
            "Maintenance capture does not support REPLACE conflict handling; "
            "use ON CONFLICT DO UPDATE or explicit DELETE and INSERT in a transaction"
        )


def require_single_statement(sql: str) -> None:
    """Reject statement chaining, ignoring ordinary quoted values and comments."""
    # PostgreSQL accepts multiple commands in one execute; SQLite already refuses
    # them. Reject dollar quotes in this deliberately small trusted-handler API.
    text = re.sub(
        r"--[^\n]*|/\*.*?\*/|'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"",
        " ",
        str(sql),
        flags=re.S,
    )
    if "$" in text or ";" in text.rstrip().rstrip(";"):
        raise MaintenanceTransactionError(
            "Maintenance handlers require one row statement per execute"
        )


class BoundConnection:
    """Permit row SQL only while the handler's originating transaction is active.

    This is an API boundary for trusted Python plugins, not a sandbox against
    arbitrary Python reflection or external side effects. Retaining it beyond
    the handler invocation is rejected.
    """

    def __init__(self, connection: Any):
        """Borrow an existing connection without opening or owning a transaction."""
        self._connection = connection
        self._active = True

    def invalidate(self) -> None:
        """End the handler's permission to use this borrowed view."""
        self._active = False

    def _check(self, sql: str | None = None) -> None:
        if not self._active or not transaction_active(self._connection):
            raise MaintenanceTransactionError(
                "Maintenance transaction is no longer active"
            )
        if sql is not None and statement_keyword(sql) not in {
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
            require_single_statement(sql)
            reject_sqlite_replace(sql)

    def execute(self, sql: str, values=()):
        """Execute row SQL on the borrowed connection and retain its result cursor."""
        self._check(sql)
        return BoundCursor(self, self._connection.execute(sql, values))

    def executemany(self, sql: str, values):
        """Execute one bounded caller-supplied batch on the same connection."""
        self._check(sql)
        return BoundCursor(self, self._connection.executemany(sql, values))

    def cursor(self):
        """Create a cursor with the same lifetime and transaction restrictions."""
        self._check()
        return BoundCursor(self, self._connection.cursor())

    def commit(self):
        """Refuse committing a transaction owned by the write operation."""
        raise MaintenanceTransactionError("Maintenance handlers cannot commit")

    def rollback(self):
        """Refuse discarding work owned by the write operation."""
        raise MaintenanceTransactionError("Maintenance handlers cannot roll back")

    def close(self):
        """Refuse closing a connection owned by the write operation."""
        raise MaintenanceTransactionError(
            "Maintenance handlers cannot close the connection"
        )


class BoundCursor:
    """Keep cursor execution and fetching inside the handler's borrowed scope."""

    def __init__(self, connection: BoundConnection, cursor: Any):
        """Retain the borrowed owner and its cursor for this invocation only."""
        self._owner = connection
        self._cursor = cursor

    @property
    def description(self):
        """Return the current result's DB-API column metadata."""
        return self._cursor.description

    @property
    def lastrowid(self):
        """Return the backend's last inserted identity when available."""
        return self._cursor.lastrowid

    @property
    def rowcount(self):
        """Return the backend's affected-row count."""
        return self._cursor.rowcount

    def execute(self, sql, values=()):
        """Execute one row statement while the borrowed scope remains valid."""
        self._owner._check(sql)
        self._cursor.execute(sql, values)
        return self

    def executemany(self, sql, values):
        """Execute a row batch without changing transaction ownership."""
        self._owner._check(sql)
        self._cursor.executemany(sql, values)
        return self

    def fetchone(self):
        """Fetch one row inside the originating handler invocation."""
        self._owner._check()
        return self._cursor.fetchone()

    def fetchmany(self, size=1):
        """Fetch at most size rows inside the originating invocation."""
        self._owner._check()
        return self._cursor.fetchmany(size)

    def fetchall(self):
        """Fetch remaining rows while the borrowed scope is active."""
        self._owner._check()
        return self._cursor.fetchall()

    def close(self):
        """Close this cursor only, preserving the originating connection."""
        self._cursor.close()

    def __iter__(self):
        """Yield rows without permitting iteration after the handler returns."""
        while (row := self.fetchone()) is not None:
            yield row
