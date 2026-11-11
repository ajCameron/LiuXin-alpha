"""Detach SQLite write results so their statement finishes before commit."""

import sqlite3
from collections import deque
from collections.abc import Iterator, Sequence

from LiuXin_alpha.databases.db_types.driver_types import SQLCursor, SQLRow


class WriteResult:
    """Read a completed write's rows without retaining a live SQL statement.

    Only result-producing writes are buffered. Ordinary reads keep their native
    streaming cursor. Closing discards unread rows; writes are already committed
    when the driver returns this result. Subsequent SQL execution is deliberately
    outside this read-only query-cursor contract.
    """

    def __init__(self, cursor: SQLCursor) -> None:
        self.description = cursor.description
        self.lastrowid = cursor.lastrowid
        self._rows: deque[SQLRow] = deque(cursor.fetchall())
        self.rowcount = cursor.rowcount
        self.arraysize = 1
        self._closed = False

    def fetchone(self) -> SQLRow | None:
        if self._closed:
            raise sqlite3.ProgrammingError("Cannot operate on a closed cursor")
        return self._rows.popleft() if self._rows else None

    def fetchmany(self, size: int | None = None) -> list[SQLRow]:
        count = self.arraysize if size is None else size
        if count < 0:
            count = len(self._rows)
        result = []
        for _ in range(count):
            row = self.fetchone()
            if row is None:
                break
            result.append(row)
        return result

    def fetchall(self) -> Sequence[SQLRow]:
        return list(self)

    def __iter__(self) -> Iterator[SQLRow]:
        while (row := self.fetchone()) is not None:
            yield row

    def close(self) -> None:
        self._rows.clear()
        self._closed = True
