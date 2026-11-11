"""Portable query result contracts shared by drivers and their wrappers.

SQL cells are backend values, so their types remain Any. Containers, missing-row
sentinels and connection ownership are described explicitly at the boundary.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

type SQLParameters = Sequence[Any] | Mapping[str, Any]
type SQLRow = Sequence[Any] | Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class RowPage:
    """A bounded batch and the last visited ID for continuation/checkpointing.

    complete describes the lookahead at the time of this query. Later inserts
    can add results. Empty pages retain the supplied continuation ID.
    """

    rows: tuple[dict[str, Any], ...]
    next_after_id: int | None
    complete: bool


class QueryCursor(Protocol):
    """Read an executed query without assuming a particular database backend."""

    @abstractmethod
    def fetchone(self) -> SQLRow | None: ...

    @abstractmethod
    def fetchall(self) -> list[SQLRow]: ...

    @abstractmethod
    def close(self) -> None: ...

    @abstractmethod
    def __iter__(self) -> Iterator[SQLRow]: ...


class QueryConnection(Protocol):
    """The SQL execution/lifetime subset exposed by a driver wrapper."""

    @abstractmethod
    def execute(self, sql: str, parameters: SQLParameters = (), /) -> QueryCursor: ...

    @abstractmethod
    def executemany(
        self, sql: str, parameters: Iterable[SQLParameters], /
    ) -> QueryCursor: ...

    @abstractmethod
    def commit(self) -> None: ...

    @abstractmethod
    def rollback(self) -> None: ...

    @abstractmethod
    def close(self) -> None: ...
