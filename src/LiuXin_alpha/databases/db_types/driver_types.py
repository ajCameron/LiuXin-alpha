"""Portable query result contracts shared by drivers and their wrappers.

SQL cells are backend values, so their types remain Any. Containers, missing-row
sentinels and connection ownership are described explicitly at the boundary.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Protocol, Self

type SQLParameters = Sequence[Any] | Mapping[str, Any]
type SQLRow = Sequence[Any] | Mapping[str, Any]
type SQLDescription = Sequence[SQLDescriptionColumn]


class SQLDescriptionColumn(Protocol):
    """Index DB-API column facts, including native psycopg2 Column objects.

    The native extension exposes indexed fields without implementing the full
    collections.abc.Sequence interface. SQLite's description tuples also fit.
    """

    def __getitem__(self, index: int, /) -> Any: ...


class SQLCursor(Protocol):
    """Execute physical SQL using the backend's default positional row factory.

    Column values and DB-API description fields depend on the backend. Row and
    cursor operations do not. Unlike QueryCursor, this internal contract requires
    positional rows, supports subsequent execution and exposes affected-row facts.
    """

    @property
    def description(self) -> SQLDescription | None: ...

    @property
    def lastrowid(self) -> int | None: ...

    @property
    def rowcount(self) -> int: ...

    def execute(self, sql: str, parameters: SQLParameters = (), /) -> SQLCursor: ...

    def executemany(
        self, sql: str, parameters: Iterable[SQLParameters], /
    ) -> SQLCursor: ...

    def fetchone(self) -> Sequence[Any] | None: ...

    def fetchmany(self, size: int = 1, /) -> list[Sequence[Any]]: ...

    def fetchall(self) -> list[Sequence[Any]]: ...

    def close(self) -> None: ...

    def __iter__(self) -> Iterator[Sequence[Any]]: ...


class RowSQLConnection(Protocol):
    """Borrow row operations without acquiring transaction or lifetime controls."""

    def execute(self, sql: str, parameters: SQLParameters = (), /) -> SQLCursor: ...

    def executemany(
        self, sql: str, parameters: Iterable[SQLParameters], /
    ) -> SQLCursor: ...

    def cursor(self) -> SQLCursor: ...


class SQLConnection(RowSQLConnection, Protocol):
    """An owned physical connection with its default positional row factory."""

    def executescript(self, sql: str, /) -> object: ...

    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
        /,
    ) -> bool | None: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class SQLiteFunctionConnection(SQLConnection, Protocol):
    """SQLite capabilities used when installing transaction-bound SQL callbacks."""

    @property
    def isolation_level(self) -> str | None: ...

    def create_function(
        self,
        name: str,
        narg: int,
        func: Callable[..., str | bytes | int | float | None],
    ) -> None: ...


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
    def fetchall(self) -> Sequence[SQLRow]: ...

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


class SQLExecuteOperation(Protocol):
    """A bound wrapper executor, including its optional scalar binding shorthand."""

    def __call__(
        self, sql: str, values: SQLParameters | int | None = None
    ) -> QueryCursor: ...


class SQLExecuteManyOperation(Protocol):
    """A bound wrapper batch executor; None retains the legacy script operation."""

    def __call__(
        self, sql: str, values: Iterable[SQLParameters] | None = None
    ) -> None: ...


class SQLGetOperation(Protocol):
    """A bound wrapper read: all rows, or one complete row when all is False."""

    def __call__(
        self, sql: str, values: SQLParameters | int | None = None, *, all: bool = True
    ) -> list[SQLRow] | SQLRow | None: ...
