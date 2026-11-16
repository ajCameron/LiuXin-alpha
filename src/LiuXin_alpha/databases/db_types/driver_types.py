"""
Portable query results and the SQL capabilities borrowed by database helpers.

SQL cells retain backend types. SQLCursor requires positional rows; QueryCursor
also permits mappings. Neither protocol transfers ownership by itself: code
that creates a cursor closes it, and borrowed connections remain open. Backend
transaction and script policies are not made uniform by these structural types.
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
    """
    Index DB-API column facts without requiring sequence inheritance.

    SQLite tuples and native psycopg2 Column objects both expose the column
    name at index zero; remaining fields retain backend-specific values.

    Example:
        >>> name = cursor.description[0][0]  # doctest: +SKIP
    """

    def __getitem__(self, index: int, /) -> Any:
        """
        Read one DB-API description field by position.

        Example:
            >>> name = column[0]  # doctest: +SKIP


        :param index: Zero-based description field; zero selects the column name.
        :return: The backend-supplied description value.
        """
        ...


class SQLCursor(Protocol):
    """
    Execute physical SQL and fetch positional rows from its current result.

    Execution may replace an unread result. Callers close owned cursors even
    after exhaustion; cursor cleanup does not complete the connection transaction.

    Example:
        >>> row = cursor.execute("SELECT 1").fetchone()  # doctest: +SKIP
    """

    @property
    def description(self) -> SQLDescription | None:
        """
        Describe columns in the current result, if any.

        Example:
            >>> value = cursor.description  # doctest: +SKIP


        :return: Ordered column descriptions, or None when no result columns exist.
        """
        ...

    @property
    def lastrowid(self) -> int | None:
        """
        Expose the backend insertion identity when available.

        Example:
            >>> value = cursor.lastrowid  # doctest: +SKIP


        :return: Last insertion ID or None; it is not a portable batch identity
            allocator.
        """
        ...

    @property
    def rowcount(self) -> int:
        """
        Expose the backend count for the last executed operation.

        Example:
            >>> value = cursor.rowcount  # doctest: +SKIP


        :return: Affected/result row count, or -1 when the backend cannot determine
            it.
        """
        ...

    def execute(self, sql: str, parameters: SQLParameters = (), /) -> SQLCursor:
        """
        Execute one statement with separately bound values.

        Placeholder syntax and value adaptation follow the selected backend.

        Example:
            >>> result = cursor.execute("SELECT 1")  # doctest: +SKIP


        :param sql: SQL statement in the connection dialect.
        :param parameters: Positional or named bindings; empty when the statement
            needs none.
        :return: This cursor, positioned at the new result.
        """
        ...

    def executemany(
        self, sql: str, parameters: Iterable[SQLParameters], /
    ) -> SQLCursor:
        """
        Execute one statement for each supplied parameter set.

        This call alone does not promise an atomic batch; the connection transaction
        policy determines rollback and completion.

        Example:
            >>> result = cursor.executemany(sql, batches)  # doctest: +SKIP


        :param sql: Statement reused for each set of bindings.
        :param parameters: Iterable of positional or named binding sets, consumed by
            execution.
        :return: This cursor, positioned at the new result.
        """
        ...

    def fetchone(self) -> Sequence[Any] | None:
        """
        Consume the next row from the current result.

        Example:
            >>> row = cursor.fetchone()  # doctest: +SKIP


        :return: One positional row, or None after exhaustion; SQL NULL remains a
            cell.
        """
        ...

    def fetchmany(self, size: int = 1, /) -> list[Sequence[Any]]:
        """
        Consume at most size rows from the current result.

        Example:
            >>> batch = cursor.fetchmany(100)  # doctest: +SKIP


        :param size: Requested row count; use a positive integer for portable
            behavior.
        :return: A list of positional rows, possibly shorter than size and empty at
            exhaustion.
        """
        ...

    def fetchall(self) -> list[Sequence[Any]]:
        """
        Consume all remaining rows, materializing the result in memory.

        Example:
            >>> remaining = cursor.fetchall()  # doctest: +SKIP


        :return: A sequence of positional rows, empty after exhaustion.
        """
        ...

    def close(self) -> None:
        """
        Release this cursor without committing or closing its connection.

        Example:
            >>> cursor.close()  # doctest: +SKIP


        :return: None; later cursor operations are invalid.
        """
        ...

    def __iter__(self) -> Iterator[Sequence[Any]]:
        """
        Consume remaining rows using the iterator protocol.

        Exhaustion does not transfer cursor cleanup to the iterator.

        Example:
            >>> rows = list(cursor)  # doctest: +SKIP


        :return: An iterator over the remaining positional rows.
        """
        ...


class RowSQLConnection(Protocol):
    """
    Borrow row operations without acquiring transaction or lifetime controls.

    The connection owner retains commit, rollback and close responsibilities.
    Each returned cursor belongs to the caller and needs explicit cleanup.

    Example:
        >>> cursor = connection.execute("SELECT 1")  # doctest: +SKIP
        >>> cursor.close()  # doctest: +SKIP
    """

    def execute(self, sql: str, parameters: SQLParameters = (), /) -> SQLCursor:
        """
        Execute one statement with separately bound values.

        Placeholder syntax and value adaptation follow the selected backend.

        Example:
            >>> result = connection.execute("SELECT 1")  # doctest: +SKIP


        :param sql: SQL statement in the connection dialect.
        :param parameters: Positional or named bindings; empty when the statement
            needs none.
        :return: A caller-owned cursor; close it even after consuming every row.
        """
        ...

    def executemany(
        self, sql: str, parameters: Iterable[SQLParameters], /
    ) -> SQLCursor:
        """
        Execute one statement for each supplied parameter set.

        This call alone does not promise an atomic batch; the connection transaction
        policy determines rollback and completion.

        Example:
            >>> result = connection.executemany(sql, batches)  # doctest: +SKIP


        :param sql: Statement reused for each set of bindings.
        :param parameters: Iterable of positional or named binding sets, consumed by
            execution.
        :return: A caller-owned cursor; close it even after consuming every row.
        """
        ...

    def cursor(self) -> SQLCursor:
        """
        Allocate a cursor using the default positional row factory.

        Example:
            >>> cursor = connection.cursor()  # doctest: +SKIP


        :return: A new caller-owned cursor requiring close().
        """
        ...


class SQLConnection(RowSQLConnection, Protocol):
    """
    Expose physical connection operations with positional rows by default.

    Lifetime and transaction controls are available to the owner. Borrowers must
    respect the scope that supplied the handle. Native context-manager and script
    semantics differ across backends; use transaction_scope for explicit ownership.

    Example:
        >>> connection.rollback()  # doctest: +SKIP
        >>> connection.close()  # doctest: +SKIP
    """

    def executescript(self, sql: str, /) -> object:
        """
        Execute a SQL script using the selected backend's script policy.

        Implicit commits and supported transaction-control statements differ by
        backend and installed maintenance hooks. Use managed statement operations
        when composing writes with a caller-owned transaction.

        Example:
            >>> result = connection.executescript(script)  # doctest: +SKIP


        :param sql: SQL script accepted by the selected backend.
        :return: Backend-specific result; close it if it is a cursor.
        """
        ...

    def __enter__(self) -> Self:
        """
        Enter the native connection context.

        Beginning a transaction and nesting behavior are backend-specific. Context
        exit does not substitute for explicit connection lifetime management.

        Example:
            >>> with connection as active:  # doctest: +SKIP
            ...     row = active.execute("SELECT 1").fetchone()


        :return: This connection.
        """
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
        /,
    ) -> bool | None:
        """
        Apply the backend context policy on success or exception.

        Consult the concrete adapter for transaction and cleanup behavior.

        Example:
            >>> with connection:  # doctest: +SKIP
            ...     pass


        :param exc_type: Exception class from the body, or None.
        :param exc: Exception instance from the body, or None.
        :param tb: Body traceback, or None.
        :return: The backend exception-suppression decision.
        """
        ...

    def commit(self) -> None:
        """
        Complete pending work under the backend transaction policy.

        Only the connection or transaction owner should invoke this operation.

        Example:
            >>> connection.commit()  # doctest: +SKIP


        :return: None after successful completion; installed completion hooks may
            raise.
        """
        ...

    def rollback(self) -> None:
        """
        Discard pending work under the backend transaction policy.

        Only the connection or transaction owner should invoke this operation.

        Example:
            >>> connection.rollback()  # doctest: +SKIP


        :return: None; previously committed work is unaffected.
        """
        ...

    def close(self) -> None:
        """
        Release the connection and its owned resources without requesting a commit.

        Only the connection or transaction owner should invoke this operation.

        Example:
            >>> connection.close()  # doctest: +SKIP


        :return: None after successful cleanup; the handle must no longer be used.
        """
        ...


class SQLiteFunctionConnection(SQLConnection, Protocol):
    """
    Expose SQLite registration and isolation settings for SQL callbacks.

    Example:
        >>> connection.create_function("double", 1, lambda n: n * 2)  # doctest: +SKIP
    """

    @property
    def isolation_level(self) -> str | None:
        """
        Read the configured SQLite implicit-transaction mode.

        This setting alone does not tell whether an explicit transaction is active.

        Example:
            >>> mode = connection.isolation_level  # doctest: +SKIP


        :return: Configured isolation mode, or None for disabled implicit
            transactions.
        """
        ...

    def create_function(
        self,
        name: str,
        narg: int,
        func: Callable[..., str | bytes | int | float | None],
    ) -> None:
        """
        Register a Python scalar callback on this connection.

        Example:
            >>> connection.create_function("double", 1, lambda n: n * 2)  # doctest: +SKIP


        :param name: SQL function name to install or replace.
        :param narg: Argument count, or -1 for variable arity.
        :param func: Callback returning a SQLite scalar or None for SQL NULL.
        :return: None; registration lasts for this connection.
        """
        ...


@dataclass(frozen=True, slots=True)
class RowPage:
    """
    Carry a bounded physical-row batch and its exclusive continuation ID.

    next_after_id is the last visited primary key, including when the projection
    hides that column. Empty pages retain the incoming continuation. complete
    records the lookahead for this query, so later inserts may add rows unless
    the caller holds a snapshot. Frozen attributes do not freeze row dictionaries.

    Example:
        >>> page = RowPage(({"work_id": 7},), 7, True)
        >>> page.next_after_id, page.complete
        (7, True)
    """

    rows: tuple[dict[str, Any], ...]
    next_after_id: int | None
    complete: bool


class QueryCursor(Protocol):
    """
    Consume an executed query with positional or mapping rows.

    Cells retain backend values. This read interface does not require subsequent
    execution or affected-row metadata. The cursor owner closes it on every path.

    Example:
        >>> row = cursor.fetchone()  # doctest: +SKIP
    """

    @abstractmethod
    def fetchone(self) -> SQLRow | None:
        """
        Consume the next row from the current result.

        Example:
            >>> row = cursor.fetchone()  # doctest: +SKIP


        :return: One positional or mapping row, or None after exhaustion; SQL NULL
            remains a cell.
        """
        ...

    @abstractmethod
    def fetchall(self) -> Sequence[SQLRow]:
        """
        Consume all remaining rows, materializing the result in memory.

        Example:
            >>> remaining = cursor.fetchall()  # doctest: +SKIP


        :return: A sequence of positional or mapping rows, empty after exhaustion.
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """
        Release this cursor without committing or closing its connection.

        Example:
            >>> cursor.close()  # doctest: +SKIP


        :return: None; later cursor operations are invalid.
        """
        ...

    @abstractmethod
    def __iter__(self) -> Iterator[SQLRow]:
        """
        Consume remaining rows using the iterator protocol.

        Exhaustion does not transfer cursor cleanup to the iterator.

        Example:
            >>> rows = list(cursor)  # doctest: +SKIP


        :return: An iterator over the remaining positional or mapping rows.
        """
        ...


class QueryConnection(Protocol):
    """
    Expose execution and lifetime controls on a driver connection.

    Rows may be positional or mappings. Execute methods return caller-owned
    cursors; receiving a borrowed connection does not authorize completing or
    closing the transaction owned by its supplying scope.

    Example:
        >>> cursor = connection.execute("SELECT 1")  # doctest: +SKIP
        >>> cursor.close()  # doctest: +SKIP
    """

    @abstractmethod
    def execute(self, sql: str, parameters: SQLParameters = (), /) -> QueryCursor:
        """
        Execute one statement with separately bound values.

        Placeholder syntax and value adaptation follow the selected backend.

        Example:
            >>> result = connection.execute("SELECT 1")  # doctest: +SKIP


        :param sql: SQL statement in the connection dialect.
        :param parameters: Positional or named bindings; empty when the statement
            needs none.
        :return: A caller-owned cursor; close it even after consuming every row.
        """
        ...

    @abstractmethod
    def executemany(
        self, sql: str, parameters: Iterable[SQLParameters], /
    ) -> QueryCursor:
        """
        Execute one statement for each supplied parameter set.

        This call alone does not promise an atomic batch; the connection transaction
        policy determines rollback and completion.

        Example:
            >>> result = connection.executemany(sql, batches)  # doctest: +SKIP


        :param sql: Statement reused for each set of bindings.
        :param parameters: Iterable of positional or named binding sets, consumed by
            execution.
        :return: A caller-owned cursor; close it even after consuming every row.
        """
        ...

    @abstractmethod
    def commit(self) -> None:
        """
        Complete pending work under the backend transaction policy.

        Only the connection or transaction owner should invoke this operation.

        Example:
            >>> connection.commit()  # doctest: +SKIP


        :return: None after successful completion; installed completion hooks may
            raise.
        """
        ...

    @abstractmethod
    def rollback(self) -> None:
        """
        Discard pending work under the backend transaction policy.

        Only the connection or transaction owner should invoke this operation.

        Example:
            >>> connection.rollback()  # doctest: +SKIP


        :return: None; previously committed work is unaffected.
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """
        Release the connection and its owned resources without requesting a commit.

        Only the connection or transaction owner should invoke this operation.

        Example:
            >>> connection.close()  # doctest: +SKIP


        :return: None after successful cleanup; the handle must no longer be used.
        """
        ...


class SQLExecuteOperation(Protocol):
    """
    Describe a bound wrapper executor with integer binding shorthand.

    Example:
        >>> cursor = wrapper.execute("SELECT ?", 7)  # doctest: +SKIP
    """

    def __call__(
        self, sql: str, values: SQLParameters | int | None = None
    ) -> QueryCursor:
        """
        Execute through the bound wrapper and return its live result cursor.

        Example:
            >>> cursor = wrapper.execute("SELECT ?", 7)  # doctest: +SKIP


        :param sql: SQL accepted by the selected driver.
        :param values: Bindings, a bare integer for driver shorthand, or None for
            none; shared SQL converts the integer to one text binding.
        :return: A cursor owned by the caller; close it on success and failure.
        """
        ...


class SQLExecuteManyOperation(Protocol):
    """
    Describe wrapper batch execution, including the legacy script form.

    A None values argument selects script execution; an empty iterable remains
    a batch. Transaction behavior belongs to the wrapper and selected backend.

    Example:
        >>> wrapper.executemany("INSERT INTO tags(tag) VALUES (?)", [("fiction",)])  # doctest: +SKIP
    """

    def __call__(self, sql: str, values: Iterable[SQLParameters] | None = None) -> None:
        """
        Execute a batch, or select the legacy script path with values=None.

        Example:
            >>> wrapper.executemany(sql, batches)  # doctest: +SKIP


        :param sql: Repeated statement, or script when values is None.
        :param values: Iterable of binding sets; None selects script execution.
        :return: None; result-cursor cleanup belongs to the selected driver helper.
        """
        ...


class SQLGetOperation(Protocol):
    """
    Describe a wrapper read that returns all rows or one complete row.

    The wrapper get(all=False) result is a row. Native connection get helpers
    have a separate scalar convention; their signatures are not interchangeable.

    Example:
        >>> row = wrapper.get("SELECT 1, 2", all=False)  # doctest: +SKIP
    """

    def __call__(
        self, sql: str, values: SQLParameters | int | None = None, *, all: bool = True
    ) -> list[SQLRow] | SQLRow | None:
        """
        Fetch through the wrapper and close its temporary cursor.

        Example:
            >>> row = wrapper.get("SELECT 1, 2", all=False)  # doctest: +SKIP


        :param sql: SQL accepted by the selected driver.
        :param values: Bindings, a bare integer for driver shorthand, or None for
            none; shared SQL converts the integer to one text binding.
        :param all: True materializes all rows; False returns the first complete
            row.
        :return: A list of rows, or one row/None when all=False; SQL NULL stays
            inside a row.
        """
        ...
