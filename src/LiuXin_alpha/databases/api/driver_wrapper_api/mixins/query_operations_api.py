"""Declare connection-aware reads, bounded scans and optional diagnostics."""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any

from LiuXin_alpha.databases.db_types.backend_capabilities import BackendCapabilities
from LiuXin_alpha.databases.db_types.driver_types import (
    QueryConnection,
    RowPage,
    SQLParameters,
    SQLRow,
)
from LiuXin_alpha.databases.db_types.query_observation import QueryObserver


class QueryOperationsAPI(ABC):
    """
    Read results, page physical tables and observe wrapper queries.

    Prefer an explicit connection, then the active macro connection, then the
    primary handle. Open a temporary handle only when none is available. Close
    all owned cursors and temporary handles; borrowed handles are neither
    committed nor closed. Fetch SQL is trusted and is not screened for writes.
    Use macros.read_transaction() for a backend-enforced read-only snapshot.

    Example:
        >>> row = wrapper.fetch_one("SELECT 1, 2")  # doctest: +SKIP
    """

    @abstractmethod
    def fetch_one(
        self,
        sql: str,
        values: SQLParameters | None = None,
        *,
        connection: QueryConnection | None = None,
    ) -> SQLRow | None:
        """
        Fetch the first complete row and release the temporary cursor.

        Additional rows are ignored. Prefer explicit, active macro, then primary
        handles; close a fresh fallback handle only. The method does not commit.
        SQL may perform writes unless the caller supplies a read-only transaction.

        Example:
            >>> row = wrapper.fetch_one("SELECT 1, NULL")  # doctest: +SKIP


        :param sql: Trusted SQL accepted by the backend; use separate bindings for
            values.
        :param values: Positional or named bindings, or None for execution without
            bindings.
        :param connection: Borrowed handle; None prefers the active macro, then
            primary, then a fresh handle.
        :return: One backend row, positional or mapping, or None when no row is
            returned.
        """

    @abstractmethod
    def fetch_all(
        self,
        sql: str,
        values: SQLParameters | None = None,
        *,
        connection: QueryConnection | None = None,
    ) -> list[SQLRow]:
        """
        Materialize every result row and close the temporary cursor.

        Connection selection and ownership follow fetch_one. This reads an
        unbounded result into memory; use physical-table pages for bounded scans.

        Example:
            >>> rows = wrapper.fetch_all("SELECT 1 UNION ALL SELECT 2")  # doctest: +SKIP


        :param sql: Trusted SQL accepted by the backend; use separate bindings for
            values.
        :param values: Positional or named bindings, or None for execution without
            bindings.
        :param connection: Borrowed handle; None prefers the active macro, then
            primary, then a fresh handle.
        :return: A list of backend rows, empty when the result is empty.
        """

    @abstractmethod
    def fetch_scalar(
        self,
        sql: str,
        values: SQLParameters | None = None,
        *,
        connection: QueryConnection | None = None,
    ) -> Any:
        """
        Extract the sole cell of the first row, or return None for no row.

        An SQL NULL also returns None. Reject multiple projected columns when a
        row exists; an empty result returns None without a column-count check.
        Additional rows are ignored. Ownership and cleanup follow fetch_one.

        Example:
            >>> count = wrapper.fetch_scalar("SELECT count(*) FROM works")  # doctest: +SKIP


        :param sql: Trusted SQL accepted by the backend; use separate bindings for
            values.
        :param values: Positional or named bindings, or None for execution without
            bindings.
        :param connection: Borrowed handle; None prefers the active macro, then
            primary, then a fresh handle.
        :return: The first row cell, or None for SQL NULL or an empty result.
        :raises InputIntegrityError: The fetched row contains other than one column.
        """

    @abstractmethod
    def scan_page(
        self,
        table: str,
        *,
        columns: Sequence[str] | None = None,
        where: Mapping[str, Any] | None = None,
        batch_size: int = 1000,
        after_id: int | None = None,
        connection: QueryConnection | None = None,
    ) -> RowPage:
        """
        Read one bounded physical-table page in ascending primary-key order.

        Require a physical table with one non-null integer primary key and stored
        integer keys. Keep its continuation ID even when columns omits that key.
        Resolve schema for this call and close the cursor before returning.

        The continuation is exclusive: earlier IDs are not revisited, and later
        inserts can appear without a shared snapshot. Borrow a connection from
        macros.read_transaction() when successive pages must see the same state.

        Example:
            >>> page = wrapper.scan_page("works", columns=("work_title",), batch_size=100)  # doctest: +SKIP
            >>> next_id = page.next_after_id  # doctest: +SKIP


        :param table: Physical table with one non-null integer primary key.
        :param columns: Distinct existing column names; None selects all non-hidden
            columns.
        :param where: Column/value equality filters combined with AND; None values
            match SQL NULL.
        :param batch_size: Positive integer maximum number of rows returned in each
            page.
        :param after_id: Exclusive primary-key lower bound, or None to start at the
            beginning.
        :param connection: Borrowed handle; None prefers the active macro, then
            primary, then a fresh handle.
        :return: RowPage with dictionary rows, the last visited ID, and current
            lookahead completeness.
        :raises InputIntegrityError: The table, projection, batch size or stored key
            cannot support the scan.
        """

    @abstractmethod
    def iter_row_batches(
        self,
        table: str,
        *,
        columns: Sequence[str] | None = None,
        where: Mapping[str, Any] | None = None,
        batch_size: int = 1000,
        after_id: int | None = None,
        connection: QueryConnection | None = None,
    ) -> Iterator[RowPage]:
        """
        Yield bounded nonempty pages, resolving schema once for the whole scan.

        No owned cursor or temporary connection survives a yield. Borrowed
        connections and snapshot lifetime remain with the caller, including early
        exit. Empty tables yield nothing. Concurrent schema changes can fail a
        later page; the iterator does not silently restart or retry the scan.

        Example:
            >>> for page in wrapper.iter_row_batches("works", batch_size=100):  # doctest: +SKIP
            ...     consume(page.rows)


        :param table: Physical table with one non-null integer primary key.
        :param columns: Distinct existing column names; None selects all non-hidden
            columns.
        :param where: Column/value equality filters combined with AND; None values
            match SQL NULL.
        :param batch_size: Positive integer maximum number of rows returned in each
            page.
        :param after_id: Exclusive primary-key lower bound, or None to start at the
            beginning.
        :param connection: Borrowed handle; None prefers the active macro, then
            primary, then a fresh handle.
        :return: An iterator of RowPage records using the same key and projection as
            scan_page.
        """

    @abstractmethod
    def get_backend_capabilities(
        self, *, connection: QueryConnection | None = None
    ) -> BackendCapabilities:
        """
        Inspect adapter facilities and current connection limits.

        Use the normal read-handle selection and release only an owned fallback
        connection; no transaction completion is requested.

        Example:
            >>> capabilities = wrapper.get_backend_capabilities()  # doctest: +SKIP


        :param connection: Borrowed handle; None prefers the active macro, then
            primary, then a fresh handle.
        :return: A capability snapshot for the selected backend and handle.
        """

    @abstractmethod
    def set_query_observers(self, observers: Iterable[QueryObserver] = ()) -> None:
        """
        Replace the synchronous observers for subsequent wrapper operations.

        An empty collection disables timing, event construction and dispatch.
        Nested observed calls contribute to the outer event. Observer exceptions
        are logged and do not replace query results or failures. Materialize and
        validate the iterable before replacing the existing registrations.

        Example:
            >>> wrapper.set_query_observers()  # doctest: +SKIP


        :param observers: Synchronous callable listeners in dispatch order; empty
            clears registrations.
        :return: None; affects future observations on this wrapper.
        :raises TypeError: An observer is non-callable or an async
            function/callable.
        """
