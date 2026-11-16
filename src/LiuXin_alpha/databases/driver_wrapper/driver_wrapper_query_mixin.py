"""Connection-aware result fetching, bounded scans and backend diagnostics."""

from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from inspect import getattr_static, iscoroutinefunction
from typing import Any

from LiuXin_alpha.databases.db_types.backend_capabilities import BackendCapabilities
from LiuXin_alpha.databases.db_types.driver_types import (
    QueryConnection,
    RowPage,
    SQLParameters,
    SQLRow,
)
from LiuXin_alpha.databases.db_types.query_observation import QueryObserver
from LiuXin_alpha.databases.driver_wrapper._base import DriverWrapperBase
from LiuXin_alpha.databases.driver_wrapper.diagnostics import observed
from LiuXin_alpha.databases.schema_readers import quote_identifier
from LiuXin_alpha.databases.schema_specs import RelationKind
from LiuXin_alpha.databases.table_pages import read_page
from LiuXin_alpha.errors import InputIntegrityError


@dataclass(frozen=True, slots=True)
class _ScanPlan:
    """Validated physical-table projection, integer key and filters for repeated keyset pages."""

    table_sql: str
    columns: tuple[str, ...]
    id_column: str
    hide_id: bool
    where: Mapping[str, Any]


class DriverWrapperQueryMixin(DriverWrapperBase):
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

    _query_observers: tuple[QueryObserver, ...] = ()

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
        callbacks = tuple(observers)
        for callback in callbacks:
            if (
                not callable(callback)
                or iscoroutinefunction(callback)
                or iscoroutinefunction(getattr_static(callback, "__call__", None))
            ):
                raise TypeError("Query observers must be synchronous callables")
        self._query_observers = callbacks

    @contextmanager
    def _read_connection(
        self, connection: QueryConnection | None
    ) -> Iterator[QueryConnection]:
        """Prefer explicit, active macro, then primary handles; never complete them."""
        active = connection
        if active is None:
            active = self.macros.current_transaction_connection()
        if active is None:
            active = self.driver.conn
        owned = active is None
        if active is None:
            active = self.driver.get_connection()
        try:
            yield active
        finally:
            if owned:
                active.close()

    @observed("fetch_one", count="one")
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
        with self._read_connection(connection) as active:
            cursor = (
                active.execute(sql) if values is None else active.execute(sql, values)
            )
            try:
                return cursor.fetchone()
            finally:
                cursor.close()

    @observed("fetch_all", count="all")
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
        with self._read_connection(connection) as active:
            cursor = (
                active.execute(sql) if values is None else active.execute(sql, values)
            )
            try:
                rows = cursor.fetchall()
                return rows if isinstance(rows, list) else list(rows)
            finally:
                cursor.close()

    @observed("fetch_scalar")
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
        row = self.fetch_one(sql, values, connection=connection)
        if row is None:
            return None
        if len(row) != 1:
            raise InputIntegrityError("fetch_scalar requires exactly one result column")
        return next(iter(row.values())) if isinstance(row, Mapping) else row[0]

    @observed("backend_capabilities")
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
        with self._read_connection(connection) as active:
            return self.driver.direct_get_backend_capabilities(connection=active)

    def _scan_plan(
        self,
        table: str,
        columns: Sequence[str] | None,
        where: Mapping[str, Any] | None,
        connection: QueryConnection | None,
    ) -> _ScanPlan:
        """Validate table shape and requested columns before building a reusable scan plan.

        Require a non-null integer primary key. Add that key internally when the caller
        omits it, so continuation still works without exposing an unrequested column.
        Inspect schema through the supplied borrowed connection when one is available."""
        with self._read_connection(connection) as active:
            spec = self.driver.direct_get_physical_table_spec(table, connection=active)
        if spec.relation_kind != RelationKind.TABLE or len(spec.primary_key) != 1:
            raise InputIntegrityError(
                "Scanning requires a physical table with one integer primary key"
            )
        id_column = spec.primary_key[0]
        identity = next(column for column in spec.columns if column.name == id_column)
        if identity.nullable is not False or not (
            identity.affinity == "INTEGER"
            or identity.declared_type in ("smallint", "integer", "bigint")
        ):
            raise InputIntegrityError(
                "Scanning requires a non-null integer primary key"
            )
        available = {column.name for column in spec.columns}
        if isinstance(columns, str):
            raise InputIntegrityError("columns must be a sequence of column names")
        selected = (
            tuple(columns)
            if columns is not None
            else tuple(column.name for column in spec.columns if not column.hidden)
        )
        predicates = dict(where or {})
        if (
            not selected
            or len(set(selected)) != len(selected)
            or not set(selected).issubset(available)
            or not set(predicates).issubset(available)
        ):
            raise InputIntegrityError(
                "Scan columns and filters must name existing, distinct columns"
            )
        hidden_id = id_column not in selected
        if hidden_id:
            selected += (id_column,)
        table_sql = quote_identifier(spec.name)
        if spec.schema is not None:
            table_sql = quote_identifier(spec.schema) + "." + table_sql
        return _ScanPlan(table_sql, selected, id_column, hidden_id, predicates)

    @observed("scan_page", count="page")
    def _read_scan_page(
        self,
        plan: _ScanPlan,
        batch_size: int,
        after_id: int | None,
        connection: QueryConnection | None,
    ) -> RowPage:
        """Read one bounded keyset page and release its cursor before returning.

        Reject non-integer stored keys instead of truncating them into repeated pages.
        Retain the continuation ID separately when the projection hides its key column;
        the caller decides whether successive pages share an outer snapshot."""
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or batch_size < 1
        ):
            raise InputIntegrityError("batch_size must be a positive integer")
        with self._read_connection(connection) as active:
            result, complete = read_page(
                active,
                table_sql=plan.table_sql,
                columns=plan.columns,
                id_column=plan.id_column,
                where=plan.where,
                limit=batch_size,
                after_id=after_id,
            )
        # SQLite affinity alone does not require integer storage. Reject a
        # malformed key instead of truncating it and repeating the same page.
        if any(not isinstance(row[plan.id_column], int) for row in result):
            raise InputIntegrityError("Scanning requires integer primary-key values")
        continuation = result[-1][plan.id_column] if result else after_id
        if plan.hide_id:
            for row in result:
                del row[plan.id_column]
        return RowPage(result, continuation, complete)

    @observed("scan_page", count="page")
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
        plan = self._scan_plan(table, columns, where, connection)
        return self._read_scan_page(plan, batch_size, after_id, connection)

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
        plan = self._scan_plan(table, columns, where, connection)
        while True:
            page = self._read_scan_page(plan, batch_size, after_id, connection)
            if page.rows:
                yield page
            if page.complete:
                return
            after_id = page.next_after_id
