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
    table_sql: str
    columns: tuple[str, ...]
    id_column: str
    hide_id: bool
    where: Mapping[str, Any]


class DriverWrapperQueryMixin(DriverWrapperBase):
    """Fetch through borrowed handles and share the existing pagination primitive."""

    _query_observers: tuple[QueryObserver, ...] = ()

    def set_query_observers(self, observers: Iterable[QueryObserver] = ()) -> None:
        """Replace this wrapper's synchronous observers, or clear them by default.

        No timer, event construction or observer dispatch occurs when empty.
        Observer errors are logged and do not change query results. Nested calls
        contribute to their outer observation rather than duplicating events.
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
        """Fetch one complete row or None; close its cursor without committing.

        Prefer an explicit connection, otherwise the active macro or primary
        handle. SQL is trusted caller input; use macros.read_transaction() when
        backend-enforced read-only access and a consistent snapshot are needed.
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
        """Fetch all result rows and close the cursor; use scans for large tables."""
        with self._read_connection(connection) as active:
            cursor = (
                active.execute(sql) if values is None else active.execute(sql, values)
            )
            try:
                return cursor.fetchall()
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
        """Return a single-column first row, or None for no row/SQL NULL.

        Multiple projected columns are rejected. Its scalar result does not
        distinguish an empty result from NULL, so diagnostics leave rows unknown.
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
        """Read current adapter capabilities/limits on the selected connection."""
        with self._read_connection(connection) as active:
            return self.driver.direct_get_backend_capabilities(connection=active)

    def _scan_plan(
        self,
        table: str,
        columns: Sequence[str] | None,
        where: Mapping[str, Any] | None,
        connection: QueryConnection | None,
    ) -> _ScanPlan:
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
        """Read a bounded physical-table page with a resumable last-ID marker.

        Select optional columns and equality/NULL filters. Each page closes its
        cursor before returning. A continuation is exclusive; changed/deleted
        earlier IDs are not revisited and later inserts can appear. Borrow a
        macros.read_transaction() connection to pin a backend-enforced snapshot.
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
        """Yield bounded pages, resolving schema once and owning no handle at yield.

        Empty tables yield no batches. The caller controls any borrowed snapshot
        lifetime, including early exit. Concurrent schema changes may fail the
        next page; no hidden retry or inconsistent restart is attempted.
        """
        plan = self._scan_plan(table, columns, where, connection)
        while True:
            page = self._read_scan_page(plan, batch_size, after_id, connection)
            if page.rows:
                yield page
            if page.complete:
                return
            after_id = page.next_after_id
