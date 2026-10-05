"""
Implement shared SQL macro infrastructure, row/link operations and identity tooling.

SQLite and PostgreSQL-shaped drivers share validated identifiers, parameter bindings and
link records. Driver hooks qualify relations and adapt connections. Reads reuse an
active transaction connection or a cached driver connection. Outer macro writes manage a
savepoint and commit/rollback; nested calls join that boundary. Further services include
policy-aware identities, temporary tables, orphan pruning and canonical table
fingerprints.
"""

from __future__ import annotations

import base64
from collections.abc import Iterable, Mapping
from contextlib import AbstractContextManager, contextmanager, nullcontext
from dataclasses import replace
import datetime as datetime_module
import hashlib
import json
import math
import re
import threading
import uuid
from typing import Any, Iterator

from LiuXin_alpha.databases.column_metadata import (
    COLUMN_METADATA_TABLE,
    ColumnEmptyValuePolicy,
    ColumnMetadata,
    ColumnNormalizationProfile,
    infer_column_metadata,
)
from LiuXin_alpha.databases.macro_types import (
    CanonicalIdentity,
    LINK_TYPE_UNSET,
    LinkRow,
    LinkValue,
    UnreferencedRowsSpec,
    NormalizedIdentityCollision,
    NormalizedIdentityMigrationReport,
)
from LiuXin_alpha.databases.normalized_identities import (
    NORMALIZED_IDENTITIES_TABLE,
    NormalizedIdentitySpec,
    default_normalized_identity_spec,
    iter_normalized_identity_defaults,
    normalize_identity_value,
    normalized_identity_db_values,
)
from LiuXin_alpha.databases.schema_specs import LinkCardinality, StorageLinkSpec
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError


_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TEMP_TYPES = frozenset({"BLOB", "INTEGER", "NUMERIC", "REAL", "TEXT"})
_LIVE_ALLOWED_TYPES_UNSET = object()


def _identifier(value: str, *, kind: str = "identifier") -> str:
    """
    Convert and trim a name, then require the supported simple SQL identifier grammar.

    Accept an ASCII letter or underscore followed by ASCII letters, digits or
    underscores. Dotted names, embedded quotes, punctuation and empty names raise
    InputIntegrityError. This checks spelling only; it does not check schema membership
    or reserved words.

    Example:
        >>> _identifier("  work_title  ")
        'work_title'
        >>> try:
        ...     _identifier("works.title")
        ... except InputIntegrityError:
        ...     print("rejected")
        rejected


    :param value: Name converted with str before validation.
    :param kind: Diagnostic label used in the rejection message.
    :return: The stripped string accepted by the grammar.
    """
    text = str(value).strip()
    if not _SAFE_IDENTIFIER.fullmatch(text):
        raise InputIntegrityError(f"Unsafe SQL {kind}: {value!r}")
    return text


def _quoted(value: str) -> str:
    """
    Wrap a validated simple identifier in SQL double quotes.

    Validation rejects embedded quotes before the defensive quote-doubling step;
    qualified names must be composed elsewhere.

    Example:
        >>> _quoted(" title ")
        '"title"'


    :param value: Name accepted by _identifier after string conversion and trimming.
    :return: A double-quoted identifier.
    """
    return '"' + _identifier(value).replace('"', '""') + '"'


def _chunks(values: tuple[Any, ...], size: int = 500) -> Iterator[tuple[Any, ...]]:
    """
    Yield consecutive slices of the supplied tuple at the requested step size.

    A positive size yields a possibly shorter final slice. Zero raises ValueError when
    iteration starts, and a negative size yields no slices. No separate size validation
    or copying of contained objects is performed.

    Example:
        >>> list(_chunks((1, 2, 3), 2))
        [(1, 2), (3,)]
        >>> list(_chunks((1, 2), -1))
        []


    :param values: Tuple whose elements are grouped without transformation.
    :param size: Slice length and range step; normally a positive integer, defaulting to
        500.
    :return: An iterator of tuple slices.
    """
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def _row_value(row: Any, index: int, column: str) -> Any:
    """
    Read a cell by mapping key or positional index according to the row type.

    Mapping rows use get and return None for absent columns. Other rows use indexing, so
    missing positions or unsupported indexing propagate their errors.

    Example:
        >>> _row_value({"title": "A"}, 99, "title")
        'A'
        >>> _row_value({}, 0, "missing") is None
        True
        >>> _row_value((7, "A"), 1, "ignored")
        'A'


    :param row: Mapping or indexable row supplied by the backend.
    :param index: Zero-based position used only for non-Mapping rows.
    :param column: Mapping key used only when row implements Mapping.
    :return: The selected value, or None for a missing mapping key.
    """
    if isinstance(row, Mapping):
        return row.get(column)
    return row[index]


def _canonical_db_value(value: Any) -> Any:
    """
    Encode a database value as a type-tagged, JSON-serializable fingerprint value.

    Distinguish None, booleans, integers, floats and text. Integers use decimal text;
    finite floats use hex, with explicit nan/inf/-inf markers. Byte-like values become
    base64 text and date/time values use their runtime type name and ISO representation.
    Recurse through mappings and containers; sort canonical mapping keys and set
    elements by their JSON text. Lists and tuples retain order and distinct type tags,
    while set and frozenset share one tag.

    Fallback objects use their qualified type name and str value, which may not be
    stable across processes. Cyclic containers have no cycle detection, and recursive
    conversion or stringification errors propagate. The encoding is not a reversible
    serializer or a guarantee of distinct encodings for arbitrary objects.

    Example:
        >>> _canonical_db_value(None)
        ['none', None]
        >>> _canonical_db_value(True), _canonical_db_value(1)
        (['bool', True], ['int', '1'])
        >>> _canonical_db_value(-0.0)
        ['float', '-0x0.0p+0']
        >>> _canonical_db_value(float("nan"))
        ['float', 'nan']
        >>> _canonical_db_value(b"A")
        ['bytes', 'QQ==']
        >>> _canonical_db_value({2, 1})
        ['set', [['int', '1'], ['int', '2']]]
        >>> _canonical_db_value({"b": 2, "a": 1}) == _canonical_db_value({"a": 1, "b": 2})
        True
        >>> _canonical_db_value((1,))
        ['tuple', [['int', '1']]]


    :param value: Scalar, date/time, byte-like value, container or fallback object to
        encode.
    :return: Nested type-tagged lists and mapping pairs suitable for JSON encoding.
    """
    if value is None:
        return ["none", None]
    if isinstance(value, bool):
        return ["bool", value]
    if isinstance(value, int):
        return ["int", str(value)]
    if isinstance(value, float):
        if math.isnan(value):
            rendered = "nan"
        elif math.isinf(value):
            rendered = "inf" if value > 0 else "-inf"
        else:
            rendered = value.hex()
        return ["float", rendered]
    if isinstance(value, str):
        return ["str", value]
    if isinstance(value, (bytes, bytearray, memoryview)):
        return ["bytes", base64.b64encode(bytes(value)).decode("ascii")]
    if isinstance(value, (datetime_module.date, datetime_module.time, datetime_module.datetime)):
        return [type(value).__name__, value.isoformat()]
    if isinstance(value, Mapping):
        pairs = [
            (_canonical_db_value(key), _canonical_db_value(item))
            for key, item in value.items()
        ]
        pairs.sort(key=lambda pair: json.dumps(pair[0], ensure_ascii=False, sort_keys=True))
        return ["mapping", pairs]
    if isinstance(value, (set, frozenset)):
        items = [_canonical_db_value(item) for item in value]
        items.sort(key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True))
        return ["set", items]
    if isinstance(value, (list, tuple)):
        return [type(value).__name__, [_canonical_db_value(item) for item in value]]
    value_type = f"{type(value).__module__}.{type(value).__qualname__}"
    return ["object", value_type, str(value)]


class SQLPortableMacrosMixin:
    """
    Supply portable SQL services to a macro host that provides a db facade.

    The host supplies a driver, wrapper schema queries and optionally a context-manager
    lock. Identifier validation is distinct from physical schema validation. Row
    mappings and link records materialize query results; public writes join the
    outermost macro transaction, while private SQL helpers rely on their caller's
    connection and validation.

    Example:
        An attached host can group operations with ``with macros.transaction():``
        and read their uncommitted results through ``macros.get_row(table, row_id)``.
    """

    db: Any

    # ------------------------------------------------------------------------------------------------------------------
    # Infrastructure

    def _macro_driver(self) -> Any:
        """
        Resolve the attached driver, preferring the facade's direct driver attribute.

        Fall back to db.driver_wrapper.driver only when db.driver is None or absent. If
        neither resolves, raise DatabaseIntegrityError. The selected object is not
        otherwise validated.

        Example:
            >>> from types import SimpleNamespace
            >>> macros = SQLPortableMacrosMixin()
            >>> driver = object()
            >>> macros.db = SimpleNamespace(driver_wrapper=SimpleNamespace(driver=driver))
            >>> macros._macro_driver() is driver
            True


        :return: The resolved driver object.
        """
        driver = getattr(self.db, "driver", None)
        if driver is None:
            driver = getattr(getattr(self.db, "driver_wrapper", None), "driver", None)
        if driver is None:
            raise DatabaseIntegrityError("Database macros are not attached to a driver.")
        return driver

    def _macro_connection(self) -> Any:
        # Read instance state directly. Some compatibility macro classes use
        # ``__getattr__`` to report unsupported public macros, so probing a
        # private implementation attribute through ``getattr`` is observable.
        """
        Reuse the active thread-local transaction connection or the driver's cached
        connection.

        Inspect instance state directly to avoid compatibility __getattr__ hooks. When
        transaction depth is nonzero and its connection is present, return it. Otherwise
        reuse driver.conn; if absent or None, call get_connection and store the result
        on driver.conn. This read path does not close connections or start a
        transaction.

        Example:
            Within ``with macros.transaction() as conn:``,
            ``macros._macro_connection() is conn`` is true.


        :return: The active transaction connection or cached driver connection.
        """
        state = vars(self).get("_macro_transaction_state")
        if state is not None and getattr(state, "depth", 0):
            conn = getattr(state, "connection", None)
            if conn is not None:
                return conn
        driver = self._macro_driver()
        conn = getattr(driver, "conn", None)
        if conn is None:
            conn = driver.get_connection()
            driver.conn = conn
        return conn

    def _macro_table_sql(self, table: str) -> str:
        """
        Validate a table name and ask the driver to qualify it when supported.

        Call a callable driver._table_sql hook and stringify its result. Without that
        hook, return a quoted simple identifier. The hook's returned SQL is trusted and
        physical existence is not checked here.

        Example:
            A driver without _table_sql renders ``macros._macro_table_sql("works")``
            as ``'"works"'``.


        :param table: Table name validated for SQL use and resolved through the host
            driver wrapper.
        :return: Driver-qualified table SQL or a quoted simple name.
        """
        table = _identifier(table, kind="table name")
        driver = self._macro_driver()
        qualify = getattr(driver, "_table_sql", None)
        if callable(qualify):
            return str(qualify(table))
        return _quoted(table)

    def _macro_temporary_table_sql(self, table: str) -> str:
        """
        Qualify a quoted temporary relation for the driver shape.

        Use pg_temp when the driver has a schema attribute, regardless of its value,
        otherwise temp. Validate and quote the relation name but do not create or
        inspect it.

        Example:
            ``macros._macro_temporary_table_sql("selected_ids")`` yields
            ``temp."selected_ids"`` for a driver without a schema attribute.


        :param table: Table name validated for SQL use and resolved through the host
            driver wrapper.
        :return: A temp- or pg_temp-qualified quoted relation name.
        """
        driver = self._macro_driver()
        schema = "pg_temp" if hasattr(driver, "schema") else "temp"
        return f"{schema}.{_quoted(table)}"

    def _macro_temporary_declared_type(self, declared_type: str) -> str:
        """
        Translate exact BLOB spelling to BYTEA for a driver with a schema attribute.

        All other inputs pass through unchanged; no case conversion or vocabulary
        validation occurs in this helper.

        Example:
            >>> from types import SimpleNamespace
            >>> macros = SQLPortableMacrosMixin()
            >>> macros.db = SimpleNamespace(driver=SimpleNamespace(schema="main"))
            >>> macros._macro_temporary_declared_type("BLOB")
            'BYTEA'
            >>> macros._macro_temporary_declared_type("blob")
            'blob'


        :param declared_type: Portable temporary-table type spelling, normally already
            validated by the caller.
        :return: BYTEA for the selected BLOB case, otherwise the original declaration.
        """

        if declared_type == "BLOB" and hasattr(self._macro_driver(), "schema"):
            return "BYTEA"
        return declared_type

    def _macro_invalidate(self) -> None:
        """
        Invoke the driver's available property-cache invalidation hook.

        Prefer a callable _zero_prop_cache; otherwise try zero_prop_cache. If neither is
        callable, do nothing. Driver resolution and hook errors propagate.

        Example:
            After a successful outer macro transaction, this helper refreshes the
            driver cache through _zero_prop_cache when that hook is available.


        :return: None after the hook returns, or when no callable hook exists.
        """
        driver = self._macro_driver()
        invalidate = getattr(driver, "_zero_prop_cache", None)
        if not callable(invalidate):
            invalidate = getattr(driver, "zero_prop_cache", None)
        if callable(invalidate):
            invalidate()

    @contextmanager
    def _macro_transaction(self) -> Iterator[Any]:
        """
        Manage the outer macro write boundary and reuse it for nested calls on this
        thread.

        At outer entry, prefer a dedicated connection from a callable
        driver.get_connection; otherwise require driver.conn. Enter db.lock when it
        supports context management, then record thread-local depth/connection and
        create a savepoint. On normal exit release it and commit; an exception from the
        body rolls back to it, releases it, calls connection.rollback and is re-raised.
        Reset state and close only a connection obtained from the factory. Invalidate
        caches after successful completion.

        Nested entries only increment depth and yield the same connection. They
        establish no independent savepoint: an inner error caught inside the outer body
        does not itself roll back inner work. On a reused persistent connection,
        commit/rollback also affects any surrounding pending work. Factory, SQL, cleanup
        and invalidation errors propagate; the helper does not make caller-owned
        transactions independent.

        Example:
            ``with macros._macro_transaction() as conn:`` uses the connection
            shared by nested portable writes until the outer context exits.


        :return: A context manager yielding the active connection; the outer context
            owns commit/rollback.
        """
        state = vars(self).get("_macro_transaction_state")
        if state is None:
            state = threading.local()
            self._macro_transaction_state = state
        depth = getattr(state, "depth", 0)
        if depth:
            conn = state.connection
            state.depth = depth + 1
            try:
                yield conn
            finally:
                state.depth -= 1
            return
        driver = self._macro_driver()
        get_connection = getattr(driver, "get_connection", None)
        owns_connection = callable(get_connection)
        if owns_connection:
            conn = get_connection()
        else:
            # Lightweight driver adapters and test harnesses historically
            # expose only one persistent connection. Preserve that supported
            # shape while using a dedicated connection whenever the real
            # driver can provide one.
            conn = getattr(driver, "conn", None)
            if conn is None:
                raise DatabaseIntegrityError(
                    "Database driver has no connection for portable macros."
                )
        lock = getattr(self.db, "lock", None)
        lock_context = lock if hasattr(lock, "__enter__") else nullcontext()
        with lock_context:
            state.depth = 1
            state.connection = conn
            try:
                conn.execute("SAVEPOINT liuxin_portable_macro_transaction")
                try:
                    yield conn
                except BaseException:
                    conn.execute(
                        "ROLLBACK TO SAVEPOINT liuxin_portable_macro_transaction"
                    )
                    conn.execute(
                        "RELEASE SAVEPOINT liuxin_portable_macro_transaction"
                    )
                    conn.rollback()
                    raise
                else:
                    conn.execute(
                        "RELEASE SAVEPOINT liuxin_portable_macro_transaction"
                    )
                    conn.commit()
            finally:
                state.depth = 0
                state.connection = None
                if owns_connection:
                    conn.close()
        self._macro_invalidate()

    def transaction(self) -> AbstractContextManager[Any]:
        """
        Return the context manager that groups portable operations in one outer write
        boundary.

        Calling this method alone performs no SQL. Entering delegates to
        _macro_transaction, including driver-dependent connection ownership, optional
        locking, nested connection sharing and outer commit/rollback. Nested exceptions
        must escape the outer body to trigger its rollback.

        Example:
            >>> import sqlite3
            >>> from types import SimpleNamespace
            >>> conn = sqlite3.connect(":memory:")
            >>> _ = conn.execute("CREATE TABLE entries (value INTEGER)")
            >>> macros = SQLPortableMacrosMixin()
            >>> macros.db = SimpleNamespace(driver=SimpleNamespace(conn=conn))
            >>> with macros.transaction() as outer:
            ...     _ = outer.execute("INSERT INTO entries VALUES (1)")
            ...     with macros.transaction() as inner:
            ...         print(inner is outer)
            ...         _ = inner.execute("INSERT INTO entries VALUES (2)")
            True
            >>> try:
            ...     with macros.transaction() as active:
            ...         _ = active.execute("DELETE FROM entries")
            ...         raise RuntimeError("rollback")
            ... except RuntimeError:
            ...     pass
            >>> conn.execute("SELECT value FROM entries ORDER BY value").fetchall()
            [(1,), (2,)]
            >>> conn.close()


        :return: A context manager yielding the shared connection for composed macro
            operations.
        """

        return self._macro_transaction()

    def _row_id_column(self, table: str, id_column: str | None) -> str:
        """
        Resolve an ID column override or wrapper default and verify that the column
        exists.

        Validation checks identifier spelling and membership, not uniqueness or primary-
        key status. Wrapper and column validation errors propagate.

        Example:
            ``macros._row_id_column("works", None)`` uses the wrapper's ID-column
            choice and validates it against the physical headings.


        :param table: Table name validated for SQL use and resolved through the host
            driver wrapper.
        :param id_column: Physical ID column override, or None to ask the driver wrapper
            for one.
        :return: The validated ID-column name.
        """
        if id_column is None:
            id_column = self.db.driver_wrapper.get_id_column(table)
        return self._validate_columns(table, (id_column,))[0]

    @staticmethod
    def _mapping_rows(
        columns: tuple[str, ...],
        rows: Iterable[Any],
    ) -> tuple[Mapping[str, Any], ...]:
        """
        Materialize query rows as fresh dictionaries using the selected column order.

        Mapping rows contribute None for missing keys; positional rows are indexed and
        may raise for missing cells. Duplicate column names overwrite earlier entries.
        Values are retained without conversion or deep copying.

        Example:
            >>> SQLPortableMacrosMixin._mapping_rows(("id", "name"), [(1, "A"), {"id": 2}])
            ({'id': 1, 'name': 'A'}, {'id': 2, 'name': None})


        :param columns: Ordered output keys and positional-cell layout.
        :param rows: Iterable of mapping or positional rows, fully consumed.
        :return: A tuple of newly created dictionaries.
        """
        return tuple(
            {
                column: _row_value(row, index, column)
                for index, column in enumerate(columns)
            }
            for row in rows
        )

    def get_row(
        self,
        table: str,
        row_id: Any,
        *,
        id_column: str | None = None,
    ) -> Mapping[str, Any] | None:
        """
        Read and materialize all columns for at most one matching ID on the active
        connection.

        Resolve and validate the ID column, then bind row_id using SQL equality. Consume
        all matches before returning; duplicate matches raise DatabaseIntegrityError.
        Return None for no match. This read does not establish a new transaction or
        close the shared/cached connection.

        Example:
            ``macros.get_row("works", 1)`` returns the column mapping for ID 1,
            or None when no row matches.


        :param table: Table name validated for SQL use and resolved through the host
            driver wrapper.
        :param row_id: Identifier bound to an equality predicate; None is not treated as
            IS NULL.
        :param id_column: Physical ID column override, or None to ask the driver wrapper
            for one.
        :return: A new column-to-value dictionary, or None; values are not additionally
            coerced.
        """

        id_column = self._row_id_column(table, id_column)
        columns = self._column_names(table)
        conn = self._macro_connection()
        rows = self._mapping_rows(
            columns,
            conn.execute(
                f"SELECT {', '.join(_quoted(column) for column in columns)} "
                f"FROM {self._macro_table_sql(table)} "
                f"WHERE {_quoted(id_column)} = ?",
                (row_id,),
            ),
        )
        if len(rows) > 1:
            raise DatabaseIntegrityError(
                f"ID {row_id!r} matched multiple rows in {table!r}."
            )
        return rows[0] if rows else None

    def get_rows(
        self,
        table: str,
        *,
        where: Mapping[str, Any] | None = None,
        order_by: Iterable[str] = (),
    ) -> tuple[Mapping[str, Any], ...]:
        """
        Read materialized rows with validated equality filters and optional ascending
        ordering.

        Copy where into a dictionary, join predicates with AND and bind non-null values;
        None becomes IS NULL. Empty filters select all rows. Order by the listed
        physical columns without descending modifiers; an empty order leaves SQL result
        order unspecified. Use the active transaction or cached connection without
        starting another transaction.

        Example:
            ``macros.get_rows("works", where={"work_title": None},
            order_by=("work_id",))`` selects untitled rows in ascending ID order.


        :param table: Table name validated for SQL use and resolved through the host
            driver wrapper.
        :param where: Column/value equality filters with exact schema column names,
            or None for no filtering.
        :param order_by: Iterable of physical column names for ascending SQL ordering.
        :return: A tuple of new column-to-value dictionaries, possibly empty.
        """

        columns = self._column_names(table)
        predicates = dict(where or {})
        predicate_columns = self._validate_columns(table, predicates)
        order_columns = self._validate_columns(table, tuple(order_by))
        sql = (
            f"SELECT {', '.join(_quoted(column) for column in columns)} "
            f"FROM {self._macro_table_sql(table)}"
        )
        conditions: list[str] = []
        values: list[Any] = []
        for column in predicate_columns:
            value = predicates[column]
            if value is None:
                conditions.append(f"{_quoted(column)} IS NULL")
            else:
                conditions.append(f"{_quoted(column)} = ?")
                values.append(value)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        if order_columns:
            sql += " ORDER BY " + ", ".join(
                _quoted(column) for column in order_columns
            )
        return self._mapping_rows(
            columns,
            self._macro_connection().execute(sql, tuple(values)),
        )

    def insert_row(
        self,
        table: str,
        values: Mapping[str, Any],
        *,
        id_column: str | None = None,
    ) -> Any:
        """
        Validate a nonempty value mapping, insert it inside the macro transaction and
        return its ID.

        Preserve mapping order when binding values. Drivers with a schema attribute use
        INSERT RETURNING the resolved ID column and require a returned row; other
        drivers return cursor.lastrowid. The latter is the backend rowid and is not
        obtained by rereading an arbitrary id_column override. The outermost macro
        context commits; nested calls defer completion to their outer context.

        Example:
            ``new_id = macros.insert_row("works", {"work_title": "A"})`` inserts
            one row and returns the backend's selected ID result.


        :param table: Table name validated for SQL use and resolved through the host
            driver wrapper.
        :param values: Nonempty column/value mapping with exact schema column names,
            copied before validation.
        :param id_column: Physical ID column override, or None to ask the driver wrapper
            for one.
        :return: The RETURNING ID value or cursor.lastrowid; a missing RETURNING row
            raises DatabaseIntegrityError.
        """

        payload = dict(values)
        if not payload:
            raise InputIntegrityError("insert_row values cannot be empty")
        columns = self._validate_columns(table, payload)
        id_column = self._row_id_column(table, id_column)
        sql = (
            f"INSERT INTO {self._macro_table_sql(table)} "
            f"({', '.join(_quoted(column) for column in columns)}) VALUES "
            f"({', '.join('?' for _ in columns)})"
        )
        driver = self._macro_driver()
        with self._macro_transaction() as conn:
            if hasattr(driver, "schema"):
                cursor = conn.execute(
                    sql + f" RETURNING {_quoted(id_column)}",
                    tuple(payload[column] for column in columns),
                )
                row = cursor.fetchone()
                if row is None:
                    raise DatabaseIntegrityError(
                        f"Insert into {table!r} did not return an ID."
                    )
                return _row_value(row, 0, id_column)
            cursor = conn.execute(
                sql,
                tuple(payload[column] for column in columns),
            )
            return cursor.lastrowid

    def update_row(
        self,
        table: str,
        row_id: Any,
        values: Mapping[str, Any],
        *,
        id_column: str | None = None,
    ) -> None:
        """
        Update matching rows inside the macro transaction while forbidding ID-column
        changes.

        An empty payload returns immediately without validating the table or ID.
        Otherwise validate columns and reject an update to the resolved ID column. Use a
        bound equality predicate; no affected-row count or uniqueness check is made, so
        zero or multiple matches are possible for a nonunique override.

        Example:
            ``macros.update_row("works", 1, {"work_title": "Revised"})`` updates
            the selected value and joins any active macro transaction.


        :param table: Table name validated for SQL use and resolved through the host
            driver wrapper.
        :param row_id: Identifier bound to an equality predicate; None is not treated as
            IS NULL.
        :param values: Replacement column/value mapping with exact schema column names;
            empty mappings are a no-op.
        :param id_column: Physical ID column override, or None to ask the driver wrapper
            for one.
        :return: None after the write or for an empty payload.
        """

        payload = dict(values)
        if not payload:
            return
        columns = self._validate_columns(table, payload)
        id_column = self._row_id_column(table, id_column)
        if id_column in columns:
            raise InputIntegrityError("update_row cannot change the ID column")
        with self._macro_transaction() as conn:
            conn.execute(
                f"UPDATE {self._macro_table_sql(table)} SET "
                + ", ".join(f"{_quoted(column)} = ?" for column in columns)
                + f" WHERE {_quoted(id_column)} = ?",
                tuple(payload[column] for column in columns) + (row_id,),
            )

    def delete_row(
        self,
        table: str,
        row_id: Any,
        *,
        id_column: str | None = None,
    ) -> None:
        """
        Delete rows matching a bound ID inside the macro transaction.

        Validate the resolved ID column and use SQL equality. No affected-row count or
        uniqueness check is made: a missing match is a no-op, and a nonunique override
        can delete multiple rows. Nested calls share the outer write boundary.

        Example:
            ``macros.delete_row("works", 1)`` deletes matches for ID 1.


        :param table: Table name validated for SQL use and resolved through the host
            driver wrapper.
        :param row_id: Identifier bound to an equality predicate; None is not treated as
            IS NULL.
        :param id_column: Physical ID column override, or None to ask the driver wrapper
            for one.
        :return: None; the cursor result is discarded.
        """

        id_column = self._row_id_column(table, id_column)
        with self._macro_transaction() as conn:
            conn.execute(
                f"DELETE FROM {self._macro_table_sql(table)} "
                f"WHERE {_quoted(id_column)} = ?",
                (row_id,),
            )

    def _column_names(self, table: str) -> tuple[str, ...]:
        """
        Materialize wrapper-provided headings as a tuple of strings.

        Preserve wrapper order and duplicates. This helper does not separately validate
        the table spelling or the converted column names.

        Example:
            ``macros._column_names("works")`` returns the wrapper's physical
            heading order as strings.


        :param table: Table name passed unchanged to driver_wrapper.get_column_headings.
        :return: A tuple of str-converted column headings.
        """
        headings = self.db.driver_wrapper.get_column_headings(table)
        return tuple(str(column) for column in headings)

    def _validate_columns(self, table: str, columns: Iterable[str]) -> tuple[str, ...]:
        """
        Validate a simple table name and each requested physical column.

        Obtain available headings before consuming the requested columns. Strip and
        validate each name, preserving order and duplicates. Missing names raise
        InputIntegrityError with sorted unique missing columns. An empty column
        selection is allowed but still inspects the table.

        Example:
            ``macros._validate_columns("works", ("work_id", "work_title"))``
            returns those validated names when both physical columns exist.


        :param table: Table name validated for SQL use and resolved through the host
            driver wrapper.
        :param columns: Iterable of physical column names; may be empty or contain
            duplicates.
        :return: Requested names after identifier normalization, as a tuple.
        """
        table = _identifier(table, kind="table name")
        available = set(self._column_names(table))
        validated = tuple(_identifier(column, kind="column name") for column in columns)
        missing = sorted(set(validated) - available)
        if missing:
            raise InputIntegrityError(
                f"Table {table!r} does not contain column(s): {', '.join(missing)}"
            )
        return validated

    # ------------------------------------------------------------------------------------------------------------------
    # Link rows

    def _validate_link_spec(self, link_spec: StorageLinkSpec) -> StorageLinkSpec:
        """
        Check link-table columns and consistency of typed, ordered and identity flags.

        Require a StorageLinkSpec and verify endpoint, optional type/priority and extra
        columns against physical headings. typed and ordered must agree with the
        presence of their columns; type_part_of_identity requires a typed link. This
        does not validate endpoint-table existence, foreign keys, declared cardinality,
        unique constraints or allowed-type registries.

        Example:
            A spec with ``typed=True`` and ``type_link_col=None`` raises
            InputIntegrityError even when its endpoint columns exist.


        :param link_spec: StorageLinkSpec describing the physical link table and its
            capabilities.
        :return: The original StorageLinkSpec when checks pass; invalid specs raise
            InputIntegrityError.
        """
        if not isinstance(link_spec, StorageLinkSpec):
            raise InputIntegrityError("link_spec must be a StorageLinkSpec.")
        columns = [
            link_spec.primary_link_col,
            link_spec.secondary_link_col,
        ]
        if link_spec.type_link_col is not None:
            columns.append(link_spec.type_link_col)
        if link_spec.priority_link_col is not None:
            columns.append(link_spec.priority_link_col)
        columns.extend(column.name for column in link_spec.extra_link_columns)
        self._validate_columns(link_spec.link_table, columns)
        if link_spec.typed != (link_spec.type_link_col is not None):
            raise InputIntegrityError("Typed link specs must declare a type_link_col.")
        if link_spec.ordered != (link_spec.priority_link_col is not None):
            raise InputIntegrityError("Ordered link specs must declare a priority_link_col.")
        if link_spec.type_part_of_identity and not link_spec.typed:
            raise InputIntegrityError("Only typed links can include type in their identity.")
        return link_spec

    @staticmethod
    def _link_select_columns(link_spec: StorageLinkSpec) -> tuple[str, ...]:
        """
        Build the link query layout with endpoint columns first and extras last.

        Append optional type and priority columns in that order. Extra columns are
        appended only if their names are not already present. Standard column names
        themselves are not deduplicated or validated here.

        Example:
            >>> spec = StorageLinkSpec("works", "agents", "links",
            ...                        primary_link_col="work_id", secondary_link_col="agent_id")
            >>> SQLPortableMacrosMixin._link_select_columns(spec)
            ('work_id', 'agent_id')


        :param link_spec: StorageLinkSpec describing the physical link table and its
            capabilities.
        :return: A tuple of physical column names in row-decoding order.
        """
        columns = [link_spec.primary_link_col, link_spec.secondary_link_col]
        if link_spec.type_link_col is not None:
            columns.append(link_spec.type_link_col)
        if link_spec.priority_link_col is not None:
            columns.append(link_spec.priority_link_col)
        for column in link_spec.extra_link_columns:
            if column.name not in columns:
                columns.append(column.name)
        return tuple(columns)

    @staticmethod
    def _link_identity(link_spec: StorageLinkSpec, link: LinkValue | LinkRow) -> tuple[Any, ...]:
        """
        Extract the secondary ID and optional type key used within one primary endpoint.

        Include link_type only when type_part_of_identity is true. No primary ID,
        priority or extras are included. Components are not normalized or checked for
        hashability; later set/dictionary consumers impose that requirement.

        Example:
            >>> spec = StorageLinkSpec("works", "agents", "links", type_part_of_identity=True)
            >>> SQLPortableMacrosMixin._link_identity(spec, LinkValue(7, link_type="author"))
            (7, 'author')


        :param link_spec: StorageLinkSpec describing the physical link table and its
            capabilities.
        :param link: LinkValue or LinkRow from which secondary_id and optionally
            link_type are read.
        :return: A one- or two-item identity tuple.
        """
        identity = [link.secondary_id]
        if link_spec.type_part_of_identity:
            identity.append(link.link_type)
        return tuple(identity)

    def _link_row_from_db(
        self,
        link_spec: StorageLinkSpec,
        columns: tuple[str, ...],
        row: Any,
    ) -> LinkRow:
        """
        Decode a query row into endpoint, type, priority and extra link values.

        Read cells using the supplied query layout. Mapping rows yield None for missing
        fields; positional rows use indexes. Type and priority are None when their
        columns are absent from the spec. Nonstandard selected columns become a fresh
        extra dictionary. Values are not coerced, copied deeply or validated against
        allowed types.

        Example:
            >>> spec = StorageLinkSpec("works", "agents", "links",
            ...                        primary_link_col="work_id", secondary_link_col="agent_id")
            >>> row = SQLPortableMacrosMixin()._link_row_from_db(spec, ("work_id", "agent_id"), (1, 7))
            >>> (row.primary_id, row.secondary_id, row.link_type, row.extra)
            (1, 7, None, {})


        :param link_spec: StorageLinkSpec describing the physical link table and its
            capabilities.
        :param columns: Selected column layout expected for each positional row.
        :param row: Mapping or positional row returned by the supplied query.
        :return: A LinkRow containing the decoded cells.
        """
        values = {
            column: _row_value(row, index, column)
            for index, column in enumerate(columns)
        }
        standard = {
            link_spec.primary_link_col,
            link_spec.secondary_link_col,
            link_spec.type_link_col,
            link_spec.priority_link_col,
        }
        return LinkRow(
            primary_id=values[link_spec.primary_link_col],
            secondary_id=values[link_spec.secondary_link_col],
            link_type=(
                values[link_spec.type_link_col]
                if link_spec.type_link_col is not None
                else None
            ),
            priority=(
                values[link_spec.priority_link_col]
                if link_spec.priority_link_col is not None
                else None
            ),
            extra={
                column: value
                for column, value in values.items()
                if column not in standard
            },
        )

    def _read_link_rows(
        self,
        conn: Any,
        link_spec: StorageLinkSpec,
        primary_ids: tuple[Any, ...] | None,
        *,
        link_type: Any = LINK_TYPE_UNSET,
    ) -> tuple[LinkRow, ...]:
        """
        Query and materialize link rows for optional primary IDs and a type filter.

        None primary_ids reads all sources; an empty tuple returns without executing SQL
        or validating a type filter, after building relation SQL. Nonempty IDs use one
        IN clause with bound values, without chunking. An explicit type filter requires
        a typed spec: None uses IS NULL and other values use bound equality. Order by
        primary ID, descending priority when present, then secondary ID. This helper
        does not perform the public spec validation or manage the connection.

        Example:
            ``macros._read_link_rows(conn, spec, (1,), link_type=None)`` reads
            null-typed links for source 1 when the spec supports types.


        :param conn: Connection supplied by the caller; this helper does not commit or
            close it.
        :param link_spec: StorageLinkSpec describing the physical link table and its
            capabilities.
        :param primary_ids: Tuple of primary IDs, None for all sources, or an empty
            tuple for no rows.
        :param link_type: LINK_TYPE_UNSET for no filter, None for null types, or an
            explicit bound type.
        :return: A tuple of decoded LinkRow records, possibly empty.
        """
        columns = self._link_select_columns(link_spec)
        sql = (
            f"SELECT {', '.join(_quoted(column) for column in columns)} "
            f"FROM {self._macro_table_sql(link_spec.link_table)}"
        )
        conditions: list[str] = []
        values: list[Any] = []
        if primary_ids is not None:
            if not primary_ids:
                return ()
            conditions.append(
                f"{_quoted(link_spec.primary_link_col)} IN "
                f"({', '.join('?' for _ in primary_ids)})"
            )
            values.extend(primary_ids)
        if link_type is not LINK_TYPE_UNSET:
            if not link_spec.typed or link_spec.type_link_col is None:
                raise InputIntegrityError("Cannot filter an untyped link spec by link type.")
            if link_type is None:
                conditions.append(f"{_quoted(link_spec.type_link_col)} IS NULL")
            else:
                conditions.append(f"{_quoted(link_spec.type_link_col)} = ?")
                values.append(link_type)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        order_columns = [_quoted(link_spec.primary_link_col)]
        if link_spec.priority_link_col is not None:
            order_columns.append(f"{_quoted(link_spec.priority_link_col)} DESC")
        order_columns.append(_quoted(link_spec.secondary_link_col))
        sql += " ORDER BY " + ", ".join(order_columns)
        return tuple(
            self._link_row_from_db(link_spec, columns, row)
            for row in conn.execute(sql, tuple(values))
        )

    def get_link_rows(
        self,
        link_spec: StorageLinkSpec,
        primary_id: Any,
        *,
        link_type: Any = LINK_TYPE_UNSET,
    ) -> tuple[LinkRow, ...]:
        """
        Validate the link spec and read links for one primary ID on the active
        connection.

        Use the thread-local macro transaction connection when present, otherwise the
        cached driver connection. Results follow primary ID, descending stored priority
        when available, then secondary ID. Type-filter shape is checked by the reader;
        this read does not verify membership in an allowed-type registry.

        Example:
            ``macros.get_link_rows(spec, work_id, link_type="author")`` reads
            author links when the physical spec supports a type column.


        :param link_spec: StorageLinkSpec describing the physical link table and its
            capabilities.
        :param primary_id: Primary endpoint ID bound to link-table predicates.
        :param link_type: LINK_TYPE_UNSET for no filter, None for null types, or an
            explicit bound type.
        :return: A tuple of LinkRow records in portable reader order.
        """
        link_spec = self._validate_link_spec(link_spec)
        return self._read_link_rows(
            self._macro_connection(),
            link_spec,
            (primary_id,),
            link_type=link_type,
        )

    def get_link_rows_bulk(
        self,
        link_spec: StorageLinkSpec,
        primary_ids: Iterable[Any] | None = None,
        *,
        link_type: Any = LINK_TYPE_UNSET,
    ) -> dict[Any, tuple[LinkRow, ...]]:
        """
        Group materialized link rows by primary ID, preserving explicitly requested
        empty groups.

        Validate the spec, then deduplicate supplied IDs in first-seen order. Explicit
        IDs must be hashable and appear as keys even if no rows match. With
        primary_ids=None, only sources present in the query result are included. Each
        group preserves the reader's priority-descending/secondary-ID order. The query
        is not split to fit a backend parameter limit.

        Example:
            ``macros.get_link_rows_bulk(spec, (1, 2, 1))`` includes keys 1 and 2
            once each; an unlinked source has the value ().


        :param link_spec: StorageLinkSpec describing the physical link table and its
            capabilities.
        :param primary_ids: Iterable of requested primary IDs, or None to read all
            existing links.
        :param link_type: LINK_TYPE_UNSET for no filter, None for null types, or an
            explicit bound type.
        :return: A dictionary of primary IDs to tuples of LinkRow records.
        """
        link_spec = self._validate_link_spec(link_spec)
        requested = None if primary_ids is None else tuple(dict.fromkeys(primary_ids))
        rows = self._read_link_rows(
            self._macro_connection(),
            link_spec,
            requested,
            link_type=link_type,
        )
        grouped: dict[Any, list[LinkRow]] = {}
        if requested is not None:
            grouped.update((primary_id, []) for primary_id in requested)
        for row in rows:
            grouped.setdefault(row.primary_id, []).append(row)
        return {primary_id: tuple(items) for primary_id, items in grouped.items()}

    def _live_allowed_link_types(
        self,
        link_spec: StorageLinkSpec,
    ) -> tuple[str, ...] | None:
        """
        Read the optional live type registry through the database wrapper.

        Return None without querying when allowed_types_table is absent. Otherwise
        require a callable wrapper.get_allowed_link_types and a non-None iterable of
        nonblank strings. Preserve spelling, whitespace and duplicates; an empty
        registry is valid and allows no named types. Missing wrapper support raises
        InputIntegrityError, while malformed registry results raise
        DatabaseIntegrityError.

        Example:
            For a spec without an allowed-types table, this helper returns None
            and leaves static allowed_types checks to the value validator.


        :param link_spec: StorageLinkSpec describing the physical link table and its
            capabilities.
        :return: A tuple of registry values, or None when no registry is declared.
        """

        if link_spec.allowed_types_table is None:
            return None
        get_allowed_types = getattr(
            self.db.driver_wrapper,
            "get_allowed_link_types",
            None,
        )
        if not callable(get_allowed_types):
            raise InputIntegrityError(
                "Database driver wrapper cannot read the allowed-types table "
                f"{link_spec.allowed_types_table!r}."
            )
        allowed_types = get_allowed_types(link_spec)
        if allowed_types is None:
            raise DatabaseIntegrityError(
                "Driver wrapper returned no values for declared allowed-types "
                f"table {link_spec.allowed_types_table!r}."
            )
        values = tuple(allowed_types)
        if any(
            not isinstance(value, str) or not value.strip()
            for value in values
        ):
            raise DatabaseIntegrityError(
                f"Allowed-types table {link_spec.allowed_types_table!r} "
                "contains an invalid type value."
            )
        return values

    def _validate_link_type_value(
        self,
        link_spec: StorageLinkSpec,
        link_type: Any,
        *,
        live_allowed_types: tuple[str, ...] | None,
    ) -> None:
        """
        Require a typed link and check a supplied type against static and live
        allowlists.

        Untyped specs reject every call, even with None. Typed specs accept None
        immediately. Other values must be nonblank strings; exact membership is required
        in a nonempty static allowed_types tuple and in any non-None live registry.
        Values are neither trimmed nor case-normalized. Invalid types raise
        InputIntegrityError.

        Example:
            >>> spec = StorageLinkSpec("works", "agents", "links", typed=True, allowed_types=("author",))
            >>> SQLPortableMacrosMixin()._validate_link_type_value(spec, None, live_allowed_types=())
            >>> SQLPortableMacrosMixin()._validate_link_type_value(spec, "author", live_allowed_types=("author",))


        :param link_spec: StorageLinkSpec describing the physical link table and its
            capabilities.
        :param link_type: Explicit type value to validate; None is allowed only on a
            typed spec.
        :param live_allowed_types: Registry tuple, including an empty tuple to reject
            named types, or None for no live restriction.
        :return: None when the type is valid.
        """

        if not link_spec.typed:
            raise InputIntegrityError("An untyped link cannot carry a link type.")
        if link_type is None:
            return
        if not isinstance(link_type, str):
            raise InputIntegrityError("Link types must be strings or None.")
        if not link_type.strip():
            raise InputIntegrityError("Link types cannot be blank.")
        if link_spec.allowed_types and link_type not in link_spec.allowed_types:
            raise InputIntegrityError(
                f"Link type is not allowed by the link spec: {link_type!r}"
            )
        if (
            live_allowed_types is not None
            and link_type not in live_allowed_types
        ):
            raise InputIntegrityError(
                f"Link type {link_type!r} does not exist in allowed-types "
                f"table {link_spec.allowed_types_table!r}."
            )

    def _prepare_link_value(
        self,
        link_spec: StorageLinkSpec,
        link: LinkValue,
        *,
        scoped_type: Any = LINK_TYPE_UNSET,
        live_allowed_types: tuple[str, ...] | None | object = (
            _LIVE_ALLOWED_TYPES_UNSET
        ),
    ) -> LinkValue:
        """
        Validate one desired link and return a copy with resolved type and copied
        extras.

        Require LinkValue. For typed links, an explicit replacement scope fills a None
        type or must equal an existing type. Lazily read the live registry for a named
        type only when no registry result was supplied, then apply type validation.
        Untyped links reject non-None types and any explicit scope.

        A non-None priority requires an ordered spec and a finite int/float that is not
        bool. Extras must name declared non-primary-key extra columns; also exclude the
        wrapper's discovered ID column, ignoring failures of that ID discovery. Return a
        shallow extra copy. Endpoint IDs and extra values themselves are not validated.
        Policy violations raise InputIntegrityError; coercion or mapping errors may
        propagate.

        Example:
            A ``LinkValue(7, priority=True)`` is rejected even for an ordered spec;
            ``LinkValue(7, priority=2.5)`` passes the numeric priority check.


        :param link_spec: StorageLinkSpec describing the physical link table and its
            capabilities.
        :param link: Desired LinkValue containing a secondary ID and optional link
            properties.
        :param scoped_type: LINK_TYPE_UNSET for no replacement scope, or the explicit
            scoped type (possibly None).
        :param live_allowed_types: Cached registry tuple or None; the private unset
            sentinel requests lazy lookup for a named type.
        :return: A replaced LinkValue with resolved link_type and a fresh shallow extra
            dictionary.
        """
        if not isinstance(link, LinkValue):
            raise InputIntegrityError("Links must be supplied as LinkValue instances.")
        if link_spec.typed:
            link_type = link.link_type
            if scoped_type is not LINK_TYPE_UNSET:
                if link_type is None:
                    link_type = scoped_type
                elif link_type != scoped_type:
                    raise InputIntegrityError(
                        f"Link type {link_type!r} does not match replacement scope {scoped_type!r}."
                    )
            if (
                live_allowed_types is _LIVE_ALLOWED_TYPES_UNSET
                and link_type is not None
            ):
                live_allowed_types = self._live_allowed_link_types(link_spec)
            self._validate_link_type_value(
                link_spec,
                link_type,
                live_allowed_types=(
                    None
                    if live_allowed_types is _LIVE_ALLOWED_TYPES_UNSET
                    else live_allowed_types
                ),
            )
        else:
            if link.link_type is not None or scoped_type is not LINK_TYPE_UNSET:
                raise InputIntegrityError("An untyped link cannot carry a link type.")
            link_type = None

        if not link_spec.ordered and link.priority is not None:
            raise InputIntegrityError("An unordered link cannot carry a priority.")
        if link.priority is not None and (
            isinstance(link.priority, bool)
            or not isinstance(link.priority, (int, float))
            or not math.isfinite(link.priority)
        ):
            raise InputIntegrityError("Link priorities must be finite integers or floats.")

        writable_extras = {
            column.name
            for column in link_spec.extra_link_columns
            if not column.is_primary_key
        }
        try:
            link_id_column = self.db.driver_wrapper.get_id_column(link_spec.link_table)
        except Exception:
            link_id_column = None
        if link_id_column is not None:
            writable_extras.discard(link_id_column)
        invalid_extras = sorted(set(link.extra) - writable_extras)
        if invalid_extras:
            raise InputIntegrityError(
                f"Link extras are not writable columns: {', '.join(invalid_extras)}"
            )
        return replace(link, link_type=link_type, extra=dict(link.extra))

    def _find_link_row(
        self,
        conn: Any,
        link_spec: StorageLinkSpec,
        primary_id: Any,
        link: LinkValue,
    ) -> LinkRow | None:
        """
        Find at most one row matching endpoints and, when required, its type identity.

        Always bind primary and secondary IDs. If type participates in identity, match
        it using IS NULL or bound equality. Ignore type for pair-only identities.
        Materialize all matches, reject duplicates with DatabaseIntegrityError, and
        decode the sole match. This helper assumes the spec and desired link were
        validated by its caller.

        Example:
            A type-as-identity spec can distinguish author and editor rows for
            the same endpoint pair; a pair-only spec cannot.


        :param conn: Connection supplied by the caller; this helper does not commit or
            close it.
        :param link_spec: StorageLinkSpec describing the physical link table and its
            capabilities.
        :param primary_id: Primary endpoint ID bound to link-table predicates.
        :param link: Desired LinkValue containing a secondary ID and optional link
            properties.
        :return: The matching LinkRow, or None when the identity is absent.
        """
        columns = self._link_select_columns(link_spec)
        conditions = [
            f"{_quoted(link_spec.primary_link_col)} = ?",
            f"{_quoted(link_spec.secondary_link_col)} = ?",
        ]
        values: list[Any] = [primary_id, link.secondary_id]
        if link_spec.type_part_of_identity and link_spec.type_link_col is not None:
            if link.link_type is None:
                conditions.append(f"{_quoted(link_spec.type_link_col)} IS NULL")
            else:
                conditions.append(f"{_quoted(link_spec.type_link_col)} = ?")
                values.append(link.link_type)
        sql = (
            f"SELECT {', '.join(_quoted(column) for column in columns)} "
            f"FROM {self._macro_table_sql(link_spec.link_table)} "
            f"WHERE {' AND '.join(conditions)}"
        )
        rows = list(conn.execute(sql, tuple(values)))
        if len(rows) > 1:
            raise DatabaseIntegrityError(
                f"Link identity matched multiple rows in {link_spec.link_table!r}."
            )
        if not rows:
            return None
        return self._link_row_from_db(link_spec, columns, rows[0])

    def _upsert_link(
        self,
        conn: Any,
        link_spec: StorageLinkSpec,
        primary_id: Any,
        link: LinkValue,
    ) -> LinkRow:
        """
        Update an existing logical link or insert it, then reread its stored result.

        For an existing pair-only identity, overwrite the type, including clearing it
        with None. Update priority only when explicitly supplied, and update only
        supplied extras. For a new row, insert endpoints, type when supported, optional
        priority and supplied extras; omitted fields use database defaults or
        constraints. Use ON CONFLICT DO NOTHING, then reread by logical identity.

        If another uniqueness rule prevented insertion and no identity is found, raise
        DatabaseIntegrityError. A concurrent conflicting insert may be returned without
        applying the requested updates. This is a lookup/write/read sequence on the
        caller's connection, without its own transaction, validation, priority
        allocation or database-native conflict-update clause.

        Example:
            When an existing link has priority 4, upserting a prepared value with
            priority=None keeps 4; omitted extras also retain their stored values.


        :param conn: Connection supplied by the caller; this helper does not commit or
            close it.
        :param link_spec: StorageLinkSpec describing the physical link table and its
            capabilities.
        :param primary_id: Primary endpoint ID bound to link-table predicates.
        :param link: Desired LinkValue containing a secondary ID and optional link
            properties.
        :return: The reread LinkRow; missing or duplicate logical identities raise
            DatabaseIntegrityError.
        """
        existing = self._find_link_row(conn, link_spec, primary_id, link)
        if existing is not None:
            updates: dict[str, Any] = {}
            if link_spec.type_link_col is not None and not link_spec.type_part_of_identity:
                updates[link_spec.type_link_col] = link.link_type
            if link_spec.priority_link_col is not None and link.priority is not None:
                updates[link_spec.priority_link_col] = link.priority
            updates.update(link.extra)
            if updates:
                sql = (
                    f"UPDATE {self._macro_table_sql(link_spec.link_table)} SET "
                    + ", ".join(f"{_quoted(column)} = ?" for column in updates)
                    + f" WHERE {_quoted(link_spec.primary_link_col)} = ?"
                    + f" AND {_quoted(link_spec.secondary_link_col)} = ?"
                )
                values = list(updates.values()) + [primary_id, link.secondary_id]
                if link_spec.type_part_of_identity and link_spec.type_link_col is not None:
                    if link.link_type is None:
                        sql += f" AND {_quoted(link_spec.type_link_col)} IS NULL"
                    else:
                        sql += f" AND {_quoted(link_spec.type_link_col)} = ?"
                        values.append(link.link_type)
                conn.execute(sql, tuple(values))
        else:
            insert_values: dict[str, Any] = {
                link_spec.primary_link_col: primary_id,
                link_spec.secondary_link_col: link.secondary_id,
            }
            if link_spec.type_link_col is not None:
                insert_values[link_spec.type_link_col] = link.link_type
            if link_spec.priority_link_col is not None and link.priority is not None:
                insert_values[link_spec.priority_link_col] = link.priority
            insert_values.update(link.extra)
            columns = tuple(insert_values)
            sql = (
                f"INSERT INTO {self._macro_table_sql(link_spec.link_table)} "
                f"({', '.join(_quoted(column) for column in columns)}) "
                f"VALUES ({', '.join('?' for _ in columns)}) "
                "ON CONFLICT DO NOTHING"
            )
            conn.execute(sql, tuple(insert_values.values()))

        result = self._find_link_row(conn, link_spec, primary_id, link)
        if result is None:
            raise DatabaseIntegrityError(
                f"Could not upsert link ({primary_id!r}, {link.secondary_id!r}) "
                f"in {link_spec.link_table!r}; another uniqueness rule rejected it."
            )
        return result

    def upsert_link(
        self,
        link_spec: StorageLinkSpec,
        primary_id: Any,
        link: LinkValue,
    ) -> LinkRow:
        """
        Validate one desired link and upsert it within the macro transaction.

        Validate the physical spec and, for a named type, read the live registry before
        opening this method's transaction context. Prepare the value, then perform the
        lookup/write/read operation. Priority=None preserves an existing priority but is
        omitted on insertion. Public validation does not allocate a priority or enforce
        foreign-key/cardinality rules beyond database constraints.

        Example:
            ``macros.upsert_link(spec, 1, LinkValue(7, link_type="author",
            priority=1))`` writes or updates the matching validated identity.


        :param link_spec: StorageLinkSpec describing the physical link table and its
            capabilities.
        :param primary_id: Primary endpoint ID bound to link-table predicates.
        :param link: Desired LinkValue containing a secondary ID and optional link
            properties.
        :return: The stored LinkRow after upsert; outermost completion commits, while
            nested calls join the outer boundary.
        """
        link_spec = self._validate_link_spec(link_spec)
        live_allowed_types = (
            self._live_allowed_link_types(link_spec)
            if isinstance(link, LinkValue) and link.link_type is not None
            else None
        )
        link = self._prepare_link_value(
            link_spec,
            link,
            live_allowed_types=live_allowed_types,
        )
        with self._macro_transaction() as conn:
            return self._upsert_link(conn, link_spec, primary_id, link)

    def upsert_links(
        self,
        link_spec: StorageLinkSpec,
        primary_id: Any,
        links: Iterable[LinkValue],
    ) -> tuple[LinkRow, ...]:
        """
        Validate a batch of distinct logical identities and upsert them in one macro
        transaction.

        Materialize links and read the live type registry at most once when any supplied
        LinkValue has a named type. Prepare every value before writing, and reject
        duplicate secondary/type identities with InputIntegrityError. IDs used in this
        check must be hashable. Execute writes in input order and return results in that
        order. Even an empty batch enters the transaction context. An error escaping the
        outer boundary rolls the writes back.

        Example:
            ``macros.upsert_links(spec, 1, (LinkValue(7), LinkValue(8)))`` returns
            two stored records in input order when the schema permits their defaults.


        :param link_spec: StorageLinkSpec describing the physical link table and its
            capabilities.
        :param primary_id: Primary endpoint ID bound to link-table predicates.
        :param links: Iterable of LinkValue records; duplicate logical identities are
            rejected before writing.
        :return: A tuple of stored LinkRow records in input order.
        """
        link_spec = self._validate_link_spec(link_spec)
        materialized = tuple(links)
        live_allowed_types = (
            self._live_allowed_link_types(link_spec)
            if any(
                isinstance(link, LinkValue) and link.link_type is not None
                for link in materialized
            )
            else None
        )
        prepared = tuple(
            self._prepare_link_value(
                link_spec,
                link,
                live_allowed_types=live_allowed_types,
            )
            for link in materialized
        )
        identities = [self._link_identity(link_spec, link) for link in prepared]
        if len(set(identities)) != len(identities):
            raise InputIntegrityError("upsert_links received duplicate logical link identities.")
        with self._macro_transaction() as conn:
            return tuple(
                self._upsert_link(conn, link_spec, primary_id, link)
                for link in prepared
            )

    def _delete_link_row(
        self,
        conn: Any,
        link_spec: StorageLinkSpec,
        row: LinkRow,
    ) -> None:
        """
        Delete matches for a decoded link's logical identity on the supplied connection.

        Match endpoint IDs and include type only when it participates in identity, using
        IS NULL for a null type. Other row fields do not restrict deletion. No row-count
        check, spec validation or transaction is added; malformed schema uniqueness can
        permit multiple deletions.

        Example:
            ``macros._delete_link_row(conn, spec, row)`` deletes that identity
            inside a transaction established by the caller.


        :param conn: Connection supplied by the caller; this helper does not commit or
            close it.
        :param link_spec: StorageLinkSpec describing the physical link table and its
            capabilities.
        :param row: Decoded LinkRow providing primary_id, secondary_id and optional
            identity type.
        :return: None; the execution result is discarded.
        """
        conditions = [
            f"{_quoted(link_spec.primary_link_col)} = ?",
            f"{_quoted(link_spec.secondary_link_col)} = ?",
        ]
        values: list[Any] = [row.primary_id, row.secondary_id]
        if link_spec.type_part_of_identity and link_spec.type_link_col is not None:
            if row.link_type is None:
                conditions.append(f"{_quoted(link_spec.type_link_col)} IS NULL")
            else:
                conditions.append(f"{_quoted(link_spec.type_link_col)} = ?")
                values.append(row.link_type)
        conn.execute(
            f"DELETE FROM {self._macro_table_sql(link_spec.link_table)} "
            f"WHERE {' AND '.join(conditions)}",
            tuple(values),
        )

    def _set_link_row_priority(
        self,
        conn: Any,
        link_spec: StorageLinkSpec,
        row: LinkRow,
        priority: int,
    ) -> None:
        """
        Set priority for a decoded link identity without allocating or validating the
        value.

        Assert that a priority column is declared, then bind the replacement and both
        endpoint IDs. Include type only for type-as-identity specs, handling None with
        IS NULL. This is a private write helper: it does not check finite numeric
        values, uniqueness or affected-row counts, and leaves commit/rollback to its
        caller.

        Example:
            ``macros._set_link_row_priority(conn, spec, row, 5)`` writes priority 5
            for the row's logical identity, subject to database constraints.


        :param conn: Connection supplied by the caller; this helper does not commit or
            close it.
        :param link_spec: StorageLinkSpec describing the physical link table and its
            capabilities.
        :param row: Decoded LinkRow identifying the link to update.
        :param priority: Replacement priority bound directly without numeric validation
            here.
        :return: None; the execution result is discarded.
        """
        assert link_spec.priority_link_col is not None
        conditions = [
            f"{_quoted(link_spec.primary_link_col)} = ?",
            f"{_quoted(link_spec.secondary_link_col)} = ?",
        ]
        values: list[Any] = [priority, row.primary_id, row.secondary_id]
        if link_spec.type_part_of_identity and link_spec.type_link_col is not None:
            if row.link_type is None:
                conditions.append(f"{_quoted(link_spec.type_link_col)} IS NULL")
            else:
                conditions.append(f"{_quoted(link_spec.type_link_col)} = ?")
                values.append(row.link_type)
        conn.execute(
            f"UPDATE {self._macro_table_sql(link_spec.link_table)} "
            f"SET {_quoted(link_spec.priority_link_col)} = ? "
            f"WHERE {' AND '.join(conditions)}",
            tuple(values),
        )

    def _stage_link_priorities(
        self,
        conn: Any,
        link_spec: StorageLinkSpec,
        primary_id: Any,
        rows: tuple[LinkRow, ...],
        desired_priorities: tuple[int | float, ...],
    ) -> None:
        """
        Move surviving links to distinct temporary priorities above current and desired
        values.

        Return immediately if no priority column or no rows exist. Otherwise read every
        link type for the primary ID, collect int/float priorities excluding bool,
        include zero, and start at ceil(maximum) + len(rows) + 1. Assign descending
        integers in row order. This avoids temporary collisions without requiring
        nullable columns; stored non-finite priorities or backend numeric limits can
        still fail. The caller owns the transaction and validates the rows.

        Example:
            With existing priorities 1 and 2 and two survivors, temporary priorities
            start at 5 and then 4 before replacement assigns the final values.


        :param conn: Caller-supplied connection; this helper does not commit or close
            it.
        :param link_spec: StorageLinkSpec describing the link table and
            endpoint/type/ordering columns.
        :param primary_id: Primary endpoint ID whose links are replaced or
            reprioritized.
        :param rows: Existing surviving links to stage, in assignment order.
        :param desired_priorities: Final priorities to include when calculating the
            temporary range.
        :return: None; surviving rows are updated in place in the database.
        """

        if link_spec.priority_link_col is None or not rows:
            return
        all_rows = self._read_link_rows(
            conn,
            link_spec,
            (primary_id,),
        )
        numeric_priorities = [
            priority
            for priority in (
                *(row.priority for row in all_rows),
                *desired_priorities,
                0,
            )
            if isinstance(priority, (int, float)) and not isinstance(priority, bool)
        ]
        next_temporary = math.ceil(max(numeric_priorities)) + len(rows) + 1
        for row in rows:
            self._set_link_row_priority(
                conn,
                link_spec,
                row,
                next_temporary,
            )
            next_temporary -= 1

    def _replace_links(
        self,
        conn: Any,
        link_spec: StorageLinkSpec,
        primary_id: Any,
        links: tuple[LinkValue, ...],
        *,
        link_type: Any,
        live_allowed_types: tuple[str, ...] | None | object = (
            _LIVE_ALLOWED_TYPES_UNSET
        ),
    ) -> tuple[LinkRow, ...]:
        """
        Replace one primary ID's complete link set, or one explicit type scope, on the
        supplied connection.

        An explicit scope requires type-as-identity. Validate types and prepared values,
        reject duplicate logical identities, and fill missing ordered priorities with
        count minus input index. Reject duplicate priorities within each type identity
        scope, or within the whole pair-identity set. Named types share one live
        registry result.

        Delete omitted identities, move surviving priorities aside, then upsert desired
        links and reread. Empty links clear the selected scope. Supplied extras update
        only those fields; omitted extras on surviving links remain stored. Return
        reader order (priority descending when present, then secondary ID), which may
        differ from input order. This helper validates values but relies on its caller
        for physical spec validation and transaction rollback.

        Example:
            ``macros._replace_links(conn, spec, 1, (), link_type="author")``
            clears author links while preserving other types when type participates in identity.


        :param conn: Caller-supplied connection; this helper does not commit or close
            it.
        :param link_spec: StorageLinkSpec describing the link table and
            endpoint/type/ordering columns.
        :param primary_id: Primary endpoint ID whose links are replaced or
            reprioritized.
        :param links: Materialized desired LinkValue records; endpoint identities must
            be hashable for duplicate checking.
        :param link_type: LINK_TYPE_UNSET for all types, or an explicit type (including
            None) for one identity scope.
        :param live_allowed_types: Cached live registry tuple or None; the private unset
            sentinel requests lazy lookup when a named type is present.
        :return: A tuple of the stored LinkRow records remaining in the selected scope.
        """
        if link_type is not LINK_TYPE_UNSET and not link_spec.type_part_of_identity:
            raise InputIntegrityError(
                "Type-scoped replacement requires the type column to be part "
                "of the link identity."
            )
        has_named_type = (
            link_type is not LINK_TYPE_UNSET and link_type is not None
        ) or any(
            isinstance(link, LinkValue) and link.link_type is not None
            for link in links
        )
        if (
            live_allowed_types is _LIVE_ALLOWED_TYPES_UNSET
            and has_named_type
        ):
            live_allowed_types = self._live_allowed_link_types(link_spec)
        stable_allowed_types = (
            None
            if live_allowed_types is _LIVE_ALLOWED_TYPES_UNSET
            else live_allowed_types
        )
        if link_type is not LINK_TYPE_UNSET:
            self._validate_link_type_value(
                link_spec,
                link_type,
                live_allowed_types=stable_allowed_types,
            )
        prepared = tuple(
            self._prepare_link_value(
                link_spec,
                link,
                scoped_type=link_type,
                live_allowed_types=stable_allowed_types,
            )
            for link in links
        )
        identities = [self._link_identity(link_spec, link) for link in prepared]
        if len(set(identities)) != len(identities):
            raise InputIntegrityError("replace_links received duplicate logical link identities.")

        if link_spec.ordered:
            count = len(prepared)
            prepared = tuple(
                link
                if link.priority is not None
                else replace(link, priority=count - index)
                for index, link in enumerate(prepared)
            )
            priority_keys = [
                (
                    link.link_type if link_spec.type_part_of_identity else None,
                    link.priority,
                )
                for link in prepared
            ]
            if len(set(priority_keys)) != len(priority_keys):
                raise InputIntegrityError("replace_links received duplicate priorities in one ordering scope.")

        existing = self._read_link_rows(
            conn,
            link_spec,
            (primary_id,),
            link_type=link_type,
        )
        desired_identities = {
            self._link_identity(link_spec, link)
            for link in prepared
        }
        for row in existing:
            if self._link_identity(link_spec, row) not in desired_identities:
                self._delete_link_row(conn, link_spec, row)

        surviving = tuple(
            row
            for row in existing
            if self._link_identity(link_spec, row) in desired_identities
        )
        self._stage_link_priorities(
            conn,
            link_spec,
            primary_id,
            surviving,
            tuple(
                link.priority
                for link in prepared
                if link.priority is not None
            ),
        )
        for link in prepared:
            self._upsert_link(conn, link_spec, primary_id, link)
        return self._read_link_rows(
            conn,
            link_spec,
            (primary_id,),
            link_type=link_type,
        )

    def replace_links(
        self,
        link_spec: StorageLinkSpec,
        primary_id: Any,
        links: Iterable[LinkValue],
        *,
        link_type: Any = LINK_TYPE_UNSET,
    ) -> tuple[LinkRow, ...]:
        """
        Validate and replace one source's links within a macro transaction.

        Materialize input and obtain any needed live type registry before entering the
        transaction. Replacement validates each value, removes omitted identities and
        assigns descending priorities to ordered values without explicit priorities. An
        explicit type scope requires type-as-identity; an empty iterable clears that
        scope. An error escaping the outer macro boundary rolls back the writes; nested
        calls share that boundary.

        Example:
            ``macros.replace_links(spec, 1, (LinkValue(11), LinkValue(10)))``
            assigns priorities 2 and 1 for an ordered spec without explicit priorities.


        :param link_spec: StorageLinkSpec describing the link table and
            endpoint/type/ordering columns.
        :param primary_id: Primary endpoint ID whose links are replaced or
            reprioritized.
        :param links: Iterable of desired links, fully materialized before opening the
            transaction.
        :param link_type: LINK_TYPE_UNSET for all types, or an explicit type (including
            None) for one identity scope.
        :return: Stored LinkRow records in reader order after replacement.
        """
        link_spec = self._validate_link_spec(link_spec)
        materialized = tuple(links)
        live_allowed_types = (
            self._live_allowed_link_types(link_spec)
            if (
                (link_type is not LINK_TYPE_UNSET and link_type is not None)
                or any(
                    isinstance(link, LinkValue) and link.link_type is not None
                    for link in materialized
                )
            )
            else None
        )
        with self._macro_transaction() as conn:
            return self._replace_links(
                conn,
                link_spec,
                primary_id,
                materialized,
                link_type=link_type,
                live_allowed_types=live_allowed_types,
            )

    def replace_links_bulk(
        self,
        link_spec: StorageLinkSpec,
        replacements: Mapping[Any, Iterable[LinkValue]],
        *,
        link_type: Any = LINK_TYPE_UNSET,
    ) -> dict[Any, tuple[LinkRow, ...]]:
        """
        Replace multiple sources' link sets in mapping order within one macro
        transaction.

        Materialize all iterables and read the live type registry at most once before
        entering the transaction. Each source is validated and replaced sequentially; a
        later failure can follow earlier writes, which roll back when the exception
        escapes the outer boundary. Empty link iterables clear their source/scope; an
        empty mapping still enters the transaction but performs no per-source
        validation.

        Example:
            ``macros.replace_links_bulk(spec, {1: (LinkValue(10),), 2: ()})``
            sets source 1 and clears source 2 within the same transaction boundary.


        :param link_spec: StorageLinkSpec describing the link table and
            endpoint/type/ordering columns.
        :param replacements: Mapping of primary IDs to desired LinkValue iterables.
        :param link_type: LINK_TYPE_UNSET for all types, or an explicit type (including
            None) for one identity scope.
        :return: A dictionary in input key order, mapping each source to its stored link
            tuple.
        """
        link_spec = self._validate_link_spec(link_spec)
        materialized = {
            primary_id: tuple(links)
            for primary_id, links in replacements.items()
        }
        live_allowed_types = (
            self._live_allowed_link_types(link_spec)
            if (
                (link_type is not LINK_TYPE_UNSET and link_type is not None)
                or any(
                    isinstance(link, LinkValue) and link.link_type is not None
                    for links in materialized.values()
                    for link in links
                )
            )
            else None
        )
        with self._macro_transaction() as conn:
            return {
                primary_id: self._replace_links(
                    conn,
                    link_spec,
                    primary_id,
                    links,
                    link_type=link_type,
                    live_allowed_types=live_allowed_types,
                )
                for primary_id, links in materialized.items()
            }

    def replace_owned_one_to_one_values_bulk(
        self,
        link_spec: StorageLinkSpec,
        value_column: str,
        replacements: Mapping[Any, Any | None],
    ) -> dict[Any, tuple[LinkRow, ...]]:
        """
        Update, create or unlink destination values for a one-to-one link specification.

        Require one-to-one cardinality, a writable destination column other than its ID,
        and a mapping. Empty replacements return immediately after validation. In one
        macro transaction, reject multiple existing links per source, update an existing
        destination in place, or insert a destination with RETURNING and link it. None
        removes the link while retaining the destination row for possible later pruning.

        Ownership is assumed from the supplied spec; this helper does not independently
        prove that another source cannot reference the destination. Existing-row updates
        do not check affected-row counts or apply column normalization. Backend
        constraints and exceptions escaping the outer transaction determine rollback.

        Example:
            ``macros.replace_owned_one_to_one_values_bulk(spec, "note_text",
            {1: "Revised", 2: None})`` updates or creates source 1's note and unlinks source 2.


        :param link_spec: StorageLinkSpec describing the link table and
            endpoint/type/ordering columns.
        :param value_column: Existing destination value column, distinct from the
            destination ID.
        :param replacements: Mapping from source IDs to raw destination values; None
            means unlink, not store NULL.
        :return: A dictionary of source IDs to their current link tuples; unlinked
            sources map to ().
        """

        link_spec = self._validate_link_spec(link_spec)
        if link_spec.cardinality is not LinkCardinality.ONE_TO_ONE:
            raise InputIntegrityError(
                "Owned-row replacement requires a one-to-one link spec."
            )
        value_column = _identifier(value_column, kind="value column")
        if value_column == link_spec.secondary_id_col:
            raise InputIntegrityError(
                "Owned-row replacement cannot target the destination id column."
            )
        self._validate_columns(
            link_spec.secondary_table,
            (link_spec.secondary_id_col, value_column),
        )
        if not isinstance(replacements, Mapping):
            raise InputIntegrityError("replacements must be a mapping.")
        materialized = dict(replacements)
        if not materialized:
            return {}

        with self._macro_transaction() as conn:
            existing_rows = self._read_link_rows(
                conn,
                link_spec,
                tuple(materialized),
            )
            grouped: dict[Any, list[LinkRow]] = {
                primary_id: []
                for primary_id in materialized
            }
            for row in existing_rows:
                grouped.setdefault(row.primary_id, []).append(row)

            result: dict[Any, tuple[LinkRow, ...]] = {}
            for primary_id, value in materialized.items():
                current = grouped[primary_id]
                if len(current) > 1:
                    raise DatabaseIntegrityError(
                        "One-to-one source id "
                        f"{primary_id!r} has multiple rows in "
                        f"{link_spec.link_table!r}."
                    )
                if value is None:
                    result[primary_id] = self._replace_links(
                        conn,
                        link_spec,
                        primary_id,
                        (),
                        link_type=LINK_TYPE_UNSET,
                        live_allowed_types=None,
                    )
                    continue

                if current:
                    destination_id = current[0].secondary_id
                    conn.execute(
                        f"UPDATE {self._macro_table_sql(link_spec.secondary_table)} "
                        f"SET {_quoted(value_column)} = ? "
                        f"WHERE {_quoted(link_spec.secondary_id_col)} = ?",
                        (value, destination_id),
                    )
                    result[primary_id] = tuple(current)
                    continue

                cursor = conn.execute(
                    f"INSERT INTO {self._macro_table_sql(link_spec.secondary_table)} "
                    f"({_quoted(value_column)}) VALUES (?) "
                    f"RETURNING {_quoted(link_spec.secondary_id_col)}",
                    (value,),
                )
                inserted = cursor.fetchone()
                if inserted is None:
                    raise DatabaseIntegrityError(
                        "Could not create an owned destination row in "
                        f"{link_spec.secondary_table!r}."
                    )
                destination_id = _row_value(
                    inserted,
                    0,
                    link_spec.secondary_id_col,
                )
                result[primary_id] = self._replace_links(
                    conn,
                    link_spec,
                    primary_id,
                    (LinkValue(destination_id),),
                    link_type=LINK_TYPE_UNSET,
                    live_allowed_types=None,
                )
            return result

    # ------------------------------------------------------------------------------------------------------------------
    # Policy-aware lookup values

    def _column_metadata(self, table: str, column: str) -> ColumnMetadata:
        """
        Read column policy from a callable wrapper hook or infer a fallback policy.

        Return get_column_metadata directly when available, even if its result is None
        or malformed; hook errors propagate. Otherwise obtain an optional declared
        datatype from the wrapper and pass it to infer_column_metadata. No independent
        physical-column validation occurs here.

        Example:
            >>> from types import SimpleNamespace
            >>> macros = SQLPortableMacrosMixin()
            >>> macros.db = SimpleNamespace(driver_wrapper=SimpleNamespace())
            >>> macros._column_metadata("tags", "tag").comparison_column
            'tag_phash'


        :param table: Physical table name resolved through the driver wrapper.
        :param column: Column name passed unchanged to the policy hook or inference
            helper.
        :return: The wrapper result, or an inferred ColumnMetadata when no metadata hook
            exists.
        """
        getter = getattr(self.db.driver_wrapper, "get_column_metadata", None)
        if callable(getter):
            return getter(table, column)
        declared_type_getter = getattr(
            self.db.driver_wrapper,
            "get_declared_column_datatype",
            None,
        )
        declared_type = (
            declared_type_getter(table, column)
            if callable(declared_type_getter)
            else None
        )
        return infer_column_metadata(table, column, declared_type)

    @staticmethod
    def _normalise_value(value: Any, profile: ColumnNormalizationProfile) -> Any:
        """
        Delegate comparison-value derivation to the shared normalization helper.

        The profile controls NFC, trimming/casefolding or legacy tag/title search
        normalization. Non-string values are retained after profile validation.
        Unsupported profiles raise InputIntegrityError; this operation does not consult
        a database or enforce empty-value policy.

        Example:
            >>> SQLPortableMacrosMixin._normalise_value("  Straße  ", ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD)
            'strasse'
            >>> SQLPortableMacrosMixin._normalise_value(None, ColumnNormalizationProfile.NONE) is None
            True


        :param value: Value to normalize without database lookup.
        :param profile: Supported ColumnNormalizationProfile member or its exact string
            value.
        :return: The normalized text or the original non-string value.
        """
        return normalize_identity_value(value, profile)

    @staticmethod
    def _validate_ensure_value(value: Any, metadata: ColumnMetadata) -> None:
        """
        Reject raw missing values according to the column's empty-value policy.

        NULL_IS_MISSING rejects only None. NULL_OR_BLANK_IS_MISSING also rejects strings
        whose stripped form is empty. Other values/policies pass, including values whose
        later normalization becomes empty. No datatype, merge-policy or validation-
        profile checks occur. Rejection raises InputIntegrityError.

        Example:
            >>> metadata = infer_column_metadata("tags", "tag", "TEXT")
            >>> SQLPortableMacrosMixin._validate_ensure_value("Science Fiction", metadata)
            >>> try:
            ...     SQLPortableMacrosMixin._validate_ensure_value("   ", metadata)
            ... except InputIntegrityError:
            ...     print("missing")
            missing


        :param value: Display value to match or store, subject to the column empty-value
            policy.
        :param metadata: ColumnMetadata providing normalization, comparison and empty-
            value policy.
        :return: None when the raw value is permitted.
        """
        if (
            metadata.empty_value_policy is ColumnEmptyValuePolicy.NULL_IS_MISSING
            and value is None
        ):
            raise InputIntegrityError(
                f"{metadata.table}.{metadata.column} treats NULL as a missing value."
            )
        if (
            metadata.empty_value_policy
            is ColumnEmptyValuePolicy.NULL_OR_BLANK_IS_MISSING
            and (value is None or (isinstance(value, str) and not value.strip()))
        ):
            raise InputIntegrityError(
                f"{metadata.table}.{metadata.column} treats NULL or blank text as missing."
            )

    def _find_ensured_id(
        self,
        conn: Any,
        *,
        table: str,
        id_column: str,
        value_column: str,
        value: Any,
        metadata: ColumnMetadata,
        comparison_value: Any,
        scope_values: Mapping[str, Any],
    ) -> Any | None:
        """
        Find at most one ID using prepared column policy, comparison value and scope.

        Without a comparison column, a non-NONE normalization profile scans scoped
        display values and compares their Python-normalized values. Otherwise use SQL
        equality or IS NULL on the comparison/display column; a case-insensitive display
        lookup uses LOWER for schema-bearing drivers and PYNOCASE collation otherwise. A
        stored comparison column is matched directly and is not repaired.

        Scope predicates use exact values, with None represented by IS NULL. Materialize
        all matches and raise DatabaseIntegrityError for duplicates. This helper does
        not validate identifiers against headings, normalize the input comparison value,
        or open a transaction.

        Example:
            A Unicode-normalized display lookup can match "Straße" to "STRASSE"
            by scanning in Python when no stored comparison column is declared.


        :param conn: Caller-supplied connection; this helper does not commit or close
            it.
        :param table: Physical table name resolved through the driver wrapper.
        :param id_column: Existing ID column to select; uniqueness of matches is checked
            at runtime.
        :param value_column: Display-value column whose matching policy is supplied by
            column metadata.
        :param value: Original display value used for direct SQL matching when no
            comparison column is present.
        :param metadata: ColumnMetadata providing normalization, comparison and empty-
            value policy.
        :param comparison_value: Value already normalized with
            metadata.normalization_profile.
        :param scope_values: Prepared scope-column predicates; arbitrary supplied
            columns are used directly.
        :return: The sole matching row ID, or None when no match exists.
        """
        search_column = metadata.comparison_column or value_column
        scope_conditions: list[str] = []
        scope_bindings: list[Any] = []
        for column, scoped_value in scope_values.items():
            if scoped_value is None:
                scope_conditions.append(f"{_quoted(column)} IS NULL")
            else:
                scope_conditions.append(f"{_quoted(column)} = ?")
                scope_bindings.append(scoped_value)
        if (
            metadata.comparison_column is None
            and metadata.normalization_profile is not ColumnNormalizationProfile.NONE
        ):
            sql = (
                f"SELECT {_quoted(id_column)}, {_quoted(value_column)} "
                f"FROM {self._macro_table_sql(table)}"
            )
            if scope_conditions:
                sql += " WHERE " + " AND ".join(scope_conditions)
            # Python's Unicode casefolding is deliberately authoritative.
            # SQL LOWER/NOCASE are only approximate and can discard valid
            # matches such as "Straße" versus "STRASSE".
            values = tuple(scope_bindings)
            matches = [
                _row_value(row, 0, id_column)
                for row in conn.execute(sql, values)
                if self._normalise_value(
                    _row_value(row, 1, value_column),
                    metadata.normalization_profile,
                )
                == comparison_value
            ]
            if len(matches) > 1:
                raise DatabaseIntegrityError(
                    f"Policy-aware lookup matched multiple {table}.{id_column} rows."
                )
            return None if not matches else matches[0]

        sql = (
            f"SELECT {_quoted(id_column)} FROM {self._macro_table_sql(table)} "
            f"WHERE {_quoted(search_column)}"
        )
        values: tuple[Any, ...] = ()
        search_value = comparison_value if metadata.comparison_column else value
        if search_value is None:
            sql += " IS NULL"
        elif metadata.comparison_column is not None or metadata.case_sensitive:
            sql += " = ?"
            values = (search_value,)
        elif hasattr(self._macro_driver(), "schema"):
            sql = (
                f"SELECT {_quoted(id_column)} FROM {self._macro_table_sql(table)} "
                f"WHERE LOWER({_quoted(search_column)}) = LOWER(?)"
            )
            values = (search_value,)
        else:
            sql += " = ? COLLATE PYNOCASE"
            values = (search_value,)
        if scope_conditions:
            sql += " AND " + " AND ".join(scope_conditions)
            values = (*values, *scope_bindings)
        rows = list(conn.execute(sql, values))
        if len(rows) > 1:
            raise DatabaseIntegrityError(
                f"Policy-aware lookup matched multiple {table}.{id_column} rows."
            )
        return None if not rows else _row_value(rows[0], 0, id_column)

    def _ensure_table_value(
        self,
        conn: Any,
        table: str,
        value_column: str,
        value: Any,
        *,
        id_column: str,
        additional_values: Mapping[str, Any],
    ) -> Any:
        """
        Return an existing policy-matched ID or insert and reread a new value.

        Read metadata, validate the raw value and require every declared identity scope
        column in additional_values. Matching uses only those scope fields, not every
        additional field. Existing matches keep their stored display spelling and other
        values. For insertion, copy additional values, overwrite the display field and
        any comparison column, then use ON CONFLICT DO NOTHING and reread. Missing or
        duplicate results raise DatabaseIntegrityError; backend failures propagate. No
        independent transaction or physical-column validation is added.

        Example:
            Ensuring " sciencefiction " after "Science Fiction" returns the same
            tag ID under the tag comparison policy and retains the first display spelling.


        :param conn: Caller-supplied connection; this helper does not commit or close
            it.
        :param table: Physical table name resolved through the driver wrapper.
        :param value_column: Display-value column whose matching policy is supplied by
            column metadata.
        :param value: Display value to match or store, subject to the column empty-value
            policy.
        :param id_column: Validated physical ID column used for both lookups.
        :param additional_values: Extra column/value mapping with exact schema names;
            identity scope values are required, other fields only affect insertion.
        :return: The ID of the existing or newly inserted logical value.
        """
        metadata = self._column_metadata(table, value_column)
        self._validate_ensure_value(value, metadata)
        identity_spec = self._optional_normalized_identity_spec(
            table,
            value_column,
        )
        scope_values: dict[str, Any] = {}
        if identity_spec is not None and identity_spec.scope_columns:
            missing_scope = [
                column
                for column in identity_spec.scope_columns
                if column not in additional_values
            ]
            if missing_scope:
                raise InputIntegrityError(
                    f"Ensuring {table}.{value_column} requires identity scope "
                    f"column(s): {', '.join(missing_scope)}"
                )
            scope_values = {
                column: additional_values[column]
                for column in identity_spec.scope_columns
            }
        comparison_value = self._normalise_value(value, metadata.normalization_profile)
        existing_id = self._find_ensured_id(
            conn,
            table=table,
            id_column=id_column,
            value_column=value_column,
            value=value,
            metadata=metadata,
            comparison_value=comparison_value,
            scope_values=scope_values,
        )
        if existing_id is not None:
            return existing_id

        insert_values = dict(additional_values)
        insert_values[value_column] = value
        if metadata.comparison_column is not None:
            insert_values[metadata.comparison_column] = comparison_value
        columns = tuple(insert_values)
        sql = (
            f"INSERT INTO {self._macro_table_sql(table)} "
            f"({', '.join(_quoted(column) for column in columns)}) "
            f"VALUES ({', '.join('?' for _ in columns)}) "
            "ON CONFLICT DO NOTHING"
        )
        conn.execute(sql, tuple(insert_values.values()))
        ensured_id = self._find_ensured_id(
            conn,
            table=table,
            id_column=id_column,
            value_column=value_column,
            value=value,
            metadata=metadata,
            comparison_value=comparison_value,
            scope_values=scope_values,
        )
        if ensured_id is None:
            raise DatabaseIntegrityError(
                f"Could not ensure value {value!r} in {table}.{value_column}; "
                "another uniqueness or required-column rule rejected it."
            )
        return ensured_id

    def find_table_value(
        self,
        table: str,
        value_column: str,
        value: Any,
        *,
        id_column: str | None = None,
        additional_values: Mapping[str, Any] | None = None,
    ) -> Any | None:
        """
        Find a logical value by column policy and optional identity scope without
        inserting.

        Validate the table, selected columns and raw empty-value policy, then normalize
        for matching. Every declared scope field must be supplied through
        additional_values, including explicit None for a null scope. Other additional
        fields are validated as columns but do not filter the query. Matching uses the
        same comparison/scanning rules as ensure; duplicate matches raise
        DatabaseIntegrityError. Reads reuse the active/cached connection without a new
        transaction.

        Example:
            ``macros.find_table_value("tags", "tag", " sciencefiction ")``
            returns the existing canonical tag ID, or None without adding a row.


        :param table: Physical table name resolved through the driver wrapper.
        :param value_column: Display-value column whose matching policy is supplied by
            column metadata.
        :param value: Display value to match or store, subject to the column empty-value
            policy.
        :param id_column: Physical row-ID column override, or None to use the wrapper
            default.
        :param additional_values: Extra column/value mapping with exact schema names;
            identity scope values are required, other fields only affect insertion.
        :return: A matching row ID, or None if the logical value is absent.
        """

        table = _identifier(table, kind="table name")
        value_column = _identifier(value_column, kind="column name")
        id_column = (
            _identifier(id_column, kind="id column")
            if id_column is not None
            else self.db.driver_wrapper.get_id_column(table)
        )
        additional_values = dict(additional_values or {})
        self._validate_columns(
            table,
            (id_column, value_column, *additional_values),
        )
        metadata = self._column_metadata(table, value_column)
        self._validate_ensure_value(value, metadata)
        if metadata.comparison_column is not None:
            self._validate_columns(table, (metadata.comparison_column,))

        identity_spec = self._optional_normalized_identity_spec(
            table,
            value_column,
        )
        scope_values: dict[str, Any] = {}
        if identity_spec is not None and identity_spec.scope_columns:
            missing_scope = [
                column
                for column in identity_spec.scope_columns
                if column not in additional_values
            ]
            if missing_scope:
                raise InputIntegrityError(
                    f"Finding {table}.{value_column} requires identity scope "
                    f"column(s): {', '.join(missing_scope)}"
                )
            scope_values = {
                column: additional_values[column]
                for column in identity_spec.scope_columns
            }

        comparison_value = self._normalise_value(
            value,
            metadata.normalization_profile,
        )
        return self._find_ensured_id(
            self._macro_connection(),
            table=table,
            id_column=id_column,
            value_column=value_column,
            value=value,
            metadata=metadata,
            comparison_value=comparison_value,
            scope_values=scope_values,
        )

    def ensure_table_value(
        self,
        table: str,
        value_column: str,
        value: Any,
        *,
        id_column: str | None = None,
        additional_values: Mapping[str, Any] | None = None,
    ) -> Any:
        """
        Validate column names and ensure one logical value within a macro transaction.

        Match by normalization/comparison policy and declared scope. Preserve the stored
        spelling and fields of an existing match; additional values affect insertion
        apart from required identity-scope predicates. A new row stores the original
        display value and its derived comparison key. Empty-value, duplicate-match,
        missing-scope and database errors propagate; writes roll back when the error
        escapes the outer macro boundary.

        Example:
            ``macros.ensure_table_value("genres", "genre", "Fiction",
            additional_values={"genre_parent_id": None})`` ensures a root genre in its null scope.


        :param table: Physical table name resolved through the driver wrapper.
        :param value_column: Display-value column whose matching policy is supplied by
            column metadata.
        :param value: Display value to match or store, subject to the column empty-value
            policy.
        :param id_column: Physical row-ID column override, or None to use the wrapper
            default.
        :param additional_values: Extra column/value mapping with exact schema names;
            identity scope values are required, other fields only affect insertion.
        :return: The ID of the existing or inserted logical value.
        """
        table = _identifier(table, kind="table name")
        value_column = _identifier(value_column, kind="column name")
        id_column = (
            _identifier(id_column, kind="id column")
            if id_column is not None
            else self.db.driver_wrapper.get_id_column(table)
        )
        additional_values = dict(additional_values or {})
        self._validate_columns(
            table,
            (id_column, value_column, *additional_values),
        )
        metadata = self._column_metadata(table, value_column)
        if metadata.comparison_column is not None:
            self._validate_columns(table, (metadata.comparison_column,))
        with self._macro_transaction() as conn:
            return self._ensure_table_value(
                conn,
                table,
                value_column,
                value,
                id_column=id_column,
                additional_values=additional_values,
            )

    def ensure_table_values(
        self,
        table: str,
        value_column: str,
        values: Iterable[Any],
        *,
        id_column: str | None = None,
        additional_values: Mapping[str, Any] | None = None,
    ) -> dict[Any, Any]:
        """
        Ensure a materialized sequence of hashable values using one shared insertion
        scope.

        Reject unhashable input with InputIntegrityError before writing. Validate
        columns, then process every input occurrence in one macro transaction. Equal
        dictionary keys collapse in the result, although repeated values are still
        processed; different spellings can map to the same logical ID. Shared additional
        values apply to every insertion. An empty sequence still enters the transaction,
        and a later failure rolls back earlier writes when it escapes the outer
        boundary.

        Example:
            ``macros.ensure_table_values("tags", "tag", ("Fantasy", "FANTASY"))``
            returns both original spellings as keys pointing to one ID under tag policy.


        :param table: Physical table name resolved through the driver wrapper.
        :param value_column: Display-value column whose matching policy is supplied by
            column metadata.
        :param values: Iterable of hashable display values, fully consumed before
            writing.
        :param id_column: Physical row-ID column override, or None to use the wrapper
            default.
        :param additional_values: Extra column/value mapping with exact schema names;
            identity scope values are required, other fields only affect insertion.
        :return: A dictionary from original input values to ensured IDs.
        """
        table = _identifier(table, kind="table name")
        value_column = _identifier(value_column, kind="column name")
        id_column = (
            _identifier(id_column, kind="id column")
            if id_column is not None
            else self.db.driver_wrapper.get_id_column(table)
        )
        additional_values = dict(additional_values or {})
        materialized = tuple(values)
        try:
            dict.fromkeys(materialized)
        except TypeError as exc:
            raise InputIntegrityError("Ensured values must be hashable.") from exc
        self._validate_columns(
            table,
            (id_column, value_column, *additional_values),
        )
        metadata = self._column_metadata(table, value_column)
        if metadata.comparison_column is not None:
            self._validate_columns(table, (metadata.comparison_column,))
        with self._macro_transaction() as conn:
            return {
                value: self._ensure_table_value(
                    conn,
                    table,
                    value_column,
                    value,
                    id_column=id_column,
                    additional_values=additional_values,
                )
                for value in materialized
            }

    def _optional_normalized_identity_spec(
        self,
        table: str,
        value_column: str,
    ) -> NormalizedIdentitySpec | None:
        """
        Read an optional identity declaration from the wrapper, falling back only when
        the hook is absent.

        A callable get_normalized_identity_spec is authoritative: return its result,
        including None, and propagate its errors. If no callable hook exists, use the
        built-in table/value-column declaration. This helper neither validates the spec
        nor checks whether its columns exist.

        Example:
            >>> from types import SimpleNamespace
            >>> macros = SQLPortableMacrosMixin()
            >>> macros.db = SimpleNamespace(driver_wrapper=SimpleNamespace())
            >>> macros._optional_normalized_identity_spec("tags", "tag").identity_column
            'tag_phash'
            >>> macros._optional_normalized_identity_spec("works", "work_title") is None
            True


        :param table: Physical table name resolved through the driver wrapper.
        :param value_column: Display-value column whose matching policy is supplied by
            column metadata.
        :return: The selected NormalizedIdentitySpec, or None for an undeclared pair.
        """
        getter = getattr(
            self.db.driver_wrapper,
            "get_normalized_identity_spec",
            None,
        )
        if callable(getter):
            return getter(table, value_column)
        else:
            spec = default_normalized_identity_spec(table, value_column)
        return spec

    def _normalized_identity_spec(
        self,
        table: str,
        value_column: str,
    ) -> NormalizedIdentitySpec:
        """
        Require an identity declaration and validate its display, key and scope columns.

        Raise InputIntegrityError when the optional lookup returns None. Validate
        declared columns through the wrapper and return the same spec. This requires the
        derived key column to exist already; it does not migrate a legacy schema or
        validate index uniqueness.

        Example:
            ``macros._normalized_identity_spec("tags", "tag")`` requires both
            tag and tag_phash to be present before returning the declaration.


        :param table: Physical table name resolved through the driver wrapper.
        :param value_column: Display-value column whose matching policy is supplied by
            column metadata.
        :return: The existing validated NormalizedIdentitySpec.
        """
        spec = self._optional_normalized_identity_spec(table, value_column)
        if spec is None:
            raise InputIntegrityError(
                f"{table}.{value_column} is not declared as a normalized identity."
            )
        self._validate_columns(
            spec.table,
            (
                spec.value_column,
                spec.identity_column,
                *spec.scope_columns,
            ),
        )
        return spec

    def derive_identity_value(
        self,
        table: str,
        value_column: str,
        value: Any,
    ) -> Any:
        """
        Derive a key using the database-declared identity profile after validating its
        schema.

        Normalize table/value names and require the display, derived-key and scope
        columns to exist. Then delegate to normalize_identity_value without validating
        empty-value policy or requiring scope values. Non-string values, including None,
        pass through the normalization helper; canonical lookup rejects a None key
        separately.

        Example:
            ``macros.derive_identity_value("tags", "tag", " SCIENCE FICTION ")``
            returns "sciencefiction" for the standard tag identity declaration.


        :param table: Physical table name resolved through the driver wrapper.
        :param value_column: Display-value column whose matching policy is supplied by
            column metadata.
        :param value: Display value to normalize; non-strings pass through the shared
            helper.
        :return: The derived key; no row is read or inserted by this method after schema
            lookup.
        """

        table = _identifier(table, kind="table name")
        value_column = _identifier(value_column, kind="column name")
        spec = self._normalized_identity_spec(table, value_column)
        return normalize_identity_value(value, spec.normalization_profile)

    @staticmethod
    def _identity_scope_values(
        spec: NormalizedIdentitySpec,
        scope_values: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        """
        Copy exactly the declared scope fields in declaration order.

        Require the supplied key set to equal spec.scope_columns. Missing or unexpected
        names raise InputIntegrityError with diagnostic details. Values are neither
        normalized nor copied deeply; None is an explicit scope value, not an omitted
        key.

        Example:
            >>> spec = default_normalized_identity_spec("genres", "genre")
            >>> SQLPortableMacrosMixin._identity_scope_values(spec, {"genre_parent_id": None})
            {'genre_parent_id': None}
            >>> SQLPortableMacrosMixin._identity_scope_values(default_normalized_identity_spec("tags", "tag"), None)
            {}


        :param spec: NormalizedIdentitySpec describing the display column, derived key
            and scope.
        :param scope_values: Mapping with exactly the declared scope columns, including
            explicit None values; None is valid for an unscoped identity.
        :return: A new dictionary in the declared scope-column order.
        """
        supplied = dict(scope_values or {})
        expected = set(spec.scope_columns)
        if set(supplied) != expected:
            missing = sorted(expected - set(supplied))
            unexpected = sorted(set(supplied) - expected)
            details = []
            if missing:
                details.append("missing " + ", ".join(missing))
            if unexpected:
                details.append("unexpected " + ", ".join(unexpected))
            suffix = ": " + "; ".join(details) if details else ""
            raise InputIntegrityError(
                f"Identity scope for {spec.table}.{spec.value_column} must "
                f"contain exactly {list(spec.scope_columns)!r}{suffix}"
            )
        return {
            column: supplied[column]
            for column in spec.scope_columns
        }

    def get_canonical_identity_by_key(
        self,
        table: str,
        value_column: str,
        identity_value: Any,
        *,
        scope_values: Mapping[str, Any] | None = None,
        id_column: str | None = None,
    ) -> CanonicalIdentity | None:
        """
        Resolve an already-derived key and exact scope to at most one stored identity
        record.

        Require a declared, physically present identity and a non-None key. Scope keys
        must match the declaration exactly; null scope values use IS NULL. Query the
        stored identity column without normalizing it or scanning display text, so stale
        keys remain stale. Read all matches and raise DatabaseIntegrityError for
        duplicates, even if the spec does not request uniqueness. No write or new
        transaction occurs.

        Example:
            ``macros.get_canonical_identity_by_key("tags", "tag", "sciencefiction")``
            returns the stored row ID, display spelling and identity key together.


        :param table: Physical table name resolved through the driver wrapper.
        :param value_column: Display-value column whose matching policy is supplied by
            column metadata.
        :param identity_value: Already-derived non-None identity key, bound without
            further normalization.
        :param scope_values: Mapping with exactly the declared scope columns, including
            explicit None values; None is valid for an unscoped identity.
        :param id_column: Physical row-ID column override, or None to use the wrapper
            default.
        :return: A CanonicalIdentity containing the stored fields and fresh scope
            mapping, or None.
        """

        table = _identifier(table, kind="table name")
        value_column = _identifier(value_column, kind="column name")
        spec = self._normalized_identity_spec(table, value_column)
        if identity_value is None:
            raise InputIntegrityError("A normalized identity key cannot be NULL.")
        scope = self._identity_scope_values(spec, scope_values)
        id_column = (
            _identifier(id_column, kind="id column")
            if id_column is not None
            else self.db.driver_wrapper.get_id_column(table)
        )
        self._validate_columns(table, (id_column,))

        selected = (
            id_column,
            spec.value_column,
            spec.identity_column,
            *spec.scope_columns,
        )
        conditions = [f"{_quoted(spec.identity_column)} = ?"]
        values: list[Any] = [identity_value]
        for column, value in scope.items():
            if value is None:
                conditions.append(f"{_quoted(column)} IS NULL")
            else:
                conditions.append(f"{_quoted(column)} = ?")
                values.append(value)
        sql = (
            f"SELECT {', '.join(_quoted(column) for column in selected)} "
            f"FROM {self._macro_table_sql(table)} "
            f"WHERE {' AND '.join(conditions)}"
        )
        rows = list(self._macro_connection().execute(sql, tuple(values)))
        if len(rows) > 1:
            raise DatabaseIntegrityError(
                f"Normalized identity matched multiple rows in "
                f"{table}.{spec.identity_column}."
            )
        if not rows:
            return None
        row = rows[0]
        return CanonicalIdentity(
            table=table,
            row_id=_row_value(row, 0, id_column),
            value_column=spec.value_column,
            canonical_value=_row_value(row, 1, spec.value_column),
            identity_column=spec.identity_column,
            identity_value=_row_value(row, 2, spec.identity_column),
            scope_values={
                column: _row_value(row, index + 3, column)
                for index, column in enumerate(spec.scope_columns)
            },
        )

    def get_canonical_identity(
        self,
        table: str,
        value_column: str,
        value: Any,
        *,
        scope_values: Mapping[str, Any] | None = None,
        id_column: str | None = None,
    ) -> CanonicalIdentity | None:
        """
        Normalize a display value, then resolve its stored identity record in the exact
        scope.

        Delegate schema/profile validation to derive_identity_value and key lookup to
        get_canonical_identity_by_key. Missing declarations, a None derived key,
        malformed scope and duplicate matches propagate as errors. Stored display
        spelling is returned without modification or repair.

        Example:
            ``macros.get_canonical_identity("tags", "tag", " science fiction ")``
            can resolve the record stored with display text "Science Fiction".


        :param table: Physical table name resolved through the driver wrapper.
        :param value_column: Display-value column whose matching policy is supplied by
            column metadata.
        :param value: Display value normalized by the declared identity profile; no
            empty-value policy is checked.
        :param scope_values: Mapping with exactly the declared scope columns, including
            explicit None values; None is valid for an unscoped identity.
        :param id_column: Physical row-ID column override, or None to use the wrapper
            default.
        :return: The matching CanonicalIdentity, or None if no stored key matches.
        """

        identity_value = self.derive_identity_value(table, value_column, value)
        return self.get_canonical_identity_by_key(
            table,
            value_column,
            identity_value,
            scope_values=scope_values,
            id_column=id_column,
        )

    def get_canonical_value_by_identity(
        self,
        table: str,
        value_column: str,
        identity_value: Any,
        *,
        scope_values: Mapping[str, Any] | None = None,
    ) -> Any | None:
        """
        Resolve an already-derived key and return only its stored display value.

        Use the wrapper's default row-ID column and the same exact-scope, non-None-key
        and duplicate-match rules as get_canonical_identity_by_key. No additional
        normalization or write occurs. None represents either no match or a stored null
        display value.

        Example:
            ``macros.get_canonical_value_by_identity("tags", "tag", "sciencefiction")``
            returns the stored display spelling when that key exists.


        :param table: Physical table name resolved through the driver wrapper.
        :param value_column: Display-value column whose matching policy is supplied by
            column metadata.
        :param identity_value: Already-derived non-None identity key, bound without
            further normalization.
        :param scope_values: Mapping with exactly the declared scope columns, including
            explicit None values; None is valid for an unscoped identity.
        :return: The canonical display value, or None; row identity and scope fields are
            discarded.
        """

        identity = self.get_canonical_identity_by_key(
            table,
            value_column,
            identity_value,
            scope_values=scope_values,
        )
        return None if identity is None else identity.canonical_value

    def get_canonical_value(
        self,
        table: str,
        value_column: str,
        value: Any,
        *,
        scope_values: Mapping[str, Any] | None = None,
    ) -> Any | None:
        """
        Normalize and resolve a display value, returning only the stored canonical
        spelling.

        Delegate to get_canonical_identity with the wrapper's default row-ID column.
        Declaration, scope and duplicate-match failures propagate. The result is not
        inserted or rewritten; None can mean no match or a stored null display value.

        Example:
            ``macros.get_canonical_value("tags", "tag", " SCIENCE FICTION ")``
            returns "Science Fiction" if that spelling owns the matching stored key.


        :param table: Physical table name resolved through the driver wrapper.
        :param value_column: Display-value column whose matching policy is supplied by
            column metadata.
        :param value: Display value normalized by the declared identity profile; no
            empty-value policy is checked.
        :param scope_values: Mapping with exactly the declared scope columns, including
            explicit None values; None is valid for an unscoped identity.
        :return: The stored canonical value, or None.
        """

        identity = self.get_canonical_identity(
            table,
            value_column,
            value,
            scope_values=scope_values,
        )
        return None if identity is None else identity.canonical_value

    def _existing_normalized_identity_specs(
        self,
    ) -> tuple[
        tuple[NormalizedIdentitySpec, ...],
        dict[str, set[str]],
    ]:
        """
        Collect applicable built-in and wrapper identity declarations with a column
        snapshot.

        Enumerate all wrapper tables and headings. Include declarations only when their
        table, display column and scope columns exist; the derived key column may be
        missing for migration. Wrapper declarations override built-ins for the same
        table/value pair. If iterating wrapper declarations raises
        DatabaseIntegrityError, ignore that entire declared set; other errors propagate.
        Return specs sorted by table/value-column names.

        Example:
            A legacy tags table containing tag but lacking tag_phash is included
            so migration can add its derived column.


        :return: A pair of the applicable spec tuple and a dictionary of physical
            column-name sets.
        """
        tables = set(self.db.driver_wrapper.get_tables())
        columns_by_table = {
            table: set(self._column_names(table))
            for table in tables
        }
        specs: dict[tuple[str, str], NormalizedIdentitySpec] = {}
        for spec in iter_normalized_identity_defaults():
            columns = columns_by_table.get(spec.table)
            if columns is None:
                continue
            if spec.value_column not in columns:
                continue
            if not set(spec.scope_columns) <= columns:
                continue
            specs[(spec.table, spec.value_column)] = spec

        iterator = getattr(
            self.db.driver_wrapper,
            "iter_normalized_identity_specs",
            None,
        )
        if callable(iterator):
            try:
                declared_specs = tuple(iterator())
            except DatabaseIntegrityError:
                declared_specs = ()
            for spec in declared_specs:
                columns = columns_by_table.get(spec.table)
                if columns is None:
                    continue
                required = {spec.value_column, *spec.scope_columns}
                if required <= columns:
                    specs[(spec.table, spec.value_column)] = spec
        return (
            tuple(specs[key] for key in sorted(specs)),
            columns_by_table,
        )

    def _inspect_normalized_identities(
        self,
        conn: Any,
        specs: tuple[NormalizedIdentitySpec, ...],
        columns_by_table: Mapping[str, set[str]],
    ) -> tuple[
        NormalizedIdentityMigrationReport,
        tuple[tuple[NormalizedIdentitySpec, str, Any, Any], ...],
    ]:
        """
        Scan declared display values and report stale keys and scoped uniqueness
        collisions.

        Read each table in row-ID order, derive non-null display keys and compare them
        with any existing identity column. Collect updates for unequal keys, or non-null
        derived keys when the column is absent. Null derived identities do not form
        collision groups; non-null identities are grouped with their exact scope values,
        including None. Only unique specs report groups containing multiple rows. Group
        keys must be hashable. This inspection issues no writes and does not establish a
        snapshot transaction.

        Example:
            Two tag rows whose display values both derive to "duplicatetag" appear
            in a collision report before migration attempts schema changes.


        :param conn: Caller-supplied connection; this helper does not commit or close
            it.
        :param specs: Applicable declarations to inspect in supplied order.
        :param columns_by_table: Snapshot mapping physical table names to sets of their
            column names.
        :return: A report with rows_updated=0 and a tuple of (spec, id_column, row_id,
            desired_key) updates.
        """
        rows_examined = 0
        rows_needing_update = 0
        updates: list[tuple[NormalizedIdentitySpec, str, Any, Any]] = []
        collisions: list[NormalizedIdentityCollision] = []

        for spec in specs:
            columns = columns_by_table[spec.table]
            has_identity_column = spec.identity_column in columns
            id_column = self.db.driver_wrapper.get_id_column(spec.table)
            selected = [
                id_column,
                spec.value_column,
                *(
                    (spec.identity_column,)
                    if has_identity_column
                    else ()
                ),
                *spec.scope_columns,
            ]
            sql = (
                f"SELECT {', '.join(_quoted(column) for column in selected)} "
                f"FROM {self._macro_table_sql(spec.table)} "
                f"ORDER BY {_quoted(id_column)}"
            )
            identity_offset = 2 if has_identity_column else None
            scope_offset = 3 if has_identity_column else 2
            groups: dict[
                tuple[tuple[Any, ...], Any],
                list[tuple[Any, Any]],
            ] = {}
            for row in conn.execute(sql):
                rows_examined += 1
                row_id = _row_value(row, 0, id_column)
                canonical_value = _row_value(row, 1, spec.value_column)
                current_identity = (
                    _row_value(row, identity_offset, spec.identity_column)
                    if identity_offset is not None
                    else None
                )
                desired_identity = (
                    None
                    if canonical_value is None
                    else normalize_identity_value(
                        canonical_value,
                        spec.normalization_profile,
                    )
                )
                scope_tuple = tuple(
                    _row_value(row, scope_offset + index, column)
                    for index, column in enumerate(spec.scope_columns)
                )
                if desired_identity is not None:
                    groups.setdefault(
                        (scope_tuple, desired_identity),
                        [],
                    ).append((row_id, canonical_value))
                if (
                    not has_identity_column
                    and desired_identity is not None
                ) or (
                    has_identity_column
                    and current_identity != desired_identity
                ):
                    rows_needing_update += 1
                    updates.append(
                        (spec, id_column, row_id, desired_identity)
                    )

            if spec.unique:
                for (scope_tuple, identity_value), members in groups.items():
                    if len(members) < 2:
                        continue
                    collisions.append(
                        NormalizedIdentityCollision(
                            table=spec.table,
                            value_column=spec.value_column,
                            identity_column=spec.identity_column,
                            identity_value=identity_value,
                            scope_values=dict(
                                zip(spec.scope_columns, scope_tuple)
                            ),
                            row_ids=tuple(member[0] for member in members),
                            canonical_values=tuple(
                                member[1] for member in members
                            ),
                        )
                    )

        return (
            NormalizedIdentityMigrationReport(
                declarations_checked=len(specs),
                rows_examined=rows_examined,
                rows_needing_update=rows_needing_update,
                rows_updated=0,
                collisions=tuple(collisions),
            ),
            tuple(updates),
        )

    def audit_normalized_identities(self) -> NormalizedIdentityMigrationReport:
        """
        Report current derived-key drift and collisions without changing schema or rows.

        Discover applicable declarations, then inspect them on the active/cached
        connection. Missing identity columns are eligible for backfill reporting.
        Collisions are returned as records rather than raised; metadata, SQL or
        normalization failures still propagate. No independent snapshot or transaction
        is created.

        Example:
            ``report = macros.audit_normalized_identities()`` exposes
            report.collisions and report.rows_needing_update before a migration.


        :return: A NormalizedIdentityMigrationReport with scan counts, collisions and
            rows_updated=0.
        """

        specs, columns_by_table = self._existing_normalized_identity_specs()
        report, _updates = self._inspect_normalized_identities(
            self._macro_connection(),
            specs,
            columns_by_table,
        )
        return report

    @staticmethod
    def _normalized_identity_index_name(
        spec: NormalizedIdentitySpec,
        suffix: str,
    ) -> str:
        """
        Choose a deterministic index name, preserving known schema names where
        available.

        Use the built-in table/value/suffix mapping first. Otherwise compose a name from
        the table and identity column; names longer than 60 characters use a ten-hex
        SHA-1 fragment and truncated table/suffix text. The limit counts Python
        characters, not encoded bytes. This naming helper does not validate SQL
        identifier spelling.

        Example:
            >>> spec = default_normalized_identity_spec("tags", "tag")
            >>> SQLPortableMacrosMixin._normalized_identity_index_name(spec, "global")
            'idx_tags_unique_phash'


        :param spec: NormalizedIdentitySpec describing the display column, derived key
            and scope.
        :param suffix: Identity-scope suffix, normally global or scope_<binary mask>.
        :return: The chosen index name string, at most 60 characters for generated
            names.
        """
        schema_names = {
            ("backup_policies", "backup_policy_name", "global"):
                "idx_backup_policies_unique_name_norm",
            ("custom_columns", "custom_column_label", "global"):
                "idx_custom_columns_unique_label_norm",
            ("custom_columns", "custom_column_name", "global"):
                "idx_custom_columns_unique_name_norm",
            ("genres", "genre", "scope_0"):
                "idx_genres_unique_root_phash",
            ("genres", "genre", "scope_1"):
                "idx_genres_unique_parent_phash",
            ("labels", "label_text", "global"):
                "idx_labels_unique_norm",
            ("replication_policies", "replication_policy_name", "global"):
                "idx_replication_policies_unique_name_norm",
            ("series", "series", "global"):
                "idx_series_unique_name_norm",
            ("subjects", "subject", "scope_0"):
                "idx_subjects_unique_root_phash",
            ("subjects", "subject", "scope_1"):
                "idx_subjects_unique_parent_phash",
            ("tags", "tag", "global"):
                "idx_tags_unique_phash",
        }
        declared_name = schema_names.get(
            (spec.table, spec.value_column, suffix)
        )
        if declared_name is not None:
            return declared_name
        name = (
            f"uidx_{spec.table}_{spec.identity_column}_identity_{suffix}"
        )
        if len(name) <= 60:
            return name
        digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:10]
        return f"uidx_{spec.table[:24]}_{digest}_{suffix}"[:60]

    def _normalized_identity_index_statements(
        self,
        spec: NormalizedIdentitySpec,
    ) -> tuple[tuple[str, str], ...]:
        # SQLite's CREATE INDEX grammar does not accept a schema-qualified
        # table after ON, even though ordinary SELECT/UPDATE statements do.
        """
        Build partial unique-index SQL for every null/non-null identity-scope pattern.

        An unscoped identity gets one index excluding null keys. A scope with n columns
        gets 2**n indexes: each includes the non-null scope columns and the key, with
        predicates fixing that null pattern. This makes null scope values participate in
        uniqueness. Use driver-qualified tables for schema-bearing drivers and
        unqualified SQLite table names for CREATE INDEX. The helper returns SQL without
        executing it and does not inspect spec.unique; the migration caller decides
        whether indexes are needed.

        Example:
            >>> from types import SimpleNamespace
            >>> macros = SQLPortableMacrosMixin()
            >>> macros.db = SimpleNamespace(driver=SimpleNamespace())
            >>> spec = default_normalized_identity_spec("genres", "genre")
            >>> tuple(name for name, sql in macros._normalized_identity_index_statements(spec))
            ('idx_genres_unique_root_phash', 'idx_genres_unique_parent_phash')


        :param spec: NormalizedIdentitySpec describing the display column, derived key
            and scope.
        :return: An ordered tuple of (index_name, CREATE UNIQUE INDEX IF NOT EXISTS
            statement) pairs.
        """
        table_sql = (
            self._macro_table_sql(spec.table)
            if hasattr(self._macro_driver(), "schema")
            else _quoted(spec.table)
        )
        key_sql = _quoted(spec.identity_column)
        if not spec.scope_columns:
            name = self._normalized_identity_index_name(spec, "global")
            return (
                (
                    name,
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {_quoted(name)} "
                    f"ON {table_sql} ({key_sql}) "
                    f"WHERE {key_sql} IS NOT NULL",
                ),
            )

        statements: list[tuple[str, str]] = []
        # One partial index per NULL/non-NULL scope pattern makes NULL a real
        # scope value on both SQLite and PostgreSQL.  A plain composite UNIQUE
        # index would permit duplicate root taxonomy rows.
        for mask in range(1 << len(spec.scope_columns)):
            non_null_columns = [
                column
                for index, column in enumerate(spec.scope_columns)
                if mask & (1 << index)
            ]
            suffix = f"scope_{mask:0{len(spec.scope_columns)}b}"
            name = self._normalized_identity_index_name(spec, suffix)
            indexed = [*non_null_columns, spec.identity_column]
            conditions = [f"{key_sql} IS NOT NULL"]
            for index, column in enumerate(spec.scope_columns):
                conditions.append(
                    f"{_quoted(column)} IS "
                    + ("NOT NULL" if mask & (1 << index) else "NULL")
                )
            statements.append(
                (
                    name,
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {_quoted(name)} "
                    f"ON {table_sql} "
                    f"({', '.join(_quoted(column) for column in indexed)}) "
                    f"WHERE {' AND '.join(conditions)}",
                )
            )
        return tuple(statements)

    @staticmethod
    def _column_metadata_values(
        metadata: ColumnMetadata,
    ) -> tuple[Any, ...]:
        """
        Serialize the nine policy fields used by the column-metadata catalog insert.

        Convert case sensitivity to int and enum policy members to their stored values.
        Keep table, column and comparison-column names unchanged. Formatting/display
        options and any unrelated metadata fields are omitted; malformed enum fields can
        raise when accessed.

        Example:
            >>> metadata = infer_column_metadata("tags", "tag", "TEXT")
            >>> values = SQLPortableMacrosMixin._column_metadata_values(metadata)
            >>> len(values), values[:2], values[5]
            (9, ('tags', 'tag'), 'tag_phash')


        :param metadata: ColumnMetadata providing normalization, comparison and empty-
            value policy.
        :return: A tuple ordered as table, column, case sensitivity, semantic role,
            normalization, comparison column, empty policy, merge policy and validation
            profile.
        """
        return (
            metadata.table,
            metadata.column,
            int(metadata.case_sensitive),
            metadata.semantic_role.value,
            metadata.normalization_profile.value,
            metadata.comparison_column,
            metadata.empty_value_policy.value,
            metadata.merge_policy.value,
            metadata.validation_profile.value,
        )

    def _seed_normalized_identity_column_metadata(
        self,
        conn: Any,
        specs: tuple[NormalizedIdentitySpec, ...],
        columns_by_table: Mapping[str, set[str]],
    ) -> None:
        """
        Seed identity-related policy rows when the column-metadata catalog has all
        required fields.

        Return without writing if the catalog or any of its nine required columns is
        absent. For display columns, infer TEXT metadata and apply the identity
        profile/key column. Existing display rows update only case sensitivity,
        normalization and comparison column. Identity-column and normalized-identity-
        catalog rows use insert-on-conflict-do-nothing, preserving existing policy. The
        caller owns transaction and cache invalidation; the column snapshot is read
        without mutation.

        Example:
            A pre-existing tag display policy keeps its merge and validation settings
            while its comparison column is updated to tag_phash.


        :param conn: Caller-supplied connection; this helper does not commit or close
            it.
        :param specs: Applicable identity declarations whose display/key metadata should
            be seeded.
        :param columns_by_table: Snapshot mapping physical table names to sets of their
            column names.
        :return: None; eligible metadata rows are inserted or selectively updated.
        """
        catalog_columns = columns_by_table.get(COLUMN_METADATA_TABLE)
        required_catalog_columns = {
            "column_metadata_table_name",
            "column_metadata_column_name",
            "column_metadata_case_sensitive",
            "column_metadata_semantic_role",
            "column_metadata_normalization_profile",
            "column_metadata_comparison_column",
            "column_metadata_empty_value_policy",
            "column_metadata_merge_policy",
            "column_metadata_validation_profile",
        }
        if (
            catalog_columns is None
            or not required_catalog_columns <= catalog_columns
        ):
            return

        catalog_sql = self._macro_table_sql(COLUMN_METADATA_TABLE)
        for spec in specs:
            expected_case_sensitive = spec.normalization_profile in {
                ColumnNormalizationProfile.NONE,
                ColumnNormalizationProfile.UNICODE_NFC,
            }
            display_metadata = replace(
                infer_column_metadata(
                    spec.table,
                    spec.value_column,
                    "TEXT",
                ),
                case_sensitive=expected_case_sensitive,
                normalization_profile=spec.normalization_profile,
                comparison_column=spec.identity_column,
            )
            conn.execute(
                f"""
                INSERT INTO {catalog_sql} (
                  column_metadata_table_name,
                  column_metadata_column_name,
                  column_metadata_case_sensitive,
                  column_metadata_semantic_role,
                  column_metadata_normalization_profile,
                  column_metadata_comparison_column,
                  column_metadata_empty_value_policy,
                  column_metadata_merge_policy,
                  column_metadata_validation_profile
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (
                  column_metadata_table_name,
                  column_metadata_column_name
                ) DO UPDATE SET
                  column_metadata_case_sensitive =
                    excluded.column_metadata_case_sensitive,
                  column_metadata_normalization_profile =
                    excluded.column_metadata_normalization_profile,
                  column_metadata_comparison_column =
                    excluded.column_metadata_comparison_column
                """,
                self._column_metadata_values(display_metadata),
            )

            identity_metadata = infer_column_metadata(
                spec.table,
                spec.identity_column,
                "TEXT",
            )
            conn.execute(
                f"""
                INSERT INTO {catalog_sql} (
                  column_metadata_table_name,
                  column_metadata_column_name,
                  column_metadata_case_sensitive,
                  column_metadata_semantic_role,
                  column_metadata_normalization_profile,
                  column_metadata_comparison_column,
                  column_metadata_empty_value_policy,
                  column_metadata_merge_policy,
                  column_metadata_validation_profile
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (
                  column_metadata_table_name,
                  column_metadata_column_name
                ) DO NOTHING
                """,
                self._column_metadata_values(identity_metadata),
            )

        for column in sorted(
            columns_by_table.get(NORMALIZED_IDENTITIES_TABLE, ())
        ):
            catalog_metadata = infer_column_metadata(
                NORMALIZED_IDENTITIES_TABLE,
                column,
                (
                    "INTEGER"
                    if column == "normalized_identity_unique"
                    else "TEXT"
                ),
                is_primary_key=column in {
                    "normalized_identity_table_name",
                    "normalized_identity_value_column",
                },
            )
            conn.execute(
                f"""
                INSERT INTO {catalog_sql} (
                  column_metadata_table_name,
                  column_metadata_column_name,
                  column_metadata_case_sensitive,
                  column_metadata_semantic_role,
                  column_metadata_normalization_profile,
                  column_metadata_comparison_column,
                  column_metadata_empty_value_policy,
                  column_metadata_merge_policy,
                  column_metadata_validation_profile
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (
                  column_metadata_table_name,
                  column_metadata_column_name
                ) DO NOTHING
                """,
                self._column_metadata_values(catalog_metadata),
            )

    def migrate_normalized_identities(self) -> NormalizedIdentityMigrationReport:
        """
        Install identity declarations, backfill derived keys and request scoped unique
        indexes.

        Discover applicable specs, then inspect within a macro transaction before schema
        writes. Collisions raise DatabaseIntegrityError with up to ten rendered groups.
        Otherwise create the declaration catalog, add missing nullable TEXT identity
        columns, seed available column metadata, update stale keys and upsert
        declarations. Unique specs receive partial indexes for each null-scope pattern.
        Existing key columns and same-name indexes are not rebuilt or checked for
        compatible definitions.

        Outermost macro completion commits and invalidates caches; escaping errors roll
        back that boundary. Nested calls share the caller's transaction and have no
        independent rollback boundary. The report lists every requested IF NOT EXISTS
        index, including one that already existed; rows_updated counts planned update
        statements, not measured affected rows.

        Example:
            After resolving report.collisions from ``macros.audit_normalized_identities()``,
            ``macros.migrate_normalized_identities()`` installs declarations and backfills keys.


        :return: A collision-free NormalizedIdentityMigrationReport with scan/update
            counts, added columns and requested index names.
        """

        specs, columns_by_table = self._existing_normalized_identity_specs()
        columns_added: list[str] = []
        indexes_created: list[str] = []
        with self._macro_transaction() as conn:
            if (
                not hasattr(self._macro_driver(), "schema")
                and hasattr(conn, "in_transaction")
                and not conn.in_transaction
            ):
                # sqlite3's connection context only commits/rolls back a
                # transaction that has already begun.  Start one explicitly
                # so DDL and backfill are one unit.
                conn.execute("BEGIN")
            report, updates = self._inspect_normalized_identities(
                conn,
                specs,
                columns_by_table,
            )
            if report.collisions:
                rendered = "; ".join(
                    (
                        f"{collision.table}.{collision.value_column} "
                        f"key={collision.identity_value!r} "
                        f"scope={dict(collision.scope_values)!r} "
                        f"rows={collision.row_ids!r}"
                    )
                    for collision in report.collisions[:10]
                )
                raise DatabaseIntegrityError(
                    "Normalized identity migration found collisions; "
                    f"no changes were applied. {rendered}"
                )

            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS
                {self._macro_table_sql(NORMALIZED_IDENTITIES_TABLE)} (
                  normalized_identity_table_name TEXT NOT NULL,
                  normalized_identity_value_column TEXT NOT NULL,
                  normalized_identity_key_column TEXT NOT NULL,
                  normalized_identity_normalization_profile TEXT NOT NULL,
                  normalized_identity_scope_columns_json TEXT NOT NULL DEFAULT '[]',
                  normalized_identity_unique INTEGER NOT NULL DEFAULT 1,
                  PRIMARY KEY (
                    normalized_identity_table_name,
                    normalized_identity_value_column
                  ),
                  CHECK (normalized_identity_unique IN (0, 1))
                )
                """
            )
            columns_by_table[NORMALIZED_IDENTITIES_TABLE] = {
                "normalized_identity_table_name",
                "normalized_identity_value_column",
                "normalized_identity_key_column",
                "normalized_identity_normalization_profile",
                "normalized_identity_scope_columns_json",
                "normalized_identity_unique",
            }
            for spec in specs:
                columns = columns_by_table[spec.table]
                if spec.identity_column in columns:
                    continue
                conn.execute(
                    f"ALTER TABLE {self._macro_table_sql(spec.table)} "
                    f"ADD COLUMN {_quoted(spec.identity_column)} TEXT NULL"
                )
                columns.add(spec.identity_column)
                columns_added.append(
                    f"{spec.table}.{spec.identity_column}"
                )

            self._seed_normalized_identity_column_metadata(
                conn,
                specs,
                columns_by_table,
            )

            for spec, id_column, row_id, identity_value in updates:
                conn.execute(
                    f"UPDATE {self._macro_table_sql(spec.table)} "
                    f"SET {_quoted(spec.identity_column)} = ? "
                    f"WHERE {_quoted(id_column)} = ?",
                    (identity_value, row_id),
                )

            for spec in specs:
                values = normalized_identity_db_values(spec)
                conn.execute(
                    f"""
                    INSERT INTO
                    {self._macro_table_sql(NORMALIZED_IDENTITIES_TABLE)} (
                      normalized_identity_table_name,
                      normalized_identity_value_column,
                      normalized_identity_key_column,
                      normalized_identity_normalization_profile,
                      normalized_identity_scope_columns_json,
                      normalized_identity_unique
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (
                      normalized_identity_table_name,
                      normalized_identity_value_column
                    ) DO UPDATE SET
                      normalized_identity_key_column =
                        excluded.normalized_identity_key_column,
                      normalized_identity_normalization_profile =
                        excluded.normalized_identity_normalization_profile,
                      normalized_identity_scope_columns_json =
                        excluded.normalized_identity_scope_columns_json,
                      normalized_identity_unique =
                        excluded.normalized_identity_unique
                    """,
                    values,
                )
                if not spec.unique:
                    continue
                for index_name, statement in (
                    self._normalized_identity_index_statements(spec)
                ):
                    conn.execute(statement)
                    indexes_created.append(index_name)

        return NormalizedIdentityMigrationReport(
            declarations_checked=report.declarations_checked,
            rows_examined=report.rows_examined,
            rows_needing_update=report.rows_needing_update,
            rows_updated=len(updates),
            columns_added=tuple(columns_added),
            indexes_created=tuple(indexes_created),
            collisions=(),
        )

    # ------------------------------------------------------------------------------------------------------------------
    # Temporary value tables

    @contextmanager
    def temporary_value_table(
        self,
        values: Iterable[Any],
        *,
        column: str = "value",
        declared_type: str = "TEXT",
        prefix: str = "liuxin_values",
    ) -> AbstractContextManager[str]:
        """
        Return a context manager that populates a uniquely named connection-local
        temporary table.

        On entry, validate a simple column/prefix, require a prefix of at most 30
        characters, and accept only BLOB, INTEGER, NUMERIC, REAL or TEXT after
        trimming/uppercasing. Schema-bearing drivers map BLOB to BYTEA. Append a UUID to
        the prefix, create the table and consume values through executemany, preserving
        duplicates and nulls subject to backend conversion. Yield the unqualified name.

        Creation/population and cleanup use the connection's own context manager, not
        the macro transaction manager; backend commit/rollback behavior can affect
        pending work, so this is not an isolated nested macro savepoint. Finally drop
        the table even if input consumption or the body fails. A failed drop triggers
        rollback (ignoring rollback errors) and one retry; a retry failure propagates
        and can mask an earlier error. The connection stays open.

        Example:
            >>> import sqlite3
            >>> from types import SimpleNamespace
            >>> conn = sqlite3.connect(":memory:")
            >>> macros = SQLPortableMacrosMixin()
            >>> macros.db = SimpleNamespace(driver=SimpleNamespace(conn=conn))
            >>> with macros.temporary_value_table(("a", "a", None)) as table:
            ...     print(conn.execute(f'SELECT COUNT(*) FROM temp."{table}"').fetchone()[0])
            3
            >>> conn.execute("SELECT COUNT(*) FROM sqlite_temp_master WHERE name=?", (table,)).fetchone()[0]
            0
            >>> conn.close()


        :param values: Iterable consumed once during context entry, one value per
            inserted row.
        :param column: Validated temporary-column name, default value.
        :param declared_type: Allowlisted backend datatype name; values themselves are
            not pre-coerced.
        :param prefix: Validated table-name prefix, at most 30 characters before the
            UUID suffix.
        :return: A context manager yielding the generated table-name string while the
            table exists.
        """
        column = _identifier(column, kind="temporary column")
        prefix = _identifier(prefix, kind="temporary table prefix")
        if len(prefix) > 30:
            raise InputIntegrityError(
                "Temporary table prefixes cannot exceed 30 characters."
            )
        declared_type = str(declared_type).strip().upper()
        if declared_type not in _TEMP_TYPES:
            raise InputIntegrityError(
                f"Unsupported temporary-table datatype: {declared_type!r}"
            )
        backend_declared_type = self._macro_temporary_declared_type(declared_type)
        table = f"{prefix}_{uuid.uuid4().hex}"
        table_sql = self._macro_temporary_table_sql(table)
        conn = self._macro_connection()
        try:
            with conn:
                conn.execute(
                    f"CREATE TEMP TABLE {_quoted(table)} "
                    f"({_quoted(column)} {backend_declared_type})"
                )
                conn.executemany(
                    f"INSERT INTO {table_sql} ({_quoted(column)}) VALUES (?)",
                    ((value,) for value in values),
                )
            yield table
        finally:
            try:
                with conn:
                    conn.execute(f"DROP TABLE IF EXISTS {table_sql}")
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                with conn:
                    conn.execute(f"DROP TABLE IF EXISTS {table_sql}")

    def temporary_id_table(
        self,
        values: Iterable[Any],
        *,
        prefix: str = "liuxin_ids",
    ) -> AbstractContextManager[str]:
        """
        Return the temporary-value context manager configured with an INTEGER column
        named id.

        Forward values and prefix without eagerly consuming or validating them. Context
        entry, connection-level transaction side effects, cleanup retry and error
        behavior are those of temporary_value_table. Duplicates and None are not
        rejected, and Python integer types are not independently enforced.

        Example:
            ``with macros.temporary_id_table((10, 11, 11)) as table: ...``
            creates three rows in the id column for the duration of the context.


        :param values: Iterable of backend-compatible ID values, consumed on context
            entry.
        :param prefix: Simple temporary-table prefix, checked on entry and limited to 30
            characters.
        :return: The delegated context manager yielding an unqualified temporary-table
            name.
        """
        return self.temporary_value_table(
            values,
            column="id",
            declared_type="INTEGER",
            prefix=prefix,
        )

    # ------------------------------------------------------------------------------------------------------------------
    # Orphan pruning

    def _unreferenced_ids(
        self,
        conn: Any,
        table: str,
        link_specs: tuple[StorageLinkSpec, ...],
        *,
        id_column: str,
        protected_ids: tuple[Any, ...],
    ) -> tuple[Any, ...]:
        """
        Select IDs with no references through any supplied endpoint column and no
        protection match.

        Validate each link spec and include every endpoint that names the target table,
        including both sides of self-links. Refuse pruning with InputIntegrityError when
        no supplied spec references the table. Combine correlated NOT EXISTS predicates
        and optional NOT IN protection, then order by target ID. Only supplied links are
        considered; this does not discover every foreign key or prove global
        unreferenced status. The protection predicate is not chunked for backend
        parameter limits.

        Example:
            With ID 1 linked and ID 3 protected, a target table containing IDs 1, 2, 3
            yields (2,) when the supplied specs describe all relevant references.


        :param conn: Caller-supplied connection; this helper does not commit or close
            it.
        :param table: Physical table name resolved through the driver wrapper.
        :param link_specs: Supplied link declarations to check for references; both
            endpoints of a self-link are considered.
        :param id_column: Validated target ID column used for endpoint equality and
            ordering.
        :param protected_ids: IDs excluded through a bound NOT IN predicate; a None
            element suppresses all candidates under SQL NULL semantics.
        :return: A tuple of selected IDs in database ascending order; no rows are
            deleted.
        """
        reference_columns: list[tuple[str, str]] = []
        for link_spec in link_specs:
            self._validate_link_spec(link_spec)
            if link_spec.primary_table == table:
                reference_columns.append(
                    (link_spec.link_table, link_spec.primary_link_col)
                )
            if link_spec.secondary_table == table:
                reference_columns.append(
                    (link_spec.link_table, link_spec.secondary_link_col)
                )
        if not reference_columns:
            raise InputIntegrityError(
                f"No supplied link spec references table {table!r}; refusing to prune it."
            )

        target_id = f"target.{_quoted(id_column)}"
        conditions = [
            (
                f"NOT EXISTS (SELECT 1 FROM {self._macro_table_sql(link_table)} AS link_row "
                f"WHERE link_row.{_quoted(link_column)} = {target_id})"
            )
            for link_table, link_column in reference_columns
        ]
        values: list[Any] = []
        if protected_ids:
            conditions.append(
                f"{target_id} NOT IN ({', '.join('?' for _ in protected_ids)})"
            )
            values.extend(protected_ids)
        sql = (
            f"SELECT {target_id} FROM {self._macro_table_sql(table)} AS target "
            f"WHERE {' AND '.join(conditions)} ORDER BY {target_id}"
        )
        return tuple(_row_value(row, 0, id_column) for row in conn.execute(sql, tuple(values)))

    def _delete_unreferenced_rows(
        self,
        conn: Any,
        table: str,
        link_specs: tuple[StorageLinkSpec, ...],
        *,
        id_column: str,
        protected_ids: tuple[Any, ...],
    ) -> tuple[Any, ...]:
        """
        Select orphan candidates, then delete them in batches of up to 500 IDs.

        Use _unreferenced_ids for supplied-link and protection checks. Delete each
        selected chunk by ID without rechecking references or measuring affected-row
        counts. The result is the initial candidate tuple; concurrent changes, triggers
        and constraints remain backend concerns. The caller supplies the transaction.

        Example:
            ``macros._delete_unreferenced_rows(conn, "tags", specs,
            id_column="tag_id", protected_ids=())`` prunes the selected IDs on conn.


        :param conn: Caller-supplied connection; this helper does not commit or close
            it.
        :param table: Physical table name resolved through the driver wrapper.
        :param link_specs: Supplied link declarations to check for references; both
            endpoints of a self-link are considered.
        :param id_column: Validated target ID column used for selection and deletion.
        :param protected_ids: IDs excluded through a bound NOT IN predicate; a None
            element suppresses all candidates under SQL NULL semantics.
        :return: The initially selected candidate IDs, in ascending order.
        """
        ids = self._unreferenced_ids(
            conn,
            table,
            link_specs,
            id_column=id_column,
            protected_ids=protected_ids,
        )
        for chunk in _chunks(ids):
            conn.execute(
                f"DELETE FROM {self._macro_table_sql(table)} "
                f"WHERE {_quoted(id_column)} IN ({', '.join('?' for _ in chunk)})",
                chunk,
            )
        return ids

    def delete_unreferenced_rows(
        self,
        table: str,
        link_specs: Iterable[StorageLinkSpec],
        *,
        id_column: str | None = None,
        protected_ids: Iterable[Any] = (),
    ) -> tuple[Any, ...]:
        """
        Validate a target table and prune IDs unreferenced by supplied links in a macro
        transaction.

        Materialize link specs and protected IDs, resolve the ID column and validate its
        membership. Require at least one supplied endpoint to reference the table.
        Protection is applied with SQL NOT IN; include every relevant link declaration
        to avoid considering referenced rows as candidates. Delete selected IDs in
        chunks; failures roll back when they escape the outer macro boundary.

        Example:
            ``macros.delete_unreferenced_rows("tags", (work_tag_spec,),
            protected_ids=(1,))`` retains tag 1 and every tag referenced by that spec.


        :param table: Physical table name resolved through the driver wrapper.
        :param link_specs: Supplied link declarations to check for references; both
            endpoints of a self-link are considered.
        :param id_column: Physical row-ID column override, or None to use the wrapper
            default.
        :param protected_ids: IDs excluded through a bound NOT IN predicate; a None
            element suppresses all candidates under SQL NULL semantics.
        :return: The selected/deleted candidate IDs in ascending order, possibly empty.
        """
        table = _identifier(table, kind="table name")
        id_column = (
            _identifier(id_column, kind="id column")
            if id_column is not None
            else self.db.driver_wrapper.get_id_column(table)
        )
        self._validate_columns(table, (id_column,))
        link_specs = tuple(link_specs)
        protected_ids = tuple(protected_ids)
        with self._macro_transaction() as conn:
            return self._delete_unreferenced_rows(
                conn,
                table,
                link_specs,
                id_column=id_column,
                protected_ids=protected_ids,
            )

    def delete_unreferenced_rows_bulk(
        self,
        specs: Iterable[UnreferencedRowsSpec],
    ) -> dict[str, tuple[Any, ...]]:
        """
        Prune distinct target tables sequentially within one macro transaction.

        Materialize and require UnreferencedRowsSpec records, reject duplicate canonical
        table names, and validate each ID column before writing. Link validation and
        candidate selection occur as each table is processed. Later tables observe
        earlier deletions; a later failure rolls back the outer boundary when it
        escapes. An empty iterable still enters the transaction.

        Example:
            ``macros.delete_unreferenced_rows_bulk((UnreferencedRowsSpec(
            table="tags", link_specs=(work_tag_spec,), protected_ids=(1,)),))``
            returns a tags entry containing its candidate IDs.


        :param specs: Iterable of UnreferencedRowsSpec records, one per target table.
        :return: A dictionary in supplied table order, mapping each target name to its
            candidate ID tuple.
        """
        specs = tuple(specs)
        prepared: list[tuple[str, str, tuple[StorageLinkSpec, ...], tuple[Any, ...]]] = []
        seen_tables: set[str] = set()
        for spec in specs:
            if not isinstance(spec, UnreferencedRowsSpec):
                raise InputIntegrityError(
                    "delete_unreferenced_rows_bulk expects UnreferencedRowsSpec values."
                )
            table = _identifier(spec.table, kind="table name")
            if table in seen_tables:
                raise InputIntegrityError(f"Duplicate orphan-pruning table: {table!r}")
            seen_tables.add(table)
            id_column = (
                _identifier(spec.id_column, kind="id column")
                if spec.id_column is not None
                else self.db.driver_wrapper.get_id_column(table)
            )
            self._validate_columns(table, (id_column,))
            prepared.append(
                (table, id_column, tuple(spec.link_specs), tuple(spec.protected_ids))
            )
        with self._macro_transaction() as conn:
            return {
                table: self._delete_unreferenced_rows(
                    conn,
                    table,
                    link_specs,
                    id_column=id_column,
                    protected_ids=protected_ids,
                )
                for table, id_column, link_specs, protected_ids in prepared
            }

    # ------------------------------------------------------------------------------------------------------------------
    # Stable table fingerprints

    def fingerprint_table(
        self,
        target_table: str,
        columns: Iterable[str] | None = None,
        *,
        order_by: Iterable[str] | None = None,
        where: Mapping[str, Any] | None = None,
        algorithm: str = "sha256",
    ) -> str:
        """
        Hash selected, ordered row content with a table/column header and canonical
        value encoding.

        Validate a nonempty selection. Default ordering uses the wrapper ID column when
        available, otherwise all selected columns; an explicit order must be nonempty.
        Filters use sorted column names with bound equality or IS NULL. Caller-chosen
        ordering must resolve ties for rows with different selected values to guarantee
        repeatability. No independent snapshot transaction is opened.

        Hash length-prefixed UTF-8 JSON for the table/column header and each row using
        _canonical_db_value. The digest includes selected column order, table name, row
        order and value types, but not SQL predicates or ordering names themselves.
        Nonstandard objects inherit the canonical helper's string-fallback limits.
        Invalid algorithm names raise InputIntegrityError; variable-length SHAKE
        algorithms require an output length and fail at the no-argument hexdigest call.

        Example:
            >>> import sqlite3
            >>> from types import SimpleNamespace
            >>> conn = sqlite3.connect(":memory:")
            >>> conn.executescript("CREATE TABLE items (id INTEGER, name TEXT); INSERT INTO items VALUES (1, 'A');") is not None
            True
            >>> macros = SQLPortableMacrosMixin()
            >>> wrapper = SimpleNamespace(get_column_headings=lambda table: ("id", "name"), get_id_column=lambda table: "id")
            >>> macros.db = SimpleNamespace(driver=SimpleNamespace(conn=conn), driver_wrapper=wrapper)
            >>> digest = macros.fingerprint_table("items")
            >>> len(digest), digest == macros.fingerprint_table("items")
            (64, True)
            >>> conn.close()


        :param target_table: Existing physical table name, validated as a simple
            identifier.
        :param columns: Column iterable in digest order, or None for all headings in
            wrapper order.
        :param order_by: Nonempty ordering-column iterable, or None for ID/selected-
            column fallback.
        :param where: Optional equality-filter mapping; None values generate IS NULL
            predicates.
        :param algorithm: Hashlib algorithm name, stripped and lowercased; use a fixed-
            output digest such as sha256.
        :return: The hexadecimal digest string for the selected ordered rows and header.
        """
        target_table = _identifier(target_table, kind="table name")
        available = self._column_names(target_table)
        selected = available if columns is None else tuple(columns)
        if not selected:
            raise InputIntegrityError("fingerprint_table requires at least one selected column.")
        selected = self._validate_columns(target_table, selected)

        if order_by is None:
            try:
                id_column = self.db.driver_wrapper.get_id_column(target_table)
            except Exception:
                ordering = selected
            else:
                ordering = (id_column,) if id_column in available else selected
        else:
            ordering = self._validate_columns(target_table, tuple(order_by))
            if not ordering:
                raise InputIntegrityError("order_by cannot be an empty iterable.")

        filters = dict(where or {})
        self._validate_columns(target_table, filters)
        conditions: list[str] = []
        bindings: list[Any] = []
        for column in sorted(filters):
            value = filters[column]
            if value is None:
                conditions.append(f"{_quoted(column)} IS NULL")
            else:
                conditions.append(f"{_quoted(column)} = ?")
                bindings.append(value)

        sql = (
            f"SELECT {', '.join(_quoted(column) for column in selected)} "
            f"FROM {self._macro_table_sql(target_table)}"
        )
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY " + ", ".join(_quoted(column) for column in ordering)

        try:
            digest = hashlib.new(str(algorithm).strip().lower())
        except (TypeError, ValueError) as exc:
            raise InputIntegrityError(f"Unsupported fingerprint algorithm: {algorithm!r}") from exc
        header = json.dumps(
            {"table": target_table, "columns": selected},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        digest.update(len(header).to_bytes(8, "big"))
        digest.update(header)
        for row in self._macro_connection().execute(sql, tuple(bindings)):
            values = [
                _canonical_db_value(_row_value(row, index, column))
                for index, column in enumerate(selected)
            ]
            encoded = json.dumps(
                values,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
        return digest.hexdigest()


__all__ = ["SQLPortableMacrosMixin"]
