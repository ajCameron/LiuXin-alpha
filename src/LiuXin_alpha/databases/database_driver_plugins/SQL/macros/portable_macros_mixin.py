"""Portable compound SQL operations shared by SQLite and PostgreSQL."""

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
    text = str(value).strip()
    if not _SAFE_IDENTIFIER.fullmatch(text):
        raise InputIntegrityError(f"Unsafe SQL {kind}: {value!r}")
    return text


def _quoted(value: str) -> str:
    return '"' + _identifier(value).replace('"', '""') + '"'


def _chunks(values: tuple[Any, ...], size: int = 500) -> Iterator[tuple[Any, ...]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def _row_value(row: Any, index: int, column: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(column)
    return row[index]


def _canonical_db_value(value: Any) -> Any:
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
    """Implementation of the portable macro contract using SQL-92 primitives."""

    db: Any

    # ------------------------------------------------------------------------------------------------------------------
    # Infrastructure

    def _macro_driver(self) -> Any:
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
        table = _identifier(table, kind="table name")
        driver = self._macro_driver()
        qualify = getattr(driver, "_table_sql", None)
        if callable(qualify):
            return str(qualify(table))
        return _quoted(table)

    def _macro_temporary_table_sql(self, table: str) -> str:
        driver = self._macro_driver()
        schema = "pg_temp" if hasattr(driver, "schema") else "temp"
        return f"{schema}.{_quoted(table)}"

    def _macro_temporary_declared_type(self, declared_type: str) -> str:
        """Translate the small portable temporary-table type vocabulary."""

        if declared_type == "BLOB" and hasattr(self._macro_driver(), "schema"):
            return "BYTEA"
        return declared_type

    def _macro_invalidate(self) -> None:
        driver = self._macro_driver()
        invalidate = getattr(driver, "_zero_prop_cache", None)
        if not callable(invalidate):
            invalidate = getattr(driver, "zero_prop_cache", None)
        if callable(invalidate):
            invalidate()

    @contextmanager
    def _macro_transaction(self) -> Iterator[Any]:
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
        """Compose nested portable macro calls into one atomic transaction."""

        return self._macro_transaction()

    def _row_id_column(self, table: str, id_column: str | None) -> str:
        if id_column is None:
            id_column = self.db.driver_wrapper.get_id_column(table)
        return self._validate_columns(table, (id_column,))[0]

    @staticmethod
    def _mapping_rows(
        columns: tuple[str, ...],
        rows: Iterable[Any],
    ) -> tuple[Mapping[str, Any], ...]:
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
        """Return one row by ID through the transaction connection."""

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
        """Return rows matching portable equality predicates."""

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
        """Insert one row and return its assigned ID atomically."""

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
        """Update selected columns on one row atomically."""

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
        """Delete one row by ID atomically."""

        id_column = self._row_id_column(table, id_column)
        with self._macro_transaction() as conn:
            conn.execute(
                f"DELETE FROM {self._macro_table_sql(table)} "
                f"WHERE {_quoted(id_column)} = ?",
                (row_id,),
            )

    def _column_names(self, table: str) -> tuple[str, ...]:
        headings = self.db.driver_wrapper.get_column_headings(table)
        return tuple(str(column) for column in headings)

    def _validate_columns(self, table: str, columns: Iterable[str]) -> tuple[str, ...]:
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
        Read the optional allowed-type registry through the database wrapper.

        :param link_spec: Link whose optional registry should be read.
        :return: Live allowed types, or ``None`` when no registry is declared.
        :raises InputIntegrityError: If the wrapper cannot read a declared
            registry.
        :raises DatabaseIntegrityError: If the registry is malformed.
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
        Validate one type against storage capability and allowed values.

        :param link_spec: Declared link storage capabilities and policy.
        :param link_type: Explicit type value to validate.
        :param live_allowed_types: Values read from the optional registry.
        :return: None.
        :raises InputIntegrityError: If the type is structurally invalid or
            outside an allowed set.
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
        """Move existing priorities aside without relying on nullable columns."""

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
        """Replace values stored in destination rows owned by source rows."""

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
        return normalize_identity_value(value, profile)

    @staticmethod
    def _validate_ensure_value(value: Any, metadata: ColumnMetadata) -> None:
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
        """Return a matching logical value id without inserting a row."""

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
        """Derive the database-declared identity key for a display value."""

        table = _identifier(table, kind="table name")
        value_column = _identifier(value_column, kind="column name")
        spec = self._normalized_identity_spec(table, value_column)
        return normalize_identity_value(value, spec.normalization_profile)

    @staticmethod
    def _identity_scope_values(
        spec: NormalizedIdentitySpec,
        scope_values: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
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
        """Resolve a derived identity key to the canonical stored row/value."""

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
        """Resolve a display value to the canonical stored row/value."""

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
        """Resolve a derived key and return only the canonical display value."""

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
        """Resolve a display value and return only its canonical spelling."""

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
        """Report stale derived keys and collisions without changing data."""

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
        """Install declarations, backfill keys, and enforce uniqueness.

        The operation is atomic.  If normalization reveals two rows with the
        same scoped identity, no schema or data changes are retained; call
        :meth:`audit_normalized_identities` first to inspect those rows.
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
