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
import unicodedata
import uuid
from typing import Any, Iterator

from LiuXin_alpha.databases.column_metadata import (
    ColumnEmptyValuePolicy,
    ColumnMetadata,
    ColumnNormalizationProfile,
    infer_column_metadata,
)
from LiuXin_alpha.databases.macro_types import (
    LINK_TYPE_UNSET,
    LinkRow,
    LinkValue,
    UnreferencedRowsSpec,
)
from LiuXin_alpha.databases.schema_specs import StorageLinkSpec
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError


_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TEMP_TYPES = frozenset({"BLOB", "INTEGER", "NUMERIC", "REAL", "TEXT"})


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
        conn = self._macro_connection()
        lock = getattr(self.db, "lock", None)
        lock_context = lock if hasattr(lock, "__enter__") else nullcontext()
        with lock_context:
            with conn:
                yield conn
        self._macro_invalidate()

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

    def _prepare_link_value(
        self,
        link_spec: StorageLinkSpec,
        link: LinkValue,
        *,
        scoped_type: Any = LINK_TYPE_UNSET,
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
            if link_spec.allowed_types and link_type not in link_spec.allowed_types:
                raise InputIntegrityError(f"Link type is not allowed by the link spec: {link_type!r}")
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
        link = self._prepare_link_value(link_spec, link)
        with self._macro_transaction() as conn:
            return self._upsert_link(conn, link_spec, primary_id, link)

    def upsert_links(
        self,
        link_spec: StorageLinkSpec,
        primary_id: Any,
        links: Iterable[LinkValue],
    ) -> tuple[LinkRow, ...]:
        link_spec = self._validate_link_spec(link_spec)
        prepared = tuple(self._prepare_link_value(link_spec, link) for link in links)
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
    ) -> tuple[LinkRow, ...]:
        if link_type is not LINK_TYPE_UNSET and not link_spec.type_part_of_identity:
            raise InputIntegrityError(
                "Type-scoped replacement requires the type column to be part "
                "of the link identity."
            )
        prepared = tuple(
            self._prepare_link_value(link_spec, link, scoped_type=link_type)
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
        with self._macro_transaction() as conn:
            return self._replace_links(
                conn,
                link_spec,
                primary_id,
                materialized,
                link_type=link_type,
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
        with self._macro_transaction() as conn:
            return {
                primary_id: self._replace_links(
                    conn,
                    link_spec,
                    primary_id,
                    links,
                    link_type=link_type,
                )
                for primary_id, links in materialized.items()
            }

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
        if not isinstance(value, str):
            return value
        if profile is ColumnNormalizationProfile.NONE:
            return value
        if profile is ColumnNormalizationProfile.UNICODE_NFC:
            return unicodedata.normalize("NFC", value)
        if profile is ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD:
            return unicodedata.normalize("NFC", value).strip().casefold()
        if profile is ColumnNormalizationProfile.TAG_SEARCH_TERM:
            from LiuXin_alpha.metadata.standardization import make_tag_search_term

            return make_tag_search_term(unicodedata.normalize("NFC", value))
        if profile is ColumnNormalizationProfile.TITLE_SEARCH_TERM:
            from LiuXin_alpha.metadata.standardization import make_title_search_term

            return make_title_search_term(unicodedata.normalize("NFC", value))
        raise InputIntegrityError(f"Unsupported normalization profile: {profile!r}")

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
    ) -> Any | None:
        search_column = metadata.comparison_column or value_column
        if (
            metadata.comparison_column is None
            and metadata.normalization_profile is not ColumnNormalizationProfile.NONE
        ):
            sql = (
                f"SELECT {_quoted(id_column)}, {_quoted(value_column)} "
                f"FROM {self._macro_table_sql(table)}"
            )
            values: tuple[Any, ...] = ()
            if (
                metadata.normalization_profile
                is ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD
                and isinstance(value, str)
            ):
                if hasattr(self._macro_driver(), "schema"):
                    sql += (
                        f" WHERE LOWER(TRIM({_quoted(value_column)})) "
                        "= LOWER(TRIM(?))"
                    )
                else:
                    sql += (
                        f" WHERE TRIM({_quoted(value_column)}) "
                        "= TRIM(?) COLLATE PYNOCASE"
                    )
                values = (value,)
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
        comparison_value = self._normalise_value(value, metadata.normalization_profile)
        existing_id = self._find_ensured_id(
            conn,
            table=table,
            id_column=id_column,
            value_column=value_column,
            value=value,
            metadata=metadata,
            comparison_value=comparison_value,
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
        )
        if ensured_id is None:
            raise DatabaseIntegrityError(
                f"Could not ensure value {value!r} in {table}.{value_column}; "
                "another uniqueness or required-column rule rejected it."
            )
        return ensured_id

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
