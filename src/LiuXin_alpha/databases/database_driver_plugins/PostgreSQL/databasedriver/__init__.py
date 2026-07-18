"""PostgreSQL database driver for LiuXin."""

from __future__ import annotations

import uuid
from copy import deepcopy
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from LiuXin_alpha.databases.column_metadata import (
    COLUMN_METADATA_TABLE,
    ColumnMetadata,
    infer_column_metadata,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.config import (
    DEFAULT_POSTGRES_SCHEMA,
    configured_postgres_schema,
    configured_postgres_target,
    redact_postgres_target,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
    PostgresConnectionAdapter,
    connect_postgres,
    redact_postgres_error,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.schema import create_postgres_schema
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver import SQLBaseDriver
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.table_names_mixin import TableNamesMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.value_casting_mixin import ValueCastingMixin
from LiuXin_alpha.databases.database_driver_plugins.macros_base import MacrosBase
from LiuXin_alpha.databases.maintenance.dummy_maintenance_bot import DummyMaintenanceBot
from LiuXin_alpha.errors import DatabaseDriverError, DatabaseIntegrityError, InputIntegrityError
from LiuXin_alpha.utils.language_tools.pluralizers import plural_singular_mapper
from LiuXin_alpha.utils.logging import default_log


DEFAULT_SCHEMA = DEFAULT_POSTGRES_SCHEMA


class PostgresDatabaseMacros(MacrosBase):
    """PostgreSQL macro surface for operations that are implemented by the driver today."""

    @property
    def get(self):
        return self.db.get

    @property
    def execute(self):
        return self.db.driver_wrapper.execute

    @property
    def executemany(self):
        return self.db.driver_wrapper.executemany

    def direct_update_column_in_table(self, table, column, table_id_col, item_id, new_value):
        stmt = f"update {self._table_sql(table)} set {_q(column)} = %s where {_q(table_id_col)} = %s"
        self.execute(stmt, (new_value, item_id))

    def _table_sql(self, table: str) -> str:
        driver = getattr(getattr(self.db, "driver_wrapper", None), "driver", None)
        schema = getattr(driver, "schema", DEFAULT_SCHEMA)
        canonicalise = getattr(driver, "_canonicalise_table_name_for_cache", None)
        table_name = canonicalise(table) if callable(canonicalise) else str(table)
        return _qualified_table(str(schema), str(table_name))

    def __getattr__(self, name: str) -> Any:
        raise DatabaseDriverError(f"PostgreSQL macro {name!r} is not implemented yet.")


class DatabaseDriver(
    SQLBaseDriver,
    ValueCastingMixin,
    TableNamesMixin,
):
    """Initial PostgreSQL backend implementation."""

    def __init__(self, db_metadata: Mapping[str, object], db=None, set_conn: bool = True, dirty_records_queue=None):
        self.db_metadata = dict(db_metadata or {})
        self.connection_target = configured_postgres_target(self.db_metadata)
        if not self.connection_target.configured:
            raise DatabaseDriverError(
                "PostgreSQL driver requires a postgres_url, database_url, dsn, service, or PostgreSQL env target."
            )
        self.database_url = self.connection_target.value
        self.database_path = self.connection_target.label
        self.redacted_database_url = redact_postgres_target(self.connection_target)
        self.schema = configured_postgres_schema(self.db_metadata)
        self.db = db

        self._macros = PostgresDatabaseMacros(db=self.db)

        self.tables = None
        self.tables_and_columns = None
        self.categorized_tables = None
        self.all_column_names = set()
        self.locations = None
        self.event_count = 0
        self._open_connections = []
        self.helper_tables = [
            "conversion_options",
            "compressed_files",
            "column_metadata",
            "new_books",
            "database_metadata",
            "hashes",
        ]
        self.maintainer_callback = DummyMaintenanceBot()
        self.dirty_records_queue = dirty_records_queue
        self.conn = self.get_connection() if set_conn else None

    def get_connection(self) -> PostgresConnectionAdapter:
        raw = connect_postgres(self.db_metadata)
        conn = PostgresConnectionAdapter(raw)
        conn.execute(f"set search_path to {_q(self.schema)}")
        conn.commit()
        return self._register_open_connection(conn)

    def exists(self) -> bool:
        conn = None
        try:
            conn = self.get_connection()
            conn.execute("select 1")
            return True
        except Exception as exc:
            default_log.log_variables(
                "PostgreSQL existence check failed.",
                "WARNING",
                ("database_url", self.redacted_database_url),
                ("error", redact_postgres_error(exc, self.connection_target.label)),
            )
            return False
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def direct_backup(self, path=None):
        raise DatabaseDriverError("PostgreSQL backup is not a file copy. Use pg_dump/base backups outside this driver.")

    def direct_self_delete(self):
        raise DatabaseDriverError("PostgreSQL databases are not deleted by the LiuXin driver.")

    def make_scratch(self) -> str:
        raise DatabaseDriverError("PostgreSQL scratch database switching is not implemented.")

    def direct_create_new_database(self) -> None:
        conn = self._primary_connection()
        create_postgres_schema(conn, schema=self.schema)
        self._zero_prop_cache()

    def _table_sql(self, table: str) -> str:
        return _qualified_table(self.schema, self._canonicalise_table_name_for_cache(table))

    def direct_execute_sql_script(self, script: str | list[str]) -> None:
        return self.direct_executescript("\n".join(script) if isinstance(script, list) else script)

    def direct_execute_sql(self, sql: str, parameters: Sequence[Any] | None = None) -> Any:
        cur = self.direct_execute(sql, parameters)
        return getattr(cur, "lastrowid", None)

    def direct_execute(self, sql: str, values: Sequence[Any] | None = None) -> Any:
        conn = self._primary_connection()
        try:
            with conn:
                cur = conn.execute(sql, values)
            self._zero_prop_cache()
            return cur
        except Exception as exc:
            err_str = default_log.log_exception(
                "Attempting to execute PostgreSQL SQL failed.",
                exc,
                "ERROR",
                ("sql", sql),
                ("values", values),
                ("database_url", self.redacted_database_url),
            )
            raise DatabaseDriverError(err_str) from exc

    def direct_executemany(self, sql: str, values: Sequence[Sequence[Any]] | None = None) -> None:
        conn = self._primary_connection()
        try:
            with conn:
                conn.executemany(sql, values or ())
            self._zero_prop_cache()
        except Exception as exc:
            err_str = default_log.log_exception(
                "Attempting to execute PostgreSQL executemany failed.",
                exc,
                "ERROR",
                ("sql", sql),
                ("values", values),
                ("database_url", self.redacted_database_url),
            )
            raise DatabaseDriverError(err_str) from exc

    def direct_executescript(self, sqlscript: str) -> None:
        conn = self._primary_connection()
        try:
            with conn:
                conn.executescript(sqlscript)
            self._zero_prop_cache()
        except Exception as exc:
            err_str = default_log.log_exception(
                "Attempting to execute PostgreSQL script failed.",
                exc,
                "ERROR",
                ("database_url", self.redacted_database_url),
            )
            raise DatabaseDriverError(err_str) from exc

    @property
    def user_version(self) -> str:
        return str(self.direct_get_schema_version())

    def direct_get_user_version(self) -> str:
        return self.user_version

    def direct_create_main_table(
        self,
        table_name: str,
        column_headings: Mapping[str, Mapping[str, Any]] | Sequence[str] | None = None,
        index_on: str | Sequence[str] | None = "all",
        default_datatype: str = "TEXT",
        default_unique: bool = False,
    ) -> None:
        table = _assert_safe_identifier(self._canonicalise_table_name_for_cache(table_name), kind="table")
        table_col = _assert_safe_identifier(plural_singular_mapper(table), kind="column base")

        column_defs = [f"{_q(table_col + '_id')} bigserial primary key"]
        index_columns: list[str] = []
        unique_sql = " unique" if default_unique else ""

        if column_headings is None:
            column_defs.append(f"{_q(table_col)} {_postgres_column_type(default_datatype)} null{unique_sql}")
            if index_on == "all":
                index_columns.append(table_col)
            elif index_on not in (None, (), []):
                raise NotImplementedError("PostgreSQL direct_create_main_table only supports index_on='all' or None")
        else:
            if isinstance(column_headings, Mapping):
                column_items = list(column_headings.items())
            else:
                column_items = [(str(column), {}) for column in column_headings]
            requested_index_cols = None
            if index_on == "all":
                requested_index_cols = "all"
            elif index_on is None:
                requested_index_cols = set()
            elif isinstance(index_on, str):
                requested_index_cols = {index_on}
            else:
                requested_index_cols = {str(column) for column in index_on}

            for column, spec in column_items:
                suffix = _assert_safe_identifier(str(column), kind="column suffix")
                full_column = _assert_safe_identifier(f"{table_col}_{suffix}", kind="column")
                datatype = spec.get("datatype", default_datatype) if isinstance(spec, Mapping) else default_datatype
                unique = bool(spec.get("unique", default_unique)) if isinstance(spec, Mapping) else default_unique
                column_defs.append(
                    f"{_q(full_column)} {_postgres_column_type(str(datatype))} null"
                    + (" unique" if unique else "")
                )
                if requested_index_cols == "all" or suffix in requested_index_cols or full_column in requested_index_cols:
                    index_columns.append(full_column)

        column_defs.append(f"{_q(table_col + '_datestamp')} timestamp with time zone default current_timestamp")
        column_defs.append(f"{_q(table_col + '_scratch')} text null")

        statements = [
            f"create table if not exists {self._table_sql(table)} (\n  " + ",\n  ".join(column_defs) + "\n)"
        ]
        for column in index_columns:
            index_name = _assert_safe_identifier(f"{table}_{column}_index", kind="index")
            statements.append(
                f"create index if not exists {_q(index_name)} on {self._table_sql(table)} ({_q(column)})"
            )

        self._execute_schema_statements(statements)

    def direct_get_direct_link_main_tables_sql(
        self,
        primary_table: str,
        secondary_table: str,
        link_type: str = "many_many",
        requested_cols: str | Sequence[str] | None = "all",
        index_both: bool = True,
        allowed_types: Sequence[str] | None = None,
        one_link_with_one_type: bool = True,
        override_restriction_sql: str | None = None,
        nullable_fks: bool = True,
    ) -> tuple[list[str], str]:
        _ = one_link_with_one_type
        if override_restriction_sql is not None:
            raise NotImplementedError("PostgreSQL link DDL does not accept raw SQLite restriction SQL.")
        if link_type not in {
            "many_many",
            "many_many_non_exclusive",
            "one_many",
            "many_one",
            "one_one",
            "one_one_normalized",
            "rating",
        }:
            raise NotImplementedError(f"PostgreSQL link_type not recognized: {link_type!r}")

        primary_table = self._canonicalise_table_name_for_cache(primary_table)
        secondary_table = self._canonicalise_table_name_for_cache(secondary_table)
        self._assert_existing_table(primary_table)
        self._assert_existing_table(secondary_table)

        primary_base = self.direct_get_column_name(primary_table)
        secondary_base = self.direct_get_column_name(secondary_table)
        original_bases = [primary_base, secondary_base]
        sorted_bases = sorted(original_bases)
        if original_bases == sorted_bases:
            left_table, right_table = primary_table, secondary_table
            left_base, right_base = primary_base, secondary_base
        else:
            left_table, right_table = secondary_table, primary_table
            left_base, right_base = secondary_base, primary_base
            if link_type == "many_one":
                link_type = "one_many"
            elif link_type == "one_many":
                link_type = "many_one"

        link_base = _assert_safe_identifier(f"{left_base}_{right_base}_link", kind="link column base")
        link_table = _assert_safe_identifier(f"{link_base}s", kind="link table")
        left_id_col = self.direct_get_id_column(left_table)
        right_id_col = self.direct_get_id_column(right_table)
        left_fk_col = _assert_safe_identifier(f"{link_base}_{left_base}_id", kind="link column")
        right_fk_col = _assert_safe_identifier(f"{link_base}_{right_base}_id", kind="link column")
        fk_null_sql = "null" if nullable_fks else "not null"

        requested = _normalise_requested_link_columns(requested_cols)
        has_type = requested == "all" or "type" in requested
        has_priority = requested == "all" or "priority" in requested

        column_defs = [
            f"{_q(link_base + '_id')} bigserial primary key",
            (
                f"{_q(left_fk_col)} bigint {fk_null_sql} references {self._table_sql(left_table)}"
                f" ({_q(left_id_col)}) on delete cascade on update cascade"
            ),
            (
                f"{_q(right_fk_col)} bigint {fk_null_sql} references {self._table_sql(right_table)}"
                f" ({_q(right_id_col)}) on delete cascade on update cascade"
            ),
        ]
        for suffix, definition in _link_extra_columns(link_base, requested):
            _ = suffix
            column_defs.append(definition)

        for constraint in _link_constraints(
            link_type=link_type,
            link_base=link_base,
            left_base=left_base,
            right_base=right_base,
            left_fk_col=left_fk_col,
            right_fk_col=right_fk_col,
            has_type=has_type,
            has_priority=has_priority,
        ):
            column_defs.append(constraint)

        statements = [
            f"create table if not exists {self._table_sql(link_table)} (\n  " + ",\n  ".join(column_defs) + "\n)"
        ]
        if index_both:
            statements.append(
                f"create index if not exists {_q(link_base + '_' + left_base + '_id_index')} "
                f"on {self._table_sql(link_table)} ({_q(left_fk_col)})"
            )
            statements.append(
                f"create index if not exists {_q(link_base + '_' + right_base + '_id_index')} "
                f"on {self._table_sql(link_table)} ({_q(right_fk_col)})"
            )
        if requested == "all" or "sequence_number" in requested:
            sequence_col = f"{link_base}_sequence_number"
            statements.append(
                f"create unique index if not exists {_q(link_base + '_' + left_base + '_sequence_idx')} "
                f"on {self._table_sql(link_table)} ({_q(left_fk_col)}, {_q(sequence_col)}) "
                f"where {_q(sequence_col)} is not null"
            )
        if has_type and allowed_types is not None:
            allowed_table = _assert_safe_identifier(f"{link_table}__types", kind="allowed types table")
            statements.append(f"create table if not exists {self._table_sql(allowed_table)} ({_q('type')} text primary key)")
            for allowed_type in allowed_types:
                statements.append(
                    f"insert into {self._table_sql(allowed_table)} ({_q('type')}) values ({_pg_literal(allowed_type)}) "
                    f"on conflict ({_q('type')}) do nothing"
                )

        return statements, link_table

    def direct_link_main_tables(
        self,
        primary_table: str,
        secondary_table: str,
        link_type: str = "many_many",
        requested_cols: str | Sequence[str] | None = "all",
        index_both: bool = True,
        allowed_types: Sequence[str] | None = None,
        override_restriction_sql: str | None = None,
        nullable_fks: bool = True,
    ) -> str:
        if allowed_types is not None:
            allowed_types = tuple(str(value) for value in allowed_types)
        requested = _normalise_requested_link_columns(requested_cols)
        has_type = requested == "all" or "type" in requested
        sql_statements, link_table = self.direct_get_direct_link_main_tables_sql(
            primary_table=primary_table,
            secondary_table=secondary_table,
            link_type=link_type,
            requested_cols=requested_cols,
            index_both=index_both,
            allowed_types=None,
            override_restriction_sql=override_restriction_sql,
            nullable_fks=nullable_fks,
        )
        if allowed_types is not None and has_type:
            allowed_table = _assert_safe_identifier(f"{link_table}__types", kind="allowed types table")
            sql_statements.append(
                f"create table if not exists {self._table_sql(allowed_table)} ({_q('type')} text primary key)"
            )

        conn = self._primary_connection()
        try:
            with conn:
                for statement in sql_statements:
                    conn.execute(statement)
                if allowed_types is not None and has_type:
                    allowed_table = _assert_safe_identifier(f"{link_table}__types", kind="allowed types table")
                    conn.executemany(
                        f"insert into {self._table_sql(allowed_table)} ({_q('type')}) values (%s) "
                        f"on conflict ({_q('type')}) do nothing",
                        [(value,) for value in allowed_types],
                    )
            self._zero_prop_cache()
            return link_table
        except Exception as exc:
            err_str = default_log.log_exception(
                "PostgreSQL link-table creation failed.",
                exc,
                "ERROR",
                ("primary_table", primary_table),
                ("secondary_table", secondary_table),
                ("link_type", link_type),
                ("database_url", self.redacted_database_url),
            )
            raise DatabaseDriverError(err_str) from exc

    def direct_unlink_main_tables(self, primary_table: str, secondary_table: str) -> None:
        link_table, _ = _link_table_name_col_name(primary_table, secondary_table)
        conn = self._primary_connection()
        try:
            with conn:
                conn.execute(f"drop table if exists {self._table_sql(link_table)} cascade")
            self._zero_prop_cache()
        except Exception as exc:
            err_str = default_log.log_exception(
                "PostgreSQL link-table drop failed.",
                exc,
                "ERROR",
                ("primary_table", primary_table),
                ("secondary_table", secondary_table),
                ("link_table", link_table),
                ("database_url", self.redacted_database_url),
            )
            raise DatabaseDriverError(err_str) from exc

    def direct_get_schema_version(self) -> str | None:
        conn = self._short_connection()
        try:
            cur = conn.execute(
                """
                select md5(coalesce(string_agg(
                    table_schema || '.' || table_name || ':' || column_name || ':' || data_type,
                    ',' order by table_schema, table_name, ordinal_position
                ), '')) as schema_fingerprint
                from information_schema.columns
                where table_schema = %s
                """,
                (self.schema,),
            )
            row = cur.fetchone()
            return _row_value(row, 0, "schema_fingerprint")
        finally:
            conn.close()

    def _invalidate_schema_caches(self) -> None:
        self.tables = None
        self.tables_and_columns = None
        declared_types_cache = getattr(self, "_declared_types_cache", None)
        if isinstance(declared_types_cache, dict):
            declared_types_cache.clear()
        try:
            delattr(self, "_schema_version_cached")
        except Exception:
            pass

    def direct_get_tables(self, force_refresh: bool = False) -> list[str]:
        if force_refresh:
            self._invalidate_schema_caches()
        if self.tables is not None and not force_refresh:
            current = self.direct_get_schema_version()
            cached = getattr(self, "_schema_version_cached", None)
            if cached is None or current is None or cached == current:
                return self.tables
            self._invalidate_schema_caches()

        conn = self._short_connection()
        try:
            cur = conn.execute(
                """
                select table_name
                from information_schema.tables
                where table_schema = %s
                  and table_type in ('BASE TABLE', 'VIEW')
                order by table_name
                """,
                (self.schema,),
            )
            self.tables = [str(_row_value(row, 0, "table_name")) for row in cur.fetchall()]
            self._schema_version_cached = self.direct_get_schema_version()
            return self.tables
        finally:
            conn.close()

    def direct_get_tables_and_columns(self, force_refresh: bool = False) -> dict[str, list[str]]:
        if self.tables_and_columns is not None and not force_refresh:
            current = self.direct_get_schema_version()
            cached = getattr(self, "_schema_version_cached", None)
            if cached is None or current is None or cached == current:
                return self.tables_and_columns
        if force_refresh:
            self._invalidate_schema_caches()

        conn = self._short_connection()
        try:
            cur = conn.execute(
                """
                select table_name, column_name
                from information_schema.columns
                where table_schema = %s
                order by table_name, ordinal_position
                """,
                (self.schema,),
            )
            tables_and_columns: dict[str, list[str]] = {}
            for row in cur.fetchall():
                table = str(_row_value(row, 0, "table_name"))
                column = str(_row_value(row, 1, "column_name"))
                tables_and_columns.setdefault(table, []).append(column)
            self.tables_and_columns = tables_and_columns
            self.tables = sorted(tables_and_columns)
            self._schema_version_cached = self.direct_get_schema_version()
            return tables_and_columns
        finally:
            conn.close()

    def direct_get_column_headings(self, table: str, normalize: bool = False) -> list[str]:
        table = self._canonicalise_table_name_for_cache(table)
        tables_and_columns = self.direct_get_tables_and_columns()
        try:
            return tables_and_columns[table]
        except KeyError as exc:
            raise InputIntegrityError(f"table {table} not found") from exc

    def direct_get_declared_types_for_table(self, table: str) -> dict[str, str]:
        table = self._canonicalise_table_name_for_cache(table)
        cache = getattr(self, self._DECLARED_TYPES_CACHE_ATTR, None)
        if cache is None:
            cache = {}
            setattr(self, self._DECLARED_TYPES_CACHE_ATTR, cache)
        if table in cache:
            return cache[table]

        conn = self._short_connection()
        try:
            cur = conn.execute(
                """
                select column_name, data_type
                from information_schema.columns
                where table_schema = %s and table_name = %s
                order by ordinal_position
                """,
                (self.schema, table),
            )
            types = {
                str(_row_value(row, 0, "column_name")): str(_row_value(row, 1, "data_type"))
                for row in cur.fetchall()
            }
            cache[table] = types
            return types
        finally:
            conn.close()

    def _get_declared_types_for_table(self, table: str) -> dict[str, str]:
        return self.direct_get_declared_types_for_table(table)

    def direct_get_case_sensitivity(self, table: str, column: str) -> bool:
        return self.direct_get_column_metadata(table, column).case_sensitive

    def direct_get_column_metadata(self, table: str, column: str) -> ColumnMetadata:
        table_name, column_name = self._validated_column_metadata_target(table, column)
        try:
            declared_type = self.direct_get_declared_column_datatype(
                table_name,
                column_name,
            )
        except DatabaseIntegrityError:
            declared_type = None
        fallback = infer_column_metadata(
            table_name,
            column_name,
            declared_type,
        )
        if COLUMN_METADATA_TABLE not in set(self.direct_get_tables()):
            return fallback

        conn = self._short_connection()
        try:
            cur = conn.execute(
                f"""
                select
                  "column_metadata_case_sensitive",
                  "column_metadata_semantic_role",
                  "column_metadata_normalization_profile",
                  "column_metadata_comparison_column",
                  "column_metadata_empty_value_policy",
                  "column_metadata_merge_policy",
                  "column_metadata_validation_profile"
                from {self._table_sql(COLUMN_METADATA_TABLE)}
                where "column_metadata_table_name" = %s
                  and "column_metadata_column_name" = %s
                limit 1
                """,
                (table_name, column_name),
            )
            row = cur.fetchone()
        finally:
            conn.close()
        if row is None:
            return fallback
        return self._column_metadata_from_values(
            table_name,
            column_name,
            *(
                _row_value(row, index, name)
                for index, name in enumerate(
                    (
                        "column_metadata_case_sensitive",
                        "column_metadata_semantic_role",
                        "column_metadata_normalization_profile",
                        "column_metadata_comparison_column",
                        "column_metadata_empty_value_policy",
                        "column_metadata_merge_policy",
                        "column_metadata_validation_profile",
                    )
                )
            ),
        )

    def direct_set_column_metadata(self, metadata: ColumnMetadata) -> None:
        metadata = self._validated_column_metadata_input(metadata)
        if COLUMN_METADATA_TABLE not in set(self.direct_get_tables()):
            raise DatabaseIntegrityError(
                "database has no column_metadata table; migrate the schema before storing column policy"
            )

        conn = self._primary_connection()
        with conn:
            conn.execute(
                f"""
                insert into {self._table_sql(COLUMN_METADATA_TABLE)} (
                  "column_metadata_table_name",
                  "column_metadata_column_name",
                  "column_metadata_case_sensitive",
                  "column_metadata_semantic_role",
                  "column_metadata_normalization_profile",
                  "column_metadata_comparison_column",
                  "column_metadata_empty_value_policy",
                  "column_metadata_merge_policy",
                  "column_metadata_validation_profile"
                ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                on conflict (
                  "column_metadata_table_name",
                  "column_metadata_column_name"
                ) do update set
                  "column_metadata_case_sensitive" = excluded."column_metadata_case_sensitive",
                  "column_metadata_semantic_role" = excluded."column_metadata_semantic_role",
                  "column_metadata_normalization_profile" = excluded."column_metadata_normalization_profile",
                  "column_metadata_comparison_column" = excluded."column_metadata_comparison_column",
                  "column_metadata_empty_value_policy" = excluded."column_metadata_empty_value_policy",
                  "column_metadata_merge_policy" = excluded."column_metadata_merge_policy",
                  "column_metadata_validation_profile" = excluded."column_metadata_validation_profile"
                """,
                self._column_metadata_db_values(metadata),
            )

    def direct_set_case_sensitivity(
        self,
        table: str,
        column: str,
        case_sensitive: bool,
    ) -> None:
        table_name, column_name = self._validated_column_metadata_target(table, column)
        if type(case_sensitive) is not bool:
            raise InputIntegrityError("case_sensitive must be a bool")
        if COLUMN_METADATA_TABLE not in set(self.direct_get_tables()):
            raise DatabaseIntegrityError(
                "database has no column_metadata table; migrate the schema before storing column policy"
            )

        conn = self._primary_connection()
        with conn:
            conn.execute(
                f"""
                insert into {self._table_sql(COLUMN_METADATA_TABLE)} (
                  "column_metadata_table_name",
                  "column_metadata_column_name",
                  "column_metadata_case_sensitive"
                ) values (%s, %s, %s)
                on conflict (
                  "column_metadata_table_name",
                  "column_metadata_column_name"
                ) do update set
                  "column_metadata_case_sensitive" = excluded."column_metadata_case_sensitive"
                """,
                (table_name, column_name, int(case_sensitive)),
            )

    def direct_is_column_case_sensitive(self, table: str, column: str) -> bool:
        """Compatibility alias for :meth:`direct_get_case_sensitivity`."""

        return self.direct_get_case_sensitivity(table, column)

    def direct_set_column_case_sensitive(
        self,
        table: str,
        column: str,
        case_sensitive: bool,
    ) -> None:
        """Compatibility alias for :meth:`direct_set_case_sensitivity`."""

        self.direct_set_case_sensitivity(table, column, case_sensitive)

    def direct_get_relation_type(self, name: str) -> str | None:
        conn = self._short_connection()
        try:
            cur = conn.execute(
                """
                select case table_type when 'VIEW' then 'view' else 'table' end as relation_type
                from information_schema.tables
                where table_schema = %s and table_name = %s
                limit 1
                """,
                (self.schema, str(name)),
            )
            row = cur.fetchone()
            return _row_value(row, 0, "relation_type")
        finally:
            conn.close()

    def direct_get_view_column_headings(self, view: str) -> list[str]:
        view_name = self._canonicalise_table_name_for_cache(view)
        conn = self._short_connection()
        try:
            cur = conn.execute(
                """
                select column_name
                from information_schema.columns
                where table_schema = %s and table_name = %s
                order by ordinal_position
                """,
                (self.schema, view_name),
            )
            headings = [str(_row_value(row, 0, "column_name")) for row in cur.fetchall()]
        finally:
            conn.close()
        if not headings:
            raise InputIntegrityError(f"view {view_name!r} not found or has no columns")
        return headings

    def direct_get_view_row_dict_from_id(self, view: str, row_id: int) -> dict[str, Any] | None:
        view_name = self._canonicalise_table_name_for_cache(view)
        if self.direct_get_relation_type(view_name) != "view":
            raise InputIntegrityError(f"view {view_name!r} not found")

        headings = self.direct_get_view_column_headings(view_name)
        conn = self._short_connection()
        try:
            cur = conn.execute(
                f"select * from {self._table_sql(view_name)} where {_q('id')} = %s",
                (row_id,),
            )
            rows = cur.fetchall()
        finally:
            conn.close()

        if len(rows) > 1:
            raise DatabaseIntegrityError(f"Search yielded multiple rows for view {view_name}.id={row_id!r}")
        if not rows:
            return None
        return self._row_to_dict_from_db_row(table=view_name, headings=headings, row=rows[0])

    def direct_get_triggers(self) -> list[str]:
        conn = self._short_connection()
        try:
            cur = conn.execute(
                """
                select distinct trigger_name
                from information_schema.triggers
                where trigger_schema = %s
                order by trigger_name
                """,
                (self.schema,),
            )
            return [str(_row_value(row, 0, "trigger_name")) for row in cur.fetchall()]
        finally:
            conn.close()

    def direct_drop_triggers(self, triggers: Sequence[str]) -> bool:
        trigger_names = [str(trigger).strip() for trigger in triggers if str(trigger).strip()]
        if not trigger_names:
            return True

        conn = self._primary_connection()
        try:
            with conn:
                for trigger_name in trigger_names:
                    rows = self._trigger_tables(trigger_name, conn=conn)
                    for table_name in rows:
                        conn.execute(
                            f"drop trigger if exists {_q(trigger_name)} on {self._table_sql(table_name)} cascade"
                        )
            self._zero_prop_cache()
            return True
        except Exception as exc:
            err_str = default_log.log_exception(
                "PostgreSQL trigger drop failed.",
                exc,
                "ERROR",
                ("triggers", trigger_names),
                ("database_url", self.redacted_database_url),
            )
            raise DatabaseDriverError(err_str) from exc

    def _trigger_tables(self, trigger_name: str, *, conn: PostgresConnectionAdapter) -> list[str]:
        cur = conn.execute(
            """
            select distinct event_object_table
            from information_schema.triggers
            where trigger_schema = %s and trigger_name = %s
            order by event_object_table
            """,
            (self.schema, trigger_name),
        )
        return [str(_row_value(row, 0, "event_object_table")) for row in cur.fetchall()]

    def direct_get_record_count(self, target_table: str) -> int:
        self._assert_existing_table(target_table)
        conn = self._short_connection()
        try:
            cur = conn.execute(f"select count(*) from {self._table_sql(target_table)}")
            row = cur.fetchone()
            return int(_row_value(row, 0, "count") or 0)
        finally:
            conn.close()

    def direct_get_all_rows(self, table: str, sort_column: str | None = None, reverse: bool = False) -> list[dict[str, Any]]:
        table = self._canonicalise_table_name_for_cache(table)
        headings = self.direct_get_column_headings(table)
        if sort_column is not None and sort_column not in headings:
            raise InputIntegrityError(f"sort column {sort_column!r} not found in table {table!r}")

        order_sql = ""
        if sort_column is not None:
            direction = "desc" if reverse else "asc"
            order_sql = f" order by {_q(sort_column)} {direction}"

        conn = self._short_connection()
        try:
            cur = conn.execute(f"select * from {self._table_sql(table)}{order_sql}")
            return [self._row_to_dict_from_db_row(table=table, headings=headings, row=row) for row in cur.fetchall()]
        finally:
            conn.close()

    def direct_get_row_dict_from_id(self, table: str, row_id: int) -> dict[str, Any] | bool:
        table = self._canonicalise_table_name_for_cache(table)
        headings = self.direct_get_column_headings(table)
        table_id_name = self.direct_get_id_column(table)
        conn = self._short_connection()
        try:
            cur = conn.execute(
                f"select * from {self._table_sql(table)} where {_q(table_id_name)} = %s",
                (row_id,),
            )
            rows = cur.fetchall()
        finally:
            conn.close()

        if len(rows) > 1:
            raise DatabaseDriverError(f"Search yielded multiple rows for {table}.{table_id_name}={row_id!r}")
        if not rows:
            return False
        return self._row_to_dict_from_db_row(table=table, headings=headings, row=rows[0])

    def direct_search_table(self, table: str, column: str, search_term: Any) -> list[dict[str, Any]]:
        table = self._canonicalise_table_name_for_cache(table)
        headings = self.direct_get_column_headings(table)
        if column not in headings:
            raise InputIntegrityError(f"column {column!r} not found in table {table!r}")
        if search_term is None:
            raise InputIntegrityError("PostgreSQL direct_search_table requires a non-None search term.")

        stmt = f"select * from {self._table_sql(table)} where {_q(column)} = %s"
        values = (search_term,)

        conn = self._short_connection()
        try:
            cur = conn.execute(stmt, values)
            return [self._row_to_dict_from_db_row(table=table, headings=headings, row=row) for row in cur.fetchall()]
        finally:
            conn.close()

    def direct_multi_column_search(
        self,
        search_index: Sequence[Sequence[Any]],
        iterator_return: bool = False,
    ) -> Iterator[dict[str, Any]] | list[dict[str, Any]] | None:
        if not search_index:
            return None

        columns = []
        for term in search_index:
            try:
                columns.append(str(term[0]))
            except Exception as exc:
                raise InputIntegrityError(f"Malformed search term: {term!r}") from exc

        tables = {self.direct_identify_table_from_column(column) for column in columns}
        if len(tables) != 1:
            raise InputIntegrityError(f"Columns must belong to one table: {columns!r}")
        table = self._canonicalise_table_name_for_cache(tables.pop())
        headings = self.direct_get_column_headings(table)

        predicates: list[str] = []
        bindings: list[Any] = []
        for term in search_index:
            try:
                column, operator, search_term = term[0], term[1], term[2]
            except Exception as exc:
                raise InputIntegrityError(f"Malformed search term: {term!r}") from exc

            column = str(column).strip()
            if column not in headings:
                raise InputIntegrityError(f"column {column!r} not found in table {table!r}")
            op = str(operator).strip().upper()
            if op not in {"=", "==", "!=", "<>", "<", "<=", ">", ">=", "LIKE", "ILIKE", "IN", "IS", "IS NOT"}:
                raise InputIntegrityError(f"Unsupported PostgreSQL search operator: {operator!r}")
            _reject_unsafe_search_value(search_term)

            if search_term is None:
                if op in {"=", "==", "IS"}:
                    predicates.append(f"{_q(column)} is null")
                    continue
                if op in {"!=", "<>", "IS NOT"}:
                    predicates.append(f"{_q(column)} is not null")
                    continue
                raise InputIntegrityError(f"operator {operator!r} cannot be used with None")

            if op == "IN":
                if isinstance(search_term, (str, bytes, bytearray)) or not hasattr(search_term, "__iter__"):
                    raise InputIntegrityError("IN operator requires a non-string iterable")
                values = list(search_term)
                if not values:
                    predicates.append("1 = 0")
                    continue
                for value in values:
                    _reject_unsafe_search_value(value)
                placeholders = ", ".join(["%s"] * len(values))
                predicates.append(f"{_q(column)} in ({placeholders})")
                bindings.extend(values)
                continue

            if op == "==":
                op = "="
            predicates.append(f"{_q(column)} {op.lower()} %s")
            bindings.append(search_term)

        stmt = f"select * from {self._table_sql(table)} where " + " and ".join(predicates)
        if iterator_return:
            return self._iterator_return(stmt, headings, table=table, bindings=tuple(bindings))

        conn = self._short_connection()
        try:
            cur = conn.execute(stmt, tuple(bindings))
            return [self._row_to_dict_from_db_row(table=table, headings=headings, row=row) for row in cur.fetchall()]
        finally:
            conn.close()

    def direct_get_random_row_dict(self, target_table: str, direct: bool = False) -> dict[str, Any] | None:
        table = self._canonicalise_table_name_for_cache(target_table)
        headings = self.direct_get_column_headings(table)
        conn = self._short_connection()
        try:
            cur = conn.execute(f"select * from {self._table_sql(table)} order by random() limit 1")
            row = cur.fetchone()
            if row is None:
                return None
            return self._row_to_dict_from_db_row(table=table, headings=headings, row=row)
        finally:
            conn.close()

    def direct_get_row_dict_iterator(
        self,
        table: str,
        sort_column: str | None = None,
        reverse: bool = False,
    ) -> Iterator[dict[str, Any]]:
        table = self._canonicalise_table_name_for_cache(table)
        headings = self.direct_get_column_headings(table)
        if sort_column is not None and sort_column not in headings:
            raise InputIntegrityError(f"sort column {sort_column!r} not found in table {table!r}")

        if sort_column is not None:
            direction = "desc" if reverse else "asc"
            stmt = f"select * from {self._table_sql(table)} order by {_q(sort_column)} {direction}"
            yield from self._iterator_return(stmt, headings, table=table)
            return

        id_column = self.direct_get_id_column(table)
        start_id = 0
        while True:
            conn = self._short_connection()
            try:
                cur = conn.execute(
                    f"select * from {self._table_sql(table)} "
                    f"where {_q(id_column)} > %s order by {_q(id_column)} limit 10",
                    (start_id,),
                )
                rows = cur.fetchall()
            finally:
                conn.close()
            if not rows:
                break
            for row in rows:
                row_dict = self._row_to_dict_from_db_row(table=table, headings=headings, row=row)
                yield row_dict
                start_id = int(row_dict[id_column])

    def direct_get_unique_values_set(self, target_column: str) -> set[Any]:
        target_table = self.direct_identify_table_from_column(target_column)
        self._assert_existing_column(target_table, target_column)
        conn = self._short_connection()
        try:
            cur = conn.execute(f"select distinct {_q(target_column)} from {self._table_sql(target_table)}")
            return {_row_value(row, 0, target_column) for row in cur.fetchall()}
        finally:
            conn.close()

    def direct_get_unique_values_iterator(self, target_column: str) -> Iterator[Any]:
        for value in self.direct_get_unique_values_set(target_column):
            yield value

    def direct_get_max(self, column: str) -> int | None:
        return self._direct_get_column_extreme(column, function_name="max")

    def direct_get_min(self, column: str) -> int | None:
        return self._direct_get_column_extreme(column, function_name="min")

    def direct_add_simple_row_dict(self, row_dict: dict[str, Any]) -> Any:
        row_dict = dict(row_dict)
        target_table = self.direct_identify_table_from_row(row_dict)
        row_dict.pop("table", None)

        table_id_col = self.direct_get_id_column(target_table)
        columns = list(row_dict)
        conn = self._primary_connection()
        if not columns:
            stmt = f"insert into {self._table_sql(target_table)} default values returning {_q(table_id_col)}"
            values = None
        else:
            col_sql = ", ".join(_q(column) for column in columns)
            placeholders = ", ".join(["%s"] * len(columns))
            stmt = (
                f"insert into {self._table_sql(target_table)} ({col_sql}) "
                f"values ({placeholders}) returning {_q(table_id_col)}"
            )
            values = tuple(row_dict[column] for column in columns)

        try:
            with conn:
                cur = conn.execute(stmt, values)
                row = cur.fetchone()
            self._zero_prop_cache()
            return _row_value(row, 0, table_id_col)
        except Exception as exc:
            err_str = default_log.log_exception(
                "PostgreSQL row insert failed.",
                exc,
                "ERROR",
                ("target_table", target_table),
                ("row_dict", row_dict),
                ("database_url", self.redacted_database_url),
            )
            raise DatabaseDriverError(err_str) from exc

    def direct_add_multiple_simple_row_dicts(self, row_dict_list: list[dict[str, Any]]) -> bool:
        for row_dict in row_dict_list:
            self.direct_add_simple_row_dict(row_dict)
        return True

    def direct_update_row_dict(self, row_dict: dict[str, Any]) -> bool:
        target_table = self.direct_identify_table_from_row(row_dict)
        row_dict = deepcopy(dict(row_dict))
        row_dict.pop("table", None)

        table_id_col = self.direct_get_id_column(target_table)
        if table_id_col not in row_dict:
            raise InputIntegrityError(f"Cannot update {target_table!r}: missing id column {table_id_col!r}")
        target_row_id = row_dict.pop(table_id_col)
        if not row_dict:
            return True

        assignments = ", ".join(f"{_q(column)} = %s" for column in row_dict)
        values = tuple(None if value == "None" else value for value in row_dict.values()) + (target_row_id,)
        stmt = f"update {self._table_sql(target_table)} set {assignments} where {_q(table_id_col)} = %s"

        conn = self._primary_connection()
        try:
            with conn:
                conn.execute(stmt, values)
            self._zero_prop_cache()
            return True
        except Exception as exc:
            err_str = default_log.log_exception(
                "PostgreSQL row update failed.",
                exc,
                "ERROR",
                ("target_table", target_table),
                ("row_dict", row_dict),
                ("database_url", self.redacted_database_url),
            )
            raise DatabaseDriverError(err_str) from exc

    def direct_update_columns(self, id_values_map, field=None, table=None) -> bool:
        if not id_values_map:
            return True
        if field is None:
            raise InputIntegrityError("PostgreSQL direct_update_columns requires a field for one-column updates.")
        target_table = table or self.direct_identify_table_from_column(field)
        table_id_col = self.direct_get_id_column(target_table)
        rows = [
            (None if value == "None" else value, row_id)
            for row_id, value in id_values_map.items()
        ]
        conn = self._primary_connection()
        with conn:
            conn.executemany(
                f"update {self._table_sql(target_table)} set {_q(field)} = %s where {_q(table_id_col)} = %s",
                rows,
            )
        self._zero_prop_cache()
        return True

    def direct_delete_many_by_ids(self, target_table: str, row_ids) -> bool:
        table = self._canonicalise_table_name_for_cache(target_table)
        self._assert_existing_table(table)
        id_column = self.direct_get_id_column(table)
        values = [(row_id,) for row_id in row_ids]
        if not values:
            return True

        conn = self._primary_connection()
        try:
            with conn:
                conn.executemany(
                    f"delete from {self._table_sql(table)} where {_q(id_column)} = %s",
                    values,
                )
            self._zero_prop_cache()
            return True
        except Exception as exc:
            err_str = default_log.log_exception(
                "PostgreSQL delete-many-by-ids failed.",
                exc,
                "ERROR",
                ("target_table", table),
                ("row_ids", [value[0] for value in values]),
                ("database_url", self.redacted_database_url),
            )
            raise DatabaseDriverError(err_str) from exc

    def direct_delete(self, target_table: str, column: str, value: Any, many: bool = False) -> bool:
        table = self._canonicalise_table_name_for_cache(target_table)
        self._assert_existing_table(table)
        self._assert_existing_column(table, column)

        conn = self._primary_connection()
        try:
            with conn:
                if many:
                    values = [(item,) for item in value]
                    if not values:
                        return True
                    conn.executemany(
                        f"delete from {self._table_sql(table)} where {_q(column)} = %s",
                        values,
                    )
                elif value is None:
                    conn.execute(f"delete from {self._table_sql(table)} where {_q(column)} is null")
                else:
                    conn.execute(
                        f"delete from {self._table_sql(table)} where {_q(column)} = %s",
                        (value,),
                    )
            self._zero_prop_cache()
            return True
        except TypeError as exc:
            raise InputIntegrityError("PostgreSQL multi-value delete requires an iterable value.") from exc
        except Exception as exc:
            err_str = default_log.log_exception(
                "PostgreSQL delete failed.",
                exc,
                "ERROR",
                ("target_table", table),
                ("column", column),
                ("value", value),
                ("many", many),
                ("database_url", self.redacted_database_url),
            )
            raise DatabaseDriverError(err_str) from exc

    def direct_delete_many(self, target_table: str, column: str, values: Any) -> bool:
        return self.direct_delete(target_table=target_table, column=column, value=values, many=True)

    def direct_delete_row_by_id(self, target_table: str, row_id: int) -> bool:
        table = self._canonicalise_table_name_for_cache(target_table)
        self._assert_existing_table(table)
        id_column = self.direct_get_id_column(table)

        conn = self._primary_connection()
        try:
            with conn:
                conn.execute(
                    f"delete from {self._table_sql(table)} where {_q(id_column)} = %s",
                    (row_id,),
                )
            self._zero_prop_cache()
            return True
        except Exception as exc:
            err_str = default_log.log_exception(
                "PostgreSQL delete-by-id failed.",
                exc,
                "ERROR",
                ("target_table", table),
                ("row_id", row_id),
                ("database_url", self.redacted_database_url),
            )
            raise DatabaseDriverError(err_str) from exc

    def direct_clear_table(self, target_table: str) -> bool:
        table = self._canonicalise_table_name_for_cache(target_table)
        self._assert_existing_table(table)

        conn = self._primary_connection()
        try:
            with conn:
                conn.execute(f"delete from {self._table_sql(table)}")
                cur = conn.execute(f"select count(*) from {self._table_sql(table)}")
                row = cur.fetchone()
            self._zero_prop_cache()
            return int(_row_value(row, 0, "count") or 0) == 0
        except Exception as exc:
            err_str = default_log.log_exception(
                "PostgreSQL table clear failed.",
                exc,
                "ERROR",
                ("target_table", table),
                ("database_url", self.redacted_database_url),
            )
            raise DatabaseDriverError(err_str) from exc

    def direct_get_highest_id(self, target_table: str) -> int:
        id_col = self.direct_get_id_column(target_table)
        conn = self._short_connection()
        try:
            cur = conn.execute(f"select coalesce(max({_q(id_col)}), 0) from {self._table_sql(target_table)}")
            row = cur.fetchone()
            return int(_row_value(row, 0, "max") or 0)
        finally:
            conn.close()

    def direct_get_db_unique_id(self):
        conn = self._short_connection()
        try:
            cur = conn.execute(
                f'select "database_metadata_unique_id" from {self._table_sql("database_metadata")} limit 2'
            )
            rows = cur.fetchall()
        finally:
            conn.close()
        if not rows:
            return None
        if len(rows) > 1:
            raise DatabaseDriverError("database_metadata has more than one row.")
        return _row_value(rows[0], 0, "database_metadata_unique_id")

    def direct_set_db_unique_id(self, force_value=None) -> None:
        unique_id = str(force_value or uuid.uuid4())
        conn = self._primary_connection()
        with conn:
            cur = conn.execute(f'select "database_metadata_id" from {self._table_sql("database_metadata")} limit 1')
            row = cur.fetchone()
            if row:
                database_metadata_id = _row_value(row, 0, "database_metadata_id")
                conn.execute(
                    f'update {self._table_sql("database_metadata")} set "database_metadata_unique_id" = %s '
                    f'where "database_metadata_id" = %s',
                    (unique_id, database_metadata_id),
                )
            else:
                conn.execute(
                    f'insert into {self._table_sql("database_metadata")} '
                    '("database_metadata_unique_id") values (%s)',
                    (unique_id,),
                )

    def direct_read_metadata(self, md_field_name: str) -> Any:
        field = self._metadata_field_name(md_field_name)
        conn = self._short_connection()
        try:
            cur = conn.execute(f"select {_q(field)} from {self._table_sql('database_metadata')} limit 1")
            row = cur.fetchone()
            return None if row is None else _row_value(row, 0, field)
        finally:
            conn.close()

    def direct_write_metadata(self, md_field_name: str, md_field_value: Any) -> None:
        field = self._metadata_field_name(md_field_name)
        conn = self._primary_connection()
        with conn:
            cur = conn.execute(f'select "database_metadata_id" from {self._table_sql("database_metadata")} limit 1')
            row = cur.fetchone()
            if row:
                database_metadata_id = _row_value(row, 0, "database_metadata_id")
                conn.execute(
                    f"update {self._table_sql('database_metadata')} "
                    f"set {_q(field)} = %s where \"database_metadata_id\" = %s",
                    (md_field_value, database_metadata_id),
                )
            else:
                conn.execute(
                    f"insert into {self._table_sql('database_metadata')} ({_q(field)}) values (%s)",
                    (md_field_value,),
                )

    def _metadata_field_name(self, md_field_name: str) -> str:
        field = str(md_field_name)
        if not field.startswith("database_metadata_"):
            field = "database_metadata_" + field
        allowed_values = self.direct_get_column_headings("database_metadata")
        if field not in allowed_values:
            raise InputIntegrityError(f"metadata field {field!r} not found")
        return field

    def _assert_existing_table(self, table: str) -> None:
        if table not in self.direct_get_tables_and_columns():
            raise InputIntegrityError(f"table {table!r} not found")

    def _assert_existing_column(self, table: str, column: str) -> None:
        if column not in self.direct_get_column_headings(table):
            raise InputIntegrityError(f"column {column!r} not found in table {table!r}")

    def _row_to_dict_from_db_row(self, *, table: str, headings: Sequence[str], row: Any) -> dict[str, Any]:
        if isinstance(row, Mapping):
            values = tuple(row.get(heading) for heading in headings)
        else:
            values = tuple(row)
        return self._row_to_dict(table=table, headings=headings, row=values)

    def _execute_schema_statements(self, statements: Sequence[str]) -> None:
        conn = self._primary_connection()
        try:
            with conn:
                for statement in statements:
                    conn.execute(statement)
            self._zero_prop_cache()
        except Exception as exc:
            err_str = default_log.log_exception(
                "PostgreSQL schema statement execution failed.",
                exc,
                "ERROR",
                ("database_url", self.redacted_database_url),
            )
            raise DatabaseDriverError(err_str) from exc

    def _iterator_return(
        self,
        stmt: str,
        headings: Sequence[str],
        *,
        table: str,
        bindings: Sequence[Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        conn = self._short_connection()
        try:
            cur = conn.execute(stmt, bindings)
            for row in cur.fetchall():
                yield self._row_to_dict_from_db_row(table=table, headings=headings, row=row)
        finally:
            conn.close()

    def _direct_get_column_extreme(self, column: str, *, function_name: str) -> int | None:
        if function_name not in {"max", "min"}:
            raise InputIntegrityError(f"Unsupported column aggregate: {function_name!r}")
        target_table = self.direct_identify_table_from_column(column)
        self._assert_existing_column(target_table, column)
        conn = self._short_connection()
        try:
            cur = conn.execute(f"select {function_name}({_q(column)}) from {self._table_sql(target_table)}")
            row = cur.fetchone()
            value = _row_value(row, 0, function_name)
        finally:
            conn.close()
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _canonicalise_table_name_for_cache(table: str) -> str:
        text = str(table).strip()
        if "." in text:
            text = text.split(".")[-1].strip()
        if text.startswith("[") and text.endswith("]") and len(text) >= 2:
            return text[1:-1]
        if len(text) >= 2 and text[0] == text[-1] and text[0] in {"`", '"', "\\", "%", "_"}:
            return text[1:-1]
        return text

    def _primary_connection(self) -> PostgresConnectionAdapter:
        conn = getattr(self, "conn", None)
        if conn is None:
            conn = self.get_connection()
            self.conn = conn
        else:
            try:
                conn.execute("select 1")
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                conn = self.get_connection()
                self.conn = conn
        return conn

    def _short_connection(self) -> PostgresConnectionAdapter:
        return self.get_connection()


def _q(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _qualified_table(schema: str, table_name: str) -> str:
    return f"{_q(schema)}.{_q(table_name)}"


def _assert_safe_identifier(value: str, *, kind: str = "identifier") -> str:
    text = str(value).strip()
    if not text:
        raise InputIntegrityError(f"PostgreSQL {kind} cannot be blank.")
    if not all(char.isalnum() or char == "_" for char in text):
        raise InputIntegrityError(f"PostgreSQL {kind} contains unsupported characters: {value!r}")
    if text[0].isdigit():
        raise InputIntegrityError(f"PostgreSQL {kind} cannot start with a digit: {value!r}")
    return text


def _postgres_column_type(datatype: str) -> str:
    text = str(datatype or "text").strip().lower()
    if text in {"text", "varchar", "character varying", "str", "string"}:
        return "text"
    if text in {"integer", "int", "bigint", "smallint"}:
        return "bigint" if text in {"integer", "int"} else text
    if text in {"real", "float", "double", "double precision"}:
        return "double precision"
    if text in {"bool", "boolean"}:
        return "boolean"
    if text in {"datetime", "timestamp", "timestamp with time zone", "timestamptz"}:
        return "timestamp with time zone"
    if text in {"json", "jsonb"}:
        return "jsonb"
    if text in {"blob", "bytes", "bytea"}:
        return "bytea"
    raise InputIntegrityError(f"Unsupported PostgreSQL column datatype: {datatype!r}")


def _pg_literal(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _normalise_requested_link_columns(requested_cols: str | Sequence[str] | None) -> str | set[str]:
    if requested_cols is None:
        return set()
    if isinstance(requested_cols, str):
        if requested_cols.strip().lower() == "all":
            return "all"
        raise InputIntegrityError("requested_cols must be 'all', None, or an iterable of column suffixes.")
    return {str(value).strip().lower() for value in requested_cols if str(value).strip()}


def _link_extra_columns(link_base: str, requested: str | set[str]) -> list[tuple[str, str]]:
    all_columns: dict[str, str] = {
        "priority": "bigint default 0",
        "primary": "bigint null default 0",
        "type": "text null",
        "origin": "text null",
        "source": "text null",
        "policy": "text null",
        "data": "text null",
        "index": "text null",
        "sequence_number": "bigint null",
        "is_required": "bigint default 1",
    }
    if requested == "all":
        suffixes = list(all_columns)
    else:
        suffixes = [suffix for suffix in all_columns if suffix in requested]
        for extra in sorted(requested - set(all_columns) - {"nullable"}):
            _assert_safe_identifier(extra, kind="link extra column suffix")
            all_columns[extra] = "text null"
            suffixes.append(extra)
        if "source" not in suffixes:
            suffixes.append("source")

    out: list[tuple[str, str]] = []
    for suffix in suffixes:
        column = _assert_safe_identifier(f"{link_base}_{suffix}", kind="link column")
        out.append((suffix, f"{_q(column)} {all_columns[suffix]}"))
    out.append(("datestamp", f"{_q(link_base + '_datestamp')} timestamp with time zone default current_timestamp"))
    out.append(("scratch", f"{_q(link_base + '_scratch')} text null"))
    return out


def _link_constraints(
    *,
    link_type: str,
    link_base: str,
    left_base: str,
    right_base: str,
    left_fk_col: str,
    right_fk_col: str,
    has_type: bool,
    has_priority: bool,
) -> list[str]:
    constraints: list[str] = []
    type_col = f"{link_base}_type"
    priority_col = f"{link_base}_priority"

    def constraint_name(suffix: str) -> str:
        return _q(_assert_safe_identifier(f"{link_base}_{suffix}", kind="constraint"))

    if link_type == "many_many":
        constraints.append(
            f"constraint {constraint_name('unique_pair')} unique ({_q(right_fk_col)}, {_q(left_fk_col)})"
        )
        if has_priority:
            constraints.append(
                f"constraint {constraint_name('priority_per_left')} unique ({_q(left_fk_col)}, {_q(priority_col)})"
            )
    elif link_type == "many_many_non_exclusive":
        if has_type:
            constraints.append(
                f"constraint {constraint_name('unique_pair_type')} "
                f"unique ({_q(right_fk_col)}, {_q(left_fk_col)}, {_q(type_col)})"
            )
            if has_priority:
                constraints.append(
                    f"constraint {constraint_name('priority_per_left_type')} "
                    f"unique ({_q(left_fk_col)}, {_q(type_col)}, {_q(priority_col)})"
                )
        else:
            constraints.append(
                f"constraint {constraint_name('unique_pair_nonexclusive')} "
                f"unique ({_q(right_fk_col)}, {_q(left_fk_col)})"
            )
    elif link_type == "one_many":
        constraints.append(
            f"constraint {constraint_name('one_many')} unique ({_q(right_fk_col)})"
        )
        if has_priority:
            constraints.append(
                f"constraint {constraint_name('priority_per_left')} unique ({_q(left_fk_col)}, {_q(priority_col)})"
            )
    elif link_type == "many_one":
        constraints.append(
            f"constraint {constraint_name('many_one')} unique ({_q(left_fk_col)})"
        )
        if has_priority:
            constraints.append(
                f"constraint {constraint_name('priority_per_right')} unique ({_q(right_fk_col)}, {_q(priority_col)})"
            )
    elif link_type in {"one_one", "one_one_normalized"}:
        constraints.append(
            f"constraint {constraint_name(left_base + '_appears_once')} unique ({_q(left_fk_col)})"
        )
        constraints.append(
            f"constraint {constraint_name(right_base + '_appears_once')} unique ({_q(right_fk_col)})"
        )
    elif link_type == "rating":
        if has_type:
            constraints.append(
                f"constraint {constraint_name('one_type_per_left')} unique ({_q(left_fk_col)}, {_q(type_col)})"
            )
        else:
            constraints.append(
                f"constraint {constraint_name('one_rating_per_left')} unique ({_q(left_fk_col)})"
            )
    else:
        raise NotImplementedError(f"PostgreSQL link_type not recognized: {link_type!r}")
    return constraints


def _link_table_name_col_name(primary_table: str, secondary_table: str) -> tuple[str, str]:
    bases = [plural_singular_mapper(str(primary_table)), plural_singular_mapper(str(secondary_table))]
    bases.sort()
    column_name = _assert_safe_identifier(f"{bases[0]}_{bases[1]}_link", kind="link column base")
    return f"{column_name}s", column_name


def _row_value(row: Any, position: int, key: str) -> Any:
    if row is None:
        return None
    if isinstance(row, Mapping):
        return row.get(key)
    try:
        return row[position]
    except Exception:
        return None


def _reject_unsafe_search_value(value: Any) -> None:
    if not isinstance(value, (str, bytes, bytearray)):
        return
    try:
        text = value.decode("utf-8", errors="replace") if isinstance(value, (bytes, bytearray)) else str(value)
    except Exception:
        return
    if ";" in text or "--" in text or "/*" in text or "*/" in text or "\x00" in text:
        raise InputIntegrityError("Unsafe-looking search value rejected")
