"""
Implement PostgreSQL schema introspection, native CRUD, link DDL and portable macros.

Read helpers generally open and close short connections; mutations generally use
the reusable primary connection and its transaction context. SQLBaseDriver supplies
shared cache/connection tracking, while ValueCastingMixin converts returned cells.
"""

from __future__ import annotations

import uuid
from copy import deepcopy
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import replace
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
from LiuXin_alpha.databases.api.portable_macros_api import PortableMacrosAPI
from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.portable_macros_mixin import (
    SQLPortableMacrosMixin,
)
from LiuXin_alpha.databases.normalized_identities import (
    add_derived_identity_values,
    default_normalized_identity_spec,
    normalize_identity_value,
    normalized_identity_defaults_for_table,
)
from LiuXin_alpha.databases.maintenance.dummy_maintenance_bot import DummyMaintenanceBot
from LiuXin_alpha.errors import DatabaseDriverError, DatabaseIntegrityError, InputIntegrityError
from LiuXin_alpha.utils.language_tools.pluralizers import plural_singular_mapper
from LiuXin_alpha.utils.logging import default_log


DEFAULT_SCHEMA = DEFAULT_POSTGRES_SCHEMA


class PostgresDatabaseMacros(
    MacrosBase,
    SQLPortableMacrosMixin,
    PortableMacrosAPI,
):
    """
    Combine portable macros with native PostgreSQL column updates.

    The attached database facade supplies reads and a driver wrapper for writes.
    Unknown macro attributes raise DatabaseDriverError rather than AttributeError.

    Example:
        ``PostgresDatabaseMacros(db)`` shares the facade and its configured driver;
        it does not create a separate connection.
    """

    @property
    def get(self):
        """
        Expose the attached database facade fetch callable.

        Example:
            ``macros.get(sql, values)`` uses the same fetch behavior as db.get.


        :return: Bound db.get callable.
        """
        return self.db.get

    @property
    def execute(self):
        """
        Expose the attached driver wrapper single-statement callable.

        Example:
            ``macros.execute(sql, values)`` uses the wrapper transaction behavior.


        :return: Bound driver_wrapper.execute callable.
        """
        return self.db.driver_wrapper.execute

    @property
    def executemany(self):
        """
        Expose the attached driver wrapper repeated-execution callable.

        Example:
            ``macros.executemany(sql, rows)`` forwards repeated parameter sets.


        :return: Bound driver_wrapper.executemany callable.
        """
        return self.db.driver_wrapper.executemany

    def direct_update_column_in_table(self, table, column, table_id_col, item_id, new_value):
        """
        Update one value and its derived identity when the default identity column exists.

        Bind values, quote identifiers and delegate execution to the wrapper. A None value
        clears the derived identity; other values use the default normalization profile.

        Example:
            Updating series to a new label also refreshes series_name_norm when that
            physical column is available.


        :param table: Table name resolved within the configured driver schema.
        :param column: Column name belonging to the selected table.
        :param table_id_col: ID column used in the WHERE predicate.
        :param item_id: ID of the row to update.
        :param new_value: Replacement value for the selected column.
        :return: None; executes a wrapper-managed update.
        """
        spec = default_normalized_identity_spec(table, column)
        if (
            spec is not None
            and spec.identity_column
            in set(self.db.driver_wrapper.get_column_headings(table))
        ):
            identity_value = (
                None
                if new_value is None
                else normalize_identity_value(
                    new_value,
                    spec.normalization_profile,
                )
            )
            stmt = (
                f"update {self._table_sql(table)} "
                f"set {_q(column)} = %s, {_q(spec.identity_column)} = %s "
                f"where {_q(table_id_col)} = %s"
            )
            self.execute(stmt, (new_value, identity_value, item_id))
        else:
            stmt = (
                f"update {self._table_sql(table)} "
                f"set {_q(column)} = %s where {_q(table_id_col)} = %s"
            )
            self.execute(stmt, (new_value, item_id))

    def _table_sql(self, table: str) -> str:
        """
        Quote a table in the attached driver schema, falling back to public.

        Use the driver canonicalizer when available; otherwise stringify the supplied name.

        Example:
            With driver.schema set to library, works becomes ``"library"."works"``.


        :param table: Table name resolved within the configured driver schema.
        :return: Quoted schema-qualified table reference.
        """
        driver = getattr(getattr(self.db, "driver_wrapper", None), "driver", None)
        schema = getattr(driver, "schema", DEFAULT_SCHEMA)
        canonicalise = getattr(driver, "_canonicalise_table_name_for_cache", None)
        table_name = canonicalise(table) if callable(canonicalise) else str(table)
        return _qualified_table(str(schema), str(table_name))

    def __getattr__(self, name: str) -> Any:
        """
        Reject macro attributes absent from this class and its inherited interfaces.

        Example:
            Requesting an unsupported macro raises DatabaseDriverError naming it.


        :param name: Missing attribute requested by Python attribute lookup.
        :return: Never returns; always raises DatabaseDriverError.
        """
        raise DatabaseDriverError(f"PostgreSQL macro {name!r} is not implemented yet.")


class DatabaseDriver(
    SQLBaseDriver,
    ValueCastingMixin,
    TableNamesMixin,
):
    """
    Provide the PostgreSQL implementation of the shared SQL driver contract.

    Construction resolves target/schema and optionally opens a tracked connection.
    Backup, database deletion and scratch switching are unsupported. Mutating helpers
    manage their own transaction contexts; callers must not assume a multi-call batch
    is atomic. Table names are normally resolved inside this driver schema.

    Example:
        ``DatabaseDriver({"service": "library"}, set_conn=False)`` prepares the
        backend without opening a connection until one is needed.
    """

    @staticmethod
    def direct_get_column_base(table_name: str) -> str:
        """
        Use the shared plural-to-singular mapping for column prefixes.

        Example:
            >>> DatabaseDriver.direct_get_column_base("digital_assets")
            'digital_asset'


        :param table_name: Table name normalized to its unqualified spelling.
        :return: Canonical singular column prefix.
        """

        return plural_singular_mapper(table_name)

    def __init__(self, db_metadata: Mapping[str, object], db=None, set_conn: bool = True, dirty_records_queue=None):
        """
        Resolve configuration and initialize caches, macros and optional connection.

        Copy metadata, require a configured URL/service, and retain the optional facade.
        Raw target fields may contain credentials; redacted_database_url is for display.
        Raise DatabaseDriverError when target resolution yields nothing.

        Example:
            ``DatabaseDriver({"postgres_url": "postgresql:///library"}, set_conn=False)``
            initializes state without requiring a live server.


        :param db_metadata: Connection metadata copied before configuration resolution.
        :param db: Optional owning database facade, used by macros and cache refresh hooks.
        :param set_conn: Whether to open the primary connection during construction.
        :param dirty_records_queue: Optional queue retained for shared driver dirty-record handling.
        :return: None; populates instance state.
        """
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
        """
        Open, configure and register a new connection adapter.

        Set the quoted schema search_path and commit that setting before returning.
        The caller owns closing the returned handle; registration also enables shared cleanup.

        Example:
            ``conn = driver.get_connection()`` returns an adapter using driver.schema.


        :return: New tracked PostgreSQL connection adapter.
        """
        raw = connect_postgres(self.db_metadata)
        conn = PostgresConnectionAdapter(raw)
        conn.execute(f"set search_path to {_q(self.schema)}")
        conn.commit()
        return self._register_open_connection(conn)

    def exists(self) -> bool:
        """
        Probe reachability with SELECT 1 and close the probe connection.

        Return False and log redacted details on failure. This checks connectivity rather
        than the existence or completeness of LiuXin schema tables.

        Example:
            A reachable empty PostgreSQL database can return True from driver.exists().


        :return: Whether a new connection can execute the probe.
        """
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
        """
        Reject file-copy backup through the database driver.

        Use PostgreSQL backup tooling outside this API.

        Example:
            ``driver.direct_backup(path)`` raises DatabaseDriverError before creating a file.


        :param path: Unused destination retained by the shared driver contract.
        :return: Never returns; always raises DatabaseDriverError.
        """
        raise DatabaseDriverError("PostgreSQL backup is not a file copy. Use pg_dump/base backups outside this driver.")

    def direct_self_delete(self):
        """
        Reject dropping a PostgreSQL database through this driver.

        Example:
            ``driver.direct_self_delete()`` raises DatabaseDriverError without issuing DROP DATABASE.


        :return: Never returns; always raises DatabaseDriverError.
        """
        raise DatabaseDriverError("PostgreSQL databases are not deleted by the LiuXin driver.")

    def make_scratch(self) -> str:
        """
        Reject unsupported PostgreSQL scratch-database switching.

        Example:
            ``driver.make_scratch()`` raises DatabaseDriverError.


        :return: Never returns; always raises DatabaseDriverError.
        """
        raise DatabaseDriverError("PostgreSQL scratch database switching is not implemented.")

    def direct_create_new_database(self) -> None:
        """
        Initialize managed schema objects in the already configured database.

        Use the primary connection, then refresh shared property caches. This does not
        create the server database or migrate incompatible existing tables.

        Example:
            For an existing library database, this creates missing managed tables in
            driver.schema.


        :return: None; initializes schema objects and refreshes caches.
        """
        conn = self._primary_connection()
        create_postgres_schema(conn, schema=self.schema)
        self._zero_prop_cache()

    def _table_sql(self, table: str) -> str:
        """
        Canonicalize the table name and quote it within this driver schema.

        Example:
            With schema library, a qualified input public.works resolves to
            ``"library"."works"`` because the supplied qualifier is discarded.


        :param table: Table name resolved within the configured driver schema.
        :return: Quoted relation reference in the configured schema.
        """
        return _qualified_table(self.schema, self._canonicalise_table_name_for_cache(table))

    def direct_execute_sql_script(self, script: str | list[str]) -> None:
        """
        Join list fragments with newlines and run the direct script executor.

        The executor splits on semicolons without parsing SQL literals or procedural blocks.

        Example:
            ``driver.direct_execute_sql_script(["select 1;", "select 2;"])``
            executes the joined simple script.


        :param script: SQL string or list of string fragments to join.
        :return: None; delegates transaction handling to direct_executescript.
        """
        return self.direct_executescript("\n".join(script) if isinstance(script, list) else script)

    def direct_execute_sql(self, sql: str, parameters: Sequence[Any] | None = None) -> Any:
        """
        Execute SQL and return the cursor lastrowid attribute when available.

        The PostgreSQL cursor adapter sets lastrowid to None; use INSERT ... RETURNING
        through a fetching API when an inserted ID is required.

        Example:
            ``driver.direct_execute_sql("select 1")`` returns None with the native adapter.


        :param sql: Direct SQL passed through the connection adapter translations.
        :param parameters: Optional values bound by the direct executor.
        :return: Cursor lastrowid, normally None for this backend.
        """
        cur = self.direct_execute(sql, parameters)
        return getattr(cur, "lastrowid", None)

    def direct_execute(self, sql: str, values: Sequence[Any] | None = None) -> Any:
        """
        Execute one statement in the primary transaction context and refresh caches.

        Return the cursor after transaction exit. Execution/cache-refresh failures are
        logged and wrapped as DatabaseDriverError; connection acquisition precedes the
        wrapper. The caller owns cleanup of the returned cursor.

        Example:
            ``driver.direct_execute("select work_id from works")`` returns a fetchable cursor.


        :param sql: Direct SQL passed through the connection adapter translations.
        :param values: Optional bound parameter sequence.
        :return: Cursor adapter returned by connection.execute.
        """
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
        """
        Execute repeated parameter sets in one primary-connection transaction.

        Treat None or an empty sequence as no parameter sets, then refresh shared caches.
        Execution failures are logged and wrapped as DatabaseDriverError.

        Example:
            A batch of insert parameter tuples is executed in one transaction context.


        :param sql: Direct SQL passed through the connection adapter translations.
        :param values: Parameter sequences, or None for an empty batch.
        :return: None; commits through context exit and refreshes caches.
        """
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
        """
        Execute a simple semicolon-separated script in one primary transaction.

        The adapter does not parse embedded semicolons. Refresh shared caches on success;
        wrap execution failures as DatabaseDriverError.

        Example:
            ``driver.direct_executescript("select 1; select 2;")`` executes two statements.


        :param sqlscript: Script suitable for the adapter semicolon splitter.
        :return: None; transaction and cache handling are performed here.
        """
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
        """
        Expose the current schema fingerprint converted to text.

        This is not a stored SQLite-style integer user_version; a missing fingerprint
        becomes the string None.

        Example:
            ``driver.user_version`` reflects the information_schema column fingerprint.


        :return: String form of direct_get_schema_version().
        """
        return str(self.direct_get_schema_version())

    def direct_get_user_version(self) -> str:
        """
        Return the textual schema fingerprint exposed by user_version.

        Example:
            ``driver.direct_get_user_version() == driver.user_version`` when schema is unchanged.


        :return: Current user_version property value.
        """
        return self.user_version

    def direct_create_main_table(
        self,
        table_name: str,
        column_headings: Mapping[str, Mapping[str, Any]] | Sequence[str] | None = None,
        index_on: str | Sequence[str] | None = "all",
        default_datatype: str = "TEXT",
        default_unique: bool = False,
    ) -> None:
        """
        Build and execute a conventional main table and selected column indexes.

        Add a bigserial ID, nullable data columns, timestamp datestamp and scratch text.
        Normalize/validate names and map supported data types before executing DDL.
        IF NOT EXISTS preserves existing objects rather than reconciling their definitions.

        Example:
            ``driver.direct_create_main_table("notes", ["label"], index_on=None)``
            creates note_id, note_label, note_datestamp and note_scratch.


        :param table_name: Table name normalized to its unqualified spelling.
        :param column_headings: None for one base-named data column, suffix sequence, or suffix-to-datatype/unique mappings.
        :param index_on: all, None, or requested suffix/full names; without headings only all/None/empty are supported.
        :param default_datatype: Fallback type mapped to a supported PostgreSQL type.
        :param default_unique: Fallback uniqueness flag applied to each requested data column.
        :return: None; executes schema statements and refreshes shared caches.
        """
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
        """
        Generate native link-table DDL after validating both existing main tables.

        Sort singular table bases to choose link names and swap one/many orientation when
        needed. Add cardinality constraints, optional indexes and an optional type-label
        table. The type-label table does not itself restrict link values. No DDL is executed
        here, although table/column validation can query the database.

        Example:
            Linking agents and works produces agent_work_links and foreign keys to
            the two existing ID columns.


        :param primary_table: First main table in the requested relationship.
        :param secondary_table: Second main table in the requested relationship.
        :param link_type: Relationship cardinality: many_many, many_many_non_exclusive, one_many, many_one, one_one, one_one_normalized or rating.
        :param requested_cols: all, None, or a sequence of optional link-column suffixes.
        :param index_both: Whether to index both link foreign-key columns.
        :param allowed_types: Optional type labels stored in a companion __types table; no enforcing foreign key is created.
        :param one_link_with_one_type: Accepted but currently ignored; constraints follow link_type and requested columns.
        :param override_restriction_sql: Must be None; raw SQLite restriction SQL is unsupported.
        :param nullable_fks: Whether link foreign keys may be null.
        :return: Pair of SQL statement list and generated link-table name.
        """
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
        """
        Create a link table and optionally seed its type-label companion in one transaction.

        Generate DDL from existing table metadata, bind allowed type values separately,
        then refresh caches. Execution failures become DatabaseDriverError; validation
        errors from DDL generation propagate directly.

        Example:
            ``driver.direct_link_main_tables("agents", "works", allowed_types=("author",))``
            creates the link objects and seeds an author label.


        :param primary_table: First main table in the requested relationship.
        :param secondary_table: Second main table in the requested relationship.
        :param link_type: Relationship cardinality: many_many, many_many_non_exclusive, one_many, many_one, one_one, one_one_normalized or rating.
        :param requested_cols: all, None, or a sequence of optional link-column suffixes.
        :param index_both: Whether to index both link foreign-key columns.
        :param allowed_types: Optional type labels stored in a companion __types table; no enforcing foreign key is created.
        :param override_restriction_sql: Must be None; raw SQLite restriction SQL is unsupported.
        :param nullable_fks: Whether link foreign keys may be null.
        :return: Generated link-table name after successful transaction exit.
        """
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
        """
        Drop the derived link table with CASCADE and refresh shared caches.

        The separate __types companion is not explicitly dropped. Input table names feed
        the shared singular-name convention without preliminary table lookup.

        Example:
            ``driver.direct_unlink_main_tables("agents", "works")`` drops
            agent_work_links when present.


        :param primary_table: First main table in the requested relationship.
        :param secondary_table: Second main table in the requested relationship.
        :return: None; executes the drop transaction or raises DatabaseDriverError.
        """
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
        """
        Hash ordered schema/table/column/data-type catalog text on a short connection.

        The fingerprint does not encode indexes, triggers, constraints or defaults.
        Close the connection after reading.

        Example:
            Changing a column name or information_schema data_type changes the fingerprint.


        :return: Catalog MD5 text, or None if no row value is available.
        """
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
        """
        Clear table, column and declared-type caches and discard the cached fingerprint.

        Example:
            Call this before a forced schema refresh so cached headings are not reused.


        :return: None; mutates cache attributes.
        """
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
        """
        List sorted base-table/view names with fingerprint-aware caching.

        Reuse the cached list when either fingerprint is unavailable or both match.
        Return the cache object itself rather than a defensive copy.

        Example:
            ``driver.direct_get_tables(force_refresh=True)`` reads the configured schema anew.


        :param force_refresh: Whether to discard cached schema information before querying.
        :return: Cached or newly queried list of relation names.
        """
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
        """
        Read ordered column headings per relation, reusing a matching schema cache.

        A refresh also updates the table-name cache and stored fingerprint. Returned lists
        and mapping are shared cache objects, not copies.

        Example:
            ``driver.direct_get_tables_and_columns()["works"]`` lists physical work columns.


        :param force_refresh: Whether to discard cached schema information before querying.
        :return: Mapping from table/view names to ordered column-name lists.
        """
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

    def _get_unique_column_groups(self, table: str) -> tuple[tuple[str, ...], ...]:
        """
        Read column groups from valid nonpartial, nonexpression unique indexes.

        Preserve index key order within each group and order groups by index OID.
        Partial/expression indexes are excluded; the connection is closed after reading.

        Example:
            A unique index on two plain columns contributes one two-name tuple.


        :param table: Table name resolved within the configured driver schema.
        :return: Tuple of indexed column-name tuples.
        """

        table = _assert_safe_identifier(
            self._canonicalise_table_name_for_cache(table),
            kind="table",
        )
        sql = """
            select array_agg(attribute.attname order by key_column.ordinality) as columns
            from pg_catalog.pg_index as index_info
            join pg_catalog.pg_class as relation
              on relation.oid = index_info.indrelid
            join pg_catalog.pg_namespace as namespace
              on namespace.oid = relation.relnamespace
            cross join lateral unnest(index_info.indkey)
              with ordinality as key_column(attribute_number, ordinality)
            join pg_catalog.pg_attribute as attribute
              on attribute.attrelid = relation.oid
             and attribute.attnum = key_column.attribute_number
            where namespace.nspname = %s
              and relation.relname = %s
              and index_info.indisunique
              and index_info.indisvalid
              and index_info.indpred is null
              and index_info.indexprs is null
            group by index_info.indexrelid
            order by index_info.indexrelid
        """
        conn = self._short_connection()
        try:
            rows = conn.execute(sql, (self.schema, table))
            groups: list[tuple[str, ...]] = []
            for row in rows:
                columns = _row_value(row, 0, "columns")
                if columns:
                    groups.append(tuple(str(column) for column in columns))
            return tuple(groups)
        finally:
            conn.close()

    def direct_get_column_headings(self, table: str, normalize: bool = False) -> list[str]:
        """
        Return cached headings for a canonicalized table or raise InputIntegrityError.

        Example:
            ``driver.direct_get_column_headings("works")`` returns the cache-owned heading list.


        :param table: Table name resolved within the configured driver schema.
        :param normalize: Accepted by the shared interface but currently unused.
        :return: Ordered column names from the schema cache.
        """
        table = self._canonicalise_table_name_for_cache(table)
        tables_and_columns = self.direct_get_tables_and_columns()
        try:
            return tables_and_columns[table]
        except KeyError as exc:
            raise InputIntegrityError(f"table {table} not found") from exc

    def direct_get_declared_types_for_table(self, table: str) -> dict[str, str]:
        """
        Cache information_schema data_type labels by canonical table name.

        Reuse a cached mapping without a fresh fingerprint check; schema invalidation clears
        that cache separately. A table with no catalog rows yields an empty mapping.

        Example:
            For managed digital_assets, digital_asset_size_bytes maps to bigint.


        :param table: Table name resolved within the configured driver schema.
        :return: Cache-owned mapping from columns to data_type strings.
        """
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
        """
        Supply declared types to inherited casting hooks using the public type cache.

        Example:
            Inherited row casting asks this hook for the same mapping as
            direct_get_declared_types_for_table.


        :param table: Table name resolved within the configured driver schema.
        :return: Column-to-data_type mapping from the shared cache.
        """
        return self.direct_get_declared_types_for_table(table)

    def direct_get_case_sensitivity(self, table: str, column: str) -> bool:
        """
        Read the case-sensitive flag from effective column metadata.

        Example:
            ``driver.direct_get_case_sensitivity("works", "work_title")`` reads the
            stored policy or its inferred fallback.


        :param table: Table name resolved within the configured driver schema.
        :param column: Column name belonging to the selected table.
        :return: Effective column-policy case_sensitive flag.
        """
        return self.direct_get_column_metadata(table, column).case_sensitive

    def direct_get_column_metadata(self, table: str, column: str) -> ColumnMetadata:
        """
        Resolve stored column policy with inferred defaults when catalog data is absent.

        Validate the target, infer a fallback from its type, then read the metadata row if
        available. Legacy catalogs without both presentation-option columns omit those
        fields. Close the short connection before constructing the result.

        Example:
            A physical work_title column without a metadata row receives its inferred
            title policy.


        :param table: Table name resolved within the configured driver schema.
        :param column: Column name belonging to the selected table.
        :return: Effective ColumnMetadata record.
        """
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

        catalog_columns = set(
            self.direct_get_column_headings(COLUMN_METADATA_TABLE)
        )
        option_columns = {
            "column_metadata_formatting_options_json",
            "column_metadata_display_options_json",
        }
        option_selection = (
            ',\n                  "column_metadata_formatting_options_json",'
            '\n                  "column_metadata_display_options_json"'
            if option_columns <= catalog_columns
            else ""
        )

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
                  {option_selection}
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
                        *(
                            (
                                "column_metadata_formatting_options_json",
                                "column_metadata_display_options_json",
                            )
                            if option_columns <= catalog_columns
                            else ()
                        ),
                    )
                )
            ),
        )

    def direct_set_column_metadata(self, metadata: ColumnMetadata) -> None:
        """
        Validate and upsert a complete column policy in a primary transaction.

        Reject incompatible normalized-identity settings and absent/outdated metadata
        catalogs. Both presentation-option columns must exist. Update every policy field
        on table/column conflict.

        Example:
            Persisting a modified ColumnMetadata record replaces its complete policy
            including formatting and display options.


        :param metadata: Complete ColumnMetadata record for an existing table/column.
        :return: None; commits the upsert through connection context exit.
        """
        metadata = self._validated_column_metadata_input(metadata)
        self._validate_normalized_identity_metadata(metadata)
        if COLUMN_METADATA_TABLE not in set(self.direct_get_tables()):
            raise DatabaseIntegrityError(
                "database has no column_metadata table; migrate the schema before storing column policy"
            )
        required_columns = {
            "column_metadata_formatting_options_json",
            "column_metadata_display_options_json",
        }
        if not required_columns <= set(
            self.direct_get_column_headings(COLUMN_METADATA_TABLE)
        ):
            raise DatabaseIntegrityError(
                "column_metadata schema is outdated; migrate it before storing presentation options"
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
                  "column_metadata_validation_profile",
                  "column_metadata_formatting_options_json",
                  "column_metadata_display_options_json"
                ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                  "column_metadata_validation_profile" = excluded."column_metadata_validation_profile",
                  "column_metadata_formatting_options_json" = excluded."column_metadata_formatting_options_json",
                  "column_metadata_display_options_json" = excluded."column_metadata_display_options_json"
                """,
                self._column_metadata_db_values(metadata),
            )

    def direct_set_case_sensitivity(
        self,
        table: str,
        column: str,
        case_sensitive: bool,
    ) -> None:
        """
        Validate a bool flag and upsert only the column case-sensitivity field.

        Check normalized-identity policy compatibility before writing. Require the metadata
        table; an existing row keeps its other fields, while a new row relies on column
        defaults. The transaction uses the primary connection.

        Example:
            Changing work_title case sensitivity preserves other fields in an existing
            column_metadata row.


        :param table: Table name resolved within the configured driver schema.
        :param column: Column name belonging to the selected table.
        :param case_sensitive: Actual bool value; other types are rejected.
        :return: None; commits the single-field upsert.
        """
        table_name, column_name = self._validated_column_metadata_target(table, column)
        if type(case_sensitive) is not bool:
            raise InputIntegrityError("case_sensitive must be a bool")
        self._validate_normalized_identity_metadata(
            replace(
                self.direct_get_column_metadata(table_name, column_name),
                case_sensitive=case_sensitive,
            )
        )
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
        """
        Expose effective case sensitivity through the shared column-policy interface.

        Example:
            This method returns the same flag as direct_get_case_sensitivity for
            the selected table and column.


        :param table: Table name resolved within the configured driver schema.
        :param column: Column name belonging to the selected table.
        :return: Effective case_sensitive flag from column metadata.
        """

        return self.direct_get_case_sensitivity(table, column)

    def direct_set_column_case_sensitive(
        self,
        table: str,
        column: str,
        case_sensitive: bool,
    ) -> None:
        """
        Apply the case-sensitivity setter through the shared column-policy interface.

        Example:
            Calling this with a non-bool value raises the same InputIntegrityError
            as direct_set_case_sensitivity.


        :param table: Table name resolved within the configured driver schema.
        :param column: Column name belonging to the selected table.
        :param case_sensitive: Actual bool value; other types are rejected.
        :return: None; delegates validation and the transactional field update.
        """

        self.direct_set_case_sensitivity(table, column, case_sensitive)

    def direct_get_relation_type(self, name: str) -> str | None:
        """
        Look up an exact relation name in the configured schema and close the connection.

        Unlike table helpers, this lookup does not canonicalize or strip the supplied name.

        Example:
            A regular information_schema VIEW yields ``"view"``; an absent name yields None.


        :param name: Exact unqualified relation name to bind in the catalog query.
        :return: view, table, or None when no catalog row is found.
        """
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
        """
        Read ordered headings for a named relation and reject an empty result.

        This method queries columns without separately verifying the relation is a view;
        use direct_get_relation_type when that distinction matters.

        Example:
            ``driver.direct_get_view_column_headings(view)`` returns its physical column order.


        :param view: Relation name canonicalized within the configured schema.
        :return: Ordered heading list; missing/columnless relations raise InputIntegrityError.
        """
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
        """
        Read at most one view row using its literal id column.

        Require the relation type to be view. No row returns None; multiple rows raise
        DatabaseIntegrityError. Returned values use the inherited casting rules.

        Example:
            A view exposing id and title can be queried with
            ``driver.direct_get_view_row_dict_from_id(view, 7)``.


        :param view: View name canonicalized within the configured schema.
        :param row_id: ID bound to the row lookup or mutation.
        :return: Converted row dictionary or None.
        """
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
        """
        List distinct trigger names in the configured schema on a short connection.

        Example:
            A trigger attached to multiple tables appears only once in this name list.


        :return: Sorted list of trigger names.
        """
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
        """
        Drop each named trigger from every matching schema table, using CASCADE.

        Ignore blank names; no names is a successful no-op. Execute drops in one primary
        transaction and refresh caches. Execution failures become DatabaseDriverError.

        Example:
            Passing one trigger name drops all its matching attachments in driver.schema.


        :param triggers: Trigger names; each is converted to text and stripped.
        :return: True after successful drops or an empty request.
        """
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
        """
        Find sorted table attachments for a trigger on a caller-owned connection.

        Example:
            A trigger attached to works and agents returns both table names once each.


        :param trigger_name: Exact trigger name bound in the catalog query.
        :param conn: Open connection adapter; this helper does not close it.
        :return: Sorted distinct event-object table names.
        """
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
        """
        Validate a table, count its rows and close the short connection.

        Example:
            ``driver.direct_get_record_count("works")`` returns zero for an empty table.


        :param target_table: Table containing the records to access.
        :return: Integer count; a missing/falsy scalar is treated as zero.
        """
        self._assert_existing_table(target_table)
        conn = self._short_connection()
        try:
            cur = conn.execute(f"select count(*) from {self._table_sql(target_table)}")
            row = cur.fetchone()
            return int(_row_value(row, 0, "count") or 0)
        finally:
            conn.close()

    def direct_get_all_rows(self, table: str, sort_column: str | None = None, reverse: bool = False) -> list[dict[str, Any]]:
        """
        Fetch and cast all rows with optional validated column ordering.

        Without sort_column, no ordering is requested and reverse has no effect.
        The entire result is materialized before the short connection closes.

        Example:
            ``driver.direct_get_all_rows("works", sort_column="work_id", reverse=True)``
            returns work rows in descending ID order.


        :param table: Table name resolved within the configured driver schema.
        :param sort_column: Optional existing column used for ORDER BY.
        :param reverse: Whether an explicit sort column is ordered descending.
        :return: List of converted row dictionaries.
        """
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
        """
        Fetch one main-table row by its conventional ID column.

        No match returns False, not None; multiple matches raise DatabaseDriverError.
        The short connection closes before conversion.

        Example:
            ``driver.direct_get_row_dict_from_id("works", missing_id)`` returns False.


        :param table: Table name resolved within the configured driver schema.
        :param row_id: ID bound to the row lookup or mutation.
        :return: Converted row dictionary or False.
        """
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
        """
        Perform a bound equality search on one validated column.

        Reject a None search term rather than translating it to IS NULL. Equality follows
        the database column semantics; this method does not apply column-policy folding.

        Example:
            ``driver.direct_search_table("works", "work_title", "Example")`` returns
            all rows equal to that title.


        :param table: Table name resolved within the configured driver schema.
        :param column: Column name belonging to the selected table.
        :param search_term: Non-None value bound to the equality predicate.
        :return: List of converted matching rows without an explicit ordering.
        """
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
        """
        AND validated predicates from one inferred table, with bound search values.

        Support comparison operators, LIKE/ILIKE, IN and null-aware IS/IS NOT. Empty IN
        produces a false predicate. Reject malformed terms, mixed-table columns, unsupported
        operators and strings containing the restricted SQL-like markers. Iterator mode
        still fetches all rows internally before yielding converted dictionaries.

        Example:
            ``driver.direct_multi_column_search([("work_id", "IN", [1, 2])])``
            returns matching work rows; an empty search_index returns None.


        :param search_index: Sequence of column/operator/value triples; extra term elements are ignored.
        :param iterator_return: Whether to return the deferred generator instead of a materialized list.
        :return: Matching row list, a generator, or None for an empty request.
        """
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
        """
        Select and cast one row using ORDER BY random() LIMIT 1.

        Example:
            An empty target table yields None rather than False.


        :param target_table: Table containing the records to access.
        :param direct: Accepted by the shared interface but currently unused.
        :return: Converted random row dictionary or None.
        """
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
        """
        Yield converted rows using explicit ordering or positive-ID keyset batches.

        With sort_column, fetch all rows through the shared generator. Without it, read
        batches of ten IDs greater than zero on separate short connections; zero/negative
        IDs are omitted and concurrent changes are not covered by one snapshot. reverse
        affects only the explicit-sort path.

        Example:
            ``driver.direct_get_row_dict_iterator("works")`` walks increasing positive
            work IDs, opening a short connection per batch.


        :param table: Table name resolved within the configured driver schema.
        :param sort_column: Optional validated ORDER BY column; otherwise use positive-ID paging.
        :param reverse: Whether an explicit sort column is descending; ignored for ID paging.
        :return: Generator yielding converted row dictionaries.
        """
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
        """
        Infer a column owner and materialize DISTINCT raw values as a set.

        No inherited value conversion is applied; None is retained when returned.
        The short connection closes after fetching.

        Example:
            Distinct source values including SQL NULL produce a set containing None.


        :param target_column: Column whose table is inferred by the shared naming contract.
        :return: Unordered set of raw distinct values.
        """
        target_table = self.direct_identify_table_from_column(target_column)
        self._assert_existing_column(target_table, target_column)
        conn = self._short_connection()
        try:
            cur = conn.execute(f"select distinct {_q(target_column)} from {self._table_sql(target_table)}")
            return {_row_value(row, 0, target_column) for row in cur.fetchall()}
        finally:
            conn.close()

    def direct_get_unique_values_iterator(self, target_column: str) -> Iterator[Any]:
        """
        Yield from the materialized distinct-value set in unspecified order.

        Example:
            Iterating this method allocates the full set before yielding its first value.


        :param target_column: Column whose values are selected through direct_get_unique_values_set.
        :return: Generator over distinct raw values.
        """
        for value in self.direct_get_unique_values_set(target_column):
            yield value

    def direct_get_max(self, column: str) -> int | None:
        """
        Return the column maximum coerced to int when possible.

        Example:
            An empty/all-null column yields None; a fractional numeric maximum is
            truncated by integer conversion.


        :param column: Column name belonging to the selected table.
        :return: Integer maximum or None when conversion fails.
        """
        return self._direct_get_column_extreme(column, function_name="max")

    def direct_get_min(self, column: str) -> int | None:
        """
        Return the column minimum coerced to int when possible.

        Example:
            A numeric string minimum can be converted to int; nonnumeric text yields None.


        :param column: Column name belonging to the selected table.
        :return: Integer minimum or None when conversion fails.
        """
        return self._direct_get_column_extreme(column, function_name="min")

    def direct_add_simple_row_dict(self, row_dict: dict[str, Any]) -> Any:
        """
        Insert one copied row mapping and return its ID through RETURNING.

        Infer the target, remove the table marker and derive supported normalized identities.
        An empty payload uses DEFAULT VALUES. Execute in a primary transaction, refresh
        caches and wrap execution failures as DatabaseDriverError.

        Example:
            Inserting ``{"table": "works", "work_title": "Example"}`` returns the new work ID.


        :param row_dict: Input column/value mapping with an optional table marker; not mutated.
        :return: ID scalar returned by PostgreSQL, or None if no value is available.
        """
        row_dict = dict(row_dict)
        target_table = self.direct_identify_table_from_row(row_dict)
        row_dict.pop("table", None)
        if normalized_identity_defaults_for_table(target_table):
            row_dict = add_derived_identity_values(
                target_table,
                row_dict,
                available_columns=set(
                    self.direct_get_column_headings(target_table)
                ),
            )

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
        """
        Insert mappings sequentially using the single-row transaction helper.

        This is not an atomic batch: earlier inserts may already be committed when a later
        row fails. Individual generated IDs are discarded.

        Example:
            If the third insert raises, the first two successful row inserts remain committed.


        :param row_dict_list: Row mappings passed in order to direct_add_simple_row_dict.
        :return: True after all rows are inserted, including an empty list.
        """
        for row_dict in row_dict_list:
            self.direct_add_simple_row_dict(row_dict)
        return True

    def direct_update_row_dict(self, row_dict: dict[str, Any]) -> bool:
        """
        Update a copied row by its required ID, deriving supported identity columns.

        Remove the table marker, convert literal string None values to SQL NULL, and require
        the ID column. An ID-only payload is a successful no-op. True indicates successful
        execution, not that a row matched; execution failures become DatabaseDriverError.

        Example:
            ``{"table": "works", "work_id": 7, "work_title": "Revised"}`` updates
            work 7 without modifying the caller mapping.


        :param row_dict: Row mapping containing its conventional ID and replacement column values.
        :return: True after successful execution or an empty update payload.
        """
        target_table = self.direct_identify_table_from_row(row_dict)
        row_dict = deepcopy(dict(row_dict))
        row_dict.pop("table", None)
        row_dict = {
            column: None if value == "None" else value
            for column, value in row_dict.items()
        }
        if normalized_identity_defaults_for_table(target_table):
            row_dict = add_derived_identity_values(
                target_table,
                row_dict,
                available_columns=set(
                    self.direct_get_column_headings(target_table)
                ),
            )

        table_id_col = self.direct_get_id_column(target_table)
        if table_id_col not in row_dict:
            raise InputIntegrityError(f"Cannot update {target_table!r}: missing id column {table_id_col!r}")
        target_row_id = row_dict.pop(table_id_col)
        if not row_dict:
            return True

        assignments = ", ".join(f"{_q(column)} = %s" for column in row_dict)
        values = tuple(row_dict.values()) + (target_row_id,)
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
        """
        Apply one-column ID/value updates, keeping an available derived identity in sync.

        An empty mapping returns True before field validation. Convert literal string None
        to SQL NULL and execute the parameter batch in one primary transaction. Database
        errors propagate directly.

        Example:
            Updating series names by ID also updates the physical series_name_norm column.


        :param id_values_map: Mapping from row IDs to replacement field values.
        :param field: Required column name when the update mapping is nonempty.
        :param table: Optional target table; otherwise infer it from field.
        :return: True after execution or for an empty mapping.
        """
        if not id_values_map:
            return True
        if field is None:
            raise InputIntegrityError("PostgreSQL direct_update_columns requires a field for one-column updates.")
        target_table = table or self.direct_identify_table_from_column(field)
        table_id_col = self.direct_get_id_column(target_table)
        identity_spec = default_normalized_identity_spec(target_table, field)
        if (
            identity_spec is not None
            and identity_spec.identity_column
            in set(self.direct_get_column_headings(target_table))
        ):
            rows = [
                (
                    None if value == "None" else value,
                    (
                        None
                        if value in (None, "None")
                        else normalize_identity_value(
                            value,
                            identity_spec.normalization_profile,
                        )
                    ),
                    row_id,
                )
                for row_id, value in id_values_map.items()
            ]
            stmt = (
                f"update {self._table_sql(target_table)} "
                f"set {_q(field)} = %s, {_q(identity_spec.identity_column)} = %s "
                f"where {_q(table_id_col)} = %s"
            )
        else:
            rows = [
                (None if value == "None" else value, row_id)
                for row_id, value in id_values_map.items()
            ]
            stmt = (
                f"update {self._table_sql(target_table)} "
                f"set {_q(field)} = %s where {_q(table_id_col)} = %s"
            )
        conn = self._primary_connection()
        with conn:
            conn.executemany(stmt, rows)
        self._zero_prop_cache()
        return True

    def direct_delete_many_by_ids(self, target_table: str, row_ids) -> bool:
        """
        Delete supplied IDs in one primary transaction after validating the table.

        Materialize parameter tuples first; an empty ID iterable returns True. Refresh
        caches on success and wrap execution failures as DatabaseDriverError.

        Example:
            Deleting IDs [1, 2] issues repeated bound ID predicates in one transaction.


        :param target_table: Table containing the records to access.
        :param row_ids: Iterable of IDs bound individually in DELETE statements.
        :return: True on successful execution; not a count or proof that IDs existed.
        """
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
        """
        Delete rows matching one value or repeated bound values in one transaction.

        Single-value None becomes IS NULL. In many mode, values use equality predicates, so
        None elements do not select SQL NULL and strings iterate character by character.
        Validate table/column first; wrap TypeError as InputIntegrityError and other
        execution failures as DatabaseDriverError.

        Example:
            ``driver.direct_delete("works", "work_title", None)`` removes rows whose
            work_title is SQL NULL.


        :param target_table: Table containing the records to access.
        :param column: Column name belonging to the selected table.
        :param value: Scalar equality value, or iterable when many is True.
        :param many: Whether to execute one equality DELETE per element of value.
        :return: True after successful execution, including an empty iterable in many mode.
        """
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
        """
        Delete repeated column values through direct_delete with many enabled.

        Example:
            Passing ["old", "obsolete"] deletes both equality matches in one transaction.


        :param target_table: Table containing the records to access.
        :param column: Column name belonging to the selected table.
        :param values: Iterable of equality values; None elements do not become IS NULL.
        :return: True after the delegated delete completes.
        """
        return self.direct_delete(target_table=target_table, column=column, value=values, many=True)

    def direct_delete_row_by_id(self, target_table: str, row_id: int) -> bool:
        """
        Delete a validated table row by its conventional ID in a primary transaction.

        Example:
            Deleting an ID that does not exist still returns True when SQL executes successfully.


        :param target_table: Table containing the records to access.
        :param row_id: ID bound to the row lookup or mutation.
        :return: True after execution and cache refresh; failures become DatabaseDriverError.
        """
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
        """
        Delete all table rows, then verify the remaining count within the transaction.

        Use DELETE rather than TRUNCATE; identity sequences are not reset here.
        Refresh caches on success and wrap execution failures as DatabaseDriverError.

        Example:
            Clearing works preserves the table definition and its identity sequence state.


        :param target_table: Table containing the records to access.
        :return: Whether the count observed after DELETE is zero.
        """
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
        """
        Read the maximum conventional ID and use zero for an empty/all-null result.

        Example:
            An empty works table gives ``driver.direct_get_highest_id("works") == 0``.


        :param target_table: Table containing the records to access.
        :return: Integer maximum ID, or zero for no usable scalar.
        """
        id_col = self.direct_get_id_column(target_table)
        conn = self._short_connection()
        try:
            cur = conn.execute(f"select coalesce(max({_q(id_col)}), 0) from {self._table_sql(target_table)}")
            row = cur.fetchone()
            return int(_row_value(row, 0, "max") or 0)
        finally:
            conn.close()

    def direct_get_db_unique_id(self):
        """
        Read the database identity while checking that metadata has at most one row.

        Example:
            No metadata row yields None; two rows raise DatabaseDriverError.


        :return: Stored unique-ID value or None.
        """
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
        """
        Update the first metadata row, or insert one, with a supplied/generated UUID text.

        A falsy force_value generates uuid4. This setter does not enforce the single-row
        metadata invariant checked by the getter.

        Example:
            ``driver.direct_set_db_unique_id()`` stores a fresh uuid4 string.


        :param force_value: Truthy value converted to text, or a falsy value to request uuid4.
        :return: None; writes in the primary transaction.
        """
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
        """
        Read one validated field from the first database_metadata row.

        Example:
            ``driver.direct_read_metadata("db_name")`` resolves database_metadata_db_name.


        :param md_field_name: Metadata column name, with or without the database_metadata_ prefix.
        :return: Raw field value or None when no metadata row exists.
        """
        field = self._metadata_field_name(md_field_name)
        conn = self._short_connection()
        try:
            cur = conn.execute(f"select {_q(field)} from {self._table_sql('database_metadata')} limit 1")
            row = cur.fetchone()
            return None if row is None else _row_value(row, 0, field)
        finally:
            conn.close()

    def direct_write_metadata(self, md_field_name: str, md_field_value: Any) -> None:
        """
        Set a validated metadata field on the first row, inserting when none exists.

        The primary transaction owns the write; extra metadata rows are not rejected here.

        Example:
            ``driver.direct_write_metadata("db_name", "Library")`` stores the library label.


        :param md_field_name: Metadata column name, with or without the database_metadata_ prefix.
        :param md_field_value: Value bound to the selected metadata field.
        :return: None; commits the update/insert through context exit.
        """
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
        """
        Prefix and validate a database_metadata column name against current headings.

        Example:
            The suffix db_name becomes database_metadata_db_name when that column exists.


        :param md_field_name: Metadata column name, with or without the database_metadata_ prefix.
        :return: Validated full metadata column name; unknown names raise InputIntegrityError.
        """
        field = str(md_field_name)
        if not field.startswith("database_metadata_"):
            field = "database_metadata_" + field
        allowed_values = self.direct_get_column_headings("database_metadata")
        if field not in allowed_values:
            raise InputIntegrityError(f"metadata field {field!r} not found")
        return field

    def _assert_existing_table(self, table: str) -> None:
        """
        Require an exact key in the table/column catalog without canonicalizing it.

        Example:
            The unqualified key works passes when present; callers normalize qualified names first.


        :param table: Table name resolved within the configured driver schema.
        :return: None on success; raises InputIntegrityError otherwise.
        """
        if table not in self.direct_get_tables_and_columns():
            raise InputIntegrityError(f"table {table!r} not found")

    def _assert_existing_column(self, table: str, column: str) -> None:
        """
        Require a column among the selected table headings.

        Example:
            An unknown work column raises InputIntegrityError before a mutation is attempted.


        :param table: Table name resolved within the configured driver schema.
        :param column: Column name belonging to the selected table.
        :return: None on success; raises InputIntegrityError otherwise.
        """
        if column not in self.direct_get_column_headings(table):
            raise InputIntegrityError(f"column {column!r} not found in table {table!r}")

    def _row_to_dict_from_db_row(self, *, table: str, headings: Sequence[str], row: Any) -> dict[str, Any]:
        """
        Align raw row values to headings and apply inherited declared-type conversion.

        Mappings supply missing headings as None; positional rows must contain enough cells.

        Example:
            A mapping row is reordered to headings before shared value casting runs.


        :param table: Table name resolved within the configured driver schema.
        :param headings: Ordered column headings used to map and cast raw values.
        :param row: Mapping or positional sequence returned by the PostgreSQL cursor.
        :return: New heading-to-converted-value dictionary.
        """
        if isinstance(row, Mapping):
            values = tuple(row.get(heading) for heading in headings)
        else:
            values = tuple(row)
        return self._row_to_dict(table=table, headings=headings, row=values)

    def _execute_schema_statements(self, statements: Sequence[str]) -> None:
        """
        Run ordered DDL in one primary transaction and refresh shared caches.

        Execution failures are logged and wrapped as DatabaseDriverError.

        Example:
            Table creation and its index statements are executed in their supplied order.


        :param statements: SQL statements executed sequentially without extra parsing here.
        :return: None; executes the schema transaction.
        """
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
        """
        Fetch all rows on first iteration and yield converted dictionaries.

        Keep the short connection open until exhaustion, generator close or failure.
        Despite its generator interface, this buffers the entire query result in fetchall.

        Example:
            Close the generator explicitly when stopping early to release its connection promptly.


        :param stmt: SQL query to execute through the connection adapter.
        :param headings: Ordered column headings used to map and cast raw values.
        :param table: Table name resolved within the configured driver schema.
        :param bindings: Optional parameter sequence bound to the query.
        :return: Generator of converted row dictionaries.
        """
        conn = self._short_connection()
        try:
            cur = conn.execute(stmt, bindings)
            for row in cur.fetchall():
                yield self._row_to_dict_from_db_row(table=table, headings=headings, row=row)
        finally:
            conn.close()

    def _direct_get_column_extreme(self, column: str, *, function_name: str) -> int | None:
        """
        Query min/max on an inferred column owner and coerce the scalar to int.

        Validate the aggregate name and physical column first. Close the short connection
        before conversion; TypeError/ValueError from int become None.

        Example:
            A value of 3.8 becomes 3; an absent, null or nonnumeric result becomes None.


        :param column: Column name belonging to the selected table.
        :param function_name: Exactly max or min; other names raise InputIntegrityError.
        :return: Integer aggregate value or None.
        """
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
        """
        Strip a qualifier and one recognized surrounding delimiter pair from table text.

        Use the last dot-separated segment, then remove brackets or matching backtick,
        double-quote, backslash, percent or underscore delimiters. This is a legacy name
        normalizer rather than a SQL identifier parser; embedded dots are not preserved.

        Example:
            >>> DatabaseDriver._canonicalise_table_name_for_cache("public.works")
            'works'


        :param table: Table name resolved within the configured driver schema.
        :return: Unqualified normalized table name.
        """
        text = str(table).strip()
        if "." in text:
            text = text.split(".")[-1].strip()
        if text.startswith("[") and text.endswith("]") and len(text) >= 2:
            return text[1:-1]
        if len(text) >= 2 and text[0] == text[-1] and text[0] in {"`", '"', "\\", "%", "_"}:
            return text[1:-1]
        return text

    def _primary_connection(self) -> PostgresConnectionAdapter:
        """
        Return the retained connection, reopening it when absent or its probe fails.

        Probe an existing connection with SELECT 1. Any probe exception triggers best-effort
        close and replacement; the original failure is not propagated. The probe itself
        may begin a transaction, and this method does not commit it.

        Example:
            After a dead connection fails the probe, driver.conn is replaced by a new adapter.


        :return: Tracked reusable connection stored in self.conn.
        """
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
        """
        Open a fresh tracked adapter for a caller-owned short operation.

        Example:
            A read helper closes this connection in its finally block after fetching.


        :return: New connection adapter; the caller must close it.
        """
        return self.get_connection()


def _q(name: str) -> str:
    """
    Double embedded double quotes and delimit one SQL identifier.

    Example:
        >>> _q("works")
        '"works"'


    :param name: Identifier converted to text before escaping.
    :return: Quoted identifier.
    """
    return '"' + str(name).replace('"', '""') + '"'


def _qualified_table(schema: str, table_name: str) -> str:
    """
    Quote schema and table independently and join them with a dot.

    Example:
        >>> _qualified_table("library", "works")
        '"library"."works"'


    :param schema: Schema identifier to quote.
    :param table_name: Table name normalized to its unqualified spelling.
    :return: Quoted schema-qualified relation reference.
    """
    return f"{_q(schema)}.{_q(table_name)}"


def _assert_safe_identifier(value: str, *, kind: str = "identifier") -> str:
    """
    Validate a nonblank alphanumeric/underscore identifier without a leading digit.

    Python isalnum/isdigit predicates allow Unicode letters and digits; this is broader
    than the ASCII allowlist used by runtime grant builders.

    Example:
        >>> _assert_safe_identifier(" work_label ")
        'work_label'


    :param value: Identifier candidate converted to text and stripped.
    :param kind: Description included in validation errors.
    :return: Trimmed validated identifier; failures raise InputIntegrityError.
    """
    text = str(value).strip()
    if not text:
        raise InputIntegrityError(f"PostgreSQL {kind} cannot be blank.")
    if not all(char.isalnum() or char == "_" for char in text):
        raise InputIntegrityError(f"PostgreSQL {kind} contains unsupported characters: {value!r}")
    if text[0].isdigit():
        raise InputIntegrityError(f"PostgreSQL {kind} cannot start with a digit: {value!r}")
    return text


def _postgres_column_type(datatype: str) -> str:
    """
    Map supported portable type spellings to PostgreSQL DDL type names.

    An empty/falsy input defaults to text; unknown nonempty spellings raise
    InputIntegrityError. Integer/int become bigint, while smallint stays smallint.

    Example:
        >>> _postgres_column_type("blob")
        'bytea'


    :param datatype: Portable type name, matched after trimming and lowercasing.
    :return: Normalized supported SQL type.
    """
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
    """
    Quote a stringified value as a SQL text literal, doubling apostrophes.

    None is rendered as quoted text rather than SQL NULL.

    Example:
        >>> _pg_literal("author")
        "'author'"


    :param value: Value converted to text for trusted generated DDL seeds.
    :return: Escaped SQL string literal.
    """
    return "'" + str(value).replace("'", "''") + "'"


def _normalise_requested_link_columns(requested_cols: str | Sequence[str] | None) -> str | set[str]:
    """
    Normalize all/None or a suffix sequence for optional link-column selection.

    Only the string all is accepted; other scalar strings raise InputIntegrityError.
    Sequence entries are trimmed, lowercased and deduplicated without identifier validation.

    Example:
        >>> _normalise_requested_link_columns([" TYPE ", "type"])
        {'type'}


    :param requested_cols: all, None, or a sequence of optional link-column suffixes.
    :return: all or a set of normalized nonblank suffixes.
    """
    if requested_cols is None:
        return set()
    if isinstance(requested_cols, str):
        if requested_cols.strip().lower() == "all":
            return "all"
        raise InputIntegrityError("requested_cols must be 'all', None, or an iterable of column suffixes.")
    return {str(value).strip().lower() for value in requested_cols if str(value).strip()}


def _link_extra_columns(link_base: str, requested: str | set[str]) -> list[tuple[str, str]]:
    """
    Build optional link data columns plus mandatory source, datestamp and scratch.

    All selects the predefined columns. A subset adds recognized columns in fixed order
    and unknown valid suffixes as nullable text in sorted order. The nullable marker is
    ignored. Source is added even for an empty subset; datestamp/scratch always follow.

    Example:
        >>> [suffix for suffix, _ in _link_extra_columns("agent_work_link", set())]
        ['source', 'datestamp', 'scratch']


    :param link_base: Validated singular link prefix used to form column names.
    :param requested: all or a normalized suffix set from _normalise_requested_link_columns.
    :return: Ordered suffix/DDL-fragment pairs.
    """
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
    """
    Build cardinality and optional priority/type uniqueness constraints.

    Use ordinary PostgreSQL UNIQUE constraints: nullable members retain PostgreSQL
    NULL semantics. Type-specific variants depend on actual type/priority columns.
    Unknown link types raise NotImplementedError.

    Example:
        A one_one link makes each foreign-key column independently unique;
        a rating link with type makes left-ID/type pairs unique.


    :param link_type: Relationship cardinality: many_many, many_many_non_exclusive, one_many, many_one, one_one, one_one_normalized or rating.
    :param link_base: Singular link prefix used to name constraints and optional columns.
    :param left_base: Singular left-table prefix used in one-to-one constraint names.
    :param right_base: Singular right-table prefix used in one-to-one constraint names.
    :param left_fk_col: Left-side foreign-key column name.
    :param right_fk_col: Right-side foreign-key column name.
    :param has_type: Whether the link contains a type column.
    :param has_priority: Whether the link contains a priority column.
    :return: Ordered table-constraint SQL fragments.
    """
    constraints: list[str] = []
    type_col = f"{link_base}_type"
    priority_col = f"{link_base}_priority"

    def constraint_name(suffix: str) -> str:
        """
        Validate and quote one constraint name using the enclosing link prefix.

        Example:
            With link_base agent_work_link, suffix unique_pair names
            ``"agent_work_link_unique_pair"``.


        :param suffix: Constraint-specific suffix appended to the enclosing link_base.
        :return: Quoted generated constraint identifier.
        """
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
    """
    Sort singular table bases and derive the conventional link table/prefix pair.

    Example:
        >>> _link_table_name_col_name("works", "agents")
        ('agent_work_links', 'agent_work_link')


    :param primary_table: First main table in the requested relationship.
    :param secondary_table: Second main table in the requested relationship.
    :return: Plural link-table name and singular link-column prefix.
    """
    bases = [plural_singular_mapper(str(primary_table)), plural_singular_mapper(str(secondary_table))]
    bases.sort()
    column_name = _assert_safe_identifier(f"{bases[0]}_{bases[1]}_link", kind="link column base")
    return f"{column_name}s", column_name


def _row_value(row: Any, position: int, key: str) -> Any:
    """
    Read a mapping key or positional cell, tolerating missing rows/index failures.

    Example:
        >>> _row_value((7,), 0, "work_id")
        7


    :param row: Mapping or positional database row, optionally None.
    :param position: Index used only for nonmapping rows.
    :param key: Column label used only for mapping rows.
    :return: Selected raw value, or None when unavailable.
    """
    if row is None:
        return None
    if isinstance(row, Mapping):
        return row.get(key)
    try:
        return row[position]
    except Exception:
        return None


def _reject_unsafe_search_value(value: Any) -> None:
    """
    Reject string-like values containing semicolons, SQL comment markers or NUL.

    Decode byte values with replacement; non-string values and decode failures pass
    through. This policy may reject ordinary text and is separate from parameter binding.

    Example:
        A title containing a semicolon is rejected even though query values are bound.


    :param value: Search value examined by the multi-column search builder.
    :return: None for accepted values; otherwise raises InputIntegrityError.
    """
    if not isinstance(value, (str, bytes, bytearray)):
        return
    try:
        text = value.decode("utf-8", errors="replace") if isinstance(value, (bytes, bytearray)) else str(value)
    except Exception:
        return
    if ";" in text or "--" in text or "/*" in text or "*/" in text or "\x00" in text:
        raise InputIntegrityError("Unsafe-looking search value rejected")
