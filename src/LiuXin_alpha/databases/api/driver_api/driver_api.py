"""
Specify the composite contract for low-level database drivers.

DatabaseDriverAPI combines lifecycle, raw SQL, schema, policy, relationship and row
operations. Abstract bodies provide no implementation and return None if called
directly. Concrete backend results, resource ownership and transaction boundaries
must be observed; the method notes describe the existing shared SQL/SQLite
implementations where relevant.
"""

from __future__ import annotations

import abc
from collections.abc import Callable, Iterable, Iterator, Mapping
from os import PathLike
import pathlib
from queue import Queue
from typing import TYPE_CHECKING, Any, Literal, Optional, Sequence, Union, TypeVar

from LiuXin_alpha.databases.api.driver_api.mixins.add_mixin_api import DriverAddMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.custom_columns_mixin_api import (
    DriverCustomColumnsMixinAPI,
)
from LiuXin_alpha.databases.api.driver_api.mixins.delete_mixin_api import (
    DriverDeleteMixinAPI,
)
from LiuXin_alpha.databases.api.driver_api.mixins.interlink_mixin_api import (
    DriverInterlinkMixinAPI,
)
from LiuXin_alpha.databases.api.driver_api.mixins.intralink_mixin_api import (
    DriverIntralinkMixinAPI,
)
from LiuXin_alpha.databases.api.driver_api.mixins.metadata_mixin_api import (
    DriverMetadataMixinAPI,
)
from LiuXin_alpha.databases.api.driver_api.mixins.names_mixin_api import (
    DriverNamesMixinAPI,
)
from LiuXin_alpha.databases.api.driver_api.mixins.new_books_compressed_files_mixin_api import (
    DriverNewBooksCompressedFilesMixinAPI,
)
from LiuXin_alpha.databases.api.driver_api.mixins.properties_mixin_api import (
    DriverDatabasePropertiesMixinAPI,
)
from LiuXin_alpha.databases.api.driver_api.mixins.search_mixin_api import (
    DriverSearchMixinAPI,
)
from LiuXin_alpha.databases.api.driver_api.mixins.tables_mixin_api import (
    DriverTablesMixinAPI,
)
from LiuXin_alpha.databases.api.driver_api.mixins.tree_mixin_api import (
    DriverTreeMixinAPI,
)
from LiuXin_alpha.databases.api.driver_api.mixins.triggers_mixin_api import (
    DriverTriggersMixinAPI,
)
from LiuXin_alpha.databases.api.driver_api.mixins.update_mixin_api import (
    DriverUpdateMixinAPI,
)
from LiuXin_alpha.databases.api.driver_api.mixins.view_mixin_api import (
    DriverViewMixinAPI,
)
from LiuXin_alpha.databases.api.portable_macros_api import DatabaseMacrosAPI
from LiuXin_alpha.databases.db_types.backend_capabilities import BackendCapabilities
from LiuXin_alpha.databases.db_types.driver_types import (
    QueryConnection,
    SQLConnection,
    RowSQLConnection,
    QueryCursor,
    SQLParameters,
)
from LiuXin_alpha.databases.schema_specs import PhysicalTableSpec

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.api.maintenance_api import MaintenanceCallbackSinkAPI


ConnectionT = TypeVar("ConnectionT", bound=SQLConnection)


class DatabaseDriverAPI(
    DriverTreeMixinAPI,
    DriverMetadataMixinAPI,
    DriverInterlinkMixinAPI,
    DriverIntralinkMixinAPI,
    DriverCustomColumnsMixinAPI,
    DriverDatabasePropertiesMixinAPI,
    DriverNamesMixinAPI,
    DriverSearchMixinAPI,
    DriverAddMixinAPI,
    DriverDeleteMixinAPI,
    DriverTablesMixinAPI,
    DriverTriggersMixinAPI,
    DriverNewBooksCompressedFilesMixinAPI,
    DriverViewMixinAPI,
    DriverUpdateMixinAPI,
):
    """
    Specify the composite contract for low-level database drivers.

    DatabaseDriverAPI combines lifecycle, raw SQL, schema, policy, relationship and row
    operations. Abstract bodies provide no implementation and return None if called
    directly. Concrete backend results, resource ownership and transaction boundaries
    must be observed; the method notes describe the existing shared SQL/SQLite
    implementations where relevant. Abstract members must be implemented
    by a backend; their empty bodies return None when called directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DatabaseDriverAPI)
        True
    """

    conn: SQLConnection | None
    db: DatabaseAPI | None
    dirty_records_queue: Queue[tuple[str, int, str]] | None
    maintainer_callback: MaintenanceCallbackSinkAPI

    @abc.abstractmethod
    def __init__(
        self,
        db_metadata: dict[str, Any],
        db: DatabaseAPI | None = None,
        set_conn: bool = True,
        dirty_records_queue: Queue[tuple[str, int, str]] | None = None,
    ) -> None:
        """
        Initialize driver state and optionally open the primary connection.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Retain the metadata and facade by reference, install the FRBR creation hook and
        macro helper, initialize schema caches and connection tracking, and attach a
        no-op maintenance callback. A truthy set_conn opens a configured connection
        immediately; otherwise conn is None. Store the dirty-record queue after
        connection setup. No schema builder or maintenance worker is started here.
        Missing database_path raises KeyError, and connection setup errors propagate.

        Example:
            >>> driver = concrete_driver_type(
            ...     {"database_path": database_path}, set_conn=False
            ... )  # doctest: +SKIP


        :param db_metadata: Mapping containing database_path; the path value is copied
            to database_path while the mapping itself is retained.
        :param db: Optional database facade retained for macro and shared-mixin
            operations.
        :param set_conn: Open a primary connection immediately when truthy; False defers
            all connection setup.
        :param dirty_records_queue: Queue or other object retained for later
            dirty-record handling; not consumed here.
        :return: None; initializes this driver and possibly its primary connection.
        """

    @abc.abstractmethod
    def _read_row_for_update(
        self,
        connection: RowSQLConnection,
        table: str,
        id_column: str,
        row_id: Any,
        *,
        lock_row: bool = False,
    ) -> dict[str, Any] | None:
        """Read stored values on the caller's connection without completing it.

        Use the physical table's headings and backend qualification; request a
        row lock only where supported. Missing records return None. Database
        cell values, including the bound identity, remain backend-defined.
        """

    @staticmethod
    @abc.abstractmethod
    def _canonicalise_table_name_for_cache(table: str) -> str:
        """
        Coerce and trim a name, discard its schema prefix and remove one wrapper pair.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Recognize brackets and symmetric quote, backslash, percent or underscore
        wrappers. This is a cache-key convenience, not an SQL parser or identifier
        validator; failed text coercion returns the input.

        Example:
            >>> driver._canonicalise_table_name_for_cache("works")  # doctest: +SKIP


        :param table: Table or view name used for schema introspection.
        :return: The canonicalized name, or the original input if coercion fails.
        """

    @abc.abstractmethod
    def _close_all_open_connections(self) -> None:
        """Close owned handles without committing, retaining failed closes for retry.

        Built-in handles deregister on close. Detach the primary reference while
        retaining any handle whose close raised in the driver's ownership registry.
        """

    @abc.abstractmethod
    def _coerce_db_value(
        self, value: Any, declared_type: Any
    ) -> Optional[Union[bool, int, float, str, bytes]]:
        """
        Convert a database cell according to the declared type's conversion bucket.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Preserve None. Numeric buckets convert booleans to numbers and retain numeric
        objects; INTEGER turns integral floats into ints, while REAL converts numeric
        objects to floats. INTEGER and NUMERIC try integer text before decimal or
        exponent text; REAL parses matching text as a float. Whitespace is stripped for
        parsing only. Malformed numeric text retains its original spelling, and failed
        numeric-text conversions are suppressed. No finite-value check is applied, so
        overflow text can produce infinity.

        BLOB converts bytes, bytearray and memoryview to bytes. Other fallback paths use
        force_unicode, currently an alias for str: byte-like inputs outside BLOB are
        represented as text rather than decoded, so b"12" does not become the number 12.
        TEXT stringifies non-null values. Errors from stringification or direct
        numeric-object conversion can propagate.

        Example:
            >>> driver._coerce_db_value("Example", "TEXT")  # doctest: +SKIP


        :param value: Database cell to convert; no container or text decoding protocol
            is imposed on fallback objects.
        :param declared_type: Backend declaration used by _sqlite_affinity; an empty or
            unknown declaration still selects NUMERIC conversion.
        :return: None, an int or float, bytes for byte-like BLOB input, or text.
        """

    @staticmethod
    @abc.abstractmethod
    def _coerce_untyped_value(
        value: Any,
    ) -> Optional[Union[bool, int, float, str, bytes]]:
        """
        Preserve scalar numeric kinds and stringify values without a declared type.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Return None unchanged and coerce bool, int and float instances to their
        corresponding built-in types, testing bool first. Convert memoryview to bytes.
        For bytes and bytearray, first make bytes and then call force_unicode, currently
        str, so the result includes the bytes representation rather than decoded
        content. Other values also use str; its errors propagate.

        Numeric strings retain their text and whitespace because no affinity is
        available to justify parsing them.

        Example:
            >>> driver._coerce_untyped_value("Example")  # doctest: +SKIP


        :param value: Cell whose type is not described by a table declaration.
        :return: None, a built-in bool/int/float, bytes for memoryview, or text.
        """

    @staticmethod
    @abc.abstractmethod
    def _normalize_declared_type(declared_type: Any) -> str:
        """
        Uppercase the first type token and remove a parenthesized size suffix.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Convert non-null input with str, trim surrounding whitespace and discard tokens
        after the first. This is a lexical convenience rather than a SQL declaration
        parser.

        Example:
            >>> driver._normalize_declared_type("TEXT")  # doctest: +SKIP


        :param declared_type: Backend type declaration or another stringifiable value;
            None means no type.
        :return: Uppercase first token without its size suffix, or empty text.
        """

    @abc.abstractmethod
    def _register_open_connection(self, conn: ConnectionT) -> ConnectionT:
        """Track a live connection and return it unchanged.

        Built-in connection implementations deregister immediately on close.
        Foreign raw connections without callback attributes remain owned until
        driver shutdown. Maintenance binding failures propagate.
        """

    @abc.abstractmethod
    def _row_to_dict(
        self,
        *,
        table: Optional[str] = None,
        headings: Sequence[Any],
        row: Sequence[Any],
    ) -> dict[Any, Any]:
        """
        Pair headings with converted row cells, retaining set-valued cells by identity.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        A truthy table selects its cached declared-type map and typed conversion.
        Missing entries use an empty declaration, which selects NUMERIC; a falsey table
        instead uses untyped conversion. Fetch the map before iterating, even for empty
        headings. Set-valued cells bypass conversion without copying.

        Iterate headings and index the row: surplus cells are ignored and too few cells
        raise IndexError. Duplicate headings overwrite earlier values. Headings must be
        hashable; the legacy set-heading branch still attempts dictionary insertion, so
        a plain set heading raises TypeError. Schema lookup and cell conversion errors
        propagate.

        Example:
            >>> driver._row_to_dict(
            ...     headings=["work_id", "work_title"], row=(1, "Example")
            ... )  # doctest: +SKIP


        :param table: Trusted table spelling for declared-type lookup, or a falsey value
            such as None to avoid that lookup and use untyped conversion.
        :param headings: Ordered dictionary keys; may contain non-string hashable values
            and duplicates, but plain set keys are unsupported.
        :param row: Indexable cell sequence with at least one value per heading.
        :return: A new heading-to-value dictionary; retained sets are shared with row.
        """

    @staticmethod
    @abc.abstractmethod
    def _sanitize_embedded_nul_text(
        *, target_table: str, row_dict: dict[str, Any]
    ) -> None:
        """
        Replace embedded NUL characters in string values with visible ``<NUL>`` text.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Mutate the mapping in place; non-string values are untouched. The table argument
        is currently unused.

        Example:
            >>> driver._sanitize_embedded_nul_text(
            ...     target_table="works", row_dict={"work_id": 1}
            ... )  # doctest: +SKIP


        :param target_table: Table context accepted by the helper but not consulted.
        :param row_dict: Mutable column/value mapping whose string payloads are
            sanitized.
        :return: ``None``.
        """

    # Todo: Probably not an API level function
    @classmethod
    @abc.abstractmethod
    def _sqlite_affinity(cls, declared_type: str) -> str:
        """
        Select this mixin's conversion bucket from the normalized first type token.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Check substrings in order: INT, then CHAR/CLOB/TEXT, then BLOB, then
        REAL/FLOA/DOUB. Empty and unrecognized tokens fall through to NUMERIC. The
        preceding normalization discards size suffixes and later tokens; this is a
        simplified classification, not a complete SQL type parser.

        Example:
            >>> driver._sqlite_affinity("TEXT")  # doctest: +SKIP


        :param declared_type: Type declaration passed to _normalize_declared_type; None
            or blank text produces the NUMERIC fallback.
        :return: One of INTEGER, TEXT, BLOB, REAL or NUMERIC.
        """

    # Todo: See below?
    @abc.abstractmethod
    def _zero_prop_cache(self) -> None:
        """
        Invalidate schema caches when the schema version changed or cannot be checked.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Locations are always invalidated. Wrapper-derived cache clearing and metadata
        refresh are best effort; their exceptions are suppressed.

        Example:
            >>> driver._zero_prop_cache()  # doctest: +SKIP


        :return: ``None``.
        """

    @abc.abstractmethod
    def call_after_table_changes(self) -> None:
        """
        Refresh metadata and force the combined table/column cache to be rebuilt.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.call_after_table_changes()  # doctest: +SKIP


        :return: ``None``.
        """

    @abc.abstractmethod
    def close(self) -> None:
        """
        Close all known connections while retaining the driver object for reuse.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.close()  # doctest: +SKIP


        :return: ``None``.
        """

    @abc.abstractmethod
    def direct_backup(
        self, path: str | PathLike[str] | None = None
    ) -> str | PathLike[str]:
        """Publish a verified, create-only SQLite backup of committed state.

        Include WAL data using a separate reader; preserve primary pending writes.
        Reject existing destinations and source/sidecar paths. PostgreSQL backup
        remains an explicit external operation rather than a file copy.
        """

    @abc.abstractmethod
    def direct_create_new_database(self) -> None:
        """Create without replacing an existing file or populated server schema.

        File drivers must be constructed without opening the target. Preparation
        owns and releases its connections, and publication must detect collisions.
        """

    def direct_replace_database(
        self, *, backup_path: str | PathLike[str]
    ) -> pathlib.Path:
        """
        Replace offline storage with a required backup, if the backend supports it.
        """
        raise NotImplementedError("This driver does not support database replacement.")

    @abc.abstractmethod
    def direct_get_backend_capabilities(
        self, *, connection: QueryConnection | None = None
    ) -> BackendCapabilities:
        """Report the adapter's facilities and live limits without committing."""

    @abc.abstractmethod
    def direct_get_physical_table_spec(
        self, table: str, *, connection: QueryConnection | None = None
    ) -> PhysicalTableSpec:
        """Inspect native schema on the supplied handle; close only owned resources."""

    @abc.abstractmethod
    def _get_unique_column_groups(self, table: str) -> tuple[tuple[str, ...], ...]:
        """Return non-partial unique index columns for relationship introspection."""

    @abc.abstractmethod
    def direct_execute(
        self, sql: str, values: SQLParameters | int | None = None
    ) -> QueryCursor:
        """
        Execute one statement using a live primary connection and its transaction
        context.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Open a missing primary handle; propagate a failed liveness probe without
        replacing an existing handle or completing its transaction. Convert an integer
        binding to a one-element text tuple. Statement execution still uses the
        connection context and its commit/rollback semantics. Execution failures
        become DatabaseDriverError; finally attempt a cache refresh.

        Example:
            >>> driver.direct_execute("SELECT 1")  # doctest: +SKIP


        :param sql: SQL text to execute, with placeholders when bindings are supplied.
        :param values: Optional bindings; a bare integer is converted to a one-element
            string tuple.
        :return: The backend result cursor; the caller owns its cursor lifetime.
        """

    @abc.abstractmethod
    def direct_executemany(
        self, sql: str, values: Iterable[SQLParameters] | None = None
    ) -> None:
        """
        Execute a batch on the primary connection, or delegate an unbound script.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Normalize scalar entries in an outer tuple before SQL execution. Consume
        other binding iterables once, without retrying failures. The connection
        context rolls back on input, adaptation or SQL errors; DatabaseDriverError
        retains the original cause. This context also completes earlier primary
        work. Refresh caches after success. With values=None, delegate to
        direct_executescript.

        Example:
            >>> driver.direct_executemany("SELECT 1")  # doctest: +SKIP


        :param sql: SQL text to execute, with placeholders when bindings are supplied.
        :param values: Binding rows, an outer tuple of scalar values, or ``None`` for
            script execution.
        :return: ``None``.
        """

    @abc.abstractmethod
    def direct_get_db_unique_id(self) -> Optional[str]:
        """
        Read the database identity from its sole metadata row.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Multiple rows raise DatabaseIntegrityError. Reuse the primary connection when
        available, otherwise close the temporary handle in finally.

        Example:
            >>> driver.direct_get_db_unique_id()  # doctest: +SKIP


        :return: The stored identity value, or ``None`` when there are no rows or the
            value is null.
        """

    @abc.abstractmethod
    def direct_get_null_row(self, table: str) -> dict[str, Any] | Literal[False]:
        """
        Check for and fetch the ID-zero row using separate queries.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        A concurrent deletion between the queries can still produce False.

        Example:
            >>> driver.direct_get_null_row("works")  # doctest: +SKIP


        :param table: Table name resolved by the host driver.
        :return: The sentinel row dictionary, or ``False`` if absent.
        """

    @abc.abstractmethod
    def direct_has_null_row(self, table: str) -> bool:
        """
        Check for an ID-zero sentinel in a validated table and always close the query
        connection.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.direct_has_null_row("works")  # doctest: +SKIP


        :param table: Table name resolved by the host driver.
        :return: Whether a row with ID zero exists.
        """

    @abc.abstractmethod
    def direct_self_delete(self) -> None:
        """
        Close known handles and remove the configured database file.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Access checks and removal failures raise DatabaseDriverError. Windows permission
        failures receive bounded retries; successful deletion invalidates caches.

        Example:
            >>> driver.direct_self_delete()  # doctest: +SKIP


        :return: ``None``.
        """

    @abc.abstractmethod
    def direct_update_null_row(
        self,
        table: str,
        updates: Mapping[str, Any] | Iterable[tuple[str, Any]] | None = None,
        **fields: Any,
    ) -> bool:
        """
        Merge updates and delegate a sentinel-row update to the row writer.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Missing sentinels raise InputIntegrityError. Keyword fields mutate an input dict
        in place and win over its values. Supplied ID-column updates also override the
        initial zero, so callers must omit that column to target the sentinel.

        Example:
            >>> driver.direct_update_null_row(
            ...     "works", {"work_title": "Unknown"}
            ... )  # doctest: +SKIP


        :param table: Table name resolved by the host driver.
        :param updates: Optional dict or dict-convertible pairs of column/value updates.
        :param fields: Column/value keyword overrides merged into the updates mapping.
        :return: ``True`` after the update helper returns.
        """

    @abc.abstractmethod
    def direct_dirty_record(self, table: str, table_id: int, reason: str) -> None:
        """
        Enqueue a dirty-record notification, warning if the table is unrecognized.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        An unknown table is still queued. This method performs no database write.

        Example:
            >>> driver.direct_dirty_record(
            ...     "works", 1, "title changed"
            ... )  # doctest: +SKIP


        :param table: Table name checked against the host's current table collection.
        :param table_id: ID of the affected record.
        :param reason: Reason string retained in the queued tuple.
        :return: ``None``.
        """

    @abc.abstractmethod
    def dump_and_restore(
        self,
        callback: Callable[[str], object] | None = lambda x: x,
        sql: str | bytes | None = None,
    ) -> None:
        """Rebuild a file-backed SQLite database from its SQL dump.

        Require idle tracked connections and caller-coordinated exclusive access.
        Load into a private candidate, then validate integrity and foreign keys
        before replacing the original. Prefix SQL and progress callbacks remain
        supported. Failures before publication preserve the source; a completed
        replacement has no automatic rollback. PostgreSQL does not implement this.
        """

    @abc.abstractmethod
    def direct_execute_sql(
        self, sql: str, parameters: SQLParameters | None = None
    ) -> Optional[int]:
        """
        Execute one statement on a fresh connection and commit.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Return lastrowid and close the owned cursor/connection on every path;
        backend execution errors propagate.

        Example:
            >>> driver.direct_execute_sql("SELECT 1")  # doctest: +SKIP


        :param sql: SQL text to execute, with placeholders when bindings are supplied.
        :param parameters: Optional sequence of values bound to SQL placeholders.
        :return: The backend cursor lastrowid, whose meaning depends on the statement.
        """

    @abc.abstractmethod
    def direct_executescript(self, sqlscript: str) -> None:
        """
        Run trusted multi-statement SQL on the primary connection and refresh caches.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Create a primary handle if absent, wrap execution failures as
        DatabaseDriverError and attempt refresh in finally. Backend executescript
        transaction behavior still applies.

        Example:
            >>> driver.direct_executescript(
            ...     "CREATE TABLE sample (value TEXT);"
            ... )  # doctest: +SKIP


        :param sqlscript: SQL script text passed to the backend executescript method.
        :return: ``None``.
        """

    @abc.abstractmethod
    def exists(self) -> bool:
        """
        Check whether the configured database path exists, without opening it.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.exists()  # doctest: +SKIP


        :return: Whether ``os.path.exists(database_path)`` succeeds.
        """

    # Todo: Need a protocol for this
    # Todo: direct_*
    @abc.abstractmethod
    def get_connection(self) -> SQLConnection:
        """
        Open, configure and track a fresh SQLite connection.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Register set/list/dict adapters and PYSET/PYLIST/PYDICT/DATE converters
        globally, enable callback tracebacks, then open database_path with declared-type
        parsing and check_same_thread=False. Every call opens a distinct connection,
        including an independent database for the literal :memory: path. Existing
        primary handles are not replaced.

        Request foreign-key enforcement and warn if SQLite does not report it enabled.
        Install aggregates, collations, regex/tree/sort/UUID helpers, an always-true
        books_list_filter and maintenance callbacks. SQLite drivers can opt into
        progress callbacks through set_progress_callbacks after connection setup;
        no progress handler is installed by default. DIRTY_RECORD and NEW_DIRTY_RECORD
        look up the current maintainer on each invocation; DIRTY_INTERLINK_RECORD
        binds the maintainer method at connection creation. Cross-thread access still
        requires caller coordination.

        Best-effort LANGUAGE_ID registration may seed and lock an existing FRBR
        languages table, committing changes; exceptions from that registration are
        suppressed. Other setup failures close the new handle before propagating. The
        opening OperationalError handler reads obsolete exception.message, so standard
        Python 3 errors become AttributeError before its intended DatabaseDriverError
        can be raised.

        Example:
            >>> driver.get_connection()  # doctest: +SKIP


        :return: An open SQLite_Connection tracked for inherited driver.close; callers
            may close it earlier and must manage their transactions.
        """

    @abc.abstractmethod
    def get_id_from_row_dict(self, row_dict: dict[str, Any]) -> Any:
        """
        Infer the row table and extract its ID column when present.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Table inference can mutate the input and raise; an absent ID is the only
        explicit False result.

        Example:
            >>> driver.get_id_from_row_dict({"work_id": 1})  # doctest: +SKIP


        :param row_dict: Column-to-value mapping; table inference removes any ``table``
            key in place.
        :return: The stored ID value, or ``False`` when that column is absent.
        """

    @abc.abstractmethod
    def direct_identify_table_from_column(
        self,
        column_heading: str,
        headings_and_columns: Optional[dict[str, set[str]]] = None,
    ) -> str:
        """
        Return the first table containing a column in the supplied or current mapping.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Ambiguity is resolved by mapping order; an unknown column raises
        InputIntegrityError.

        Example:
            >>> driver.direct_identify_table_from_column("work_title")  # doctest: +SKIP


        :param column_heading: Exact column heading to locate.
        :param headings_and_columns: Optional table-to-column-list mapping; ``None``
            reads the driver schema.
        :return: The first matching table name.
        """

    @abc.abstractmethod
    def direct_identify_table_from_row(
        self, row_dict: dict[str, Any]
    ) -> str | Literal[False]:
        """
        Find the first table whose columns contain every supplied row key.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Remove a ``table`` key from the input. An empty remaining mapping returns False;
        no match raises DatabaseIntegrityError with partial-match diagnostics. Values do
        not participate in matching.

        Example:
            >>> driver.direct_identify_table_from_row({"work_id": 1})  # doctest: +SKIP


        :param row_dict: Column-to-value mapping; table inference removes any ``table``
            key in place.
        :return: The first matching table name, or ``False`` for an empty mapping.
        """

    @abc.abstractmethod
    def direct_iterator_return(
        self,
        stmt: str,
        headings: Iterable[str],
        table: Optional[str] = None,
        bindings: SQLParameters | None = None,
    ) -> Iterator[dict[str, Any]]:
        """
        Execute supplied SQL lazily and yield converted row dictionaries.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        A table enables declared-type conversion. The connection closes in finally when
        exhausted, explicitly closed or interrupted by an exception; abandoning a live
        generator can retain it.

        Example:
            >>> driver.direct_iterator_return(
            ...     "SELECT work_id, work_title FROM works",
            ...     ["work_id", "work_title"],
            ...     table="works",
            ... )  # doctest: +SKIP


        :param stmt: Trusted SQL statement to execute.
        :param headings: Reusable ordered column-name iterable matching each result row.
        :param table: Optional table context for declared-type conversion.
        :param bindings: Optional parameter sequence passed unchanged to execute.
        :return: A generator of row dictionaries despite the None annotation.
        """

    @property
    @abc.abstractmethod
    def macros(self) -> "DatabaseMacrosAPI":
        """
        Expose the macro helper already installed on this driver.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.macros  # doctest: +SKIP


        :return: The stored ``_macros`` object.
        """

    @abc.abstractmethod
    def refresh(self, reconnect: bool = False) -> None:
        """
        Invalidate cached state while preserving a usable primary connection.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Probe failures propagate and retain the primary transaction and TEMP objects.
        Only an explicit reconnect replaces the handle.

        Example:
            >>> driver.refresh()  # doctest: +SKIP


        :param reconnect: Force a new primary connection even when the current one
            responds to a liveness probe.
        :return: ``None``.
        """

    @abc.abstractmethod
    def reopen(self) -> None:
        """
        Replace the primary connection, suppressing errors from closing the old handle.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Opening failures propagate; caches are not refreshed here.

        Example:
            >>> driver.reopen()  # doctest: +SKIP


        :return: ``None``.
        """

    @abc.abstractmethod
    def shell(self) -> None:
        """
        Run an interactive SQL loop on a fresh connection.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Print the target and warning, then concatenate input lines without added
        newlines until sqlite3.complete_statement accepts the buffer. Execute one
        statement and commit after each successful execution; print fetched rows only
        when the stripped text starts with SELECT. SQLite errors are printed and the
        buffer is discarded without an explicit rollback. Other errors propagate.

        A blank line exits even with incomplete buffered SQL. Normal exit refreshes
        cached metadata and closes the connection; exceptions or interrupts have no
        finally cleanup. This is unrestricted SQL on the configured database, with no
        confirmation or read-only mode.

        Example:
            >>> driver.shell()  # doctest: +SKIP


        :return: None; may commit arbitrary SQL changes and refresh the driver caches.
        """

    @abc.abstractmethod
    def simple_print_progress_handler(self) -> None:
        """
        Print the counter at multiples of one hundred million, then increment it.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.simple_print_progress_handler()  # doctest: +SKIP


        :return: ``None``.
        """

    @abc.abstractmethod
    def sql_dump(self) -> Iterator[str]:
        """Yield SQL using the borrowed primary connection without completing it.

        Pending primary writes are included. Exhaustion and early close never
        commit, roll back or close that connection. Serialize connection use for
        the whole iteration; use a caller-owned read transaction for a stable
        multi-statement snapshot.
        """

    @abc.abstractmethod
    def zero_prop_cache(self) -> None:
        """
        Invoke the internal cache invalidation and metadata refresh routine.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.zero_prop_cache()  # doctest: +SKIP


        :return: ``None``.
        """
