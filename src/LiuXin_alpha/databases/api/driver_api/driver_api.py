"""
Specify the composite contract for low-level database drivers.

DatabaseDriverAPI combines lifecycle, raw SQL, schema, policy, relationship and row
operations. Abstract bodies provide no implementation and return None if called
directly. Concrete backend results, resource ownership and transaction boundaries
must be observed; the method notes describe the existing shared SQL/SQLite
implementations where relevant. Historical annotation mismatches and the duplicated
direct_executescript declaration are retained.
"""

from __future__ import annotations

from typing import Optional, Union, Any, Sequence, Iterator, Iterable

import abc

from LiuXin_alpha.databases.api.macros_api import MacrosAPI
from LiuXin_alpha.databases.api.driver_api.mixins.tree_mixin_api import DriverTreeMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.metadata_mixin_api import DriverMetadataMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.interlink_mixin_api import DriverInterlinkMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.properties_mixin_api import DriverDatabasePropertiesMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.names_mixin_api import DriverNamesMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.intralink_mixin_api import DriverIntralinkMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.search_mixin_api import DriverSearchMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.add_mixin_api import DriverAddMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.delete_mixin_api import DriverDeleteMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.custom_columns_mixin_api import DriverCustomColumnsMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.tables_mixin_api import DriverTablesMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.triggers_mixin_api import DriverTriggersMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.new_books_compressed_files_mixin_api import DriverNewBooksCompressedFilesMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.view_mixin_api import DriverViewMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.update_mixin_api import DriverUpdateMixinAPI


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
    implementations where relevant. Historical annotation mismatches and the duplicated
    direct_executescript declaration are retained. Abstract members must be implemented
    by a backend; their empty bodies return None when called directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DatabaseDriverAPI)
        True
    """

    @abc.abstractmethod
    def __init__(self, db_metadata, db=None, set_conn=True, dirty_records_queue=None) -> None:
        """
        Initialize driver state and optionally open the primary connection.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Retain the metadata and facade by reference, install the FRBR creation hook and
        macro helper, initialize schema caches and connection tracking, and attach a no-op
        maintenance callback. A truthy set_conn opens a configured connection immediately;
        otherwise conn is None. Store the dirty-record queue after connection setup. No
        schema builder or maintenance worker is started here. Missing database_path raises
        KeyError, and connection setup errors propagate.

        Example:
            >>> driver = concrete_driver_type({"database_path": database_path}, set_conn=False)  # doctest: +SKIP


        :param db_metadata: Mapping containing database_path; the path value is copied to
            database_path while the mapping itself is retained.
        :param db: Optional database facade retained for macro and shared-mixin operations.
        :param set_conn: Open a primary connection immediately when truthy; False defers all
            connection setup.
        :param dirty_records_queue: Queue or other object retained for later dirty-record
            handling; not consumed here.
        :return: None; initializes this driver and possibly its primary connection.
        """

    @staticmethod
    @abc.abstractmethod
    def _canonicalise_table_name_for_cache(table: str) -> str:
        """
        Coerce and trim a name, discard its schema prefix and remove one wrapper pair.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Recognize brackets and symmetric quote, backslash, percent or underscore wrappers.
        This is a cache-key convenience, not an SQL parser or identifier validator; failed
        text coercion returns the input.

        Example:
            >>> driver._canonicalise_table_name_for_cache("works")  # doctest: +SKIP


        :param table: Table or view name used for schema introspection.
        :return: The canonicalized name, or the original input if coercion fails.
        """

    @abc.abstractmethod
    def _close_all_open_connections(self) -> None:
        """
        Best-effort close tracked handles and the primary connection.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Individual close errors are suppressed; tracking is cleared and ``conn`` becomes
        ``None``.

        Example:
            >>> driver._close_all_open_connections()  # doctest: +SKIP


        :return: ``None``.
        """

    @abc.abstractmethod
    def _coerce_db_value(self, value: Any, declared_type: Any) -> Optional[Union[bool, int, float, str, bytes]]:
        """
        Convert a database cell according to the declared type's conversion bucket.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Preserve None. Numeric buckets convert booleans to numbers and retain numeric
        objects; INTEGER turns integral floats into ints, while REAL converts numeric
        objects to floats. INTEGER and NUMERIC try integer text before decimal or exponent
        text; REAL parses matching text as a float. Whitespace is stripped for parsing only.
        Malformed numeric text retains its original spelling, and failed numeric-text
        conversions are suppressed. No finite-value check is applied, so overflow text can
        produce infinity.

        BLOB converts bytes, bytearray and memoryview to bytes. Other fallback paths use
        force_unicode, currently an alias for str: byte-like inputs outside BLOB are
        represented as text rather than decoded, so b"12" does not become the number 12.
        TEXT stringifies non-null values. Errors from stringification or direct
        numeric-object conversion can propagate.

        Example:
            >>> driver._coerce_db_value("Example", "TEXT")  # doctest: +SKIP


        :param value: Database cell to convert; no container or text decoding protocol is
            imposed on fallback objects.
        :param declared_type: Backend declaration used by _sqlite_affinity; an empty or
            unknown declaration still selects NUMERIC conversion.
        :return: None, an int or float, bytes for byte-like BLOB input, or text.
        """

    @staticmethod
    @abc.abstractmethod
    def _coerce_untyped_value(value: Any) -> Optional[Union[bool, int, float, str, bytes]]:
        """
        Preserve scalar numeric kinds and stringify values without a declared type.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Return None unchanged and coerce bool, int and float instances to their
        corresponding built-in types, testing bool first. Convert memoryview to bytes. For
        bytes and bytearray, first make bytes and then call force_unicode, currently str, so
        the result includes the bytes representation rather than decoded content. Other
        values also use str; its errors propagate.

        Numeric strings retain their text and whitespace because no affinity is available to
        justify parsing them.

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
        after the first. This is a lexical convenience rather than a SQL declaration parser.

        Example:
            >>> driver._normalize_declared_type("TEXT")  # doctest: +SKIP


        :param declared_type: Backend type declaration or another stringifiable value; None
            means no type.
        :return: Uppercase first token without its size suffix, or empty text.
        """

    @abc.abstractmethod
    def _register_open_connection(self, conn: Any) -> None:
        """
        Track a connection for later bulk closing and return it unchanged.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Tracking failures are logged without preventing connection use.

        Example:
            >>> driver._register_open_connection(connection)  # doctest: +SKIP


        :param conn: Open connection supplied by the caller.
        :return: The supplied connection.
        """

    @abc.abstractmethod
    def _row_to_dict(
            self,
            *,
            table: Optional[str] = None,
            headings: Sequence[Any],
            row: Sequence[Any]) -> dict[Any, Any]:
        """
        Pair headings with converted row cells, retaining set-valued cells by identity.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        A truthy table selects its cached declared-type map and typed conversion. Missing
        entries use an empty declaration, which selects NUMERIC; a falsey table instead uses
        untyped conversion. Fetch the map before iterating, even for empty headings.
        Set-valued cells bypass conversion without copying.

        Iterate headings and index the row: surplus cells are ignored and too few cells
        raise IndexError. Duplicate headings overwrite earlier values. Headings must be
        hashable; the legacy set-heading branch still attempts dictionary insertion, so a
        plain set heading raises TypeError. Schema lookup and cell conversion errors
        propagate.

        Example:
            >>> driver._row_to_dict(headings=["work_id", "work_title"], row=(1, "Example"))  # doctest: +SKIP


        :param table: Trusted table spelling for declared-type lookup, or a falsey value
            such as None to avoid that lookup and use untyped conversion.
        :param headings: Ordered dictionary keys; may contain non-string hashable values and
            duplicates, but plain set keys are unsupported.
        :param row: Indexable cell sequence with at least one value per heading.
        :return: A new heading-to-value dictionary; retained sets are shared with row.
        """

    @staticmethod
    @abc.abstractmethod
    def _sanitize_embedded_nul_text(*, target_table: str, row_dict: dict) -> None:
        """
        Replace embedded NUL characters in string values with visible ``<NUL>`` text.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Mutate the mapping in place; non-string values are untouched. The table argument is
        currently unused.

        Example:
            >>> driver._sanitize_embedded_nul_text(target_table="works", row_dict={"work_id": 1})  # doctest: +SKIP


        :param target_table: Table context accepted by the helper but not consulted.
        :param row_dict: Mutable column/value mapping whose string payloads are sanitized.
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

        Check substrings in order: INT, then CHAR/CLOB/TEXT, then BLOB, then REAL/FLOA/DOUB.
        Empty and unrecognized tokens fall through to NUMERIC. The preceding normalization
        discards size suffixes and later tokens; this is a simplified classification, not a
        complete SQL type parser.

        Example:
            >>> driver._sqlite_affinity("TEXT")  # doctest: +SKIP


        :param declared_type: Type declaration passed to _normalize_declared_type; None or
            blank text produces the NUMERIC fallback.
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
    def direct_backup(self, path=None):
        """
        Copy the database file using the local backup helper inside a connection context.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        A falsy helper result raises DatabaseDriverError. This is a file-copy backup, and
        this method does not explicitly close the acquired connection.

        Example:
            >>> driver.direct_backup()  # doctest: +SKIP


        :param path: Optional backup destination; ``None`` lets the file helper choose one.
        :return: The truthy backup result, normally the destination path.
        """

    @abc.abstractmethod
    def direct_create_new_database(self) -> None:
        """
        Create parent directories and build the schema at the configured path.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        The backend builder supplies the schema. Language seeding/locking is best effort;
        the connection commits and closes on success, with no finally cleanup on failure.

        Example:
            >>> driver.direct_create_new_database()  # doctest: +SKIP


        :return: ``None``.
        """

    @abc.abstractmethod
    def direct_execute(self, sql: str, values: Optional[tuple[Any, ...]] = None) -> None:
        """
        Execute one statement using a live primary connection and its transaction context.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Replace a missing/broken primary handle; convert an integer binding to a one-element
        text tuple. Execution failures become DatabaseDriverError. Finally attempt a cache
        refresh while preserving a usable handle, so TEMP objects remain available.

        Example:
            >>> driver.direct_execute("SELECT 1")  # doctest: +SKIP


        :param sql: SQL text to execute, with placeholders when bindings are supplied.
        :param values: Optional bindings; a bare integer is converted to a one-element
            string tuple.
        :return: The backend execution result, normally a cursor, despite the None
            annotation.
        """

    @abc.abstractmethod
    def direct_executemany(
            self,
            sql: str,
            values: Optional[tuple[tuple[Any, ...], ...]] = None) -> None:
        """
        Execute a batch on the primary connection, or delegate an unbound script.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Only outer tuples receive scalar-to-one-tuple normalization. ValueError retries try
        alternate binding shapes; execution failures become DatabaseDriverError. Refresh
        caches after success. With values=None, delegate to direct_executescript.

        Example:
            >>> driver.direct_executemany("SELECT 1")  # doctest: +SKIP


        :param sql: SQL text to execute, with placeholders when bindings are supplied.
        :param values: Binding rows, an outer tuple of scalar values, or ``None`` for script
            execution.
        :return: ``None``.
        """

    @abc.abstractmethod
    def direct_executescript(self, sqlscript: str) -> None:
        """
        Run trusted multi-statement SQL on the primary connection and refresh caches.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Create a primary handle if absent, wrap execution failures as DatabaseDriverError
        and attempt refresh in finally. Backend executescript transaction behavior still
        applies.

        This earlier declaration is overwritten by the later direct_executescript definition
        in the same class; it does not provide a separate runtime overload.

        Example:
            >>> driver.direct_executescript("CREATE TABLE sample (value TEXT);")  # doctest: +SKIP


        :param sqlscript: SQL script text passed to the backend executescript method.
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


        :return: The stored identity value, or ``None`` when there are no rows or the value
            is null.
        """

    @abc.abstractmethod
    def direct_get_null_row(self, table: str) -> Optional[dict[str, Any]] | bool:
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
        Check for an ID-zero sentinel in a validated table and always close the query connection.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.direct_has_null_row("works")  # doctest: +SKIP


        :param table: Table name resolved by the host driver.
        :return: Whether a row with ID zero exists.
        """

    # Todo: This is a macro
    @abc.abstractmethod
    def direct_run_ta_update(self, ta_row_id):
        """
        Launch the legacy aggregate-update thread when its preference enables it.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Only the exact string "true" for run_ta_update_after_each_change dispatches
        run_ta_updates with a one-element ID list and this driver. The helper starts a
        daemon thread; this method does not wait for completion. "false" and other values,
        including boolean True, cause no dispatch. Preference lookup and launch errors
        propagate; background failures are not returned to the caller.

        Example:
            >>> driver.direct_run_ta_update(1)  # doctest: +SKIP


        :param ta_row_id: Aggregate/title row ID passed unchanged in the worker input list;
            not validated here.
        :return: None, regardless of whether an update thread is launched.
        """
        ...

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
            updates: Optional[dict[str, Any]] = None,
            **fields: Any) -> bool:
        """
        Merge updates and delegate a sentinel-row update to the row writer.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Missing sentinels raise InputIntegrityError. Keyword fields mutate an input dict in
        place and win over its values. Supplied ID-column updates also override the initial
        zero, so callers must omit that column to target the sentinel.

        Example:
            >>> driver.direct_update_null_row("works", {"work_title": "Unknown"})  # doctest: +SKIP


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
            >>> driver.direct_dirty_record("works", 1, "title changed")  # doctest: +SKIP


        :param table: Table name checked against the host's current table collection.
        :param table_id: ID of the affected record.
        :param reason: Reason string retained in the queued tuple.
        :return: ``None``.
        """

    @abc.abstractmethod
    def dump_and_restore(self, callback=lambda x: x, sql: Optional[str] = None) -> None:
        """
        Rebuild the file from its SQL dump, replace it and reopen the driver.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Read user_version from the primary connection, dump through a separate configured
        connection to a UTF-8 temporary SQL file, and restore into a temporary database
        beside database_path. Prefix SQL runs before the dump rather than replacing it.
        Restore execution accumulates lines until a complete statement is available and
        calls execute once per accumulated statement, so multiple complete statements on one
        line are not supported.

        The restore connection enables foreign keys and a subset of compatibility
        functions/collations, not every driver callback. Dumps needing other callbacks may
        fail. Restore the saved user_version after SQL execution, then close all
        driver-tracked handles and atomically rename the rebuilt file over database_path.
        Always attempt reopen after that rename attempt, even if renaming failed; a reopen
        error can mask an earlier error. No schema-cache refresh is performed.

        Use a file-backed target and commit pending writes before calling: the separate dump
        connection cannot include uncommitted changes on another connection, and closing
        tracked handles can discard them. Coordinate exclusive access yourself; there is no
        backup, locking protocol or rollback of a completed replacement. Temporary
        dump/restore connections are closed best-effort and temporary files are cleaned up
        by their contexts. Callback, decoding, SQL and filesystem errors propagate; failures
        before replacement leave the original file in place.

        Example:
            >>> driver.dump_and_restore()  # doctest: +SKIP


        :param callback: Callable receiving two localized progress strings, before dumping
            and restoring. Its return value is ignored; None installs a local identity
            callback.
        :param sql: Optional prefix: bytes decoded as UTF-8, or any other non-None value
            converted with str. None adds no prefix; supplied SQL must be compatible with
            the subsequent dump.
        :return: None; replaces the database file and installs a fresh primary connection on
            success.
        """

    @abc.abstractmethod
    def direct_execute_sql(self, sql: str, parameters=None) -> Optional[int]:
        """
        Execute one statement on a fresh connection and commit.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Return lastrowid without explicitly closing the connection or refreshing caches;
        backend execution errors propagate.

        Example:
            >>> driver.direct_execute_sql("SELECT 1")  # doctest: +SKIP


        :param sql: SQL text to execute, with placeholders when bindings are supplied.
        :param parameters: Optional sequence of values bound to SQL placeholders.
        :return: The backend cursor lastrowid, whose meaning depends on the statement.
        """

    @abc.abstractmethod
    def direct_executescript(self, sqlscript):
        """
        Run trusted multi-statement SQL on the primary connection and refresh caches.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Create a primary handle if absent, wrap execution failures as DatabaseDriverError
        and attempt refresh in finally. Backend executescript transaction behavior still
        applies.

        Example:
            >>> driver.direct_executescript("CREATE TABLE sample (value TEXT);")  # doctest: +SKIP


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
    def get_connection(self):
        """
        Open, configure and track a fresh SQLite connection.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Register set/list/dict adapters and PYSET/PYLIST/PYDICT/DATE converters globally,
        enable callback tracebacks, then open database_path with declared-type parsing and
        check_same_thread=False. Every call opens a distinct connection, including an
        independent database for the literal :memory: path. Existing primary handles are not
        replaced.

        Request foreign-key enforcement and warn if SQLite does not report it enabled.
        Install aggregates, collations, regex/tree/sort/UUID helpers, an always-true
        books_list_filter, maintenance callbacks and a progress callback after every
        virtual-machine instruction. The progress handler prints the initial counter value,
        zero. DIRTY_RECORD and NEW_DIRTY_RECORD look up the current maintainer on each
        invocation; DIRTY_INTERLINK_RECORD binds the maintainer method at connection
        creation. Cross-thread access still requires caller coordination.

        Best-effort LANGUAGE_ID registration may seed and lock an existing FRBR languages
        table, committing changes; exceptions from that registration are suppressed. Other
        setup failures propagate without guaranteed cleanup. The opening OperationalError
        handler reads obsolete exception.message, so standard Python 3 errors become
        AttributeError before its intended DatabaseDriverError can be raised.

        Example:
            >>> driver.get_connection()  # doctest: +SKIP


        :return: An open SQLite_Connection tracked for inherited driver.close; callers may
            close it earlier and must manage their transactions.
        """

    @abc.abstractmethod
    def get_id_from_row_dict(self, row_dict: dict[str, Any]) -> int:
        """
        Infer the row table and extract its ID column when present.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Table inference can mutate the input and raise; an absent ID is the only explicit
        False result.

        Example:
            >>> driver.get_id_from_row_dict({"work_id": 1})  # doctest: +SKIP


        :param row_dict: Column-to-value mapping; table inference removes any ``table`` key
            in place.
        :return: The stored ID value, or ``False`` when that column is absent.
        """

    @abc.abstractmethod
    def direct_identify_table_from_column(
            self,
            column_heading: str,
            headings_and_columns: Optional[dict[str, set[str]]] = None) -> str:
        """
        Return the first table containing a column in the supplied or current mapping.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Ambiguity is resolved by mapping order; an unknown column raises
        InputIntegrityError.

        Example:
            >>> driver.direct_identify_table_from_column("work_title")  # doctest: +SKIP


        :param column_heading: Exact column heading to locate.
        :param headings_and_columns: Optional table-to-column-list mapping; ``None`` reads
            the driver schema.
        :return: The first matching table name.
        """

    @abc.abstractmethod
    def direct_identify_table_from_row(self, row_dict: dict[str, Any]) -> Optional[str]:
        """
        Find the first table whose columns contain every supplied row key.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Remove a ``table`` key from the input. An empty remaining mapping returns False; no
        match raises DatabaseIntegrityError with partial-match diagnostics. Values do not
        participate in matching.

        Example:
            >>> driver.direct_identify_table_from_row({"work_id": 1})  # doctest: +SKIP


        :param row_dict: Column-to-value mapping; table inference removes any ``table`` key
            in place.
        :return: The first matching table name, or ``False`` for an empty mapping.
        """

    @abc.abstractmethod
    def direct_iterator_return(
            self,
            stmt: str,
            headings: Iterable[str],
            table: Optional[str] = None,
            bindings: Optional[tuple[str, ...]] = None) -> None:
        """
        Execute supplied SQL lazily and yield converted row dictionaries.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        A table enables declared-type conversion. The connection closes in finally when
        exhausted, explicitly closed or interrupted by an exception; abandoning a live
        generator can retain it.

        Example:
            >>> driver.direct_iterator_return("SELECT work_id, work_title FROM works", ["work_id", "work_title"], table="works")  # doctest: +SKIP


        :param stmt: Trusted SQL statement to execute.
        :param headings: Reusable ordered column-name iterable matching each result row.
        :param table: Optional table context for declared-type conversion.
        :param bindings: Optional parameter sequence passed unchanged to execute.
        :return: A generator of row dictionaries despite the None annotation.
        """


    @property
    @abc.abstractmethod
    def macros(self) -> "MacrosAPI":
        """
        Expose the macro helper already installed on this driver.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.macros  # doctest: +SKIP


        :return: The stored ``_macros`` object.
        """

    @abc.abstractmethod
    def make_scratch(self) -> str:
        """
        Copy the database file into a scratch directory and switch its configured path.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Existing connection handles are not replaced; callers must manage their lifecycle.
        Copy errors propagate.

        Example:
            >>> driver.make_scratch()  # doctest: +SKIP


        :return: The new database path.
        """

    @abc.abstractmethod
    def refresh(self, reconnect: bool = False) -> None:
        """
        Invalidate cached state while preserving a usable primary connection.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Probe failures or an explicit reconnect replace the primary handle; retaining it
        preserves connection-local TEMP objects.

        Example:
            >>> driver.refresh()  # doctest: +SKIP


        :param reconnect: Force a new primary connection even when the current one responds
            to a liveness probe.
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

        Print the target and warning, then concatenate input lines without added newlines
        until sqlite3.complete_statement accepts the buffer. Execute one statement and
        commit after each successful execution; print fetched rows only when the stripped
        text starts with SELECT. SQLite errors are printed and the buffer is discarded
        without an explicit rollback. Other errors propagate.

        A blank line exits even with incomplete buffered SQL. Normal exit refreshes cached
        metadata and closes the connection; exceptions or interrupts have no finally
        cleanup. This is unrestricted SQL on the configured database, with no confirmation
        or read-only mode.

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
    def sql_dump(self):
        """
        Yield SQL dump lines from the existing primary connection.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        The generator enters the connection transaction context when iteration begins and
        delegates to iterdump. Exhausting it successfully commits any pending transaction;
        exceptional exit, including closing a suspended generator, rolls it back. It does
        not close the connection. It requires a usable primary conn and does not open one
        when initialization was deferred. SQLite dump scope applies: this is not a copy of
        connection-local callbacks or every pragma.

        Example:
            >>> driver.sql_dump()  # doctest: +SKIP


        :return: Iterator of SQL text lines; SQL execution and transaction effects occur
            during iteration.
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
