"""
Combine stdlib driver connections with APSW-backed dump and restore.

Importing this module requires apsw and the bundled APSW shell.
DatabaseDriver.get_connection returns the local sqlite3 subclass; the separate APSW
Connection supplies shell compatibility for file rebuilds. Both connection classes
expose different transaction and result APIs. Driver.close tracks ordinary
connections; rebuild contexts close their APSW handles separately.
"""

from __future__ import print_function, annotations

import datetime

import apsw
import os
import sqlite3
import re

from LiuXin_alpha.databases.search_functions import register_search_functions
from LiuXin_alpha.databases.db_types.driver_types import SQLParameters
from LiuXin_alpha.databases.api.maintenance_api import MaintenanceCallbackSinkAPI
from LiuXin_alpha.databases.connection_hooks import SQLiteCompletionConnection
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.sqlite_progress import (
    SQLiteProgressMixin,
)
import uuid
from contextlib import closing
from functools import partial
import pathlib
from queue import Queue

from typing import (
    Literal,
    overload,
    Union,
    Optional,
    Any,
    TYPE_CHECKING,
    Iterable,
    Iterator,
    Callable,
    cast,
)

from LiuXin_alpha.databases.maintenance.dummy_maintenance_bot import DummyMaintenanceBot
from LiuXin_alpha.utils.logging import LiuXin_print, LiuXin_warning_print

from LiuXin_alpha.databases.database_driver_plugins.SQL.macros import (
    SQLiteDatabaseMacros,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.custom_columns import (
    SQLiteCustomColumnsDriverMixin,
)

from LiuXin_alpha.errors import DatabaseDriverError

from LiuXin_alpha.utils.language_tools.lx_name_manip import authors_str_to_sort_str

from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.utils.date import utcfromtimestamp
from LiuXin_alpha.utils.databases.apsw_shell import Shell
from LiuXin_alpha.utils.ptempfiles import TemporaryFile
from LiuXin_alpha.utils.localization import _
from LiuXin_alpha.utils.libraries.liuxin_six import user_input
from LiuXin_alpha.utils.storage.local.filenames import atomic_rename

from LiuXin_alpha.databases.database_driver_plugins.SQL.utility_mixins import (
    SQLiteTableLinkingMixin,
)

from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver import (
    SQLBaseDriver,
    _create_new_database,
)

# Todo: Don't do this...
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.utils import (
    PyListAggregate,
    PySetAggregate,
    SortAggregate,
    SqliteAumSortedConcatenate,
    SqliteSortedConcatenate,
    SqliteIdentifiersConcat,
    SqliteConcatenate,
    SqliteStupidConcatenate,
    register_sqlite_aggregate,
    icu_collator,
    py_date_converter,
    py_dict_adapter,
    py_dict_converter,
    py_list_adapter,
    py_list_converter,
    py_set_adapter,
    py_set_converter,
    pynocase,
    Concatenate,
    SortedConcatenate,
    AumSortedConcatenate,
    IdentifiersConcat,
    DynamicFilter,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.utils import (
    _author_sort_sql_value,
)
from LiuXin_alpha.metadata.title_names import title_to_title_sort

from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.calibre_emulation_mixin import (
    CalibreEmulationMixin,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.sql_execution_mixin import (
    SQLExecutionMixin,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.math_mixin import (
    MathFunctionsMixin,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.dirty_records_mixin import (
    DirtyRecordsMixin,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.table_names_mixin import (
    TableNamesMixin,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.tree_mixjn import (
    TreeMethodsMixin,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.metadata_mixin import (
    MetadataMethodMixin,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.triggers_mixin import (
    TriggersMixin,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.search_mixin import (
    SearchMixin,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.value_casting_mixin import (
    ValueCastingMixin,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.new_book_mixin import (
    BookGroupMixin,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.delete_mixin import (
    DeleteMixin,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.add_mixin import (
    AddingMixin,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.update_mixin import (
    UpdateMixin,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.view_mixin import (
    ViewMixin,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.table_creation_mixin import (
    TableCreationMixin,
)

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class Connection(apsw.Connection):
    """
    Configure an APSW connection for legacy shell and query compatibility.

    Construction sets a 10-second busy timeout, a 5000-page cache target and in-memory
    temporary storage, then registers sort, UUID, filter, collation and aggregate
    helpers. This class is separate from the stdlib connection returned by
    DatabaseDriver.get_connection; it does not enable foreign keys here. Callers own and
    close the connection.

    Example:
        >>> conn = Connection(":memory:")
        >>> conn.get("PRAGMA temp_store")
        [(2,)]
        >>> conn.close()
    """

    BUSY_TIMEOUT = 10000  # milliseconds

    def __init__(self, path: Union[str, pathlib.Path]) -> None:
        """
        Open the APSW target and install compatibility settings and callbacks.

        APSW opens the supplied target before setup; opening or registration failures
        propagate without a local cleanup wrapper. The default books_list_filter accepts
        every value. Aggregate registrations preserve legacy implementations and do not
        establish compatibility with every APSW version.

        Example:
            >>> conn = Connection(":memory:")
            >>> conn.get("SELECT books_list_filter(42)")
            [(1,)]
            >>> conn.close()


        :param path: Database target forwarded to apsw.Connection; use a filename or
            :memory: as accepted by the installed binding.
        :return: None; initializes an open APSW connection.
        """
        apsw.Connection.__init__(self, str(path))

        self.setbusytimeout(self.BUSY_TIMEOUT)
        self.execute("pragma cache_size=5000")
        self.execute("pragma temp_store=2")

        encoding = next(self.execute("pragma encoding"))[0]
        self.createcollation("PYNOCASE", partial(pynocase, encoding=encoding))

        register_search_functions(self)
        self._register_scalar("title_sort", title_to_title_sort, 1)
        self._register_scalar("author_to_author_sort", _author_sort_sql_value, 1)
        self._register_scalar("uuid4", lambda: str(uuid.uuid4()), 0)

        # Dummy functions for dynamically created filters
        self._register_scalar("books_list_filter", lambda x: 1, 1)
        self.createcollation("icucollate", icu_collator)

        # Legacy aggregators (never used) but present for backwards compat
        self._register_aggregate("sortconcat", SortedConcatenate, 2)
        self._register_aggregate(
            "sortconcat_bar", partial(SortedConcatenate, sep="|"), 2
        )
        self._register_aggregate(
            "sortconcat_amper", partial(SortedConcatenate, sep="&"), 2
        )
        self._register_aggregate("identifiers_concat", IdentifiersConcat, 2)
        self._register_aggregate("concat", Concatenate, 1)
        self._register_aggregate("aum_sortconcat", AumSortedConcatenate, 4)

    def _register_scalar(
        self, name: str, callback: Callable[..., apsw.SQLiteValue], arity: int
    ) -> None:
        """Bridge fixed Python signatures to APSW's SQL-selected callback arity."""
        register = cast(
            "Callable[[str, Callable[..., apsw.SQLiteValue], int], None]",
            self.create_scalar_function,
        )
        register(name, callback, arity)

    def _register_aggregate(
        self,
        name: str,
        factory: Callable[
            [], tuple[object, Callable[..., None], Callable[..., apsw.SQLiteValue]]
        ],
        arity: int,
    ) -> None:
        """Register state/step/final callbacks with their SQL-selected arity.

        Each factory types its own state and callback arguments. APSW's stub
        uses arbitrary positional values, so adapt only this registration edge.
        """
        register = cast(
            "Callable[[str, Callable[[], tuple[object, Callable[..., None], Callable[..., apsw.SQLiteValue]]], int], None]",
            self.create_aggregate_function,
        )
        register(name, factory, arity)

    def create_dynamic_filter(self, name: str) -> None:
        """
        Register an initially empty membership filter as a scalar SQL function.

        Create a DynamicFilter with no allowed IDs and register it under the requested
        name with arity one. Its Python instance is retained by SQLite but is not
        returned or stored on this wrapper, so this method exposes no handle for
        changing its membership. Re-registering a name replaces its callback.

        Example:
            >>> conn = Connection(":memory:")
            >>> conn.create_dynamic_filter("selected_books")
            >>> conn.get("SELECT selected_books(1)")
            [(0,)]
            >>> conn.close()


        :param name: Scalar-function name passed directly to APSW, without
            application-level validation.
        :return: None; subsequent calls to the registered filter return zero until its
            hidden membership changes.
        """
        f = DynamicFilter(name)
        self._register_scalar(name, f, 1)

    def get(
        self,
        sql: str,
        bindings: apsw.Bindings | None = None,
        /,
        *,
        all: bool = True,
        **options: object,
    ) -> list[tuple[apsw.SQLiteValue, ...]] | apsw.SQLiteValue:
        """Execute a query and fetch all rows or the first scalar, closing its cursor.

        Missing scalar rows and SQL NULL return None. Other legacy keyword
        options are ignored. The connection and caller transaction remain open.
        """
        with closing(self.execute(sql, bindings)) as cursor:
            if all:
                return cursor.fetchall()
            row = cursor.fetchone()
            return row[0] if row else None

    def execute(
        self,
        statements: str = "",
        bindings: apsw.Bindings | None = None,
        *,
        can_cache: bool = True,
        prepare_flags: int = 0,
        explain: int = -1,
        sql: str | None = None,
    ) -> apsw.Cursor:
        """
        Execute SQL and optional bindings on a newly created APSW cursor.

        Return the cursor execution result without fetching rows, closing the cursor or
        starting a wrapper transaction. APSW SQL and binding rules, including its
        multiple-statement behavior, apply; execution errors propagate.

        Example:
            >>> conn = Connection(":memory:")
            >>> list(conn.execute("SELECT ?", (7,)))
            [(7,)]
            >>> conn.close()


        :param statements: SQL text interpreted by APSW; use placeholders for bound data.
        :param sql: Legacy alias for statements.
        :param can_cache: Permit APSW statement caching.
        :param prepare_flags: Native SQLite preparation flags.
        :param explain: Native APSW explain mode.
        :param bindings: Optional sequence or mapping accepted by APSW; forwarded
            unchanged under the native APSW binding contract.
        :return: The result cursor returned by APSW execute.
        """
        if sql is not None:
            if statements:
                raise TypeError("Supply statements or sql, not both")
            statements = sql
        cursor = self.cursor()
        try:
            return cursor.execute(
                statements,
                bindings,
                can_cache=can_cache,
                prepare_flags=prepare_flags,
                explain=explain,
            )
        except BaseException:
            cursor.close()
            raise

    def executemany(
        self,
        statements: str = "",
        sequenceofbindings: Iterable[apsw.Bindings] = (),
        *,
        can_cache: bool = True,
        prepare_flags: int = 0,
        explain: int = -1,
        sql: str | None = None,
        sequence_of_bindings: Iterable[apsw.Bindings] | None = None,
    ) -> apsw.Cursor:
        """
        Execute binding rows within an APSW connection transaction context.

        Enter the connection context before calling cursor.executemany and leave it
        before returning the cursor. Normal context exit commits or releases its
        savepoint; exceptions inside the context roll back. If a result-producing
        statement leaves lazy cursor work, that later iteration occurs outside this
        wrapper context. Inputs and execution errors are passed through to APSW.

        Example:
            >>> conn = Connection(":memory:")
            >>> _ = conn.execute("CREATE TABLE sample (value INTEGER)")
            >>> _ = conn.executemany("INSERT INTO sample VALUES (?)", [(2,), (4,)])
            >>> conn.get("SELECT SUM(value) FROM sample")
            [(6,)]
            >>> conn.close()


        :param sql: Statement to repeat with each binding row.
        :param sequence_of_bindings: Iterable of parameter sequences or mappings
            accepted by cursor.executemany.
        :return: The APSW cursor result; the surrounding connection context has already
            exited.
        """
        if sql is not None:
            if statements:
                raise TypeError("Supply statements or sql, not both")
            statements = sql
        if sequence_of_bindings is not None:
            sequenceofbindings = sequence_of_bindings
        cursor = self.cursor()
        try:
            with self:
                cursor.executemany(
                    statements,
                    sequenceofbindings,
                    can_cache=can_cache,
                    prepare_flags=prepare_flags,
                    explain=explain,
                )
            return cursor
        except BaseException:
            cursor.close()
            raise


class SQLite_Connection(SQLiteCompletionConnection):
    """
    Extend sqlite3.Connection with all-row, single-row and scalar fetching.

    The get and get_row helpers forward positional arguments to execute and interpret
    only the all keyword. They do not commit or close the connection. Direct
    construction uses sqlite3 defaults. Custom row factories may return arbitrary
    objects, so all-row results are list[Any]; scalar/single-row modes retain the
    native dynamic result. Driver-specific converters, callbacks and
    pragmas are installed by DatabaseDriver.get_connection instead.

    Example:
        >>> conn = SQLite_Connection(":memory:")
        >>> conn.get_row("SELECT 3, 4", all=False)
        (3, 4)
        >>> conn.close()
    """

    @overload
    def get(
        self, sql: str, parameters: SQLParameters = (), /, *,
        all: Literal[True] = True, **options: object,
    ) -> list[Any]: ...

    @overload
    def get(
        self, sql: str, parameters: SQLParameters = (), /, *,
        all: Literal[False], **options: object,
    ) -> Any: ...

    @overload
    def get(
        self, sql: str, parameters: SQLParameters = (), /, *,
        all: bool, **options: object,
    ) -> Any: ...

    def get(
        self,
        sql: str,
        parameters: SQLParameters = (),
        /,
        *,
        all: bool = True,
        **options: object,
    ) -> Any:
        """
        Execute one statement and fetch all rows or the first scalar.

        With a truthy all option, return fetchall(). Otherwise return column zero of
        fetchone(), or None when no row is available. A selected SQL NULL also becomes
        None in scalar mode. Positional bindings pass directly to sqlite3; other keyword
        options are ignored and no transaction is committed.

        Operational errors retain their SQLite exception type and original cause.
        The cursor closes after fetching, including on failure.

        Example:
            >>> conn = SQLite_Connection(":memory:")
            >>> conn.get("SELECT ? UNION ALL SELECT ?", (2, 4))
            [(2,), (4,)]
            >>> conn.get("SELECT 2, 4", all=False)
            2
            >>> conn.get("SELECT 2 WHERE 0", all=False) is None
            True
            >>> conn.get("SELECT NULL", all=False) is None
            True
            >>> conn.close()


        :param sql: SQL statement.
        :param parameters: Positional or named SQL bindings.
        :param all: True selects all rows; False selects the first scalar.
        :param options: Other legacy keyword options are ignored.
        :return: A list of rows by default, or the first column of the first row in
            scalar mode; None for an absent scalar row.
        """
        try:
            cursor = self.execute(sql, parameters)
        except sqlite3.OperationalError as exc:
            raise sqlite3.OperationalError(
                f"Couldn't execute - operational error: {exc}"
            ) from exc
        with closing(cursor):
            if all:
                return cursor.fetchall()
            row = cursor.fetchone()
            return row[0] if row else None

    @overload
    def get_row(
        self, sql: str, parameters: SQLParameters = (), /, *,
        all: Literal[True] = True, **options: object,
    ) -> list[Any]: ...

    @overload
    def get_row(
        self, sql: str, parameters: SQLParameters = (), /, *,
        all: Literal[False], **options: object,
    ) -> Any: ...

    @overload
    def get_row(
        self, sql: str, parameters: SQLParameters = (), /, *,
        all: bool, **options: object,
    ) -> Any: ...

    def get_row(
        self,
        sql: str,
        parameters: SQLParameters = (),
        /,
        *,
        all: bool = True,
        **options: object,
    ) -> Any:
        """
        Execute one statement and fetch all rows or one complete row.

        A truthy all option returns fetchall(); a false option returns fetchone(), with
        None for no row. An SQL NULL remains part of the returned row. Extra keyword
        options are ignored, while positional bindings go directly to execute. The
        helper neither commits nor closes the connection.

        Operational errors retain their SQLite exception type and original cause.
        The cursor closes after fetching, including on failure.

        Example:
            >>> conn = SQLite_Connection(":memory:")
            >>> conn.get_row("SELECT ?, NULL", (5,), all=False)
            (5, None)
            >>> conn.get_row("SELECT 5 WHERE 0", all=False) is None
            True
            >>> conn.get_row("SELECT 5 WHERE 0")
            []
            >>> conn.close()


        :param sql: SQL statement.
        :param parameters: Positional or named SQL bindings.
        :param all: True selects all rows; False selects one row.
        :param options: Other legacy keyword options are ignored.
        :return: A list of rows by default, or one row when all is false; None if that
            single row is absent. Row shape follows the connection row factory.
        """
        try:
            cursor = self.execute(sql, parameters)
        except sqlite3.OperationalError as exc:
            raise sqlite3.OperationalError(
                f"Couldn't execute - operational error: {exc}"
            ) from exc
        with closing(cursor):
            if all:
                return cursor.fetchall()
            row = cursor.fetchone()
            return row if row else None


class DatabaseDriver(
    SQLiteProgressMixin,
    SQLBaseDriver,
    SQLiteCustomColumnsDriverMixin,
    SQLiteTableLinkingMixin,
    ValueCastingMixin,
    CalibreEmulationMixin,
    SQLExecutionMixin,
    MathFunctionsMixin,
    DirtyRecordsMixin,
    TableNamesMixin,
    TreeMethodsMixin,
    MetadataMethodMixin,
    TriggersMixin,
    SearchMixin,
    BookGroupMixin,
    DeleteMixin,
    AddingMixin,
    UpdateMixin,
    ViewMixin,
    TableCreationMixin,
):
    """
    Provide the legacy APSW-named driver with stdlib working connections.

    Ordinary queries use SQLite_Connection from sqlite3, shared SQL mixins and tracked
    handles. APSW Connection and Shell are used specifically by dump_and_restore.
    Construction normally opens a working connection but does not create the FRBR
    schema; set_conn=False defers opening. Inherited close/reopen manage ordinary
    handles. Callers coordinate concurrent use and file replacement.

    Example:
        >>> driver = DatabaseDriver({"database_path": ":memory:"}, set_conn=False)
        >>> driver.conn is None
        True
        >>> driver.close()
    """

    def __init__(
        self,
        db_metadata: dict[str, Any],
        db: Optional["DatabaseAPI"] = None,
        set_conn: bool = True,
        dirty_records_queue: Queue[tuple[str, int, str]] | None = None,
    ) -> None:
        """
        Initialize driver state and optionally open the primary connection.

        Retain the metadata and facade by reference, install the FRBR creation hook and
        macro helper, initialize schema caches and connection tracking, and attach a no-
        op maintenance callback. A truthy set_conn opens a configured connection
        immediately; otherwise conn is None. Store the dirty-record queue after
        connection setup. No schema builder or maintenance worker is started here.
        Missing database_path raises KeyError, and connection setup errors propagate.

        Example:
            >>> metadata = {"database_path": ":memory:"}
            >>> queue = []
            >>> driver = DatabaseDriver(
            ...     metadata, set_conn=False, dirty_records_queue=queue
            ... )
            >>> driver.db_metadata is metadata and driver.dirty_records_queue is queue
            True
            >>> driver.event_count, driver.conn
            (0, None)
            >>> driver.close()


        :param db_metadata: Mapping containing database_path; the path value is copied
            to database_path while the mapping itself is retained.
        :param db: Optional database facade retained for macro and shared-mixin
            operations.
        :param set_conn: Open a primary connection immediately when truthy; False defers
            all connection setup.
        :param dirty_records_queue: Queue or other object retained for later dirty-
            record handling; not consumed here.
        :return: None; initializes this driver and possibly its primary connection.
        """
        self._create_new_database = _create_new_database

        self.db_metadata = db_metadata
        self.database_path = db_metadata["database_path"]
        self.db = db

        self._macros = SQLiteDatabaseMacros(db=self.db)

        # These attributes will be used as caches for computationally expensive information off the database
        self.tables = None
        self.tables_and_columns = None
        self.categorized_tables = None
        self.all_column_names = set()

        # locations are loaded from the DatabasePing object
        self.locations = None

        # Counts callbacks when simple_print_progress_handler is explicitly enabled.
        self.event_count = 0

        # Track every connection created by this driver so we can reliably release file handles
        # (particularly important on Windows, where open SQLite files cannot be deleted).
        self._open_connections = []

        # Some tables shouldn't be touched - these are the helper tables
        self.helper_tables = [
            "conversion_options",
            "compressed_files",
            "new_books",
            "database_metadata",
            "hashes",
        ]

        # The maintenance bot allows the behavior of the database to be customized with python code.
        self.maintainer_callback: MaintenanceCallbackSinkAPI = DummyMaintenanceBot()

        # Parse some of the preference values which affect the behavior of the database

        # Store a connection to be used for locking
        if set_conn:
            self.conn = self.get_connection()
        else:
            self.conn = None

        # This will be usefully set when the database starts up
        self.dirty_records_queue = dirty_records_queue

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - CONNECTION METHODS TO THE DATABASE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    # Internal, implementation dependant method. Should not be exposed to the outside
    supports_normalized_search = True

    def get_connection(self) -> sqlite3.Connection:
        """
        Open, configure and track a fresh SQLite connection.

        Register set/list/dict adapters and PYSET/PYLIST/PYDICT/DATE converters
        globally, enable callback tracebacks, then open database_path with declared-type
        parsing and check_same_thread=False. Every call opens a distinct connection,
        including an independent database for the literal :memory: path. Existing
        primary handles are not replaced.

        Request foreign-key enforcement and warn if SQLite does not report it enabled.
        Install aggregates, collations, regex/tree/sort/UUID helpers, an always-true
        books_list_filter and maintenance callbacks. Progress callbacks are opt-in
        through set_progress_callbacks and are installed after connection setup.
        With no callbacks, SQLite has no Python progress handler. All three dirty
        callbacks look up the current maintainer on each invocation, including
        handles opened before the real maintainer replaces its startup placeholder.
        Cross-thread access still requires caller coordination.

        Best-effort LANGUAGE_ID registration may seed and lock an existing FRBR
        languages table, committing changes; exceptions from that registration are
        suppressed. Other setup failures close the new handle before propagating. The
        opening OperationalError handler reports DatabaseDriverError with the original
        SQLite diagnostic.

        Example:
            >>> driver = DatabaseDriver({"database_path": ":memory:"}, set_conn=False)
            >>> conn = driver.get_connection()
            >>> conn.get("PRAGMA foreign_keys", all=False)
            1
            >>> conn.get_row(
            ...     "SELECT 'abc' REGEXP 'b', books_list_filter(999)", all=False
            ... )
            (1, 1)
            >>> driver.conn is None and conn in driver._open_connections
            True
            >>> driver.close()


        :return: An open SQLite_Connection tracked for inherited driver.close; callers
            may close it earlier and must manage their transactions.
        """
        return self._open_connection(seed_languages=True)

    def _open_connection(self, *, seed_languages: bool) -> sqlite3.Connection:
        """Configure an owned handle, optionally disabling legacy language repair.

        Snapshot validation must register callbacks without modifying the database
        it is supposed to validate. Ordinary connection setup retains its existing
        language bootstrap behavior.
        """
        # Todo: Should only have to do all this once? Surely?
        # Registering converter and adaptor to deal with columns containing sets
        sqlite3.register_adapter(set, py_set_adapter)
        sqlite3.register_converter("PYSET", py_set_converter)

        # Registering converter and adaptor to deal with columns containing lists
        sqlite3.register_adapter(list, py_list_adapter)
        sqlite3.register_converter("PYLIST", py_list_converter)

        # Registering converter and adapter to deal with columns containing dictionaries
        sqlite3.register_adapter(dict, py_dict_adapter)
        sqlite3.register_converter("PYDICT", py_dict_converter)

        # The built in date adaptor chokes when passed a u'None' - replacing it with home brew until can properly
        # sanitize database inputs
        sqlite3.register_converter("DATE", py_date_converter)
        # Enable callbacks in case of error within added functions
        sqlite3.enable_callback_tracebacks(True)

        conn: SQLite_Connection | None = None
        try:
            try:
                conn = SQLite_Connection(
                    self.database_path,
                    detect_types=sqlite3.PARSE_DECLTYPES,
                    check_same_thread=False,
                )

                # Aggregator allows sets of Unicode to be stored directly as the result of queries
                register_sqlite_aggregate(conn, "pyset", 1, PySetAggregate)
                register_sqlite_aggregate(conn, "sortag", 1, SortAggregate)
                register_sqlite_aggregate(conn, "pylist", 1, PyListAggregate)

            except sqlite3.OperationalError as e:
                error_message = str(e)
                err_str = "Unable to open database connection.\n"
                err_str += "error_message: {}\n".format(error_message)
                err_str += "database_path: {}\n".format(self.database_path)
                raise DatabaseDriverError(err_str)

            # http://ubuntuforums.org/showthread.php?t=1895895
            # Tests the connection for foreign key support - issues a warning if it isn't present
            conn.execute("PRAGMA foreign_keys=ON")
            rows = conn.execute("PRAGMA foreign_keys")
            test = None
            for row in rows:
                test = row
            if test != (1,):
                default_log.warn("Warning - foreign key support not enabled.")

            # Adds regex search support to the connection
            def regexp(expr: str, item: str) -> bool:
                """
                Search a value using a freshly compiled Python regular expression.

                Installed as the two-argument SQLite REGEXP function on the enclosing
                connection. Matching is a search, not a full-string match. There is no
                pattern cache or NULL coercion: invalid patterns or incompatible values
                raise in Python and become SQLite callback errors when invoked through SQL.

                Example:
                    On a connection configured by get_connection:
                    >>> conn.get("SELECT 'abc' REGEXP 'b'", all=False)  # doctest: +SKIP
                    1


                :param expr: Pattern supplied to re.compile without extra flags or
                    normalization.
                :param item: Search target passed unchanged to the compiled pattern.
                :return: True when search finds a match, otherwise False; SQLite exposes the
                    boolean as 1 or 0.
                """
                reg = re.compile(expr)
                return reg.search(item) is not None

            register_search_functions(conn)
            conn.create_function("REGEXP", 2, regexp)

            # Add the TREE_AGGREGATOR to the connection - allows for string representation of the position of a row in a
            # tree
            conn.create_function("TREE_AG", 3, self.direct_get_tree_aggregation_str)

            # Adds a function which creates sort strings from strings of authors
            # Adds again under a different name for close calibre compatibility
            conn.create_function("AUTHORS_SORT", 1, authors_str_to_sort_str)

            # More generally, add a function which will callback to the maintenance bot to tell it that particular row in
            # a table has changed and might need attention
            conn.create_function(
                "DIRTY_RECORD",
                2,
                lambda table, row_id: self.maintainer_callback.dirty_record(table, row_id),
            )
            conn.create_function(
                "DIRTY_INTERLINK_RECORD",
                5,
                lambda update_type, table1, table2, table1_id, table2_id: (
                    self.maintainer_callback.dirty_interlink_record(
                        update_type, table1, table2, table1_id, table2_id
                    )
                ),
            )
            conn.create_function(
                "NEW_DIRTY_RECORD",
                2,
                lambda table, row_id: self.maintainer_callback.new_dirty_record(
                    table, row_id
                ),
            )

            # calibre - functions included here for compatibility
            conn.create_function("title_sort", 1, title_to_title_sort)
            conn.create_function("author_to_author_sort", 1, _author_sort_sql_value)
            conn.create_function("uuid4", 0, lambda: str(uuid.uuid4()))

            # calibre - Dummy functions for dynamically created filters
            conn.create_function("books_list_filter", 1, lambda x: 1)

            conn.create_collation("icucollate", icu_collator)

            # calibre aggregate functions, included here for compatibility
            register_sqlite_aggregate(conn, "aum_sortconcat", 4, SqliteAumSortedConcatenate)

            register_sqlite_aggregate(conn, "concat", 1, SqliteConcatenate)
            register_sqlite_aggregate(conn, "concat_error", 1, SqliteStupidConcatenate)

            register_sqlite_aggregate(
                conn, "identifiers_concat", 2, SqliteIdentifiersConcat
            )

            register_sqlite_aggregate(conn, "sortconcat", 2, SqliteSortedConcatenate)
            register_sqlite_aggregate(
                conn, "sortconcat_bar", 2, partial(SqliteSortedConcatenate, sep="|")
            )
            register_sqlite_aggregate(
                conn, "sortconcat_amper", 2, partial(SqliteSortedConcatenate, sep="&")
            )

            # Register the custom collators (ported from calibre, for compatibility)
            encoding = next(conn.execute("PRAGMA encoding"))[0]
            conn.create_collation("PYNOCASE", partial(pynocase, encoding=encoding))

            # FRBR constants convenience: resolve language tokens to `languages.language_id`.
            # Safe no-op on non-FRBR databases.
            try:
                from LiuXin_alpha.utils.language_tools import (
                    register_language_id_sql_function,
                )

                register_language_id_sql_function(
                    conn, function_name="LANGUAGE_ID", ensure_seeded=seed_languages
                )
            except Exception:
                pass

            self._configure_progress_handler(conn)
            return self._register_open_connection(conn)
        except BaseException:
            if conn is not None:
                conn.close()
            raise

    def last_modified(self) -> "datetime.datetime":
        """
        Read the configured database file modification time in UTC.

        Stat database_path on each call and convert st_mtime with utcfromtimestamp. This
        is the main file timestamp, so it may not reflect changes still held in a WAL or
        an uncommitted transaction. The result is a timezone-aware datetime.
        Missing-file and other stat errors propagate; no connection is opened.

        Example:
            >>> import tempfile
            >>> with tempfile.NamedTemporaryFile() as file:
            ...     driver = DatabaseDriver(
            ...         {"database_path": file.name}, set_conn=False
            ...     )
            ...     modified = driver.last_modified()
            >>> modified.utcoffset().total_seconds()
            0.0


        :return: UTC datetime derived from the current file mtime, using the shared
            timestamp converter.
        """
        return utcfromtimestamp(os.stat(self.database_path).st_mtime)

    # Todo: Needs to actually be written.
    def last_modified_epoch_k(self) -> None:
        """
        Reserve the legacy millisecond modification-time hook.

        The method currently has only a docstring body: it does not stat the file,
        compute an epoch or inspect the driver. It returns None.

        Example:
            >>> DatabaseDriver.last_modified_epoch_k(object()) is None
            True


        :return: None; millisecond timestamp calculation is not implemented.
        """

    # Use with extreme caution - no safeguards
    def shell(self) -> None:
        """
        Run an interactive SQL loop on a fresh connection.

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
            Start an interactive session and enter a blank line to finish:
            >>> driver.shell()  # doctest: +SKIP


        :return: None; may commit arbitrary SQL changes and refresh the driver caches.
        """
        conn = self.get_connection()
        cur = conn.cursor()

        input_buffer = ""

        info_str = "DatabasePing: {} shell.".format(self.database_path)
        LiuXin_print(info_str)
        wrn_str = "Exercise extreme caution."
        LiuXin_warning_print(wrn_str)

        LiuXin_print("Enter your SQL commands to execute in sqlite3.")
        LiuXin_print("Enter a blank line to exit.")

        while True:
            line = user_input()
            if line == "":
                break
            input_buffer += line
            if sqlite3.complete_statement(input_buffer):
                try:
                    input_buffer = input_buffer.strip()
                    cur.execute(input_buffer)
                    conn.commit()

                    if input_buffer.lstrip().upper().startswith("SELECT"):
                        print(cur.fetchall())
                except sqlite3.Error as e:
                    print("An error occurred:", e.args[0])
                input_buffer = ""

        # Certain cached constants may have changed - thus invalidating some of them to force renew next time a call is
        # made to them
        self._zero_prop_cache()

        conn.close()

    def sql_dump(self) -> Iterator[str]:
        """Yield SQL from the borrowed primary connection without completing it.

        Pending writes on that connection are included. Exhaustion, cancellation
        and consumer failure never commit or roll back caller work. Callers must
        serialize connection use for the entire iteration; use an explicit caller
        read transaction when a stable multi-statement snapshot is needed.
        Closing the generator releases its dump cursor, not the connection.

        >>> driver = DatabaseDriver({"database_path": ":memory:"})
        >>> _ = driver.conn.execute("CREATE TABLE sample (value TEXT)")
        >>> _ = driver.conn.execute("INSERT INTO sample VALUES ('pending')")
        >>> lines = list(driver.sql_dump())
        >>> any("pending" in line for line in lines)
        True
        >>> driver.conn.in_transaction
        True
        >>> dump = driver.sql_dump()
        >>> next(dump)
        'BEGIN TRANSACTION;'
        >>> dump.close()
        >>> driver.conn.get("SELECT COUNT(*) FROM sample", all=False)
        1
        >>> driver.conn.rollback()
        >>> driver.conn.get("SELECT COUNT(*) FROM sample", all=False)
        0
        >>> driver.close()
        """
        connection = self.conn
        if not isinstance(connection, sqlite3.Connection):
            raise DatabaseDriverError(
                "A live SQLite primary connection is required to dump SQL"
            )
        yield from connection.iterdump()

    def dump_and_restore(
        self,
        callback: Callable[[str], object] | None = lambda x: x,
        sql: str | bytes | None = None,
    ) -> None:
        """Rebuild privately, validate the completed graph, then replace and reopen.

        The APSW shell reads and restores the SQL dump.
        Foreign keys are checked after loading so forward references and cycles are
        supported. Integrity and foreign-key failures prevent publication. The
        original user_version is retained, and restored connections use the normal
        driver callbacks and collations. Prefix SQL executes before the dump.

        All tracked connections must have no pending writes, both at entry and
        before publication. Closing finalizes owned cursors before their native
        connections. Failed closes or surviving WAL/journal files refuse
        publication; the method never deletes a live journal to force replacement.
        The caller must coordinate exclusive access by other
        database users. There is no automatic backup or rollback after replacement;
        preparation and validation failures leave the original file in place.
        Temporary resources are cleaned up and rename failures attempt reopen.

        :param callback: Optional receiver of dumping/restoring progress messages.
        :param sql: Optional SQL prefix; bytes are decoded as UTF-8.
        :return: None after publishing a validated rebuilt file.
        """
        from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.sqlite_files import (
            require_idle_sqlite_connections,
            require_sqlite_replacement_ready,
            validate_sqlite_file,
        )

        require_idle_sqlite_connections(self._open_connections)
        if callback is None:

            def silent_callback(x: str) -> str:
                """
                Return a progress value unchanged when no callback was supplied.

                This local fallback is created only for callback=None. The enclosing
                method ignores its return value, so it adds no output or progress side
                effects.

                Example:
                    Select the silent fallback during a prepared file rebuild:
                    >>> driver.dump_and_restore(callback=None)  # doctest: +SKIP


                :param x: Progress message received from the enclosing dump/restore
                    operation.
                :return: The same object passed as x.
                """
                return x

            callback = silent_callback

        version = self.user_version
        if version is None:
            raise DatabaseDriverError(
                "A readable user_version is required to rebuild the database"
            )
        uv = int(version)

        with TemporaryFile(suffix=".sql") as fname:
            # Always generate a full dump of the current database.
            # If *sql* is provided, treat it as a prefix that will be executed before the dump is restored.
            callback(_("Dumping database to SQL") + "...")

            prefix_sql = ""
            if sql is not None:
                if isinstance(sql, bytes):
                    prefix_sql = sql.decode("utf-8")
                else:
                    prefix_sql = str(sql)

            with open(fname, "w", encoding="utf-8", newline="\n") as buf:
                if prefix_sql:
                    buf.write(prefix_sql)
                    if not prefix_sql.endswith("\n"):
                        buf.write("\n")

                with closing(Connection(path=self.database_path)) as aspw_conn:
                    shell = Shell(db=aspw_conn, stdout=buf)
                    shell.process_command(".dump")

            with TemporaryFile(
                suffix="_tmpdb.db", dir=os.path.dirname(self.database_path)
            ) as tmpdb:
                callback(_("Restoring database from SQL") + "...")
                with closing(Connection(tmpdb)) as conn:
                    shell = Shell(db=conn, encoding="utf-8")
                    shell.process_command(".read " + fname.replace(os.sep, "/"))
                    conn.execute("PRAGMA user_version=%d;" % uv)

                validate_sqlite_file(tmpdb)
                require_idle_sqlite_connections(self._open_connections)
                self.close()
                try:
                    require_sqlite_replacement_ready(
                        self.database_path, self._open_connections
                    )
                    atomic_rename(tmpdb, self.database_path)
                finally:
                    self.reopen()
