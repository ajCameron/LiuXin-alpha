
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
import uuid
from contextlib import closing
from functools import partial
import pathlib

from typing import Union, Optional, Any, Sequence, Tuple, TYPE_CHECKING, Iterable, Iterator, Callable

from LiuXin_alpha.databases.maintenance.dummy_maintenance_bot import DummyMaintenanceBot
from LiuXin_alpha.utils.logging import LiuXin_print, LiuXin_warning_print

from LiuXin_alpha.databases.database_driver_plugins.SQL.macros import SQLiteDatabaseMacros
from LiuXin_alpha.databases.database_driver_plugins.SQL.custom_columns import (
    SQLiteCustomColumnsDriverMixin,
)

from LiuXin_alpha.errors import DatabaseDriverError

from LiuXin_alpha.utils.language_tools.lx_name_manip import authors_str_to_sort_str

from LiuXin_alpha.databases.maintenance import run_ta_updates

from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.utils.date import utcfromtimestamp
from LiuXin_alpha.utils.databases.apsw_shell import Shell
from LiuXin_alpha.utils.ptempfiles import TemporaryFile
from LiuXin_alpha.utils.localization import _
from LiuXin_alpha.utils.libraries.liuxin_six import user_input
from LiuXin_alpha.utils.storage.local.filenames import atomic_rename

from LiuXin_alpha.databases.database_driver_plugins.SQL.utility_mixins import SQLiteTableLinkingMixin

from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver import SQLBaseDriver, _create_new_database

# Todo: Don't do this...
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.utils import *
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.utils import _author_to_author_sort, title_sort

from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.calibre_emulation_mixin import CalibreEmulationMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.sql_execution_mixin import SQLExecutionMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.math_mixin import MathFunctionsMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.dirty_records_mixin import DirtyRecordsMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.table_names_mixin import TableNamesMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.tree_mixjn import TreeMethodsMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.metadata_mixin import MetadataMethodMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.triggers_mixin import TriggersMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.search_mixin import SearchMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.value_casting_mixin import ValueCastingMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.new_book_mixin import BookGroupMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.delete_mixin import DeleteMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.add_mixin import AddingMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.update_mixin import UpdateMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.view_mixin import ViewMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.table_creation_mixin import TableCreationMixin

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
        apsw.Connection.__init__(self, path)

        self.setbusytimeout(self.BUSY_TIMEOUT)
        self.execute("pragma cache_size=5000")
        self.execute("pragma temp_store=2")

        encoding = next(self.execute("pragma encoding"))[0]
        self.createcollation("PYNOCASE", partial(pynocase, encoding=encoding))

        self.createscalarfunction("title_sort", title_sort, 1)
        self.createscalarfunction("author_to_author_sort", _author_to_author_sort, 1)
        self.createscalarfunction("uuid4", lambda: str(uuid.uuid4()), 0)

        # Dummy functions for dynamically created filters
        self.createscalarfunction("books_list_filter", lambda x: 1, 1)
        self.createcollation("icucollate", icu_collator)

        # Legacy aggregators (never used) but present for backwards compat
        self.createaggregatefunction("sortconcat", SortedConcatenate, 2)
        self.createaggregatefunction("sortconcat_bar", partial(SortedConcatenate, sep="|"), 2)
        self.createaggregatefunction("sortconcat_amper", partial(SortedConcatenate, sep="&"), 2)
        self.createaggregatefunction("identifiers_concat", SqliteIdentifiersConcat, 2)
        self.createaggregatefunction("concat", Concatenate, 1)
        self.createaggregatefunction("aum_sortconcat", AumSortedConcatenate, 4)

    def create_dynamic_filter(self, name: str) -> None:
        """
        Register an initially empty membership filter as a scalar SQL function.

        Create a DynamicFilter with no allowed IDs and register it under the requested name
        with arity one. Its Python instance is retained by SQLite but is not returned or
        stored on this wrapper, so this method exposes no handle for changing its
        membership. Re-registering a name replaces its callback.

        Example:
            >>> conn = Connection(":memory:")
            >>> conn.create_dynamic_filter("selected_books")
            >>> conn.get("SELECT selected_books(1)")
            [(0,)]
            >>> conn.close()


        :param name: Scalar-function name passed directly to APSW, without application-level
            validation.
        :return: None; subsequent calls to the registered filter return zero until its
            hidden membership changes.
        """
        f = DynamicFilter(name)
        self.createscalarfunction(name, f, 1)

    def get(self, *args: Any, **kw: Any) -> Optional[Any]:
        """
        Execute through a new APSW cursor and fetch all rows or one scalar.

        Only the all keyword is read; it defaults to True, and other keywords are ignored.
        False mode uses the legacy cursor.next method and catches only StopIteration or
        IndexError to return None. APSW versions without that cursor method raise
        AttributeError instead, even for empty results. No explicit transaction or cursor
        cleanup is added.

        Example:
            >>> conn = Connection(":memory:")
            >>> conn.get("SELECT ? UNION ALL SELECT ?", (2, 4))
            [(2,), (4,)]
            >>> conn.close()


        :param args: Arguments passed unchanged to cursor.execute, normally SQL followed by
            optional bindings.
        :param kw: all selects all-row mode when truthy and the legacy scalar branch
            otherwise.
        :return: Fetched row list by default; first-column value or None in the legacy
            scalar branch when supported.
        """
        ans = self.cursor().execute(*args)
        if kw.get("all", True):
            return ans.fetchall()
        try:
            return ans.next()[0]
        except (StopIteration, IndexError):
            return None

    def execute(self, sql: str, bindings: Optional[tuple[str]] = None) -> Any:
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


        :param sql: SQL text interpreted by APSW; use placeholders for bound data.
        :param bindings: Optional sequence or mapping accepted by APSW; forwarded unchanged
            despite the narrow tuple annotation.
        :return: The result cursor returned by APSW execute.
        """
        cursor = self.cursor()
        return cursor.execute(sql, bindings)

    def executemany(self, sql: str, sequence_of_bindings: Sequence[Optional[tuple[str]]]) -> Any:
        """
        Execute binding rows within an APSW connection transaction context.

        Enter the connection context before calling cursor.executemany and leave it before
        returning the cursor. Normal context exit commits or releases its savepoint;
        exceptions inside the context roll back. If a result-producing statement leaves lazy
        cursor work, that later iteration occurs outside this wrapper context. Inputs and
        execution errors are passed through to APSW.

        Example:
            >>> conn = Connection(":memory:")
            >>> _ = conn.execute("CREATE TABLE sample (value INTEGER)")
            >>> _ = conn.executemany("INSERT INTO sample VALUES (?)", [(2,), (4,)])
            >>> conn.get("SELECT SUM(value) FROM sample")
            [(6,)]
            >>> conn.close()


        :param sql: Statement to repeat with each binding row.
        :param sequence_of_bindings: Iterable of parameter sequences or mappings accepted by
            cursor.executemany.
        :return: The APSW cursor result; the surrounding connection context has already
            exited.
        """
        with self:  # Disable autocommit mode, for performance
            return self.cursor().executemany(sql, sequence_of_bindings)


class SQLite_Connection(sqlite3.Connection):
    """
    Extend sqlite3.Connection with all-row, single-row and scalar fetching.

    The get and get_row helpers forward positional arguments to execute and interpret
    only the all keyword. They do not commit or close the connection. Direct
    construction uses sqlite3 defaults; driver-specific converters, callbacks and
    pragmas are installed by DatabaseDriver.get_connection instead.

    Example:
        >>> conn = SQLite_Connection(":memory:")
        >>> conn.get_row("SELECT 3, 4", all=False)
        (3, 4)
        >>> conn.close()
    """
    def get(self, *args: Any, **kw: Any) -> Optional[Any]:
        """
        Execute one statement and fetch all rows or the first scalar.

        With a truthy all option, return fetchall(). Otherwise return column zero of
        fetchone(), or None when no row is available. A selected SQL NULL also becomes
        None in scalar mode. Positional bindings pass directly to sqlite3; other keyword
        options are ignored and no transaction is committed.

        The OperationalError handler accesses the obsolete exception.message attribute.
        With standard Python 3 sqlite3 exceptions this raises AttributeError, masking
        the original error instead of producing the intended diagnostic. Other execution
        and fetch errors propagate.

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


        :param args: SQL statement followed, when needed, by its sequence or mapping of
            bound values.
        :param kw: Only all is consulted: defaults to True; any false value selects
            scalar mode.
        :return: A list of rows by default, or the first column of the first row in
            scalar mode; None for an absent scalar row.
        """
        try:
            ans = self.execute(*args)
        except sqlite3.OperationalError as e:
            err_str = "Couldn't execute - operational error\n"
            err_str += "args: {}\n".format(args)
            err_str += "error message: {}\n".format(e.message)
            err_str += "errors args: {}\n".format(e.args)
            raise sqlite3.OperationalError(err_str)
        if not kw.get("all", True):
            ans = ans.fetchone()
            if not ans:
                ans = [None]
            return ans[0]
        return ans.fetchall()

    def get_row(self, *args: Any, **kw: Any) -> Optional[Any]:
        """
        Execute one statement and fetch all rows or one complete row.

        A truthy all option returns fetchall(); a false option returns fetchone(), with
        None for no row. An SQL NULL remains part of the returned row. Extra keyword
        options are ignored, while positional bindings go directly to execute. The
        helper neither commits nor closes the connection.

        As in get, the OperationalError handler reads exception.message, which masks
        standard Python 3 SQLite operational errors with AttributeError. Other execution
        and fetch errors propagate.

        Example:
            >>> conn = SQLite_Connection(":memory:")
            >>> conn.get_row("SELECT ?, NULL", (5,), all=False)
            (5, None)
            >>> conn.get_row("SELECT 5 WHERE 0", all=False) is None
            True
            >>> conn.get_row("SELECT 5 WHERE 0")
            []
            >>> conn.close()


        :param args: SQL statement followed by optional positional bindings accepted by
            execute.
        :param kw: Only all is consulted: defaults to True; any false value selects one-
            row mode.
        :return: A list of rows by default, or one row when all is false; None if that
            single row is absent. Row shape follows the connection row factory.
        """
        try:
            ans = self.execute(*args)
        except sqlite3.OperationalError as e:
            err_str = "Couldn't execute - operational error\n"
            err_str += "args: {}\n".format(args)
            err_str += "error message: {}\n".format(e.message)
            err_str += "errors args: {}\n".format(e.args)
            raise sqlite3.OperationalError(err_str)
        if not kw.get("all", True):
            ans = ans.fetchone()
            if not ans:
                return None
            return ans
        return ans.fetchall()


class DatabaseDriver(
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
    TableCreationMixin):
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
            db_metadata,
            db: Optional["DatabaseAPI"] = None,
            set_conn: bool = True,
            dirty_records_queue=None) -> None:
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
            >>> driver = DatabaseDriver(metadata, set_conn=False, dirty_records_queue=queue)
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

        # Used to keep track of the number of instructions executed on this database, so database activity can be
        # monitored
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
        self.maintainer_callback = DummyMaintenanceBot()

        # Parse some of the preference values which affect the behavior of the database

        # Store a connection to be used for locking
        if set_conn:
            self.conn = self.get_connection()
        else:
            self.conn = None

        # This will be usefully set when the database starts up
        self.dirty_records_queue = dirty_records_queue

    # Todo: This needs to be terminated during shutdown
    # Todo: This also needs to be written
    # Todo: May make no sense in a WEMI context
    def direct_run_ta_update(self, ta_row_id: int) -> None:
        """
        Launch the legacy aggregate-update thread when its preference enables it.

        Only the exact string "true" for run_ta_update_after_each_change dispatches
        run_ta_updates with a one-element ID list and this driver. The helper starts a
        daemon thread; this method does not wait for completion. "false" and other
        values, including boolean True, cause no dispatch. Preference lookup and launch
        errors propagate; background failures are not returned to the caller.

        Example:
            With the preference set to the string "true", this schedules the update:
            >>> driver.direct_run_ta_update(42)  # doctest: +SKIP


        :param ta_row_id: Aggregate/title row ID passed unchanged in the worker input
            list; not validated here.
        :return: None, regardless of whether an update thread is launched.
        """
        if preferences["run_ta_update_after_each_change"] == "true":
            run_ta_updates(
                [
                    ta_row_id,
                ],
                self,
            )
        elif preferences["run_ta_update_after_each_change"] == "false":
            pass
        else:
            pass

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - CONNECTION METHODS TO THE DATABASE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    # Internal, implementation dependant method. Should not be exposed to the outside
    def get_connection(self) -> "sqlite3.Connection":
        """
        Open, configure and track a fresh SQLite connection.

        Register set/list/dict adapters and PYSET/PYLIST/PYDICT/DATE converters
        globally, enable callback tracebacks, then open database_path with declared-type
        parsing and check_same_thread=False. Every call opens a distinct connection,
        including an independent database for the literal :memory: path. Existing
        primary handles are not replaced.

        Request foreign-key enforcement and warn if SQLite does not report it enabled.
        Install aggregates, collations, regex/tree/sort/UUID helpers, an always-true
        books_list_filter, maintenance callbacks and a progress callback after every
        virtual-machine instruction. The progress handler prints the initial counter
        value, zero. DIRTY_RECORD and NEW_DIRTY_RECORD look up the
        current maintainer on each invocation; DIRTY_INTERLINK_RECORD binds the
        maintainer method at connection creation. Cross-thread access still requires
        caller coordination.

        Best-effort LANGUAGE_ID registration may seed and lock an existing FRBR
        languages table, committing changes; exceptions from that registration are
        suppressed. Other setup failures propagate without guaranteed cleanup. The
        opening OperationalError handler reads obsolete exception.message, so standard
        Python 3 errors become AttributeError before its intended DatabaseDriverError
        can be raised.

        Example:
            >>> driver = DatabaseDriver({"database_path": ":memory:"}, set_conn=False)
            >>> conn = driver.get_connection()
            0
            >>> conn.get("PRAGMA foreign_keys", all=False)
            1
            >>> conn.get_row("SELECT 'abc' REGEXP 'b', books_list_filter(999)", all=False)
            (1, 1)
            >>> driver.conn is None and conn in driver._open_connections
            True
            >>> driver.close()


        :return: An open SQLite_Connection tracked for inherited driver.close; callers
            may close it earlier and must manage their transactions.
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

        try:
            conn = SQLite_Connection(
                self.database_path,
                detect_types=sqlite3.PARSE_DECLTYPES,
                check_same_thread=False,
            )

            # Aggregator allows sets of Unicode to be stored directly as the result of queries
            conn.create_aggregate("pyset", 1, PySetAggregate)
            conn.create_aggregate("sortag", 1, SortAggregate)
            conn.create_aggregate("pylist", 1, PyListAggregate)
            # The progress handler used to monitor the SQLite virtual machine must be added to the conn
            conn.set_progress_handler(self.simple_print_progress_handler, 1)

        except sqlite3.OperationalError as e:
            error_message = e.message
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
        def regexp(expr, item):
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

        conn.create_function("REGEXP", 2, regexp)

        # Add the TREE_AGGREGATOR to the connection - allows for string representation of the position of a row in a
        # tree
        conn.create_function("TREE_AG", 3, self.direct_get_tree_aggregation_str)

        # Adds a function which creates sort strings from strings of authors
        # Adds again under a different name for close calibre compatibility
        conn.create_function("AUTHORS_SORT", 1, authors_str_to_sort_str)

        # Adds a function which will be used to update the title_aggregate table in a separate worker process
        conn.create_function("TA_UPDATE", 1, self.direct_run_ta_update)

        # More generally, add a function which will callback to the maintenance bot to tell it that particular row in
        # a table has changed and might need attention
        conn.create_function("DIRTY_RECORD", 2, lambda table, row_id: self.maintainer_callback.dirty_record(table, row_id))
        conn.create_function("DIRTY_INTERLINK_RECORD", 5, self.maintainer_callback.dirty_interlink_record)
        conn.create_function("NEW_DIRTY_RECORD", 2, lambda table, row_id: self.maintainer_callback.new_dirty_record(table, row_id))

        # calibre - functions included here for compatibility
        conn.create_function("title_sort", 1, title_sort)
        conn.create_function("author_to_author_sort", 1, _author_to_author_sort)
        conn.create_function("uuid4", 0, lambda: str(uuid.uuid4()))

        # calibre - Dummy functions for dynamically created filters
        conn.create_function("books_list_filter", 1, lambda x: 1)

        conn.create_collation("icucollate", icu_collator)

        # calibre aggregate functions, included here for compatibility
        conn.create_aggregate("aum_sortconcat", 4, SqliteAumSortedConcatenate)

        conn.create_aggregate("concat", 1, Concatenate)
        conn.create_aggregate("concat_error", 1, StupidConcatenate)

        conn.create_aggregate("identifiers_concat", 2, IdentifiersConcat)

        conn.create_aggregate("sortconcat", 2, SqliteSortedConcatenate)
        conn.create_aggregate("sortconcat_bar", 2, partial(SqliteSortedConcatenate, sep="|"))
        conn.create_aggregate("sortconcat_amper", 2, partial(SqliteSortedConcatenate, sep="&"))

        # Register the custom collators (ported from calibre, for compatibility)
        encoding = next(conn.execute("PRAGMA encoding"))[0]
        conn.create_collation("PYNOCASE", partial(pynocase, encoding=encoding))

        # FRBR constants convenience: resolve language tokens to `languages.language_id`.
        # Safe no-op on non-FRBR databases.
        try:
            from LiuXin_alpha.utils.language_tools import register_language_id_sql_function

            register_language_id_sql_function(conn, function_name="LANGUAGE_ID", ensure_seeded=True)
        except Exception:
            pass

        return self._register_open_connection(conn)

    def last_modified(self) -> "datetime.date":
        """
        Read the configured database file modification time in UTC.

        Stat database_path on each call and convert st_mtime with utcfromtimestamp. This
        is the main file timestamp, so it may not reflect changes still held in a WAL or
        an uncommitted transaction. The result is a timezone-aware datetime despite the
        legacy date annotation. Missing-file and other stat errors propagate; no
        connection is opened.

        Example:
            >>> import tempfile
            >>> with tempfile.NamedTemporaryFile() as file:
            ...     driver = DatabaseDriver({"database_path": file.name}, set_conn=False)
            ...     modified = driver.last_modified()
            >>> modified.utcoffset().total_seconds()
            0.0


        :return: UTC datetime derived from the current file mtime, using the shared
            timestamp converter.
        """
        return utcfromtimestamp(os.stat(self.database_path).st_mtime)

    # Todo: Needs to actually be written.
    def last_modified_epoch_k(self) -> int:
        """
        Reserve the legacy millisecond modification-time hook.

        The method currently has only a docstring body: it does not stat the file, compute
        an epoch or inspect the driver. It returns None despite its int annotation.

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
        """
        Yield SQL dump lines from the existing primary connection.

        The generator enters the connection transaction context when iteration begins
        and delegates to iterdump. Exhausting it successfully commits any pending
        transaction; exceptional exit, including closing a suspended generator, rolls it
        back. It does not close the connection. It requires a usable primary conn and
        does not open one when initialization was deferred. SQLite dump scope applies:
        this is not a copy of connection-local callbacks or every pragma.

        Example:
            >>> driver = DatabaseDriver({"database_path": ":memory:"})
            0
            >>> _ = driver.conn.execute("CREATE TABLE sample (value TEXT)")
            >>> _ = driver.conn.execute("INSERT INTO sample VALUES (?)", ("saved",))
            >>> lines = list(driver.sql_dump())
            >>> any("saved" in line for line in lines)
            True
            >>> driver.conn.in_transaction
            False
            >>> _ = driver.conn.execute("INSERT INTO sample VALUES ('discarded')")
            >>> dump = driver.sql_dump()
            >>> next(dump)
            'BEGIN TRANSACTION;'
            >>> dump.close()
            >>> driver.conn.get("SELECT COUNT(*) FROM sample", all=False)
            1
            >>> driver.close()


        :return: Iterator of SQL text lines; SQL execution and transaction effects occur
            during iteration.
        """
        with self.conn:
            for line in self.conn.iterdump():
                yield line

    def dump_and_restore(
            self,
            callback: Callable[[str, ], None] = lambda x: x,
            sql: Optional[str] = None) -> None:
        """
        Rebuild the database file through the bundled APSW shell.

        Read user_version from the primary stdlib connection, then write a complete APSW
        .dump to a temporary UTF-8 SQL file, optionally preceded by extra SQL. Restore with
        .read into a temporary file beside database_path and restore user_version afterward.
        Connection contexts close both APSW handles. The shell accepts SQL and dot commands,
        so the prefix must be trusted. The .read filename is appended without quoting;
        scratch paths containing spaces or shell-token quoting characters can fail to parse.

        After restoration, close all tracked driver handles, atomically replace the original
        file and attempt reopen even if renaming fails. A reopen failure can mask the rename
        error. Reopen uses stdlib SQLite again and does not refresh schema caches. There is
        no backup or exclusive-use lock. Commit pending writes and coordinate other
        connections first: the separate dump connection cannot see uncommitted work and
        closing handles can discard it.

        Callback, decoding, shell and filesystem errors propagate. Temporary-file contexts
        remove their files; failures before replacement leave the original file in place,
        while replacement cannot be rolled back by this method.

        Example:
            With committed data and a shell-compatible scratch path:
            >>> driver.dump_and_restore(callback=print, sql="CREATE TABLE restore_note (value TEXT);")  # doctest: +SKIP


        :param callback: Callable receiving localized dumping and restoring messages; return
            values are ignored. None selects an identity callback.
        :param sql: Optional trusted prefix: bytes are decoded as UTF-8, other non-None
            values converted with str; this augments the full dump.
        :return: None; replaces the file and reopens the primary stdlib connection on
            success.
        """
        if callback is None:

            def callback(x):
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

        uv = int(self.user_version)

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

            with TemporaryFile(suffix="_tmpdb.db", dir=os.path.dirname(self.database_path)) as tmpdb:
                callback(_("Restoring database from SQL") + "...")
                with closing(Connection(tmpdb)) as conn:
                    shell = Shell(db=conn, encoding="utf-8")
                    shell.process_command(".read " + fname.replace(os.sep, "/"))
                    conn.execute("PRAGMA user_version=%d;" % uv)

                self.close()
                try:
                    atomic_rename(tmpdb, self.database_path)
                finally:
                    self.reopen()
