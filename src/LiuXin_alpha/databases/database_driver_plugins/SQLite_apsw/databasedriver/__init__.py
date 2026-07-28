
"""
Contains the logic to actually provide a SQLite database driver - using apsw as the acess backend.
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
    Uses apsw to provide a connection to the database.
    """

    BUSY_TIMEOUT = 10000  # milliseconds

    def __init__(self, path: Union[str, pathlib.Path]) -> None:
        """
        Constructor.

        :param path:
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
        Create and register the dymanic filters on the database.

        :param name:
        :return:
        """
        f = DynamicFilter(name)
        self.createscalarfunction(name, f, 1)

    def get(self, *args: Any, **kw: Any) -> Optional[Any]:
        """
        Front end for the cursor get - uses next to pull a single result.

        :param args:
        :param kw:
        :return:
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
        Allows direct execution on the database through the cursor.

        :param sql:
        :param bindings:
        :return:
        """
        cursor = self.cursor()
        return cursor.execute(sql, bindings)

    def executemany(self, sql: str, sequence_of_bindings: Sequence[Optional[tuple[str]]]) -> Any:
        """
        Execute many statements on the database through the cursor.

        :param sql:
        :param sequence_of_bindings:
        :return:
        """
        with self:  # Disable autocommit mode, for performance
            return self.cursor().executemany(sql, sequence_of_bindings)


class SQLite_Connection(sqlite3.Connection):
    """
    Add some helper methods around the SQLite connection.
    """
    def get(self, *args: Any, **kw: Any) -> Optional[Any]:
        """
        Helper method for retrieving results from a database.

        :param args:
        :param kw:
        :return:
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
        Helper method designed to retrieve entire rows from the database.

        :param args:
        :return:
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
    Represents a collection of all the methods needed to interface with an actual database.

    Any method starting with the word direct is intended to be directly exposed to the outside world.
# Ideally only these should be present (this is intended to contain only the bare minimum required to interact with the
# actual, on disk database.
# NOTE - Using the variable substitution features in SQLite3 provides much better results than anything home baked for
# preventing SQL injection attacks and escaping strings properly. Use this instead.
    """

    def __init__(
            self,
            db_metadata,
            db: Optional["DatabaseAPI"] = None,
            set_conn: bool = True,
            dirty_records_queue=None) -> None:
        """
        Initializing the class with db_metadata.

        Which is an object assumed to have a dictionary like interface which provides all the necessary fields to
        connect to a database of the given type.
        This DatabaseDriver (SQLite - apsw backed) requires the database_path. That's about it.
        :param db_metadata:
        :param db: The database this process is driving. Hopefully infinite recursion will not result.
        :param set_conn: Set the globally used connection for the class
        :return:
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
        Runs the separate worker process which updates the titles_aggregate table after the basic update has occurred.

        :param ta_row_id:
        :return:
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
        Method which creates a connection with foreign key support. Returns a connection.
        :return conn: A connection to the database
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
        Return last modified time as a UTC datetime object

        :return:
        """
        return utcfromtimestamp(os.stat(self.database_path).st_mtime)

    # Todo: Needs to actually be written.
    def last_modified_epoch_k(self) -> int:
        """
        The epoch in miliseconds since the UNIX epoch.

        :return:
        """

    # Use with extreme caution - no safeguards
    def shell(self) -> None:
        """
        Drops you into an SQLite shell.

        Be careful. There are no safeguards.
        :return:
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
        Dump the current database out to a series of sql statements.

        :return:
        """
        with self.conn:
            for line in self.conn.iterdump():
                yield line

    def dump_and_restore(
            self,
            callback: Callable[[str, ], None] = lambda x: x,
            sql: Optional[str] = None) -> None:
        """
        Dump the database - and all the information in it - to a series SQL statements.

        :param callback: Report the progress of the dump.
        :param sql: These statements will be written into the start of the file before the data is saved to it - so they
                    will be executed before the rest as the database is restored.
        :return:
        """
        if callback is None:

            def callback(x):
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
