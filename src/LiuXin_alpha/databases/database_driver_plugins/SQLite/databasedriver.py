# This class is intended to be linked to a specific instance of a DatabasePing.
# The internals of the database are deliberately separated here, to make changes more directly without influencing the
# DatabasePing class itself
# Also should allow live switching of the DatabaseDriver in and out (i.e. if you want to switch from SQLite to SQL -
# separating the DatabasePing logic and the Driver logic would seem to make sense).

from __future__ import print_function

import apsw
import codecs
import os
import pprint
import random
import re
import shutil
import sqlite3
import uuid
from contextlib import closing
from copy import deepcopy
from functools import partial

from six import iterkeys
from six import iteritems

from LiuXin.utils.general_ops.io_ops import LiuXin_print
from LiuXin.utils.general_ops.io_ops import LiuXin_debug_print
from LiuXin.utils.general_ops.io_ops import LiuXin_warning_print
from LiuXin.utils.general_ops.io_ops import y_n_input

from LiuXin.constants import VERBOSE_DEBUG

from LiuXin.databases.drivers.SQLite.database_generator.database_generator import (
    create_new_database,
)
from LiuXin.databases.drivers.SQLite.macros import SQLiteDatabaseMacros
from LiuXin.databases.drivers.SQLite.custom_columns import (
    SQLiteCustomColumnsDriverMixin,
)

from LiuXin.exceptions import LogicalError
from LiuXin.exceptions import DatabaseDriverError
from LiuXin.exceptions import RowIntegrityError
from LiuXin.exceptions import InputIntegrityError
from LiuXin.exceptions import DatabaseIntegrityError

from LiuXin.folder_stores.file_manager.LX_name_manip import authors_str_to_sort_str
from LiuXin.folder_stores.file_manager import path_ok

from LiuXin.databases.maintenance_bot import run_ta_updates
from LiuXin.databases.backup import backup_local_file

from LiuXin.preferences import preferences

from LiuXin.utils.calibre import isbytestring, force_unicode
from LiuXin.utils.general_ops.language_tools import plural_singular_mapper
from LiuXin.utils.icu import sort_key
from LiuXin.utils.logger import default_log
from LiuXin.utils.ptempfiles import get_scratch_folder
from LiuXin.utils.date import utcfromtimestamp
from LiuXin.utils.apsw_shell import Shell
from LiuXin.utils.ptempfiles import TemporaryFile
from LiuXin.utils.localization import _
from LiuXin.utils.lx_libraries.liuxin_six import force_cmp
from LiuXin.utils.lx_libraries.liuxin_six import user_input
from LiuXin.utils.filenames import atomic_rename

from LiuXin.metadata import author_to_author_sort, title_sort

from LiuXin.databases.drivers.SQLite.utility_mixins import SQLiteTableLinkingMixin

# Py2/Py3 compatibility layer
from LiuXin.utils.lx_libraries.liuxin_six import six_unicode

from past.builtins import basestring


class Connection(apsw.Connection):

    BUSY_TIMEOUT = 10000  # milliseconds

    def __init__(self, path):
        apsw.Connection.__init__(self, path)

        self.setbusytimeout(self.BUSY_TIMEOUT)
        self.execute("pragma cache_size=5000")
        self.execute("pragma temp_store=2")

        encoding = self.execute("pragma encoding").next()[0]
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

    def create_dynamic_filter(self, name):
        f = DynamicFilter(name)
        self.createscalarfunction(name, f, 1)

    def get(self, *args, **kw):
        ans = self.cursor().execute(*args)
        if kw.get("all", True):
            return ans.fetchall()
        try:
            return ans.next()[0]
        except (StopIteration, IndexError):
            return None

    def execute(self, sql, bindings=None):
        cursor = self.cursor()
        return cursor.execute(sql, bindings)

    def executemany(self, sql, sequence_of_bindings):
        with self:  # Disable autocommit mode, for performance
            return self.cursor().executemany(sql, sequence_of_bindings)


class DummyMaintenanceBot(object):
    """
    Is not a maintenance bot - but presents some of the same methods.
    """

    def __init__(self):
        pass

    def dirty_record(self, table, row_id):
        pass

    def new_dirty_record(self, table, row_id):
        pass

    def dirty_interlink_record(self, update_type, table1, table2, table1_id, table2_id):
        pass


class SQLite_Connection(sqlite3.Connection):
    def get(self, *args, **kw):
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

    def get_row(self, *args, **kw):
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


# Any method starting with the word direct is intended to be directly exposed to the outside world.
# Ideally only these should be present (this is intended to contain only the bare minimum required to interact with the
# actual, on disk database.
# NOTE - Using the variable substitution features in SQLite3 provides much better results than anything home baked for
# preventing SQL injection attacks and escaping strings properly. Use this instead.
class DatabaseDriver(SQLiteCustomColumnsDriverMixin, SQLiteTableLinkingMixin):
    """
    Represents a collection of all the methods needed to interface with an actual database.
    """

    def __init__(self, db_metadata, db=None, set_conn=True, dirty_records_queue=None):
        """
        Initializing the class with db_metadata. Which is an object assumed to have a dictionary like interface which
        provides all the necessary fields to connect to a database of the given type.
        This DatabaseDriver (SQLite) requires the database_path. That's about it.
        :param db_metadata:
        :param db: The database this process is driving. Hopefully infinite recursion will not result.
        :param set_conn: Set the globally used connection for the class
        :return:
        """
        self.db_metadata = db_metadata
        self.database_path = db_metadata["database_path"]
        self.db = db

        self.macros = SQLiteDatabaseMacros(db=self.db)

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

    def exists(self):
        """
        Checks to see if the database file exists - returns True if it does, false if it doesn't.
        :return:
        """
        return os.path.exists(self.database_path)

    def make_scratch(self):
        """
        Makes a scratch copy of the database - shifts over to using that instead of the main one.
        :return:
        """
        scratch_folder = get_scratch_folder()
        scratch_db_path = os.path.join(scratch_folder, "scratch.db")
        shutil.copyfile(src=self.database_path, dst=scratch_db_path)
        self.database_path = scratch_db_path

    def _zero_prop_cache(self):
        """
        Zero any cached properties - used when significant changes have.may have been made to the database.
        :return:
        """
        self.tables = None
        self.tables_and_columns = None
        self.categorized_tables = None
        self.all_column_names = set()

        self.locations = None

        try:
            self.db.refresh_db_metadata()
        except:
            pass

    def close(self):
        """
        Shutdown the connection to the database - but leave the drive class in existence so it can be re-opened.
        :return:
        """
        self.conn.close()

    def refresh(self):
        """
        Refreshes the database - zeros all cached objects and connects again.
        :return:
        """
        self.conn = self.get_connection()
        self._zero_prop_cache()

    def reopen(self):
        """
        Re-opes the connection to the database.
        :return:
        """
        self.conn = self.get_connection()

    def direct_backup(self, path=None):
        """
        Backup the DatabasePing.
        :param path: The path to backup the database to - if none is provided, autogenerated
        :return:
        """
        # Acquire a conn object - use it to lock the DatabasePing
        conn = self.get_connection()
        with conn:

            # Preform the backup
            backup_status = backup_local_file(self.database_path, override_path=path)
            if backup_status:
                info_str = "DatabasePing backup successfully complete.\n"
                default_log.log_variables(
                    info_str,
                    "INFO",
                    ("database_path", self.database_path),
                    ("database_backup_path", backup_status),
                )
            else:
                wrn_str = "DatabasePing backup failed.\n"
                default_log.log_variables(
                    wrn_str,
                    "WARN",
                    ("database_path", self.database_path),
                    ("database_backup_path", backup_status),
                )
                raise DatabaseDriverError(wrn_str)

    def direct_self_delete(self):
        """
        Delete the on_disc database file.
        :return:
        """
        # Lock the database. Delete the SQLite file.
        conn = self.get_connection()
        with conn:

            # Check that the file can be accessed and the process has the privilages to run the delete
            if not path_ok(self.database_path):
                err_str = "DatabasePing file cannot be accessed for delete.\n"
                err_str += "database_file_path: {}\n".format(self.database_path)
                default_log.error(err_str)
                raise DatabaseDriverError(err_str)

            # Remove the database file
            os.remove(self.database_path)

            # Check that the delete has gone through i.e. the path no longer exists.
            if os.path.exists(self.database_path):
                err_str = "DatabasePing cannot be deleted - process failed silently.\n"
                err_str += "database_path: {}\n".format(self.database_path)
                raise DatabaseDriverError(err_str)

        # With the database gone the caches should also be emptied
        self._zero_prop_cache()

    def simple_print_progress_handler(self):
        """
        The most basic progress handler - prints the number of events every hundred million events.
        :return:
        """
        if self.event_count % 100000000 == 0:
            LiuXin_print(self.event_count)
            self.event_count += 1
        else:
            self.event_count += 1

    def direct_run_ta_update(self, ta_row_id):
        """
        Runs the separate worker process which updates the titles_aggregate table after the basic update has occured.
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
    def get_connection(self):
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
            conn = SQLite_Connection(self.database_path, detect_types=sqlite3.PARSE_DECLTYPES)

            # Aggregator allows sets of unicode to be stored directly as the result of queries
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
        conn.create_function("TREE_AG", 3, self.tree_aggregator)

        # Adds a function which creates sort strings from strings of authors
        # Adds again under a different name for close calibre compatibility
        conn.create_function("AUTHORS_SORT", 1, authors_str_to_sort_str)

        # Adds a function which will be used to update the title_aggregate table in a separate worker process
        conn.create_function("TA_UPDATE", 1, self.direct_run_ta_update)

        # More generally, add a function which will callback to the maintenance bot to tell it that particular row in
        # a table has changed and might need attention
        conn.create_function("DIRTY_RECORD", 2, self.maintainer_callback.dirty_record)
        conn.create_function("DIRTY_INTERLINK_RECORD", 4, self.maintainer_callback.dirty_interlink_record)
        conn.create_function("NEW_DIRTY_RECORD", 2, self.maintainer_callback.new_dirty_record)

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
        encoding = conn.execute("PRAGMA ENCODING").next()[0]
        conn.create_collation("PYNOCASE", partial(pynocase, encoding=encoding))

        return conn

    def last_modified(self):
        """
        Return last modified time as a UTC datetime object
        :return:
        """
        return utcfromtimestamp(os.stat(self.database_path).st_mtime)

    # Use with extreme caution - no safeguards
    def shell(self):
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

    # Todo: This should be something like "execute sql script" - to distinguish it from the execute method in the conn
    def executescript(self, script):
        """
        Allows arbitrary scripts to be executed on the database.
        Try not to shoot yourself in the foot.
        :param script: This will be executed directly on the database.
        :return:
        """
        conn = self.get_connection()
        conn.executescript(script)
        conn.close()

    def execute_sql(self, sql, parameters=None):
        """
        Execute the given sql using a new conn, which will be closed after the execution.
        :param sql:
        :param parameters:
        :return:
        """
        conn = self.get_connection()
        last_row_id = conn.execute(sql, parameters).lastrowid
        conn.commit()
        return last_row_id

    def sql_dump(self):
        """
        Dump the current database out to a series of sql statements.
        :return:
        """
        with self.conn:
            for line in self.conn.iterdump():
                yield line

    def dump_and_restore(self, callback=lambda x: x, sql=None):
        """
        Dump the database - and all the information in it - to a series of
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

            if sql is None:
                callback(_("Dumping database to SQL") + "...")
                with codecs.open(fname, "wb", encoding="utf-8") as buf:
                    aspw_conn = Connection(path=self.database_path)
                    shell = Shell(db=aspw_conn, stdout=buf)
                    shell.process_command(".dump")
            else:
                with open(fname, "wb") as buf:
                    buf.write(sql if isinstance(sql, bytes) else sql.encode("utf-8"))

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

    @property
    def user_version(self):
        for row in self.conn.execute("pragma user_version;"):
            return row[0]

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - DB CREATION METHODS

    def direct_create_new_database(self):
        """
        Creates a new database using the SQL and other instructions present in the database_generator
        :return None:
        """
        if not os.path.exists(os.path.dirname(self.database_path)):
            os.makedirs(os.path.dirname(self.database_path))

        conn = self.get_connection()
        create_new_database(conn)
        conn.commit()
        conn.close()

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - DB METADATA METHODS

    # Either uses the data from self.tables_and_columns, or gets the data while populating it
    def direct_get_tables(self, force_refresh=False):
        """
        Returns a index of the names of all tables in the database.
        :param force_refresh: Force the driver to introspect the database again
        :return:
        """
        if force_refresh:
            self.tables = None
            self.conn = self.get_connection()

        if self.tables is None:
            stmt = "SELECT name FROM sqlite_master WHERE type = 'table';"
            processed_return = []
            for row in self.conn.execute(stmt):
                processed_return.append(row[0])

            self.tables = processed_return
            return processed_return
        else:
            return self.tables

    def direct_get_column_headings(self, table):
        """
        Gets an index of column headings for the given table. Tries to use the cached version - falls back on direct
        access if that fails.
        :param table:
        :return column_headings:
        """
        if self.tables_and_columns is None:
            tables_and_columns = self.direct_get_tables_and_columns()
            try:
                return tables_and_columns[table]
            except KeyError:
                raise InputIntegrityError("table {} not found".format(table))

        else:
            try:
                return self.tables_and_columns[table]
            except KeyError:
                raise InputIntegrityError("table {} not found".format(table))

    def direct_get_tables_and_columns(self):
        """
        Returns a dictionary keyed by the table name with the column headings as the values.
        :return table_and_columns:
        """
        # If the information is already cached, returning it. If not generating it, then returning it
        if self.tables_and_columns is not None:
            return self.tables_and_columns

        self.tables_and_columns = dict()
        tables = self.direct_get_tables()
        conn = self.get_connection()
        c = conn.cursor()
        for table in tables:
            stmt = "PRAGMA table_info({})".format(table)
            headings = []
            for row in c.execute(stmt):
                headings.append(row[1])
            self.tables_and_columns[table] = headings
        conn.close()

        return self.tables_and_columns

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - TABLE CREATION METHODS
    # Todo: Need a way to change the data type of the default column - also the data type of any additional columns created
    # Todo: Pull the "new" out of the name - that's implcit
    # Todo: Need a way to designate this new table "custom"
    def direct_create_new_main_table(
        self,
        table_name,
        column_headings=None,
        index_on="all",
        default_datatype="TEXT",
        default_unique=False,
    ):
        """
        Create a new main table on the database.

        :param table_name: Name for the new main table (please obey the naming scheme). Trying to create a table with a
                           name the same as that of another in the database)

        :param column_headings: Columns names (in the final table the name of the table _ column name.
                                The final table with have additional datestamp and scratch columns.
                                Columns headings should be provided in the form of a dictionary (optionally ordered)
                                Keyed with the name of the column and valued with the datatype for that column.

        :param index_on: The columns to also create indexes for - defaults to 'all' - which will generate an index for
                         all the requested custom columns

        :param default_datatype: The default datatype what will be used if no other is provided. Defaults to txt.



        :return:
        """
        table_col = plural_singular_mapper(table_name)

        indices = []

        # TABLE PREAMBLE

        table_comment = """
-- -----------------------------------------------------
-- Table `{0}`
-- -----------------------------------------------------
""".format(
            table_name
        )

        table_head = """
        CREATE TABLE IF NOT EXISTS `{0}` (
    `{1}_id` INTEGER PRIMARY KEY,

        """.format(
            table_name, table_col
        )

        # COLUMN CONTENT
        if column_headings is None:

            # - In the case where the column headings are None, then generate the default column headings
            table_columns = """
        `{table_col}` {datatype} NULL,
            """.format(
                table_name=table_name, table_col=table_col, datatype=default_datatype
            )

            if index_on == "all":

                default_col_index = "CREATE INDEX {0}_default_col_index ON {0} ({1});".format(table_name, table_col)
                indices.append(default_col_index)

            else:

                raise NotImplementedError

        else:

            # - Process the columns headings object to produce the requested headings
            col_template = """
        `{0}_{1}` {2} NULL,            
            """.format(
                table_col, "{0}", "{1}"
            )

            additional_columns = []
            for col in column_headings:

                try:
                    additional_columns.append(col_template.format(col, column_headings[col]["datatype"]))
                except KeyError:
                    # If no datatype is present in the specifications dict, use the default
                    additional_columns.append(col_template.format(col, default_datatype))

            table_columns = "\n".join(additional_columns)

        # TABLE FINISHING
        table_tail = """

    `{1}_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,

    `{1}_scratch` TEXT NULL);
        """.format(
            table_name, table_col
        )

        table_sqlite = table_comment + table_head + table_columns + table_tail

        full_script = [
            table_sqlite,
        ]
        full_script.extend(indices)

        # # Index for the custom columns
        # assert index_on == "all", "Cannot but index on all custom columns"
        # default_col_index = "CREATE INDEX {0}_default_col_index ON {0} ({1});".format(table_name, table_col)
        # full_script.append(default_col_index)

        self.executescript("\n".join(full_script))

        self._zero_prop_cache()

    # Todo: To driver base class
    def direct_get_column_name(self, table_name):
        """
        Return a column name for the given table name - just takes the singular form of the table name,
        :param table_name:
        :return:
        """
        return plural_singular_mapper(table_name)

    def direct_unlink_main_tables(self, primary_table, secondary_table):
        """
        Break an existing link between two main tables. The link will be broken regardless of type.
        :param primary_table:
        :param secondary_table:
        :return:
        """
        table_name, column_name = self._get_link_table_name_col_name(primary_table, secondary_table)

        unlink_sqlite = """
        DROP TABLE {};
        """.format(
            table_name
        )

        self.execute_sql(unlink_sqlite)

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # -

    def direct_add_simple_row_dict(self, row_dict):
        """
        Takes a single row in the form of a dictionary and adds the values to the database.
        :param row_dict:
        :return :
        """
        target_table = self.__identify_table_from_row(row_dict)

        # Assembling a list of placeholders of the form ?,?,?
        values_placeholders = ""
        for i in range(len(row_dict)):
            values_placeholders += "?,"
        values_placeholders = values_placeholders[:-1]

        # These are the column headings values will be inserted into
        column_headings = row_dict.keys()
        column_placeholders = ""
        for i in range(len(row_dict)):
            column_placeholders += force_unicode(column_headings[i]) + ","
        column_placeholders = column_placeholders[:-1]

        # these are the values that will be inserted
        values = [row_dict[col_name] for col_name in column_headings]

        stmt = "INSERT into `{}` ({}) VALUES ({})".format(target_table, column_placeholders, values_placeholders)

        conn = self.get_connection()
        c = conn.cursor()

        if VERBOSE_DEBUG:
            LiuXin_debug_print("add_simple_row about to execute SQL code.")
            LiuXin_debug_print(stmt, " on ", target_table, " with values ", values)

        try:
            c.execute(stmt, values)
        except sqlite3.OperationalError as e:
            err_str = "sqlite3.OperationalError."
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("row_dict", row_dict),
                ("target_table", target_table),
                ("stmt", stmt),
            )
            raise DatabaseDriverError(err_str)
        except sqlite3.IntegrityError as e:
            err_str = "sqlite3.IntegrityError."
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("row_dict", row_dict),
                ("target_table", target_table),
                ("table_sqlite", self.get_table_sqlite(table=target_table, conn=conn)),
            )
            raise DatabaseIntegrityError(err_str)
        finally:
            conn.commit()
            conn.close()

    def direct_add_multiple_simple_row_dicts(self, row_dict_list):
        """
        Takes an index of new rows in the form of dictionaries. Adds them to the database.
        :param row_dict_list: Takes a list of simple rows
        """
        if len(row_dict_list) == 0:
            return True

        # Gets a reference element. Errors will be thrown if every row doesn;t match this one.
        reference_row_dict = row_dict_list[0]
        target_table = self.__identify_table_from_row(reference_row_dict)

        # TODO: re-write add_multiple_simple_rows to handle multiple different types of row
        for row in row_dict_list:
            if target_table != self.__identify_table_from_row(row):
                raise InputIntegrityError("Rows from different tables.")

        # TODO: extend the method to deal with this
        for statement in row_dict_list:
            # Check that we're dealing with rows of the same type
            if set([rk for rk in reference_row_dict.keys()]) != set([rk for rk in statement.keys()]):
                raise InputIntegrityError("Rows with different column names.")

            table_id_col = self.direct_get_id_column(target_table)

            if table_id_col in statement and statement[table_id_col] is not None:
                raise InputIntegrityError("Cannot update a row using this method!")

        for i in range(len(row_dict_list)):
            if "table" in row_dict_list[i].keys():
                del row_dict_list[i]["table"]

        # With all those checks run we should have a nice, consistent set of dictionaries to insert into target_table
        reference_row_dict = row_dict_list[0]

        values_placeholders = ""
        for i in range(len(reference_row_dict)):
            values_placeholders += "?,"

        values_placeholders = values_placeholders[:-1]

        # building the list of values
        column_list_string = ""

        column_headings = reference_row_dict.keys()

        for i in range(len(reference_row_dict)):
            if column_headings[i] != "table":
                column_list_string += force_unicode(column_headings[i]) + ","

        column_list_string = column_list_string[:-1]

        stmt = "INSERT into `{}` ({}) VALUES ({})".format(target_table, column_list_string, values_placeholders)

        values = []

        for statement in row_dict_list:
            values.append(statement.values())

        conn = self.get_connection()
        c = conn.cursor()

        info_str = "add_multiple_simple_rows about to execute SQL code."
        default_log.log_variables(info_str, "INFO", ("target_table", target_table), ("values", values))

        try:
            c.executemany(stmt, values)
        except sqlite3.OperationalError as e:
            err_str = "sqlite3.OperationalError."
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("row_dict_list", row_dict_list),
                ("target_table", target_table),
                ("stmt", stmt),
                ("values", values),
            )
            raise DatabaseDriverError(err_str)
        except sqlite3.IntegrityError as e:
            err_str = "sqlite3.IntegrityError."
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("row_dict_list", row_dict_list),
                ("target_table", target_table),
                ("table_sqlite", self.get_table_sqlite(table=target_table, conn=conn)),
                ("stmt", stmt),
                ("values", values),
            )
            raise DatabaseIntegrityError(err_str)
        finally:
            conn.commit()
            conn.close()

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO DELETE ROWS FROM THE DATABASE START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    def direct_delete_many_by_ids(self, target_table, row_ids):
        """
        Delete many entries from the given table.
        :param target_table:
        :param values:
        :return:
        """
        row_ids = ((str(rid),) for rid in row_ids)

        # Todo: Check that this is used everywhere it should be
        if not self.validate_existing_table_name(target_table):
            err_str = "target_table not found in database.\n"
            err_str = default_log.log_variables(err_str, "ERROR", ("target_table", target_table), ("row_ids", row_ids))
            raise InputIntegrityError(err_str)

        conn = self.get_connection()
        target_table_id_column = self._get_id_column(target_table)
        stmt = "DELETE FROM {} WHERE {} = ?;".format(target_table, target_table_id_column)
        try:
            conn.executemany(stmt, row_ids)
        except sqlite3.OperationalError as e:
            err_str = "Operational error on table.\n"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("target_table", target_table),
                ("row_ids", row_ids),
                ("stmt", stmt),
            )
            conn.commit()
            conn.close()
            raise DatabaseDriverError(err_str)
        except sqlite3.IntegrityError as e:
            err_str = "IntegrityError on table."
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("target_table", target_table),
                ("row_ids", row_ids),
                ("stmt", stmt),
            )
            conn.commit()
            conn.close()
            raise DatabaseIntegrityError(err_str)
        finally:
            conn.commit()
            conn.close()

        # Todo: Add checking that the delete has gone through
        return True

    # Todo: Merge
    def direct_delete(self, target_table, column, value, many=False):
        """
        Delete all the entries in the target_table whose column matches that value.
        :param target_table:
        :param column:
        :param value:
        :param many: Is it a single value or many
        :return:
        """
        if not self.validate_existing_table_name(target_table):
            err_str = "target_table not found in database.\n"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("target_table", target_table),
                ("column", column),
                ("value", value),
            )
            raise InputIntegrityError(err_str)

        conn = self.get_connection()
        stmt = "DELETE FROM {} WHERE {} = ?;".format(target_table, column)
        try:
            if not many:
                conn.execute(stmt, (value,))
            else:
                value = tuple([(str(v),) for v in value])
                conn.executemany(stmt, value)
        except sqlite3.OperationalError as e:
            err_str = "Operational error on table.\n"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("target_table", target_table),
                ("column", column),
                ("value", value),
                ("stmt", stmt),
            )
            conn.commit()
            conn.close()
            raise DatabaseDriverError(err_str)
        except sqlite3.IntegrityError as e:
            err_str = "IntegrityError on table.\n"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("target_table", target_table),
                ("column", column),
                ("value", value),
                ("stmt", stmt),
            )
            conn.commit()
            conn.close()
            raise DatabaseIntegrityError(err_str)
        finally:
            conn.commit()
            conn.close()

        # Todo: Add checking that the delete has gone through
        return True

    # Todo: Standardize on "table" not "target_table"
    def direct_delete_many(self, target_table, column, values):
        """
        Delete all the entries in the target_table whose column matches that value.
        :param target_table:
        :param column:
        :param values:
        :return:
        """
        self.direct_delete(target_table=target_table, column=column, value=values, many=True)

    def direct_delete_row_by_id(self, target_table, row_id):
        """
        Takes a table and a row_id - deletes the row with that id.
        :param target_table:
        :param row_id:
        :return:
        """
        if not self.validate_existing_table_name(target_table):
            err_str = "target_table not found in database."
            err_str = default_log.log_variables(err_str, "ERROR", ("target_table", target_table), ("row_id", row_id))
            raise InputIntegrityError(err_str)

        conn = self.get_connection()
        target_table_id_column = self._get_id_column(target_table)
        stmt = "DELETE FROM {} WHERE {} = ?;".format(target_table, target_table_id_column)

        try:
            conn.execute(stmt, (row_id,))
        except sqlite3.OperationalError as e:
            err_str = "Operational error on table."
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("target_table", target_table),
                ("row_id", row_id),
                ("stmt", stmt),
            )
            conn.commit()
            raise DatabaseDriverError(err_str)
        except sqlite3.IntegrityError as e:
            err_str = "IntegrityError on table.\n"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("target_table", target_table),
                ("row_id", row_id),
                ("stmt", stmt),
            )
            default_log.log_exception(message=err_str, exception=e, level="ERROR")
            conn.commit()
            raise DatabaseIntegrityError(err_str)
        finally:
            conn.commit()

        # Todo: Add checking that the delete has gone through
        return True

    def direct_clear_table(self, target_table):
        """
        Deletes every record from a table.
        :param target_table:
        :param prompt:
        :return:
        """
        if not self.validate_existing_table_name(target_table):
            err_str = "target_table not found in database.\n"
            err_str = default_log.log_variables(err_str, "ERROR", ("target_table", target_table))
            raise InputIntegrityError(err_str)

        # Lock the database (to stop anything being assigned into the space that has just been freed by the delete
        # between the delete and the check) - clear the table - check that there are actually no rows in the table
        conn = self.get_connection()

        row_count = None
        try:
            with conn:
                # Delete the row
                stmt = "DELETE FROM {};".format(target_table)
                conn.execute(stmt)
                conn.commit()

                # Check to see if there are actually any rows left in the table
                stmt = "SELECT COUNT(*) FROM {};".format(target_table)
                c = conn.cursor()
                for row in c.execute(stmt):
                    row_count = row[0]
                if row_count is None:
                    row_count = 0

        except sqlite3.OperationalError as e:
            err_str = "Unable to delete target row - OperationalError.\n"
            err_str = default_log.log_exception(err_str, e, "ERROR", ("target_table", target_table))
            raise DatabaseDriverError(err_str)

        except sqlite3.IntegrityError as e:
            err_str = "Unable to delete target row - IntegrityError.\n"
            err_str = default_log.log_exception(err_str, e, "ERROR", ("target_table", target_table))
            raise DatabaseIntegrityError(err_str)

        finally:
            conn.close()

        if row_count == 0:
            return True
        else:
            return False

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO GET INFORMATION ABOUT TABLES ON THE DATABASE START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    def direct_get_highest_id(self, target_table):
        """
        Getting a random id from the database using u'SELECT * FROM {} ORDER BY RANDOM() LIMIT 1' is really slow in the
        case of large tables.
        Something a little snappier would be nice.
        Returns the highest id in the table.
        :param target_table:
        :return:
        """
        target_table = force_unicode(target_table)
        target_table_id = self._get_id_column(target_table)
        stmt = "SELECT max({}) FROM {};".format(target_table_id, target_table)

        conn = self.get_connection()
        c = conn.cursor()

        for row in c.execute(stmt):
            conn.close()
            return row[0]

        # In the case where the table has no entries
        return None

    def direct_get_record_count(self, target_table):
        """
        Returns the number of records in a given table.
        :param target_table:
        :return:
        """
        if not self.validate_existing_table_name(target_table):
            err_str = "target_table not found in database.\n"
            err_str = default_log.log_variables(err_str, "ERROR", ("target_table", target_table))
            raise InputIntegrityError(err_str)

        conn = self.get_connection()
        c = conn.cursor()
        stmt = "SELECT COUNT(*) FROM {}".format(target_table)
        for row in c.execute(stmt):
            conn.close()
            return row[0]

        raise NotImplementedError("This position should never be reached")

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO SEARCH THE DATABASE START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    def direct_get_random_row_dict(self, target_table, direct=False):
        """
        Returns a random row_dict from the specified table.
        :param target_table:
        :param direct:
        :return:
        """
        conn = self.get_connection()
        c = conn.cursor()
        target_table = force_unicode(deepcopy(target_table))

        # checks that you're requesting data from an existing table
        if not self.validate_existing_table_name(target_table):
            err_str = "table name passed into direct_get_random_row_dict failed validation.\n"
            err_str = default_log.log_variables(err_str, "ERROR", ("target_table", target_table))
            raise InputIntegrityError(err_str)

        highest_id = self.direct_get_highest_id(target_table)
        try:
            highest_id = int(highest_id)
        except TypeError:
            wrn_str = (
                "Unable to coerce highest_id to integer. "
                "Assuming this means that the table is empty. "
                "Could also mean that non-integer ids are being used - in which case this method cannot be used"
            )
            wrn_str = default_log.log_variables(wrn_str, "WARN", ("highest_id", highest_id))
            conn.close()
            return None

        if direct:
            headings = self.direct_get_column_headings(target_table)
            stmt = "SELECT * FROM {} ORDER BY RANDOM() LIMIT 1".format(target_table)
            for row in c.execute(stmt):
                this_row = dict()
                for i in range(len(headings)):
                    if not isinstance(row[i], set):
                        this_row[headings[i]] = force_unicode(row[i])
                    else:
                        this_row[headings[i]] = row[i]
                conn.close()
                return this_row

        elif not direct:
            random.seed()
            conn.close()

            while True:
                new_row_id = random.randint(1, highest_id)
                candidate_row = self.direct_get_row_dict_from_id(table=target_table, row_id=new_row_id)
                if candidate_row:
                    return candidate_row

        # In the case where there are no rows in the table, returns None
        return None

    def direct_get_all_rows(self, table, sort_column=None, reverse=False):
        """
        Returns all rows from a given table in the database in the form of an index of row_dicts.
        Should only be used with small tables. Otherwise the memory cost is prohibitive.
        :param table: Yield the rows from this table
        :param sort_column: Sort the rows by the values in this column
        :param reverse: Should the order of the rows be reversed?
        :return:
        """
        conn = self.get_connection()
        c = conn.cursor()
        table = force_unicode(table)
        headings = self.direct_get_column_headings(table)

        # checks that you're requesting data from an existing table
        if not self.validate_existing_table_name(table):
            err_str = "table name passed into direct_get_all_rows failed validation.\n"
            err_str = default_log.log_variables(err_str, "ERROR", ("table", table))
            raise InputIntegrityError(err_str)

        # Check that the sort_column is in the requested table
        if sort_column not in headings and sort_column is not None:
            err_str = "table and sort_column are not consistent.\n"
            err_str = default_log.log_variables(err_str, "ERROR", ("table", table), ("sort_column", sort_column))
            raise InputIntegrityError(err_str)

        if sort_column is None:
            stmt = "SELECT * FROM {};".format(table)
        else:
            if not reverse:
                stmt = "SELECT * FROM {} ORDER BY {} ASC;".format(table, sort_column)
            else:
                stmt = "SELECT * FROM {} ORDER BY {} DESC".format(table, sort_column)

        results = []
        for row in c.execute(stmt):
            this_row = dict()
            for i in range(len(headings)):
                if not isinstance(row[i], set):
                    this_row[headings[i]] = force_unicode(row[i])
                else:
                    this_row[headings[i]] = row[i]
            results.append(this_row)

        conn.close()
        return results

    def direct_get_row_dict_iterator(self, table, sort_column=None, reverse=False):
        """
        Provides an iterator which returns all the rows in a specified table in the form of row_dicts. Ordered by id
        :param table: Get an iterator for all the rows in this table.
        :param sort_column: The column the table should be sorted by
        :param reverse: Should the order of the rows be reversed?
        :return:
        """
        table = force_unicode(table)
        table_id_column = self._get_id_column(table)
        headings = self.direct_get_column_headings(table)

        # checks that you're requesting data from an existing table
        if not self.validate_existing_table_name(table):
            err_str = "table name passed into direct_get_all_rows failed validation."
            err_str = default_log.log_variables(err_str, "ERROR", ("table", table))
            raise InputIntegrityError(err_str)

        # Check that the sort_column comes from the table
        if sort_column is not None:
            if sort_column not in headings:
                err_str = "requested sort_column is not in the requested table.\n"
                err_str = default_log.log_variables(err_str, "ERROR", ("table", table), ("sort_column", sort_column))
                raise InputIntegrityError(err_str)

        start_id_value = 0
        if sort_column is None:

            # reads data from the database in 10 row chunks - then closing the connection. Should leave the database
            # unlocked for most of the time
            while True:

                conn = self.get_connection()
                c = conn.cursor()
                this_stmt = "SELECT * FROM {} WHERE {} > {} ORDER BY {} LIMIT 10;".format(
                    table, table_id_column, start_id_value, table_id_column
                )
                c.execute(this_stmt)
                current_rows = deepcopy(c.fetchall())
                conn.close()

                if not current_rows:
                    conn.close()
                    break
                for row in current_rows:
                    this_row = dict()
                    for i in range(len(headings)):
                        if not isinstance(row[i], set):
                            this_row[headings[i]] = force_unicode(row[i])
                        else:
                            this_row[headings[i]] = row[i]
                    yield this_row
                    start_id_value = this_row[table_id_column]

        else:

            # Sort the table by the sort_column and then by the id? Don't have a good solution for this yet (due to
            # concern that the sort order will change while the update is running
            # Do something with timestamps
            raise NotImplementedError("Cannot currently cope with this combination")

    def direct_get_unique_values_set(self, target_column):
        """
        Returns a set of the unique values in a column.
        :param target_column:
        :return values_set: A set of all the unique values in that column
        """
        target_table = self.__identify_table_from_column(column_heading=target_column)
        stmt = "SELECT DISTINCT {} FROM {};".format(target_column, target_table)
        values_set = set()
        conn = self.get_connection()
        c = conn.cursor()

        for value in c.execute(stmt):
            values_set.add(value[0])
        conn.close()
        return values_set

    def direct_get_unique_values_iterator(self, target_column):
        """
        Iterates over the unique values in a column.
        Helps to keep memory usage down when dealing with very large tables.
        :param target_column:
        :return:
        """
        # Needs to sort the table after every retrieval - so will be very slow for large databases
        # Todo: Come back and optimize/make this work
        # target_table = self.__identify_table_from_column(column_heading=target_column)
        # stmt = 'SELECT DISTINCT {} FROM {} WHERE {} > {} LIMIT 1;'
        # stmt = stmt.format(target_column, target_table)
        values_set = self.direct_get_unique_values_set(target_column)
        for value in values_set:
            yield value

    def direct_get_row_dict_from_id(self, table, row_id):
        """
        Attempts to get a specific row from the table give. Returns the result as a dictionary kweyed with the column
        name and valued with the values from that row.
        :param table: The table to search in
        :param row_id: The id this function will be looking for
        :return row/False: The requested Row. False if nothing is found.
        """
        table = force_unicode(table)
        row_id = force_unicode(row_id)

        conn = self.get_connection()
        c = conn.cursor()

        headings = self.direct_get_column_headings(table)
        table_id_name = self._get_id_column(table)

        stmt = "SELECT * FROM {} WHERE {} = ?".format(table, table_id_name)

        rows = []
        result = dict()
        try:
            for row in c.execute(stmt, (row_id,)):
                for i in range(len(headings)):
                    if not isinstance(headings[i], set):
                        result[headings[i]] = force_unicode(row[i])
                    else:
                        result[headings[i]] = row[i]
                rows.append(result)
        except sqlite3.InterfaceError as e:
            err_str = "Interface error while trying to find a row\n"
            err_str = default_log.log_exception(err_str, e, "ERROR", ("row_id", row_id))
            raise DatabaseDriverError(err_str)

        if len(rows) > 1:
            err_str = "Error - search yielded multiple rows. Aborting.\n"
            err_str += repr(rows)
            default_log.error(err_str)
            conn.close()
            raise DatabaseIntegrityError(err_str)
        elif len(rows) == 0:
            info_str = "Warning - search yielded no results. Consider sources of logical error."
            default_log.log_variables(info_str, "INFO", ("table", table), ("row_id", row_id))
            conn.close()
            return False
        else:
            conn.close()
            return result

    def direct_get_view_row_dict_from_id(self, view, row_id):
        """
        Retrieve a row from a view and return it as a dictionary, keyed with the column headings of the row and valued
        with the values of that column.
        :param view:
        :param row_id:
        :return:
        """
        view = force_unicode(view)
        row_id = force_unicode(row_id)

        conn = self.get_connection()
        c = conn.cursor()

        headings = self.direct_get_view_column_headings(view)
        table_id_name = "id"

        stmt = "SELECT * FROM {} WHERE {} = ?".format(view, table_id_name)

        rows = []
        result = dict()
        for row in c.execute(stmt, (row_id,)):
            for i in range(len(headings)):
                if not isinstance(headings[i], set):
                    result[headings[i]] = force_unicode(row[i])
                else:
                    result[headings[i]] = row[i]
            rows.append(result)

        if len(rows) > 1:
            err_str = "Error - search yielded multiple rows. Aborting.\n"
            err_str += repr(rows)
            default_log.error(err_str)
            conn.close()
            raise DatabaseIntegrityError(err_str)
        elif len(rows) == 0:
            info_str = "Warning - search yielded no results. Consider sources of logical error."
            default_log.log_variables(info_str, "INFO", ("table", view), ("row_id", row_id))
            conn.close()
            return False
        else:
            conn.close()
            return result

    def direct_get_view_column_headings(self, view):
        """
        Returns the column headings for the given view.
        :param view:
        :return:
        """
        # Todo: Add checking against injection attacks
        stmt = "PRAGMA TABLE_INFO({})".format(view)

        conn = self.get_connection()
        c = conn.cursor()

        view_columns = []
        for i in c.execute(stmt):
            view_columns.append(i[1])

        return view_columns

    def direct_search_table(self, table=None, column=None, search_term=None):
        """
        Searches a specified column in a table by the given search term.
        Returns an empty index if no results are found.
        :param table: The table to search (can be unspecified - but don't want to break backwards compatibility
        :param column: The column to search in
        :param search_term: The string to search with
        :return results: An index of row_dicts
        """
        if (table is not None) and (column is not None) and (search_term is not None):
            try:
                table = force_unicode(table)
                column = force_unicode(column)
                search_term = force_unicode(search_term)
            except UnicodeDecodeError:
                err_str = "search_table was passed something it couldn't coerce to unicode?\n"
                err_str += "table: " + repr(table) + "\n"
                err_str += "column: " + repr(column) + "\n"
                err_str += "search_term: " + repr(search_term) + "\n"
                default_log.error(err_str)
                raise InputIntegrityError(err_str)

        elif (table is None) and (column is not None) and (search_term is not None):
            try:
                column = force_unicode(column)
                search_term = force_unicode(search_term)
            except UnicodeDecodeError:
                err_str = "search_table was passed something it couldn't coerce to unicode?\n"
                err_str += "table: " + repr(table) + "\n"
                err_str += "column: " + repr(column) + "\n"
                err_str += "search_term: " + repr(search_term) + "\n"
                default_log.error(err_str)
                raise InputIntegrityError(err_str)

        else:
            err_str = "Request to search table was not properly formatted.\n"
            err_str += "table: " + repr(table) + "\n"
            err_str += "column: " + repr(column) + "\n"
            err_str += "search_term: " + repr(search_term) + "\n"
            default_log.error(err_str)
            raise InputIntegrityError(err_str)

        conn = self.get_connection()
        c = conn.cursor()

        stmt = "SELECT * FROM {} WHERE {} = ?;".format(table, column)

        results = []
        headings = self.direct_get_column_headings(table)
        try:

            for row in c.execute(stmt, (search_term,)):
                this_row = dict()
                for i in range(len(headings)):
                    if not isinstance(headings[i], set):
                        this_row[headings[i]] = force_unicode(row[i])
                    else:
                        this_row[headings[i]] = row[i]
                results.append(this_row)

        except sqlite3.OperationalError as e:
            err_str = "Unable to update - OperationalError - search term might be malformed\n"
            err_str = default_log.log_exception(err_str, e, "ERROR", ("stmt", stmt), ("search_term", search_term))
            conn.close()
            raise InputIntegrityError(err_str)

        conn.close()
        return results

    # Todo: Check for field degeneracy
    def direct_update_columns(self, id_values_map, field=None, table=None):
        """
        For when you only want to update specific columns in rows.
        Detects the kind of map entered - preforms different actions depending on what it is.
        :return:
        """
        # Check to see if the map is one-one (a id_values_map keyed with an id and values with a single entry - with a
        # field and table for targeting - one value is changed in each row)
        # or one-to-many (and id_values_map keyed with an id and values with a dictionary keyed with the column name and
        # valued with the new column value)

        def detect_mode(int_id_values_map):
            if len(int_id_values_map) == 0:
                return None

            sample_key = iter(int_id_values_map).next()
            sample_values = int_id_values_map[sample_key]
            if isinstance(sample_values, dict):
                return "many"
            else:
                return "one"

        mode = detect_mode(id_values_map)

        if mode == "one":

            # Checking that the field and table make sense
            field_table = self.__identify_table_from_column(field)
            if table is not None:
                if field_table != table:
                    wrn_str = "LiuXin.databases.SQLITE.databasedriver:direct_update_columns was fed inconsistent data."
                    wrn_str += "the given column doesn't belong to the given table.\n"
                    default_log.log_variables(
                        wrn_str,
                        "WARNING",
                        ("field", field),
                        ("table", table),
                        ("id_values_map", id_values_map),
                    )
                    target_table = field_table
                else:
                    target_table = table
            else:
                target_table = table

            # Building the sequence - need it in the form of a tuple of tuples - value, id
            sequence = ((v, k) for k, v in iteritems(id_values_map))

            # Building the statement
            table_id_col = self._get_id_column(target_table)
            stmt = "UPDATE {} SET {}=? WHERE {}=?".format(target_table, field, table_id_col)

            # Executing the statement and the sequence together
            conn = self.get_connection()
            conn.executemany(stmt, sequence)
            conn.commit()

        elif mode == "many":

            # Todo: Fix
            raise NotImplementedError

    def direct_update_row_dict(self, row_dict):
        """
        Takes a row in the form of a row_dict. Updates that row_dict into the database.
        This is the method Row ultimately calls to update itself - THUS DO NOT CALL WITH ROW. IT WAS CAUSE RECURSION.
        :param row_dict:
        :return:
        """
        target_table = self.__identify_table_from_row(row_dict)
        row_dict = deepcopy(row_dict)

        # Trying to write a u'None' to a column with a foreign key constraint causes problems. Replacing all of these
        # with actual None
        new_row_dict = dict()
        for column in row_dict:
            if row_dict[column] == "None":
                new_row_dict[column] = None
            else:
                new_row_dict[column] = row_dict[column]
        row_dict = new_row_dict

        # working out what the id column for the table is called
        row_id = self._get_id_column(target_table)
        if row_id in row_dict:
            target_row_id = row_dict[row_id]
            del row_dict[row_id]
        else:
            err_str = "update_row_in_table method has failed.\n"
            err_str += " It was unable to find a valid row_id.\n"
            err_str += "row_dict: " + pprint.pformat(row_dict) + "\n"
            default_log.error(err_str)
            raise RowIntegrityError(err_str)

        # If removing the id column has reduced the length of the row to zero, then no further action need be taken.
        # some check should be added here to make sure the column you're trying to update has the row you're
        # trying to update in it
        if len(row_dict) == 0:
            return True

        # Assembling a list of placeholders of the form (?,?,?)
        number_of_values = len(row_dict)
        values_placeholders = "("
        for i in range(number_of_values):
            values_placeholders += "?,"
        values_placeholders = values_placeholders[:-1]
        values_placeholders += ")"

        # These are the column headings values will be inserted into, with corresponding values
        column_headings = row_dict.keys()
        values = row_dict.values()

        # building the list of value
        column_list = ""
        for i in range(number_of_values):
            column_list += force_unicode(column_headings[i]) + " = ? ,"
        column_list = column_list[:-1]
        values.append(target_row_id)

        stmt = "UPDATE {} SET {} WHERE {} = ?".format(target_table, column_list, row_id)

        conn = self.get_connection()
        c = conn.cursor()

        # info_str = "Command about to be executed on the database.\n"
        # info_str += "stmt: " + stmt + "\n"
        # info_str += "values: " + unicode(values) + "\n"
        # info_str += "target_row_id: " + unicode(target_row_id) + "\n"
        # info_str += "row_dict: " + unicode(row_dict) + "\n"
        # default_log.info(info_str)

        try:
            c.execute(stmt, values)
            conn.commit()
            conn.close()
        except sqlite3.InterfaceError as e:
            err_str = "Unable to update - InterfaceError.\n"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("stmt", stmt),
                ("values", values),
                ("row_dict", row_dict),
            )
            conn.close()
            raise DatabaseDriverError(err_str)
        except sqlite3.OperationalError as e:
            err_str = "Unable to update - OperationalError.\n"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("stmt", stmt),
                ("values", values),
                ("row_dict", row_dict),
            )
            conn.close()
            raise DatabaseDriverError(err_str)
        except sqlite3.IntegrityError as e:
            conn.commit()
            conn.close()
            err_str = "Unable to update - IntegrityError.\n"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("stmt", stmt),
                ("values", values),
                ("row_dict", row_dict),
            )
            conn.close()
            raise DatabaseIntegrityError(err_str)

    # A copy of a function a level up, at database level - implemented here as well to make recursion loops less likely
    def __identify_table_from_row(self, row_dict):
        """
        Takes a row. Attempts to identify which row it came from.
        :param row_dict: The row (dict) to be parsed
        :return table_name: The table name (string)
        """
        if "table" in row_dict.keys():
            del row_dict["table"]

        tables_and_columns = self.direct_get_tables_and_columns()
        table = tables_and_columns.keys()
        row_columns_set = set(key for key in row_dict.keys())

        if VERBOSE_DEBUG:
            err_str = "Calling identify_table_from_row.\n"
            err_str += "Table_and_columns: " + repr(tables_and_columns) + "\n"
            err_str += "Tables: " + repr(table) + "\n"
            LiuXin_debug_print(err_str)

        # if this method is called with a null row it will complain. If warn is true
        if len(row_dict) == 0:
            info_str = "Warning - identify_table_from_row called with empty row."
            default_log.info(info_str)
            return False

        for table in tables_and_columns.keys():

            column_heading_set = set(heading for heading in tables_and_columns[table])
            if row_columns_set.issubset(column_heading_set):
                return table

        # If this point in the algorithm has been reached then something has gone wrong.
        # Searching for partial matches - tables with some, but not all of the column names
        partial_match_tables = set()
        unmatched_columns = set()

        for column_heading in row_dict.keys():
            try:
                column_table = self.__identify_table_from_column(column_heading, print_error=False)
                partial_match_tables.add(column_table)
            except InputIntegrityError:
                unmatched_columns.add(column_heading)

        err_str = "SQLite:databasedriver:__identify_table_from_row unable to find matching table.\n"
        if len(partial_match_tables) > 0:
            err_str += "partial matches found for some column_headings.\n"
            err_str += "partial_match_tables: " + pprint.pformat(partial_match_tables) + "\n"
        if len(unmatched_columns) > 0:
            err_str += "some column_headings could not be matched.\n"
            err_str += "unmatched_columns: " + pprint.pformat(unmatched_columns) + "\n"
        err_str += "row_dict: " + pprint.pformat(row_dict) + "\n"
        if preferences.parse(
            "include_full_rep_if_row_cant_be_identified",
            rtn_value_type="bool",
            default=False,
        ):
            err_str += "tables_and_columns: " + pprint.pformat(tables_and_columns) + "\n"
        default_log.error(err_str)
        raise DatabaseIntegrityError(err_str)

    def validate_existing_table_name(self, test_name):
        """
        Test to see if a candidate table name is valid (contains no SQL control characters).
        Intended to help with SQL injection attack proofing. Should be spread to all columns as well.
        :param test_name:
        :return True/False:
        """
        # If the name matches a pre-existing one it is automatically valid (this function is used to validate input)
        # Not to validate potential new table names.
        tables_and_columns = self.direct_get_tables_and_columns()

        # intended to help with SQL injection attack proofing
        try:
            test_name = force_unicode(test_name)
        except UnicodeDecodeError:
            err_str = "Attempt to validate_existing_table_name has failed. Could not coerce test_name to unicode."
            err_str += "test_name: " + repr(test_name) + "\n"
            raise InputIntegrityError(err_str)

        # Testing for SQL special characters - things which might be used to build an attack
        sql_forbidden_chars = [";", ":", "&"]
        for character in sql_forbidden_chars:
            if character in test_name:
                return False

        # stripping whitespace
        test_name = test_name.strip()
        tables = tables_and_columns.keys()
        possible_tables = []

        # characters to be appended to the beginning and end of a table name (characters that SQL ignores)
        additional_characters = ["`", "\\", "", "%", "_"]
        for table in tables:
            for character in additional_characters:
                current_name = character + table + character
                possible_tables.append(current_name)

        if test_name in possible_tables:
            return True
        else:
            return False

    def direct_get_id_column(self, table, tables_and_columns=None):
        """
        Return the id column for a given table.
        :param table:
        :param tables_and_columns:
        :return:
        """
        table = force_unicode(table)
        tables_and_columns = self.direct_get_tables_and_columns()
        try:
            headings = tables_and_columns[table]
        except KeyError as e:
            err_str = "DatabaseDriver.direct_get_id_column failed - table couldn't be found.\n"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("table", table),
                ("tables", sorted(tables_and_columns.keys())),
            )
            raise InputIntegrityError(err_str)

        # Check for the special case where there is just a column called "id"
        if "id" in headings:
            return "id"

        candidate_ids = []
        for heading in headings:
            if heading.endswith("_id"):
                candidate_ids.append(heading)
        if len(candidate_ids) > 1:
            candidate_ids = sorted(candidate_ids, key=len)
            return candidate_ids[0]
        elif len(candidate_ids) == 0:
            err_str = "Error - get_id_column failed - no column with a name ending in id found"
            err_str = default_log.log_variables(err_str, "ERROR", ("headings", headings))
            raise InputIntegrityError(err_str)
        else:
            return candidate_ids[0]

    def direct_get_datestamp_column(self, table, tables_and_columns=None):
        """
        Return the id column for a given table.
        :param table:
        :param tables_and_columns:
        :return:
        """
        table = force_unicode(table)
        tables_and_columns = self.direct_get_tables_and_columns()
        try:
            headings = tables_and_columns[table]
        except KeyError as e:
            err_str = "DatabaseDriver.direct_get_id_column failed - table couldn't be found.\n"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("table", table),
                ("tables", sorted(tables_and_columns.keys())),
            )
            raise InputIntegrityError(err_str)

        # Check for the special case where there is just a column called "id"
        if "datestamp" in headings:
            return "datestamp"

        candidate_ids = []
        for heading in headings:
            if heading.endswith("_datestamp"):
                candidate_ids.append(heading)
        if len(candidate_ids) > 1:
            candidate_ids = sorted(candidate_ids, key=len)
            return candidate_ids[0]
        elif len(candidate_ids) == 0:
            err_str = "Error - get_id_column failed - no column with a name ending in id found"
            err_str = default_log.log_variables(err_str, "ERROR", ("headings", headings))
            raise InputIntegrityError(err_str)
        else:
            return candidate_ids[0]

    # needs testing
    # Currently assumes that there is a column with a name ending in id and that if this is true for multiple rows that
    # the shortest string ending in id is the id string. Should be tested every time a new column is added
    def _get_id_column(self, table, tables_and_columns=None):
        """
        Every table in the database should have an id column.
        Currently assumes that there is a column with a name ending in id and that if this is true for multiple rows that
        the shortest string ending in id is the id string. Should be tested every time a new column is added
        :param table:
        :param tables_and_columns:
        :return:
        """
        return self.direct_get_id_column(table=table, tables_and_columns=tables_and_columns)

    def __identify_table_from_column(self, column_heading, headings_and_columns=None, print_error=True):
        """
        Takes a column heading (and optionally a headings and columns dict). Works out the table it falls into.
        :param column_heading: Each column heading should be unique in the database
        :param headings_and_columns: COMPLETELY SUPERFLUOUS
        :param print_error: Will be replaced with LiuXin debug print
        :return:
        """
        if headings_and_columns is None:
            headings_and_columns_local = self.direct_get_tables_and_columns()
        else:
            headings_and_columns_local = headings_and_columns
        tables = headings_and_columns_local.keys()

        for table in tables:
            column_headings = headings_and_columns_local[table]
            if column_heading in column_headings:
                return table
        else:
            if print_error:
                err_str = "identify_table_from_column failed.\n"
                err_str += repr(column_heading) + " was not recognized.\n"
                default_log.error(err_str)
                raise InputIntegrityError(err_str)
            else:
                raise InputIntegrityError

    # Todo - Merge with direct_get_unique_values - after an upgrade to allow specify a table
    def direct_get_all_values(self, table, column):
        """
        Returns a set of all values in the given column in the given table.
        :param table: The table to be searched
        :param column: The column in that table
        :return:
        """
        if table is not None:
            table = deepcopy(force_unicode(table))
        else:
            table = self.__identify_table_from_column(column)
        column = deepcopy(force_unicode(column))

        current_values = set()
        conn = self.get_connection()
        c = conn.cursor()

        stmt = "SELECT {} FROM {}".format(column, table)
        for row in c.execute(stmt):
            current_values.add(row[0])
        return current_values

    def direct_get_all_hashes(self):
        """
        Returns a set of all hashes in the database.
        :return:
        """
        file_hashes = self.direct_get_all_values(table="files", column="file_hash")
        cf_hashes_1 = self.direct_get_all_values(table="compressed_files", column="compressed_file_hash_1")
        cf_hashes_2 = self.direct_get_all_values(table="compressed_files", column="compressed_file_hash_2")
        nb_hashes_1 = self.direct_get_all_values(table="new_books", column="new_book_hash_1")
        nb_hashes_2 = self.direct_get_all_values(table="new_books", column="new_book_hash_2")
        other_hashes = self.direct_get_all_values(table="hashes", column="hash")
        return (
            file_hashes.union(cf_hashes_1).union(cf_hashes_2).union(nb_hashes_1).union(nb_hashes_2).union(other_hashes)
        )

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # METHODS SPECIFIC TO DEALING WITH NEW BOOKS START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    def direct_get_next_book_group(self):
        """
        Returns the next group of files from new_books and the group_id corresponding to that group.
        :return book_grouping, min_group_id:
        """
        conn = self.get_connection()
        c = conn.cursor()

        stmt = "SELECT min(new_book_group_id) FROM `new_books`"
        # Returns off the database are passed around in the form of dictionaries (at this level)
        book_grouping = []
        min_group_id = None
        for row in c.execute(stmt):
            min_group_id = row[0]

        stmt2 = "SELECT * FROM `new_books` WHERE new_book_group_id = ?"
        headings = self.direct_get_column_headings("`new_books`")
        for row in c.execute(stmt2, (min_group_id,)):
            this_row = dict()
            for i in range(len(row)):
                this_row[headings[i]] = row[i]
            book_grouping.append(this_row)

        conn.close()
        return book_grouping, min_group_id

    # This should definitely not be here
    def sum_book_group_sizes(self, book_group):
        """
        Takes a book group in the form of a index of dictionaries.
        :param book_group: A .. group of books?
        :return book_group_size: In bytes
        """
        size = 0
        for book in book_group:
            size += book["new_book_size"]
        return size

    def direct_delete_book_group(self, group_id):
        """
        Takes the id of a group of files in the new_books table. Deletes them.
        :param group_id: The id of the group of books we are searching for
        :return:
        """

        conn = self.get_connection()
        c = conn.cursor()

        stmt = "DELETE FROM new_books WHERE new_book_group_id = ?"
        c.execute(stmt, group_id)
        conn.commit()
        conn.close()

    def direct_get_row_count(self, table):
        """
        Gets the row count off the table.
        :param table:
        :return:
        """
        conn = self.get_connection()
        c = conn.cursor()

        stmt = "SELECT COUNT(*) FROM {}".format(table)

        for row in c.execute(stmt):
            conn.close()
            return row[0]

    # ----------------------------------------------------------------------------------------------------------------------
    # - SPECIALIZED METHODS TO READ AND SET THE DATABASE METADATA START HERE
    # ----------------------------------------------------------------------------------------------------------------------

    def direct_get_db_unique_id(self):
        """
        It is useful to embed certain information about the database in it directly (thus you can tell your dealing with
        the same database, even if it's been moved to a different place or converted into a different format).
        The database_unique_id is a uuid4 string for the database which is written into the database on creation to
        uniquely define it's instance number forwever more.
        :return:
        """
        stmt = "SELECT `database_metadata_unique_id` FROM `database_metadata`"
        conn = self.get_connection()
        unique_ids = []
        for row in conn.execute(stmt):
            unique_ids.append(row[0])
        conn.close()
        if len(unique_ids) == 0:
            return None
        elif len(unique_ids) == 1:
            return unique_ids[0]
        else:
            err_str = "Unable to return a unique database_metadata_unique_id.\n"
            err_str += "Thus database_metadata has more than one row.\n"
            err_str += "This should never happen.\n"
            err_str += "unique_ids: " + repr(unique_ids) + "\n"
            raise DatabaseIntegrityError(err_str)

    def direct_set_db_unique_id(self, force_value=None):
        """
        Allows you to set the database unique id.
        If no force value is supplied, just uses uuid to generate one and inserts it instead.
        Prompts to proceed if it detects the value is already set
        :param force_value: Default None
        :return:
        """
        if force_value is None:
            new_force_value = str(uuid.uuid4())
        else:
            new_force_value = force_value

        conn = self.get_connection()
        test_val = self.direct_get_db_unique_id()
        if test_val is not None:

            stmt = (
                "UPDATE `database_metadata` SET `database_metadata_unique_id` = ? " "WHERE `database_metadata_id` = 1"
            )
            conn.execute(stmt, (new_force_value,))
            conn.commit()
            conn.close()
            actual_value = self.direct_get_db_unique_id()
            if actual_value != new_force_value:
                err_str = "Attempt to change database_metadata_unique_id failed.\n"
                err_str += "new_force_value: " + repr(new_force_value) + "\n"
                err_str += "actual_value: " + repr(actual_value) + "\n"
                raise DatabaseIntegrityError(err_str)
            return True

        else:

            stmt = "INSERT into `database_metadata` (`database_metadata_unique_id`) VALUES (?)"
            conn.execute(stmt, (new_force_value,))
            conn.commit()
            conn.close()
            actual_value = self.direct_get_db_unique_id()
            if actual_value != new_force_value:
                err_str = "Attempt to change database_metadata_unique_id failed.\n"
                err_str += "new_force_value: " + repr(new_force_value) + "\n"
                err_str += "actual_value: " + repr(actual_value) + "\n"
                raise DatabaseIntegrityError(err_str)
            return True

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - SEARCH METHODS START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    # Todo: Paused while adding a method to import metadata from a csv file - so I can test something fancy with a join
    def direct_multi_column_search(self, search_index, iterator_return=False):
        """
        Takes an index of tuples (or indexes - the method is not fussy provided it contains the required terms). Which
        can then be used to search the database.
        Tuples should take the form (column_name, binary_comparison_operator, target_value).
        Binary comparison operators can include the LIKE operator.
        Every tuple is joined together by an AND statement.
        Will currently fail unless every row is in the same table.
        Thus [(u'creator', u'=', u'David Weber'),(u'series',u'=',u'Honor Harrington')] becomes
        SELECT * FROM `creators` * WHERE creator = 'David Weber' AND series = 'Honor Harrington';
        :param search_index:
        :param iterator_return: Should an iterator leading to the database be returned? Default: False - in which case
        result is returned as an index
        :return found_rows:
        """
        if len(search_index) == 0:
            if VERBOSE_DEBUG:
                debug_str = "multi-column search has been passed an empty index.\n"
                LiuXin_debug_print(debug_str)
            else:
                return None

        # Builds a set of the requested tables - to check that every column comes from the same table
        columns_set = set()
        table_set = set()
        for term in search_index:
            columns_set.add(term[0])
        for column in columns_set:
            table_set.add(self.__identify_table_from_column(column))
        if len(table_set) == 0:
            err_str = "Attempt to parse the search_index has failed.\n"
            err_str += "table_set is empty.\n"
            err_str += "search_index: " + repr(search_index) + "\n"
            raise InputIntegrityError(err_str)
        elif len(table_set) > 1:
            err_str = "Columns seem to come from multiple tables.\n"
            err_str += "columns_set: " + repr(columns_set) + "\n"
            err_str += "table_set: " + repr(table_set) + "\n"
            err_str += "search_index: " + repr(search_index) + "\n"
            raise InputIntegrityError(err_str)
        else:
            target_table = table_set.pop()

        stmt = "SELECT * FROM {} WHERE ".format(target_table)
        final_search_terms = []

        for this_term in search_index:
            this_condition = ""
            search_term = this_term[2]
            this_condition += "{} {} {}".format(this_term[0], this_term[1], search_term)

            final_search_terms.append(this_condition)

        final_stmt = stmt + " AND ".join(final_search_terms)

        conn = self.get_connection()
        c = conn.cursor()
        headings = self.direct_get_column_headings(target_table)
        if not iterator_return:
            all_results = []
            for row in c.execute(final_stmt):
                this_row = dict()
                for i in range(len(headings)):
                    this_row[headings[i]] = force_unicode(row[i])
                all_results.append(this_row)
                conn.close()
            return all_results
        else:
            return self.iterator_return(final_stmt, headings)

    def iterator_return(self, stmt, headings):
        """
        Python version <3.3 does not allow 'return' with argument inside generators. Thus hiving it off to a separate
        method.
        :param stmt: stmt to be executed on the table.
        :param headings: Headings for the results of the statement
        :return:
        """
        conn = self.get_connection()
        c = conn.cursor()
        for row in c.execute(stmt):
            this_row = dict()
            for i in range(len(headings)):
                this_row[headings[i]] = force_unicode(row[i])
                yield this_row
            else:
                # Finally, when the generator is exhausted, terminating the connection properly
                # Todo: Test this happens
                default_log.info("Connection has closed!")
                conn.close()

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - LOCATIONAL SEARCH AND THE HELPER FUNCTIONS NEEDED TO SUPPORT IT START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    # Algorithm is as follows.
    # The output of the search qiuery parser looks something like ['or', ['and', ['or', ['token', u'titles', u'thing'],
    # ['token', u'creators', u'david']], ['token', 'all', u'simon']], ['or', ['token', u'genres', u'thing'],
    # ['token', u'genres', u'thing']]]
    # which was u'((titles:thing or creators:david) and simon) or genres:thing or genres:thing'
    # 1) Scans down looking for an index which is of the form ['string','string','string']
    # 2) Converts it, in place, into a string.
    # 3) Continues, until the entire tree has been converted
    # 4) Should end up with something which is semantically identical to the initial query, before it was parsed
    def locational_search(self, parsed_query):
        """
        Takes an index parsed from a search query - builds an appropriate search query from that parsed query and
        executes it on the database.
        :param parsed_query: A query parsed by the SearchQueryParser.
        :return:
        """
        parsed_query = deepcopy(parsed_query)
        locations = self.locations
        if self.locations is None:
            wrn_str = "DatabaseDriver doesn't have locations loaded.\n"
            LiuXin_debug_print(wrn_str)

        # The tables which will be needed to include in the inner join can be calculated from the required locations
        required_locations = set()

        # Scans down looking for an instance of an index of the form ['string', 'string', 'string'] to transform them
        while not isinstance(parsed_query, unicode):
            # index_location - used to specify a position within the parsed query tree structure
            index_location = []
            current_level = parsed_query
            while not self.can_index_be_transformed(current_level):
                for i in range(len(current_level)):
                    token = current_level[i]
                    if hasattr(token, "__iter__"):
                        current_level = token
                        index_location.append(i)
                        break
                else:
                    err_str = "Attempt to parse query has failed.\n"
                    err_str += "parsed_query: " + repr(parsed_query) + "\n"
                    raise LogicalError(err_str)

            # Including the location in the list of required locations
            if current_level[0] == "token":
                required_locations.add(current_level[1])

            # Using the index_location as a guide to build some code to actually change the value (because the number of
            # indices is variable and this seems to be the best way to access it)
            transformed_index = self.transform_index(current_level)
            stmt = "parsed_query"
            for value in index_location:
                stmt += force_unicode("[" + force_unicode(value) + "]")
            stmt += " = transformed_index"
            print(stmt)
            exec(stmt)
        print(parsed_query)

    @staticmethod
    def can_index_be_transformed(target_index):
        """
        Tests to see if an index can be transformed into pure string form.
        :param target_index:
        :return:
        """
        if not hasattr(target_index, "__iter__"):
            return False

        if len(target_index) != 3:
            err_str = "can_index_be_transformed in locational_search has been passed a poorly formed index.\n"
            err_str += "target_index: " + repr(target_index) + "\n"
            raise InputIntegrityError(err_str)
        if hasattr(target_index[1], "__iter__") or hasattr(target_index[2], "__iter__"):
            return False
        else:
            return True

    @staticmethod
    def transform_index(target_index):
        """
        Takes an index - transforms it into intermediate form.
        :param target_index:
        :return:
        """

        if target_index[0] == "token":
            return target_index[1] + ':"' + target_index[2] + '"'
        elif target_index[0] == "or":
            return "( " + target_index[1] + " OR " + target_index[2] + " )"
        elif target_index[0] == "and":
            return "( " + target_index[1] + " AND " + target_index[2] + " )"
        else:
            err_str = "transform_index in locational_search has failed while trying to parse a query.\n"
            err_str += "target_index: " + repr(target_index) + "\n"
            raise LogicalError(err_str)

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - CERTAIN FUNCTIONS ARE SUFFICIENTLY COMPLEX THEY ARE HARD TO IMPLEMENT IN PURE SQL - RUN AS PART OF THE DATABASE
    #   CHECKING CYCLE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    def direct_set_full_column(self, target_table):
        """
        Rows which are part of a tree structure have a _full column. This is a string representation of their place in
        the tree structure. This method populates the full column for the target table.
        :return:
        """
        target_table = deepcopy(target_table)
        conn = self.get_connection()
        target_table_id_column = self._get_id_column(target_table)
        target_table_full_column = self.get_full_column_name(target_table)
        if target_table_full_column is None:
            err_str = "Cannot set full column - table: {} - does not have one".format(target_table)
            raise InputIntegrityError(err_str)

        target_table_display_column = self.get_display_column(target_table)

        for row in self.direct_get_row_dict_iterator(target_table):

            row_id = row[target_table_id_column]
            agg_value = self.tree_aggregator(
                table=target_table,
                table_display_column=target_table_display_column,
                table_row_id=row_id,
            )

            final_stmt = "UPDATE `{}` SET {} = ? WHERE {} = ?;".format(
                target_table, target_table_full_column, target_table_id_column
            )

            try:
                conn.execute(final_stmt, (agg_value, row_id))
                conn.commit()
            except sqlite3.OperationalError as e:
                err_str = "Unable to complete operation.\n"
                err_str = default_log.log_exception(err_str, e, "ERROR", ("final_stmt", final_stmt), ("row", row))
                raise DatabaseDriverError(err_str)
        else:
            # If the code every reaches this point everything should have worked already
            return True

    def get_display_column(self, table_name):
        """
        Gets the display column for a table (currently based off the shortest column which is not the id column)
        :param table_name:
        :return display_column:
        """
        table_name = deepcopy(table_name)
        table_id_column = self._get_id_column(table_name)
        tables_and_columns = self.direct_get_tables_and_columns()
        # Don't want to accidentally remove the title_id from the tables_and_columns cache
        column_names = deepcopy(tables_and_columns[table_name])

        # a display column should never be the id column. Removing it.
        column_names.remove(table_id_column)
        column_names.sort(key=lambda x: len(x))
        if len(column_names) == 0:
            err_str = "table_name seems to only have an id column. If that.\n"
            err_str += "table_name: " + repr(table_name) + "\n"
            raise DatabaseIntegrityError(err_str)
        else:
            return column_names[0]

    def get_full_column_name(self, target_table):
        """
        Rows which are part of a tree like structure should have a full column. Use to store a string representation of
        the
        This method finds and returns that
        column.
        :param target_table:
        :return target_table_full_column:
        """
        table_and_columns = self.direct_get_tables_and_columns()
        columns = table_and_columns[target_table]

        full_pat = r"^.*_full$"
        full_re = re.compile(full_pat, re.I)
        for column in columns:
            if full_re.match(column) is not None:
                return column
        else:
            return None

    # Todo: This is absolutely, hideously, heinously inefficient
    def direct_set_tree_ids(self, table):
        """
        Every tree in a tree like structure should have a unique id assigned to every row in that tree.
        This function ensures that.
        :param table:
        :return:
        """
        table = deepcopy(table)
        table_id_column = self._get_id_column(table)
        table_tree_id_column = self.get_tree_id_column(table)
        if table_tree_id_column is None:
            err_str = "Cannot set_tree_ids - there doesn't seem to be a tree id for this table - {}".format(table)
            raise InputIntegrityError(err_str)
        table_display_column = self.get_display_column(table)
        conn = self.get_connection()

        stmt = "UPDATE {} SET {} = ? WHERE {} = ?".format(table, table_tree_id_column, table_id_column)
        final_stmt = stmt

        for row in self.direct_get_row_dict_iterator(table):

            row_id = row[table_id_column]

            root_series = self.get_root_series(row)
            root_phash = "{}_{}".format(root_series[table_id_column], root_series[table_display_column])
            conn.execute(final_stmt, (root_phash, row_id))
            conn.commit()

        else:

            return True

    def get_tree_id_column(self, target_table):
        """
        Each table which is in the form of a tree like structure has a tree_id column. The entry in this column is
        unique for every tree in the table. If none is present then it's assumed that the table isn't organized in a
        tree like structure.
        :param target_table:
        :return:
        """
        table_and_columns = self.direct_get_tables_and_columns()
        columns = table_and_columns[target_table]

        full_pat = r"^.*_tree_id$"
        full_re = re.compile(full_pat, re.I)
        for column in columns:
            if full_re.match(column) is not None:
                return column
        else:
            return None

    def get_id_from_row_dict(self, row_dict):
        """
        Takes a row. Extracts an id from it if possible. If not returns False
        :param row_dict:
        """
        row_table = self.__identify_table_from_row(row_dict)
        row_id_column = self._get_id_column(row_table)

        if row_id_column not in row_dict.keys():
            return False
        else:
            return row_dict[row_id_column]

    # Todo: Should be called direct_get_root_series
    def direct_get_root_series(self, start_row):
        return self.get_root_series(start_row)

    def get_root_series(self, start_row):
        """
        Gets the row at the root of the given tree. In the case of a trivial tree just returns the given row.
        :param start_row:
        :return root_row:
        """
        return self.__get_linear_row_index(start_row)[0]

    def get_all_tree_rows(self, start_row):
        """
        Starts from a series. Walks up the series tree, and then walks back down, collecting all references in one set.
        This is going to take a number of database operations.
        :param start_row:
        :return:
        """
        row_table = self.__identify_table_from_row(start_row)
        row_parent_column = self.__get_parent_column_name(row_table)
        root_series = self.get_root_series(start_row)

        row_pool = [root_series]

        found_series = []

        # the series pool contains the series which we're currently working with as with walk down the series tree
        # series in the series pool haven't had all their children series found yet
        # once a series has had all it's children series found it's transferred to found series
        while len(row_pool) != 0:

            current_series = row_pool.pop()
            current_id = current_series["series_id"]
            # finds all the series which refer to the current_series in the series_parent column
            child_rows = self.direct_search_table(table=row_table, column=row_parent_column, search_term=current_id)
            for row in child_rows:
                if row not in row_pool:
                    row_pool.append(row)
            if current_series not in found_series:
                found_series.append(current_series)

        return found_series

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - CUSTOM COLUMN CREATION METHODS

    # Todo: Merge with zero_prop_cache - they do the same thing
    def call_after_table_changes(self):
        """
        Call after any operations which might change the table content of the database.

        :return:
        """
        self._zero_prop_cache()
        self.tables_and_columns = None

    @staticmethod
    def _validate_table_name(table_name):
        """
        Validate that the given table name is valid.
        :param table_name: The name of the table to preform validation for.
        :return:
        """
        table_name_regex = r"^[a-zA-Z_]+$"
        if re.match(table_name_regex, table_name):
            return True
        return False

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO DEAL WITH TRIGGERS START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    def direct_get_triggers(self):
        """
        Returns a list of all triggers defined on the database.
        Returns an empty set if there are
        :return:
        """
        conn = self.get_connection()
        stmt = "SELECT name FROM sqlite_master WHERE type = 'trigger';"
        triggers = []
        try:
            for row in conn.execute(stmt):
                triggers.append(row[0])
            conn.close()
        except sqlite3.OperationalError:
            conn.close()
            raise
        return triggers

    def direct_drop_triggers(self, triggers):
        """
        Takes a list of triggers by name - drops all of them from the DatabasePing.
        :return:
        """
        conn = self.get_connection()
        stmt = "DROP TRIGGER {};"
        try:
            for trigger in triggers:
                current_stmt = stmt.format(trigger)
                conn.execute(current_stmt)
                conn.commit()
            conn.close()
        except sqlite3.OperationalError:
            conn.close()
            raise
        return True

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO DEAL WITH THE DB METADATA START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    def __initialize_md(self):
        """
        Checks that the MetaData table has one and only one row.
        :return None: All changes are made internally to the database
        """
        md_rows = self.direct_get_all_rows("database_metadata")
        if len(md_rows) == 0:
            md_row_dict = dict()
            md_row_dict["database_metadata_scratch"] = "None"
            self.direct_add_simple_row_dict(md_row_dict)
            return True
        elif len(md_rows) == 1:
            return True
        else:
            err_str = "database_metadata table has more than 1 row.\n"
            err_str += "md_rows: " + pprint.pformat(md_rows) + "\n"
            raise DatabaseIntegrityError(err_str)

    def direct_write_metadata(self, md_field_name, md_field_value):
        """
        Allows for storing data in the MetaData table of the database.
        The table only has one row - if another value is given then it will be written over.
        :param md_field_name: The name of then field where the value will be stored
        :param md_field_value: The value of the field.
        :return:
        """
        md_field_name = force_unicode(deepcopy(md_field_name))

        # Check that the field name exists and can be written to
        if not md_field_name.startswith("database_metadata_"):
            n_md_field_name = "database_metadata_" + md_field_name
        else:
            n_md_field_name = md_field_name
        allowed_values = self.direct_get_column_headings("database_metadata")
        if n_md_field_name not in allowed_values:
            err_str = "Metadata cannot be written to database. - md_field_name is not recognized.\n"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("md_field_name", md_field_name),
                ("n_md_field_name", n_md_field_name),
                ("md_field_value", md_field_value),
            )
            raise ValueError(err_str)

        # After this method has been run there should be one and only one row in the 'database_metadata' table
        self.__initialize_md()
        md_rows = self.direct_get_all_rows("database_metadata")
        md_row = md_rows[0]
        md_row[n_md_field_name] = md_field_value
        self.direct_update_row_dict(row_dict=md_row)

    def direct_read_metadata(self, md_field_name):
        """
        Read metadata from the database.
        :param md_field_name:
        :return:
        """
        md_field_name = force_unicode(deepcopy(md_field_name))

        # Check that the field name exists and can be written to
        if not md_field_name.startswith("database_metadata_"):
            n_md_field_name = "database_metadata_" + md_field_name
        else:
            n_md_field_name = md_field_name
        allowed_values = self.direct_get_column_headings("database_metadata")
        if n_md_field_name not in allowed_values:
            err_str = "Metadata cannot be read from the database - md_field_name is not recognized.\n"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("md_field_name", md_field_name),
                ("n_md_field_name", n_md_field_name),
            )
            raise ValueError(err_str)

        # After this method has been run there should be one and only one row in the 'database_metadata' table
        self.__initialize_md()
        md_rows = self.direct_get_all_rows("database_metadata")
        md_row = md_rows[0]
        candidate_value = md_row[n_md_field_name]
        if candidate_value.lower() == "none":
            return None
        else:
            return deepcopy(candidate_value)

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - HELPER FUNCTIONS THAT WILL BE ADDED TO THE DATABASE CONNECTION START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    # HELPER FUNCTIONS TO RUN THE TREE AGGREGATOR START HERE
    def tree_aggregator(self, table, table_display_column, table_row_id):
        """
        Builds a string, starting at the current index and working it's way back up to the root of the tree.
        Useful for expressing the position of an element in a tree in a single string.
        For example, used with series it would produce ....: series_grandfather: series_father: series
        :param table: The table to search in
        :param table_display_column: The column to be used as a display column
        :param table_row_id: The id to start at
        :return return_str: ....: row_grandfather: row_father: row - All being the display columns at each level
        """
        start_row = self.direct_get_row_dict_from_id(table, table_row_id)
        row_column_index = self.__get_linear_index_of_columns(start_row, table_display_column)

        row_column_index = [six_unicode(force_unicode(_)) for _ in row_column_index]
        return_str = ": ".join(row_column_index)
        return return_str

    # Todo - Promote this to an actual method with tests
    def __get_linear_index_of_columns(self, start_row, display_column):
        """
        Takes a starting row. Calls get_linear_row_index to get a list of rows with order .......... ->
        grandparent_series -> parent_series -> series. Extracts the designated column from each of these rows to form a
        linear index of columns. Could be used, for example, in series to create a full series string.
        What is actually used is a stripped down version of these functions, which has been added directly to the
        connection.
        :param start_row:
        :param display_column: What column do you want as a display for the
        :return:
        """
        display_column = deepcopy(display_column)
        if display_column not in start_row:
            err_str = "Warning - get_linear_index_of_columns failed. \n"
            err_str += "display_column not found in start_row.\n"
            err_str += "start_row: " + repr(start_row) + "\n"
            err_str += "display_column: " + repr(display_column) + "\n"
            raise InputIntegrityError(err_str)
        row_index = self.__get_linear_row_index(start_row)
        row_column_index = []

        for row in row_index:
            if display_column not in row:
                err_str = "Warning - get_linear_index_of_columns failed. \n"
                err_str += "display_column not found in a row.\n"
                err_str += "start_row: " + repr(start_row) + "\n"
                err_str += "display_column: " + repr(display_column) + "\n"
                err_str += "row_column_index: " + repr(row_column_index) + "\n"
                err_str += "row_index: " + repr(row_index) + "\n"
                raise InputIntegrityError(err_str)
            row_column_index.append(row[display_column])

        return row_column_index

    def __get_linear_row_index(self, start_row):
        """
        Takes a starting row. Iterates up the tree building an index of all the rows_dicts as it goes.
        :param start_row: A Row that the method will iterate back from
        :return tree_row_index: An index of all the Rows in the tree forwards e.g.
        .......... -> grandparent_series -> parent_series -> series
        """
        start_row_dict = start_row
        row_table = self.__identify_table_from_row(start_row_dict)
        row_parent_column = self.__get_parent_column_name(row_table)
        linear_index = []
        current_row = start_row_dict
        try:
            current_parent = start_row_dict[row_parent_column]
            if current_parent is None:
                linear_index.append(start_row)
                return linear_index

            elif isinstance(current_parent, int):
                pass

            elif force_unicode(current_parent).upper() == "NONE":
                linear_index.append(start_row)
                return linear_index

        except KeyError:
            linear_index.append(start_row)
            return linear_index

        while force_unicode(six_unicode(current_parent)).upper() != "NONE":
            # extracting the current parent id
            try:
                current_parent = current_row[row_parent_column]
                if current_parent == "NONE":
                    linear_index = [current_row] + linear_index
                    return linear_index
            except KeyError:
                linear_index = [current_row] + linear_index
                return linear_index

            linear_index = [current_row] + linear_index
            if current_parent != "None" and current_parent is not None:
                current_row = self.direct_get_row_dict_from_id(table=row_table, row_id=current_parent)
            else:
                return linear_index

        # If the program ever reaches this point something has gone badly wrong
        if VERBOSE_DEBUG:
            err_str = "get_linear_row_string has failed.\n"
            err_str += "start_row: " + repr(start_row_dict) + "\n"
            raise DatabaseIntegrityError(err_str)
        else:
            raise DatabaseIntegrityError

    # We assume that the row has an element ending with "_parent". This (it is hoped) is a pointer backwards up the tree
    # to the row above it.
    def __get_parent_column_name(self, table_name):
        """
        Takes a table name. Works out if the table has an element ending in "_parent" and returns the parent column name
        if it exists.
        Returns False otherwise
        :param table_name:
        :return parent_column_name/False:
        """
        table_name = deepcopy(table_name)
        tables_and_columns = self.direct_get_tables_and_columns()
        if table_name not in tables_and_columns:
            if VERBOSE_DEBUG:
                err_str = "Input to get_parent_column_name not recognized.\n"
                err_str += "table_name: " + repr(table_name) + "\n"
                err_str += "is not recognized.\n"
                raise InputIntegrityError(err_str)
            else:
                raise InputIntegrityError
        column_names = tables_and_columns[table_name]

        candidate_index = []
        for name in column_names:
            if name.lower().endswith("_parent"):
                candidate_index.append(name)

        if len(candidate_index) > 1:
            err_str = "Multiple candidates found to be the _parent row.\n"
            err_str += "All candidates: " + repr(candidate_index) + "\n"
            raise DatabaseIntegrityError(err_str)
        elif len(candidate_index) == 1:
            return candidate_index[0]
        elif len(candidate_index) == 0:
            return False
        else:
            raise LogicalError

    def get_table_sqlite(self, table, conn=None):
        """
        Gets the SQLite for the given table. Useful for debugging.
        :param table:
        :param conn: Allows passing in a connection - provided as this is intended to be used for debugging.
        """
        if conn is None:
            conn = self.get_connection()

        stmt = "SELECT SQL FROM sqlite_master WHERE TYPE = 'table' AND NAME = '{}';".format(table)
        for row in conn.execute(stmt):
            return row[0]
        else:
            raise InputIntegrityError("Table name was probably not found")

    @staticmethod
    def _get_table_col_base(table_name):
        """
        Returns the base name for a column in the given table. e.g. "title" for "titles"
        :param table_name: Return the base column for this table
        :return:
        """
        from LiuXin.utils.general_ops.language_tools import plural_singular_mapper

        return plural_singular_mapper(table_name)

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO DEAL WITH THE DIRTIED RECORDS QUEUE START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    def dirty_record(self, table, table_id, reason):
        """
        Add a record to the dirtied dictionary.
        :param table:
        :param table_id:
        :param reason:
        :return:
        """
        if table not in self.tables:
            wrn_str = "Unable to dirtied record - table not found.\n"
            default_log.log_variables(
                wrn_str,
                "WARNING",
                ("table", table),
                ("table_id", table_id),
                ("reason", reason),
            )
        dirtied_records.put((table, table_id, reason))

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - DIRECT SQLITE EXECUTION METHODS START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    def direct_get_max(self, column):
        """
        Get the maximum value from a column and return it.
        """
        col_table = self.__identify_table_from_column(column)
        stmt = "SELECT MAX({}) FROM {};".format(column, col_table)

        conn = self.get_connection()
        max_val = conn.execute(stmt).next()[0]
        return max_val

    def direct_get_min(self, column):
        """
        Get the maximum value from a column and return it.
        """
        col_table = self.__identify_table_from_column(column)
        stmt = "SELECT MIN({}) FROM {};".format(column, col_table)

        conn = self.get_connection()
        min_val = conn.execute(stmt).next()[0]
        return min_val

    # Todo: Add zero methods for all the data caches after any of these are used
    # Ideally these would never be used. They are here for testing,
    def direct_execute(self, sql, values=None):
        """
        Execute SQL directly on the database.
        :param sql: SQL code to execute on the database
        :param values: The values to execute with the code.
        """
        if isinstance(values, int):
            values = (force_unicode(values),)

        conn = self.get_connection()
        try:
            with conn as c:
                if values is not None:
                    query_results = c.execute(sql, values)
                    c.commit()
                    return query_results
                else:
                    query_results = c.execute(sql)
                    c.commit()
                    return query_results
        except sqlite3.OperationalError as e:
            err_str = "Attempting to execute that SQL caused an operational error."
            err_str = default_log.log_exception(err_str, e, "ERROR", ("sql", sql), ("values", values))
            raise DatabaseDriverError(err_str)
        except ValueError as e:
            err_str = "Attempting to execute that SQL caused a ValueError"
            err_str = default_log.log_exception(err_str, e, "ERROR", ("sql", sql), ("values", values))
            raise DatabaseDriverError(err_str)
        except Exception as e:
            err_str = "Attempting to execute that SQL threw an Exception"
            err_str = default_log.log_exception(err_str, e, "ERROR", ("sql", sql), ("values", values))
            raise DatabaseDriverError(err_str)
        finally:
            conn.commit()
            self.conn.commit()
            self.refresh()

    def execute_sql(self, sql, values=None):
        """
        Front end for the direct_execute method.
        :param sql:
        :param values:
        :return:
        """
        self.direct_execute(sql=sql, values=values)

    def direct_executemany(self, sql, values=None):
        """
        Executes many statements on the database.
        Tries to preform sensible input transforms on the values before executing them. This might lead to some problems
        but I can't immediately think of cases where they would, and it's a bit more convenient this way.
        e.g. if values=("Some string", "Another string") these will be transformed to
                       (("Some string", ), ("Another string", )) before any attempt is made to execute them directly.
        (As, usually, you don't supply bindings in the form of chars in a string - which seems to be the default
         assumption SQLite makes)
        :param sql:
        :param values:
        :return:
        """
        # Preflight the values to try and transform them into something that'll behave as expected
        if isinstance(values, tuple):
            new_values = list()
            for update_val in values:
                if isinstance(update_val, (basestring, int, float)):
                    new_values.append((update_val,))
                else:
                    new_values.append(update_val)
            values = tuple(new_values)

        # Todo: Theoretically possibly to fool the database into doing manifestly stupid shit here by feeding in the
        conn = self.get_connection()
        try:
            with conn as c:
                if values is not None:
                    try:
                        c.executemany(sql, values)
                    except ValueError:

                        try:
                            c.executemany(sql, tuple(values))
                        except ValueError:
                            values = tuple([(v,) for v in values])
                            c.executemany(sql, values)
                else:
                    c.executemany(sql, ())
        except Exception as e:
            err_str = "direct_executemany has failed"
            err_str = default_log.log_exception(err_str, e, "ERROR", ("sql", sql), ("values", values))
            raise DatabaseDriverError(err_str)

        conn.commit()

    def direct_executescript(self, sqlscript):
        """
        Execute a script on the database
        :param sqlscript: A series of statements to execute. Seperated by ;
        """
        conn = self.get_connection()
        try:
            with conn as c:
                c.executescript(sqlscript)
                c.commit()
        except Exception as e:
            err_str = "Executing a script has failed"
            err_str = default_log.log_exception(err_str, e, "ERROR", ("sql_script", sqlscript))
            raise DatabaseDriverError(err_str)
        finally:
            conn.commit()
            self.refresh()

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - CALIBRE EMULATION FUNCTIONS START HERE

    def direct_last_modified(self):
        """
        Returns the last modification time for the databases as a utc (unix time code) timestamp.
        :return:
        """
        return utcfromtimestamp(os.stat(self.database_path).st_mtime)


#
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# HELPER FUNCTIONS WHICH DO NOT NEED THE DATABASE TO WORK START HERE
# ----------------------------------------------------------------------------------------------------------------------

# Helper functions which allow direct use of sets a column of a table


def py_date_converter(date_string):
    """
    The standard datetime adaptor chokes when it's fed a None value.
    :param date_string:
    :return:
    """
    return date_string


def py_set_converter(py_set_string):
    """
    Converted intended to be used with set fields from the database - turns them into sets of unicode strings.
    Takes a string from the database and returns it as a set.
    :param py_set_string:
    :return py_set:
    """
    py_set_string = deepcopy(py_set_string)
    # Accounting for the way SQL escapes quotes
    py_set_string = py_set_string.replace("''", "'")

    py_set = set()
    last_char = " "
    current_string = ""
    accumulation_mode = False
    for char in py_set_string:
        if char == "'" and last_char != "\\":
            accumulation_mode = not accumulation_mode
            if current_string:
                py_set.add(current_string)
                current_string = ""
        elif char == "'" and last_char == "\\":
            # The SQL \ used to escape a quote is no longer needed
            if accumulation_mode:
                current_string = current_string[:-1]
                current_string += char
            else:
                err_str = "parsing a string into a python set has gone wrong.\n"
                err_str += "py_set_string: " + repr(py_set_string) + "\n"
                raise DatabaseDriverError(err_str)
        elif char == '"' and last_char == "\\":
            # The SQL \ used to escape a double quote is no longer needed
            if accumulation_mode:
                current_string = current_string[:-1]
                current_string += char
            else:
                err_str = "parsing a string into a python set has gone wrong.\n"
                err_str += "py_set_string: " + repr(py_set_string) + "\n"
        elif accumulation_mode:
            current_string += char
        else:
            if char != ",":
                err_str = "parsing a string into a python set has gone wrong.\n"
                err_str += "py_set_string: " + repr(py_set_string) + "\n"
                raise DatabaseDriverError(err_str)
        last_char = char
    return py_set


def py_set_adapter(py_set):
    """
    Takes a set - turning it into a string suitable for storing within an SQLite databaase, which can be parsed back out
    by the py_set_converted function.
    :param py_set:
    :return:
    """
    py_set = deepcopy(py_set)
    py_list = []
    for element in py_set:
        # Coerce to unicode, escape any SQL special characters, then add to the list of elements
        element = force_unicode(element)
        element = element.replace("'", "''")
        element = element.replace('"', '\\"')
        py_list.append(element)
    return "'" + "','".join(py_list) + "'"


# Todo: Make safe - can currently be used to execute arbitrary code
def py_list_converter(py_list_string):
    """
    Converter intended to be used with list fields from the database - turns them into a list of unicode strings.
    Takes a string from the database and returns it as a list of unicode strings.
    :param py_list_string:
    :return py_list:
    """
    from LiuXin.utils.general_ops.io_ops import safe_parse_string_list

    return safe_parse_string_list(py_list_string)


def py_list_adapter(py_list):
    """
    Takes a list and turns it into a string suitable for inserting into the database.
    :param py_list:
    :return:
    """
    py_list_str = force_unicode(py_list)
    if py_list_str.startswith("["):
        py_list_str = py_list_str[1:]
    if py_list_str.endswith("]"):
        py_list_str = py_list_str[:-1]
    return py_list_str


def py_dict_converter(py_dict_string):
    """
    Converter intended to be used to store dictionaries on the database.
    Takes a string from the database and returns it as a dictionary
    :param py_dict_string:
    :return:
    """
    rtn_dict = dict()
    py_dict_string = deepcopy(py_dict_string)
    working_str = "rtn_dict = " + py_dict_string
    exec(working_str)
    return rtn_dict


def py_dict_adapter(py_dict):
    """
    Takes a dictionary and turns it into a string suitable for storing on the database.
    :param py_dict:
    :return:
    """
    return force_unicode(py_dict)


class PyListAggregate:
    """
    Aggregation function intended to be used with SQLite. Preserves order and builds a list from the given elements.
    Called this to keep with the PySet convention and for clarity.
    """

    def __init__(self):
        self.py_list = []

    def step(self, value):
        self.py_list.append(value)

    def finalize(self):
        return py_list_adapter(self.py_list)


class PySetAggregate:
    """
    Aggregation function intended to be used with SQLite. Does not preserve order.
    Called this so as to not conflict with the SQL/SQLite SET keyword.
    """

    def __init__(self):
        self.py_set = set()

    def step(self, value):
        self.py_set.add(value)

    def finalize(self):
        return "'" + "','".join(self.py_set) + "'"


# Helper functions used to make aggregate short strings (for example makes the creators_sort field for a title.
class SortAggregate:
    """
    Aggregation function intended to be used with SQLite.
    Takes strings. Concats them separated by a '&'. Preserving order.
    """

    def __init__(self):
        self.py_list = []

    def step(self, value):
        if value.startswith("'"):
            value = value[1:]
        if value.endswith("'"):
            value = value[:-1]
        self.py_list.append(value)

    def finalize(self):
        return " & ".join(self.py_list)


class SqliteAumSortedConcatenate:
    """
    String concatenation aggregator for the author sort map
    """

    def __init__(self):
        self.ctxt = dict()

    def step(self, ndx, author, sort, link):
        if author is not None:
            self.ctxt[ndx] = ":::".join((six_unicode(author), six_unicode(sort), six_unicode(link)))

    def finalize(self):
        ctxt = self.ctxt
        keys = list(iterkeys(ctxt))
        l = len(keys)
        if l == 0:
            return None
        if l == 1:
            return ctxt[keys[0]]
        return ":#:".join([ctxt[v] for v in sorted(keys)])


class SqliteSortedConcatenate:
    def __init__(self, sep=","):
        self.sep = sep
        self.ctxt = dict()

    def step(self, ndx, value):
        if value is not None:
            self.ctxt[ndx] = value

    def finalize(self):
        ctxt = self.ctxt
        if len(ctxt) == 0:
            return None
        return self.sep.join(map(ctxt.get, sorted(iterkeys(ctxt))))


class SqliteIdentifiersConcat:
    def __init__(self):
        self.ctxt = []

    def step(self, key, val):
        self.ctxt.append("%s:%s" % (key, val))

    def finalize(self):
        return ",".join(self.ctxt)


# Extra collators {{{
def pynocase(one, two, encoding="utf-8"):
    if isbytestring(one):
        try:
            one = one.decode(encoding, "replace")
        except:
            pass
    if isbytestring(two):
        try:
            two = two.decode(encoding, "replace")
        except:
            pass
    return force_cmp(one.lower(), two.lower())


def _author_to_author_sort(x):
    if not x:
        return ""
    return author_to_author_sort(x.replace("|", ","))


def icu_collator(s1, s2):
    return force_cmp(sort_key(force_unicode(s1, "utf-8")), sort_key(force_unicode(s2, "utf-8")))


# }}}


# Unused aggregators {{{
def Concatenate(sep=","):
    """
    String concatenation aggregator for sqlite
    :param sep:
    :return:
    """

    def step(ctxt, value):
        if value is not None:
            ctxt.append(value)

    def finalize(ctxt):
        if not ctxt:
            return None
        return sep.join(ctxt)

    return [], step, finalize


def StupidConcatenate(sep=","):
    """
    String concatenation aggregator for sqlite
    :param sep:
    :return:
    """

    def step(ctxt, value):
        if value is not None:
            ctxt.append(value)

    def finalize(ctxt):
        assert True is False, sep.join(ctxt)

    return [], step, finalize


def SortedConcatenate(sep=","):
    """
    String concatenation aggregator for sqlite, sorted by supplied index
    :param sep:
    :return:
    """

    def step(ctxt, ndx, value):
        if value is not None:
            ctxt[ndx] = value

    def finalize(ctxt):
        if len(ctxt) == 0:
            return None
        return sep.join(map(ctxt.get, sorted(iterkeys(ctxt))))

    return {}, step, finalize


def IdentifiersConcat():
    """
    String concatenation aggregator for the identifiers map
    :return:
    """

    def step(ctxt, key, val):
        ctxt.append("%s:%s" % (key, val))

    def finalize(ctxt):
        return ",".join(ctxt)

    return [], step, finalize


def AumSortedConcatenate():
    """
    String concatenation aggregator for the author sort map
    :return:
    """

    def step(ctxt, ndx, author, sort, link):
        if author is not None:
            ctxt[ndx] = ":::".join((author, sort, link))

    def finalize(ctxt):
        keys = list(iterkeys(ctxt))
        l = len(keys)
        if l == 0:
            return None
        if l == 1:
            return ctxt[keys[0]]
        return ":#:".join([ctxt[v] for v in sorted(keys)])

    return {}, step, finalize


# }}}


class DynamicFilter(object):
    """
    Calibre filter - no longer used - present for ledgacy comatibility with older calibre databases.
    """

    def __init__(self, name):
        self.name = name
        self.ids = frozenset([])

    def __call__(self, id_):
        return int(id_ in self.ids)

    def change(self, ids):
        self.ids = frozenset(ids)
