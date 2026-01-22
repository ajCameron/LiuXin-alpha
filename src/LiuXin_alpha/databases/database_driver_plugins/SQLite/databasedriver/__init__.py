# This class is intended to be linked to a specific instance of a DatabasePing.
# The internals of the database are deliberately separated here, to make changes more directly without influencing the
# DatabasePing class itself
# Also should allow live switching of the DatabaseDriver in and out (i.e. if you want to switch from SQLite to SQL -
# separating the DatabasePing logic and the Driver logic would seem to make sense).

from __future__ import print_function

import codecs
import gc
import time
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

from six import iterkeys, iteritems, string_types

from LiuXin_alpha.utils.date import utcfromtimestamp

from LiuXin_alpha.utils.logging import LiuXin_print, LiuXin_debug_print, LiuXin_warning_print

from LiuXin_alpha.constants import VERBOSE_DEBUG

from LiuXin_alpha.databases.database_driver_plugins.SQLite.database_generator.database_generator import (
    create_new_database,
)
from LiuXin_alpha.databases.database_driver_plugins.SQLite.macros import SQLiteDatabaseMacros
from LiuXin_alpha.databases.database_driver_plugins.SQLite.custom_columns import (
    SQLiteCustomColumnsDriverMixin,
)

from LiuXin_alpha.errors import LogicalError, DatabaseDriverError, RowIntegrityError, InputIntegrityError, DatabaseIntegrityError

from LiuXin_alpha.utils.language_tools.lx_name_manip import authors_str_to_sort_str
from LiuXin_alpha.utils.paths import path_ok

from LiuXin_alpha.databases.maintenance_bot import run_ta_updates
from LiuXin_alpha.databases.backup import backup_local_file

from LiuXin_alpha.preferences import preferences


from LiuXin_alpha.utils.language_tools import plural_singular_mapper

from LiuXin_alpha.utils.logging import default_log


from LiuXin_alpha.utils.ptempfiles import TemporaryFile
from LiuXin_alpha.utils.localization import _
from LiuXin_alpha.utils.libraries.liuxin_six import force_cmp, user_input, force_unicode
from LiuXin_alpha.utils.storage.local.filenames import atomic_rename

from LiuXin_alpha.metadata.utils import author_to_author_sort, title_sort

from LiuXin_alpha.databases.database_driver_plugins.SQLite.utility_mixins import SQLiteTableLinkingMixin

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode, six_unicode as unicode

from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver import SQLBaseDriver

from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.utils import *
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.utils import _author_to_author_sort

from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.calibre_emulation_mixin import CalibreEmulationMixin
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.sql_execution_mixin import SQLExecutionMixin
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.math_mixin import MathFunctionsMixin
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.dirty_records_mixin import DirtyRecordsMixin
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.table_names_mixin import TableNamesMixin
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.tree_mixjn import TreeMethodsMixin
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.metadata_mixin import MetadataMethodMixin
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.triggers_mixin import TriggersMixin
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.search_mixin import SearchMixin
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.value_casting_mixin import ValueCastingMixin
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.book_group_mixin import BookGroupMixin
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.delete_mixin import DeleteMixin
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.add_mixin import AddingMixin
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.update_mixin import UpdateMixin
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.view_mixin import ViewMixin
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.table_creation_mixin import TableCreationMixin




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
    SearchMixin,
    TriggersMixin,
    BookGroupMixin,
    DeleteMixin,
    AddingMixin,
    UpdateMixin,
    ViewMixin,
    TableCreationMixin
):
    """
    Represents a collection of all the methods needed to interface with an actual database.
    """

    def __init__(self, db_metadata, db=None, set_conn=True, dirty_records_queue=None):
        """
        Initializing the class with db_metadata.

        Which is an object assumed to have a dictionary like interface which
        provides all the necessary fields to connect to a database of the given type.
        This DatabaseDriver (SQLite) requires the database_path. That's about it.
        Others may need a username and password or similar.
        :param db_metadata:
        :param db: The database this process is driving. Hopefully infinite recursion will not result.
        :param set_conn: Set the globally used connection for the class
        :return:
        """
        self._create_new_database = create_new_database

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

    # Todo: We need a registyr to make sure we have singleton connections to each db
    # Todo: We need a registyr to make sure we have singleton connections to each db
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
        encoding = next(conn.execute("PRAGMA encoding"))[0]
        conn.create_collation("PYNOCASE", partial(pynocase, encoding=encoding))

        return self._register_open_connection(conn)

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
        Dump the database to SQL, restore it into a fresh temp db, then atomically replace.

        This is a pure-sqlite3 implementation (no APSW dependency).

        :param callback: Report progress.
        :param sql: Optional SQL bytes/str to prepend to the dump before restoration.
        """
        if callback is None:

            def callback(x):
                return x

        uv = int(self.user_version)

        with TemporaryFile(suffix=".sql") as fname:

            # --------------------
            # Write dump (optionally prefixed with extra SQL)
            # --------------------
            callback(_("Dumping database to SQL") + "...")

            prefix_sql = ""
            if sql is not None:
                # Historically, callers pass extra SQL to be executed *before* the dump is restored.
                # Treat this as a prefix rather than a complete override of the dump.
                if isinstance(sql, bytes):
                    prefix_sql = sql.decode("utf-8")
                else:
                    prefix_sql = str(sql)

            dump_conn = self.get_connection()
            try:
                with open(fname, "w", encoding="utf-8", newline="\n") as buf:
                    if prefix_sql:
                        buf.write(prefix_sql)
                        if not prefix_sql.endswith("\n"):
                            buf.write("\n")

                    for line in dump_conn.iterdump():
                        buf.write(line)
                        if not line.endswith("\n"):
                            buf.write("\n")
            finally:
                try:
                    dump_conn.close()
                except Exception:
                    pass

            # --------------------
            # Restore into temp db
            # --------------------
            with TemporaryFile(suffix="_tmpdb.db", dir=os.path.dirname(self.database_path)) as tmpdb:
                callback(_("Restoring database from SQL") + "...")

                restore_conn = SQLite_Connection(tmpdb, detect_types=sqlite3.PARSE_DECLTYPES)
                # Allow iterdump()'s explicit BEGIN/COMMIT statements to run without
                # sqlite3 implicitly starting its own transaction (fixes 'cannot start a transaction within a transaction').
                restore_conn.isolation_level = None
                try:
                    # Ensure compat collations/functions exist during schema creation.
                    restore_conn.execute("PRAGMA foreign_keys=ON")
                    restore_conn.create_function("title_sort", 1, title_sort)
                    restore_conn.create_function("author_to_author_sort", 1, _author_to_author_sort)
                    restore_conn.create_function("uuid4", 0, lambda: str(uuid.uuid4()))
                    restore_conn.create_function("books_list_filter", 1, lambda x: 1)
                    restore_conn.create_collation("icucollate", icu_collator)

                    # Register the custom collator (ported from calibre)
                    encoding = next(restore_conn.execute("PRAGMA encoding"))[0]
                    restore_conn.create_collation("PYNOCASE", partial(pynocase, encoding=encoding))

                    # Stream the SQL file to avoid loading huge dumps into memory
                    statement = ""
                    with codecs.open(fname, "r", encoding="utf-8") as f:
                        for line in f:
                            statement += line
                            if sqlite3.complete_statement(statement):
                                st = statement.strip()
                                if st:
                                    restore_conn.execute(st)
                                statement = ""
                        if statement.strip():
                            restore_conn.execute(statement)

                    restore_conn.execute("PRAGMA user_version=%d;" % uv)
                    restore_conn.commit()
                finally:
                    try:
                        restore_conn.close()
                    except Exception:
                        pass

                self.close()
                try:
                    atomic_rename(tmpdb, self.database_path)
                finally:
                    self.reopen()


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
