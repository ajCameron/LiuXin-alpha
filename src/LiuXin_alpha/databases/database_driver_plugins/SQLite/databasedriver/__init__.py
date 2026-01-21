# This class is intended to be linked to a specific instance of a DatabasePing.
# The internals of the database are deliberately separated here, to make changes more directly without influencing the
# DatabasePing class itself
# Also should allow live switching of the DatabaseDriver in and out (i.e. if you want to switch from SQLite to SQL -
# separating the DatabasePing logic and the Driver logic would seem to make sense).

from __future__ import print_function

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

from six import iterkeys, iteritems, string_types

from LiuXin_alpha.utils.date import utcfromtimestamp

from LiuXin_alpha.utils.logging import LiuXin_print, LiuXin_debug_print, LiuXin_warning_print
from LiuXin_alpha.utils.terminal import y_n_input

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

from LiuXin_alpha.utils.ptempfiles import get_scratch_folder

from LiuXin_alpha.utils.ptempfiles import TemporaryFile
from LiuXin_alpha.utils.localization import _
from LiuXin_alpha.utils.libraries.liuxin_six import force_cmp, user_input, force_unicode
from LiuXin_alpha.utils.storage.local.filenames import atomic_rename

from LiuXin_alpha.metadata.utils import author_to_author_sort, title_sort

from LiuXin_alpha.databases.database_driver_plugins.SQLite.utility_mixins import SQLiteTableLinkingMixin

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode, six_unicode as unicode

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
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.book_group_mixin import BookGroupMixin
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.delete_mixin import DeleteMixin
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.add_mixin import AddingMixin
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver.update_mixin import UpdateMixin




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
    SQLiteCustomColumnsDriverMixin,
    SQLiteTableLinkingMixin,
    CalibreEmulationMixin,
    SQLExecutionMixin,
    MathFunctionsMixin,
    DirtyRecordsMixin,
    TableNamesMixin, TreeMethodsMixin, MetadataMethodMixin, TriggersMixin, BookGroupMixin, DeleteMixin, AddingMixin, UpdateMixin):
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

    @property
    def macros(self):
        """
        Returns the macros helper class.

        :return:
        """
        return self._macros

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
        Shutdown the connection to the database - but leave the driver class in existence so it can be re-opened.

        On Windows, failing to close SQLite connections will keep the database file locked.

        :return:
        """
        conn = getattr(self, 'conn', None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        self.conn = None

    def refresh(self):
        """
        Refreshes the database - zeros all cached objects and connects again.

        Important: close any existing connection before replacing it to avoid leaking file handles.

        :return:
        """
        old = getattr(self, 'conn', None)
        if old is not None:
            try:
                old.close()
            except Exception:
                pass
        self.conn = self.get_connection()
        self._zero_prop_cache()

    def reopen(self):
        """
        Re-opens the connection to the database.

        Important: close any existing connection before replacing it to avoid leaking file handles.

        :return:
        """
        old = getattr(self, 'conn', None)
        if old is not None:
            try:
                old.close()
            except Exception:
                pass
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
        encoding = next(conn.execute("PRAGMA encoding"))[0]
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
            # Write dump
            # --------------------
            if sql is None:
                callback(_("Dumping database to SQL") + "...")
                dump_conn = self.get_connection()
                try:
                    with codecs.open(fname, "w", encoding="utf-8") as buf:
                        for line in dump_conn.iterdump():
                            buf.write(line)
                            if not line.endswith("\n"):
                                buf.write("\n")
                finally:
                    try:
                        dump_conn.close()
                    except Exception:
                        pass
            else:
                with open(fname, "wb") as buf:
                    buf.write(sql if isinstance(sql, bytes) else sql.encode("utf-8"))

            # --------------------
            # Restore into temp db
            # --------------------
            with TemporaryFile(suffix="_tmpdb.db", dir=os.path.dirname(self.database_path)) as tmpdb:
                callback(_("Restoring database from SQL") + "...")

                restore_conn = SQLite_Connection(tmpdb, detect_types=sqlite3.PARSE_DECLTYPES)
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

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO DELETE ROWS FROM THE DATABASE START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------


    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO GET INFORMATION ABOUT TABLES ON THE DATABASE START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------





    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO SEARCH THE DATABASE START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------



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



    # ----------------------------------------------------------------------------------------------------------------------
    #
    # METHODS SPECIFIC TO DEALING WITH NEW BOOKS START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------




    # ----------------------------------------------------------------------------------------------------------------------
    # - SPECIALIZED METHODS TO READ AND SET THE DATABASE METADATA START HERE
    # ----------------------------------------------------------------------------------------------------------------------



    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - SEARCH METHODS START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------




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


    #
    # ----------------------------------------------------------------------------------------------------------------------
