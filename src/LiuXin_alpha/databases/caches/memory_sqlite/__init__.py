# Class to support loading and storing an SQLite database in memory

import re
import sqlite3
import uuid
from copy import deepcopy
from functools import partial

from LiuXin.databases.drivers.SQLite.databasedriver import SQLite_Connection

from LiuXin.databases.drivers.SQLite.databasedriver import DatabaseDriver
from LiuXin.databases.drivers.SQLite.databasedriver import (
    py_set_adapter,
    py_set_converter,
)
from LiuXin.databases.drivers.SQLite.databasedriver import (
    py_list_adapter,
    py_list_converter,
)
from LiuXin.databases.drivers.SQLite.databasedriver import (
    py_dict_adapter,
    py_dict_converter,
)
from LiuXin.databases.drivers.SQLite.databasedriver import py_date_converter
from LiuXin.databases.drivers.SQLite.databasedriver import (
    PySetAggregate,
    SortAggregate,
    PyListAggregate,
)

# Todo: These should be over in metadata
from LiuXin.databases.drivers.SQLite.databasedriver import authors_str_to_sort_str
from LiuXin.databases.drivers.SQLite.databasedriver import title_sort
from LiuXin.databases.drivers.SQLite.databasedriver import _author_to_author_sort
from LiuXin.databases.drivers.SQLite.databasedriver import icu_collator
from LiuXin.databases.drivers.SQLite.databasedriver import SqliteAumSortedConcatenate
from LiuXin.databases.drivers.SQLite.databasedriver import Concatenate
from LiuXin.databases.drivers.SQLite.databasedriver import IdentifiersConcat
from LiuXin.databases.drivers.SQLite.databasedriver import SqliteSortedConcatenate
from LiuXin.databases.drivers.SQLite.databasedriver import pynocase

from LiuXin.exceptions import DatabaseDriverError

from LiuXin.utils.logger import default_log


# Todo: This is only needed due to bad structural choices in the original SQLite driver
class Memory_SQLite_Connection(SQLite_Connection):
    def close(self, *args, **kwargs):
        pass


class MemoryDatabaseDriver(DatabaseDriver):
    def __init__(self, db_metadata, db=None):
        super(MemoryDatabaseDriver, self).__init__(db_metadata=db_metadata, db=db, set_conn=False)

        self._memory_conn = Memory_SQLite_Connection(":memory:", detect_types=sqlite3.PARSE_DECLTYPES)

    def get_connection(self):
        return self._memory_conn

    def initial_get_connection(self):
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
            conn = self._memory_conn

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

        conn.create_aggregate("identifiers_concat", 2, IdentifiersConcat)

        conn.create_aggregate("sortconcat", 2, SqliteSortedConcatenate)
        conn.create_aggregate("sortconcat_bar", 2, partial(SqliteSortedConcatenate, sep="|"))
        conn.create_aggregate("sortconcat_amper", 2, partial(SqliteSortedConcatenate, sep="&"))

        # Register the custom collators (ported from calibre, for compatibility)
        encoding = conn.execute("PRAGMA ENCODING").next()[0]
        conn.create_collation("PYNOCASE", partial(pynocase, encoding=encoding))

        return conn

    def load_from_db(self, target_db):
        """
        Load data from the database into memory.
        :param target_db:
        :return:
        """
        memory_db = self._memory_conn
        memory_db_cursor = memory_db.cursor()

        # Load the in memory database with data from the backend
        for line in target_db.driver_wrapper.driver.sql_dump():
            memory_db_cursor.executescript(line)
        memory_db.commit()

        # Add the python functions required for the database to function properly
        self.conn = self.initial_get_connection()

        return memory_db

    def last_modified(self):
        """
        Return last modified time as a UTC datetime object
        :return:
        """
        raise NotImplementedError


def in_memory_db_factory(db):
    """
    Load the given database into memory and return.
    :param db:
    :return:
    """
    from LiuXin.databases.database import Database

    in_memory_driver = MemoryDatabaseDriver(db_metadata={"database_path": ":memory:"}, db=None)
    in_memory_driver.load_from_db(target_db=db)

    return Database(existing_driver=in_memory_driver)
