
"""
Contains the actual live database module for LiuXin.

Currently, the database speaks to a single backend (probably SQLite).
It is NOT thread safe - you need to do your own locking elsewhere.
"""

from __future__ import unicode_literals

import re
import os
import pprint
import queue as Queue
from copy import deepcopy


from typing import Optional

from LiuXin_alpha.databases.api import DatabaseAPI, DatabaseDriverWrapperAPI, DatabaseDriverAPI

from LiuXin_alpha.constants.paths import LiuXin_default_database
from LiuXin_alpha.databases.database.constants import HELPER_TABLES

from LiuXin_alpha.databases.database_driver_plugins import loadDatabaseDriver
from LiuXin_alpha.databases.database_driver_plugins.driver_wrapper import DriverWrapper
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.databases.maintenance_bot import Maintainer
from LiuXin_alpha.databases.custom_columns import CustomColumnDatabaseMixin

from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.errors import DatabaseIntegrityError

from LiuXin_alpha.preferences import preferences

from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.databases.database.rating_mixin import DatabaseRatingMixin
from LiuXin_alpha.databases.database.null_rows_mixin import DatabaseNullRowsMixin
from LiuXin_alpha.databases.database.metadata_mixin import DatabaseMetadataMixin
from LiuXin_alpha.databases.database.dirtied_mixin import DatabaseDirtiedRecordsMixin
from LiuXin_alpha.databases.database.search_mixin import DatabaseSearchMixin
from LiuXin_alpha.databases.database.interlink_mixin import DatabaseInterlinkRowsMixin
from LiuXin_alpha.databases.database.intralink_mixin import DatabaseIntralinkRowsMixin
from LiuXin_alpha.databases.database.tree_mixin import DatabaseTreeMixin

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


# Todo: Embed this version number in the database - so that we can check the version of the code used to produce each
#       test database
__object_version__ = (1, 0, 0)

# Todo: Point uuid requests to the library_id instead


class Database(
    CustomColumnDatabaseMixin,
    DatabaseRatingMixin,
    DatabaseNullRowsMixin,
    DatabaseMetadataMixin,
    DatabaseDirtiedRecordsMixin,
    DatabaseSearchMixin,
    DatabaseInterlinkRowsMixin,
    DatabaseIntralinkRowsMixin,
    DatabaseTreeMixin,
    DatabaseAPI,
):
    """
    Represents a database which LiuXin could be connected to. Access to the database should always be through this class
    The default database is simply the database located in LiuXin_data.
    Everything returned from this class should be a Row.
    To get a Row return - call database.method. To get a row_dict - call database.backend.method
    """

    _driver: DatabaseDriverAPI
    _driver_wrapper: DatabaseDriverWrapperAPI

    # Legacy convenience aliases (kept for backwards compatibility / test contracts):
    # - conn: primary driver connection
    # - lock: wrapper lock connection
    # - execute / executemany / executescript / get: direct-SQL escape hatches
    #   NOTE: these are meaningful only for SQL-like backends (SQLite/sqlite3/apsw, etc.).
    #   Non-SQL drivers may leave them unset.
    # These are *attributes* (not methods) so Database.close() can clear them to
    # help release handles on Windows.
    conn = None
    lock = None
    execute = None
    executemany = None
    executescript = None
    get = None
    shell = None
    get_connection = None

    # Todo: Split some of these out into factory methods and slim this down
    def __init__(
        self,
        metadata=None,
        db_type: str = "SQLite",
        create: bool = False,
        backup: bool = True,
        existing_driver: Optional[DatabaseDriverAPI] = None,
    ) -> None:
        """
        If the database type is not set defaults to SQLite.

        :param metadata: The metadata dictionary for the table
        :param db_type: The type of the database to be loaded
        :param create: Should a new database be created?
        :param backup: If create, and this is True, then the main database will be backed up before a new database is
                       created. This is intended to keep an old copy of the database around when you create a new one.
                       Defaults to True
        :type backup: bool
        :return:
        """
        self.metadata = None
        self.type = None

        self._macros = None

        # Fundamental constants for this database
        if existing_driver is None:
            self.standard_init(metadata=metadata, db_type=db_type, create=create, backup=backup)
        else:
            assert metadata is None, "driver is provided - it's assumed that the db metadata is contained within"
            self.existing_driver_init(existing_driver)
        # Used as a lookup cache for if the link table in question has a priority column
        # Keyed with the table, value with True or False
        self._link_has_priority = dict()

        # Queue for dirtied records
        self.dirty_records_queue = Queue.Queue()
        # Persistent helper table for metadata sidecar write-out (historic name: metadata_dirtied_books)
        self._metadata_dirtied_table = "metadata_dirtied_books"


        self.driver.dirty_records_queue = self.dirty_records_queue
        self.driver_wrapper.dirty_records_queue = self.dirty_records_queue

    @property
    def driver(self) -> DatabaseDriverAPI:
        """
        Return the live database driver.

        :return:
        """
        return self._driver

    @property
    def driver_wrapper(self) -> DatabaseDriverWrapperAPI:
        """
        Return the live database driver wrapper.

        :return:
        """
        return self._driver_wrapper

    @property
    def macros(self):
        """
        Return the macros object for the database.

        :return:
        """
        return self._macros

    def set_macros(self, new_macros) -> None:
        """
        Set the macros class for the database.

        :param new_macros:
        :return:
        """
        assert new_macros is not None, "Need to set macros to something that exists"
        self._macros = new_macros

    def existing_driver_init(self, existing_driver: DatabaseDriverAPI) -> None:
        """
        Startup method called when the drivber already exists. Useful for testing.

        :param existing_driver:
        :return:
        """
        # Load the driver constructor - use this to make the driver instance for this database
        self.set_driver(existing_driver)
        self.set_macros(existing_driver.macros)

        # Load the backend with the driver.
        self.lock = self.driver_wrapper.lock

        # Check to see if the database currently exists
        self._exists = True

        # categorized tables - sets of the names of each table in each category
        # all_tables - The names of every table known to the database

        # main_tables - the basic unit - titles, creators, series e.t.c - visible to the GUI and store the book
        #               metadata

        # custom_tables - Tables created by the user
        # custom_column_tables - Tables created to hold custom column data

        # interlink_tables - tables used to link the main tables together - what creators are associated to a title
        # intralink_tables - tables used to link the main tables back to themselves

        # dirtiable_tables - tables which can be dirtied - i.e. the maintenance bot should be informed when changes
        #                  - are made to them

        # helper_tables - data is stored in the database - for convenience - but isn't book or asset metadata
        self.all_tables = None

        self._main_tables = None

        self.custom_tables = None

        self._interlink_tables = None
        self.intralink_tables = None

        self.dirtiable_tables = None

        self.helper_tables = HELPER_TABLES

        self.refresh_db_metadata()

        self.driver_wrapper.all_tables = self.all_tables
        self.driver_wrapper.main_tables = self.main_tables
        self.driver_wrapper.interlink_tables = self.interlink_tables
        self.driver_wrapper.intralink_tables = self.intralink_tables
        self.driver_wrapper.helper_tables = self.helper_tables
        self.driver_wrapper.dirtiable_tables = self.dirtiable_tables

        # The rating table should be in a particular form - check that it is
        self.check_rating_table()
        self.ensure_null_rows()

        # Todo: What is going on here naming wise? Merge these two
        self.maintenance = Maintainer(self)
        self.maintainer = self.maintenance
        self.driver.maintainer_callback = self.maintenance
        self.clean = self.maintenance.clean

        # Global database preferences - just a copy of the main program preferences, but can be overridden if needed
        # Todo: This is confusing with the preferences stored in the database - call those db prefs
        self.preferences = preferences

        # As this probably hasn't been done for the existing driver - load a reference to this database into the macros
        # and the driver - the two places that it should be needed
        # Todo: This should be handled by properties
        self.driver_wrapper.db = self
        self.driver.db = self
        self.macros.db = self

    def standard_init(self, metadata=None, db_type="SQLite", create=False, backup=True):
        """
        Standard constructor - for when the driver doesn't already exist.

        :param metadata:
        :param db_type:
        :param create:
        :param backup:
        :return:
        """
        if metadata is None:
            metadata = {"database_path": LiuXin_default_database}

        db_path = metadata.get("database_path")
        path_existed = bool(db_path) and db_path != ":memory:" and os.path.exists(db_path)

        self.metadata = metadata
        self.type = db_type
        self.set_driver(loadDatabaseDriver(db_type)(self.metadata, self))

        if create or (not path_existed and db_path not in (None, ":memory:")):
            if path_existed:
                self.create_new_database(blank=True, backup=backup)
            else:
                self.create_new_database(blank=False, backup=False)

            # reload driver after schema creation
            self.set_driver(loadDatabaseDriver(db_type)(self.metadata, self))
            self.lock = self.driver_wrapper.lock

        # Check to see if the database currently exists
        self._exists = self.check_exists()

        if self._exists:
            # categorized tables - sets of the names of each table in each category
            # main_tables - the basic unit - titles, creators, series e.t.c - visible to the GUI and store the book
            #               metadata
            # interlink_tables - tables used to link the main tables together - what creators are associated to a title
            # intralink_tables - tables used to link the main tables back to themselves
            # dirtiable_tables - tables which can be dirtied - i.e. the maintenance bot should be informed when changes
            #                  - are made to them
            # helper_tables - data is stored in the database - for convenience - but isn't book or asset metadata
            self.all_tables = None
            self._main_tables = None
            self.custom_tables = None
            self._interlink_tables = None
            self.intralink_tables = None

            self.dirtiable_tables = None

            self.allowed_type_tables = None

            self.helper_tables = HELPER_TABLES


            # DatabasePing uuid - unique identifier given to the database
            self._uuid = None

            self.refresh_db_metadata()

            self.driver_wrapper.all_tables = self.all_tables
            self.driver_wrapper.main_tables = self.main_tables
            self.driver_wrapper.interlink_tables = self.interlink_tables
            self.driver_wrapper.intralink_tables = self.intralink_tables
            self.driver_wrapper.helper_tables = self.helper_tables
            self.driver_wrapper.dirtiable_tables = self.dirtiable_tables

            # The rating table should be in a particular form - check that it is
            self.check_rating_table()
            self.ensure_null_rows()

        # Todo: What is going on here naming wise? Merge these two
        self.maintenance = Maintainer(self)
        self.maintainer = self.maintenance
        self.driver.maintainer_callback = self.maintenance
        self.clean = self.maintenance.clean

        # Global database preferences - just a copy of the main program preferences, but can be overridden if needed
        self.preferences = preferences

        # As this probably hasn't been done for the existing driver - load a reference to this database into the macros
        # and the driver - the two places that it should be needed
        # Todo: This should be handled by properties
        self.driver_wrapper.db = self
        self.driver.db = self
        self.macros.db = self


    def set_driver(self, new_driver: DatabaseDriverAPI) -> None:
        """
        Set the database driver.

        This method is also responsible for tearing down any previous driver/wrapper resources.
        Without this, SQLite connections can be leaked during driver reloads (e.g. schema creation),
        which keeps database files locked on Windows.

        :param new_driver:
        :return:
        """
        # Close any existing wrapper (it holds its own SQLite connection for locking)
        old_wrapper = getattr(self, "_driver_wrapper", None)
        if old_wrapper is not None:
            try:
                old_wrapper.close()
            except Exception:
                pass

        # Close any existing driver connection
        old_driver = getattr(self, "_driver", None)
        if old_driver is not None and old_driver is not new_driver:
            try:
                conn = getattr(old_driver, "conn", None)
                if conn is not None:
                    try:
                        conn.commit()
                    except Exception:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                    try:
                        conn.close()
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                old_driver.conn = None
            except Exception:
                pass

        self._driver = new_driver
        # The wrapper is coupled to the driver and provides a locking connection.
        self._driver_wrapper = DriverWrapper(self._driver)
        self.set_macros(self._driver.macros)

        # Convenience lock handle
        try:
            self.lock = self._driver_wrapper.lock
        except Exception:
            pass

        # Legacy convenience aliases used across the codebase and relied upon by tests.
        # NOTE: these are intentionally attribute bindings to the wrapper/driver so they can
        # be cleared on close() to help break reference cycles and release SQLite file locks.
        try:
            self.conn = getattr(self._driver, "conn", None)
        except Exception:
            self.conn = None

        for name in (
            "execute",
            "executemany",
            "executescript",
            "get",
            "shell",
            "get_connection",
        ):
            try:
                setattr(self, name, getattr(self._driver_wrapper, name))
            except Exception:
                try:
                    setattr(self, name, None)
                except Exception:
                    pass

    def set_driver_wrapper(self, new_driver_wrapper: DatabaseDriverWrapperAPI) -> None:
        """
        Set the database driver wrapper.

        :param new_driver_wrapper:
        :return:
        """
        self._driver_wrapper = new_driver_wrapper
        assert self._driver_wrapper.macros is not None
        self._driver_wrapper.macros = self.macros

    def __del__(self):
        """
        Preform shutdown.

        Note: This is best-effort cleanup. For deterministic shutdown (especially on Windows, where open SQLite handles
        keep database files locked), call close() or use the database as a context manager.

        :return:
        """
        try:
            self.close()
        except Exception:
            # Never raise from __del__
            pass

    def close(self) -> None:
        """
        Close any open resources associated with this database.

        In particular, ensure all SQLite connections are closed so temporary database files can be deleted on Windows.

        :return:
        """
        # Capture references early so break_cycles() can't erase them before we close.
        driver = getattr(self, "_driver", None)
        wrapper = getattr(self, "_driver_wrapper", None)
        maintenance = getattr(self, "maintenance", None)

        # Stop the background maintenance thread (if it exists)
        try:
            maint_thread = getattr(maintenance, "maintainer", None)
            if maint_thread is not None and hasattr(maint_thread, "stop"):
                maint_thread.stop()
                # The thread is daemon=True, but joining briefly helps tests release resources promptly.
                try:
                    maint_thread.join(timeout=1)
                except Exception:
                    pass
        except Exception:
            pass

        # Close the wrapper's lock connection
        try:
            if wrapper is not None and hasattr(wrapper, "close"):
                wrapper.close()
            else:
                lock = getattr(self, "lock", None)
                if lock is not None:
                    try:
                        lock.commit()
                    except Exception:
                        pass
                    try:
                        lock.close()
                    except Exception:
                        pass
        except Exception:
            pass

        # Close the driver's primary connection
        try:
            if driver is not None:
                conn = getattr(driver, "conn", None)
                if conn is not None:
                    try:
                        conn.commit()
                    except Exception:
                        # If commit fails, try rollback then close
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                    try:
                        conn.close()
                    except Exception:
                        pass
                try:
                    driver.conn = None
                except Exception:
                    pass
        except Exception:
            pass

        # Remove convenience aliases that can keep connections alive
        for attr in (
            "conn",
            "lock",
            "execute",
            "executemany",
            "executescript",
            "get",
            "shell",
            "get_connection",
        ):
            try:
                setattr(self, attr, None)
            except Exception:
                pass

        # Break reference cycles last
        try:
            self.break_cycles()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    # Todo: Might actually want to delete these objects - and this might be an internal method
    def break_cycles(self):
        """
        Explicitly zero all stored objects in the right order.

        :return:
        """
        # These may not exist if __init__ failed partway through.
        for attr in (
            "_driver_wrapper",
            "_driver",
            "_macros",
            "maintenance",
            "maintainer",
            "dirty_records_queue",
            "_link_has_priority",
            # Convenience aliases (see set_driver)
            "conn",
            "lock",
            "execute",
            "executemany",
            "executescript",
            "get",
            "shell",
            "get_connection",
        ):
            try:
                setattr(self, attr, None)
            except Exception:
                pass

    @property
    def main_tables(self) -> frozenset[str]:
        """
        Return the defined main tables.

        :return:
        """
        return frozenset(self._main_tables)

    @property
    def interlink_tables(self) -> frozenset[str]:
        """
        Return the defined interlink tables.

        :return:
        """
        return frozenset(self._interlink_tables)

    def check_exists(self):
        """
        Check to see if the database exists according to the driver.
        Helpful for debugging - driver dependant as to what this means (might mean the database file is there. Might
        mean that we can connect to the database.
        :return:
        """
        return self.driver.exists()

    # Todo: These methods also private?
    def refresh_db_metadata(self):
        """
        Read appropriate metadata off the database.
        :return:
        """
        self.all_tables = set([t for t in self.get_tables()])
        self._main_tables = set()
        self.custom_tables = set()
        self._interlink_tables = set()
        self.intralink_tables = set()
        self.allowed_type_tables = set()
        # Check helper tables exist (report *all* missing; helper_tables is a set-like).
        missing_helpers = sorted(set(self.helper_tables) - set(self.all_tables))
        if missing_helpers:
            import difflib

            suggestions = {
                t: difflib.get_close_matches(t, sorted(self.all_tables), n=3, cutoff=0.6)
                for t in missing_helpers
            }

            err_str = "Unable to find required helper table(s) in the database"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("missing_helper_tables", pprint.pformat(missing_helpers)),
                ("suggestions", pprint.pformat(suggestions)),
                ("all_tables", pprint.pformat(self.all_tables)),
            )
            raise DatabaseIntegrityError(err_str)
        # Populate the individual categories
        for table in self.all_tables:
            table_cat = self.categorize_table(table)

            if table_cat == "main":
                self._main_tables.add(table)
                continue

            if table_cat == "interlink":
                self._interlink_tables.add(table)
                continue

            if table_cat == "intralink":
                self.intralink_tables.add(table)
                continue

            if table_cat == "helper":
                continue

            if table_cat == "custom":
                self.custom_tables.add(table)
                continue

            if table_cat == "allowed_types":
                self.allowed_type_tables.add(table)
                continue

            err_str = "This position should never be reached"
            err_str = default_log.log_variables(err_str, "ERROR", ("table_cat", table_cat), ("table", table))
            raise NotImplementedError(err_str)

        # The dirtiable tables are a union of the main tables and the two types of link table
        self.dirtiable_tables = deepcopy(self.main_tables.union(self.interlink_tables).union(self.intralink_tables))

        self._uuid = self.driver_wrapper.get_uuid()
        if six_unicode(self._uuid).lower().strip() == "none":
            self.driver_wrapper.set_uuid()

        # Remove SQLite sequence from the main tables - if present - this is for internal use only
        self._main_tables.discard("sqlite_sequence")

    # Todo: Backup is somewhat useless if there is no way to restore
    def backup(self):
        """
        Backs up the current DatabasePing - passthrough method for the DatabaseDriver method - which, by necessity, has to
        do the heavy lifting due to the differences in how database could be implemented.
        :return:
        """
        self.driver.direct_backup()

    # Todo: This is not, actually, write locking. This just makes a throwaway copy of the database
    def lock_writing(self):
        """
        Creates a copy of the database in a LiuXin scratch folder - switches so tat the database now reads off the copy
        instead of off the main version.
        :return:
        """
        self.driver.make_scratch()

    def create_new_database(self, blank: bool = True, backup: bool = True) -> None:
        """
        Creates a database if it doesn't exist, and loads it with the requested tables and columns).

        :param blank: Delete the database that already exists first.
        :param backup: Back the database up before trying to create the new one.
        :return:
        """
        if backup:
            self.driver.direct_backup()

        if blank:
            self.driver.direct_self_delete()

        self.driver.direct_create_new_database()

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO OUTPUT BASIC INFORMATION ABOUT THE DATABASE START HERE

    def __unicode__(self):
        """
        Unicode representation of some basic information.
        :return:
        """
        rtn_str = "LiuXin DatabasePing - Type:{}\n".format(self.type)
        rtn_str += "metadata:\n"
        rtn_str += pprint.pformat(self.metadata) + "\n"
        return rtn_str

    def __str__(self):
        return self.__unicode__().encode("utf-8")

    def __repr__(self):
        """
        A very basic representation of the database object.
        :return:
        """
        db_type = self.type
        db_path = self.metadata["database_path"]
        return "[ LX_database - type - " + six_unicode(db_type) + " at " + six_unicode(db_path) + " ]"

    # Todo: Might want to consider renaming this to full_repr, for consistency
    def full_rep(self):
        """
        Prints a represntation of all the tables in the database.
        :return:
        """
        ans = list()
        ans.append("LiuXin_Database")
        ans.append("database_uuid: {}".format(self.uuid))

        ans.append("DatabasePing MetaData")
        ans.append(pprint.pformat(self.metadata, indent=2))
        ans.append("")

        ans.append("Main_tables")
        ans.append(pprint.pformat(self.main_tables, indent=2))
        ans.append("")

        ans.append("Interlink_tables")
        ans.append(pprint.pformat(self.interlink_tables, indent=2))
        ans.append("")

        ans.append("Intralink_tables")
        ans.append(pprint.pformat(self.intralink_tables, indent=2))
        ans.append("")

        ans.append("Helper_tables")
        ans.append(pprint.pformat(self.intralink_tables, indent=2))
        ans.append("")

        return "\n".join(ans)

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO DEAL WITH TABLE CATEGORIES START HERE

    def categorize_table(self, table_name):
        """
        Takes a table - determines which of the four categories it belongs to. Returns the result as a string.
        :param table_name: The name of the table? I don't know, what do you want from me here.
        :return table_type: main, interlink, intralink or helper
        """
        table_name = six_unicode(table_name)
        if table_name not in self.all_tables:
            err_str = "Error - categorize_table has been passed an invalid table name."
            err_str = default_log.log_variables(err_str, "ERROR", ("table_name", table_name))
            raise InputIntegrityError(err_str)

        if table_name.startswith("allowed_types__"):
            return "allowed_types"

        # Helper tables should have been specified before this function is first called
        if table_name in self.helper_tables:
            return "helper"

        cc_pattern = re.compile(r"[a-zA-Z0-9_]+_custom_column_[0-9]+_link")
        cc_match = cc_pattern.match(table_name)
        if table_name.startswith("custom_column_") or cc_match is not None:
            return "custom"

        interlink_pattern = re.compile(r"[\sa-zA-Z0-9_]+_links")
        interlink_match = interlink_pattern.match(table_name)
        if interlink_match is not None:
            return "interlink"

        intralink_pattern = re.compile(r"[\sa-zA-Z0-9_]+_intralink")
        intralink_match = intralink_pattern.match(table_name)
        if intralink_match is not None:
            return "intralink"

        return "main"

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO ADD TO THE DATABASE START HERE

    def dupe_row(self, row):
        """
        Duplicate a row - will fail if the row has a unique constraint.
        :return:
        """
        row_table = row.table
        row_table_id_col = self.driver_wrapper.get_id_column(row_table)

        new_row = self.get_blank_row(row_table)
        new_row_id = new_row.row_id

        # Store the old row id - replace the row dict in the new blank row - replace the row_id - sync
        new_row.row_dict = row.row_dict
        new_row[row_table_id_col] = new_row_id
        try:
            new_row.sync()
        except DatabaseIntegrityError:
            # Probably a violation of a unique constraint - abort and tidy up
            self.delete(new_row)
            raise

        return new_row

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO DELETE FROM THE DATABASE START HERE

    def delete(self, row):
        """
        Takes a row - deletes it from the database.
        :param row:
        :return:
        """
        row_table = row.table
        row_id = row.row_id
        if row_id is None:
            err_str = "Unable to delete given row - row_id is not found."
            err_str = default_log.log_variables(err_str, "ERROR", ("row", row))
            raise InputIntegrityError(err_str)
        self.driver_wrapper.delete_by_id(target_table=row_table, row_id=row_id)

    #
    # ----------------------------------------------------------------------------------------------------------------------

    def get_blank_row(self, table):
        """
        Return a blank row (with id) for the given table in the database.
        :param table:
        :return:
        """
        blank_row_dict = self.driver_wrapper.get_blank_row(table)
        return Row(database=self, row_dict=blank_row_dict)

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - SPECIALIZED UPDATE METHODS START HERE

    def update_columns(self, values_map, field=None, table=None):
        """
        Pass through for the backend method.
        :return:
        """
        self.driver_wrapper.update_columns(values_map=values_map, field=field, table=table)

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - MAGIC METHODS START HERE

    def __eq__(self, other):
        """
        If the DatabasePing metadata is the same, then the database is
        :param other:
        :return:
        """
        if self.metadata == other.metadata:
            return True
        else:
            return False


#
# ----------------------------------------------------------------------------------------------------------------------
########################################################################################################################
########################################################################################################################

