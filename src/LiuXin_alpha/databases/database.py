
"""
Contains the actual live database module for LiuXin.

Currently the database speaks to a single backend (probably SQLite).
It is NOT thread safe - you need to do your own locking elsewhere.
"""

from __future__ import unicode_literals

import re
import os
import pprint
import queue as Queue
import uuid
from copy import deepcopy
from numbers import Number

from typing import Optional

from LiuXin_alpha.databases.api import DatabaseAPI, DatabaseDriverWrapperAPI, DatabaseDriverAPI

from LiuXin_alpha.constants.paths import LiuXin_default_database

from LiuXin_alpha.databases.database_driver_plugins import loadDatabaseDriver
from LiuXin_alpha.databases.database_driver_plugins.driver_wrapper import DriverWrapper
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.databases.maintenance_bot import Maintainer
from LiuXin_alpha.databases.custom_columns import CustomColumnDatabaseMixin

from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.errors import DatabaseIntegrityError

from LiuXin_alpha.preferences import preferences

from LiuXin_alpha.utils.logging import default_log

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

from LiuXin_alpha.databases.bootstrap_constants import AGENTS_NULL_CANONICAL_NAME

# Todo: Embed this version number in the database - so that we can check the version of the code used to produce each
#       test database
__object_version__ = (1, 0, 0)

# Todo: Point uuid requests to the library_id instead

HELPER_TABLES = frozenset(
    {
        "conversion_options",
        "compressed_files",
        "custom_columns",
        "database_metadata",
        "database_version",
        "feeds",
        "hashes",
        "last_read_positions",
        "library_id",
        "metadata_dirtied_books",
        "new_books",
        "preferences",
        # FRBR plugin data
        "works_plugin_data",
        "expressions_plugin_data",
        "manifestations_plugin_data",
        "items_plugin_data",
        # Workflow tables
        "file_derivations",
        "file_workflow",
        "file_workflow_events",
        "item_workflow",
        "item_workflow_events",
        "transform_runs",
        "transform_run_inputs",
        "transform_run_outputs",
        "workflow_states",
        "workflow_steps",
    }
)



class Database(CustomColumnDatabaseMixin, DatabaseAPI):
    """
    Represents a database which LiuXin could be connected to. Access to the database should always be through this class
    The default database is simply the database located in LiuXin_data.
    Everything returned from this class should be a Row.
    To get a Row return - call database.method. To get a row_dict - call database.backend.method
    """

    _driver: DatabaseDriverAPI
    _driver_wrapper: DatabaseDriverWrapperAPI

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

    @property
    def uuid(self):
        if self._uuid is not None:
            return self._uuid
        else:
            self._uuid = self.driver_wrapper.get_uuid()
            return self._uuid

    @uuid.setter
    def uuid(self, value):
        self._uuid = value
        self.driver_wrapper.set_uuid(value)

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
        """        Close any open resources associated with this database.

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
        for attr in ("lock",):
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
        ):
            try:
                setattr(self, attr, None)
            except Exception:
                pass

    # Todo: THis might also want to be an internal method
    def check_rating_table(self):
        """
        Checks that there is a valid ratings table.

        It should have 11 entries - each should be an integer from 0-10. Check that these exist. Do nothing if they do,
        error should if they do, but not in the expected form and insert them if they do not.
        :return:
        """
        for i in range(1, 12):
            rating = six_unicode(i - 1)
            rating_id = six_unicode(i)
            rating_row = self.get_row_from_id("ratings", rating_id)

            if rating_row is None:
                new_row_dict = {
                    "rating_id": rating_id,
                    "rating": six_unicode(float(rating) / 2.0),
                }
                self.driver_wrapper.add_row(new_row_dict)

            else:

                if float(rating_row["rating"]) != float(rating) / 2.0:
                    err_str = "Rating row malformed - correcting"
                    default_log.log_variables(
                        err_str,
                        "INFO",
                        ("rating", rating),
                        ('rating_row["rating"]', six_unicode(rating_row["rating"])),
                    )
                    rating_row["rating"] = float(rating) / 2.0
                    rating_row.sync()

        # rating_11_row = self.get_row_from_id("ratings", 11)
        # if rating_11_row is not None:
        #     self.delete(rating_11_row)

    # Todo: THese methods should be private - only run during startup
    def ensure_null_rows(self):
        """
        Ensure required sentinel/null rows exist.

        Historically, LiuXin used id=0 in certain tables as a "null" record for
        link tables.

        In the FRBR-first/WEMI schema, publishing entities are modelled via
        `agents` (+ subtype sidecars like `org_agents`) rather than a dedicated
        `publishers` table.
        """

        # Ensure the series null row
        if getattr(self, "all_tables", None) is None or "series" in self.all_tables:
            series_0_row = self.driver_wrapper.get_row_from_id("series", 0)
            if not series_0_row:
                series_null_row = {"series_id": 0}
                self.driver_wrapper.add_row(series_null_row)
            else:
                # Convention: the sentinel row's display value is NULL
                series_0_row["series"] = None
                self.driver_wrapper.update_row(series_0_row)

        # Preferred (FRBR-first): ensure an organisation agent sentinel row
        if getattr(self, "all_tables", None) is None or "agents" in self.all_tables:
            agent_0_row = self.driver_wrapper.get_row_from_id("agents", 0)
            if not agent_0_row:
                agent_null_row = {
                    "agent_id": 0,
                    "agent_type": "organisation",
                    # agent_canonical_name is NOT NULL in the current schema
                    "agent_canonical_name": AGENTS_NULL_CANONICAL_NAME,
                }
                self.driver_wrapper.add_row(agent_null_row)
            else:
                agent_0_row["agent_type"] = "organisation"
                # Always repair/normalize the sentinel row's canonical name.
                # (It's NOT NULL in schema, so we use a clearly intentional string.)
                if agent_0_row.get("agent_canonical_name") != AGENTS_NULL_CANONICAL_NAME:
                    agent_0_row["agent_canonical_name"] = AGENTS_NULL_CANONICAL_NAME
                self.driver_wrapper.update_row(agent_0_row)

        # Legacy fallback (Calibre-style DBs): keep the old publishers sentinel row if that table exists.
        elif getattr(self, "all_tables", None) is None or "publishers" in self.all_tables:
            pub_0_row = self.driver_wrapper.get_row_from_id("publishers", 0)
            if not pub_0_row:
                pub_null_row = {"publisher_id": 0}
                self.driver_wrapper.add_row(pub_null_row)
            else:
                pub_0_row["publisher"] = None
                self.driver_wrapper.update_row(pub_0_row)

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

    @property
    def library_id(self):
        """
        The UUID for this library. As long as the user only operates on libraries with LiuXin, it will be unique.
        :return:
        """
        if getattr(self, "_library_id_", None) is None:
            ans = self.driver_wrapper.get("SELECT library_id_uuid FROM library_id", all=False)
            if ans is None:
                ans = str(uuid.uuid4())
                self.library_id = ans
            else:
                self._library_id_ = ans
        return self._library_id_

    @library_id.setter
    def library_id(self, value):
        """
        Setter function for the library id - handles updating the database with the new id.
        :param value:
        :return:
        """
        self._library_id_ = six_unicode(value)
        self.macros.set_library_id(value)

    @property
    def database_version(self):
        """
        The UUID for this library. As long as the user only operates on libraries with LiuXin, it will be unique.
        :return:
        """
        if getattr(self, "_database_version_", None) is None:
            c = self.conn.cursor()
            version_val = None

            for row in c.execute("SELECT database_version_version FROM database_version;"):
                version_val = row[0]
            self._database_version_ = version_val
        return self._database_version_

    @database_version.setter
    def database_version(self, value):
        """
        Setter function for the library id - handles updating the database with the new id.
        :param value:
        :return:
        """
        self._database_version_ = six_unicode(value)
        self.macros.set_database_version(value)

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
    # - METHODS TO GET BASIC INFORMATION ABOUT THE DATABASE START HERE

    def get_tables(self, force_refresh: bool = False):
        """
        Directly get the tables for the currently loaded database
        :return:
        """
        return self.driver_wrapper.get_tables(force_refresh=force_refresh)

    # Methods to get basic information about the database start here
    def get_column_headings(self, table):
        """
        Gets the column headings for a table in the database.
        :param table:
        :return column_headings: An index of column headings in the order they appear on the database
        """
        return self.driver_wrapper.get_column_headings(table)

    def get_view_column_headings(self, view):
        """
        Gets the column headings for a table in the database.
        :param table:
        :return column_headings: An index of column headings in the order they appear on the database
        """
        return self.driver_wrapper.get_view_column_headings(view)

    def get_tables_and_columns(self):
        """
        Returns a dictionary keyed by the table name with the column headings as the values.
        :return table_and_columns:
        """
        return self.driver_wrapper.get_tables_and_columns()

    def get_record_count(self, target_table):
        """
        Returns the number of records in a given table.
        :param target_table:
        :return:
        """
        return self.driver_wrapper.get_record_count(target_table)

    def get_max(self, column):
        """
        Get the maximum value from the given column.
        :param column:
        :return:
        """
        return self.driver.direct_get_max(column)

    def get_min(self, column):
        """
        Get the minimum value from the given column.
        :param column:
        :return:
        """
        return self.driver.direct_get_min(column)

    #
    # ----------------------------------------------------------------------------------------------------------------------
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

    def row_counts(self):
        """
        Returns a string representation of the row counts for every table in the DatabasePing.
        :return:
        """
        ans = list()
        ans.append("LiuXin _Database: Table row_counts")
        ans.append("database_uuid: {}".format(self.uuid))

        for table_type in [
            "main_tables",
            "interlink_tables",
            "intralink_tables",
            "helper_tables",
        ]:

            type_tables = sorted([t for t in deepcopy(object.__getattribute__(self, table_type))])
            ans.append("\n{}:\n".format(table_type))

            for table in type_tables:
                ans.append("{}: {}".format(table, self.get_record_count(table)))

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
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO SEARCH THE DATABASE START HERE

    def search(self, table, column, search_term):
        """
        Search the database for specific values.
        :param table: Table to search in
        :param column: Column within that table
        :param search_term: The thing to search with (will be coerced to unicode)
        :return:
        """
        return [Row(row_dict=r, database=self) for r in self.driver_wrapper.search(table, column, search_term)]

    # Todo: This does not work
    def multi_column_search(self, search_index, iterator_return=False):
        """
        Takes an index of tuples (or indexes - the method is not fussy provided it contains the required terms). Which
        can then be used to search the database.
        Tuples should take the form (column_name, binary_comparison_operator, target_value).
        Binary comparison operators can include the LIKE operator.
        Every tuple is joined together by an AND statement.
        Thus [(u'creator', u'=', u'David Weber'),(u'series',u'=',u'Honor Harrington')] becomes
        SELECT * FROM `creators` * WHERE creator = 'David Weber' AND series = 'Honor Harrington';
        # Todo: Which WILL NOT work
        :param search_index:
        :param iterator_return: Should the return be in the form of an iterator, on an index of row_dicts
        :return found_rows:
        """
        row_dicts = self.driver.direct_multi_column_search(search_index=search_index, iterator_return=iterator_return)

    def get_unique(self, target_column):
        return self.get_values_set(target_column=target_column)

    def get_values_set(self, target_column, iterator_return=False):
        """
        Gets a set of the unique values that a particular column has.
        :param target_column: Which column should the unique values be extracted from?
        :param iterator_return: Should the function return an iterator or not?
        :return:
        """
        if iterator_return:
            return self.driver.direct_get_unique_values_iterator(target_column=target_column)
        else:
            return self.driver.direct_get_unique_values_set(target_column=target_column)

    def get_row_from_id(self, table, row_id):
        """
        Gets a row from its particular id.

        :param table: The table to search in
        :param row_id: The id of the row to search for
        :return row: A row with the relevant id - or None if the row can't be found
        """
        row_dict = self.driver_wrapper.get_row_from_id(table, row_id)
        if not row_dict:
            return None
        else:
            return Row(row_dict=row_dict, database=self)

    def get_random_row(self, table):
        """
        Return a randomly chosen row from the given table
        :param table:
        :return:
        """
        row_dict = self.driver_wrapper.get_random_row(table=table)
        return Row(row_dict=row_dict, database=self)

    def get_all_rows(self, table, iterator_return=True, sort_column=None, reverse=False):
        """
        Returns all rows from a given table in the database in the form of a list of Rows, or an iterator.
        Iterator_return is on by default, as otherwise the return could be very large.
        :param table:
        :param iterator_return:
        :param sort_column:
        :param reverse:
        :return:
        """
        if iterator_return:
            if reverse or sort_column is not None:
                raise NotImplementedError("Need to go back and work on the driver.")
            else:
                return self.__get_all_rows_iterator_return(table)
        else:
            row_dicts = self.driver_wrapper.get_all_rows(table, sort_column, reverse)
            return [Row(row_dict=r, database=self) for r in row_dicts]

    def __get_all_rows_iterator_return(self, table):
        """
        Helper function to get round one of the limitations of Python 2.7 (that you can't have both a return and a
        yield statement in the same function. Can be merged into get_all_rows after upgrading.
        :param table:
        :return:
        """
        row_dict_iterator = self.driver.direct_get_row_dict_iterator(table)
        for row_dict in row_dict_iterator:
            yield Row(row_dict=row_dict, database=self)

    # Todo: Test
    def chunk_iterator(self, column, target_table=None):
        """
        Iterates through a table retuning rows from it grouped by the grouping_column.
        :param column: Return will be grouped using this column
        :param target_table: The table to be grouped - if None will assume that the grouping column is in the
        target_table
        :return:
        """
        column = six_unicode(deepcopy(column))
        column_table = self.driver_wrapper.identify_table_from_column(column)

        # Iterate over the table - yield rows from the table in chunks
        if target_table is None or (target_table == column_table):

            for unique_val in self.get_values_set(target_column=column, iterator_return=True):
                yield self.search(table=column_table, column=column, search_term=unique_val)

        elif target_table != column_table:

            # Iterate over the column. For each unique value in that column get the rows that correspond to it. Then
            # get all the rows in the other table linked to it - return them as a chunk
            for unique_val in self.get_values_set(target_column=column, iterator_return=True):
                return_rows = []
                for ct_row in self.search(table=column_table, column=column, search_term=unique_val):
                    return_rows += [
                        r for r in self.get_interlinked_rows(target_row=ct_row, secondary_table=target_table)
                    ]
                yield return_rows

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
    # - METHODS TO READ INTERLINK TABLES START HERE

    def get_interlink_row(self, primary_row, secondary_row, onelink=True):
        """
        Get the row connecting the primary_row and the secondary row. Errors if there is more than one. Returns None if
        there is less than one.
        If the tables can't be linked, errors.
        :param primary_row:
        :param secondary_row:
        :param onelink: If True assumes that there should be either one or zero links between the given two rows.
                        If False then there can be any number of links. Returns all of them as a list.
        :return:
        """
        primary_table = primary_row.table
        secondary_table = secondary_row.table

        link_table_name = self.driver_wrapper.get_link_table_name(primary_table, secondary_table)
        if not link_table_name or (primary_table == secondary_table):
            err_str = "Given tables cannot be connected - or you have used an interlink method, not the intralink one"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
                ("link_table_name", link_table_name),
            )
            raise InputIntegrityError(err_str)

        # Search the interlink table for a row which matches the required criteria
        interlink_table = self.driver_wrapper.get_link_table_name(primary_table, secondary_table)
        primary_link_col = self.driver_wrapper.get_link_column(
            primary_table,
            secondary_table,
            self.driver_wrapper.get_id_column(primary_table),
        )
        secondary_link_col = self.driver_wrapper.get_link_column(
            primary_table,
            secondary_table,
            self.driver_wrapper.get_id_column(secondary_table),
        )

        # Search for links which reference the primary_row
        candidate_rows = []
        link_rows = self.search(
            table=interlink_table,
            column=primary_link_col,
            search_term=primary_row.row_id,
        )
        secondary_id = six_unicode(secondary_row.row_id)
        for row in link_rows:
            if secondary_id == six_unicode(row[secondary_link_col]):
                candidate_rows.append(row)

        if len(candidate_rows) == 0:
            return None
        elif len(candidate_rows) == 1:
            if onelink:
                return candidate_rows[0]
            else:
                return candidate_rows
        else:
            if onelink:
                err_str = "Only one link is permitted between each row pair"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("primary_row", primary_row),
                    ("secondary_row", secondary_row),
                    ("link_table_name", link_table_name),
                    ("candidate_rows", candidate_rows),
                )
                raise DatabaseIntegrityError(err_str)
            else:
                return candidate_rows

    def get_interlink_rows(self, primary_row, secondary_table):
        """
        Get all the interlink rows connecting the primary row and any row in the secondary table.
        :param primary_row:
        :param secondary_table:
        :return:
        """
        primary_table = primary_row.table

        link_table_name = self.driver_wrapper.get_link_table_name(primary_table, secondary_table)
        if not link_table_name or (primary_table == secondary_table):
            err_str = "Given tables cannot be connected - or you have used an interlink method, not the intralink one"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_table", secondary_table),
                ("link_table_name", link_table_name),
            )
            raise InputIntegrityError(err_str)

        # Search the interlink table for a row which matches the required criteria
        interlink_table = self.driver_wrapper.get_link_table_name(primary_table, secondary_table)
        primary_link_col = self.driver_wrapper.get_link_column(
            primary_table,
            secondary_table,
            self.driver_wrapper.get_id_column(primary_table),
        )

        link_rows = self.search(
            table=interlink_table,
            column=primary_link_col,
            search_term=primary_row.row_id,
        )
        try:
            priority_col = self.driver_wrapper.get_link_column(primary_table, secondary_table, "priority")
        except DatabaseIntegrityError:
            pass
        else:
            link_rows = sorted(link_rows, key=lambda x: x[priority_col])
        return link_rows

    def get_interlinked_rows(self, target_row, secondary_table, type_filter=None):
        """
        Takes a row and the name of another table. Finds all the rows in the second table linked to the given row.
        Returns them as an index ordered by their priority.
        :param target_row:
        :param secondary_table:
        :param type_filter: Only results which are linked to the target_row with a link of this type will be retured
        :return row_list (ordered by priority)/[]:
        """
        if not isinstance(target_row, Row):
            err_str = "Input to the DatabasePing class has to be in the form of Rows"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("target_row", target_row),
                ("secondary_table", secondary_table),
            )
            raise InputIntegrityError(err_str)

        if secondary_table not in self.main_tables and secondary_table not in self.helper_tables:
            err_str = "Secondary table needs to be in either the main tables or the helper tables"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("target_row", target_row),
                ("secondary_table", secondary_table),
            )
            raise InputIntegrityError(err_str)

        if target_row.table == secondary_table:
            err_str = "This method is for interlink rows, not intralink rows."
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("target_row", target_row),
                ("secondary_table", secondary_table),
            )
            raise InputIntegrityError(err_str)

        primary_table = target_row.table
        primary_id = target_row.row_id
        primary_id_col = self.driver_wrapper.get_id_column(primary_table)

        secondary_id_col = self.driver_wrapper.get_id_column(secondary_table)

        # Get the name of the link table - check to see if it exists (if it doesn't, returns None) - signalling that no
        # link exists
        link_table = self.driver_wrapper.get_link_table_name(primary_table, secondary_table)
        if not link_table:
            return []

        link_table_col = self.driver_wrapper.get_column_base(link_table)
        primary_table_link_col = link_table_col + "_" + primary_id_col
        secondary_table_link_col = link_table_col + "_" + secondary_id_col
        link_priority_col = link_table_col + "_priority"

        link_rows = self.driver_wrapper.search(table=link_table, column=primary_table_link_col, search_term=primary_id)
        if not link_rows:
            return []

        # The highest priority rows will be the first in the list - if there is a priority row to order them
        try:
            link_rows = sorted(link_rows, key=lambda x: x[link_priority_col], reverse=True)
        except KeyError:
            pass

        if type_filter is None:
            secondary_ids = [r[secondary_table_link_col] for r in link_rows]
            secondary_rows = [self.get_row_from_id(table=secondary_table, row_id=r_id) for r_id in secondary_ids]
            return secondary_rows
        else:
            link_type_column = link_table_col + "_type"
            secondary_ids = [r[secondary_table_link_col] for r in link_rows if r[link_type_column] == type_filter]
            secondary_rows = [self.get_row_from_id(table=secondary_table, row_id=r_id) for r_id in secondary_ids]
            return secondary_rows

    def get_interlink_values(self, target_row, secondary_column):
        """
        Takes a row and a column - in a table linked to the row.

        Returns a set of every value of that column in a row linked to the given target row - for example, searching with a title_row "creator" yields every creator linked
        to that target row.
        :param target_row:
        :param secondary_column:
        :return values_set:
        """
        secondary_table = self.driver_wrapper.identify_table_from_column(secondary_column)
        linked_rows = self.get_interlinked_rows(target_row=target_row, secondary_table=secondary_table)
        return set([r[secondary_column] for r in linked_rows])

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO WRITE TO INTERLINK TABLES START HERE

    def _check_for_link_table_priority(self, link_table_name, primary_link_table_name, secondary_link_table_name):
        """
        Check to see if the link table has a priority column.
        :param link_table_name:
        :return:
        """
        if link_table_name in self._link_has_priority:
            return self._link_has_priority[link_table_name]
        else:
            try:
                self.driver_wrapper.get_link_column(primary_link_table_name, secondary_link_table_name, "priority")
            except DatabaseIntegrityError:
                self._link_has_priority[link_table_name] = False
                return False

            self._link_has_priority[link_table_name] = True
            return True

    # Todo: Remain type to link type
    def interlink_rows(self, primary_row, secondary_row, priority="highest", type=None, **col_value_pairs):
        """
        Link two rows - col_value_pairs provide a means of adding more information to the link - they can include such
        things as index and type.
        priority accepts integer values, or highest/lowest. This will set the priority to the highest/lowest value in
        that column of the link table. Which is crude, but can be prettified later.
        :param primary_row:
        :param secondary_row:
        :param priority:
        :param type: The type of link
        :param col_value_pairs:
        :return link_row:
        """
        # Check that the tables can be interlinked
        primary_row_table = primary_row.table
        secondary_row_table = secondary_row.table
        link_table = self.driver_wrapper.get_link_table_name(primary_row_table, secondary_row_table)
        if not link_table:
            err_str = "Tables cannot be linked - no such link table exists"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
            )
            raise InputIntegrityError(err_str)

        # Check that both the rows have ids
        primary_id = primary_row.row_id
        secondary_id = secondary_row.row_id
        if primary_id is None or secondary_id is None:
            err_str = "Table cannot be linked - one of the rows doesn't have an id"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
            )
            raise InputIntegrityError(err_str)

        link_row = dict()
        for col in col_value_pairs:
            link_row_col = self.driver_wrapper.get_link_column(primary_row_table, secondary_row_table, col)
            link_row[link_row_col] = col_value_pairs[col]

        # Make the link dict - do not add it as yet
        primary_row_id_col = self.driver_wrapper.get_id_column(primary_row_table)
        primary_link_col = self.driver_wrapper.get_link_column(
            primary_row_table, secondary_row_table, primary_row_id_col
        )

        secondary_row_id_col = self.driver_wrapper.get_id_column(secondary_row_table)
        secondary_link_col = self.driver_wrapper.get_link_column(
            primary_row_table, secondary_row_table, secondary_row_id_col
        )

        link_row[primary_link_col] = primary_id
        link_row[secondary_link_col] = secondary_id

        # Process the priority - only numbers can be written into the priority column
        if priority != "not_set":

            if self._check_for_link_table_priority(link_table, primary_row_table, secondary_row_table):
                priority_col = self.driver_wrapper.get_link_column(primary_row_table, secondary_row_table, "priority")

                # Set the priority of the link if the table has a priority column
                if priority_col is not None:
                    priority_key = six_unicode(priority).lower().strip()
                    if priority is None:
                        link_row[priority_col] = 0
                    elif priority_key == "highest" or priority_key == "lowest":
                        priority_num = (
                            self.get_max(priority_col) if priority_key == "highest" else self.get_min(priority_col)
                        )
                        try:
                            priority_val = int(priority_num) + 1 if priority_key == "highest" else int(priority_num) - 1
                        except (ValueError, TypeError) as e:
                            # Correct a bug which throws an error when t a link table is empty
                            link_row_count = self.driver_wrapper.get_record_count(target_table=link_table)
                            if link_row_count != 0:
                                err_str = (
                                    "get_max for a priority column appears to have returned something not a number"
                                )
                                err_str = default_log.log_exception(
                                    err_str,
                                    e,
                                    "ERROR",
                                    ("priority_num", priority_num),
                                    ("primary_row", primary_row),
                                    ("secondary_row", secondary_row),
                                    ("priority", priority),
                                )
                                raise DatabaseIntegrityError(err_str)
                            else:
                                info_str = "Link table appeared to be empty - setting piority_val to 1 and continuing"
                                default_log.log_variables(info_str, "INFO")
                                priority_val = 1
                        link_row[priority_col] = priority_val

                    elif isinstance(priority, Number):
                        link_row[priority_col] = priority

                    else:
                        err_str = "priority type not recognized and cannot be parsed"
                        err_str = default_log.log_variables(
                            err_str,
                            "ERROR",
                            ("primary_row", primary_row),
                            ("secondary_row", secondary_row),
                            ("priority", priority),
                        )
                        raise InputIntegrityError(err_str)

        # Process the type - Todo: Add checking that the type is valid for that combination
        if type is not None:
            type_col = self.driver_wrapper.get_link_column(primary_row_table, secondary_row_table, "type")
            link_row[type_col] = type

        # Acquire an id for the link row and add it
        link_table_id = self.driver_wrapper.get_id_column(link_table)
        blank_link_row = self.driver_wrapper.get_blank_row(link_table)
        link_row[link_table_id] = blank_link_row[link_table_id]

        # Todo: This is pretty inefficient - try and tidy it up
        # Sync the new data back to the database
        link_row = Row(row_dict=link_row, database=self)
        try:
            link_row.sync()
        except DatabaseIntegrityError:
            self.delete(link_row)
            raise

        return link_row

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO UPDATE A LINK BETWEEN TWO ROWS START HERE

    def dupe_interlinks(
        self,
        src_row,
        dst_row,
        swap_priorities=False,
        restrict_to_tables=None,
        force_priority=None,
    ):
        """
        Duplicates the interlinks from one row and applied them to another.
        The dst row will end up having a higher priority in the links that the src row.
        :param src_row: Interlinks from this row will be applied to the dst_row
        :param dst_row:
        :param swap_priorities: If true then swap the priorities of the two rows so that src_row ends up higher
                                priority than dst_row
        :param restrict_to_tables: If not None then only interlinks from these tables will be copies
        :type restrict_to_tables: None or an iterable of table names
        :param force_priority: If force_priority is not None then the string is passed into the interlink_rows method
        :type force_priority: None, or a priority string acceptable as the priority argument of the interlink_rows
                              method.
        :return:
        """
        # So this method only tries to handle interlinks
        if restrict_to_tables is None:
            other_main_tables = set(t for t in deepcopy(self.main_tables))
            other_main_tables.remove(src_row.table)
        else:
            other_main_tables = restrict_to_tables

        # Identify all the rows linked to the src_row - then link them to the dst row
        for main_table in other_main_tables:
            src_linked_rows = self.get_interlinked_rows(target_row=src_row, secondary_table=main_table)
            src_linked_rows.reverse()
            for src_linked_row in src_linked_rows:

                if force_priority is None:
                    self.interlink_rows(primary_row=dst_row, secondary_row=src_linked_row)
                else:
                    self.interlink_rows(
                        primary_row=dst_row,
                        secondary_row=src_linked_row,
                        priority=force_priority,
                    )

                if swap_priorities:
                    self.swap_priorities(src_row=src_linked_row, dst_row_1=src_row, dst_row_2=dst_row)

    def swap_priorities(self, src_row, dst_row_1, dst_row_2):
        """
        Swap the priorities of two rows linked to the same src row.
        :param src_row: The row which is linked to dst_row_1 and dst_row_2
        :param dst_row_1:
        :param dst_row_2:
        :return:
        """
        src_row_table = src_row.table
        dst_table = dst_row_1.table
        link_priority_column = self.driver_wrapper.get_link_column(src_row_table, dst_table, "priority")

        dst_row_1_link = self.get_interlink_row(primary_row=src_row, secondary_row=dst_row_1)
        dst_row_2_link = self.get_interlink_row(primary_row=src_row, secondary_row=dst_row_2)

        priority_hold = dst_row_1_link[link_priority_column]
        dst_row_1_link[link_priority_column] = dst_row_2_link[link_priority_column]
        dst_row_2_link[link_priority_column] = priority_hold

        # Need this to get around the uniquen constraint
        dst_row_1_link[link_priority_column] = None
        dst_row_1_link.sync()

        # Actually do the work of writing the change out
        dst_row_1_link.sync()
        dst_row_2_link.sync()

    # Todo: Need tests for the other col-value pairs
    def update_interlink(self, primary_row, secondary_row, priority="unchanged", **col_value_pairs):
        """
        Update the link row connecting the primary_row and the secondary_row.
        Errors if there is no link to update.
        :param primary_row: The primary row in the link
        :param secondary_row: The secondary row in the link
        :param priority: highest, lowest or unchanged
        :param col_value_pairs: Pass an other link variables you want updated as keywords
        :return interlink_row: The updated row, with the updates having been written out to the database
        """
        interlink_row = self.get_interlink_row(primary_row=primary_row, secondary_row=secondary_row)
        primary_row_table = primary_row.table
        secondary_row_table = secondary_row.table

        # Update the priority to the newly given quantity
        # Process the priority - only numbers can be written into the priority column
        priority_col = self.driver_wrapper.get_link_column(primary_row_table, secondary_row_table, "priority")
        priority_key = six_unicode(priority).lower().strip()
        if priority is None:
            interlink_row[priority_col] = 0
        elif priority_key == "unchanged":
            pass
        elif priority_key == "highest" or priority_key == "lowest":
            priority_num = self.get_max(priority_col) if priority_key == "highest" else self.get_min(priority_col)
            try:
                priority_val = int(priority_num) + 1 if priority_key == "highest" else int(priority_num) - 1
            except (ValueError, TypeError) as e:
                err_str = "get_max for a priority column appears to have returned something not a number"
                err_str = default_log.log_exception(
                    err_str,
                    e,
                    "ERROR",
                    ("priority_num", priority_num),
                    ("primary_row", primary_row),
                    ("secondary_row", secondary_row),
                    ("priority", priority),
                )
                raise DatabaseIntegrityError(err_str)
            else:
                interlink_row[priority_col] = priority_val
        elif isinstance(priority, Number):
            interlink_row[priority_col] = priority
        else:
            err_str = "priority type not recognized and cannot be parsed"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
                ("priority", priority),
                ("priority_type", type(priority)),
            )
            raise InputIntegrityError(err_str)

        # Update everything else specified by the keyword pairs
        for col in col_value_pairs:
            link_row_col = self.driver_wrapper.get_link_column(primary_row_table, secondary_row_table, col)
            interlink_row[link_row_col] = col_value_pairs[col]

        interlink_row.sync()
        return interlink_row

    # Todo: Test this with both a tuple and list of ids
    def update_interlink_priority(self, primary_row, secondary_table, ordered_ids):
        """
        Re-write the priorities of all the rows in a secondary table that are linked to a primary row.
        :param primary_row: All the rows linked to this row from the secondary table will have their priorities updated
        :param secondary_table: All rows, linked to the primary row, in this secondary table will be updated
        :param ordered_ids: The order of the ids - the rows in the secondary table will be re-ordered so they have this
                            order.
        :return:
        """
        secondary_rows = self.get_interlinked_rows(target_row=primary_row, secondary_table=secondary_table)
        assert len(secondary_rows) == len(ordered_ids)

        secondary_row_map = dict((int(r.row_id), r) for r in secondary_rows)

        # Add the rows in the order specified by the ordered_ids
        ordered_ids = deepcopy(ordered_ids)
        ordered_ids.reverse()

        for row_id in ordered_ids:
            secondary_row = secondary_row_map[int(row_id)]
            self.update_interlink(primary_row, secondary_row, priority="highest")

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHOD TO UNLINK TWO ROWS STARTS HERE

    def unlink_interlink(self, primary_row, secondary_row):
        """
        Remove any interlink rows linking the priamry_row and the secondary_row.
        Errors if there is not such row to delete.
        :param primary_row:
        :param secondary_row:
        :return:
        """
        link_row = self.get_interlink_row(primary_row=primary_row, secondary_row=secondary_row)
        self.delete(link_row)

    # Todo: Test on a table like ratings, where we can have multiple links between the same title and rating but with
    #       different types. That caused this method to error.
    # Todo: Test on multiple different type filters - including types filters which are lists
    def unlink_all(self, primary_row, secondary_table, type_filter=None):
        """
        Removes every interlink between the primary row and any row in the secondary table.
        :param primary_row:
        :param secondary_table:
        :param type_filter: If provided, then only links with this type will be removed
        :return:
        """
        linked_to_rows = self.get_interlinked_rows(target_row=primary_row, secondary_table=secondary_table)
        if type_filter is None:
            for linked_row in linked_to_rows:
                interlink_row = self.get_interlink_row(primary_row=primary_row, secondary_row=linked_row)
                self.delete(interlink_row)
        else:
            interlink_column = self.driver_wrapper.get_link_column(primary_row.table, secondary_table, "type")
            for linked_row in linked_to_rows:
                try:
                    interlink_row = self.get_interlink_row(primary_row=primary_row, secondary_row=linked_row)
                    interlink_rows = [
                        interlink_row,
                    ]
                except DatabaseIntegrityError:
                    # We might be dealing with a table like ratings
                    interlink_rows = self.get_interlink_row(
                        primary_row=primary_row, secondary_row=linked_row, onelink=False
                    )

                for ilr in interlink_rows:
                    if ilr[interlink_column] == type_filter:
                        self.delete(ilr)

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO WRITE TO INTRALINK TABLES START HERE

    # Todo: Need to extend to account for the other interlink data types
    def intralink_rows(self, primary_row, secondary_row, link_type):
        """
        Intralink two rows - with an allowed link_type.
        :param primary_row: This will be entered as the primary row
        :param secondary_row: This will be entered as the secondary row
        :param link_type:
        :return:
        """
        link_type = six_unicode(link_type).lower().strip()
        if not primary_row.table == secondary_row.table:
            err_str = "Cannot intralink rows from different table types"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
                ("link_type", link_type),
            )
            raise InputIntegrityError(err_str)
        table = primary_row.table

        if primary_row.row_id is None or secondary_row.row_id is None:
            err_str = "Both rows must have ids set before they can be linked."
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
                ("link_type", link_type),
            )
            raise InputIntegrityError(err_str)

        # Checks that the intralink type is one of those allowed for this table in preferences
        # Todo: Move this to init for performance
        allowed_types_name = "allowed_{0}_intralink_types".format(primary_row.table)
        try:
            allowed_link_types = self.preferences[allowed_types_name]
        except KeyError:
            info_str = "Allowed type name not found in preferences - no restrictions applied to intralink type"
            default_log.log_variables(info_str, "INFO", ("allowed_type_name", allowed_types_name))
        else:
            allowed_link_types = frozenset([six_unicode(lt).lower().strip() for lt in allowed_link_types])
            if link_type not in allowed_link_types:
                err_str = "Unable to intralink rows - link type not recognized"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("primary_row", primary_row),
                    ("secondary_row", secondary_row),
                    ("link_type", link_type),
                    ("allowed_link_types", allowed_link_types),
                )
                raise InputIntegrityError(err_str)

        intralink_row = dict()
        primary_col = self.driver_wrapper.get_intralink_column(table, "primary_id")
        secondary_col = self.driver_wrapper.get_intralink_column(table, "secondary_id")
        type_col = self.driver_wrapper.get_intralink_column(table, "type")
        intralink_row[primary_col] = primary_row.row_id
        intralink_row[secondary_col] = secondary_row.row_id
        intralink_row[type_col] = link_type

        intralink_row = Row(row_dict=intralink_row, database=self)
        intralink_row.ensure_row_has_id()
        intralink_row.sync()

        return intralink_row

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO READ INTRALINKED ROWS START HERE

    def get_intralink_row(self, primary_row, secondary_row):
        """
        Get the intralink row connecting the primary and secondary row - if any.
        :param primary_row:
        :param secondary_row:
        :return:
        """
        primary_table = primary_row.table
        secondary_table = secondary_row.table

        link_table_name = self.driver_wrapper.get_link_table_name(primary_table, secondary_table)
        if not link_table_name or (primary_table != secondary_table):
            err_str = "Given tables cannot be connected - or you have used an interlink method, not the intralink one"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
                ("link_table_name", link_table_name),
            )
            raise InputIntegrityError(err_str)

        primary_id_col = self.driver_wrapper.get_link_column(primary_table, primary_table, "primary_id")
        secondary_id_col = self.driver_wrapper.get_link_column(primary_table, primary_table, "secondary_id")

        candidate_rows = []
        # Search the table using the primary_id - refine using the secondary to return the actually desired result
        primary_id = six_unicode(primary_row.row_id)
        secondary_id = six_unicode(secondary_row.row_id)
        for row in self.search(table=link_table_name, column=primary_id_col, search_term=primary_id):
            if secondary_id == six_unicode(row[secondary_id_col]):
                candidate_rows.append(row)

        if len(candidate_rows) == 0:
            return None
        elif len(candidate_rows) == 1:
            return candidate_rows[0]
        else:
            err_str = "Rows are joined by more than one intralink row - which shouldn't happen."
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("candidate_rows", candidate_rows),
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
            )
            raise DatabaseIntegrityError(err_str)

    def get_intralink_rows(self, row, primary=True, secondary=True, link_type_filter=None):
        """
        Returns all intralink rows involving the given row.
        :param row:
        :param primary: If True return link rows where this row is the primary
        :type primary: bool
        :param secondary: If True return lik rows where this row is the secondary
        :type secondary: bool
        :param link_type_filter: Filter to remove any links but the ones with this type
        :return:
        """
        table = row.table
        row_id = six_unicode(row.row_id)

        intralink_table = self.driver_wrapper.get_link_table_name(table, table)
        intralink_table_primary_row = self.driver_wrapper.get_link_column(table, table, "primary_id")
        intralink_table_secondary_row = self.driver_wrapper.get_link_column(table, table, "secondary_id")

        row_pool = []
        # Search the intralink table for mentions of the id in the primary column
        if primary:
            primary_intralink_rows = self.search(
                table=intralink_table,
                column=intralink_table_primary_row,
                search_term=row_id,
            )
            row_pool.extend([r for r in primary_intralink_rows])

        # Search the intralink table for mentions of the id in the secondary column
        if secondary:
            secondary_intralink_rows = self.search(
                table=intralink_table,
                column=intralink_table_secondary_row,
                search_term=row_id,
            )
            row_pool.extend([r for r in secondary_intralink_rows])

        if link_type_filter is None:
            return row_pool
        else:
            intralink_table_link_type = self.driver_wrapper.get_link_column(table, table, "type")
            filtered_row_pool = [
                r for r in row_pool if six_unicode(r[intralink_table_link_type]) == six_unicode(link_type_filter)
            ]
            return filtered_row_pool

    def get_intralinked_rows(self, primary_row, secondary_row):
        """
        Get any rows intralinked to the given primary row.
        The row must be primary in the link - if it's secondary that means something different.
        If the primary_row is not None, and the secondary row is None, returns every title linked to that row with that
        row as the primary_id (so returns purely secondary rows).
        If the secondary_row is not None, and the primary row is None, returns all the title linked to that row with
        that row as the secondary_id (so returns purely secondary rows).
        If both the primary and the secondary rows are not None - errors. You probably want the intralink_row. There's
        a specific method for that and everything.
        :param primary_row:
        :param secondary_row:
        :return:
        """
        if primary_row is not None and secondary_row is not None:
            err_str = "You seem to have both the title rows that you could want - do you want the intralink row itself?"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
            )
            raise InputIntegrityError(err_str)
        if primary_row is None and secondary_row is None:
            err_str = "Both primary and secondary rows supplied to get_intralinked_rows where null"
            default_log.error(err_str)
            raise InputIntegrityError(err_str)

        # Get every row with a the primary_row_id as it's primary - return that
        if primary_row is not None:
            table = primary_row.table
            primary_row_id = six_unicode(primary_row.row_id)

            intralink_table = self.driver_wrapper.get_link_table_name(table, table)
            intralink_table_primary_row = self.driver_wrapper.get_link_column(table, table, "primary_id")
            intralink_table_secondary_row = self.driver_wrapper.get_link_column(table, table, "secondary_id")

            intralink_rows = self.search(
                table=intralink_table,
                column=intralink_table_primary_row,
                search_term=primary_row_id,
            )

            intralinked_rows = []
            for link_row in intralink_rows:
                secondary_id = link_row[intralink_table_secondary_row]
                intralinked_rows.append(self.get_row_from_id(table=table, row_id=secondary_id))
            return intralink_rows

        # Get every row with a the secondary_row_id as it's primary - return that
        elif secondary_row is not None:
            table = secondary_row.table
            secondary_row_id = six_unicode(secondary_row.row_id)

            intralink_table = self.driver_wrapper.get_link_table_name(table, table)
            intralink_table_primary_row = self.driver_wrapper.get_link_column(table, table, "primary_id")
            intralink_table_secondary_row = self.driver_wrapper.get_link_column(table, table, "secondary_id")

            intralink_rows = self.search(
                table=intralink_table,
                column=intralink_table_secondary_row,
                search_term=secondary_row_id,
            )

            intralinked_rows = []
            for link_row in intralink_rows:
                primary_id = link_row[intralink_table_primary_row]
                intralinked_rows.append(self.get_row_from_id(table=table, row_id=primary_id))
            return intralink_rows

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO DELETE INTRALINK ROWS START HERE

    # Todo: Consider renaming - unlink_intralink
    def unlinked_intralink(self, primary_row, secondary_row):
        """
        Unlink two rows that have been interlinked.
        If primary_row and secondary_row are both not None, removes any interlink between the primary and the
        secondary row.
        If the primary_row is not None - deletes any intralink rows with that row as the primary.
        If the secondary_row is not None - deletes any intralink rows with that row as secondary.
        If both are None - errors.
        :param primary_row:
        :param secondary_row:
        :return:
        """
        if primary_row is not None and secondary_row is not None:

            link_row = self.get_intralink_row(primary_row=primary_row, secondary_row=secondary_row)
            # Deal with the case where there is no link to remove
            if link_row is None:
                return
            self.delete(link_row)

        elif primary_row is not None and secondary_row is None:

            table = primary_row.table
            primary_id = primary_row.row_id

            # Search the intralink table for any rows with the given primary_id - delete them
            intralink_table = self.driver_wrapper.get_link_table_name(table1=table, table2=table)
            intralink_table_primary = self.driver_wrapper.get_link_column(
                table1=table, table2=table, column_type="primary_id"
            )
            link_rows = self.search(
                table=intralink_table,
                column=intralink_table_primary,
                search_term=primary_id,
            )

            [self.delete(l_r) for l_r in link_rows]

        elif primary_row is None and secondary_row is not None:

            table = primary_row.table
            secondary_id = secondary_row.row_id

            # Search the intralink table for any rows with the given primary_id - delete them
            intralink_table = self.driver_wrapper.get_link_table_name(table1=table, table2=table)
            intralink_table_primary = self.driver_wrapper.get_link_column(
                table1=table, table2=table, column_type="secondary_id"
            )
            link_rows = self.search(
                table=intralink_table,
                column=intralink_table_primary,
                search_term=secondary_id,
            )

            [self.delete(l_r) for l_r in link_rows]

        elif primary_row is None and secondary_row is None:

            err_str = "unlink_intralink called without content"
            default_log.error(err_str)
            raise InputIntegrityError(err_str)

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO READ TREE STRUCTURES FROM TABLES START HERE

    def get_root_row(self, start_row):
        """
        Get the root series of a tree.
        ALWAYS USE THIS INSTEAD OF get_root_series
        :param start_row:
        :return:
        """
        return self.get_root_series(start_row=start_row)

    # Todo: This method is terribly names - should be merged with the above and removed
    def get_root_series(self, start_row):
        """
        Get the root series of a tree.
        :param start_row:
        :return:
        """
        row_dict_list = self.driver_wrapper.get_linear_row_list(start_row.row_dict)
        return Row(database=self, row_dict=row_dict_list[0])

    def get_children(self, src_row):
        """
        Returns the immediate children of a row.
        :param src_row:
        :return:
        """
        src_row_table = src_row.table
        src_row_id = src_row.row_id
        table_parent_column = self.driver_wrapper.get_parent_column(src_row_table)
        return self.search(table=src_row_table, column=table_parent_column, search_term=src_row_id)

    def get_linear_row_list(self, start_row):
        """
        Takes a starting row. Iterates up the tree, making an index of rows as it goes.
        Starts from the highest entry, then proceeds down.
        .......... -> grandparent_series -> parent_series -> series
        :param start_row:
        :return tree_row_index:
        """
        row_dict_list = self.driver_wrapper.get_linear_row_list(start_row.row_dict)
        return [Row(row_dict=r, database=self) for r in row_dict_list]

    def get_all_tree_rows(self, start_row, back_iterate=True):
        """
        if back_iterate - start from a row - walk back up the tree to the root - then  walks back down the tree - adding
        every row it finds to the row set which it then returns.
        :param start_row:
        :param back_iterate:
        :return:
        """
        row_table = start_row.table
        row_parent_column = self.driver_wrapper.get_parent_column(row_table)
        row_id_column = self.driver_wrapper.get_id_column(row_table)
        if back_iterate:
            root_series = self.get_root_series(start_row)
        else:
            root_series = start_row

        row_pool = set()
        row_pool.add(root_series)
        found_series = set()

        while len(row_pool) != 0:

            current_series = row_pool.pop()
            current_id = current_series[row_id_column]

            # finds all the series which refer to the current_series in the series_parent column
            child_rows = self.search(table=row_table, column=row_parent_column, search_term=current_id)
            for row in child_rows:
                row_pool.add(row)

            found_series.add(current_series)

        return found_series

    def walk(self, start_row):
        """
        Walk the tree - yielding all the rows as you go.
        :param start_row:
        :return:
        """
        start_row_dict = start_row.row_dict
        for table_row_dict in self.driver_wrapper.walk(start_row_dict):
            yield Row(row_dict=table_row_dict, database=self)

    def search_tree(self, root_row, for_ids):
        """
        Search a tree looking for any of the ids in the for_ids object - if one is found which is in the object return
        True, else return False.
        e.g. used when trying to find out if a row is in the tree that's rooted at the root row - for example if you
        want to find out if a folder is inside another folder.
        :param root_row: The row to start the search with
        :param for_ids: Every id in the tree will be checked against this object.
        :return:
        """
        root_row_dict = root_row.row_dict
        target_table = root_row.table
        target_table_id_col = self.driver_wrapper.get_id_column(target_table)

        matched_ids = set()
        for child_row in self.driver_wrapper.walk(start_row=root_row_dict):
            if child_row[target_table_id_col] in for_ids:
                matched_ids.add(child_row[target_table_id_col])
        return matched_ids

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO WRITE TREE STRUCTURES START HERE

    # Todo: What happens when you try and nest rows from different tables
    # Todo: What happens when you try and nest a row inside itself? (should fail - might not)
    def nest_rows(self, parent_row, child_rows):
        """
        Takes a container row and a collection of target_rows. The target_rows are placed inside the container row.
        :param parent_row: A row in the form of a dict which will end up being the stem for all the rows in
        target_rows
        :param child_rows: Either one row, or an iterable of rows
        :return True/False: Checks against the database and makes sure that the change has been made (optional)
        """
        container_table = parent_row.table
        # Deals with the case of child_rows being a single row
        if isinstance(child_rows, Row):
            child_rows = [child_rows]

        # extract the id from the container_row - then set the parent category in all the target_rows to be that id
        container_row_id = parent_row.row_id
        target_rows_parent_column = self.driver_wrapper.get_parent_column(container_table)
        for row in child_rows:
            row[target_rows_parent_column] = container_row_id
            self.driver_wrapper.update_row(row.row_dict)

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - TREE STRUCTURES - DELETE

    def delete_tree(self, parent_row):
        """
        Removes the tree rooted at the parent_row entirely - all entries in the tree are removed.
        :param parent_row:
        :return:
        """
        # Due to the foreign key constraints removing the parent of a bunch of folders should also take out all children
        # of those folders. So deleting the root row should be enough to take out all the folders associated with it
        self.delete(parent_row)

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


