from __future__ import unicode_literals

import json
import re
import pprint
import queue as Queue
import uuid
from copy import deepcopy
from numbers import Number

from LiuXin.paths import LiuXin_default_database

from LiuXin.databases.drivers import loadDatabaseDriver
from LiuXin.databases.row import Row
from LiuXin.databases.maintenance_bot import Maintainer
from LiuXin.databases.custom_columns import CustomColumnDatabaseMixin
from LiuXin.databases.custom_columns import CustomColumnsDriverWrapperMixin

from LiuXin.exceptions import InputIntegrityError
from LiuXin.exceptions import DatabaseIntegrityError
from LiuXin.exceptions import LogicalError

from LiuXin.preferences import preferences

from LiuXin.utils.general_ops.language_tools import plural_singular_mapper
from LiuXin.utils.general_ops.python_tools import get_unique_id
from LiuXin.utils.general_ops.python_tools import smart_dictionary_merge
from LiuXin.utils.logger import default_log
from LiuXin.utils.localization import trans as _
from LiuXin.utils.general_ops.json_ops import to_json_str

# Py2/Py3 compatibility layer
from LiuXin.utils.lx_libraries.liuxin_six import six_unicode

# Todo: Embed this version number in the database - so that we can check the version of the code used to produce each
#       test database
__object_version__ = (0, 2, 17)

# Todo: Point uuid requests to the library_id instead


class Database(CustomColumnDatabaseMixin):
    """
    Represents a database which LiuXin could be connected to. Access to the database should always be through this class
    The default database is simply the database located in LiuXin_data.
    Everything returned from this class should be a Row.
    To get a Row return - call database.method. To get a row_dict - call database.backend.method
    """

    # Todo: Split some of these out into factory methods and slim this down
    def __init__(
        self,
        metadata=None,
        db_type="SQLite",
        create=False,
        backup=True,
        existing_driver=None,
    ):
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

        # Helper attributes
        self.driver = None
        self.driver_wrapper = None
        self.macros = None

        # Fundamental constants for this database
        if existing_driver is None:
            self.standard_init(metadata=metadata, db_type=db_type, create=create, backup=backup)
        else:
            assert metadata is None, "driver is provided - it's assumed that the db metadata is contained within"
            self.existing_driver_init(existing_driver)

        # Todo: This is stupid and should be replaced with something less ... intellectually challenged
        self.get = self.driver.conn.get
        self.conn = self.driver.conn

        # Used as a lookup cache for if the link table in question has a priority column
        # Keyed with the table, value with True or False
        self._link_has_priority = dict()

        # Queue for dirtied records
        self.dirty_records_queue = Queue.Queue()

        self.driver.dirty_records_queue = self.dirty_records_queue
        self.driver_wrapper.dirty_records_queue = self.dirty_records_queue

    def existing_driver_init(self, existing_driver):
        """
        Startup method called when the drivber already exists. Useful for testing.
        :param existing_driver:
        :return:
        """
        # Load the driver constructor - use this to make the driver instance for this database
        self.driver = existing_driver
        self.macros = self.driver.macros

        # Load the backend with the driver.
        self.driver_wrapper = DriverWrapper(self.driver)
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

        self.main_tables = None

        self.custom_tables = None

        self.interlink_tables = None
        self.intralink_tables = None

        self.dirtiable_tables = None

        self.helper_tables = {
            "conversion_options",
            "compressed_files",
            "new_books",
            "database_metadata",
            "hashes",
            "preferences",
            "books_plugin_data",
            "ratings",
            "metadata_dirtied_books",
            "library_id",
            "database_version",
        }

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
            metadata = dict()
            metadata["database_path"] = LiuXin_default_database
        self.metadata = metadata

        # Load the driver constructor - use this to make the driver instance for this database
        self.type = db_type
        self.driver = loadDatabaseDriver(db_type)(self.metadata, self)
        self.macros = self.driver.macros

        # Load the backend with the driver.
        self.driver_wrapper = DriverWrapper(self.driver)
        self.lock = self.driver_wrapper.lock

        # If the create keyword is set to True, then create the database anew.
        if create:
            self.create_new_database(backup=backup)
            # Reload the database driver to take the update into account
            self.driver = loadDatabaseDriver(db_type)(self.metadata, self)
            self.driver_wrapper = DriverWrapper(self.driver)
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
            self.main_tables = None
            self.custom_tables = None
            self.interlink_tables = None
            self.intralink_tables = None

            self.dirtiable_tables = None

            self.allowed_type_tables = None

            self.helper_tables = {
                "conversion_options",
                "compressed_files",
                "new_books",
                "database_metadata",
                "hashes",
                "preferences",
                "books_plugin_data",
                "ratings",
                "metadata_dirtied_books",
                "library_id",
                "database_version",
            }

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

    def __del__(self):
        """
        Preform shutdown.
        :return:
        """
        self.break_cycles()
        self.lock.close()

    # Todo: Might actually want to delete these objects - and this might be an internal method
    def break_cycles(self):
        """
        Explicitly zero all stored objects in the right order.
        :return:
        """
        self.driver_wrapper = None
        self.driver = None
        self.maintenance = None

    # Todo: THis might also want to be an internal method
    def check_rating_table(self):
        """
        Checks that the ratings table is as it should be.
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
        Checks that the null rows have been entered as they should be.
        NUll rows are used when linking two tables togeher - to indicate that the first result should be recorded as
        None - e.g. series linked to a title - if you want the first series to be None, then either make no links or
        link the title to the Null row first.
        Not every table has a null row - sometimes nullification is accomplished by deleting all the rows instead.
        :return:
        """
        # Ensure the series null row - check if it already exists - if it does then make sure that the series value
        # is set to None - if not then create it.
        series_0_row = self.driver_wrapper.get_row_from_id("series", 0)
        if not series_0_row:
            series_null_row = dict()
            series_null_row["series_id"] = 0
            self.driver_wrapper.add_row(series_null_row)
        else:
            series_0_row["series"] = None
            self.driver_wrapper.update_row(series_0_row)

        # Ensure the publisher null row
        pub_0_row = self.driver_wrapper.get_row_from_id("publishers", 0)
        if not pub_0_row:
            pub_null_row = dict()
            pub_null_row["publisher_id"] = 0
            self.driver_wrapper.add_row(pub_null_row)
        else:
            pub_0_row["publisher"] = None
            self.driver_wrapper.update_row(pub_0_row)

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
        self.main_tables = set()
        self.custom_tables = set()
        self.interlink_tables = set()
        self.intralink_tables = set()
        self.allowed_type_tables = set()

        # Check that the pre set helper tables can be found in the all_tables field
        for helper_table in self.helper_tables:

            if helper_table not in self.all_tables:
                err_str = "Unable to find a helper table in the database return"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("helper_table", helper_table),
                    ("all_tables", pprint.pformat(self.all_tables)),
                )
                raise DatabaseIntegrityError(err_str)

        # Populate the individual categories
        for table in self.all_tables:
            table_cat = self.categorize_table(table)
            if table_cat == "main":
                self.main_tables.add(table)
                continue
            if table_cat == "interlink":
                self.interlink_tables.add(table)
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
        self.main_tables.discard("sqlite_sequence")

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

    def create_new_database(self, blank=True, backup=True):
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

    def get_tables(self):
        """
        Directly get the tables for the currently loaded database
        :return:
        """
        return self.driver_wrapper.get_tables()

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
        Gets a row from it's particular id.
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
        Takes a row and a column - in a table linked to the row. Returns a set of every value of that column in a row
        linked to the given target row - for example, searching with a title_row "creator" yields every creator linked
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


class DriverWrapper(CustomColumnsDriverWrapperMixin):
    """
    Everything coming out of this class should be a be a row_dictionary.
    """

    def __init__(self, driver):
        """
        Initialize with the database driver - this will be used to access the
        :param driver:
        :return:
        """
        self.driver = driver

        # Will be loaded by the parent DatabasePing process with the allowed table names
        self.all_tables = None
        self.main_tables = None
        self.interlink_tables = None
        self.intralink_tables = None
        self.helper_tables = None

        self.dirtiable_tables = []
        self.dirty_records_queue = None

        # Acquires a lock for the database that can be used in a with statement.
        self.lock = self.get_connection()

        super(DriverWrapper, self).__init__(db=None, macros=None)

    def __del__(self):
        self.break_cycles()

    def break_cycles(self):
        """
        Preform shutdown in a sensible order - deleting each of the objects in the right order.
        :return:
        """
        del self.lock
        del self.driver

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO GET COLUMNS NAMES FROM TABLES AND VISA VERSA START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def get_column_base(self, table_name):
        """
        Returns the base column name for the given table - all column names are formed from this base - typically of the
        form base_something (e.g. the base of titles is title, such as title_id).
        :param table_name:
        :return:
        """
        return self.driver.direct_get_column_base(table_name)

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO GET BASIC INFORMATION ABOUT THE DATABASE START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def get_tables(self):
        """
        Directly get the tables for the currently loaded database
        :return:
        """
        return self.driver.direct_get_tables(force_refresh=True)

    def get_column_headings(self, table):
        """
        Gets the column headings for a table in the database.
        :param table:
        :return column_headings: An index of column headings in the order they appear on the database
        """
        return self.driver.direct_get_column_headings(table)

    def get_view_column_headings(self, view):
        """
        Gets the column headings for a table in the database.
        :param table:
        :return column_headings: An index of column headings in the order they appear on the database
        """
        return self.driver.direct_get_view_column_headings(view)

    def get_tables_and_columns(self):
        """
        Returns a dictionary keyed by the table name with the column headings as the values.
        :return table_and_columns:
        """
        return self.driver.direct_get_tables_and_columns()

    def get_highest_id(self, target_table):
        """
        Gets and returns the highest id in the ids column of a table.
        :param target_table:
        :return:
        """
        return self.driver.direct_get_highest_id(target_table)

    @property
    def user_version(self):
        """
        Returns the user_version for this database.
        :return:
        """
        return self.driver.user_version

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO GET INFORMATION ABOUT SPECIFIC TABLES START HERE
    # ------------------------------------------------------------------------------------------------------------------
    # Todo: Need to standardize target_table, table and table_name to something
    def get_record_count(self, target_table):
        """
        Returns the number of records in a given table.
        :param target_table:
        :return:
        """
        return self.driver.direct_get_record_count(target_table)

    def get_id_column(self, table):
        """
        Every table in the database should have an id column.
        Currently assumes that there is a column with a name ending in id and that if this is true for multiple rows
        that the shortest string ending in id is the id string. Should be tested every time a new column is added.
        :param table:
        :return:
        """
        return self.driver.direct_get_id_column(table)

    def get_datestamp_column(self, table):
        """
        Return the datestamp column for the given table - every table should have one, as it's needed in version control
         - deciding which data should have primacy when merging two rows.
        :param table: The table to retrive the datestamp column for
        :return:
        """
        return self.driver.direct_get_datestamp_column(table)

    def check_for_intralink_table(self, table_name):
        """
        Takes the name of a table. Returns the name of the intralink table if one exists, or False if it doesn't
        :param table_name:
        :return False or intralink_table_name:
        """
        table_name = six_unicode(table_name).lower()
        column_name_local = self.get_column_base(table_name)

        intralink_name = "{0}_{0}_intralinks".format(column_name_local)

        # checks that the given table name and the generated table name are in the list of known table names
        table_names = self.get_tables_and_columns().keys()

        if (table_name in table_names) and (intralink_name in table_names):
            return intralink_name
        else:
            return False

    def get_interlinked_tables(self, table_name):
        """
        Takes a table name - works out every table which is linked to it. Returns the set of linked tables.
        Does not include an intralink tables, if the main_table has it.
        :param table_name:
        :return linked_tables:
        """
        linked_tables = set()
        for main_table in self.main_tables:
            possible_interlink_table = self.get_link_table_name(main_table, table_name)
            if possible_interlink_table in self.interlink_tables:
                linked_tables.add(main_table)
        return linked_tables

    def get_link_table_name(self, table1, table2):
        """
        Takes two tables. Returns their link table name (if one exists). Returns False otherwise.
        This method can thus be used to both check to see if such a link exists and
        :param table1:
        :param table2:
        :return link_table_name/False: The name of the link table, if valid, or false if the table doesn't exist.
        """
        valid_tables = self.get_tables()

        if table1 != table2:
            table1_row_name = self.get_column_base(table1)
            table2_row_name = self.get_column_base(table2)
            tables = [table1_row_name, table2_row_name]
            tables.sort()
            link_table_name = "{}_{}_links"
            link_table_name = link_table_name.format(tables[0], tables[1])

            if link_table_name not in valid_tables:
                return False
            else:
                return link_table_name
        else:
            table_row_name = self.get_column_base(table1)
            link_table_name = "{}_{}_intralinks"
            link_table_name = link_table_name.format(table_row_name, table_row_name)

            if link_table_name not in valid_tables:
                return False
            else:
                return link_table_name

    def get_interlink_column(self, table1, table2, column_type):
        """
        See get_link_column.
        :param table1:
        :param table2:
        :param column_type:
        :return:
        """
        return self.get_link_column(table1, table2, column_type)

    # Todo: This shouldn't be a DatabaseIntegrityError - something like "no such error"
    def get_link_column(self, table1, table2, column_type):
        """
        Get the name of a column in the link table connecting the two table - for example. table1 = "titles",
        table2 = "creators", column_type = "priority" returns creator_title_link_priority.
        Returns False if the table doesn't exist - errors if the table exists but the requested column doesn't
        :param table1:
        :param table2:
        :param column_type:
        :return:
        """
        link_table = self.get_link_table_name(table1=table1, table2=table2)

        # Todo: I think? This currently does nothing useful - as this is not a sane way of doing an existence check
        # If the link_table doesn't exist - error out
        if not link_table:
            err_str = "Tables cannot be joined"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("table1", table1),
                ("table2", table2),
                ("column_type", column_type),
            )
            raise InputIntegrityError(err_str)

        link_col_base = self.get_column_base(link_table)
        link_col = link_col_base + "_" + six_unicode(column_type)

        allowed_columns = self.get_column_headings(link_table)
        if link_col not in allowed_columns:
            err_str = "column_type not recognized"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("table1", table1),
                ("table2", table2),
                ("column_type", column_type),
                ("link_col", link_col),
                ("allowed_columns", allowed_columns),
            )
            raise DatabaseIntegrityError(err_str)
        else:
            return link_col

    def get_intralink_column(self, table, column_type):
        """
        Get the name of an intralink column in the intralink table connecting two rows in the same table.
        e.g. a call with ("titles", "type") will return title_title_intralink_type
        If the table can't be intralinked, return False.
        :param table:
        :param column_type:
        :return:
        """
        return self.get_link_column(table, table, column_type)

    def get_scratch_column(self, table):
        """
        Every table in the database should have a scratch column. This finds the name of that column for the table.
        :param table:
        :return:
        """
        column_headings = self.get_column_headings(table)
        for heading in column_headings:
            if heading.endswith("scratch"):
                return heading

        err_str = "Warning - get_scratch_column failed to find a scratch column for that table.\n"
        err_str = default_log.log_variables(err_str, "ERROR", ("table", table), ("column_headings", column_headings))
        raise DatabaseIntegrityError(err_str)

    def get_parent_column(self, table_name):
        """
        Returns the parent column for the table if it exists.
        :param table_name:
        :return:
        """
        table_name = deepcopy(table_name)
        tables_and_columns = self.get_tables_and_columns()
        if table_name not in tables_and_columns:
            err_str = "get_parent_column failed - input was not a regonized table."
            err_str = default_log.log_variables(err_str, "ERROR", ("table", table_name))
            raise InputIntegrityError(err_str)

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

    def get_display_column(self, table_name):
        """
        Gets the display column for a table (currently based off the shortest column which is not the id column)
        :param table_name:
        :return display_column:
        """
        # Todo: Merge with the method over in the driver - as they are basically identical
        table_name = deepcopy(table_name)
        table_id_column = self.get_id_column(table_name)
        tables_and_columns = self.get_tables_and_columns()
        column_names = deepcopy(tables_and_columns[table_name])

        # a display column should never be the id column. Removing it.
        try:
            column_names.remove(table_id_column)
        except ValueError:
            err_str = "identified table_id_column not in column names.\n"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("table_name", table_name),
                ("table_id_column", table_id_column),
                ("column_names", column_names),
            )
            raise DatabaseIntegrityError(err_str)
        column_names.sort(key=lambda x: len(x))
        if len(column_names) == 0:
            err_str = "table_name seems to only have an id column. If that.\n"
            err_str = default_log.log_variables(err_str, "ERROR", ("table_name", table_name))
            raise DatabaseIntegrityError(err_str)
        else:
            return column_names[0]

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO READ AND WRITE METADATA TO THE DATABASE START HERE
    # ------------------------------------------------------------------------------------------------------------------
    # Todo: Be nice to be able to get a full readout of all the metadata fields
    # Todo: ... Actually use this?
    def read_metadata(self, field):
        """
        MetaData can be embedded directly into the database. This method allows you to read it.
        :param field: The field that will be read
        :return value: The value of the field from the MetaData table
        """
        return self.driver.direct_read_metadata(md_field_name=field)

    def write_metadata(self, field, value):
        """
        Write the given value to the specified field on the database.
        :param field:
        :param value:
        :return:
        """
        return self.driver.direct_write_metadata(md_field_name=field, md_field_value=value)

    def get_uuid(self):
        """
        Each database should have a unique identifier
        :return:
        """
        return self.driver.direct_get_db_unique_id()

    def set_uuid(self, new_force_value=None):
        """
        Sets the database unique id to be a certain value.
        :param new_force_value: If provided the db_unique id will be set to this value. If not it'll be a random uuid4.
        :return status: True/False (actually wither True, or an error is raised)
        """
        status = self.driver.direct_set_db_unique_id(force_value=new_force_value)
        return status

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO ADD TO THE DATABASE START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def add_row(self, row_dict):
        """
        Takes a single row in the form of a dictionary and adds the values to the database.
        :param row_dict:
        :return:
        """
        self.driver.direct_add_simple_row_dict(row_dict)

    def add_multiple_rows(self, row_dict_list):
        """
        Takes an index of row_dicts and adds each of them to the database.
        :param row_dict_list:
        :return:
        """
        self.driver.direct_add_multiple_simple_row_dicts(row_dict_list)

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO UPDATE THE ROW/DATABASE START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def update_row(self, row_dict):
        """
        Takes a row in the form of a row_dict. Updates that row_dict into the database.
        This is the method Row ultimately calls to update itself - THUS DO NOT CALL WITH ROW. IT WAS CAUSE A REALLY
        INEFFICIENT RECURSION
        :param row_dict:
        :return:
        """
        status = self.driver.direct_update_row_dict(row_dict)
        return status

    def ensure_row_has_id(self, row_dict):
        """
        Takes a row_dict - ensures that it has an id (pulling one off a blank row if required)
        :param row_dict:
        :return row_dict::
        """
        row_dict = deepcopy(row_dict)
        table_name = self.identify_table_from_row_dict(row_dict)
        id_name = self.get_id_column(table_name)

        if id_name in row_dict.keys():
            test = row_dict[id_name]
            if test is not None:
                return row_dict
            else:
                blank_row = self.get_blank_row(table_name)
                row_dict[id_name] = blank_row[id_name]
                return row_dict
        else:
            blank_row = self.get_blank_row(table_name)
            row_dict[id_name] = blank_row[id_name]
            return row_dict

    def update_column(self, table, row_id, column, new_value):
        """
        Set the column entry for the specified table and row_id to zero.
        :param table:
        :param row_id:
        :param column:
        :param new_value:
        :return:
        """
        # Check that the column exists and is in the specified table
        col_table = self.identify_table_from_column(column)
        if table != col_table:
            err_str = "LiuXin.databases.database:nullify_column failed - column/table didn't match\n"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("table", table),
                ("row_id", row_id),
                ("column", column),
            )
            raise InputIntegrityError(err_str)

        # Having the row deleted or changed while this function runs would be annoying
        with self.lock:
            # Get the row - update the column - write back to the database
            target_row = self.get_row_from_id(table=table, row_id=row_id)
            target_row[column] = new_value
            self.update_row(target_row)

        return True

    def update_columns(self, values_map, field=None, table=None):
        """
        Bulk update takes a sequences for updating and writes it's values into the field of the specified table.
        Values map should be keyed with the id of the record and values with a dictionary of the values which should
        be updated - or keyed with the id, values with a string, and the map should be provided with a field name (from
        which the table can be calculated) and/or a table in case the field name is ambiguous.
        :param values_map
        :param field:
        :param table:
        :return:
        """
        return self.driver.direct_update_columns(id_values_map=values_map, field=field, table=table)

    def complete_row(self, partial_row):
        """
        Takes a partial row - tries to complete it from the database.
        The values already in the row are taken in preference to the values off the database.
        :param partial_row:
        :return:
        """
        partial_row = deepcopy(partial_row)
        partial_table = self.identify_table_from_row_dict(partial_row)
        partial_row_id = self.get_id_from_row(partial_row)

        if partial_row_id is None:
            err_str = "Couldn't complete partial row - id was not found"
            err_str = default_log.log_variables(err_str, "ERROR", ("partial_row", partial_row))
            raise InputIntegrityError(err_str)

        db_full_row = self.get_row_from_id(table=partial_table, row_id=partial_row_id)
        if db_full_row is False:
            raise InputIntegrityError("row couldn't be completed - {}".format(partial_row))

        return smart_dictionary_merge(partial_row, db_full_row, key_protect=True)

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO DELETE FROM THE DATABASE START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def delete(self, target_table, column, value):
        """
        Deletes all the entries which equal that column value in the table.
        :param target_table:
        :param column:
        :param value: If is a list, or set, deletes all the elements in that list or set.
        :return:
        """
        if isinstance(value, (list, set)):
            return self.driver.direct_delete_many(target_table=target_table, column=column, values=value)
        else:
            return self.driver.direct_delete(target_table=target_table, column=column, value=value)

    def delete_by_id(self, target_table, row_id):
        """
        Deletes all the entries which have that id from that table.
        :param target_table:
        :param row_id: If is a list or set, deletes all the elements in that list or set.
        :return:
        """
        if isinstance(row_id, (list, set)):
            return self.driver.direct_delete_many_by_ids(target_table, row_id)
        else:
            return self.driver.direct_delete_row_by_id(target_table, row_id)

    def nullify_column(self, table, row_id, column):
        """
        Delete an entry for in a specified column for a specified row in a table
        :param table:
        :param row_id:
        :param column:
        :return:
        """
        return self.update_column(table, row_id, column, None)

    def clear(self, target_table):
        """
        A separate method - so as to reduce the chance of accidentally
        :param target_table:
        :return:
        """
        return self.driver.direct_clear_table(target_table)

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO SEARCH THE DATABASE START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def get_row_from_id(self, table, row_id):
        """
        Gets a row_dict directly from the DatabasePing.
        :param table:
        :param row_id:
        :return:
        """
        return self.driver.direct_get_row_dict_from_id(table, row_id)

    # Todo: Need a method to get the name of all the views for a database
    def get_view_row_from_id(self, view, row_id):
        """
        Returns a row from a view of the database.
        :param view:
        :param row_id:
        :return:
        """
        return self.driver.direct_get_view_row_dict_from_id(view, row_id)

    # Todo: Should also be a "get_all_view_rows" method, for symmetry
    def get_all_rows(self, table, sort_column=None, reverse=False):
        """
        Gets a list of all the rows dicts in the given table from the database.
        :param table:
        :param sort_column:
        :param reverse:
        :return:
        """
        return self.driver.direct_get_all_rows(table, sort_column, reverse)

    def search(self, table, column, search_term):
        """
        Searches a specified column in a table by the given search term. Returns all rows which match that term.
        :param table:
        :param column:
        :param search_term:
        :return:
        """
        return self.driver.direct_search_table(table, column, search_term)

    def get_blank_row(self, table):
        """
        There are times when the actual row_id of a row matters.
        Such as when it's going to be written into the name of a folder or file.table
        get_blank_row gives you an empty row which data can be written into.
        :param table: The table the row should be in.
        """
        table = str(table)

        # using this as a key to find the row after it has been added to the table
        new_row_id = get_unique_id()

        table_scratch_column = self.get_scratch_column(table)

        # a row identified by a unique row id in the scratch column should now exist in the table
        new_row = dict()
        new_row[table_scratch_column] = new_row_id
        self.add_row(new_row)
        # this required removing the not-null constraints - this might cause trouble later

        rows = self.search(table, table_scratch_column, new_row_id)

        if len(rows) == 0:
            err_str = "Error - get_blank_row failed to create new blank row. Aborting.\n"
            raise DatabaseIntegrityError(err_str)
        elif len(rows) > 1:
            err_str = "Error - get_blank_row found multiple rows with the same UUID.\n"
            err_str += repr(rows)
            raise DatabaseIntegrityError(err_str)

        row = rows[0]

        # # Check that the scratch column actually matches the generated value - this has happened.
        # if row[table_scratch_column] != new_row_id:
        #     err_str = "Scratch columns did not match!"
        #     raise DatabaseIntegrityError(err_str)

        # blanking the table scratch column. Should be applied if the row is synced back into the database.
        row[table_scratch_column] = ""
        return row

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO GET INFORMATION FROM ROW DICTS START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def identify_table_from_row_dict(self, row_dict):
        """
        Takes a row. Attempts to identify which row it came from.
        :param row_dict: The row (dict) to be parsed
        :return table_name: The table name (string)
        """
        # if this method is called with a null row it will complain. If warn is true
        if isinstance(row_dict, Row):
            err_str = "LiuXin.databases.database:identify_table_from_row_dict passed a Row not a row.\n"
            err_str = default_log.log_variables(err_str, "ERROR", ("row_dict", row_dict))
            raise NotImplementedError(err_str)
        elif len(row_dict) == 0:
            return False

        # If the row could be from multiple rows then an error should be thrown
        candidate_matches = []
        tables_and_columns = self.get_tables_and_columns()
        tables = tables_and_columns.keys()
        row_columns = row_dict.keys()

        current_match = True
        for table in tables:
            # Using the known tables and columns to preform the test
            current_columns = tables_and_columns[table]
            for column in row_columns:
                if column not in current_columns:
                    current_match = False
            if current_match:
                candidate_matches.append(table)
            current_match = True

        if len(candidate_matches) > 1:
            err_str = "identify_table_from_row has produced multiple results.\n"
            err_str += "Check the database.\n"
            err_str += "Candidate_matches: " + repr(candidate_matches) + "\n"
            err_str += "Row_dict: " + repr(row_dict) + "\n"
            raise DatabaseIntegrityError(err_str)
        # You could validate the table name here - but it's produced from data off the table it should be valid anyway
        elif len(candidate_matches) == 1:
            return candidate_matches[0]
        elif len(candidate_matches) == 0:
            err_str = "identify_table_from_row unable to find matching table\n"
            err_str += "row_dict: " + repr(row_dict) + "\n"
            raise DatabaseIntegrityError(err_str)
        else:
            raise LogicalError("Logical error in identify_table_from_row")

    def get_id_from_row(self, row_dict):
        """
        Takes a row. Extracts an id from it if possible. If not returns False
        :param row_dict:
        """
        row_table = self.identify_table_from_row_dict(row_dict)
        row_id_column = self.get_id_column(row_table)

        if row_id_column not in row_dict.keys():
            return None
        else:
            return row_dict[row_id_column]

    # Todo: Need to remove the error option - should just always error
    def identify_table_from_column(self, column_heading, error=True):
        """
        Takes a column heading. Works out the table it comes from.
        :param column_heading:
        :param error: Should the method error out, or return None
        :return:
        """
        column_heading = six_unicode(deepcopy(column_heading))
        headings_and_columns = self.get_tables_and_columns()
        tables = headings_and_columns.keys()

        for table in tables:
            column_headings = headings_and_columns[table]
            if column_heading in column_headings:
                return table
        else:
            err_str = "identify_table_from_column failed.\n"
            err_str = default_log.log_variables(err_str, "INFO", ("column_heading", column_heading))
            if error:
                raise InputIntegrityError(err_str)
            else:
                return None

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO DEAL WITH TREE STRUCTURES IN TABLES
    # ------------------------------------------------------------------------------------------------------------------
    # Todo: Needs to throw an error when used on a table without a tree structure
    def get_linear_row_list(self, start_row):
        """
        Takes a starting row. Iterates up the tree, making an index of rows as it goes.
        Starts from the highest entry, then proceeds down.
        .......... -> grandparent_series -> parent_series -> series
        :param start_row:
        :return tree_row_index:
        """
        table = self.identify_table_from_row_dict(start_row)
        table_parent_column = self.get_parent_column(table)

        linear_rows = []
        current_row = start_row
        try:
            current_parent_id = start_row[table_parent_column]
            if six_unicode(current_parent_id).lower() == "none" or current_parent_id is None:
                linear_rows.append(start_row)
                return linear_rows
        except KeyError:
            linear_rows.append(start_row)
            return linear_rows

        while six_unicode(current_parent_id).upper() != "NONE" and current_parent_id is not None:
            # extracting the current parent id
            try:
                current_parent_id = current_row[table_parent_column]
                if current_parent_id == "NONE":
                    linear_rows = [current_row] + linear_rows
                    return linear_rows
            except KeyError:
                linear_rows = [current_row] + linear_rows
                return linear_rows

            linear_rows = [current_row] + linear_rows

            if six_unicode(current_parent_id).lower() != "none" and current_parent_id is not None:
                current_row = self.get_row_from_id(table=table, row_id=current_parent_id)
            else:
                break

        return linear_rows

    # Todo: Again, should error when called on a table which does not have a tree structure
    def set_tree_ids(self, table):
        """
        Every tree should have a unique tree id - this goes through and makes sure it's been set for every tree in the
        given table.
        :param table:
        :return:
        """
        return self.driver.direct_set_tree_ids(table)

    def set_full_column(self, table):
        """
        Rows which are part of a tree structure have a _full column. This is a string representation of their place in
        the tree structure. This method populates the full column for the target table.
        :param table:
        :return:
        """
        return self.driver.direct_set_full_column(target_table=table)

    def walk(self, start_row):
        """
        Walk the tree yielding all the rows in it, starting with the start_row itself.
        :param start_row: Walk starts here.
        :return:
        """
        table = self.identify_table_from_row_dict(start_row)
        table_id_col = self.get_id_column(table)
        table_parent_col = self.get_parent_column(table)

        if table_parent_col is None or table_parent_col is False:
            err_str = "Given table does not have a tree structure - so can't be walked"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("start_row", start_row),
                ("table", table),
                ("table_id_col", table_id_col),
                ("table_parent_col", table_parent_col),
            )
            raise InputIntegrityError(err_str)

        return self._walk(start_row, table, table_id_col, table_parent_col)

    def _walk(self, start_row, table, table_id_col, table_parent_col):
        # Load the ids pool with the ids of parent rows - search for them in the parent column and yield those rows
        # If a row has no children (not referenced in any parent column) then it's a leaf row and we're done for that
        # branch
        ids_pool = set()
        ids_pool.add(int(start_row[table_id_col]))

        # Start the walk by yielding the start row - then working through the ids pool - take each id from it, find all
        # the children, yield them and add their ids for recursion on down. Continue until all rows have been yielded.
        yield start_row
        while ids_pool:

            working_id = ids_pool.pop()
            working_children = self.search(table=table, column=table_parent_col, search_term=working_id)

            for child_row in working_children:
                ids_pool.add(int(child_row[table_id_col]))
                yield child_row

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO DEAL WITH TRIGGERS START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def get_triggers(self):
        """
        Returns all the triggers currently defined on the database.
        :return:
        """
        return self.driver.direct_get_triggers()

    def drop_triggers(self, triggers):
        """
        Drops triggers which are named in the list
        :param triggers:
        :return:
        """
        return self.driver.direct_drop_triggers(triggers)

    def drop_all_triggers(self):
        """
        Drops all triggers which are defined on the database.
        :return:
        """
        all_triggers = self.get_triggers()
        return self.drop_triggers(all_triggers)

    # ------------------------------------------------------------------------------------------------------------------
    # - SPECIAL METHODS START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def get_all_hashes(self):
        """
        Returns a set of all the hashes on the database.
        :return:
        """
        return self.driver.direct_get_all_hashes()

    # Todo: THese should be semi-private, because they're not offered all the time
    def shell(self):
        """
        Provides a shell for the underlying database.
        Front end for the database driver method.
        :return:
        """
        return self.driver.shell()

    def get_connection(self):
        """
        Gets a connection to the database - used for locking the database.
        :return:
        """
        return self.driver.get_connection()

    # Todo: row_dict has been outmoded - cut it out here
    def get_random_row(self, table, row_dict=None, direct_access=False):
        """
        Returns a random row from the given table. If row_dict is True returns it in the form of a dictionary - if it's
        False returns it in the form of a Row.
        Note DO NOT USE THIS WHEN CONSTRUCTING TEST DATABASES/FSM!
        The results of this method is platform dependant - so the test object will not be reproducible. This is rarely
        helpful.
        :param table:
        :param row_dict:
        :param direct_access:
        :return:
        """
        if row_dict is not None:
            raise NotImplementedError("row_dict was unexpectedly not None")
        return self.driver.direct_get_random_row_dict(target_table=table, direct=direct_access)

    # ------------------------------------------------------------------------------------------------------------------
    # - DIRECT EXECUTION SQL METHODS START HERE
    # ------------------------------------------------------------------------------------------------------------------
    # These methods should not be used if at all possible. They are here for testing a prototyping.

    # Todo: Turn semi private - very dependant on implementation
    def execute(self, sql, values=None):
        """
        Run SQL directly on the database.
        :param sql:
        :param values: Default to None
        :return:
        """
        return self.driver.direct_execute(sql, values)

    def executemany(self, sql, values=None):
        """
        Run an executemany command direct on the database
        :param sql:
        :param values:
        :return:
        """
        try:
            return self.driver.direct_executemany(sql, values)
        except ValueError as e:
            err_str = "ValueError while trying to executemany"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("sql", sql),
                ("values", values),
                ("type(values)", type(values)),
            )
            raise ValueError(err_str)

    def executescript(self, sqlscript):
        """
        Execute an SQL script on the database.
        :param sqlscript:
        :return:
        """
        return self.driver.direct_executescript(sqlscript)

    def get(self, *args, **kw):
        ans = self.execute(*args)
        if kw.get("all", True):
            return ans.fetchall()
        try:
            return ans.next()[0]
        except (StopIteration, IndexError):
            return None

    # Todo: Might want to be a get_dirtied method for symmetry
    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO DEAL WITH THE DIRTIED_QUEUE START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def get_dirtied_count(self):
        """
        Return the number of records in the dirtied records Queue.
        This calls the qsize method of the Queue and is thus only approximate.
        :return:
        """
        return self.dirty_records_queue.qsize()

    # Todo: Move this into the database - don't want to deal with the queue and want persistent between sessions
    def dirty_record(self, table, row_id, reason):
        """
        Add a record to the dirtied dictionary.
        :param table:
        :param row_id:
        :param reason:
        :return:
        """
        if table not in self.dirtiable_tables:
            wrn_str = "Unable to dirtied record - table not found.\n"
            default_log.log_variables(
                wrn_str,
                "WARNING",
                ("table", table),
                ("row_id", row_id),
                ("reason", reason),
            )
        else:
            self.dirty_records_queue.put((table, row_id, reason))

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO CREATE NEW MAIN/INTERLINK TABLES/COLUMNS START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def create_new_main_table(
        self,
        table_name,
        column_headings=None,
        link_to=None,
        link_type=None,
        link_properties=None,
    ):
        """
        Create a new main table and (optionally) link it to an existing main table.
        :param table_name: The name of the new table to create
        :param column_headings: Column headings for the new table
        :param link_to: Optionally - immediately link the new main table to another, existing main table.
        :param link_type:
        :param link_properties: If the new main table is being linked to another table, then the link should have these
                                properties (columns in the link table)
        :return:
        """
        self.driver.direct_create_new_main_table(table_name=table_name, column_headings=column_headings)

        # Link the new main table to an existing main table - if requested
        if link_to is not None:
            self.driver.direct_link_main_tables(
                primary_table=link_to,
                secondary_table=table_name,
                link_type=link_type,
                requested_cols=link_properties,
            )

    def link_main_tables(self, primary_table, secondary_table, link_type, link_properties=None):
        """
        Create a link between two existing main tables.
        This method functions by creating an interlink table joining the two objects.
        :param primary_table: This table will be linked to ...
        :param secondary_table: ... that table.
        :param link_type: Type of link to form (e.g. "one_one", "one_many", "many_one" or "many_many")
        :param link_properties: Columns to add to the link table. Used to specify properties of the link (e.g. "type",
                                "priority" e.t.c)
        :return:
        """
        self.driver.direct_link_main_tables(
            primary_table=primary_table,
            secondary_table=secondary_table,
            link_type=link_type,
            requested_cols=link_properties,
        )
