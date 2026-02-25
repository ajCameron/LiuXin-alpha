
"""
The driver wrapper provides some utility methods around the driver to improve convenience.
"""
from __future__ import unicode_literals

from typing import Optional

from copy import deepcopy

from LiuXin_alpha.databases.custom_columns import CustomColumnsDriverWrapperMixin
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import InputIntegrityError, DatabaseIntegrityError, LogicalError
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.python_tools import smart_dictionary_merge, get_unique_id
from LiuXin_alpha.databases.api import DatabaseDriverWrapperAPI, MacrosAPI


class DriverWrapper(CustomColumnsDriverWrapperMixin, DatabaseDriverWrapperAPI):
    """
    Everything coming out of this class should be a row_dict.
    """

    _macros: MacrosAPI

    def __init__(self, driver, db = None):
        """
        Initialize with the database driver - this will be used to access the

        :param driver:
        :return:
        """
        self.driver = driver
        self.set_macros(driver.macros)

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

        super(DriverWrapper, self).__init__(db=db, macros=None)

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def set_macros(self, new_macros: MacrosAPI) -> None:
        """
        Set the macros class for the individual driver.

        :param new_macros:
        :return:
        """
        assert new_macros is not None
        self._macros = new_macros

    @property
    def macros(self) -> MacrosAPI:
        """
        Return the current macros object for the DriverWrappoer.

        :return:
        """
        return self._macros

    def close(self) -> None:
        """
        Close any open resources.

        In particular, close the SQLite connection created for locking.

        :return:
        """
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
        self.break_cycles()

    def break_cycles(self):
        """
        Preform shutdown in a sensible order - deleting each of the objects in the right order.
        :return:
        """
        try:
            self.lock = None
        except Exception:
            pass
        try:
            self.driver = None
        except Exception:
            pass

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
    def get_tables(self, force_refresh: bool = False):
        """
        Directly get the tables for the currently loaded database

        :return:
        """
        return self.driver.direct_get_tables(force_refresh=force_refresh)

    def get_relation_type(self, name: str) -> Optional[str]:
        """Return the relation type for `name` (e.g. 'table' or 'view') if known.

        SQLite-backed schemas increasingly use *views* as compatibility surfaces.
        Attempting to create rows in a view fails at the database level; this helper
        lets higher-level methods provide a clearer error earlier.
        """
        name = str(name)
        try:
            cur = self.execute(
                "SELECT type FROM sqlite_master WHERE name = ? COLLATE NOCASE LIMIT 1;",
                (name,),
            )
            for row in cur:
                if row and row[0] is not None:
                    return str(row[0]).strip().lower()
        except Exception:
            return None
        return None

    def is_view(self, name: str) -> bool:
        """Return True iff `name` exists and is a SQLite view."""
        return self.get_relation_type(name) == "view"

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
        # Returns the SQLite rowid / INTEGER PRIMARY KEY value if available.
        return self.driver.direct_add_simple_row_dict(row_dict)

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

        # Clearer error for schemas that expose compatibility surfaces as *views*.
        # Views are read-only in SQLite unless backed by INSTEAD OF triggers.
        rel_type = self.get_relation_type(table)
        if rel_type == "view":
            err_str = "get_blank_row cannot create a writable row for '{}' because it is a view (read-only).\n".format(table)
            err_str += "Pick an underlying base table instead (or add INSTEAD OF triggers if you truly want writable views).\n"
            raise InputIntegrityError(err_str)

        # using this as a key to find the row after it has been added to the table
        new_row_id = get_unique_id()

        table_scratch_column = self.get_scratch_column(table)

        # Special-case: `books.book_id` is also a FOREIGN KEY to `titles.title_id`.
        # Creating a blank `books` row therefore requires a matching `titles` row first.
        if table == "books":
            title_row = self.get_blank_row("titles")
            title_id_col = self.get_id_column("titles")
            book_id_col = self.get_id_column("books")

            new_row = {book_id_col: title_row[title_id_col], table_scratch_column: new_row_id}
            self.add_row(new_row)

            rows = self.search(table, table_scratch_column, new_row_id)

            if len(rows) == 0:
                err_str = "Error - get_blank_row failed to create new blank row. Aborting.\n"
                raise DatabaseIntegrityError(err_str)
            elif len(rows) > 1:
                err_str = "Error - get_blank_row found multiple rows with the same UUID.\n"
                err_str += repr(rows)
                raise DatabaseIntegrityError(err_str)

            row = rows[0]

            # blanking the table scratch column. Should be applied if the row is synced back into the database.
            row[table_scratch_column] = ""
            return row

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

        IMPORTANT:
        sqlite3/apsw execute()/executemany() only accept a single statement.
        In older Calibre-derived code paths, executemany(sql) was sometimes used as
        a "run this multi-statement DDL block" helper, with values left as None.
        When values is None, route to executescript() instead.
        :param sql:
        :param values:
        :return:
        """
        try:
            if values is None:
                # Multi-statement scripts must go through executescript.
                return self.driver.direct_executescript(sql)
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
            return next(ans)
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
