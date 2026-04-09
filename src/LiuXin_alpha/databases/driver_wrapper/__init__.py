
"""
The driver wrapper provides some utility methods around the driver to improve convenience.
"""

from __future__ import unicode_literals, annotations

from typing import Optional, TYPE_CHECKING

from copy import deepcopy

from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_custom_columns_mixin import CustomColumnsDriverWrapperMixin
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import InputIntegrityError, DatabaseIntegrityError, LogicalError
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.python_tools import smart_dictionary_merge, get_unique_id
from LiuXin_alpha.databases.schema_specs import (
    StorageTableSpec,
    StorageColumnSpec,
    RelationKind,
    StorageLinkSpec)
from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_names_mixin import DriverWrapperNamesMixin
from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_add_mixin import DriverWrapperAddMixin
from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_update_mixin import DriverWrapperUpdateMixin
from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_delete_mixin import DriverWrapperDeleteMixin
from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_view_mixin import DriverWrapperViewMixin
from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_tree_mixin import DriverWrapperTreeMixin
from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_metadata_mixin import DriverWrapperMetadataMixin
from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_search_mixin import DriverWrapperSearchMixin
from LiuXin_alpha.databases.api.database_api.driver_wrapper import DatabaseDriverWrapperAPI

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import MacrosAPI, DatabaseAPI, DatabaseDriverAPI


class DriverWrapper(
    CustomColumnsDriverWrapperMixin,
    DriverWrapperAddMixin,
    DriverWrapperUpdateMixin,
    DriverWrapperDeleteMixin,
    DriverWrapperNamesMixin,
    DriverWrapperViewMixin,
    DriverWrapperTreeMixin,
    DriverWrapperSearchMixin,
    DriverWrapperMetadataMixin,
    DatabaseDriverWrapperAPI):
    """
    Everything coming out of this class should be a row_dict.
    """

    _macros: "MacrosAPI"

    def __init__(
            self,
            driver: "DatabaseDriverAPI",
            db: Optional["DatabaseAPI"] = None) -> None:
        """
        Initialize with the database driver for direct access.

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

    def get_table_spec(self, table: str, force_refresh: bool = False) -> StorageTableSpec:
        if force_refresh:
            self._clear_derived_schema_caches()

        relation_type = self.get_relation_type(table)
        if relation_type is None:
            raise ValueError(f"No such relation: {table!r}")

        columns: list[StorageColumnSpec] = []
        headings = (
            self.get_view_column_headings(table)
            if relation_type == "view"
            else self.get_column_headings(table)
        )

        declared_types = {}
        if hasattr(self.driver, "_get_declared_types_for_table") and relation_type == "table":
            try:
                declared_types = self.driver._get_declared_types_for_table(table)
            except Exception:
                declared_types = {}

        for ordinal, col in enumerate(headings):
            declared = declared_types.get(col)
            affinity = None
            if declared and hasattr(self.driver, "_sqlite_affinity"):
                try:
                    affinity = self.driver._sqlite_affinity(declared)
                except Exception:
                    affinity = None

            columns.append(
                StorageColumnSpec(
                    name=col,
                    ordinal=ordinal,
                    declared_type=declared,
                    affinity=affinity,
                    nullable=True,  # tighten later with PRAGMA table_info
                )
            )

        return StorageTableSpec(
            name=table,
            relation_kind=RelationKind(relation_type),
            columns=tuple(columns),
            id_column=self.get_id_column(table) if relation_type == "table" else None,
            parent_column=self.get_parent_column(table) if relation_type == "table" else None,
            datestamp_column=self.get_datestamp_column(table) if relation_type == "table" else None,
            scratch_column=self.get_scratch_column(table) if relation_type == "table" else None,
            is_main_table=table in getattr(self, "main_tables", ()),
            is_link_table=table in getattr(self, "interlink_tables", ()),
            is_intralink_table=table in getattr(self, "intralink_tables", ()),
            linked_tables=tuple(sorted(self.get_interlinked_tables(table))) if relation_type == "table" else (),
        )

    def get_link_spec(self, table1: str, table2: str, *, force_refresh: bool = False) -> Optional[StorageLinkSpec]:
        if force_refresh:
            self._clear_derived_schema_caches()

        link_table = self.get_link_table_name(table1, table2)
        if not link_table:
            return None

        primary_link_col = self.get_link_column(table1, table2, self.get_id_column(table1))
        secondary_link_col = self.get_link_column(table1, table2, self.get_id_column(table2))

        try:
            priority_link_col = self.get_link_column(table1, table2, "priority")
        except Exception:
            priority_link_col = None

        try:
            type_link_col = self.get_link_column(table1, table2, "type")
        except Exception:
            type_link_col = None

        link_columns = set(self.get_column_headings(link_table))
        used = {primary_link_col, secondary_link_col}
        if priority_link_col:
            used.add(priority_link_col)
        if type_link_col:
            used.add(type_link_col)

        extra_specs = tuple(
            col for col in self.get_table_spec(link_table).columns
            if col.name not in used
        )

        allowed_types_table = None
        for cand in (f"{link_table}__types", f"allowed_types__{link_table}"):
            if cand in set(self.get_tables(force_refresh=False)):
                allowed_types_table = cand
                break

        return StorageLinkSpec(
            primary_table=table1,
            secondary_table=table2,
            link_table=link_table,
            primary_id_col=self.get_id_column(table1),
            secondary_id_col=self.get_id_column(table2),
            primary_link_col=primary_link_col,
            secondary_link_col=secondary_link_col,
            priority_link_col=priority_link_col,
            type_link_col=type_link_col,
            ordered=priority_link_col is not None,
            typed=type_link_col is not None,
            allowed_types_table=allowed_types_table,
            extra_link_columns=extra_specs,
        )

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def set_macros(self, new_macros: "MacrosAPI") -> None:
        """
        Set the macros class for the individual driver.

        :param new_macros:
        :return:
        """
        assert new_macros is not None
        self._macros = new_macros

    @property
    def macros(self) -> "MacrosAPI":
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
    # - METHODS TO GET INFORMATION ABOUT SPECIFIC TABLES START HERE
    # ------------------------------------------------------------------------------------------------------------------

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


    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO UPDATE THE ROW/DATABASE START HERE
    # ------------------------------------------------------------------------------------------------------------------

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
