"""
API for the database and related classes.

The structure of a functional database goes like this, from top to bottom

DatabaseAPI
    You talk to this from outside this module. Only this.
DatabaseDriverWrapperAPI
    Wraps the driver to add some additional functionality.
    Call here for a RowAPI respecting class.
DatabaseDriverAPI
    Responsible for actually talking to the database - per database type.
    Call here for the row as a dictionary.

MacrosAPI
    Macros inherit from a base macros class which implements functions on the database.
    The macros class for a particular type of



"""

from __future__ import annotations

import abc
import datetime

from typing import Any, Callable, Dict, List, Optional, Tuple, Union, Iterator, Iterable


class DatabaseBuilderAPI(abc.ABC):
    """
    API for the fundamental database builder class.
    """

    @abc.abstractmethod
    def set_database_version(self) -> None:
        """
        Set the database version.

        :return:
        """


class RowAPI(abc.ABC):
    """
    API for a row off the database.
    """

    def __init__(self, database: DatabaseAPI, row_dict: Optional[dict[str, str]] = None,
                 read_only: bool = False) -> None:
        """
        Represents a single row from the LiuXin database.

        :param database: A LiuXin database object
        :param row_dict: Keyed with the column names and valued with their values.
        :param read_only: If True then the row is loaded in read only mode
        :return:
        """

    @abc.abstractmethod
    def sync(self) -> None:
        """
        Synchronize the row with the database.

        :return:
        """

    @property
    @abc.abstractmethod
    def table(self) -> str:
        """
        Return the name of the table this row is in.

        :return:
        """

    @abc.abstractmethod
    def make_read_only(self) -> None:
        """
        Convert this object to a read only row.

        :return:
        """

    @abc.abstractmethod
    def refresh_db_properties(self) -> None:
        """
        Read the properties for the row off the database.

        :return:
        """

    @property
    @abc.abstractmethod
    def row_dict(self) -> Optional[dict[str, str]]:
        """
        Return the row dict stored in this row.

        :return:
        """
        raise NotImplementedError("You need to define this property.")

    @row_dict.setter
    @abc.abstractmethod
    def row_dict(self, val: Optional[dict[str, str]]) -> None:
        """
        Set the row dict stored in this row.

        :param val:
        :return:
        """
        raise NotImplementedError("You need to define this property.")

    # Todo: Validation for the convert for the individual row entry
    @abc.abstractmethod
    def __setitem__(self, key: str, value: Union[str, int, float, datetime.datetime]) -> None:
        """
        Allows a dictionary like interface to the row.

        :param key:
        :param value:
        :return:
        """

    @abc.abstractmethod
    def __getitem__(self, item: str) -> Union[str, int, float, datetime.datetime]:
        """
        Allows a dictionary like interface to the row.

        :param item:
        :return:
        """

    @abc.abstractmethod
    def update_and_check(self) -> None:
        """
        Updates the metadata stored about the row in the class.

        :return:
        """

    @abc.abstractmethod
    def load_row_from_id(self, row_id: Optional[int] = None, table: Optional[str] = None) -> None:
        """
        If an id is present, load or reload the row_dict from it.

        :param row_id: The id of the row to load - if None, tries to use the id already present
        :param table: The name of the table to load the row from
        :return:
        """

    @abc.abstractmethod
    def load_blank_row(self, table: Optional[str] = None) -> None:
        """
        Load a blank row off the given database - will block if the table or row_dict fields are already full.

        :param table:
        :return:
        """

    @abc.abstractmethod
    def ensure_row_has_id(self) -> None:
        """
        Makes sure that the row_dict has an id in it.

        :return:
        """

    @abc.abstractmethod
    def no_sync(self) -> None:
        """
        Method to replace sync if we're in read only mode.

        :return:
        """

    # -------------------------------
    # - COMPARISON METHODS START HERE

    @abc.abstractmethod
    def __hash__(self) -> int:
        """
        A hash for the row based on the table, id and database - will fail unless all three of these are filled.

        :return:
        """

    @abc.abstractmethod
    def __eq__(self, other: RowAPI) -> bool:
        """
        Uses the hash function to test equality.

        :param other:
        :return:
        """

    # -------------------------------
    # -----------------------------------------------
    #
    # - DICTIONARY EMULATION MAGIC METHODS START HERE
    @abc.abstractmethod
    def keys(self) -> Iterable[str]:
        """
        Returns the keys from the row_dict dictionary.

        :return:
        """

    @abc.abstractmethod
    def __iter__(self) -> Iterator[str]:
        """
        Allows use of the in statement in content of a for loop.

        Iterates over all the column headings in the row.
        If the row has been loaded from the database then all column headings will be set - including if the row is
        black. If the row is being constructed rom the invididual keys, only the keys that have been set will be
        returned.
        :return:
        """

    @abc.abstractmethod
    def __contains__(self, item: str) -> bool:
        """
        Allows use of the in statement - returns true if the item is in the row_dict - false otherwise.

        :param item:
        :return:
        """

    # -----------------------------------------------
    # ------------------------
    #
    # - COPY MAGIC STARTS HERE

    @abc.abstractmethod
    def __deepcopy__(self, memo: dict[Any, Any]) -> "RowAPI":
        """
        Allows for deep copying.

        :param memo:
        :return:
        """

    # ------------------------


class DatabaseDriverAPI(abc.ABC):
    """
    Every database drive must descend from this class.
    """

    def direct_executescript(self, script: str) -> None:
        """
        Execute a script on the database - should be phased out.

        :param script:
        :return:
        """

    def direct_execute(self, script: str) -> None:
        """
        Execute a script on the database - should be phased out.

        :param script:
        :return:
        """

    @property
    @abc.abstractmethod
    def macros(self) -> MacrosAPI:
        """
        Return the macros for the given driver.

        :return:
        """


class DatabaseDriverWrapperAPI(abc.ABC):
    """
    API for driver wrappers.

    Driver wrappers sit between :class:`DatabaseAPI` and :class:`DatabaseDriverAPI`.

    - The driver returns "raw" rows (dicts) and exposes low-level operations.
    - The wrapper adds convenience methods, small bits of policy, and utility helpers.

    This API is intended to reflect the surface area of
    :class:`LiuXin_alpha.databases.database_driver_plugins.driver_wrapper.DriverWrapper`.
    """

    # Row dictionaries used throughout the wrapper layer. Values are intentionally permissive:
    # many columns contain ints/floats/dates/None in practice.
    RowDict = dict[str, Any]

    def __init__(self, db: Optional["DatabaseAPI"] = None, macros: Optional["MacrosAPI"] = None) -> None:
        # These are wired up by Database.refresh_db_metadata() / set_driver() after construction.
        self.db: Optional["DatabaseAPI"] = db

        if macros is not None:
            self.set_macros(macros)

        # Cooperative multiple inheritance: if a mixin also wants (db, macros), let it initialize.
        # If not, ignore TypeError and continue.
        try:
            super().__init__(db, macros)  # type: ignore[misc]
        except TypeError:
            try:
                super().__init__()  # type: ignore[misc]
            except TypeError:
                # If even that fails, let the error propagate in normal operation.
                pass

    @property
    @abc.abstractmethod
    def macros(self) -> MacrosAPI:
        """
        Macros API for the driver.

        :return:
        """

    @abc.abstractmethod
    def set_macros(self, new_macros: MacrosAPI) -> None:
        """
        Set the Macros system for the driver.

        :param new_macros:
        :return:
        """

    # ------------------------------------------------------------------------------------------------------------------
    # - LIFECYCLE / CLEANUP
    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def close(self) -> None:
        """Close any open resources held by the wrapper."""

    @abc.abstractmethod
    def break_cycles(self) -> None:
        """Best-effort shutdown helper to break ref-cycles between wrapper/driver/db."""

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO GET COLUMN NAMES FROM TABLES AND VICE VERSA
    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_column_base(self, table_name: str) -> str:
        """Return the base column name for the given table."""

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO GET BASIC INFORMATION ABOUT THE DATABASE
    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_tables(self, force_refresh: bool = False) -> Iterable[str]:
        """Return the table names for the currently loaded database."""

    @abc.abstractmethod
    def get_column_headings(self, table: str) -> list[str]:
        """Return the column headings for a table."""

    @abc.abstractmethod
    def get_view_column_headings(self, view: str) -> list[str]:
        """Return the column headings for a view."""

    @abc.abstractmethod
    def get_tables_and_columns(self) -> dict[str, list[str]]:
        """Return a mapping of table name -> column headings."""

    @abc.abstractmethod
    def get_highest_id(self, target_table: str) -> Optional[int]:
        """Return the highest id value in the given table (or None if empty)."""

    @property
    @abc.abstractmethod
    def user_version(self) -> Any:
        """Return the database user_version (driver-specific type)."""

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO GET INFORMATION ABOUT SPECIFIC TABLES
    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_record_count(self, target_table: str) -> int:
        """Return the number of records in a given table."""

    @abc.abstractmethod
    def get_id_column(self, table: str) -> str:
        """Return the id column name for the given table."""

    @abc.abstractmethod
    def get_datestamp_column(self, table: str) -> str:
        """Return the datestamp column name for the given table."""

    @abc.abstractmethod
    def check_for_intralink_table(self, table_name: str) -> Union[str, bool]:
        """Return the intralink table name if it exists, else False."""

    @abc.abstractmethod
    def get_interlinked_tables(self, table_name: str) -> set[str]:
        """Return the set of main tables interlinked to the given table."""

    @abc.abstractmethod
    def get_link_table_name(self, table1: str, table2: str) -> Union[str, bool]:
        """Return the link table name for the given tables, or False if no link table exists."""

    @abc.abstractmethod
    def get_interlink_column(self, table1: str, table2: str, column_type: str) -> str:
        """Alias for get_link_column for clarity."""

    @abc.abstractmethod
    def get_link_column(self, table1: str, table2: str, column_type: str) -> str:
        """Return a column name in the link table connecting the two tables."""

    @abc.abstractmethod
    def get_intralink_column(self, table: str, column_type: str) -> str:
        """Return a column name in the intralink table for the given table."""

    @abc.abstractmethod
    def get_scratch_column(self, table: str) -> str:
        """Return the scratch column for the given table."""

    @abc.abstractmethod
    def get_parent_column(self, table_name: str) -> Union[str, bool]:
        """Return the parent column for the table if it exists, else False."""

    @abc.abstractmethod
    def get_display_column(self, table_name: str) -> str:
        """Return a suitable 'display' column for the given table."""

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO READ AND WRITE METADATA TO THE DATABASE
    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def read_metadata(self, field: str) -> Any:
        """Read a metadata field from the database metadata table."""

    @abc.abstractmethod
    def write_metadata(self, field: str, value: Any) -> Any:
        """Write a metadata field to the database metadata table."""

    @abc.abstractmethod
    def get_uuid(self) -> Any:
        """Return the unique identifier for the database."""

    @abc.abstractmethod
    def set_uuid(self, new_force_value: Any = None) -> Any:
        """Set (or generate) the unique identifier for the database."""

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO ADD TO THE DATABASE
    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def add_row(self, row_dict: RowDict) -> None:
        """Insert a single row dictionary into the database."""

    @abc.abstractmethod
    def add_multiple_rows(self, row_dict_list: Iterable[RowDict]) -> None:
        """Insert many row dictionaries into the database."""

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO UPDATE THE ROW/DATABASE
    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def update_row(self, row_dict: RowDict) -> Any:
        """Update a row dictionary into the database."""

    @abc.abstractmethod
    def ensure_row_has_id(self, row_dict: RowDict) -> RowDict:
        """Ensure a row dict has an id value (allocating one if necessary)."""

    @abc.abstractmethod
    def update_column(self, table: str, row_id: Any, column: str, new_value: Any) -> bool:
        """Update a single column on a single row."""

    @abc.abstractmethod
    def update_columns(self, values_map: dict[Any, Any], field: Optional[str] = None,
                       table: Optional[str] = None) -> Any:
        """Bulk update convenience wrapper around the driver."""

    @abc.abstractmethod
    def complete_row(self, partial_row: RowDict) -> RowDict:
        """Merge a partial row with the full row from the database (existing keys win)."""

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO DELETE FROM THE DATABASE
    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def delete(self, target_table: str, column: str, value: Any) -> Any:
        """Delete rows where column == value (or many values)."""

    @abc.abstractmethod
    def delete_by_id(self, target_table: str, row_id: Any) -> Any:
        """Delete rows by id (or many ids)."""

    @abc.abstractmethod
    def nullify_column(self, table: str, row_id: Any, column: str) -> bool:
        """Set a column to NULL/None for a given row."""

    @abc.abstractmethod
    def clear(self, target_table: str) -> Any:
        """Clear all rows from a table."""

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO SEARCH THE DATABASE
    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_row_from_id(self, table: str, row_id: Any) -> Union[RowDict, bool]:
        """Return a single row dict for a given id (or False if not found)."""

    @abc.abstractmethod
    def get_view_row_from_id(self, view: str, row_id: Any) -> Any:
        """Return a row dict from a view."""

    @abc.abstractmethod
    def get_all_rows(self, table: str, sort_column: Optional[str] = None, reverse: bool = False) -> Iterable[RowDict]:
        """Return all row dicts for a table (optionally sorted)."""

    @abc.abstractmethod
    def search(self, table: str, column: str, search_term: Any) -> Iterable[RowDict]:
        """Search a column in a table and return matching row dicts."""

    @abc.abstractmethod
    def get_blank_row(self, table: str) -> RowDict:
        """Create and return a blank row dict in the given table."""

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO GET INFORMATION FROM ROW DICTS
    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def identify_table_from_row_dict(self, row_dict: RowDict) -> Union[str, bool]:
        """Infer the table name from a row dict."""

    @abc.abstractmethod
    def get_id_from_row(self, row_dict: RowDict) -> Any:
        """Extract the id value from a row dict if present."""

    @abc.abstractmethod
    def identify_table_from_column(self, column_heading: str, error: bool = True) -> Optional[str]:
        """Infer the table name from a column heading."""

    # ------------------------------------------------------------------------------------------------------------------
    # - TREE STRUCTURE HELPERS
    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_linear_row_list(self, start_row: RowDict) -> list[RowDict]:
        """Return the lineage of rows up the tree to the root (root-first)."""

    @abc.abstractmethod
    def set_tree_ids(self, table: str) -> Any:
        """Ensure tree ids are populated for the given table."""

    @abc.abstractmethod
    def set_full_column(self, table: str) -> Any:
        """Populate the _full column for rows in a tree-structured table."""

    @abc.abstractmethod
    def walk(self, start_row: RowDict) -> Iterator[RowDict]:
        """Yield all rows in a tree, starting from start_row (BFS)."""

    @abc.abstractmethod
    def _walk(self, start_row: RowDict, table: str, table_id_col: str, table_parent_col: str) -> Iterator[RowDict]:
        """Internal implementation for walk()."""

    # ------------------------------------------------------------------------------------------------------------------
    # - TRIGGERS
    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_triggers(self) -> Any:
        """Return triggers currently defined on the database."""

    @abc.abstractmethod
    def drop_triggers(self, triggers: Any) -> Any:
        """Drop the named triggers."""

    @abc.abstractmethod
    def drop_all_triggers(self) -> Any:
        """Drop all triggers on the database."""

    # ------------------------------------------------------------------------------------------------------------------
    # - SPECIAL METHODS
    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_all_hashes(self) -> Any:
        """Return all hashes stored in the database."""

    @abc.abstractmethod
    def shell(self) -> Any:
        """Open an interactive shell for the underlying database, if supported."""

    @abc.abstractmethod
    def get_connection(self) -> Any:
        """Return a connection handle suitable for use as a lock."""

    @abc.abstractmethod
    def get_random_row(self, table: str, row_dict: Any = None, direct_access: bool = False) -> RowDict:
        """Return a random row dict from a table."""

    # ------------------------------------------------------------------------------------------------------------------
    # - DIRECT EXECUTION SQL METHODS (discouraged)
    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def execute(self, sql: str, values: Any = None) -> Any:
        """Execute SQL directly on the database."""

    @abc.abstractmethod
    def executemany(self, sql: str, values: Any = None) -> Any:
        """Execute many SQL statements directly on the database."""

    @abc.abstractmethod
    def executescript(self, sqlscript: str) -> Any:
        """Execute a script directly on the database."""

    @abc.abstractmethod
    def get(self, *args: Any, **kw: Any) -> Any:
        """Convenience wrapper around execute() + fetch semantics."""

    # ------------------------------------------------------------------------------------------------------------------
    # - DIRTY RECORD QUEUE SUPPORT
    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_dirtied_count(self) -> int:
        """Return the approximate number of dirtied records queued."""

    @abc.abstractmethod
    def dirty_record(self, table: str, row_id: Any, reason: str) -> None:
        """Add a record to the dirtied queue (best-effort)."""

    # ------------------------------------------------------------------------------------------------------------------
    # - CREATE/LINK TABLES
    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def create_new_main_table(
            self,
            table_name: str,
            column_headings: Optional[Iterable[str]] = None,
            link_to: Optional[str] = None,
            link_type: Optional[str] = None,
            link_properties: Optional[Iterable[str]] = None,
    ) -> None:
        """Create a new main table and (optionally) link it to an existing main table."""

    @abc.abstractmethod
    def link_main_tables(
            self,
            primary_table: str,
            secondary_table: str,
            link_type: str,
            link_properties: Optional[Iterable[str]] = None,
    ) -> None:
        """Create a link (interlink table) between two existing main tables."""


class DatabaseAPI(abc.ABC):
    """
    API for the Database itself.
    """

    @abc.abstractmethod
    def set_driver(self, new_driver: DatabaseDriverAPI) -> None:
        """
        Set the database driver.

        :param new_driver:
        :return:
        """

    @abc.abstractmethod
    def set_driver_wrapper(self, new_driver_wrapper: DatabaseDriverWrapperAPI) -> None:
        """
        Set the database driver wrapper.

        :param new_driver_wrapper:
        :return:
        """

    @property
    @abc.abstractmethod
    def driver(self) -> DatabaseDriverAPI:
        """
        Get the driver instance.

        :return:
        """

    @abc.abstractmethod
    def get_all_rows(self, table: str) -> Iterable[RowAPI]:
        """
        Return all the rows for the given table.

        :param table:
        :return:
        """

    @property
    @abc.abstractmethod
    def main_tables(self) -> frozenset[str]:
        """
        Return the main tables defined in this database.

        :return:
        """

    @property
    @abc.abstractmethod
    def interlink_tables(self) -> frozenset[str]:
        """
        Return the interlink tables defined by the database.

        :return:
        """

    @property
    @abc.abstractmethod
    def driver_wrapper(self) -> DatabaseDriverWrapperAPI:
        """
        Return the driver wrapper.

        :return:
        """

    @abc.abstractmethod
    def get_row_from_id(self, table: str, row_id: int) -> RowAPI:
        """
        Get the given row from the database.

        :param table:
        :param row_id:
        :return:
        """

    @abc.abstractmethod
    def delete(self, row: RowAPI) -> None:
        """
        Delete a row from the database.

        :param row:
        :return:
        """

    @abc.abstractmethod
    def get_intralink_rows(self, row: RowAPI, primary: bool = True, secondary: bool = False) -> list[RowAPI]:
        """
        Return the intralink rows linked to the given row.

        :param row:
        :param primary:
        :param secondary:
        :return:
        """


class DatabaseCacheAPI(abc.ABC):
    """
    Every local cache containing data from the database must descend from this class.
    """


class DatabaseMaintainerAPI(abc.ABC):
    """
    Maintenance bot which runs on the database.
    """

    def __init__(self, db: DatabaseAPI) -> None:
        """
        Attach the database to the maintainer which will work on it.

        :param db:
        """
        # Weakref to make sure the class doesn't block shutdown of the database
        self.db = db

    @abc.abstractmethod
    def dirty_record(self, table: str, row_id: int) -> None:
        """
        Notify the maintenance bot that a change has occurred to the table (put it in the maintain queue).

        :param table:
        :param row_id:
        :return:
        """

    @abc.abstractmethod
    def new_dirty_record(self, table: str, row_id: int) -> None:
        """
        Replacement for the dirty record method for testing.

        :param table:
        :param row_id:
        :return:
        """

    @abc.abstractmethod
    def dirty_interlink_record(
            self, update_type: str, table1: str, table2: str, table1_id: int, table2_id: int
    ) -> None:
        """
        Notify the maintenance bot that an interlink record has been changed.

        Used for updating the books_aggregate table when stuff happens to the relevant other tables.
        :param update_type:
        :param table1:
        :param table2:
        :param table1_id:
        :param table2_id:
        :return:
        """

    @abc.abstractmethod
    def clean(self, table: str, item_ids: Iterable[int]) -> None:
        """
        Clean the relevant table of the relevant item_ids

        :param table:
        :param item_ids:
        :return:
        """

    @abc.abstractmethod
    def merge(self, table: str, item_1_id: int, item_2_id: int) -> None:
        """
        Consider merging two items on the database.

        :param table:
        :param item_1_id:
        :param item_2_id: All the item 2 ids will be repointed to item_1_id - then it'll be deleted
        :return:
        """


class MaintenanceBotAPI(abc.ABC):
    """
    API for the maintenance bot thread itself.
    """

    @abc.abstractmethod
    def stop(self) -> None:
        """
        Preform thread shutdown.

        :return:
        """

    @abc.abstractmethod
    def rename_item(
            self,
            item_id: int,
            table: str,
            value: bool,
            now: bool = True,
            db: Optional[DatabaseAPI] = None) -> None:
        """
        Register a rename action has occurred on an item.

        :param item_id:
        :param table:
        :param value: The item value will be renamed to this
        :param now:
        :param db:
        :return:
        """


class MacrosAPI(abc.ABC):
    """
    Macros are chained statements, even as a single piece of code or a function.
    """