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
        """
        Close any open resources held by the wrapper.
        """

    @abc.abstractmethod
    def break_cycles(self) -> None:
        """
        Best-effort shutdown helper to break ref-cycles between wrapper/driver/db.
        """

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO GET COLUMN NAMES FROM TABLES AND VICE VERSA
    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_column_base(self, table_name: str) -> str:
        """
        Return the base column name for the given table.

        :param table_name: From the given table name.
        :return:
        """

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO GET BASIC INFORMATION ABOUT THE DATABASE
    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_tables(self, force_refresh: bool = False) -> Iterable[str]:
        """
        Return the table names for the currently loaded database.

        :param force_refresh: bool - use the cache version - or not
        :return:
        """

    @abc.abstractmethod
    def get_column_headings(self, table: str) -> list[str]:
        """
        Return the column headings for a table.

        :param table:
        :return:
        """

    @abc.abstractmethod
    def get_view_column_headings(self, view: str) -> list[str]:
        """
        Return the column headings for a view.

        :param view:
        :return:
        """

    @abc.abstractmethod
    def get_tables_and_columns(self) -> dict[str, list[str]]:
        """
        Return a mapping of table name -> column headings.

        :return:
        """

    @abc.abstractmethod
    def get_highest_id(self, target_table: str) -> Optional[int]:
        """
        Return the highest id value in the given table (or None if empty).

        :param target_table: Name of a table in the database.
        :return:
        """

    @property
    @abc.abstractmethod
    def user_version(self) -> Any:
        """Return the database user_version (driver-specific type)."""

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO GET INFORMATION ABOUT SPECIFIC TABLES
    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_record_count(self, target_table: str) -> int:
        """
        Return the number of records in a given table.

        :param target_table:
        :return:
        """

    @abc.abstractmethod
    def get_id_column(self, table: str) -> str:
        """
        Return the id column name for the given table.

        :param table:
        :return:
        """

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


class DatabaseRatingMixinAPI(abc.ABC):
    """Typed API for ``DatabaseRatingMixin``."""

    @abc.abstractmethod
    def check_rating_table(self) -> None:
        """Ensure canonical rows exist in ``ratings`` and repair malformed entries."""


class DatabaseNullRowsMixinAPI(abc.ABC):
    """Typed API for ``DatabaseNullRowsMixin``."""

    @abc.abstractmethod
    def ensure_null_rows(self) -> None:
        """Ensure required sentinel/null rows exist for schema-specific tables."""


class DatabaseMetadataMixinAPI(abc.ABC):
    """Typed API for ``DatabaseMetadataMixin``."""

    @property
    @abc.abstractmethod
    def uuid(self) -> str:
        ...

    @uuid.setter
    @abc.abstractmethod
    def uuid(self, value: str) -> None:
        ...

    @property
    @abc.abstractmethod
    def library_id(self) -> str:
        ...

    @library_id.setter
    @abc.abstractmethod
    def library_id(self, value: str) -> None:
        ...

    @property
    @abc.abstractmethod
    def database_version(self) -> str:
        ...

    @database_version.setter
    @abc.abstractmethod
    def database_version(self, value: str) -> None:
        ...

    @abc.abstractmethod
    def get_tables(self, force_refresh: bool = False) -> Iterable[str]:
        ...

    @abc.abstractmethod
    def get_column_headings(self, table: str) -> list[str]:
        ...

    @abc.abstractmethod
    def get_view_column_headings(self, view: str) -> list[str]:
        ...

    @abc.abstractmethod
    def get_tables_and_columns(self) -> dict[str, list[str]]:
        ...

    @abc.abstractmethod
    def get_record_count(self, target_table: str) -> int:
        ...

    @abc.abstractmethod
    def get_max(self, column: str) -> Any:
        ...

    @abc.abstractmethod
    def get_min(self, column: str) -> Any:
        ...

    @abc.abstractmethod
    def row_counts(self) -> str:
        ...


class DatabaseDirtiedRecordsMixinAPI(abc.ABC):
    """Typed API for ``DatabaseDirtiedRecordsMixin``."""

    @property
    @abc.abstractmethod
    def metadata_dirtied_table(self) -> str:
        ...

    @abc.abstractmethod
    def get_dirtied_count(self, *, include_persisted: bool = False) -> int:
        ...

    @abc.abstractmethod
    def dirty_record(self, table: str, row_id: int, reason: str = "") -> None:
        ...

    @abc.abstractmethod
    def get_persisted_dirtied_count(self) -> int:
        ...

    @abc.abstractmethod
    def persist_dirtied_records(self, *, limit: Optional[int] = None) -> int:
        ...


class DatabaseSearchMixinAPI(abc.ABC):
    """Typed API for ``DatabaseSearchMixin``."""

    @abc.abstractmethod
    def search(self, table: str, column: str, search_term: Any) -> list["RowAPI"]:
        ...

    @abc.abstractmethod
    def multi_column_search(self, search_index: Any, iterator_return: bool = False) -> Any:
        ...

    @abc.abstractmethod
    def get_unique(self, target_column: str) -> Any:
        ...

    @abc.abstractmethod
    def get_values_set(self, target_column: str, iterator_return: bool = False) -> Any:
        ...

    @abc.abstractmethod
    def get_row_from_id(self, table: str, row_id: int) -> Optional["RowAPI"]:
        ...

    @abc.abstractmethod
    def get_random_row(self, table: str) -> "RowAPI":
        ...

    @abc.abstractmethod
    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = True,
        sort_column: Optional[str] = None,
        reverse: bool = False,
    ) -> Union[list["RowAPI"], Iterator["RowAPI"]]:
        ...

    @abc.abstractmethod
    def chunk_iterator(self, column: str, target_table: Optional[str] = None) -> Iterator[list["RowAPI"]]:
        ...


class DatabaseInterlinkRowsMixinAPI(abc.ABC):
    """Typed API for ``DatabaseInterlinkRowsMixin``."""

    @abc.abstractmethod
    def get_interlink_row(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        onelink: bool = True,
    ) -> Optional[Union["RowAPI", list["RowAPI"]]]:
        ...

    @abc.abstractmethod
    def get_interlink_rows(self, primary_row: "RowAPI", secondary_table: str) -> list["RowAPI"]:
        ...

    @abc.abstractmethod
    def get_interlinked_rows(
        self,
        target_row: Optional["RowAPI"] = None,
        secondary_table: Optional[str] = None,
        type_filter: Optional[str] = None,
        **kwargs: Any,
    ) -> list["RowAPI"]:
        ...

    @abc.abstractmethod
    def get_interlink_values(self, target_row: "RowAPI", secondary_column: str) -> set[Any]:
        ...

    @abc.abstractmethod
    def interlink_rows(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        priority: Optional[Union[int, float, str]] = "highest",
        type: Optional[str] = None,
        **col_value_pairs: Any,
    ) -> "RowAPI":
        ...

    @abc.abstractmethod
    def dupe_interlinks(
        self,
        src_row: "RowAPI",
        dst_row: "RowAPI",
        swap_priorities: bool = False,
        restrict_to_tables: Optional[Iterable[str]] = None,
        force_priority: Optional[Union[int, float, str]] = None,
    ) -> None:
        ...

    @abc.abstractmethod
    def swap_priorities(self, src_row: "RowAPI", dst_row_1: "RowAPI", dst_row_2: "RowAPI") -> None:
        ...

    @abc.abstractmethod
    def update_interlink(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        priority: Optional[Union[int, float, str]] = "unchanged",
        **col_value_pairs: Any,
    ) -> "RowAPI":
        ...

    @abc.abstractmethod
    def update_interlink_priority(self, primary_row: "RowAPI", secondary_table: str, ordered_ids: Iterable[int]) -> None:
        ...

    @abc.abstractmethod
    def unlink_interlink(self, primary_row: "RowAPI", secondary_row: "RowAPI") -> None:
        ...

    @abc.abstractmethod
    def unlink_all(self, primary_row: "RowAPI", secondary_table: str, type_filter: Optional[str] = None) -> None:
        ...


class DatabaseIntralinkRowsMixinAPI(abc.ABC):
    """Typed API for ``DatabaseIntralinkRowsMixin``."""

    @abc.abstractmethod
    def intralink_rows(self, primary_row: "RowAPI", secondary_row: "RowAPI", link_type: str) -> "RowAPI":
        ...

    @abc.abstractmethod
    def get_intralink_row(self, primary_row: "RowAPI", secondary_row: "RowAPI") -> Optional["RowAPI"]:
        ...

    @abc.abstractmethod
    def get_intralink_rows(
        self,
        row: "RowAPI",
        primary: bool = True,
        secondary: bool = True,
        link_type_filter: Optional[str] = None,
    ) -> list["RowAPI"]:
        ...

    @abc.abstractmethod
    def get_intralinked_rows(
        self,
        primary_row: Optional["RowAPI"],
        secondary_row: Optional["RowAPI"],
    ) -> list["RowAPI"]:
        ...

    @abc.abstractmethod
    def unlinked_intralink(self, primary_row: Optional["RowAPI"], secondary_row: Optional["RowAPI"]) -> None:
        ...


class DatabaseTreeMixinAPI(abc.ABC):
    """Typed API for ``DatabaseTreeMixin``."""

    @abc.abstractmethod
    def get_root_row(self, start_row: "RowAPI") -> "RowAPI":
        ...

    @abc.abstractmethod
    def get_root_series(self, start_row: "RowAPI") -> "RowAPI":
        ...

    @abc.abstractmethod
    def get_children(self, src_row: "RowAPI") -> list["RowAPI"]:
        ...

    @abc.abstractmethod
    def get_linear_row_list(self, start_row: "RowAPI") -> list["RowAPI"]:
        ...

    @abc.abstractmethod
    def get_all_tree_rows(self, start_row: "RowAPI", back_iterate: bool = True) -> set["RowAPI"]:
        ...

    @abc.abstractmethod
    def walk(self, start_row: "RowAPI") -> Iterator["RowAPI"]:
        ...

    @abc.abstractmethod
    def search_tree(self, root_row: "RowAPI", for_ids: Iterable[int]) -> set[int]:
        ...

    @abc.abstractmethod
    def nest_rows(self, parent_row: "RowAPI", child_rows: Union["RowAPI", Iterable["RowAPI"]]) -> None:
        ...

    @abc.abstractmethod
    def delete_tree(self, parent_row: "RowAPI") -> None:
        ...


class DatabaseTriggerHelpersAPI(abc.ABC):
    """Typed API for trigger helper passthroughs exposed by ``Database``."""

    @abc.abstractmethod
    def get_triggers(self) -> Any:
        ...

    @abc.abstractmethod
    def drop_triggers(self, triggers: Any) -> Any:
        ...

    @abc.abstractmethod
    def drop_all_triggers(self) -> Any:
        ...


class DatabaseAPI(
    DatabaseRatingMixinAPI,
    DatabaseNullRowsMixinAPI,
    DatabaseMetadataMixinAPI,
    DatabaseDirtiedRecordsMixinAPI,
    DatabaseSearchMixinAPI,
    DatabaseInterlinkRowsMixinAPI,
    DatabaseIntralinkRowsMixinAPI,
    DatabaseTreeMixinAPI,
    DatabaseTriggerHelpersAPI,
    abc.ABC,
):
    """
    API for the live Database class.

    The concrete implementation is :class:`LiuXin_alpha.databases.database.Database`, which is a composite built from
    multiple mixins (search, links, tree navigation, dirtied-record tracking, etc.).

    This API aims to describe the *public* surface area of that composite so that:
    - callers can type against a stable interface, and
    - tests can provide lightweight fakes/mocks that satisfy the same contract.
    """

    # ---------------------------------------------------------------------------------------------
    # Driver wiring / core handles
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def set_driver(self, new_driver: "DatabaseDriverAPI") -> None:
        """Attach a new low-level driver instance."""

    @abc.abstractmethod
    def set_driver_wrapper(self, new_driver_wrapper: "DatabaseDriverWrapperAPI") -> None:
        """Attach a new driver wrapper instance."""

    @abc.abstractmethod
    def set_macros(self, new_macros: "MacrosAPI") -> None:
        """Attach a new macros instance."""

    @property
    @abc.abstractmethod
    def driver(self) -> "DatabaseDriverAPI":
        """Return the low-level driver instance."""

    @property
    @abc.abstractmethod
    def driver_wrapper(self) -> "DatabaseDriverWrapperAPI":
        """Return the driver wrapper instance."""

    @property
    @abc.abstractmethod
    def macros(self) -> "MacrosAPI":
        """Return the macros instance."""

    # These are commonly present on the concrete Database, but are not required for all fakes.
    # They are therefore declared as attributes rather than abstract properties.
    lock: Any
    metadata: Optional[dict[str, Any]]
    type: Optional[str]

    # ---------------------------------------------------------------------------------------------
    # Lifecycle / housekeeping
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def close(self) -> None:
        """Close open resources (driver connections, wrapper connections, etc.)."""

    @abc.abstractmethod
    def refresh_db_metadata(self) -> None:
        """Refresh cached schema metadata (table categories, uuid cache, etc.)."""

    @abc.abstractmethod
    def check_exists(self) -> bool:
        """Return True if the underlying database exists and is accessible."""

    @abc.abstractmethod
    def backup(self) -> None:
        """Create a backup of the database (delegates to the driver)."""

    @abc.abstractmethod
    def lock_writing(self) -> None:
        """Switch the database to write against a scratch copy (driver-dependent)."""

    @abc.abstractmethod
    def create_new_database(self, blank: bool = True, backup: bool = True) -> None:
        """Create (or recreate) the database schema."""

    @abc.abstractmethod
    def break_cycles(self) -> None:
        """Best-effort cycle breaking to aid deterministic GC during teardown."""

    # Context manager helpers (implemented by the concrete Database).
    @abc.abstractmethod
    def __enter__(self) -> "DatabaseAPI":
        ...

    @abc.abstractmethod
    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        ...

    # ---------------------------------------------------------------------------------------------
    # Table categorization / sets
    # ---------------------------------------------------------------------------------------------
    # Note: some of these are maintained as attributes on the concrete Database during refresh_db_metadata().
    all_tables: Optional[set[str]]
    custom_tables: Optional[set[str]]
    intralink_tables: Optional[set[str]]
    dirtiable_tables: Optional[set[str]]
    helper_tables: Optional[set[str]]
    allowed_type_tables: Optional[set[str]]

    @property
    @abc.abstractmethod
    def main_tables(self) -> frozenset[str]:
        """Return the main (entity) tables defined in this database."""

    @property
    @abc.abstractmethod
    def interlink_tables(self) -> frozenset[str]:
        """Return the interlink tables defined in this database."""

    @abc.abstractmethod
    def categorize_table(self, table: str) -> str:
        """Return the category string for a table name (main/interlink/intralink/helper/custom/...)."""

    # ---------------------------------------------------------------------------------------------
    # Database metadata (uuid/library_id/version)
    # ---------------------------------------------------------------------------------------------
    @property
    @abc.abstractmethod
    def uuid(self) -> str:
        """Database UUID (used for cache keys, change detection, etc.)."""

    @uuid.setter
    @abc.abstractmethod
    def uuid(self, value: str) -> None:
        ...

    @property
    @abc.abstractmethod
    def library_id(self) -> str:
        """Library UUID (unique identifier for the library itself)."""

    @library_id.setter
    @abc.abstractmethod
    def library_id(self, value: str) -> None:
        ...

    @property
    @abc.abstractmethod
    def database_version(self) -> str:
        """Schema version string stored in the database."""

    @database_version.setter
    @abc.abstractmethod
    def database_version(self, value: str) -> None:
        ...

    # ---------------------------------------------------------------------------------------------
    # Schema inspection helpers
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_tables(self, force_refresh: bool = False) -> Iterable[str]:
        """Return an iterable of table names for the current database."""

    @abc.abstractmethod
    def get_column_headings(self, table: str) -> list[str]:
        """Return the column names for a table, in database order."""

    @abc.abstractmethod
    def get_view_column_headings(self, view: str) -> list[str]:
        """Return the column names for a view, in database order."""

    @abc.abstractmethod
    def get_tables_and_columns(self) -> dict[str, list[str]]:
        """Return mapping of table name -> list of column headings."""

    @abc.abstractmethod
    def get_record_count(self, target_table: str) -> int:
        """Return the number of rows in a table."""

    @abc.abstractmethod
    def get_max(self, column: str) -> Any:
        """Return the max value found in the given column (fully qualified or driver-specific)."""

    @abc.abstractmethod
    def get_min(self, column: str) -> Any:
        """Return the min value found in the given column (fully qualified or driver-specific)."""

    @abc.abstractmethod
    def row_counts(self) -> str:
        """Return a human readable string of row counts for key table categories."""

    # ---------------------------------------------------------------------------------------------
    # Row factories / direct row operations
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_blank_row(self, table: Optional[str] = None) -> "RowAPI":
        """Return a blank row for the given table (with default columns populated)."""

    @abc.abstractmethod
    def dupe_row(self, row: "RowAPI") -> "RowAPI":
        """Duplicate a row (table-dependent behaviour)."""

    @abc.abstractmethod
    def delete(self, row: "RowAPI") -> None:
        """Delete a row from the database."""

    @abc.abstractmethod
    def update_columns(self, values_map: Any, field: Optional[str] = None, table: Optional[str] = None) -> None:
        """Bulk update columns (pass-through to the wrapper)."""

    # ---------------------------------------------------------------------------------------------
    # Dirtied-record tracking
    # ---------------------------------------------------------------------------------------------
    @property
    @abc.abstractmethod
    def metadata_dirtied_table(self) -> str:
        """Name of the helper table used to persist dirtied-record events."""

    @abc.abstractmethod
    def get_dirtied_count(self, *, include_persisted: bool = False) -> int:
        """Return number of dirtied-record events in memory (and optionally persisted ones)."""

    @abc.abstractmethod
    def dirty_record(self, table: str, row_id: int, reason: str = "") -> None:
        """Queue a dirtied-record event."""

    @abc.abstractmethod
    def get_persisted_dirtied_count(self) -> int:
        """Return number of persisted dirtied-record events."""

    @abc.abstractmethod
    def persist_dirtied_records(self, *, limit: Optional[int] = None) -> int:
        """Persist queued dirtied-record events into the helper table; returns number persisted."""

    # ---------------------------------------------------------------------------------------------
    # Search / retrieval
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def search(self, table: str, column: str, search_term: Any) -> list["RowAPI"]:
        """Search a table for rows matching the given column == search_term (driver-specific matching)."""

    @abc.abstractmethod
    def multi_column_search(self, search_index: Any, iterator_return: bool = False) -> Any:
        """Multi-column search helper (driver-dependent / may be incomplete)."""

    @abc.abstractmethod
    def get_unique(self, target_column: str) -> Any:
        """Convenience wrapper for get_values_set()."""

    @abc.abstractmethod
    def get_values_set(self, target_column: str, iterator_return: bool = False) -> Any:
        """Return the unique values for a column (as a set or iterator)."""

    @abc.abstractmethod
    def get_row_from_id(self, table: str, row_id: int) -> Optional["RowAPI"]:
        """Return the row with the given id from table, or None if not found."""

    @abc.abstractmethod
    def get_random_row(self, table: str) -> "RowAPI":
        """Return a randomly chosen row from a table."""

    @abc.abstractmethod
    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = True,
        sort_column: Optional[str] = None,
        reverse: bool = False,
    ) -> Union[list["RowAPI"], Iterator["RowAPI"]]:
        """Return all rows from a table as list or iterator."""

    @abc.abstractmethod
    def chunk_iterator(self, column: str, target_table: Optional[str] = None) -> Iterator[list["RowAPI"]]:
        """Iterate over grouped chunks of rows based on unique values in a column."""

    # ---------------------------------------------------------------------------------------------
    # Interlink tables (many-to-many between two *different* tables)
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_interlink_row(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        onelink: bool = True,
    ) -> Optional[Union["RowAPI", list["RowAPI"]]]:
        """Return the interlink row(s) connecting two rows from different tables."""

    @abc.abstractmethod
    def get_interlink_rows(self, primary_row: "RowAPI", secondary_table: str) -> list["RowAPI"]:
        """Return interlink rows connecting primary_row to any row in secondary_table."""

    @abc.abstractmethod
    def get_interlinked_rows(
        self,
        target_row: Optional["RowAPI"] = None,
        secondary_table: Optional[str] = None,
        type_filter: Optional[str] = None,
        **kwargs: Any,
    ) -> list["RowAPI"]:
        """Return the rows in secondary_table linked to target_row (optionally filtering by type)."""

    @abc.abstractmethod
    def get_interlink_values(self, target_row: "RowAPI", secondary_column: str) -> set[Any]:
        """Return the values from a secondary column across all interlinks from target_row."""

    @abc.abstractmethod
    def interlink_rows(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        priority: Optional[Union[int, float, str]] = "highest",
        type: Optional[str] = None,
        **col_value_pairs: Any,
    ) -> "RowAPI":
        """Create an interlink between two rows and return the created interlink row."""

    @abc.abstractmethod
    def dupe_interlinks(
        self,
        src_row: "RowAPI",
        dst_row: "RowAPI",
        swap_priorities: bool = False,
        restrict_to_tables: Optional[Iterable[str]] = None,
        force_priority: Optional[Union[int, float, str]] = None,
    ) -> None:
        """Duplicate interlinks from src_row to dst_row."""

    @abc.abstractmethod
    def swap_priorities(self, src_row: "RowAPI", dst_row_1: "RowAPI", dst_row_2: "RowAPI") -> None:
        """Swap interlink priorities between dst_row_1 and dst_row_2 for the link anchored at src_row."""

    @abc.abstractmethod
    def update_interlink(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        priority: Optional[Union[int, float, str]] = "unchanged",
        **col_value_pairs: Any,
    ) -> "RowAPI":
        """Update (or create) an interlink between two rows (priority-aware)."""

    @abc.abstractmethod
    def update_interlink_priority(
        self,
        primary_row: "RowAPI",
        secondary_table: str,
        ordered_ids: Iterable[int],
    ) -> None:
        """Rewrite interlink priorities for primary_row -> secondary_table according to ordered secondary ids."""

    @abc.abstractmethod
    def unlink_interlink(self, primary_row: "RowAPI", secondary_row: "RowAPI") -> None:
        """Remove an interlink between two rows."""

    @abc.abstractmethod
    def unlink_all(self, primary_row: "RowAPI", secondary_table: str, type_filter: Optional[str] = None) -> None:
        """Remove all interlinks from primary_row to rows in secondary_table (optionally filtering by type)."""

    # ---------------------------------------------------------------------------------------------
    # Intralink tables (many-to-many within the *same* table)
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def intralink_rows(self, primary_row: "RowAPI", secondary_row: "RowAPI", link_type: str) -> "RowAPI":
        """Create an intralink between two rows from the same table."""

    @abc.abstractmethod
    def get_intralink_row(self, primary_row: "RowAPI", secondary_row: "RowAPI") -> Optional["RowAPI"]:
        """Return the intralink row connecting two rows (or None if not present)."""

    @abc.abstractmethod
    def get_intralink_rows(
        self,
        row: "RowAPI",
        primary: bool = True,
        secondary: bool = True,
        link_type_filter: Optional[str] = None,
    ) -> list["RowAPI"]:
        """Return intralink rows linked to the given row (primary/secondary direction flags)."""

    @abc.abstractmethod
    def get_intralinked_rows(
        self,
        primary_row: Optional["RowAPI"],
        secondary_row: Optional["RowAPI"],
    ) -> list["RowAPI"]:
        """Return rows intralinked to primary_row/secondary_row (direction depends on driver_wrapper policy)."""

    @abc.abstractmethod
    def unlinked_intralink(self, primary_row: Optional["RowAPI"], secondary_row: Optional["RowAPI"]) -> None:
        """Remove an intralink between two rows."""

    # ---------------------------------------------------------------------------------------------
    # Tree helpers (hierarchies expressed via intralinks)
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_root_row(self, start_row: "RowAPI") -> "RowAPI":
        """Return the root row for a tree anchored at start_row."""

    @abc.abstractmethod
    def get_root_series(self, start_row: "RowAPI") -> "RowAPI":
        """Return the lineage from start_row up to the root (inclusive)."""

    @abc.abstractmethod
    def get_children(self, src_row: "RowAPI") -> list["RowAPI"]:
        """Return immediate children of src_row in the tree."""

    @abc.abstractmethod
    def get_linear_row_list(self, start_row: "RowAPI") -> list["RowAPI"]:
        """Return a linearized list of tree rows starting from start_row."""

    @abc.abstractmethod
    def get_all_tree_rows(self, start_row: "RowAPI", back_iterate: bool = True) -> set["RowAPI"]:
        """Return all rows in the tree under start_row (optionally iterating 'backwards')."""

    @abc.abstractmethod
    def walk(self, start_row: "RowAPI") -> Iterator["RowAPI"]:
        """Yield rows in the tree in a driver-defined walk order."""

    @abc.abstractmethod
    def search_tree(self, root_row: "RowAPI", for_ids: Iterable[int]) -> set[int]:
        """Search a tree for particular row ids."""

    @abc.abstractmethod
    def nest_rows(self, parent_row: "RowAPI", child_rows: Union["RowAPI", Iterable["RowAPI"]]) -> None:
        """Nest child rows under parent_row in the tree."""

    @abc.abstractmethod
    def delete_tree(self, parent_row: "RowAPI") -> None:
        """Delete parent_row and all its descendants from the tree."""



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
