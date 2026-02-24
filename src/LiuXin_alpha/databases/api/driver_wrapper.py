"""Driver-wrapper API contracts."""

from __future__ import annotations

import abc

from typing import Any, Iterable, Iterator, Optional, Union

from .macros import MacrosAPI

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
