"""Top-level Database API contract."""

from __future__ import annotations

import abc

from typing import Any, Iterable, Iterator, Optional, Union

from .database_mixins import (
    DatabaseDirtiedRecordsMixinAPI,
    DatabaseInterlinkRowsMixinAPI,
    DatabaseIntralinkRowsMixinAPI,
    DatabaseMetadataMixinAPI,
    DatabaseNullRowsMixinAPI,
    DatabaseRatingMixinAPI,
    DatabaseSearchMixinAPI,
    DatabaseTreeMixinAPI,
    DatabaseTriggerHelpersAPI,
)

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

    @abc.abstractmethod
    def get_write_telemetry_snapshot(self, *, recent_limit: int = 8) -> dict[str, Any]:
        """Return lightweight live telemetry about observed database write activity."""

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
