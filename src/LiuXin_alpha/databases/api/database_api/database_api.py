"""
Top-level Database API contract.

All database like objects should descend from this API.
"""

from __future__ import annotations

import abc

from typing import Any, Iterable, Optional, TYPE_CHECKING

from LiuXin_alpha.databases.api.database_api.mixins import (
    DatabaseNullRowsMixinAPI,
    DatabaseRatingMixinAPI,
)
from LiuXin_alpha.databases.api.database_api.mixins.metadata_mixin_api import DatabaseMetadataMixinAPI

from LiuXin_alpha.databases.api.database_api.mixins.triggers_mixin_api import DatabaseTriggerHelpersAPI
from LiuXin_alpha.databases.api.database_api.mixins.tree_mixin_api import DatabaseTreeMixinAPI
from LiuXin_alpha.databases.api.database_api.mixins.linked_rows_mixin_api import DatabaseLinkedRowsMixinAPI
from LiuXin_alpha.databases.api.database_api.mixins.interlink_mixin_api import DatabaseInterlinkRowsMixinAPI
from LiuXin_alpha.databases.api.database_api.mixins.intralink_mixin_api import DatabaseIntralinkRowsMixinAPI
from LiuXin_alpha.databases.api.database_api.mixins.search_mixin_api import DatabaseSearchMixinAPI
from LiuXin_alpha.databases.api.database_api.mixins.dirty_records_mixin_api import DatabaseDirtiedRecordsMixinAPI

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.row_api import RowAPI
    from LiuXin_alpha.databases.api.macros_api import MacrosAPI
    from LiuXin_alpha.databases.api.metadata_sql_api import MetadataSQLAPI



# Todo: We need a names mixin
class DatabaseAPI(
    DatabaseRatingMixinAPI,
    DatabaseNullRowsMixinAPI,
    DatabaseMetadataMixinAPI,
    DatabaseDirtiedRecordsMixinAPI,
    DatabaseSearchMixinAPI,
    DatabaseInterlinkRowsMixinAPI,
    DatabaseIntralinkRowsMixinAPI,
    DatabaseTreeMixinAPI,
    DatabaseLinkedRowsMixinAPI,
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

    @abc.abstractmethod
    def set_metadata_sql(self, new_metadata_sql: "MetadataSQLAPI") -> None:
        """Attach metadata-aware SQL helpers."""

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

    @property
    @abc.abstractmethod
    def metadata_sql(self) -> "MetadataSQLAPI":
        """Return metadata-aware SQL helpers."""

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

    # ------------------------------------
    # Basic id methods
    # ------------------------------------

    @abc.abstractmethod
    def get_table_from_column(self, column_name: str) -> str:
        """
        Return the table a column is in.

        :param column_name:
        :return:
        """


