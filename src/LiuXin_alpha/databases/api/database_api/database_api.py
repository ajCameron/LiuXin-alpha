"""
Declare the abstract composite interface for database facades.

Mixins contribute search, relationship, metadata and maintenance contracts. These declarations describe overridable operations, not a functioning database; the concrete Database implementation owns backend connections and runtime services. Type annotations do not validate inputs at runtime.
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
    Combine database lifecycle, schema and row-operation contracts.

    Subclasses must supply all abstract members, including inherited mixin methods. Attribute annotations such as metadata and lock neither initialize state nor enforce runtime values. Concrete Database behavior includes backend-dependent operations and best-effort cleanup; this interface does not promise transactional context-manager rollback.

    Example:
        Type a consumer argument as DatabaseAPI and pass an initialized Database or a compatible complete implementation; DatabaseAPI itself cannot be instantiated.
    """

    # ---------------------------------------------------------------------------------------------
    # Driver wiring / core handles
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def set_driver(self, new_driver: "DatabaseDriverAPI") -> None:
        """
        Replace the driver and rebuild wrapper, macros and metadata SQL bindings.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: Close the old wrapper first. For a different driver, try committing its primary connection, roll back only if commit fails, then close it. Cleanup errors are suppressed. Install the new wrapper and convenience aliases, including storage aliases, which may become None. This does not rerun schema discovery, queue wiring or all runtime back-references; failures during new setup leave partial state.

        Example:
            For a compatible open replacement driver, db.set_driver(replacement) releases the previous database connections before constructing its new wrapper.


        :param new_driver: Driver to install; supplying the current driver retains its primary connection.
        :return: None.
        """

    @abc.abstractmethod
    def set_driver_wrapper(self, new_driver_wrapper: "DatabaseDriverWrapperAPI") -> None:
        """
        Assign a wrapper and replace its macros reference with the facade macros.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: Assignment precedes validation. This neither closes the previous wrapper nor refreshes convenience aliases, locks or back-references.

        Example:
            During controlled assembly, db.set_driver_wrapper(wrapper) requires wrapper.macros already set before copying db.macros.


        :param new_driver_wrapper: Wrapper whose existing macros attribute must be non-None.
        :return: None.
        :raises AssertionError: The newly assigned wrapper has macros=None.
        """

    @abc.abstractmethod
    def set_macros(self, new_macros: "MacrosAPI") -> None:
        """
        Replace the facade macros reference after rejecting None.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: This does not update the driver, wrapper or collaborator back-references.

        Example:
            When assembling a compatible facade, db.set_macros(macros) stores that object without rebuilding other collaborators.


        :param new_macros: Macros object to retain directly.
        :return: None.
        :raises AssertionError: new_macros is None.
        """

    @abc.abstractmethod
    def set_metadata_sql(self, new_metadata_sql: "MetadataSQLAPI") -> None:
        """
        Replace the metadata SQL reference after rejecting None.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: No back-reference or other collaborator is updated here.

        Example:
            During custom assembly, db.set_metadata_sql(helper) retains helper; the caller must arrange its db binding.


        :param new_metadata_sql: Metadata SQL helper to retain directly.
        :return: None.
        :raises AssertionError: new_metadata_sql is None.
        """

    @property
    @abc.abstractmethod
    def driver(self) -> "DatabaseDriverAPI":
        """
        Return the currently bound backend driver.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: Return the currently bound backend driver.

        Example:
            For an open db, db.driver.conn accesses the primary backend connection when the driver exposes one.


        :return: Driver object, or None after cleanup.
        """

    @property
    @abc.abstractmethod
    def driver_wrapper(self) -> "DatabaseDriverWrapperAPI":
        """
        Return the wrapper that provides facade operations and the lock connection.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: Return the wrapper that provides facade operations and the lock connection.

        Example:
            For an open SQL db, db.driver_wrapper.lock is the wrapper connection rather than the primary driver connection.


        :return: Current wrapper, or None after cleanup.
        """

    @property
    @abc.abstractmethod
    def macros(self) -> "MacrosAPI":
        """
        Return the currently bound schema-specific SQL macros.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: Return the currently bound schema-specific SQL macros.

        Example:
            After construction, db.macros is the macros object taken from the bound driver.


        :return: Macros collaborator, or None before binding or after cleanup.
        """

    @property
    @abc.abstractmethod
    def metadata_sql(self) -> "MetadataSQLAPI":
        """
        Return the helper used for metadata-aware SQL operations.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: Return the helper used for metadata-aware SQL operations.

        Example:
            For an initialized db, db.metadata_sql.db refers back to the owning facade.


        :return: Current MetadataSQL collaborator, or None before binding or after cleanup.
        """

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
        """
        Stop maintenance and release database connections with best-effort cleanup.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: Request maintenance stop and wait at most one second for its thread. Close the wrapper or fallback lock, then attempt to commit and close the primary connection; roll back only if that commit fails. Suppress ordinary cleanup errors. Repeated calls tolerate cleared state. This does not call StorageManager.close or the driver-wide close method, and cannot guarantee every reference cycle is removed.

        Example:
            Use db.close() in a finally block to release an open facade; pending primary-connection changes may be committed.


        :return: None; connection aliases and selected collaborator references are cleared.
        """

    @abc.abstractmethod
    def refresh_db_metadata(self) -> None:
        """
        Rebuild cached table categories and validate required helper tables.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: Optional compatibility tables may be absent. Missing required helpers raise after category state has been reset, with close-name suggestions. Classification is name-based. A missing UUID requests set_uuid without re-reading it. sqlite_sequence is removed from main_tables after forming dirtiable_tables, so it can remain in that union. Wrapper category aliases are not refreshed by this method itself.

        Example:
            During initialization, db.refresh_db_metadata() classifies the discovered schema before its categories are copied onto the wrapper.


        :return: None; category sets, dirtiable_tables and the cached UUID are assigned.
        :raises DatabaseIntegrityError: Required non-optional helper tables are missing.
        """

    @abc.abstractmethod
    def check_exists(self) -> bool:
        """
        Ask the bound driver whether its database exists.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: This does not update the cached _exists field or independently validate schema completeness.

        Example:
            For an open db, db.check_exists() delegates the existence check to its driver.


        :return: The driver exists result, whose meaning depends on the backend.
        """

    @abc.abstractmethod
    def backup(self) -> None:
        """
        Request a backend-specific backup of the current database.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: Request a backend-specific backup of the current database.

        Example:
            For a configured open db, db.backup() invokes driver.direct_backup(); the backend chooses the backup destination and behavior.


        :return: None; any driver return value is discarded.
        """

    @abc.abstractmethod
    def lock_writing(self) -> None:
        """
        Request a scratch copy through the driver make_scratch hook.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: This method does not acquire a write lock. The SQL base implementation copies the database file and changes its path attribute without reopening existing connections.

        Example:
            Call db.lock_writing() only when the selected backend scratch-copy behavior is suitable; it is not a transaction lock.


        :return: None; any scratch path returned by the driver is discarded.
        """

    @abc.abstractmethod
    def create_new_database(self, blank: bool = True, backup: bool = True) -> None:
        """
        Optionally back up and delete the database before requesting schema creation.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: Operations run in backup, deletion, creation order. No rollback, driver reload or metadata refresh occurs here; backend failures propagate after any earlier side effects.

        Example:
            On a disposable configured backend, db.create_new_database(blank=False, backup=False) requests creation without first backing up or deleting it.


        :param blank: Delete the existing database through the driver before creation.
        :param backup: Request a backup before any deletion or creation.
        :return: None.
        """

    @abc.abstractmethod
    def break_cycles(self) -> None:
        """
        Clear selected runtime and connection references without closing resources.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: Each assignment is best effort, including on partially initialized instances. Storage, preferences and the clean alias are not cleared. Call close first when handles still need releasing; clearing references alone does not perform shutdown.

        Example:
            Normal db.close() invokes this after attempting to close its captured connections.


        :return: None.
        """

    # Context manager helpers (implemented by the concrete Database).
    @abc.abstractmethod
    def __enter__(self) -> "DatabaseAPI":
        """
        Return this facade for use inside a cleanup context.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: No new transaction is opened.

        Example:
            For an open db, with db as active: binds active to db and closes it when the block exits.


        :return: This Database instance.
        """

        ...

    @abc.abstractmethod
    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        """
        Close the facade and allow a context-body exception to propagate.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: Exception details are ignored by close; pending work may be committed even on an exceptional exit.

        Example:
            An exception raised inside with db: propagates after cleanup; use an explicit transaction API when rollback is required.


        :param exc_type: Exception class passed by the context protocol, or None.
        :param exc: Exception instance passed by the context protocol, or None.
        :param tb: Traceback passed by the context protocol, or None.
        :return: False, so exceptions are not suppressed.
        """

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
        """
        Snapshot the cached main-table category as an immutable set.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: Snapshot the cached main-table category as an immutable set.

        Example:
            For an initialized db, "works" in db.main_tables checks its cached main-table classification.


        :return: New frozenset of main-table names.
        :raises TypeError: The underlying category is still None.
        """

    @property
    @abc.abstractmethod
    def interlink_tables(self) -> frozenset[str]:
        """
        Snapshot the cached inter-table relationship category as an immutable set.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: Snapshot the cached inter-table relationship category as an immutable set.

        Example:
            For an initialized db, sorted(db.interlink_tables) lists the cached relationship tables.


        :return: New frozenset of interlink-table names.
        :raises TypeError: The underlying category is still None.
        """

    @abc.abstractmethod
    def categorize_table(self, table: str) -> str:
        """
        Classify a known table name using ordered naming conventions.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: Precedence is allowed_types__ prefix, configured helper names, custom-column patterns, interlink pattern, intralink pattern, then main. Regex checks accept matching prefixes; classification does not inspect columns or foreign keys.

        Example:
            For a concrete db with works in all_tables, db.categorize_table("works") returns "main".


        :param table: Name converted to text and checked against all_tables.
        :return: One of allowed_types, helper, custom, interlink, intralink or main.
        :raises InputIntegrityError: The text name is absent from all_tables.
        """

    # ---------------------------------------------------------------------------------------------
    # Schema inspection helpers
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_tables(self, force_refresh: bool = False) -> Iterable[str]:
        """
        Require discovery of database table names with optional cache refresh.

        The concrete facade delegates to its wrapper; backend discovery may include compatibility views.

        Example:
            For a concrete db, names = db.get_tables(force_refresh=True) refreshes the table-name lookup.


        :param force_refresh: Request a fresh backend schema view instead of cached names.
        :return: Iterable of table names from an implementation.
        """

    @abc.abstractmethod
    def get_column_headings(self, table: str) -> list[str]:
        """
        Require ordered column discovery for a table.

        Example:
            For a concrete db, db.get_column_headings("works") returns names usable in row operations.


        :param table: Target table name.
        :return: Column-name list in backend order.
        """

    @abc.abstractmethod
    def get_view_column_headings(self, view: str) -> list[str]:
        """
        Require ordered column discovery for a view.

        Example:
            For a concrete db exposing the compatibility view, db.get_view_column_headings("titles") inspects its columns.


        :param view: Target view name.
        :return: Column-name list in backend order.
        """

    @abc.abstractmethod
    def get_tables_and_columns(self) -> dict[str, list[str]]:
        """
        Require a mapping from table names to their column collections.

        Iterate items() when both the table and its columns are needed.

        Example:
            For a concrete db, for table, columns in db.get_tables_and_columns().items(): visits each table/column pair.


        :return: Mapping of table names to column names; annotated as lists, although backend collections may differ.
        """

    @abc.abstractmethod
    def get_record_count(self, target_table: str) -> int:
        """
        Require the number of records in a target table.

        Example:
            For a concrete db, count = db.get_record_count("works") reads the current table size.


        :param target_table: Table to count using backend semantics.
        :return: Integer record count.
        """

    @abc.abstractmethod
    def get_max(self, column: str) -> Any:
        """
        Require the backend maximum for a named column.

        Example:
            For a concrete db, maximum = db.get_max("work_id") asks its driver for the column maximum.


        :param column: Column identifier interpreted by the backend.
        :return: Maximum value, with empty-column and conversion behavior determined by the driver.
        """

    @abc.abstractmethod
    def get_min(self, column: str) -> Any:
        """
        Require the backend minimum for a named column.

        Example:
            For a concrete db, minimum = db.get_min("work_id") asks its driver for the column minimum.


        :param column: Column identifier interpreted by the backend.
        :return: Minimum value, with empty-column and conversion behavior determined by the driver.
        """

    @abc.abstractmethod
    def row_counts(self) -> str:
        """
        Require a textual summary of table record counts.

        The concrete facade queries tables in its cached main, interlink, intralink and helper categories.

        Example:
            For an initialized concrete db, summary = db.row_counts() captures counts as text without printing it.


        :return: Human-readable string describing categorized tables and their counts.
        """

    # ---------------------------------------------------------------------------------------------
    # Row factories / direct row operations
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_blank_row(self, table: Optional[str] = None) -> "RowAPI":
        """
        Require allocation of a blank persistent row with its generated identity.

        The concrete implementation inserts before wrapping the record, so this is a write rather than an in-memory factory.

        Example:
            For a concrete writable db, row = db.get_blank_row("works") allocates a persistent record.


        :param table: Target table; the interface permits None, but concrete Database callers must provide a valid table.
        :return: RowAPI-compatible record bound to the database.
        """

    @abc.abstractmethod
    def dupe_row(self, row: "RowAPI") -> "RowAPI":
        """
        Insert a blank row, copy source fields and synchronize under the new ID.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: Restore the new ID after copying the source row_dict. If sync raises DatabaseIntegrityError, delete the new row and reraise; cleanup errors can mask that failure. Other errors do not trigger this cleanup. Unique source values may prevent duplication.

        Example:
            In a table whose copied values satisfy its constraints, duplicate = db.dupe_row(row) allocates a separate row ID.


        :param row: Row whose table and field values supply the duplicate.
        :return: New persisted Row with its allocated identity.
        :raises DatabaseIntegrityError: Synchronization violates a database constraint.
        """

    @abc.abstractmethod
    def delete(self, row: "RowAPI") -> None:
        """
        Delete the supplied row identity through this facade wrapper.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: Ownership is not checked: deletion uses this database with the supplied table and ID. The passed Row object is not cleared.

        Example:
            For a Row belonging to db, db.delete(row) removes its persistent record through delete_by_id.


        :param row: Row-like object providing table and a non-None row_id.
        :return: None.
        :raises InputIntegrityError: The supplied row_id is None.
        """

    @abc.abstractmethod
    def update_columns(self, values_map: Any, field: Optional[str] = None, table: Optional[str] = None) -> None:
        """
        Forward a bulk value mapping and optional column/table hints to the wrapper.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: Forward a bulk value mapping and optional column/table hints to the wrapper.

        Example:
            For a backend-supported values_map, db.update_columns(values_map, field=column, table=table) forwards those arguments unchanged.


        :param values_map: Value mapping in the format accepted by the selected driver.
        :param field: Optional target field hint.
        :param table: Optional target table hint.
        :return: None; the wrapper return value is discarded.
        """

    # ------------------------------------
    # Basic id methods
    # ------------------------------------

    @abc.abstractmethod
    def get_table_from_column(self, column_name: str) -> str:
        """
        Search an iterable of table/column pairs for the first containing table.

        Abstract declaration; subclasses supply the operation. In the concrete Database implementation: The loop iterates get_tables_and_columns directly. The normal implementation returns a dictionary, so table-name keys are unpacked rather than key/value pairs; ordinary names raise ValueError before lookup. This legacy method does not currently adapt that mapping with items().

        Example:
            With the normal mapping-returning wrapper, use explicit iteration over db.get_tables_and_columns().items() when looking up a column; this method retains its legacy unpacking limitation.


        :param column_name: Column name to find in each yielded collection.
        :return: First matching table name if pair iteration succeeds.
        :raises ValueError: A yielded table-name key cannot unpack into two values.
        :raises InputIntegrityError: Iteration finishes without finding the column.
        """


