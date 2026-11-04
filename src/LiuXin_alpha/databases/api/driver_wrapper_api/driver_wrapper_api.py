"""
Define the contract between database objects and low-level driver wrappers.

The concrete DriverWrapper combines dictionary-row helpers, driver delegation,
schema discovery and custom-column operations. Abstract members declare the
concrete result shapes, including False and None for missing results. Construction
and behavior belong to concrete wrappers. SQL values remain unconstrained within
typed row containers and portable connection/cursor protocols.
"""

from __future__ import annotations

import abc
from collections.abc import Iterable, Iterator, Mapping, Set
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Never,
)

from LiuXin_alpha.databases.api.driver_wrapper_api.mixins.query_operations_api import (
    QueryOperationsAPI,
)
from LiuXin_alpha.databases.api.driver_wrapper_api.mixins.schema_introspection_api import (
    SchemaIntrospectionAPI,
)
from LiuXin_alpha.databases.db_types.driver_types import (
    QueryConnection,
    QueryCursor,
    SQLParameters,
    SQLRow,
)

if TYPE_CHECKING:
    from queue import Queue
    from threading import RLock

    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.api.driver_api import DatabaseDriverAPI
    from LiuXin_alpha.databases.api.portable_macros_api import DatabaseMacrosAPI
    from LiuXin_alpha.databases.schema_specs import StorageLinkSpec


class DatabaseDriverWrapperAPI(SchemaIntrospectionAPI, QueryOperationsAPI):
    """
    Define the contract between database objects and low-level driver wrappers.

    The concrete DriverWrapper combines dictionary-row helpers, driver delegation,
    schema discovery and custom-column operations. These abstract declarations
    describe concrete containers and missing-result sentinels. Construction and
    behavior belong to concrete wrappers; no operational methods live here.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DatabaseDriverWrapperAPI)
        True
    """

    db: DatabaseAPI | None
    lock: RLock | None
    all_tables: Set[str] | None
    main_tables: Set[str] | None
    interlink_tables: Set[str] | None
    intralink_tables: Set[str] | None
    helper_tables: Set[str] | None
    dirtiable_tables: Set[str] | None
    dirty_records_queue: Queue[tuple[str, int, str]] | None

    @property
    @abc.abstractmethod
    def driver(self) -> DatabaseDriverAPI:
        """Return the bound backend; a detached wrapper raises RuntimeError."""

    @driver.setter
    @abc.abstractmethod
    def driver(self, driver: DatabaseDriverAPI) -> None:
        """Bind a backend without transferring ownership of its connections."""

    @abc.abstractmethod
    def get_allowed_tables_snapshot(self) -> frozenset[str]:
        """Snapshot populated table groups, falling back to relation discovery.

        The concrete wrapper unions its cached groups first. If they are empty,
        it queries tables and views, treating either discovery failure as empty.
        The resulting names are not independently checked for existence.
        """

    @abc.abstractmethod
    def __del__(self) -> None:
        """
        Attempt wrapper shutdown during finalization.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Calls close() and suppresses ordinary exceptions, including failures caused by
        partial initialization. Use explicit close() for deterministic cleanup.

        Example:
            >>> wrapper.__del__()  # doctest: +SKIP


        :return: None.
        """

    @abc.abstractmethod
    def _canonicalise_cc_in_table(self, in_table: str) -> str:
        """
        Resolve the legacy books attachment name against available table groups.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Existing main, interlink or intralink names pass through. If books is absent,
        prefer manifestations, then items, then works. Other unknown names pass through
        unchanged for later validation.

        Example:
            >>> wrapper._canonicalise_cc_in_table("works")  # doctest: +SKIP


        :param in_table: Table to which the custom column is attached.
        :return: Resolved attachment name, or the original name.
        """

    @abc.abstractmethod
    def _get_custom_column_row(self, in_table: str, cc_name: str) -> None:
        """
        Reserve a hook for retrieving a custom-column definition row.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        The current body is pass: it performs no lookup and ignores both arguments.

        Example:
            >>> wrapper._get_custom_column_row("works", "note")  # doctest: +SKIP


        :param in_table: Table to which the custom column is attached.
        :param cc_name: Custom column name.
        :return: None; this hook remains unimplemented.
        """

    @abc.abstractmethod
    def _walk(
        self,
        start_row: dict[str, Any],
        table: str,
        table_id_col: str,
        table_parent_col: str,
    ) -> Iterator[dict[str, Any]]:
        # Load the ids pool with the ids of parent rows - search for them in the parent column and yield those rows
        # If a row has no children (not referenced in any parent column) then it's a leaf row and we're done for that
        # branch
        """
        Yield a starting row and descendants selected through parent-column searches.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Converts IDs to integers and keeps pending IDs in a set. Yields the original
        starting mapping first, then children returned by search(), scheduling each
        child ID. A separate visited set rejects cycles/repeated rows before yielding
        the duplicate. Sibling order is unspecified; memory grows with visited IDs.

        The concrete implementation consumes and yields plain dictionaries.

        Example:
            >>> wrapper._walk(
            ...     series_row, "works", "series_id", "series_parent"
            ... )  # doctest: +SKIP


        :param start_row: Starting row dictionary containing its identifier and any
            required parent value.
        :param table: Table name in the current schema.
        :param table_id_col: Identifier column in the traversed table.
        :param table_parent_col: Parent-reference column in the traversed table.
        :return: Iterator yielding the starting row and discovered descendants.
        """

    @abc.abstractmethod
    def add_multiple_rows(self, row_dict_list: list[dict[str, Any]]) -> None:
        """
        Insert a batch atomically, binding values by named column.

        The shared SQL/SQLite writer requires one table, matching column sets and no
        non-null explicit IDs. Input dictionaries may have different key orders and are
        not mutated. Normalization and custom-column NUL handling match single inserts.
        A failure rolls back the whole batch and closes its owned connection. PostgreSQL
        also rolls back the full batch while retaining its primary connection.

        Example:
            A uniqueness violation on the second row leaves neither row inserted.

        :param row_dict_list: Iterable of row mappings; shared SQL requires homogeneous
            rows.
        :return: None; the driver result is discarded.
        """

    @abc.abstractmethod
    def add_row(self, row_dict: dict[str, Any]) -> int:
        """
        Insert a copied row mapping, deriving configured normalized identity values.

        The shared SQL/SQLite writer sanitizes NUL text in custom-column values, commits
        only on success and always closes its connection. PostgreSQL retains its primary
        connection. Backend exceptions propagate through this interface.

        Example:
            A work_title value of "None" remains ordinary text after insertion.

        :param row_dict: Column/value mapping, optionally including a table hint; not
            mutated.
        :return: The inserted row ID.
        """

    @abc.abstractmethod
    def break_cycles(self) -> None:
        """
        Clear the lock and driver references without closing either resource.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Attribute-assignment errors are suppressed independently. The db and macro
        references remain; the database/driver owner closes SQL connections separately.

        Example:
            >>> wrapper.break_cycles()  # doctest: +SKIP


        :return: None.
        """

    @abc.abstractmethod
    def check_for_intralink_table(self, table_name: str) -> str | Literal[False]:
        """
        Check for the conventionally named intralink table of a main table.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Lowercases the supplied name, derives its column base and forms
        <base>_<base>_intralinks. Both the main table and generated name must occur in
        discovered table headings.

        Example:
            >>> wrapper.check_for_intralink_table("works")  # doctest: +SKIP


        :param table_name: Table name in the current schema.
        :return: Intralink-table name, or False when either table is absent.
        """

    @abc.abstractmethod
    def clear(self, target_table: str) -> bool:
        """
        Delete every row, commit, and count remaining rows before closing the
        connection.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_clear_table hook. The following backend details
        describe the shared SQL/SQLite implementation; backend errors propagate.

        Translate SQLite operational/integrity errors. The count is taken after an
        explicit commit and is not an atomic guarantee against concurrent inserts.

        Example:
            >>> wrapper.clear("works")  # doctest: +SKIP


        :param target_table: Existing table name, validated by the driver.
        :return: Whether the follow-up row count is zero.
        """

    @abc.abstractmethod
    def close(self) -> None:
        """
        Clear wrapper references without committing or closing driver connections.

        The lock is a mutex, not a SQL connection. The Database/driver owner
        releases database handles separately. Repeated calls tolerate cleared state.

        Example:
            >>> wrapper.close()  # doctest: +SKIP


        :return: None.
        """

    @abc.abstractmethod
    def complete_row(self, partial_row: dict[str, Any]) -> dict[str, Any]:
        """
        Fill missing keys of a copied row dictionary from its database row.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Infers the table and requires a non-None ID. Missing IDs and an explicit False
        result from row lookup raise InputIntegrityError. Existing supplied values take
        precedence, including None; the source mapping is unchanged. Other backend
        missing-row sentinels are not explicitly handled here.

        Example:
            >>> wrapper.complete_row({"work_id": 1})  # doctest: +SKIP


        :param partial_row: Partial plain row mapping containing a usable ID.
        :return: Merged dictionary with supplied keys protected.
        """

    # Todo: Issue deprecitation warnings from this - we want it gone
    @property
    @abc.abstractmethod
    def conn(self) -> QueryConnection | None:
        """
        Resolve an override or the current owner/driver connection.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        An explicit override is probed with SELECT 1 when it exposes execute; an
        override without that method is accepted as-is. Probe failures clear the stored
        override where possible. Then prefer db.driver.conn, followed by
        self.driver.conn, suppressing lookup errors. If neither works, returns the
        original local override, which may still be stale after a failed probe. No new
        connection is opened.

        Example:
            >>> wrapper.conn  # doctest: +SKIP


        :return: Override or live driver connection; possibly None or the failed
            override when all fallbacks fail.
        """

    @abc.abstractmethod
    def create_custom_column(
        self,
        name: str,
        datatype: str = "text",
        is_multiple: bool = False,
        label: str | None = None,
        editable: bool = True,
        display: Mapping[str, Any] | None = None,
        in_table: str = "books",
        table: str | None = None,
        make_category: bool | None = None,
    ) -> int:
        """
        Create a custom-column definition and its physical value storage.

        The table alias overrides the default books attachment; conflicting nondefault
        aliases raise TypeError. Resolves legacy attachment names and asserts membership
        in the owner table groups. Rejects unsupported datatypes, invalid labels, and
        multiple numeric/date/bool/rating columns. Labels default to
        <attachment>__<name> and must begin with a letter and contain lowercase word
        characters.

        Text, rating, series and enumeration use normalized value/link storage. Multiple
        comments and series request ordering. display is serialized as JSON; for
        composite columns, make_category updates that mapping. Definition insertion
        and physical DDL share one owned transaction: either both commit or both roll
        back. Nested macro transactions are refused before writing, so facade caches
        are updated only after a real commit. Returns the definition ID.

        Example:
            >>> wrapper.create_custom_column("note")  # doctest: +SKIP


        :param name: Custom-column display name.
        :param datatype: Supported CUSTOM_DATA_TYPES name, such as text, int, comments,
            series or composite.
        :param is_multiple: Whether multiple values are requested; unsupported scalar
            combinations raise NotImplementedError.
        :param label: Optional lowercase registry label; None derives one from
            attachment and name.
        :param editable: Editable flag recorded in the definition.
        :param display: JSON-serializable options; None becomes an empty dict.
        :param in_table: Attachment table name; defaults to books.
        :param table: Optional alias for in_table; conflicting explicit names raise
            TypeError.
        :param make_category: Optional boolean-like composite browser-category setting;
            ignored for other datatypes.
        :return: New integer custom-column definition ID.
        """

    @abc.abstractmethod
    def create_new_main_table(
        self,
        table_name: str,
        column_headings: Mapping[str, Mapping[str, str]] | None = None,
        link_to: str | None = None,
        link_type: Literal["many_many", "many_one", "one_many", "one_one"]
        | None = None,
        link_properties: Iterable[str] | None = None,
    ) -> None:
        """
        Create a main table and optionally link it to an existing table.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        A non-None link_type is asserted even when link_to is None. Creates the table
        first, then optionally links link_to as primary and the new table as secondary.
        Separate backend operations can commit independently; failure to create the link
        does not imply removal of the new table. Wrapper derived caches are not
        explicitly cleared here.

        The concrete wrapper accepts a single link_to table and one cardinality string;
        iterable annotations here do not imply multi-target support.

        Example:
            >>> wrapper.create_new_main_table(
            ...     "samples", {"sample_value": "TEXT"}, link_type="many_many"
            ... )  # doctest: +SKIP


        :param table_name: Name of the new main table.
        :param column_headings: Backend column specification; shared SQL expects a
            mapping of headings to SQL types.
        :param link_to: Existing primary table to link, or None to create only the new
            table.
        :param link_type: Required non-None cardinality: one_one, many_one, one_many or
            many_many.
        :param link_properties: Optional requested link-column suffixes passed to the
            driver.
        :return: None.
        """

    @staticmethod
    @abc.abstractmethod
    def custom_table_names(num: int, in_table: str = "books") -> tuple[str, str]:
        """
        Construct custom-value and owner-link table names from a numeric ID.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Converts num with int(), logging and re-raising ValueError; other conversion
        errors propagate. Does not check table existence or validate the attachment
        name.

        Example:
            >>> wrapper.custom_table_names(1)  # doctest: +SKIP


        :param num: Custom-column metadata identifier.
        :param in_table: Table to which the custom column is attached.
        :return: Pair (custom_column_<id>, <in_table>_custom_column_<id>_link).
        """

    # Todo: Rename for greater clarity.
    # Todo: Might want to return the affected ids
    @abc.abstractmethod
    def delete(self, target_table: str, column: str, value: Any) -> bool | None:
        """
        Delete rows matching a scalar value or a list/set of values.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Lists and sets dispatch to direct_delete_many(); all other values, including
        tuples, use direct_delete(). A failed batch rolls back all deletions in that
        call. Driver validation and exception translation still apply.

        Example:
            >>> wrapper.delete("works", "work_title", "Example")  # doctest: +SKIP


        :param target_table: Table whose rows are affected.
        :param column: Column name in the selected table.
        :param value: Scalar match value, or list/set selecting the bulk deletion path.
        :return: Unmodified driver result: shared SQL returns None for list/set
            batches and True for scalar deletion; PostgreSQL returns True.
        """

    @abc.abstractmethod
    def delete_by_id(
        self, target_table: str, row_id: int | list[int] | set[int]
    ) -> bool:
        """
        Delete one row ID or dispatch list/set IDs to the bulk driver helper.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Only list and set select direct_delete_many_by_ids(); other objects use
        direct_delete_row_by_id(). Backend errors propagate, and a success result does
        not prove a row existed.

        Example:
            >>> wrapper.delete_by_id("works", 1)  # doctest: +SKIP


        :param target_table: Table whose rows are affected.
        :param row_id: Single ID, or a list/set of IDs.
        :return: Unmodified driver deletion result.
        """

    @abc.abstractmethod
    def delete_custom_column(self, num: int) -> None:
        """
        Mark a custom-column definition for deferred deletion.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Calls the configured macro provider; tables are removed later by
        deleted_marked_custom_columns(), typically during reload. Does not update the
        local custom-table cache.

        Example:
            >>> wrapper.delete_custom_column(1)  # doctest: +SKIP


        :param num: Custom-column metadata identifier.
        :return: None.
        """

    @abc.abstractmethod
    def deleted_marked_custom_columns(self) -> None:
        """Atomically remove marked definitions and their physical SQLite storage.

        The macro reuses the current transaction through a savepoint, or owns a
        new unit on an idle connection. Selection and deletion share that unit.
        """

    @abc.abstractmethod
    def get_custom_tables(self) -> set[str]:
        """
        Read registered custom-table names through the owner driver's live connection.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Calls db.macros.direct_get_custom_tables(conn=db.driver.conn), ignoring this
        mixin's connection override. This is a fresh macro result, not a copy of the
        local custom_tables set.

        Example:
            >>> wrapper.get_custom_tables()  # doctest: +SKIP


        :return: Set of custom table/link-table names produced by the macro.
        """

    @abc.abstractmethod
    def direct_get_custom_extra(self, link_table: str, index: int) -> Any:
        """
        Read the extra cell for one owner in a legacy custom link table.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Passes the resolved connection to db.macros.direct_get_custom_and_extra().
        Shared SQL looks up the _book owner column and selects only the first _extra
        value, without ordering; it does not return the custom value itself.

        Example:
            >>> wrapper.direct_get_custom_extra(
            ...     "books_custom_column_1_link", 1
            ... )  # doctest: +SKIP


        :param link_table: Trusted legacy custom link-table name.
        :param index: Owner identifier matched against the link _book column.
        :return: Selected extra scalar or the connection adapter's missing-value result.
        """

    @abc.abstractmethod
    def direct_get_custom_id_val_pairs(self, table: str) -> list[tuple[int, Any]]:
        """
        Read all ID/value pairs from a custom table on the resolved connection.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Passes table and conn to the owner macro without sorting or flattening. The
        historical tuple annotation does not describe the usual list of pairs.

        Example:
            >>> wrapper.direct_get_custom_id_val_pairs(
            ...     "custom_column_1"
            ... )  # doctest: +SKIP


        :param table: Table name in the current schema.
        :return: Macro result, normally a list of (ID, value) tuples.
        """

    @abc.abstractmethod
    def dirty_record(self, table: str, row_id: int, reason: str) -> None:
        """
        Enqueue a change notification for a configured dirtiable table.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        If table is absent from dirtiable_tables, logs a warning and leaves the queue
        untouched. Otherwise puts (table, row_id, reason) on the configured queue
        without deduplication or persistence.

        Example:
            >>> wrapper.dirty_record("works", 1, "title changed")  # doctest: +SKIP


        :param table: Table name in the current schema.
        :param row_id: Identifier of the target row.
        :param reason: Description of the change to include in the queue entry.
        :return: None.
        """

    @abc.abstractmethod
    def drop_all_triggers(self) -> bool:
        """
        Discover and drop all persistent triggers through the driver.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Calls get_triggers() then drop_triggers(). Shared SQLite discovery excludes TEMP
        triggers and removal commits each trigger separately, so a later failure can
        leave earlier drops applied.

        Example:
            >>> wrapper.drop_all_triggers()  # doctest: +SKIP


        :return: Result of drop_triggers(); True in the shared SQL implementation,
            including an empty trigger list.
        """

    @abc.abstractmethod
    def drop_triggers(self, triggers: list[str]) -> bool:
        """
        Drop each named trigger and commit after each removal.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_drop_triggers hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Names become SQL syntax and must be trusted. A missing trigger raises
        OperationalError; earlier removals stay committed. Close on success or that
        error.

        Example:
            >>> wrapper.drop_triggers(["sample_audit"])  # doctest: +SKIP


        :param triggers: Iterable of trusted trigger identifiers inserted into DROP
            TRIGGER statements.
        :return: ``True`` after all removals, including an empty input.
        """

    @abc.abstractmethod
    def ensure_row_has_id(self, row_dict: dict[str, Any]) -> dict[str, Any]:
        """
        Copy a row dictionary and allocate a database row if its ID is absent or None.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Infers the table from all keys. Existing non-None IDs pass through without
        checking row existence. Otherwise get_blank_row() writes a new row and supplies
        its ID; the remaining supplied values are not written by this method. The input
        mapping is deep-copied.

        Example:
            >>> wrapper.ensure_row_has_id(
            ...     {"work_id": 1, "work_title": "Example"}
            ... )  # doctest: +SKIP


        :param row_dict: Plain mapping from column names to row values.
        :return: Deep copy of the supplied mapping with an identifier present.
        """

    @abc.abstractmethod
    def execute(
        self, sql: str, values: SQLParameters | int | None = None
    ) -> QueryCursor:
        """
        Execute one statement using a live primary connection and its transaction
        context.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_execute hook. The following backend details
        describe the shared SQL/SQLite implementation; backend errors propagate.

        Replace a missing/broken primary handle; convert an integer binding to a
        one-element text tuple. Execution failures become DatabaseDriverError. Finally
        attempt a cache refresh while preserving a usable handle, so TEMP objects remain
        available.

        Example:
            >>> wrapper.execute("SELECT 1")  # doctest: +SKIP


        :param sql: SQL text to execute, with placeholders when bindings are supplied.
        :param values: Optional bindings; a bare integer is converted to a one-element
            string tuple.
        :return: The backend query cursor; the caller must close it.
        """

    @abc.abstractmethod
    def executemany(
        self, sql: str, values: Iterable[SQLParameters] | None = None
    ) -> None:
        """
        Execute repeated bindings or a legacy SQL script, depending on values.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        When values is None, forwards sql to direct_executescript(); otherwise calls
        direct_executemany(sql, values). Only ValueError is caught, logged with arguments and
        re-raised with added context; other errors propagate. Transaction behavior comes
        from the selected backend operation.

        Example:
            >>> wrapper.executemany("SELECT 1")  # doctest: +SKIP


        :param sql: SQL statement text for repeated bindings, or script text when values
            is None.
        :param values: Iterable of binding sequences/mappings, or None to select script
            execution.
        :return: Result of the selected driver helper; None for both script execution
            and repeated bindings in the shared SQL implementation.
        """

    @abc.abstractmethod
    def executescript(self, sqlscript: str) -> None:
        """
        Run trusted multi-statement SQL on the primary connection and refresh caches.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_executescript hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Create a primary handle if absent, wrap execution failures as
        DatabaseDriverError and attempt refresh in finally. Backend executescript
        transaction behavior still applies.

        Example:
            >>> wrapper.executescript(
            ...     "CREATE TABLE sample (value TEXT);"
            ... )  # doctest: +SKIP


        :param sqlscript: SQL script text passed to the backend executescript method.
        :return: ``None``.
        """

    @abc.abstractmethod
    def get(
        self, sql: str, values: SQLParameters | int | None = None, *, all: bool = True
    ) -> list[SQLRow] | SQLRow | None:
        """
        Execute SQL with optional bindings and fetch all rows or the first row.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        The all keyword defaults to True. With all=False, return the complete first
        row, or None for no match. Always close the cursor after fetching, including
        on errors. Transaction ownership remains that of execute().

        Example:
            >>> wrapper.get("SELECT 1", all=False)  # doctest: +SKIP


        :param sql: SQL statement passed to execute().
        :param values: Optional bound parameters.
        :param all: Whether to fetch all rows or just the first row.
        :return: List of rows by default; a single row or None when all=False.
        """

    @abc.abstractmethod
    def get_all_hashes(self) -> set[str]:
        """
        Union non-null values from recognized hash columns across supported tables.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_get_all_hashes hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Skip tables whose headings cannot be read; value-query failures propagate. Empty
        strings and other non-null values are retained.

        Example:
            >>> wrapper.get_all_hashes()  # doctest: +SKIP


        :return: A set of discovered non-null hash values.
        """

    @abc.abstractmethod
    def get_all_rows(
        self, table: str, sort_column: str | None = None, reverse: bool = False
    ) -> list[dict[str, Any]]:
        """
        Load every row into memory, optionally ordering by a validated column.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_get_all_rows hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Close the query connection after successful iteration. Use only when
        materializing the whole table is acceptable.

        Example:
            >>> wrapper.get_all_rows("works")  # doctest: +SKIP


        :param table: Table name resolved by the host driver.
        :param sort_column: Optional column belonging to the table; ``None`` adds no
            ORDER BY.
        :param reverse: Use descending order when a sort column is supplied; otherwise
            ignored.
        :return: A list of converted row dictionaries.
        """

    @abc.abstractmethod
    def get_blank_row(self, table: str) -> dict[str, Any]:
        """
        Insert a minimally populated row and read it back by a unique scratch token.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Stringifies table and rejects views with InputIntegrityError. Requires a scratch
        column. For books, reserves a titles row first and reuses its ID; for
        asset_replicas, seeds a nonempty blank/<token> storage key. Zero or multiple
        scratch matches raise DatabaseIntegrityError after insertion. Clears the scratch
        value only in the returned mapping: the stored token remains until a later
        update. Backend defaults and constraints still apply, and earlier writes are not
        rolled back here.

        Example:
            >>> wrapper.get_blank_row("works")  # doctest: +SKIP


        :param table: Table name in the current schema.
        :return: Inserted row dictionary with its ID and an empty scratch value.
        """

    @abc.abstractmethod
    def get_column_base(self, table_name: str) -> str:
        """
        Use the shared plural/singular mapper to obtain a column prefix.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_get_column_base hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Example:
            >>> wrapper.get_column_base("works")  # doctest: +SKIP


        :param table_name: Table name passed unchanged to the shared mapper.
        :return: Canonical singular prefix.
        """

    @abc.abstractmethod
    def get_column_headings(self, table: str) -> list[str]:
        """
        Look up cached column names after canonicalizing the table identifier.

        Delegates to the driver's direct_get_column_headings hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Check the combined cache's schema version before using it. Unknown tables
        raise InputIntegrityError; inspection preserves the primary transaction.

        Example:
            >>> wrapper.get_column_headings("works")  # doctest: +SKIP


        :param table: Table or view name used for schema introspection.
        :return: The cached column-name list.
        """

    @abc.abstractmethod
    def get_allowed_link_types(
        self,
        link_spec: StorageLinkSpec,
        *,
        force_refresh: bool = False,
    ) -> tuple[str, ...] | None:
        """
        Read the live type values from a link specification's optional registry.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Requires StorageLinkSpec, otherwise TypeError. With no registry declaration
        return None. A declared registry must exist and expose type, one non-ID _type
        column, or a sole remaining non-ID column, in that order. Missing/ambiguous
        columns and non-string or blank values raise DatabaseIntegrityError. NULL values
        are ignored, duplicates are removed, and remaining strings retain the backend
        sort order without trimming. Values are read on every call.

        Example:
            >>> wrapper.get_allowed_link_types(link_spec)  # doctest: +SKIP


        :param link_spec: Link specification whose allowed_types_table is to be read.
        :param force_refresh: Whether to refresh table discovery, also clearing derived
            wrapper caches.
        :return: Tuple of distinct type strings; empty tuple for an empty registry, or
            None without one.
        """

    # Todo: This needs to go. No direct connections if we can help it.
    @abc.abstractmethod
    def get_connection(self) -> QueryConnection:
        """
        Request a connection from the driver for caller-managed use.

        Supported drivers return a new connection distinct from driver.conn; callers own
        cleanup of additional connections. Backend configuration and connection errors
        propagate.

        Example:
            >>> wrapper.get_connection()  # doctest: +SKIP


        :return: Backend connection returned without modification.
        """

    @abc.abstractmethod
    def get_datestamp_column(self, table: str) -> str:
        """
        Choose literal datestamp, otherwise the shortest recognized timestamp heading.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_get_datestamp_column hook. The following
        backend details describe the shared SQL/SQLite implementation; backend errors
        propagate.

        Recognize _datestamp, _timestamp and their _ep_k variants. Unknown tables or
        absent candidates raise InputIntegrityError; ties preserve schema order.

        Example:
            >>> wrapper.get_datestamp_column("works")  # doctest: +SKIP


        :param table: Existing table name used for schema lookup.
        :return: The selected timestamp column name.
        """

    @abc.abstractmethod
    def get_dirtied_count(self) -> int:
        """
        Return the approximate size of the configured dirty-record queue.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        The parent database must have assigned dirty_records_queue. qsize() is an
        observation, not a synchronization guarantee.

        Example:
            >>> wrapper.get_dirtied_count()  # doctest: +SKIP


        :return: Queue-reported item count.
        """

    @abc.abstractmethod
    def get_display_column(self, table_name: str) -> str:
        """
        Choose the shortest non-ID column from a copied list of table headings.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Removes the conventional ID column, sorts the remaining list by length and takes
        its first entry; ties preserve the original list order. A missing ID or no
        remaining columns raises DatabaseIntegrityError. Unknown tables raise lookup
        errors. Requires a mutable heading list supporting remove() and sort(); a set
        does not satisfy this implementation despite broader schema annotations.

        Example:
            >>> wrapper.get_display_column("works")  # doctest: +SKIP


        :param table_name: Table name in the current schema.
        :return: Shortest remaining column heading.
        """

    @abc.abstractmethod
    def get_highest_id(self, target_table: str) -> int | None:
        """
        Query the maximum ID value in a table, closing the connection after its result.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_get_highest_id hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Example:
            >>> wrapper.get_highest_id("works")  # doctest: +SKIP


        :param target_table: Existing table name used in the aggregate query.
        :return: The scalar maximum ID, or ``None`` for an empty result/table.
        """

    @abc.abstractmethod
    def get_id_column(self, table: str) -> str:
        """
        Choose literal id, otherwise the shortest heading ending in _id.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_get_id_column hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Length ties preserve schema order. Unknown tables or absent candidates raise
        InputIntegrityError; this does not inspect primary-key constraints.

        Example:
            >>> wrapper.get_id_column("works")  # doctest: +SKIP


        :param table: Existing table name used for schema lookup.
        :return: The selected ID column name.
        """

    @abc.abstractmethod
    def get_id_from_row(self, row_dict: dict[str, Any]) -> int | None:
        """
        Extract the conventional identifier from a row dictionary.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        First infers the table from row keys, then obtains that table's ID heading.
        Table-resolution errors propagate; the value is not coerced to int.

        Example:
            >>> wrapper.get_id_from_row(
            ...     {"work_id": 1, "work_title": "Example"}
            ... )  # doctest: +SKIP


        :param row_dict: Plain mapping from column names to row values.
        :return: Stored ID value, or None if the ID key is absent or its value is None.
        """

    @abc.abstractmethod
    def get_interlink_column(self, table1: str, table2: str, column_type: str) -> str:
        """
        Forward interlink-column lookup to get_link_column().

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Preserves endpoint order and the requested suffix. Missing links and missing
        columns raise the errors from the canonical helper.

        Example:
            >>> wrapper.get_interlink_column(
            ...     "agents", "works", "type"
            ... )  # doctest: +SKIP


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :param column_type: Suffix of the requested link column, such as type, priority
            or an endpoint ID name.
        :return: Resolved physical column name.
        """

    @abc.abstractmethod
    def get_interlinked_tables(self, table_name: str) -> set[str]:
        """
        Find main tables linked to the named table by discovered interlink tables.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Tests conventional link names against the interlink-table group. Intralinks are
        excluded; no rows or foreign-key contents are inspected.

        Example:
            >>> wrapper.get_interlinked_tables("works")  # doctest: +SKIP


        :param table_name: Table name in the current schema.
        :return: Set of linked main-table names.
        """

    @abc.abstractmethod
    def get_intralink_column(self, table: str, column_type: str) -> str:
        """
        Resolve a self-link column by passing the same table as both endpoints.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to get_link_column(); missing tables or columns raise its lookup
        errors. Use primary_id and secondary_id to distinguish endpoint references.

        Example:
            >>> wrapper.get_intralink_column("works", "type")  # doctest: +SKIP


        :param table: Table name in the current schema.
        :param column_type: Suffix of the requested link column, such as type, priority
            or an endpoint ID name.
        :return: Resolved intralink-column name.
        """

    @abc.abstractmethod
    def get_linear_row_list(self, start_row: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Follow parent references and return the chain from ancestor to starting row.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Infers table and parent heading from the starting mapping. Missing parent keys,
        None and case-insensitive text none terminate the chain; the starting mapping is
        retained as-is. Parent rows are fetched through get_row_from_id(). Repeated
        parents, empty input and missing ancestors raise InputIntegrityError; a
        table without parents returns the start row. No siblings are traversed.

        Example:
            >>> wrapper.get_linear_row_list(series_row)  # doctest: +SKIP


        :param start_row: Starting row dictionary containing its identifier and any
            required parent value.
        :return: List of row dictionaries ordered from highest reached ancestor to
            start_row.
        """

    @abc.abstractmethod
    def get_link_column(self, table1: str, table2: str, column_type: str) -> str:
        """
        Resolve and validate a conventional column in the endpoint link table.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Requires get_link_table_name() to find a table, otherwise raises
        InputIntegrityError. Appends the stringified column_type to that table's column
        base. A heading absent from the link table raises DatabaseIntegrityError; no
        False sentinel is returned.

        Example:
            >>> wrapper.get_link_column("agents", "works", "type")  # doctest: +SKIP


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :param column_type: Suffix of the requested link column, such as type, priority
            or an endpoint ID name.
        :return: Existing physical link-column name.
        """

    @abc.abstractmethod
    def get_link_table_name(self, table1: str, table2: str) -> str | Literal[False]:
        """
        Find and optionally cache the conventional table linking two endpoint tables.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Stringifies names; distinct endpoints use sorted singular bases and _links,
        equal endpoints use a repeated base and _intralinks. Returns False if the
        generated table is absent. Cache keys are symmetric and include misses. When
        available, backend schema_version changes clear the cache. Version-query
        errors propagate without replacing the previous cache version.

        Example:
            >>> wrapper.get_link_table_name("agents", "works")  # doctest: +SKIP


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :return: Existing conventional link-table name, or False.
        """

    @abc.abstractmethod
    def get_parent_column(self, table_name: str) -> str | Literal[False] | None:
        """Delegate discovery of legacy *_parent and current *_parent_id columns.

        Prefer a sole *_parent_id when both forms exist. Ambiguity raises
        DatabaseIntegrityError; absence returns False.
        """

    @abc.abstractmethod
    def get_random_row(
        self, table: str, direct_access: bool = False
    ) -> dict[str, Any] | None:
        """
        Pick a random row using SQLite RANDOM or rejection sampling over positive IDs.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_get_random_row_dict hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        The default repeatedly samples IDs from 1 to the maximum and reseeds Python's
        global RNG. Sparse IDs can be slow; tables without positive integer IDs are
        unsuitable. A non-convertible null maximum returns None.

        Example:
            >>> wrapper.get_random_row("works")  # doctest: +SKIP


        :param table: Existing table name to query.
        :param direct_access: Use ORDER BY RANDOM rather than retrying random positive
            IDs.
        :return: A converted row dictionary, or ``None`` for an empty table.
        """

    @abc.abstractmethod
    def get_record_count(self, target_table: str) -> int:
        """
        Validate a table name and count its rows using a fresh connection.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_get_record_count hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Close the connection after reading the count; an invalid table raises
        InputIntegrityError.

        Example:
            >>> wrapper.get_record_count("works")  # doctest: +SKIP


        :param target_table: Existing table name used in the aggregate query.
        :return: The raw COUNT result, normally an integer.
        """

    @abc.abstractmethod
    def get_relation_type(self, name: str) -> str | None:
        """Return table/view/None through the backend's read-only lookup.

        Inspection never commits, rolls back or replaces the primary connection.
        Backend errors propagate rather than being reported as an absent relation.
        """

    @abc.abstractmethod
    def get_row_from_id(
        self, table: str, row_id: int
    ) -> dict[str, Any] | Literal[False]:
        """
        Bind a text-coerced ID and require at most one matching row.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_get_row_dict_from_id hook. The following
        backend details describe the shared SQL/SQLite implementation; backend errors
        propagate.

        Multiple matches raise DatabaseIntegrityError; SQLite InterfaceError becomes
        DatabaseDriverError. Normal found/missing results close the connection.

        Example:
            >>> wrapper.get_row_from_id("works", 1)  # doctest: +SKIP


        :param table: Table name resolved by the host driver.
        :param row_id: ID converted to Unicode before binding to the query.
        :return: The converted row dictionary, or ``False`` if absent.
        """

    @abc.abstractmethod
    def get_scratch_column(self, table: str) -> str:
        """
        Find the first table heading ending with the case-sensitive suffix scratch.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Raises DatabaseIntegrityError when no matching column exists; ties use heading
        order.

        Example:
            >>> wrapper.get_scratch_column("works")  # doctest: +SKIP


        :param table: Table name in the current schema.
        :return: First matching scratch-column name.
        """

    @abc.abstractmethod
    def get_tables(self, force_refresh: bool = False) -> list[str]:
        """
        Return cached table/view names, rebuilding when a known schema version changes.

        Delegates to the driver's direct_get_tables hook. The following backend details
        describe the shared SQL/SQLite implementation; backend errors propagate.

        Suppress an ``_v`` name when its unsuffixed counterpart exists. The returned
        list is the cache itself; refresh preserves the primary connection and its
        pending transaction.

        A refresh first clears derived wrapper schema caches when that hook exists, then
        refreshes backend table discovery.

        Example:
            >>> wrapper.get_tables()  # doctest: +SKIP


        :param force_refresh: Invalidate cached schema data before introspection.
        :return: The mutable list of visible table and view names.
        """

    @abc.abstractmethod
    def get_tables_and_columns(self) -> dict[str, list[str]]:
        """
        Build or reuse the table/view-to-column-list cache with schema-version checks.

        Delegates to the driver's direct_get_tables_and_columns hook. The following
        backend details describe the shared SQL/SQLite implementation; backend errors
        propagate.

        Read schema metadata without replacing the primary connection or ending its
        transaction. Return the cache object directly.

        Example:
            >>> wrapper.get_tables_and_columns()  # doctest: +SKIP


        :return: The cached mapping from visible table/view names to ordered column
            lists.
        """

    @abc.abstractmethod
    def get_triggers(self) -> list[str]:
        """
        Read trigger names from sqlite_master and close on success or OperationalError.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_get_triggers hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        TEMP triggers are not included and no ordering is specified.

        Example:
            >>> wrapper.get_triggers()  # doctest: +SKIP


        :return: A list of trigger names.
        """

    @abc.abstractmethod
    def get_uuid(self) -> str | None:
        """
        Read the database identity from its sole metadata row.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_get_db_unique_id hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Multiple rows raise DatabaseIntegrityError. Reuse the primary connection when
        available, otherwise close the temporary handle in finally.

        Example:
            >>> wrapper.get_uuid()  # doctest: +SKIP


        :return: The stored identity value, or ``None`` when there are no rows or the
            value is null.
        """

    @abc.abstractmethod
    def get_view_column_headings(self, view: str) -> list[str]:
        """
        Read relation column names in the order returned by PRAGMA TABLE_INFO.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_get_view_column_headings hook. The following
        backend details describe the shared SQL/SQLite implementation; backend errors
        propagate.

        Quote the relation name and inspect the primary connection, preserving
        pending schema changes and the caller's transaction. Tables also work; an
        unknown relation normally yields an empty list. Always close the cursor
        and any temporary connection opened when no primary exists.

        Example:
            >>> wrapper.get_view_column_headings("sample_view")  # doctest: +SKIP


        :param view: Relation name; no check distinguishes tables from views.
        :return: A fresh list of column names, possibly empty.
        """

    # Todo: A ViewRowAPI
    @abc.abstractmethod
    def get_view_row_from_id(
        self, view: str, row_id: int, *, connection: QueryConnection | None = None
    ) -> dict[str, Any] | None:
        """Read at most one view row by its literal id column, on one connection.

        Quote the relation name and inspect/convert it on the same schema snapshot.
        Fetch at most two rows; duplicates raise DatabaseIntegrityError, missing
        matches return None. Always close cursors and only owned connections.
        Borrowed writes and transaction ownership remain with the caller.
        """

    # Todo: Just error - don't given an option
    @abc.abstractmethod
    def identify_table_from_column(
        self, column_heading: str, error: bool = True
    ) -> str | None:
        """
        Return the first discovered table containing a column heading.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Copies and stringifies the heading before searching. Ambiguous columns resolve
        to the first table in discovery order. If no table matches, logs the miss and
        either raises InputIntegrityError or returns None.

        Example:
            >>> wrapper.identify_table_from_column("work_title")  # doctest: +SKIP


        :param column_heading: Column name to locate in discovered table headings.
        :param error: Whether a missing column should raise InputIntegrityError.
        :return: First matching table name, or None when missing and error is False.
        """

    @abc.abstractmethod
    def identify_table_from_row_dict(
        self, row_dict: dict[str, Any]
    ) -> str | Literal[False]:
        """
        Find the unique table containing every key in a row dictionary.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        An empty mapping returns False. A Row object raises NotImplementedError.
        Multiple matches or no matching table raise DatabaseIntegrityError; values are
        not examined. Uses discovered table headings, so naming collisions can make a
        partial row ambiguous.

        Example:
            >>> wrapper.identify_table_from_row_dict(
            ...     {"work_id": 1, "work_title": "Example"}
            ... )  # doctest: +SKIP


        :param row_dict: Plain mapping from column names to row values.
        :return: Unique table name, or False for an empty mapping.
        """

    @abc.abstractmethod
    def is_view(self, name: str) -> bool:
        """
        Check whether relation discovery reports an SQLite view.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Concrete DriverWrapper compares get_relation_type(name) with the exact string
        view. Unknown objects give False; backend lookup errors propagate.

        Example:
            >>> wrapper.is_view("sample_view")  # doctest: +SKIP


        :param name: Schema object name to inspect.
        :return: True only when relation discovery reports view.
        """

    @abc.abstractmethod
    def link_main_tables(
        self,
        primary_table: str,
        secondary_table: str,
        link_type: Literal["many_many", "many_one", "one_many", "one_one"],
        link_properties: Iterable[str] | None = None,
    ) -> None:
        """
        Create an interlink table joining two existing main tables.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Forwards cardinality and requested link columns to the driver and discards its
        result. Schema validation, SQL execution and commits belong to the backend;
        derived wrapper caches are not explicitly invalidated here.

        Example:
            >>> wrapper.link_main_tables(
            ...     "agents", "works", "many_many"
            ... )  # doctest: +SKIP


        :param primary_table: Primary endpoint table.
        :param secondary_table: Secondary endpoint table.
        :param link_type: Cardinality: one_one, many_one, one_many or many_many.
        :param link_properties: Optional link-column suffixes forwarded as
            requested_cols.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def macros(self) -> DatabaseMacrosAPI:
        """
        Return the macro provider currently stored on the wrapper.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Access before initialization can raise AttributeError.

        Example:
            >>> wrapper.macros  # doctest: +SKIP


        :return: The stored _macros object, without copying.
        """

    # Todo: Check if it's it's null value, or NULL
    @abc.abstractmethod
    def nullify_column(self, table: str, row_id: int, column: str) -> bool:
        """
        Set a cell to None through the wrapper's single-column update path.

        Uses update_column(), including its column/table check, lock context and
        targeted, normalization-aware write. Does not substitute schema defaults or a
        column-policy empty value.

        Example:
            >>> wrapper.nullify_column("works", 1, "work_title")  # doctest: +SKIP


        :param table: Table name in the current schema.
        :param row_id: Identifier of the target row.
        :param column: Column name in the selected table.
        :return: Result of update_column(), normally True.
        """

    @abc.abstractmethod
    def read_metadata(self, field: str) -> Any:
        """
        Read a validated metadata field, creating the placeholder row if needed.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_read_metadata hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        SQL null and values whose lowercase string is ``none`` return None. Other values
        are deep-copied. Unknown fields raise ValueError.

        Example:
            >>> wrapper.read_metadata("scratch")  # doctest: +SKIP


        :param field: Metadata column name, with or without the ``database_metadata_``
            prefix.
        :return: The copied field value, or ``None`` for null/none sentinels.
        """

    @abc.abstractmethod
    def search(self, table: str, column: str, search_term: Any) -> list[dict[str, Any]]:
        """
        Find exact matches for a bound text search term in a validated column.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_search_table hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        The table must validate and the column must be a known simple identifier. The
        wrapper requires table explicitly, and the shared backend does not infer it.
        Malformed requests and SQLite operational errors raise InputIntegrityError.

        Example:
            >>> wrapper.search("works", "work_title", "Example")  # doctest: +SKIP


        :param table: Table name resolved by the host driver.
        :param column: Column name to read or match.
        :param search_term: Non-null value coerced to text; byte-like inputs must be
            UTF-8.
        :return: Converted matching rows, or an empty list.
        """

    @abc.abstractmethod
    def set_custom_column_metadata(
        self,
        num: int,
        name: str | None = None,
        label: str | None = None,
        is_editable: bool | None = None,
        display: Mapping[str, Any] | None = None,
        in_table: str | None = None,
    ) -> bool:
        """Atomically update definition fields, preserving omitted fields.

        Attachment changes require a storage migration and are rejected here.
        The macro joins the current transaction without committing unrelated
        writes. The caller remains responsible for facade metadata refresh.
        """

    @abc.abstractmethod
    def set_full_column(self, table: str) -> bool:
        """
        Write ancestor display paths for positive-ID rows, committing each row
        separately.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_set_full_column hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Require the host's ``get_full_column_name`` helper and the aggregation helper's
        dependencies. A missing full column raises InputIntegrityError; SQLite
        operational failures become DatabaseDriverError. Sentinel rows are skipped and
        the acquired write connection is not explicitly closed.

        Example:
            >>> wrapper.set_full_column("series")  # doctest: +SKIP


        :param table: Existing table whose derived column is required.
        :return: ``True`` after all selected rows have been processed.
        """

    @abc.abstractmethod
    def set_macros(self, new_macros: DatabaseMacrosAPI) -> None:
        """
        Store a non-None macro provider on this wrapper.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Raises AssertionError for None while assertions are enabled. This only updates
        the wrapper reference; it does not modify driver or database macro references.

        Example:
            >>> wrapper.set_macros(macros)  # doctest: +SKIP


        :param new_macros: Macro provider to retain as _macros.
        :return: None.
        """

    @abc.abstractmethod
    def set_tree_ids(self, table: str) -> bool:
        """
        Write each positive-ID row's root ID and display value as a tree identifier.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_set_tree_ids hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Use ``<root_id>_<root_display>``; an existing ID-zero sentinel receives
        ``0_<display>`` separately. Commit each update, with no atomic batch or explicit
        connection close. Missing tree-ID columns raise InputIntegrityError.

        Example:
            >>> wrapper.set_tree_ids("series")  # doctest: +SKIP


        :param table: Existing table name used for schema lookup.
        :return: ``True`` after the updates complete.
        """

    @abc.abstractmethod
    def set_uuid(self, new_force_value: str | None = None) -> bool:
        """
        Write a supplied identity or new UUID4, commit, and verify it by rereading.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_set_db_unique_id hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        A non-null current identity updates row ID 1 without prompting. A null/missing
        identity takes the insert branch, so a pre-existing null-valued row can produce
        duplicate metadata rows. Verification failures raise DatabaseIntegrityError
        after the write commits.

        Example:
            >>> wrapper.set_uuid()  # doctest: +SKIP


        :param new_force_value: Identity value to write; ``None`` generates a UUID4
            string.
        :return: ``True`` when rereading matches the requested value.
        """

    @abc.abstractmethod
    def shell(self) -> None:
        """
        Enter the interactive shell supplied by the active driver.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Returns the backend result unchanged. The stdlib SQLite backend raises
        NotImplementedError; shell availability and interactive I/O depend on the
        backend.

        Example:
            >>> wrapper.shell()  # doctest: +SKIP


        :return: Backend shell result, if the call returns.
        """

    # Todo: I thiiinkk we know that it's string or number for all database entries
    @abc.abstractmethod
    def update_column(
        self, table: str, row_id: int, column: str, new_value: Any
    ) -> bool:
        """
        Update one cell and its derived keys without replaying unrelated columns.

        First resolves column ownership; mismatching table names raise
        InputIntegrityError. Within self.lock, checks that the row exists and submits
        only the requested field through the normalization-aware batch writer.
        Missing rows raise InputIntegrityError; backend write errors propagate.
        The reentrant mutex serializes cooperating callers on this wrapper;
        the row writer owns its SQL transaction. The mutex alone does not group multiple
        writes into one transaction or coordinate separate database facades.

        Example:
            >>> wrapper.update_column(
            ...     "works", 1, "work_title", "Revised"
            ... )  # doctest: +SKIP


        :param table: Table name in the current schema.
        :param row_id: Identifier of the target row.
        :param column: Column name in the selected table.
        :param new_value: Replacement cell value; None requests SQL NULL through the row
            writer.
        :return: True after the read/update path completes, not an affected-row count.
        """

    @abc.abstractmethod
    def update_columns(
        self,
        values_map: dict[int, Any],
        field: str | None = None,
        table: str | None = None,
    ) -> bool | None:
        """
        Update one field for each ID as an atomic batch, preserving derived keys.

        In shared SQL, infer the table from the field; a conflicting table hint warns
        and the inferred table wins. Empty mappings are no-ops and dictionary-valued
        updates remain unsupported. A failed update rolls back earlier rows in the
        batch. Any owned SQLite connection is closed; PostgreSQL retains its primary
        connection.

        Example:
            Updating tag_phash alone rederives each key from the stored tag text.

        :param values_map: Mapping from row IDs to replacement scalar values.
        :param field: Column to update.
        :param table: Optional expected table name; PostgreSQL uses it as the table
            hint.
        :return: None for shared SQL; True for PostgreSQL.
        """

    # Todo: Where we say "num" in ref to custom columns, change it to cc_id or similar
    @abc.abstractmethod
    def update_custom_column(
        self, in_table: str, cc_name: str, value: Any, extra: str | None = None
    ) -> Never:
        """
        Reject the unfinished custom-column value update operation.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Always raises NotImplementedError before reading the database or modifying
        values.

        The concrete wrapper also accepts an optional extra argument, but remains
        unimplemented.

        Example:
            >>> wrapper.update_custom_column(
            ...     "works", "note", "Example"
            ... )  # doctest: +SKIP


        :param in_table: Table to which the custom column is attached.
        :param cc_name: Custom column name.
        :param value: Proposed custom value; currently unused.
        :return: No normal return; always raises NotImplementedError.
        """

        ...

    @abc.abstractmethod
    def update_row(self, row_dict: dict[str, Any]) -> bool | None:
        """
        Update the supplied columns of an identified row without mutating the input.

        Literal text "None" stays text; Python None requests SQL NULL. Derived-key-only
        updates read the omitted display value on the write connection and recompute the
        key. A missing ID raises an input/row integrity error. Shared SQL commits only
        on success, rolls back errors and always closes its owned connection; PostgreSQL
        retains its primary connection. Backend exceptions propagate.

        Example:
            Updating only tag_phash recomputes it from the stored tag text.

        :param row_dict: Column/value mapping including the row ID and optional table
            hint.
        :return: True for an ID-only no-op; shared SQL otherwise returns None,
            PostgreSQL True.
        """

    @property
    @abc.abstractmethod
    def user_version(self) -> int | str | None:
        """
        Return the driver's application-controlled user_version value.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        This property simply forwards driver.user_version; it does not query or modify
        SQLite schema_version. SQLite backends return an integer; other backends
        may return text or None.

        Example:
            >>> wrapper.user_version  # doctest: +SKIP


        :return: Unmodified driver user_version value.
        """

    @abc.abstractmethod
    def walk(self, start_row: dict[str, Any]) -> Iterator[dict[str, Any]]:
        """
        Resolve a row's tree columns and return an iterator over its descendants.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Infers the table and ID heading. A missing parent heading (None or False) raises
        InputIntegrityError before iteration. Returns _walk() without consuming it;
        traversal starts with the supplied row. Sibling order is unspecified.
        A cycle or repeated descendant raises InputIntegrityError during iteration.

        Example:
            >>> wrapper.walk(series_row)  # doctest: +SKIP


        :param start_row: Starting row dictionary containing its identifier and any
            required parent value.
        :return: Iterator of row dictionaries beginning with start_row.
        """

    @abc.abstractmethod
    def write_metadata(self, field: str, value: Any) -> None:
        """
        Validate a metadata field, initialize the sole row and update its value.

        Abstract wrapper hook. The behavior below describes DriverWrapper and its
        mixins; this declaration supplies no implementation.

        Delegates to the driver's direct_write_metadata hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Unknown fields raise ValueError; multiple metadata rows raise
        DatabaseIntegrityError. Persistence is delegated to the row update helper.

        Example:
            >>> wrapper.write_metadata("scratch", "reviewed")  # doctest: +SKIP


        :param field: Metadata column name, with or without the ``database_metadata_``
            prefix.
        :param value: Value assigned to the validated metadata column.
        :return: ``None``.
        """
