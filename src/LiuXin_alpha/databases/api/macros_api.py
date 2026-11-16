"""
Declare the legacy SQL macro surface alongside inherited portable macros.

MacrosAPI is an abstract contract, not an executable fallback. Method notes describe the
current SQLite macro implementation, including legacy return shapes and
connection/commit differences. Identifiers and SQL fragments generally come from trusted
schema metadata; most legacy methods interpolate them without validation. Use
PortableMacrosAPI for the backend-neutral record-based operations.
"""

from __future__ import annotations

import abc
from collections.abc import Callable, Iterable, Mapping, Sequence
from LiuXin_alpha.databases.db_types.driver_types import (
    SQLConnection,
    SQLCursor,
    SQLParameters,
    QueryCursor,
)
from typing import Any, Literal, overload

from LiuXin_alpha.databases.api.portable_macros_api import DatabaseMacrosAPI
from LiuXin_alpha.databases.db_types.macro_types import LegacyLinkData
from LiuXin_alpha.databases.db_types.driver_types import (
    QueryConnection,
)


class MacrosAPI(DatabaseMacrosAPI, abc.ABC):
    """
    Specify database-bound SQL convenience operations.

    Inherit portable link/identity/transaction operations and declare the older macro
    methods expected by SQLiteDatabaseMacros. Implementations must provide abstract
    methods and callable properties. Results distinguish nullable scalars from
    row lists and typed link groups. No shared transaction or cache-invalidation
    guarantee applies to every method; consult each operation’s concrete behavior.

    Example:
        macros = require_legacy_macros(db.macros)
        linked = macros.get_link_data("works", "agents", work_id)
    """










    @abc.abstractmethod
    def bulk_add_links(
        self,
        link_table: str,
        src_col: str,
        dst_col: str,
        values: Iterable[tuple[int, int]],
    ) -> None:
        """
        Insert endpoint pairs and assign increasing priorities when the schema has them.

        Discover the conventional priority column from the table stem. If present, start
        each source at its stored maximum or zero and increment once per input pair;
        otherwise insert only endpoints. Existing duplicates are not removed and
        allocation/insertion has no enclosing transaction here.

        Example:
            macros.bulk_add_links(link_table, source_column, target_column, [(1, 10),
            (1, 11)])


        :param link_table: Link table inspected through driver_wrapper.
        :param src_col: Source endpoint column.
        :param dst_col: Destination endpoint column.
        :param values: Iterable of (source_id, destination_id) pairs, materialized once.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def bulk_delete_in_table(
        self, table: str, column: str, column_values: Iterable[Iterable[Any]]
    ) -> None:
        """Delete equality matches through the portable transaction owner.

        Validate the table, column and all one-cell bindings before writing.
        Preserve SQL equality semantics for None. A failing batch rolls back;
        nested operations compose inside the caller's portable transaction.
        """
        ...

    @abc.abstractmethod
    def bulk_delete_items_in_table_two_matching_cols(
        self, table: str, col_1: str, col_2: str, column_values: Iterable[Iterable[Any]]
    ) -> None:
        """Delete matching column pairs through the portable transaction owner.

        Validate both physical columns and every binding before effects. Bind
        values with equality predicates; None does not match SQL NULL. Batch
        failures roll back without committing an enclosing portable transaction.
        """
        ...

    @abc.abstractmethod
    def bulk_update_link_table(
        self,
        link_table: str,
        update_column: str,
        other_column: str,
        values: Iterable[tuple[Any, Any, Any]],
    ) -> None:
        """
        Replace a link column where its old value and a second column both match.

        Example:
            macros.bulk_update_link_table(link_table, value_column, owner_column, [(20,
            10, 1)])


        :param link_table: Trusted link table name.
        :param update_column: Column to update and compare with its old value.
        :param other_column: Additional equality column.
        :param values: Iterable of (new_value, old_value, other_column_value) triples.
        :return: None; the abstract method supplies no implementation.
        """

        ...






    @abc.abstractmethod
    def create_temp_id_tables(
        self, temp_tables: Iterable[str], conn: SQLConnection | None = None
    ) -> None:
        """
        Replace named temporary ID tables on the chosen connection.

        Drop only temp-schema objects, then create each as id INTEGER PRIMARY KEY.
        Existing main-schema tables of the same name are preserved. Uses executescript;
        do not assume it joins a caller transaction unchanged.

        Example:
            macros.create_temp_id_tables(("selected_ids",), conn=conn)


        :param temp_tables: Iterable of temporary table identifiers validated before
            executing the script.
        :param conn: Optional project SQLite-compatible connection; None selects the
            implementation’s default connection.
        :return: None; the abstract method supplies no implementation.
        """

        ...




    @abc.abstractmethod
    def delete_in_table(self, table: str, column: str, value: Any) -> None:
        """Delete equality matches through the validated portable row primitive.

        The selected column need not be unique. No match is a no-op, and None
        does not match SQL NULL. Reuse the caller's portable transaction.
        """
        ...

    @abc.abstractmethod
    def destroy_temp_tables(
        self, temp_tables: Iterable[str], conn: SQLConnection | None = None
    ) -> None:
        """
        Drop named temporary tables without touching namesakes in the main schema.

        Use DROP TABLE IF EXISTS temp.name in an executescript call. The method does not
        close the connection.

        Example:
            macros.destroy_temp_tables(("selected_ids",), conn=conn)


        :param temp_tables: Iterable of validated temporary table identifiers.
        :param conn: Optional project SQLite-compatible connection; None selects the
            implementation’s default connection.
        :return: None; the abstract method supplies no implementation.
        """

        ...







    @abc.abstractmethod
    def generic_clean_update(
        self,
        link_table: str,
        link_col: str,
        value_for_clear: int | Iterable[SQLParameters],
    ) -> None:
        """
        Delete links selected by an integer or supplied batch bindings.

        Integers, including bool, use execute with a wrapped binding. All other inputs
        pass directly to executemany, so a plain list of integer IDs is not wrapped
        automatically.

        Example:
            macros.generic_clean_update(link_table, owner_column, [(1,), (2,)])


        :param link_table: Trusted link table name.
        :param link_col: Column compared with bound values.
        :param value_for_clear: Integer scalar, or iterable of one-item parameter
            sequences.
        :return: None; the abstract method supplies no implementation.
        """

        ...




    @abc.abstractmethod
    def get_all_table_link_data(
        self, table1: str, table2: str, typed: bool = False, priority: bool = False
    ) -> dict[int, LegacyLinkData]:
        """
        Read links for every primary row and shape them for legacy cache callers.

        Use portable bulk link retrieval. List order follows its link-row order; no
        numeric priority values are returned. Per-type containers are defaultdicts, and
        owners without links retain empty containers when included by the bulk result.

        Example:
            by_work = macros.get_all_table_link_data("works", "agents", typed=True,
            priority=True)


        :param table1: Primary table whose current row IDs are enumerated.
        :param table2: Secondary table resolved through the oriented link specification.
        :param typed: Whether to group secondary IDs by link_type.
        :param priority: Whether to retain returned link order in lists instead of sets.
        :return: Mapping from primary IDs to sets/lists, or per-type mappings of those
            containers; empty dict when no link spec exists.
        """

        ...









    # Todo: This seems to be an interface weirdness



    @abc.abstractmethod
    def get_foreign_key_replacement_trigger(
        self,
        target_table: str,
        search_column: str = "book",
        target_id: str = "book_id",
        old: bool = True,
    ) -> str:
        """
        Build a DELETE fragment referring to the triggering row’s OLD key.

        Even old=False emits OLD. Identifiers are interpolated directly.

        Example:
            statement = macros.get_foreign_key_replacement_trigger(link_table,
            owner_column, "book_id")


        :param target_table: Trusted table to delete from.
        :param search_column: Column in that table compared with the old row key.
        :param target_id: Column name referenced through OLD.
        :param old: Retained compatibility argument; ignored by the current
            implementation.
        :return: SQL DELETE statement text, not an installed trigger.
        """

        ...

    @abc.abstractmethod
    def get_linked_ids(
        self,
        link_table: str,
        left_id_col: str,
        right_id_col: str,
        left_id: int,
        type_filter: Any = None,
    ) -> set[int]:
        """
        Collect distinct right-endpoint IDs for one left endpoint.

        None cannot request a SQL NULL type filter here; portable link APIs distinguish
        omitted and NULL filters with LINK_TYPE_UNSET.

        Example:
            agent_ids = macros.get_linked_ids(link_table, work_column, agent_column,
            work_id, type_filter="author")


        :param link_table: Trusted link table.
        :param left_id_col: Left endpoint column used in the equality condition.
        :param right_id_col: Right endpoint column to select.
        :param left_id: Left endpoint ID.
        :param type_filter: Optional value for the conventional stem_type column; None
            disables filtering.
        :return: Set of right-endpoint IDs.
        """

        ...

    @abc.abstractmethod
    def get_unique_values(self, table: str, column: str) -> set[Any]:
        """
        Collect distinct stored values from one column using a Python set.

        The SELECT does not use DISTINCT; Python set insertion deduplicates values and
        requires hashable results.

        Example:
            values = macros.get_unique_values("tags", "tag")


        :param table: Trusted table name.
        :param column: Trusted column name.
        :return: Set of retrieved values, including None when present.
        """

        ...

    @abc.abstractmethod
    def get_values_one_condition[DefaultT](
        self,
        table: str,
        rtn_column: str,
        cond_column: str,
        value: Any,
        default_value: DefaultT | None = None,
    ) -> set[Any] | DefaultT | None:
        """
        Collect values matching one SQL equality condition.

        Other errors propagate. The fallback is not a substitute for an empty result.

        Example:
            names = macros.get_values_one_condition("tags", "tag", "tag_id", 7,
            default_value=set())


        :param table: Trusted table name.
        :param rtn_column: Column whose values are collected.
        :param cond_column: Column used in the equality condition.
        :param value: Bound condition value.
        :param default_value: Fallback returned only if execution, iteration or set
            insertion raises TypeError.
        :return: Set of matching values, including an empty set for no rows;
            default_value after a caught TypeError.
        """

        ...

    @abc.abstractmethod
    def hash_table(self, target_table: str, columns: Iterable[str]) -> str:
        """
        Compute the legacy MD5 form of the portable table fingerprint.

        Delegate to fingerprint_table with algorithm="md5". The default row ordering
        uses a discovered ID column or falls back to selected columns; column order
        affects the fingerprint. No table rows are modified.

        Example:
            before = macros.hash_table("tags", ("tag_id", "tag"))


        :param target_table: Table whose selected content is read.
        :param columns: Nonempty iterable of existing column names, materialized into a
            tuple.
        :return: MD5 hexadecimal digest of the table/column header and canonicalized
            selected rows.
        """

        ...


    @abc.abstractmethod
    def insert_values_into_temp_table(
        self, temp_table: str, values: Iterable[Any], conn: SQLConnection | None = None
    ) -> None:
        """
        Insert scalar IDs into a previously created temporary one-column table.

        Materialize bindings before executemany. The table must already exist on that
        same connection; this method neither clears it nor commits explicitly.

        Example:
            macros.insert_values_into_temp_table("selected_ids", [1, 2], conn=conn)


        :param temp_table: Validated temporary table name; always addressed through temp
            schema.
        :param values: Iterable of scalar values wrapped into one-item bindings.
        :param conn: Optional project SQLite-compatible connection; None selects the
            implementation’s default connection.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def make_generic_link(
        self,
        link_table: str,
        left_link_col: str,
        right_link_col: str,
        priority_col: str,
        left_id: int,
        right_id: int,
    ) -> None:
        """
        Insert an endpoint pair at one above the source’s current maximum priority.

        Use one INSERT SELECT statement, treating an empty group as priority zero before
        incrementing. Existing duplicate links and uniqueness constraints are left to
        the database.

        Example:
            macros.make_generic_link(link_table, left_column, right_column,
            priority_column, 1, 10)


        :param link_table: Trusted link table.
        :param left_link_col: Left/source endpoint column.
        :param right_link_col: Right/target endpoint column.
        :param priority_col: Priority column used both for MAX and insertion.
        :param left_id: Source ID whose priority group is inspected.
        :param right_id: Target ID to insert.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def make_generic_link_no_priority(
        self,
        link_table: str,
        left_link_col: str,
        right_link_col: str,
        left_id: int | None = None,
        right_id: int | None = None,
        id_pairs: Iterable[tuple[int, int]] | None = None,
    ) -> None:
        """
        Insert endpoint bindings using the legacy unprioritized column order.

        The implementation reverses the named columns without reversing supplied values.
        Callers must account for that existing behavior; no automatic orientation
        correction occurs.

        Example:
            macros.make_generic_link_no_priority(link_table, left_column, right_column,
            id_pairs=[(right_id, left_id)])


        :param link_table: Trusted link table.
        :param left_link_col: Column named as the left endpoint, emitted second in SQL.
        :param right_link_col: Column named as the right endpoint, emitted first in SQL.
        :param left_id: First bound value when id_pairs is None; currently written to
            right_link_col.
        :param right_id: Second bound value when id_pairs is None; currently written to
            left_link_col.
        :param id_pairs: Optional iterable of two-item bindings, also in emitted
            right/left column order.
        :return: None; the abstract method supplies no implementation.
        """

        ...






    @abc.abstractmethod
    def read_link_property_trios(
        self, link_table: str, link_property_col: str, first_id: str, second_id: str
    ) -> QueryCursor:
        """
        Execute a three-column read over every row of a link table.

        There is no filter or explicit ordering; the caller consumes the returned
        result.

        Example:
            rows = macros.read_link_property_trios(link_table, priority_column,
            left_column, right_column)


        :param link_table: Trusted link table name.
        :param link_property_col: Property column emitted first in each row.
        :param first_id: First endpoint column name, not an endpoint value.
        :param second_id: Second endpoint column name, not an endpoint value.
        :return: Host execute result yielding (property, first_id, second_id) rows.
        """

        ...

    @abc.abstractmethod
    def replace_in_folder_path(self, target_str: str, replacement: str) -> None:
        """
        Replace literal text throughout stored folder_path values.

        Execute an UPDATE over all rows of folders, using SQLite replace on folder_path.
        This is a database text change, not filesystem relocation or path-prefix
        validation; occurrences anywhere in each stored path are replaced.

        Example:
            macros.replace_in_folder_path("/old/root", "/new/root")


        :param target_str: Substring passed as the SQL replace search argument.
        :param replacement: Replacement substring passed as a bound argument.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def replace_in_folder_store_marker_path(
        self, target_str: str, replacement: str
    ) -> None:
        """
        Replace literal text throughout stored folder_store_marker_path values.

        Execute an UPDATE over all rows of folder_stores, using SQLite replace on
        folder_store_marker_path. This is a database text change, not filesystem
        relocation or path-prefix validation; occurrences anywhere in each stored path
        are replaced.

        Example:
            macros.replace_in_folder_store_marker_path("/old/root", "/new/root")


        :param target_str: Substring passed as the SQL replace search argument.
        :param replacement: Replacement substring passed as a bound argument.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def replace_in_folder_store_path(self, target_str: str, replacement: str) -> None:
        """
        Replace literal text throughout stored folder_store_path values.

        Execute an UPDATE over all rows of folder_stores, using SQLite replace on
        folder_store_path. This is a database text change, not filesystem relocation or
        path-prefix validation; occurrences anywhere in each stored path are replaced.

        Example:
            macros.replace_in_folder_store_path("/old/root", "/new/root")


        :param target_str: Substring passed as the SQL replace search argument.
        :param replacement: Replacement substring passed as a bound argument.
        :return: None; the abstract method supplies no implementation.
        """

        ...


    @abc.abstractmethod
    def reprioritize_link(
        self,
        link_table: str,
        left_link_col: str,
        right_link_col: str,
        left_id: int,
        right_id: int,
        new_type: Any = None,
        new_priority: Literal["MAX"] = "MAX",
    ) -> None:
        """
        Move matching endpoint links above the source’s current maximum priority.

        With new_type, perform the priority update first, then a separate type update.
        Existing type is not a matching condition, so multiple typed rows with the same
        endpoints may be affected. No transaction wraps the two updates here.

        Example:
            macros.reprioritize_link(link_table, left_column, right_column, 1, 10)


        :param link_table: Trusted link table with a conventional priority column.
        :param left_link_col: Source endpoint column.
        :param right_link_col: Target endpoint column.
        :param left_id: Source ID defining the priority group.
        :param right_id: Target ID selecting links to move.
        :param new_type: Optional non-None replacement type applied after the priority
            update.
        :param new_priority: Only the literal MAX mode is supported.
        :return: None; the abstract method supplies no implementation.
        :raises AssertionError: new_priority is not MAX when assertions are enabled.
        """

        ...


    @abc.abstractmethod
    def set_database_version(self, new_val: str) -> None:
        """
        Replace every stored database-version value, inserting one row if empty.

        Count rows first; a nonempty table is updated without a WHERE clause. This
        method does not migrate the schema or enforce a single-row invariant.

        Example:
            macros.set_database_version("2")


        :param new_val: Value written to database_version_version.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def set_library_id(self, new_val: str) -> None:
        """
        Replace every stored library UUID value, inserting one row if empty.

        A nonempty library_id table is updated without a WHERE clause. No UUID is
        generated automatically.

        Example:
            macros.set_library_id(library_uuid)


        :param new_val: Value written to library_id_uuid without format validation here.
        :return: None; the abstract method supplies no implementation.
        """

        ...



    @abc.abstractmethod
    def update_column_in_table(
        self, table: str, column: str, table_id_col: str, item_id: int, new_value: Any
    ) -> None:
        """
        Replace a row field through the database Row interface and sync it.

        Unlike direct_update_column_in_table, this resolves a Row, assigns its field,
        then calls sync. Missing-row failures and validation/persistence behavior belong
        to the Row implementation.

        Example:
            macros.update_column_in_table("tags", "tag", "tag_id", 7, "History")


        :param table: Table containing the target row.
        :param column: Row field to replace.
        :param table_id_col: Retained argument; current implementation does not use it.
        :param item_id: ID passed to db.get_row_from_id.
        :param new_value: Replacement value assigned to the row.
        :return: None; the abstract method supplies no implementation.
        """

        ...
