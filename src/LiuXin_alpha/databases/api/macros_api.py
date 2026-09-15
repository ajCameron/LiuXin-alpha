
"""
Declare the legacy SQL macro surface alongside inherited portable macros.

MacrosAPI is an abstract contract, not an executable fallback. Method notes describe the current SQLite macro implementation, including legacy return shapes and connection/commit differences. Identifiers and SQL fragments generally come from trusted schema metadata; most legacy methods interpolate them without validation. Use PortableMacrosAPI for the backend-neutral record-based operations.
"""

from __future__ import annotations

import abc
import sqlite3

from typing import Optional, Union, TYPE_CHECKING, Any, Iterable
import datetime

from LiuXin_alpha.databases.api.portable_macros_api import PortableMacrosAPI

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI


class MacrosAPI(PortableMacrosAPI, abc.ABC):
    """
    Specify database-bound SQL shortcuts and custom-column compatibility operations.

    Inherit portable link/identity/transaction operations and declare the older macro methods expected by SQLiteDatabaseMacros. Implementations must provide abstract methods and callable properties. Legacy annotations do not consistently describe nullable or row-shaped query results. No shared transaction or cache-invalidation guarantee applies to every method; consult each operation’s concrete behavior.

    Example:
        macros: MacrosAPI = db.macros
        linked = macros.get_link_data("works", "agents", work_id)
    """

    @abc.abstractmethod
    def __init__(self, db: "DatabaseAPI") -> None:
        """
        Bind a concrete macro provider to its database host.

        SQLiteDatabaseMacros delegates to MacrosBase, which stores db without opening an additional connection here.

        Example:
            macros = SQLiteDatabaseMacros(db)


        :param db: Database providing driver_wrapper, driver connections, row helpers and portable macro services.
        :return: None; the abstract method supplies no implementation.
        """

    @staticmethod
    @abc.abstractmethod
    def _cc_table_col_mapper(table: str) -> str:
        """
        Derive the conventional singular column stem for a custom table.

        This is a naming transformation, not schema introspection or identifier validation.

        Example:
            stem = macros._cc_table_col_mapper(custom_table)


        :param table: Custom value or link table name passed to the project inflector.
        :return: Singularized table name used as a column prefix.
        """

        ...

    @staticmethod
    @abc.abstractmethod
    def _get_cc_id_val(custom_column: str) -> tuple[str, str]:
        """
        Derive conventional custom-column ID and value column names.

        Example:
            id_column, value_column = macros._get_cc_id_val(custom_table)


        :param custom_column: Custom value-table name passed through the singular-name mapper.
        :return: A (stem_id, stem_value) pair; columns are not checked for existence.
        """

        ...

    @abc.abstractmethod
    def add_cc_link_with_extra(self, lt, book_id, value_id, extra=None, conn=None, target_column='value'):
        """
        Insert one conventional owner/value link with an optional extra value.

        Use the supplied connection without an explicit commit. When conn is omitted, write through db.driver.conn and commit it. A non-None extra requires an extra column; duplicate or invalid links propagate database errors.

        Example:
            macros.add_cc_link_with_extra(link_table, book_id, series_id, extra=2.5, conn=conn)


        :param lt: Custom link table whose singular stem names its columns.
        :param book_id: Owner ID bound to the stem_book column.
        :param value_id: Target ID bound to stem_target_column.
        :param extra: Optional value for stem_extra; None omits that column.
        :param conn: Optional project SQLite-compatible connection; None selects the implementation’s default connection.
        :param target_column: Suffix of the target column, normally value.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def add_cc_link_with_extra_multi(self, lt, sequence, extra=False, conn=None, target_column='value'):
        """
        Insert owner/value pairs or owner/value/extra triples in bulk.

        Commit the default driver connection when conn is omitted; do not explicitly commit a supplied connection. extra is a shape switch rather than a value applied to every row.

        Example:
            macros.add_cc_link_with_extra_multi(link_table, [(book_id, series_id, 2.5)], extra=True, conn=conn)


        :param lt: Custom link table with conventional prefixed columns.
        :param sequence: Iterable of two-item bindings, or three-item bindings when extra is true.
        :param extra: Whether each binding includes the stem_extra value.
        :param conn: Optional project SQLite-compatible connection; None selects the implementation’s default connection.
        :param target_column: Target-column suffix, normally value.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def add_cc_table_value(self, table, value, conn=None):
        """
        Insert one value into a conventional custom value table.

        With conn, return its cursor.lastrowid without explicitly committing. Otherwise call driver.direct_execute_sql, which obtains a connection and commits the insert. This does not reuse an existing value on uniqueness conflict.

        Example:
            value_id = macros.add_cc_table_value(custom_table, "Finished", conn=conn)


        :param table: Custom table whose value column is singular_stem_value.
        :param value: Value bound to the insert.
        :param conn: Optional project SQLite-compatible connection; None selects the implementation’s default connection.
        :return: Inserted row ID reported by the execution path.
        """

        ...







    @abc.abstractmethod
    def break_cc_links_by_book_id(self, lt, book_id, conn=None):
        """
        Delete conventional custom links for one owner or a supported owner collection.

        Single IDs use execute. Collections with conn become one-item binding tuples; the default path forwards a flat tuple to db.executemany or its driver fallback, so standard executemany adapters may reject it. No explicit commit occurs here.

        Example:
            macros.break_cc_links_by_book_id(link_table, [1, 2], conn=conn)


        :param lt: Custom link table.
        :param book_id: A string/integer owner ID, or tuple/list/set/generator of owner IDs.
        :param conn: Optional project SQLite-compatible connection; None selects the implementation’s default connection.
        :return: None; the abstract method supplies no implementation.
        :raises NotImplementedError: book_id has an unsupported input shape.
        """

        ...

    @abc.abstractmethod
    def break_cc_links_by_book_id_and_value(self, lt, book_id, value_id, conn=None):
        """
        Delete one owner/value pair through the default driver connection.

        The default path executes the deletion without an explicit commit.

        Example:
            macros.break_cc_links_by_book_id_and_value(link_table, book_id, value_id)


        :param lt: Custom link table.
        :param book_id: Owner ID matching stem_book.
        :param value_id: Value ID matching stem_value.
        :param conn: Must be None in the current SQLite implementation.
        :return: None; the abstract method supplies no implementation.
        :raises NotImplementedError: A connection is explicitly supplied.
        """

        ...

    @abc.abstractmethod
    def break_cc_lt_link(self, lt, book, value=None):
        """
        Delete all owner links or restrict deletion to a truthy value ID.

        The value branch uses truth testing, so 0 behaves like None. Delegate execution to the macro’s execute property without an additional commit here.

        Example:
            macros.break_cc_lt_link(link_table, book_id, value=value_id)


        :param lt: Custom link table.
        :param book: Owner ID bound to the conventional book column.
        :param value: Truthy value ID to match; any false value removes every link for this owner.
        :return: None; the abstract method supplies no implementation.
        """

        ...









    @abc.abstractmethod
    def bulk_add_links(self, link_table, src_col, dst_col, values):
        """
        Insert endpoint pairs and assign increasing priorities when the schema has them.

        Discover the conventional priority column from the table stem. If present, start each source at its stored maximum or zero and increment once per input pair; otherwise insert only endpoints. Existing duplicates are not removed and allocation/insertion has no enclosing transaction here.

        Example:
            macros.bulk_add_links(link_table, source_column, target_column, [(1, 10), (1, 11)])


        :param link_table: Link table inspected through driver_wrapper.
        :param src_col: Source endpoint column.
        :param dst_col: Destination endpoint column.
        :param values: Iterable of (source_id, destination_id) pairs, materialized once.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def bulk_delete_in_table(self, table, column, column_values):
        """
        Delete rows matching each single-column binding.

        Pass bindings directly to executemany; this method does not wrap scalar elements.

        Example:
            macros.bulk_delete_in_table("tags", "tag_id", [(1,), (2,)])


        :param table: Trusted table name.
        :param column: Column compared with a bound value.
        :param column_values: Iterable of one-item parameter sequences, such as [(1,), (2,)].
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def bulk_delete_items_in_table_two_matching_cols(self, table, col_1, col_2, column_values):
        """
        Delete rows matching each pair of column values.

        Example:
            macros.bulk_delete_items_in_table_two_matching_cols(link_table, left_column, right_column, [(1, 10)])


        :param table: Trusted table name.
        :param col_1: First equality column.
        :param col_2: Second equality column.
        :param column_values: Iterable of (first_value, second_value) bindings.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def bulk_update_link_table(self, link_table, update_column, other_column, values):
        """
        Replace a link column where its old value and a second column both match.

        Example:
            macros.bulk_update_link_table(link_table, value_column, owner_column, [(20, 10, 1)])


        :param link_table: Trusted link table name.
        :param update_column: Column to update and compare with its old value.
        :param other_column: Additional equality column.
        :param values: Iterable of (new_value, old_value, other_column_value) triples.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def check_for_cc_link(self, link_table: str, book_id: int, value_id: int, conn: Optional[sqlite3.Connection]=None) -> bool:
        """
        Look up the owner ID for a conventional custom owner/value link.

        The implementation returns get(..., all=False) directly without bool conversion. Use an explicit None check if zero could be a valid owner.

        Example:
            exists = macros.check_for_cc_link(link_table, book_id, value_id, conn=conn) is not None


        :param link_table: Custom link table.
        :param book_id: Owner ID to match.
        :param value_id: Value ID to match.
        :param conn: Optional project connection with get(sql, parameters, all=...); plain sqlite3.Connection lacks that helper.
        :return: Matching owner scalar or None under the project connection.get contract, despite the bool annotation.
        """

        ...







    @abc.abstractmethod
    def clean_custom(self, cc_num_map, cc_table_name_factory=None, conn=None):
        """
        Prune unlinked values from every normalized custom column in a metadata map.

        Build one deletion per normalized entry and execute the script only when there are statements. Commit even a supplied connection. Non-normalized entries are ignored; this neither updates the metadata map nor returns deleted IDs.

        Example:
            macros.clean_custom(column_metadata, cc_table_name_factory=table_names, conn=conn)


        :param cc_num_map: Mapping whose values contain normalized and num fields.
        :param cc_table_name_factory: Callable mapping a column number to (value_table, link_table); required when a normalized entry exists.
        :param conn: Optional project SQLite-compatible connection; None selects the implementation’s default connection.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def clear_cc_entries_from_table(self, table, book_id, conn=None):
        """
        Delete custom-table entries belonging to one conventional book owner.

        Execute directly on the supplied or default driver connection without an explicit commit.

        Example:
            macros.clear_cc_entries_from_table(custom_table, book_id, conn=conn)


        :param table: Custom table containing a singular_stem_book column.
        :param book_id: Owner ID to remove.
        :param conn: Optional project SQLite-compatible connection; None selects the implementation’s default connection.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def clear_cc_unused_table_entries(self, table, lt, conn=None):
        """
        Attempt to prune values with no matching row in a custom link table.

        The legacy SQL uses the raw link-table name in its value-column token while using the singular stem for other columns. Schemas where those names differ can fail; this contract does not imply that the generated cleanup SQL works for every conventional table. No explicit commit is performed.

        Example:
            macros.clear_cc_unused_table_entries(custom_table, link_table, conn=conn)


        :param table: Custom value table.
        :param lt: Related custom link table.
        :param conn: Optional project SQLite-compatible connection; None selects the implementation’s default connection.
        :return: None; the abstract method supplies no implementation.
        """

        ...








    @abc.abstractmethod
    def create_cc_table(
        self,
        normalized: bool,
        datatype: str,
        dt,
        table: str,
        link_table,
        collate,
        in_table='books',
        ordered=False,
        conn=None,
    ):
        """
        Create the physical value/link tables and helper objects for a custom column.

        Normalized columns receive value/link tables, indexes, validation triggers and tag-browser views; absent ratings tables yield NULL average ratings. Non-normalized columns store one value per owner. The optional connection is used for capability probes, but final DDL always goes through driver.direct_execute_sql_script. No custom_columns metadata row is inserted here. The legacy normalized value-update trigger names an unprefixed value column; do not treat every trigger as verified referential enforcement.

        Example:
            macros.create_cc_table(True, "text", "TEXT", custom_table, link_table, "COLLATE PYNOCASE", in_table=owner_table)


        :param normalized: Whether to separate reusable values from owner links.
        :param datatype: Logical datatype; series adds a REAL extra column to normalized links.
        :param dt: Trusted SQL value-type fragment.
        :param table: Custom value-table name.
        :param link_table: Custom link-table name used when normalized is true.
        :param collate: Trusted SQL collation fragment.
        :param in_table: Existing owner table whose primary key is introspected.
        :param ordered: Retained argument; current SQLite implementation does not use it.
        :param conn: Optional project SQLite-compatible connection; None selects the implementation’s default connection.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def create_cc_temp_tables(self, temp_tables: Iterable[str], conn: Any=None) -> None:
        """
        Replace named temporary ID tables on the chosen connection.

        Drop only temp-schema objects, then create each as id INTEGER PRIMARY KEY. Existing main-schema tables of the same name are preserved. Uses executescript; do not assume it joins a caller transaction unchanged.

        Example:
            macros.create_cc_temp_tables(("selected_ids",), conn=conn)


        :param temp_tables: Iterable of temporary table identifiers validated before executing the script.
        :param conn: Optional project SQLite-compatible connection; None selects the implementation’s default connection.
        :return: None; the abstract method supplies no implementation.
        """

        ...



    @abc.abstractmethod
    def delete_cc_item(self, table, lt, target_id, conn=None):
        """
        Delete links to one custom value, then delete the value row and commit.

        Commit the chosen connection even when supplied by the caller. There is no explicit rollback wrapper around the two statements.

        Example:
            macros.delete_cc_item(custom_table, link_table, value_id, conn=conn)


        :param table: Custom value table.
        :param lt: Related custom link table.
        :param target_id: Value row ID matched by both deletions.
        :param conn: Optional project SQLite-compatible connection; None selects the implementation’s default connection.
        :return: None; the abstract method supplies no implementation.
        """

        ...





    @abc.abstractmethod
    def delete_from_cc_table_by_id(self, table, target_id, conn=None):
        """
        Delete custom value rows matching the conventional ID column.

        Use the supplied connection or macro execute property. Link cleanup depends on schema triggers; this method contains no separate link deletion or commit.

        Example:
            macros.delete_from_cc_table_by_id(custom_table, value_id, conn=conn)


        :param table: Custom value table.
        :param target_id: Row ID bound to singular_stem_id.
        :param conn: Optional project SQLite-compatible connection; None selects the implementation’s default connection.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def delete_from_cc_table_by_value(self, table, target_id):
        """
        Delete custom rows by stored value rather than row ID.

        Example:
            macros.delete_from_cc_table_by_value(custom_table, "Obsolete")


        :param table: Custom value table.
        :param target_id: Stored value compared with singular_stem_value, despite this parameter’s name.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def delete_in_table(self, table, column, value):
        """
        Delete rows matching one bound value in a trusted table column.

        SQL equality with None does not select NULL rows. Execution and transaction handling follow the host execute helper.

        Example:
            macros.delete_in_table("tags", "tag_id", 7)


        :param table: Trusted table name.
        :param column: Trusted equality column.
        :param value: Value to match using SQL equality.
        :return: None; the abstract method supplies no implementation.
        """

        ...





    @abc.abstractmethod
    def destroy_cc_temp_tables(self, temp_tables: Iterable[str], conn: Any=None) -> None:
        """
        Drop named temporary tables without touching namesakes in the main schema.

        Use DROP TABLE IF EXISTS temp.name in an executescript call. The method does not close the connection.

        Example:
            macros.destroy_cc_temp_tables(("selected_ids",), conn=conn)


        :param temp_tables: Iterable of validated temporary table identifiers.
        :param conn: Optional project SQLite-compatible connection; None selects the implementation’s default connection.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def direct_get_custom_and_extra(self, link_table, index, conn=None):
        """
        Read only the extra scalar for the first custom link owned by a book.

        The legacy method name is broader than its SELECT. Multiple matches are not ordered.

        Example:
            series_index = macros.direct_get_custom_and_extra(link_table, book_id, conn=conn)


        :param link_table: Custom link table with a conventional extra column.
        :param index: Owner/book ID, not a positional row index.
        :param conn: Optional project connection with get(sql, parameters, all=...); plain sqlite3.Connection lacks that helper.
        :return: First extra value or None; no custom value or two-item pair is returned.
        """

        ...

    @abc.abstractmethod
    def direct_get_custom_tables(self, conn=None):
        """
        Discover custom value/link table names from SQLite schema metadata.

        Query sqlite_master for tables only; views, indexes and temporary tables are not included.

        Example:
            custom_tables = macros.direct_get_custom_tables(conn=conn)


        :param conn: Optional project connection with get(sql, parameters, all=...); plain sqlite3.Connection lacks that helper.
        :return: Set of table names matching custom_column_* or *_custom_column_*_link.
        """

        ...

    @abc.abstractmethod
    def direct_update_column_in_table(self, table, column, table_id_col, item_id, new_value):
        """
        Update one physical column and its known normalized identity when available.

        If a default identity declaration exists and its identity column is present, derive and write both values in one statement. Otherwise update only the requested column. This bypasses Row.sync and does not explicitly refresh caches.

        Example:
            macros.direct_update_column_in_table("tags", "tag", "tag_id", 7, "History")


        :param table: Trusted physical table name.
        :param column: Column receiving new_value.
        :param table_id_col: Column used to select the target row.
        :param item_id: ID or key value matched in table_id_col.
        :param new_value: Replacement stored value; None also clears an available identity column.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def do_cc_db_bulk_addition(self, temp_tables, custom_table, link_table, add, remove, conn=None):
        """
        Add and remove existing custom values for owner IDs staged in temporary tables.

        Resolve each requested value to the first matching ID, stage those IDs, remove matching owner/value pairs, then insert-or-replace the owner/addition Cartesian product. Addition wins over removal for a value in both groups. Temporary tables must already exist and be suitably cleared; the method neither clears nor drops them and makes no explicit commit.

        Example:
            macros.do_cc_db_bulk_addition(temp_tables, custom_table, link_table, ["New"], ["Old"], conn=conn)


        :param temp_tables: Three table names: selected owners, addition IDs and removal IDs.
        :param custom_table: Custom value table searched by value with PYNOCASE.
        :param link_table: Custom link table.
        :param add: Values to resolve and add; absent values are not created.
        :param remove: Values to resolve and remove.
        :param conn: Optional project SQLite-compatible connection; None selects the implementation’s default connection.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def do_custom_column_delete_by_id(self, cc_id: int) -> None:
        """
        Delete a custom_columns metadata row through driver_wrapper.

        Does not drop physical tables or mark deferred cleanup. Scalar-binding support depends on driver_wrapper; do_custom_column_delete_by_num supplies a conventional one-item tuple instead.

        Example:
            macros.do_custom_column_delete_by_id(column_id)


        :param cc_id: custom_column_id value; forwarded as a scalar binding by the legacy implementation.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def do_custom_column_delete_by_num(self, num: int) -> None:
        """
        Delete one custom_columns metadata row using a one-item binding tuple.

        This immediate metadata deletion does not itself drop value/link tables or update loaded custom-column maps.

        Example:
            macros.do_custom_column_delete_by_num(column_id)


        :param num: custom_column_id to remove.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def ensure_custom_column_value(self, cc_table: str, value: Any) -> Any:
        """
        Attempt a value insert, then retrieve the earliest matching custom-row ID.

        Suppress DatabaseDriverError from insertion, then query for a matching row. This can tolerate a uniqueness conflict but also hides other insertion failures until lookup. No result causes IndexError; this is not an atomic backend-neutral upsert.

        Example:
            value_id = macros.ensure_custom_column_value(custom_table, "Finished")


        :param cc_table: Custom table whose display and ID columns are discovered through driver_wrapper.
        :param value: Value to insert or resolve by SQL equality.
        :return: First matching ID in ascending ID order.
        :raises IndexError: No matching row is available after the insert attempt.
        """

        ...

    @property
    @abc.abstractmethod
    def execute(self):
        """
        Expose the host driver-wrapper single-statement executor.

        This is a property returning a bound callable, not an execution performed while reading the property.

        Example:
            macros.execute("SELECT 1")


        :return: Callable db.driver_wrapper.execute on the current SQLite provider.
        """

        ...

    @property
    @abc.abstractmethod
    def executemany(self):
        """
        Expose the host driver-wrapper batch executor.

        Example:
            macros.executemany("UPDATE tags SET tag=? WHERE tag_id=?", [("History", 7)])


        :return: Callable db.driver_wrapper.executemany on the current SQLite provider.
        """

        ...

    @abc.abstractmethod
    def generic_clean_update(self, link_table, link_col, value_for_clear):
        """
        Delete links selected by an integer or supplied batch bindings.

        Integers, including bool, use execute with a wrapped binding. All other inputs pass directly to executemany, so a plain list of integer IDs is not wrapped automatically.

        Example:
            macros.generic_clean_update(link_table, owner_column, [(1,), (2,)])


        :param link_table: Trusted link table name.
        :param link_col: Column compared with bound values.
        :param value_for_clear: Integer scalar, or iterable of one-item parameter sequences.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @property
    @abc.abstractmethod
    def get(self):
        """
        Expose the database host’s result-fetching compatibility helper.

        Result shape and keyword handling belong to that host helper rather than this abstract property.

        Example:
            rows = macros.get("SELECT tag_id FROM tags")


        :return: Callable db.get supplied by the database host.
        """

        ...

    @abc.abstractmethod
    def get_all_cc_custom_values(self, cc_table: str, distinct: bool=False, conn: Optional[sqlite3.Connection]=None) -> Iterable[Union[int, str, float]]:
        """
        Select every stored custom value, optionally with SQL DISTINCT.

        No result ordering or flattening is applied. The non-distinct path requests all=True; the distinct path relies on get’s default.

        Example:
            rows = macros.get_all_cc_custom_values(custom_table, distinct=True, conn=conn)


        :param cc_table: Custom value-table name.
        :param distinct: Whether to remove duplicate result values through SQL DISTINCT.
        :param conn: Optional project connection with get(sql, parameters, all=...); plain sqlite3.Connection lacks that helper.
        :return: Connection.get result: normally a list of one-column rows, despite the scalar-iterable annotation.
        """

        ...

    @abc.abstractmethod
    def get_all_cc_id_val_pairs(self, table, conn: Optional[sqlite3.Connection]=None):
        """
        Read ID/value rows from a conventional custom value table.

        Example:
            pairs = macros.get_all_cc_id_val_pairs(custom_table, conn=conn)


        :param table: Custom value-table name.
        :param conn: Optional project connection with get(sql, parameters, all=...); plain sqlite3.Connection lacks that helper.
        :return: All (id, value) rows returned by connection.get, without explicit ordering.
        """

        ...

    @abc.abstractmethod
    def get_all_cc_ids_marked_for_delete(self, conn=None) -> list[int]:
        """
        Collect metadata IDs whose deferred-delete flag equals one.

        Example:
            marked_ids = macros.get_all_cc_ids_marked_for_delete(conn=conn)


        :param conn: Optional project connection with get(sql, parameters, all=...); plain sqlite3.Connection lacks that helper.
        :return: List of custom_column_id values; no deletion or flag reset occurs.
        """

        ...

    @abc.abstractmethod
    def get_all_table_link_data(self, table1, table2, typed=False, priority=False):
        """
        Read links for every primary row and shape them for legacy cache callers.

        Use portable bulk link retrieval. List order follows its link-row order; no numeric priority values are returned. Per-type containers are defaultdicts, and owners without links retain empty containers when included by the bulk result.

        Example:
            by_work = macros.get_all_table_link_data("works", "agents", typed=True, priority=True)


        :param table1: Primary table whose current row IDs are enumerated.
        :param table2: Secondary table resolved through the oriented link specification.
        :param typed: Whether to group secondary IDs by link_type.
        :param priority: Whether to retain returned link order in lists instead of sets.
        :return: Mapping from primary IDs to sets/lists, or per-type mappings of those containers; empty dict when no link spec exists.
        """

        ...

    @abc.abstractmethod
    def get_cc_books_for_dirtying(self, table: str, link: str, id: int, conn: Optional[Any]=None) -> Iterable[str]:
        """
        Read owner rows linked to a custom value through a conventional books link table.

        This reads candidate owners only; it does not mark a dirty cache or metadata queue. The link table name is fixed to books_table_link by the implementation.

        Example:
            owner_rows = macros.get_cc_books_for_dirtying(custom_table, "value", value_id, conn=conn)


        :param table: Custom value-table name used to construct books_table_link.
        :param link: Suffix of the linked-value column to filter.
        :param id: Value bound to that link column.
        :param conn: Optional project connection with get(sql, parameters, all=...); plain sqlite3.Connection lacks that helper.
        :return: Connection.get rows containing owner IDs, despite the Iterable[str] annotation.
        """

        ...

    @abc.abstractmethod
    def get_cc_books_from_link_table(self, lt: str, lt_value: Any) -> Iterable[int]:
        """
        Read owner rows whose conventional custom-link value matches an ID.

        Use the default driver connection with no explicit ordering or deduplication.

        Example:
            owner_rows = macros.get_cc_books_from_link_table(link_table, value_id)


        :param lt: Custom link table.
        :param lt_value: Stored target ID matched in singular_stem_value.
        :return: Project connection.get rows of owner IDs, not a flattened integer sequence.
        """

        ...

    @abc.abstractmethod
    def get_cc_id_and_value_from_id(self, custom_column: str, target_id: int, conn: Optional[sqlite3.Connection]=None) -> tuple[int, str]:
        """
        Read the first ID/value row for a conventional custom-row ID.

        Example:
            row_id, value = macros.get_cc_id_and_value_from_id(custom_table, value_id, conn=conn)


        :param custom_column: Custom value-table name.
        :param target_id: ID matched in the derived stem_id column.
        :param conn: Optional project connection with get(sql, parameters, all=...); plain sqlite3.Connection lacks that helper.
        :return: First (id, value) row; actual values are not restricted to str.
        :raises IndexError: The result contains no matching row.
        """

        ...

    @abc.abstractmethod
    def get_cc_id_from_value(self, target_table: str, cc_value: Union[str, int, datetime.datetime], all: bool=False, conn: Optional[sqlite3.Connection]=None) -> int:
        """
        Resolve custom-row IDs by SQL equality against the stored value.

        No explicit ordering, normalization or insertion occurs. None is compared using SQL equality and therefore does not select a NULL value.

        Example:
            value_id = macros.get_cc_id_from_value(custom_table, "Finished", conn=conn)


        :param target_table: Custom value-table name.
        :param cc_value: Value bound to the derived stem_value column.
        :param all: True to return all one-column rows; False for the first scalar or None.
        :param conn: Optional project connection with get(sql, parameters, all=...); plain sqlite3.Connection lacks that helper.
        :return: Connection.get result selected by all; broader than the int annotation.
        """

        ...

    @abc.abstractmethod
    def get_cc_id_value_from_cc_id(self, table: str, old_id: int) -> tuple[int, str]:
        """
        Read an existing custom ID/value row through the default connection.

        Example:
            row_id, value = macros.get_cc_id_value_from_cc_id(custom_table, value_id)


        :param table: Custom value-table name.
        :param old_id: ID bound to the derived stem_id column.
        :return: First (id, value) row, with no string conversion of the value.
        :raises IndexError: No row matches old_id.
        """

        ...

    # Todo: This seems to be an interface weirdness
    @abc.abstractmethod
    def get_cc_lt_books_from_lt_value(
            self,
            lt: str,
            value: Union[str, int, datetime.datetime],
            conn: Optional[sqlite3.Connection]=None) -> Iterable[int]:
        """
        Read owner rows for a stored custom-link value.

        Example:
            owner_rows = macros.get_cc_lt_books_from_lt_value(link_table, value_id, conn=conn)


        :param lt: Custom link table.
        :param value: Stored target ID compared with stem_value; the broad annotation does not imply a display-value lookup.
        :param conn: Optional project connection with get(sql, parameters, all=...); plain sqlite3.Connection lacks that helper.
        :return: Connection.get rows of owner IDs, despite the scalar-iterable annotation.
        """

        ...

    @abc.abstractmethod
    def get_cc_series_index_indices(
            self,
            cc_series_link_table: str,
            series_id: int,
            conn: Optional[sqlite3.Connection] = None) -> tuple[Union[float, int], ...]:
        """
        Read ordered extras for owners linked to the selected custom series.

        The outer query selects every link belonging to those owners, not only links whose value equals series_id. An owner with multiple values can therefore contribute extras from other links.

        Example:
            index_rows = macros.get_cc_series_index_indices(series_link_table, series_id, conn=conn)


        :param cc_series_link_table: Custom link table containing conventional book, value and extra columns.
        :param series_id: Series value ID used to select the owner set.
        :param conn: Optional project connection with get(sql, parameters, all=...); plain sqlite3.Connection lacks that helper.
        :return: One-column rows of extra values in ascending extra order, despite the flat-tuple annotation.
        """

        ...



    @abc.abstractmethod
    def get_dirtied_cache(self):
        """
        Build a legacy owner-to-position map from metadata dirtiness rows.

        Probe schema columns to support metadata_dirtied_book or the newer table/table_id shape. The newer form excludes NULL IDs and filters books when a table column exists. Missing/unrecognized schema returns an empty dict; later query failures propagate. No ORDER BY or queue clearing is performed.

        Example:
            dirty_positions = macros.get_dirtied_cache()


        :return: Dictionary mapping each retrieved owner ID to its last enumeration position.
        """

        ...

    @abc.abstractmethod
    def get_foreign_key_replacement_trigger(self, target_table, search_column='book', target_id='book_id', old=True):
        """
        Build a DELETE fragment referring to the triggering row’s OLD key.

        Even old=False emits OLD. Identifiers are interpolated directly.

        Example:
            statement = macros.get_foreign_key_replacement_trigger(link_table, owner_column, "book_id")


        :param target_table: Trusted table to delete from.
        :param search_column: Column in that table compared with the old row key.
        :param target_id: Column name referenced through OLD.
        :param old: Retained compatibility argument; ignored by the current implementation.
        :return: SQL DELETE statement text, not an installed trigger.
        """

        ...

    @abc.abstractmethod
    def get_link_data(self, table1, table2, table1_id, typed=False, priority=False):
        """
        Return one primary row’s linked IDs in a legacy cache container.

        The SQLite implementation delegates to portable link-row retrieval. With no link spec, return an empty list or set based only on priority, even when typed=True. List order follows link-row retrieval, and numeric priority values are not included.

        Example:
            agents_by_role = macros.get_link_data("works", "agents", work_id, typed=True, priority=True)


        :param table1: Primary table.
        :param table2: Secondary table resolved through the oriented link specification.
        :param table1_id: Primary row ID.
        :param typed: Group results by link_type when true.
        :param priority: Use ordered lists instead of sets when true.
        :return: Set/list of secondary IDs, or a defaultdict of per-type sets/lists.
        """

        ...

    @abc.abstractmethod
    def get_linked_ids(self, link_table, left_id_col, right_id_col, left_id, type_filter=None):
        """
        Collect distinct right-endpoint IDs for one left endpoint.

        None cannot request a SQL NULL type filter here; portable link APIs distinguish omitted and NULL filters with LINK_TYPE_UNSET.

        Example:
            agent_ids = macros.get_linked_ids(link_table, work_column, agent_column, work_id, type_filter="author")


        :param link_table: Trusted link table.
        :param left_id_col: Left endpoint column used in the equality condition.
        :param right_id_col: Right endpoint column to select.
        :param left_id: Left endpoint ID.
        :param type_filter: Optional value for the conventional stem_type column; None disables filtering.
        :return: Set of right-endpoint IDs.
        """

        ...





    @abc.abstractmethod
    def get_unique_values(self, table, column):
        """
        Collect distinct stored values from one column using a Python set.

        The SELECT does not use DISTINCT; Python set insertion deduplicates values and requires hashable results.

        Example:
            values = macros.get_unique_values("tags", "tag")


        :param table: Trusted table name.
        :param column: Trusted column name.
        :return: Set of retrieved values, including None when present.
        """

        ...

    @abc.abstractmethod
    def get_values_one_condition(self, table, rtn_column, cond_column, value, default_value=None):
        """
        Collect values matching one SQL equality condition.

        Other errors propagate. The fallback is not a substitute for an empty result.

        Example:
            names = macros.get_values_one_condition("tags", "tag", "tag_id", 7, default_value=set())


        :param table: Trusted table name.
        :param rtn_column: Column whose values are collected.
        :param cond_column: Column used in the equality condition.
        :param value: Bound condition value.
        :param default_value: Fallback returned only if execution, iteration or set insertion raises TypeError.
        :return: Set of matching values, including an empty set for no rows; default_value after a caught TypeError.
        """

        ...

    @abc.abstractmethod
    def hash_table(self, target_table: str, columns: Iterable[str]) -> str:
        """
        Compute the legacy MD5 form of the portable table fingerprint.

        Delegate to fingerprint_table with algorithm="md5". The default row ordering uses a discovered ID column or falls back to selected columns; column order affects the fingerprint. No table rows are modified.

        Example:
            before = macros.hash_table("tags", ("tag_id", "tag"))


        :param target_table: Table whose selected content is read.
        :param columns: Nonempty iterable of existing column names, materialized into a tuple.
        :return: MD5 hexadecimal digest of the table/column header and canonicalized selected rows.
        """

        ...

    @abc.abstractmethod
    def insert_multiple_values_into_cc_table(self, table, values, conn=None):
        """
        Insert each supplied scalar into a conventional custom value column.

        No deduplication, row-ID result or explicit commit is provided. Database constraints apply to each insert.

        Example:
            macros.insert_multiple_values_into_cc_table(custom_table, ["New", "Finished"], conn=conn)


        :param table: Custom value-table name.
        :param values: Iterable of scalar values wrapped into one-item bindings.
        :param conn: Optional project SQLite-compatible connection; None selects the implementation’s default connection.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def insert_values_into_temp_table(self, temp_table: str, values: Iterable[Any], conn: Any=None) -> None:
        """
        Insert scalar IDs into a previously created temporary one-column table.

        Materialize bindings before executemany. The table must already exist on that same connection; this method neither clears it nor commits explicitly.

        Example:
            macros.insert_values_into_temp_table("selected_ids", [1, 2], conn=conn)


        :param temp_table: Validated temporary table name; always addressed through temp schema.
        :param values: Iterable of scalar values wrapped into one-item bindings.
        :param conn: Optional project SQLite-compatible connection; None selects the implementation’s default connection.
        :return: None; the abstract method supplies no implementation.
        """

        ...





    @abc.abstractmethod
    def make_generic_link(self, link_table, left_link_col, right_link_col, priority_col, left_id, right_id):
        """
        Insert an endpoint pair at one above the source’s current maximum priority.

        Use one INSERT SELECT statement, treating an empty group as priority zero before incrementing. Existing duplicate links and uniqueness constraints are left to the database.

        Example:
            macros.make_generic_link(link_table, left_column, right_column, priority_column, 1, 10)


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
    def make_generic_link_no_priority(self, link_table, left_link_col, right_link_col, left_id=None, right_id=None, id_pairs=None):
        """
        Insert endpoint bindings using the legacy unprioritized column order.

        The implementation reverses the named columns without reversing supplied values. Callers must account for that existing behavior; no automatic orientation correction occurs.

        Example:
            macros.make_generic_link_no_priority(link_table, left_column, right_column, id_pairs=[(right_id, left_id)])


        :param link_table: Trusted link table.
        :param left_link_col: Column named as the left endpoint, emitted second in SQL.
        :param right_link_col: Column named as the right endpoint, emitted first in SQL.
        :param left_id: First bound value when id_pairs is None; currently written to right_link_col.
        :param right_id: Second bound value when id_pairs is None; currently written to left_link_col.
        :param id_pairs: Optional iterable of two-item bindings, also in emitted right/left column order.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def mark_cc_for_delete(self, cc_column_id: int) -> None:
        """
        Set one custom-column metadata row’s deferred-delete flag.

        Update the flag only. Physical cleanup and loaded metadata-map changes occur elsewhere.

        Example:
            macros.mark_cc_for_delete(column_id)


        :param cc_column_id: custom_column_id to mark.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def mark_custom_column_for_delete(self, num: int) -> None:
        """
        Mark a custom-column metadata row for later cleanup.

        Compatibility spelling using driver_wrapper.execute. It does not immediately drop tables or remove metadata.

        Example:
            macros.mark_custom_column_for_delete(column_id)


        :param num: custom_column_id matched by the update.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def preform_cc_column_delete_from_map(self, num_table_lt_map: dict[int, tuple[str, str]], conn=None) -> None:
        """
        Drop mapped custom-column schema objects and remove all marked metadata rows.

        Retain the historical preform spelling. For each pair, drop the listed indexes, triggers, tag-browser views and tables with IF EXISTS. Then delete every metadata row marked for deletion, including marked rows absent from the map, and commit the chosen connection. The map key is not used to restrict that final deletion. Script execution has no enclosing rollback wrapper here.

        Example:
            macros.preform_cc_column_delete_from_map({column_id: (custom_table, link_table)}, conn=conn)


        :param num_table_lt_map: Mapping from custom-column number to (value_table, link_table) names.
        :param conn: Optional project SQLite-compatible connection; None selects the implementation’s default connection.
        :return: None; the abstract method supplies no implementation.
        """

        ...








    @abc.abstractmethod
    def read_cc_value_from_meta_2(self, num: int, book_id: int, conn: Optional[sqlite3.Connection]=None) -> Iterable[Union[int, str, float]]:
        """
        Read one custom_N projection value from the legacy meta2 view.

        The meta2 view and the requested custom_N column must already exist. This does not construct the view or resolve a custom-column label.

        Example:
            value = macros.read_cc_value_from_meta_2(column_id, book_id, conn=conn)


        :param num: Column number interpolated into the projection column name.
        :param book_id: Owner ID matched against meta2.id.
        :param conn: Optional project connection with get(sql, parameters, all=...); plain sqlite3.Connection lacks that helper.
        :return: First projected scalar or None under get(all=False), despite the iterable annotation.
        """

        ...




    @abc.abstractmethod
    def read_link_property_trios(self, link_table, link_property_col, first_id, second_id):
        """
        Execute a three-column read over every row of a link table.

        There is no filter or explicit ordering; the caller consumes the returned result.

        Example:
            rows = macros.read_link_property_trios(link_table, priority_column, left_column, right_column)


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

        Execute an UPDATE over all rows of folders, using SQLite replace on folder_path. This is a database text change, not filesystem relocation or path-prefix validation; occurrences anywhere in each stored path are replaced.

        Example:
            macros.replace_in_folder_path("/old/root", "/new/root")


        :param target_str: Substring passed as the SQL replace search argument.
        :param replacement: Replacement substring passed as a bound argument.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def replace_in_folder_store_marker_path(self, target_str: str, replacement: str) -> None:
        """
        Replace literal text throughout stored folder_store_marker_path values.

        Execute an UPDATE over all rows of folder_stores, using SQLite replace on folder_store_marker_path. This is a database text change, not filesystem relocation or path-prefix validation; occurrences anywhere in each stored path are replaced.

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

        Execute an UPDATE over all rows of folder_stores, using SQLite replace on folder_store_path. This is a database text change, not filesystem relocation or path-prefix validation; occurrences anywhere in each stored path are replaced.

        Example:
            macros.replace_in_folder_store_path("/old/root", "/new/root")


        :param target_str: Substring passed as the SQL replace search argument.
        :param replacement: Replacement substring passed as a bound argument.
        :return: None; the abstract method supplies no implementation.
        """

        ...








    @abc.abstractmethod
    def repoint_cc_lt_values(self, lt, new_id, old_id):
        """
        Replace one conventional custom-link target ID everywhere it occurs.

        Use the macro execute helper; owner IDs and extras are unchanged. Conflicting links are not merged automatically.

        Example:
            macros.repoint_cc_lt_values(link_table, surviving_id, removed_id)


        :param lt: Custom link table.
        :param new_id: Replacement target ID.
        :param old_id: Existing target ID matched in stem_value.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def reprioritize_link(self, link_table, left_link_col, right_link_col, left_id, right_id, new_type=None, new_priority='MAX'):
        """
        Move matching endpoint links above the source’s current maximum priority.

        With new_type, perform the priority update first, then a separate type update. Existing type is not a matching condition, so multiple typed rows with the same endpoints may be affected. No transaction wraps the two updates here.

        Example:
            macros.reprioritize_link(link_table, left_column, right_column, 1, 10)


        :param link_table: Trusted link table with a conventional priority column.
        :param left_link_col: Source endpoint column.
        :param right_link_col: Target endpoint column.
        :param left_id: Source ID defining the priority group.
        :param right_id: Target ID selecting links to move.
        :param new_type: Optional non-None replacement type applied after the priority update.
        :param new_priority: Only the literal MAX mode is supported.
        :return: None; the abstract method supplies no implementation.
        :raises AssertionError: new_priority is not MAX when assertions are enabled.
        """

        ...



    @abc.abstractmethod
    def set_custom_column_metadata(
        self,
        num: int,
        name: Optional[str]=None,
        label: Optional[str]=None,
        is_editable: Optional[bool]=None,
        display: Optional[str]=None,
        in_table: Optional[str]=None,
        conn=None,
    ) -> bool:
        """
        Update supplied custom-column metadata fields and commit when any are supplied.

        Update normalized name/label companions when present, using NFC/trim/casefold policy. Issue separate statements and commit even a supplied connection if changed=True. No rollback wrapper or loaded metadata-map refresh occurs here; None cannot clear a field.

        Example:
            changed = macros.set_custom_column_metadata(column_id, name="Reading state", conn=conn)


        :param num: custom_column_id to update.
        :param name: Optional replacement name; None leaves it unchanged.
        :param label: Optional replacement label; None leaves it unchanged.
        :param is_editable: Optional editability flag coerced to bool.
        :param display: Optional JSON-serializable display value; serialized with json.dumps despite the str annotation.
        :param in_table: Optional replacement owner-table name.
        :param conn: Optional project SQLite-compatible connection; None selects the implementation’s default connection.
        :return: True when at least one non-None field was supplied, regardless of row existence or value equality.
        """

        ...

    @abc.abstractmethod
    def set_database_version(self, new_val):
        """
        Replace every stored database-version value, inserting one row if empty.

        Count rows first; a nonempty table is updated without a WHERE clause. This method does not migrate the schema or enforce a single-row invariant.

        Example:
            macros.set_database_version("2")


        :param new_val: Value written to database_version_version.
        :return: None; the abstract method supplies no implementation.
        """

        ...






    @abc.abstractmethod
    def set_library_id(self, new_val):
        """
        Replace every stored library UUID value, inserting one row if empty.

        A nonempty library_id table is updated without a WHERE clause. No UUID is generated automatically.

        Example:
            macros.set_library_id(library_uuid)


        :param new_val: Value written to library_id_uuid without format validation here.
        :return: None; the abstract method supplies no implementation.
        """

        ...








    @abc.abstractmethod
    def update_cc_lt_value_by_value(self, lt, new_value_id, old_value_id, conn=None):
        """
        Replace every occurrence of one custom-link target ID.

        Use the supplied or default driver connection directly, without an explicit commit or conflict merge.

        Example:
            macros.update_cc_lt_value_by_value(link_table, surviving_id, removed_id, conn=conn)


        :param lt: Custom link table.
        :param new_value_id: Replacement stem_value ID.
        :param old_value_id: Existing stem_value ID to match.
        :param conn: Optional project SQLite-compatible connection; None selects the implementation’s default connection.
        :return: None; the abstract method supplies no implementation.
        """

        ...

    @abc.abstractmethod
    def update_cc_value(self, cc_column, cc_id, cc_value, conn=None):
        """
        Replace a custom row’s stored value by conventional row ID.

        The default path discards the macro execute result. No uniqueness reconciliation or explicit commit occurs in this method.

        Example:
            macros.update_cc_value(custom_table, value_id, "Finished", conn=conn)


        :param cc_column: Custom value-table name.
        :param cc_id: ID matched in singular_stem_id.
        :param cc_value: Replacement value bound to singular_stem_value.
        :param conn: Optional project SQLite-compatible connection; None selects the implementation’s default connection.
        :return: Supplied connection.execute result when conn is given; otherwise None.
        """

        ...

    @abc.abstractmethod
    def update_column_in_table(self, table, column, table_id_col, item_id, new_value):
        """
        Replace a row field through the database Row interface and sync it.

        Unlike direct_update_column_in_table, this resolves a Row, assigns its field, then calls sync. Missing-row failures and validation/persistence behavior belong to the Row implementation.

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



    @abc.abstractmethod
    def update_custom_column_additional_column_many(self, table, column, sequence):
        """
        Update an additional custom-link field for owner/value pairs in bulk.

        Forward bindings to db.executemany without conversion. The owner and value columns are always stem_book and stem_value.

        Example:
            macros.update_custom_column_additional_column_many(link_table, "extra", [(2.5, book_id, series_id)])


        :param table: Custom link table with conventional prefixed columns.
        :param column: Suffix appended to the singular table stem.
        :param sequence: Iterable of (new_field_value, owner_id, value_id) triples.
        :return: None; the abstract method supplies no implementation.
        """

        ...
