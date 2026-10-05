

# Todo: This feels like it needs a bit of a rethink

"""
Compose SQLite macro services and retain legacy SQL convenience operations.

SQLiteDatabaseMacros combines portable link, identity and fingerprint operations with
custom-column, temporary-table and hash aliases. The methods defined here cover links,
scalar reads/writes and legacy metadata/path updates. Most legacy helpers interpolate
trusted identifiers and bind data values. They delegate execution to the database
wrapper without adding an explicit transaction, commit or connection lifecycle of their
own.
"""

# Macros which provide pre-defined operations on the database.
# This allows you the option of replacing the generic macros which will use the methods provided by the database with
# more efficient macros tailored to the underlying database
# Macros should be shortcuts to preform useful operatios on the tables - but ones which can be all replicated using
# objects from the database
# If it's a fundamental operation, then it should be done in the driver
# Todo: In line with this, move the create custom columns logic down into the driver

from collections import defaultdict
from typing import TYPE_CHECKING

from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.cc_macros_mixin import \
    SQLiteDatabaseCustomColumnMacros
from LiuXin_alpha.databases.api import MacrosAPI

# Todo: This needs to be replaced with a column name factory
from LiuXin_alpha.databases.database_driver_plugins.macros_base import MacrosBase
from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.temp_tables_macros_mixin import TempTablesMacrosMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.hash_tables_macros_mixin import HashTablesMacrosMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.portable_macros_mixin import (
    SQLPortableMacrosMixin,
)
from LiuXin_alpha.databases.normalized_identities import (
    default_normalized_identity_spec,
    normalize_identity_value,
)


if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI


# Todo: Probably should have it's own API


class SQLiteDatabaseMacros(
    MacrosBase,
    SQLPortableMacrosMixin,
    SQLiteDatabaseCustomColumnMacros,
    TempTablesMacrosMixin,
    HashTablesMacrosMixin,
    # Todo: We're gonna need to re-write this API some
    MacrosAPI
):
    """
    Attach portable and legacy database macros to a database facade.

    Construction stores the facade through MacrosBase. The local get, execute and
    executemany properties expose host operations on demand; they do not execute SQL by
    themselves. Legacy raw-SQL methods expect trusted identifiers and suitable existing
    schema. Multi-statement helpers do not establish their own atomic transaction.

    Example:
        Given a database facade, ``macros = SQLiteDatabaseMacros(db)`` exposes
        ``macros.get_link_data("works", "agents", work_id, typed=True)``.
    """

    def __init__(self, db: "DatabaseAPI") -> None:
        """
        Retain the database facade through the base macro initializer.

        No connection is opened and no schema is inspected during construction.

        Example:
            >>> host = object()
            >>> macros = SQLiteDatabaseMacros(host)
            >>> macros.db is host
            True


        :param db: Facade supplying row access, a driver wrapper and portable-macro
            dependencies.
        :return: None; the facade is retained as db.
        """
        super(SQLiteDatabaseMacros, self).__init__(db=db)

    # Todo - These should probably be semi private
    @property
    def get(self):
        """
        Expose the current facade get operation for legacy callers.

        The attribute is resolved on every property access, with no argument adaptation
        or caching.

        Example:
            ``macros.get`` returns the same attribute obtained as ``macros.db.get``.


        :return: The facade get attribute; missing attributes raise AttributeError.
        """
        return self.db.get

    @property
    def execute(self):
        """
        Expose the driver wrapper's current single-statement execution operation.

        Example:
            ``macros.execute("SELECT 1")`` calls the driver wrapper directly.


        :return: The wrapper execute attribute; result and transaction behavior belong
            to that wrapper.
        """
        return self.db.driver_wrapper.execute

    @property
    def executemany(self):
        """
        Expose the driver wrapper's current repeated-statement execution operation.

        Example:
            ``macros.executemany(sql, [(1,), (2,)])`` forwards both bindings unchanged.


        :return: The wrapper executemany attribute, without an added transaction
            boundary.
        """
        return self.db.driver_wrapper.executemany

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - LINK MAKING METHODS

    def make_generic_link(self, link_table, left_link_col, right_link_col, priority_col, left_id, right_id):
        """
        Insert one endpoint pair with the next priority for its left endpoint.

        Use COALESCE(MAX(priority), 0) + 1 among rows whose left column matches left_id.
        An empty group starts at one; other groups do not affect the value. Existing
        priorities are not compacted and duplicate links are not checked before
        insertion.

        Example:
            For an empty link group, ``macros.make_generic_link("links", "left_id",
            "right_id", "priority", 1, 2)`` inserts the pair at priority one.


        :param link_table: Trusted physical link-table name interpolated directly into
            SQL.
        :param left_link_col: Trusted column naming the left endpoint in the link table.
        :param right_link_col: Trusted column naming the right endpoint in the link
            table.
        :param priority_col: Trusted priority-column name used in both INSERT and MAX.
        :param left_id: Left endpoint value bound as a query parameter.
        :param right_id: Right endpoint value bound as a query parameter.
        :return: None; the wrapper execution result is discarded.
        """
        stmt = (
            "INSERT INTO {0}({1}, {2}, {3}) "
            "SELECT ?, ?, COALESCE(MAX({3}), 0) + 1 "
            "FROM {0} WHERE {1} = ?".format(
                link_table,
                left_link_col,
                right_link_col,
                priority_col,
            )
        )
        self.execute(stmt, (left_id, right_id, left_id))

    # Todo: The interface for this macro is terrible and you should feel bad. Fix it.
    def make_generic_link_no_priority(
        self,
        link_table,
        left_link_col,
        right_link_col,
        left_id=None,
        right_id=None,
        id_pairs=None,
    ):
        """
        Insert links using the legacy reversed endpoint-column order.

        The INSERT names right_link_col before left_link_col. Thus the first bound value
        goes to the right column and the second to the left, despite the argument names.
        With id_pairs=None use one (left_id, right_id) binding; otherwise ignore those
        scalar arguments and pass id_pairs unchanged to executemany. No priority or type
        fields are supplied.

        Example:
            >>> import sqlite3
            >>> from types import SimpleNamespace
            >>> conn = sqlite3.connect(":memory:")
            >>> _ = conn.execute("CREATE TABLE links (left_id INTEGER, right_id INTEGER)")
            >>> macros = SQLiteDatabaseMacros(SimpleNamespace(driver_wrapper=conn))
            >>> macros.make_generic_link_no_priority("links", "left_id", "right_id", 1, 2)
            >>> conn.execute("SELECT left_id, right_id FROM links").fetchone()
            (2, 1)
            >>> conn.close()


        :param link_table: Trusted physical link-table name interpolated directly into
            SQL.
        :param left_link_col: Trusted column naming the left endpoint in the link table.
        :param right_link_col: Trusted column naming the right endpoint in the link
            table.
        :param left_id: First single-row binding, written to right_link_col; ignored
            when id_pairs is supplied.
        :param right_id: Second single-row binding, written to left_link_col; ignored
            when id_pairs is supplied.
        :param id_pairs: Iterable of two-value bindings in right-column, left-column
            order, or None for one link.
        :return: None; single or repeated execution results are discarded.
        """
        ins_stmt = "INSERT INTO {0}({2}, {1}) VALUES(?, ?)".format(link_table, left_link_col, right_link_col)
        if id_pairs is None:
            self.execute(ins_stmt, (left_id, right_id))
        else:
            self.executemany(ins_stmt, id_pairs)

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CLEAN METHOD

    # Todo: This is not actually about cleaning - it's about breaking a generic link - need to rename and
    # merge
    def generic_clean_update(self, link_table, link_col, value_for_clear):
        """
        Delete links matching one column value or a batch of parameter rows.

        An int instance, including bool, uses execute with a one-item binding. Every
        other input goes directly to executemany and must provide parameter sequences; a
        plain scalar or flat integer list is not adapted. Matching uses SQL equality, so
        a bound None does not select null cells.

        Example:
            ``macros.generic_clean_update("links", "left_id", [(1,), (2,)])``
            deletes links belonging to either listed left endpoint.


        :param link_table: Trusted physical link-table name interpolated directly into
            SQL.
        :param link_col: Trusted link-column name used in the equality predicate.
        :param value_for_clear: An int/bool for one execution, or an iterable of one-
            value parameter sequences.
        :return: None; matching rows are deleted through the wrapper.
        """
        del_stmt = "DELETE FROM {0} WHERE {1}=?".format(link_table, link_col)
        if isinstance(value_for_clear, int):
            self.execute(del_stmt, (value_for_clear,))
        else:
            self.executemany(del_stmt, value_for_clear)

    #
    # ------------------------------------------------------------------------------------------------------------------

    def get_foreign_key_replacement_trigger(self, target_table, search_column="book", target_id="book_id", old=True):
        """
        Build a DELETE statement for use inside a trigger body.

        Interpolate table and column names and always reference OLD.target_id. The old
        argument is currently ignored, including when false. This returns statement text
        only; it neither creates a trigger nor executes the DELETE.

        Example:
            >>> from types import SimpleNamespace
            >>> macros = SQLiteDatabaseMacros(SimpleNamespace())
            >>> macros.get_foreign_key_replacement_trigger("links", "owner_id", "id", old=False)
            'DELETE FROM links WHERE owner_id=OLD.id;'


        :param target_table: Trusted table from which dependent rows should be deleted.
        :param search_column: Trusted target-table column compared with the OLD row
            value.
        :param target_id: Trusted column name on the trigger's OLD row.
        :param old: Retained compatibility argument; has no effect on the generated
            statement.
        :return: A semicolon-terminated DELETE statement referencing the trigger's OLD
            row.
        """
        return "DELETE FROM {} WHERE {}=OLD.{};".format(target_table, search_column, target_id)

    #
    # ------------------------------------------------------------------------------------------------------------------


    # Todo: Merge with the below
    def direct_update_column_in_table(self, table, column, table_id_col, item_id, new_value):
        """
        Update a column and, when supported, its built-in normalized identity key.

        Consult default_normalized_identity_spec, not a database identity-catalog
        lookup. If a default exists and its key column is physically present, normalize
        the replacement with that default profile and update both values in one
        statement. None clears both columns. Otherwise update only the requested value.
        Names are interpolated, data is bound, and validation/database failures
        propagate.

        Example:
            ``macros.direct_update_column_in_table("tags", "tag", "tag_id", 1,
            "Science Fiction")`` also updates tag_phash when that key column exists.


        :param table: Trusted SQL table name interpolated without quoting or validation.
        :param column: Trusted column name interpolated directly into SQL.
        :param table_id_col: Trusted column used to match the target row ID.
        :param item_id: Row identifier used to select the record to update.
        :param new_value: Replacement value, including None for an SQL null.
        :return: None; the update statement result is discarded.
        """
        spec = default_normalized_identity_spec(table, column)
        if (
            spec is not None
            and spec.identity_column
            in set(self.db.driver_wrapper.get_column_headings(table))
        ):
            identity_value = (
                None
                if new_value is None
                else normalize_identity_value(
                    new_value,
                    spec.normalization_profile,
                )
            )
            stmt = "UPDATE {0} SET {1} = ?, {2} = ? WHERE {3} = ?;".format(
                table,
                column,
                spec.identity_column,
                table_id_col,
            )
            self.execute(stmt, (new_value, identity_value, item_id))
        else:
            stmt = "UPDATE {0} SET {1} = ? WHERE {2} = ?;".format(
                table,
                column,
                table_id_col,
            )
            self.execute(stmt, (new_value, item_id))

    def update_column_in_table(self, table, column, table_id_col, item_id, new_value):
        """
        Assign a value through a facade row object and synchronize that row.

        Fetch the row by table and item_id, set row[column], then call sync(). The
        table_id_col argument is unused. Missing-row, assignment and synchronization
        errors propagate; this method does not issue its own direct UPDATE.

        Example:
            ``macros.update_column_in_table("works", "work_title", "work_id", 1,
            "Revised")`` updates the retrieved row and calls its sync method.


        :param table: Table passed to the facade row lookup.
        :param column: Key assigned on the retrieved row object.
        :param table_id_col: Compatibility argument, ignored by this row-based
            implementation.
        :param item_id: Row identifier used to select the record to update.
        :param new_value: Replacement value, including None for an SQL null.
        :return: None after synchronization completes.
        """
        # Todo: Why isn't this working?
        # stmt = "UPDATE {0} SET {1} = ? WHERE {2} = ?;".format(table, column, table_id_col)
        # self.execute(stmt, (item_id, new_value))
        target_row = self.db.get_row_from_id(table, item_id)
        target_row[column] = new_value
        target_row.sync()

    # Todo: Merge with the driver method - which does the same thing - dry the code base out
    def get_unique_values(self, table, column):
        """
        Collect first-column query values into a Python set.

        Read the whole selected column without ordering. Duplicates collapse and SQL
        nulls remain as None. Values must be hashable; wrapper or set-insertion errors
        propagate.

        Example:
            ``macros.get_unique_values("works", "work_title")`` returns distinct
            query values, including None if the column contains SQL nulls.


        :param table: Trusted SQL table name interpolated without quoting or validation.
        :param column: Trusted column name interpolated directly into SQL.
        :return: A set of selected values, empty when the query yields no rows.
        """
        current_values = set()
        stmt = "SELECT {} FROM {};".format(column, table)
        for row in self.execute(stmt):
            current_values.add(row[0])
        return current_values

    def get_values_one_condition(self, table, rtn_column, cond_column, value, default_value=None):
        """
        Collect distinct values satisfying one parameter-bound equality condition.

        Return a set, including an empty set for no matches. default_value is used only
        when execution, iteration or adding a result to the set raises TypeError; it is
        not the missing-row result. Any partial set is discarded on that error. Other
        exceptions propagate. A bound None uses SQL equality rather than IS NULL.

        Example:
            >>> import sqlite3
            >>> from types import SimpleNamespace
            >>> conn = sqlite3.connect(":memory:")
            >>> _ = conn.execute("CREATE TABLE items (id INTEGER, label TEXT)")
            >>> macros = SQLiteDatabaseMacros(SimpleNamespace(driver_wrapper=conn))
            >>> macros.get_values_one_condition("items", "label", "id", 1, default_value="fallback")
            set()
            >>> conn.close()


        :param table: Trusted SQL table name interpolated without quoting or validation.
        :param rtn_column: Trusted selected-column name.
        :param cond_column: Trusted column compared with the bound condition value.
        :param value: Condition value bound to an SQL equality predicate.
        :param default_value: Fallback returned only for TypeError, including unhashable
            result values.
        :return: A set of matches, or default_value after a caught TypeError.
        """
        current_values = set()
        stmt = "SELECT {0} FROM {1} WHERE {2} = ?;".format(rtn_column, table, cond_column)
        try:
            for row in self.execute(stmt, (value,)):
                current_values.add(row[0])
        except TypeError:
            return default_value
        return current_values

    #
    # ------------------------------------------------------------------------------------------------------------------



    # ------------------------------------------------------------------------------------------------------------------
    #
    # - BULK DELETE METHODS

    # Todo: Rename bulk delete by values
    def bulk_delete_in_table(self, table, column, column_values):
        """
        Execute one equality-based DELETE for each supplied parameter row.

        Bindings are passed unchanged to executemany; each must contain one value. None
        bindings do not match null cells through SQL equality. Transaction and partial-
        failure behavior are delegated to the wrapper.

        Example:
            ``macros.bulk_delete_in_table("works", "work_id", [(1,), (2,)])``
            deletes rows whose IDs match either parameter row.


        :param table: Trusted SQL table name interpolated without quoting or validation.
        :param column: Trusted column name interpolated directly into SQL.
        :param column_values: Iterable of one-value parameter sequences, not a flat
            sequence of scalar IDs.
        :return: None; the executemany result is discarded.
        """
        self.executemany("DELETE FROM {0} WHERE {1}=?".format(table, column), column_values)

    def bulk_delete_items_in_table_two_matching_cols(self, table, col_1, col_2, column_values):
        """
        Delete rows matching both bound column values for each parameter pair.

        Use equality predicates joined by AND and pass parameter pairs unchanged to
        executemany. This helper creates no transaction or null-specific predicate.

        Example:
            ``macros.bulk_delete_items_in_table_two_matching_cols("links",
            "left_id", "right_id", [(1, 2)])`` deletes that endpoint pair.


        :param table: Trusted SQL table name interpolated without quoting or validation.
        :param col_1: Trusted first match-column name.
        :param col_2: Trusted second match-column name.
        :param column_values: Iterable of (first-column value, second-column value)
            parameter sequences.
        :return: None; the executemany result is discarded.
        """
        stmt = "DELETE FROM {0} WHERE {1}=? AND {2}=?;".format(table, col_1, col_2)
        self.executemany(stmt, column_values)

    def delete_in_table(self, table, column, value):
        """
        Delete every row whose selected column equals one bound value.

        No match is a normal no-op. A None binding does not match SQL nulls because the
        predicate uses equality rather than IS NULL.

        Example:
            ``macros.delete_in_table("links", "left_id", 1)`` removes every link
            with that left endpoint.


        :param table: Trusted SQL table name interpolated without quoting or validation.
        :param column: Trusted column name interpolated directly into SQL.
        :param value: Scalar bound to the equality predicate.
        :return: None; the execution result is discarded.
        """
        del_stmt = "DELETE FROM {0} WHERE {1}=?;".format(table, column)
        self.execute(del_stmt, (value,))

    def bulk_update_link_table(self, link_table, update_column, other_column, values):
        """
        Repoint selected links using a new value and two matching old values.

        Each binding contains three values: new update-column value, old update-column
        value, and other-column value. Both old values must match. Parameter triples
        pass unchanged to executemany; no priority or type recalculation is performed.

        Example:
            ``macros.bulk_update_link_table("links", "left_id", "right_id",
            [(5, 1, 2)])`` changes the left endpoint from 1 to 5 for right endpoint 2.


        :param link_table: Trusted physical link-table name interpolated directly into
            SQL.
        :param update_column: Trusted column to replace and also compare against its old
            value.
        :param other_column: Trusted second match-column name, left unchanged.
        :param values: Iterable of (new update value, old update value, other-column
            value) triples.
        :return: None; the repeated UPDATE result is discarded.
        """
        stmt = "UPDATE {0} SET {1} = ? WHERE {1} = ? AND {2} = ?".format(link_table, update_column, other_column)
        self.executemany(stmt, values)

    def bulk_add_links(self, link_table, src_col, dst_col, values):
        """
        Insert endpoint pairs, assigning per-source priorities when the schema has them.

        Materialize values as a tuple and inspect headings for the conventional column-
        base plus _priority. Without it, insert pairs directly. Otherwise read each
        source's current maximum priority once, increment it for every pair in input
        order, and insert the prepared triples. Repeated pairs remain repeated and link
        types are not set explicitly. No lock or enclosing transaction protects the
        maximum read from concurrent writes.

        Example:
            ``macros.bulk_add_links("demo_links", "demo_link_left_id",
            "demo_link_right_id", [(1, 10), (1, 11)])`` assigns successive priorities
            for source 1 when demo_link_priority exists.


        :param link_table: Trusted physical link-table name interpolated directly into
            SQL.
        :param src_col: Trusted source endpoint column used to group priority
            allocation.
        :param dst_col: Trusted destination endpoint column.
        :param values: Iterable of (source ID, destination ID) pairs, consumed before
            inspecting the schema.
        :return: None; the prepared batch is passed to executemany.
        """
        values = tuple(values)
        headings = set(self.db.driver_wrapper.get_column_headings(link_table))
        priority_col = "{}_priority".format(
            self.db.driver_wrapper.get_column_base(link_table)
        )
        if priority_col not in headings:
            stmt = "INSERT INTO {0}({1}, {2}) VALUES (?,?);".format(
                link_table,
                src_col,
                dst_col,
            )
            self.executemany(stmt, values)
            return

        next_priorities = {}
        prepared_values = []
        for src_id, dst_id in values:
            if src_id not in next_priorities:
                row = next(
                    iter(
                        self.execute(
                            "SELECT COALESCE(MAX({0}), 0) FROM {1} WHERE {2}=?".format(
                                priority_col,
                                link_table,
                                src_col,
                            ),
                            (src_id,),
                        )
                    )
                )
                next_priorities[src_id] = row[0]
            next_priorities[src_id] += 1
            prepared_values.append((src_id, dst_id, next_priorities[src_id]))
        self.executemany(
            "INSERT INTO {0}({1}, {2}, {3}) VALUES (?,?,?);".format(
                link_table,
                src_col,
                dst_col,
                priority_col,
            ),
            prepared_values,
        )

    def reprioritize_link(
        self,
        link_table,
        left_link_col,
        right_link_col,
        left_id,
        right_id,
        new_type=None,
        new_priority="MAX",
    ):
        """
        Move matching endpoint links above the current left-endpoint maximum.

        Assert that new_priority equals MAX, then derive the conventional priority
        column and update matching pairs to COALESCE(MAX(priority), 0) + 1 for that left
        endpoint. Existing matching rows participate in the maximum. With new_type
        supplied, first perform the priority update recursively, then issue a separate
        type update. These two writes have no added atomicity guarantee. Disabling
        assertions removes the mode check; the SQL still uses the maximum.

        Example:
            ``macros.reprioritize_link("demo_links", "demo_link_left_id",
            "demo_link_right_id", 1, 10)`` moves that link to the highest priority
            within source 1.


        :param link_table: Trusted physical link-table name interpolated directly into
            SQL.
        :param left_link_col: Trusted column naming the left endpoint in the link table.
        :param right_link_col: Trusted column naming the right endpoint in the link
            table.
        :param left_id: Left endpoint value bound as a query parameter.
        :param right_id: Right endpoint value bound as a query parameter.
        :param new_type: Replacement link type, or None to leave the type unchanged;
            None cannot clear the type.
        :param new_priority: Only the literal MAX is supported when assertions are
            enabled.
        :return: None; missing matches do not create a link.
        """
        assert new_priority == "MAX", "Only max mode is supported at the moment"

        link_base_col = self.db.driver_wrapper.get_column_base(link_table)
        link_priority_col = "{0}_priority".format(link_base_col)

        if new_type is None:
            stmt = (
                "UPDATE {0} "
                "SET {1} = (SELECT COALESCE(MAX({1}), 0) + 1 FROM {0} WHERE {2} = ?) "
                "WHERE {2} = ? AND {3} = ?;"
            ).format(link_table, link_priority_col, left_link_col, right_link_col)
            self.execute(stmt, (left_id, left_id, right_id))
        else:
            # First change the priority
            self.reprioritize_link(
                link_table=link_table,
                left_link_col=left_link_col,
                right_link_col=right_link_col,
                left_id=left_id,
                right_id=right_id,
                new_type=None,
                new_priority=new_priority,
            )
            # Then change the link type
            link_type_col = "{0}_type".format(link_base_col)
            stmt = "UPDATE {0} SET {3} = ? WHERE {1} = ? AND {2} = ?;".format(
                link_table, left_link_col, right_link_col, link_type_col
            )
            self.execute(stmt, (new_type, left_id, right_id))

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - READ METHODS - FOR READING DATA FROM THE BACKEND
    def read_link_property_trios(self, link_table, link_property_col, first_id, second_id):
        """
        Return the wrapper query result for link property and endpoint columns.

        Select all rows in the order (property, first endpoint, second endpoint),
        without sorting, copying or filtering. Despite their parameter names, first_id
        and second_id are column identifiers, not bound ID values.

        Example:
            ``macros.read_link_property_trios("links", "priority", "left_id",
            "right_id")`` yields those three cells per link in wrapper query order.


        :param link_table: Trusted physical link-table name interpolated directly into
            SQL.
        :param link_property_col: Trusted column containing the property to select
            first.
        :param first_id: Trusted first endpoint column name.
        :param second_id: Trusted second endpoint column name.
        :return: The wrapper execute result, normally iterable rows of three values.
        """
        stmt = "SELECT {0}, {1}, {2} FROM {3};".format(link_property_col, first_id, second_id, link_table)
        return self.execute(stmt)

    def get_all_table_link_data(self, table1, table2, typed=False, priority=False):
        """
        Collect link containers for every primary-table row using the physical link
        spec.

        Return an empty dictionary if no spec exists. Otherwise request bulk link rows
        for all facade primary-row IDs, including empty groups. Without typed grouping,
        map each primary ID directly to a set or list of secondary IDs. With typed
        grouping, each value is a defaultdict keyed by link_type with sets or lists.
        Sets collapse duplicates; lists preserve the portable reader's order, descending
        stored priority then secondary ID when a priority column exists. The priority
        flag chooses the container, not a separate query order.

        Example:
            With an existing link spec, ``macros.get_all_table_link_data("works",
            "agents", typed=False, priority=False)`` has the shape
            ``{work_id: {agent_id, ...}}``, including empty sets for unlinked works.


        :param table1: Primary table whose row IDs key the link lookup.
        :param table2: Secondary table whose linked IDs are collected.
        :param typed: Group returned secondary IDs by link_type when a link spec exists.
        :param priority: Return ordered lists retaining duplicates instead of sets.
        :return: A dictionary keyed by primary ID; values are sets/lists or typed
            defaultdicts.
        """

        link_spec = self.db.driver_wrapper.get_link_spec(table1, table2)
        if link_spec is None:
            return {}
        primary_ids = tuple(row.row_id for row in self.db.get_all_rows(table1))
        grouped = self.get_link_rows_bulk(link_spec, primary_ids)
        all_table_link_data = {}
        for primary_id, rows in grouped.items():
            if not typed and not priority:
                all_table_link_data[primary_id] = {
                    row.secondary_id for row in rows
                }
            elif not typed and priority:
                all_table_link_data[primary_id] = [
                    row.secondary_id for row in rows
                ]
            elif typed and not priority:
                link_data = defaultdict(set)
                for row in rows:
                    link_data[row.link_type].add(row.secondary_id)
                all_table_link_data[primary_id] = link_data
            else:
                link_data = defaultdict(list)
                for row in rows:
                    link_data[row.link_type].append(row.secondary_id)
                all_table_link_data[primary_id] = link_data
        return all_table_link_data

    def get_link_data(self, table1, table2, table1_id, typed=False, priority=False):
        """
        Collect linked secondary IDs for one primary ID, optionally grouped by type.

        With a link spec, untyped output is a set or ordered list; typed output is a
        defaultdict of those containers keyed by link_type. Lists retain duplicates and
        portable-reader order: descending priority then secondary ID when the spec has a
        priority column. The priority flag only changes containers. Without a spec,
        return an empty list when priority is true, otherwise an empty set, even when
        typed is true.

        Example:
            >>> from types import SimpleNamespace
            >>> row = SimpleNamespace(secondary_id=3, link_type="author")
            >>> wrapper = SimpleNamespace(get_link_spec=lambda *args: object())
            >>> host = SimpleNamespace(db=SimpleNamespace(driver_wrapper=wrapper),
            ...                        get_link_rows=lambda *args: (row, row))
            >>> for typed, priority in [(False, False), (False, True), (True, False), (True, True)]:
            ...     result = SQLiteDatabaseMacros.get_link_data(host, "works", "agents", 1, typed, priority)
            ...     print(dict(result) if typed else result)
            {3}
            [3, 3]
            {'author': {3}}
            {'author': [3, 3]}
            >>> wrapper.get_link_spec = lambda *args: None
            >>> SQLiteDatabaseMacros.get_link_data(host, "works", "agents", 1, typed=True, priority=True)
            []


        :param table1: Primary table whose row IDs key the link lookup.
        :param table2: Secondary table whose linked IDs are collected.
        :param table1_id: Primary endpoint ID passed to the portable link-row reader.
        :param typed: Group returned secondary IDs by link_type when a link spec exists.
        :param priority: Return ordered lists retaining duplicates instead of sets.
        :return: A set/list of secondary IDs, or a typed defaultdict when a spec is
            available.
        """
        link_spec = self.db.driver_wrapper.get_link_spec(table1, table2)
        if link_spec is None:
            return [] if priority else set()
        rows = self.get_link_rows(link_spec, table1_id)
        if not typed and not priority:
            return {row.secondary_id for row in rows}
        if not typed and priority:
            return [row.secondary_id for row in rows]
        if typed and not priority:
            link_container = defaultdict(set)
            for row in rows:
                link_container[row.link_type].add(row.secondary_id)
            return link_container
        link_container = defaultdict(list)
        for row in rows:
            link_container[row.link_type].append(row.secondary_id)
        return link_container


    def get_linked_ids(self, link_table, left_id_col, right_id_col, left_id, type_filter=None):
        """
        Read distinct right-endpoint IDs for one bound left endpoint.

        With a non-null type_filter, derive the conventional type column from the
        wrapper's link-table column base and bind an additional equality condition. None
        means no type filter, so it cannot select only null types. Results are unsorted
        and duplicate IDs collapse.

        Example:
            ``macros.get_linked_ids("links", "left_id", "right_id", 1)`` returns
            a set of all right endpoints linked to left endpoint 1.


        :param link_table: Trusted physical link-table name interpolated directly into
            SQL.
        :param left_id_col: Trusted column matched against left_id.
        :param right_id_col: Trusted column whose values are returned.
        :param left_id: Left endpoint value bound as a query parameter.
        :param type_filter: Optional bound type value; None disables the type predicate.
        :return: A set of selected endpoint values, empty for no matching rows.
        """
        if type_filter is None:
            stmt = "SELECT {0} FROM {1} WHERE {2} = ?;".format(right_id_col, link_table, left_id_col)
            return set(row[0] for row in self.execute(stmt, (left_id,)))
        else:
            link_type_col = "{0}_type".format(self.db.driver_wrapper.get_column_base(link_table))
            stmt = "SELECT {0} FROM {1} WHERE {2} = ? AND {3} = ?;".format(
                right_id_col, link_table, left_id_col, link_type_col
            )
            return set(row[0] for row in self.execute(stmt, (left_id, type_filter)))


    #
    # ------------------------------------------------------------------------------------------------------------------


    def replace_in_folder_store_path(self, target_str: str, replacement: str) -> None:
        """
        Replace literal substrings throughout the stored folder_store_path column.

        Apply the SQL replace function to every row in folder_stores with both strings
        bound as data. There is no row filter or path-boundary matching. This edits
        catalog text only; it does not move files, directories or marker files. Commit
        behavior belongs to the execution wrapper.

        Example:
            ``macros.replace_in_folder_store_path("/old/root", "/new/root")``
            updates matching text wherever it occurs in each stored value.


        :param target_str: Literal substring passed to the SQL replace function.
        :param replacement: Replacement substring bound as data, not a filesystem move.
        :return: None; the UPDATE result is discarded.
        """
        replace_sql = "UPDATE folder_stores SET folder_store_path = replace(folder_store_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    def replace_in_folder_store_marker_path(self, target_str: str, replacement: str) -> None:
        """
        Replace literal substrings throughout the stored folder_store_marker_path
        column.

        Apply the SQL replace function to every row in folder_stores with both strings
        bound as data. There is no row filter or path-boundary matching. This edits
        catalog text only; it does not move files, directories or marker files. Commit
        behavior belongs to the execution wrapper.

        Example:
            ``macros.replace_in_folder_store_marker_path("/old/root", "/new/root")``
            updates matching text wherever it occurs in each stored value.


        :param target_str: Literal substring passed to the SQL replace function.
        :param replacement: Replacement substring bound as data, not a filesystem move.
        :return: None; the UPDATE result is discarded.
        """
        replace_sql = "UPDATE folder_stores SET folder_store_marker_path = replace(folder_store_marker_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    def replace_in_folder_path(self, target_str: str, replacement: str) -> None:
        """
        Replace literal substrings throughout the stored folder_path column.

        Apply the SQL replace function to every row in folders with both strings bound
        as data. There is no row filter or path-boundary matching. This edits catalog
        text only; it does not move files, directories or marker files. Commit behavior
        belongs to the execution wrapper.

        Example:
            ``macros.replace_in_folder_path("/old/root", "/new/root")``
            updates matching text wherever it occurs in each stored value.


        :param target_str: Literal substring passed to the SQL replace function.
        :param replacement: Replacement substring bound as data, not a filesystem move.
        :return: None; the UPDATE result is discarded.
        """
        replace_sql = "UPDATE folders SET folder_path = replace(folder_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    # Todo: Do you need two different unique ids stores in two different places?
    def set_library_id(self, new_val):
        """
        Update all legacy library UUID rows, or insert one if the table is empty.

        Use the wrapper record count to choose between an unrestricted UPDATE and an
        INSERT. The value is bound without UUID validation. Multiple existing rows are
        all updated; no singleton constraint or transaction is established here.

        Example:
            ``macros.set_library_id("library-001")`` stores that literal value
            in library_id_uuid on the legacy library_id table.


        :param new_val: New library identifier bound without format validation.
        :return: None; the write result is discarded.
        """
        if self.db.driver_wrapper.get_record_count("library_id"):
            self.execute("UPDATE library_id SET library_id_uuid = ?", (new_val,))

        else:
            self.execute("INSERT INTO library_id (library_id_uuid) VALUES (?);", (new_val,))

    def set_database_version(self, new_val):
        """
        Update all legacy version rows, or insert one if the table is empty.

        The record count chooses an unrestricted UPDATE or an INSERT into
        database_version_version. This changes a stored marker only: it performs no
        schema migration, version validation or transaction coordination.

        Example:
            ``macros.set_database_version("2")`` changes the stored version marker
            without upgrading the schema.


        :param new_val: New database-version marker bound without format validation.
        :return: None; the write result is discarded.
        """
        if self.db.driver_wrapper.get_record_count("database_version"):
            self.execute("UPDATE database_version SET database_version_version = ?", (new_val,))

        else:
            self.execute(
                "INSERT INTO database_version (database_version_version) VALUES (?);",
                (new_val,),
            )
