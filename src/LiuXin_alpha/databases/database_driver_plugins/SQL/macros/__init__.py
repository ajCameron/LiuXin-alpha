

# Todo: This feels like it needs a bit of a rethink

"""
Macros are pre-defined operation on the database - intended for speedup.
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
    Provides pre-defined operations on an SQLite database.
    """

    def __init__(self, db: "DatabaseAPI") -> None:
        """
        Attaches to the underlying database to provide additional services.
        :param db:
        """
        super(SQLiteDatabaseMacros, self).__init__(db=db)

    # Todo - These should probably be semi private
    @property
    def get(self):
        """
        For compatibility.

        :param args:
        :param kwargs:
        :return:
        """
        return self.db.get

    @property
    def execute(self):
        """
        For compatibility.

        :return:
        """
        return self.db.driver_wrapper.execute

    @property
    def executemany(self):
        """
        For compatibility.

        :return:
        """
        return self.db.driver_wrapper.executemany

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - LINK MAKING METHODS

    def make_generic_link(self, link_table, left_link_col, right_link_col, priority_col, left_id, right_id):
        """
        Make a generic link between two entities with the next local priority.

        :param link_table:
        :param left_link_col:
        :param right_link_col:
        :param priority_col:
        :param left_id:
        :param right_id:
        :return:
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
        Write a generic link without priority or anything else. Just forming the link.
        :param link_table:
        :param left_link_col:
        :param right_link_col:
        :param left_id:
        :param right_id:
        :return:
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
        Preforms a generic clean.

        :param link_table:
        :param link_col:
        :param value_for_clear:
        :return:
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
        Used when, for whatever reason, we don't want to use a full foreign key.
        """
        return "DELETE FROM {} WHERE {}=OLD.{};".format(target_table, search_column, target_id)

    #
    # ------------------------------------------------------------------------------------------------------------------


    # Todo: Merge with the below
    def direct_update_column_in_table(self, table, column, table_id_col, item_id, new_value):
        """
        Preform an update of a column in a specified table.
        :param table:
        :param column:
        :param item_id:
        :param new_value:
        :param table_id_col:
        :return:
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
        Preform an update of a column in a specified table.
        :param table:
        :param column:
        :param item_id:
        :param new_value:
        :param table_id_col:
        :return:
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
        Returns a set of all the values of the given column of the table.
        :param table:
        :param column:
        :return:
        """
        current_values = set()
        stmt = "SELECT {} FROM {};".format(column, table)
        for row in self.execute(stmt):
            current_values.add(row[0])
        return current_values

    def get_values_one_condition(self, table, rtn_column, cond_column, value, default_value=None):
        """
        Return all the values in a table which satisfy one condition.
        :param table:
        :param rtn_column:
        :param cond_column:
        :param value:
        :param default_value: Return this if the book_id doesn't exist in the table
        :return:
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
        Preform a bulk delete in a specified table.
        :param table: Table to remove the values from
        :param column: Column in the table
        :param column_values: All entries in the column with a value in this object will be removed
        :return:
        """
        self.executemany("DELETE FROM {0} WHERE {1}=?".format(table, column), column_values)

    def bulk_delete_items_in_table_two_matching_cols(self, table, col_1, col_2, column_values):
        """
        Preform a bulk delete in a specified table - of entries matching two conditions.
        :param table:
        :param col_1:
        :param col_2:
        :param column_values:
        :return:
        """
        stmt = "DELETE FROM {0} WHERE {1}=? AND {2}=?;".format(table, col_1, col_2)
        self.executemany(stmt, column_values)

    def delete_in_table(self, table, column, value):
        """
        Delete from the table whenever the given value shows up.
        :param table:
        :param column:
        :param value:
        :return:
        """
        del_stmt = "DELETE FROM {0} WHERE {1}=?;".format(table, column)
        self.execute(del_stmt, (value,))

    def bulk_update_link_table(self, link_table, update_column, other_column, values):
        """
        Preform a bulk update on a link table - used for repoiting links - i.e. moving an identifier between titles
        :param link_table: The name of the link table
        :param update_column: The column to update
        :param other_column: The other link column
        :param values: iterable of tuples with the first element being for the column to update and the second element
                       being the other column to update
        :return:
        """
        stmt = "UPDATE {0} SET {1} = ? WHERE {1} = ? AND {2} = ?".format(link_table, update_column, other_column)
        self.executemany(stmt, values)

    def bulk_add_links(self, link_table, src_col, dst_col, values):
        """
        Bulk add links to a link table.
        Type e.t.c setting is not supported.
        :param link_table:
        :param src_col:
        :param dst_col:
        :param values:
        :return:
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
        Change the priority of a link.
        :param link_table:
        :param left_link_col:
        :param right_link_col:
        :param left_id:
        :param right_id:
        :param new_type: If this is specified, then the type will be changed
        :return:
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
        Used when caching additional properties associated with a link. Returns a trio of values of the form
        (link_property_col, first_id, second_id).
        E.g. if you want the index values of all the series_title_links then call this method with
        (series_title_links, series_title_link_index, series_title_link_series_id, series_title_link_title_id)
        and you will receive back an iterable of trios of those three values.
        :param link_table: The name of the link table to read from
        :param link_property_col:
        :param first_id:
        :param second_id:
        :return:
        """
        stmt = "SELECT {0}, {1}, {2} FROM {3};".format(link_property_col, first_id, second_id, link_table)
        return self.execute(stmt)

    def get_all_table_link_data(self, table1, table2, typed=False, priority=False):
        """
        Method to return all the link data in a given table.

        if the link is neither typed or priority, then the return will be a dictionary of dictionaries, keyed with the
        id of the first table, then the id of the second table, then a set of all the ids

        if the link is priority, but not types, the sets becomes lists of all the ids in table2, in priority order

        if the link is typed, but not priority, the return becomes a dictionary of dicts of dicts.
        First level keyed with the ids from table1, then the types, then sets of all the ids fro table2

        If the link is both types and priority the dict of dicts of dicts structure remains, but the last level is
        valued with lists of the ids in priority order, not sets.

        :param table1: The primary table
        :param table2: The secondary table
        :param typed:
        :param priority:
        :return:
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
        Return an object containing the data for all the items in table2 linked to table1.
        :param table1:
        :param table2:
        :param table1_id:
        :param typed:
        :param priority:
        :return:
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
        Return the ids linked to the given left_id in the specified link table.
        :param link_table:
        :param left_id_col:
        :param left_id:
        :param type_filter: If it's specified (not None) then only entries with the given type will be returned
        :return:
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
        Replace text in the physical folder-store path column.

        :param target_str:
        :param replacement:
        :return:
        """
        replace_sql = "UPDATE folder_stores SET folder_store_path = replace(folder_store_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    def replace_in_folder_store_marker_path(self, target_str: str, replacement: str) -> None:
        """
        Replace text in the folder-store marker path column.

        :param target_str:
        :param replacement:
        :return:
        """
        replace_sql = "UPDATE folder_stores SET folder_store_marker_path = replace(folder_store_marker_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    def replace_in_folder_path(self, target_str: str, replacement: str) -> None:
        """
        Replace text in folder paths.

        :param target_str:
        :param replacement:
        :return:
        """
        replace_sql = "UPDATE folders SET folder_path = replace(folder_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    # Todo: Do you need two different unique ids stores in two different places?
    def set_library_id(self, new_val):
        """
        Preform a set of the library id.
        :param new_val:
        :return:
        """
        if self.db.driver_wrapper.get_record_count("library_id"):
            self.execute("UPDATE library_id SET library_id_uuid = ?", (new_val,))

        else:
            self.execute("INSERT INTO library_id (library_id_uuid) VALUES (?);", (new_val,))

    def set_database_version(self, new_val):
        """
        Preform a set of the database_version.

        This should be possible, but rarely used. I'm leaving the code in her in anticipation that, when multiple
        versions of LiuXin are in the wild, we might want to allow for database upgrades.
        So you might, then, want to be able to change the version.
        :param new_val:
        :return:
        """
        if self.db.driver_wrapper.get_record_count("database_version"):
            self.execute("UPDATE database_version SET database_version_version = ?", (new_val,))

        else:
            self.execute(
                "INSERT INTO database_version (database_version_version) VALUES (?);",
                (new_val,),
            )
