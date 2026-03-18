
import sqlite3
from copy import deepcopy


from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode, force_unicode
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError, DatabaseDriverError

from LiuXin_alpha.constants import VERBOSE_DEBUG

from LiuXin_alpha.utils.logging import default_log


class TreeMethodsMixin:
    """
    Methods to handle tree methods.
    """

    def direct_set_full_column(self, target_table):
        """
        Rows which are part of a tree structure have a _full column. This is a string representation of their place in
        the tree structure. This method populates the full column for the target table.

        Note:
            This operates on "real" rows only; the row iterator intentionally excludes any sentinel/null row at id=0.

        :return:
        """
        target_table = deepcopy(target_table)
        conn = self.get_connection()
        target_table_id_column = self._get_id_column(target_table)
        target_table_full_column = self.get_full_column_name(target_table)
        if target_table_full_column is None:
            err_str = "Cannot set full column - table: {} - does not have one".format(target_table)
            raise InputIntegrityError(err_str)

        target_table_display_column = self.get_display_column(target_table)

        for row in self.direct_get_row_dict_iterator(target_table):

            row_id = row[target_table_id_column]
            agg_value = self.tree_aggregator(
                table=target_table,
                table_display_column=target_table_display_column,
                table_row_id=row_id,
            )

            final_stmt = "UPDATE `{}` SET {} = ? WHERE {} = ?;".format(
                target_table, target_table_full_column, target_table_id_column
            )

            try:
                conn.execute(final_stmt, (agg_value, row_id))
                conn.commit()
            except sqlite3.OperationalError as e:
                err_str = "Unable to complete operation.\n"
                err_str = default_log.log_exception(err_str, e, "ERROR", ("final_stmt", final_stmt), ("row", row))
                raise DatabaseDriverError(err_str)
        else:
            # If the code every reaches this point everything should have worked already
            return True


    # Todo: This is absolutely, hideously, heinously inefficient
    def direct_set_tree_ids(self, table):
        """
        Every tree in a tree like structure should have a unique id assigned to every row in that tree.
        This function ensures that.

        Note:
            We iterate over "real" rows using `direct_get_row_dict_iterator()`, which intentionally excludes
            any sentinel/null row at id=0 (if present). After updating non-null rows, we also set a deterministic
            tree id for id=0 when the row exists, so callers and contract tests get a stable value.

        :param table:
        :return:
        """
        table = deepcopy(table)
        table_id_column = self._get_id_column(table)
        table_tree_id_column = self.get_tree_id_column(table)
        if table_tree_id_column is None:
            err_str = "Cannot set_tree_ids - there doesn't seem to be a tree id for this table - {}".format(table)
            raise InputIntegrityError(err_str)
        table_display_column = self.get_display_column(table)
        conn = self.get_connection()

        stmt = "UPDATE {} SET {} = ? WHERE {} = ?".format(table, table_tree_id_column, table_id_column)
        final_stmt = stmt

        # Walk "real" rows only. The row-dict iterator intentionally skips any sentinel/null row at id=0.
        for row in self.direct_get_row_dict_iterator(table):

            row_id = row[table_id_column]

            root_series = self.get_root_series(row)
            root_phash = "{}_{}".format(root_series[table_id_column], root_series[table_display_column])
            conn.execute(final_stmt, (root_phash, row_id))
            conn.commit()
        else:

            # Handle the sentinel/null row (id=0) explicitly, if present.
            # This keeps iterators clean (real rows only) while still giving id=0 deterministic derived fields.
            null_row = self.direct_get_row_dict_from_id(table, 0)
            if null_row not in (False, None):
                null_display = null_row.get(table_display_column) if isinstance(null_row, dict) else None
                null_tree_id = f"0_{null_display}"
                conn.execute(final_stmt, (null_tree_id, 0))
                conn.commit()

            return True

    # Todo: Should be called direct_get_root_series
    def direct_get_root_series(self, start_row):
        return self.get_root_series(start_row)

    def get_root_series(self, start_row):
        """
        Gets the row at the root of the given tree. In the case of a trivial tree just returns the given row.
        :param start_row:
        :return root_row:
        """
        return self.get_linear_row_index(start_row)[0]

    def direct_get_all_tree_rows(self, start_row):
        """
        Starts from a series. Walks up the series tree, and then walks back down, collecting all references in one set.
        This is going to take a number of database operations.
        :param start_row:
        :return:
        """
        row_table = self.identify_table_from_row(start_row)
        row_parent_column = self.get_parent_column_name(row_table)
        root_series = self.get_root_series(start_row)

        row_pool = [root_series]

        found_series = []

        # the series pool contains the series which we're currently working with as with walk down the series tree
        # series in the series pool haven't had all their children series found yet
        # once a series has had all it's children series found it's transferred to found series
        while len(row_pool) != 0:

            current_series = row_pool.pop()
            current_id = current_series["series_id"]
            # finds all the series which refer to the current_series in the series_parent column
            child_rows = self.direct_search_table(table=row_table, column=row_parent_column, search_term=current_id)
            for row in child_rows:
                if row not in row_pool:
                    row_pool.append(row)
            if current_series not in found_series:
                found_series.append(current_series)

        return found_series


    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - HELPER FUNCTIONS THAT WILL BE ADDED TO THE DATABASE CONNECTION START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    # HELPER FUNCTIONS TO RUN THE TREE AGGREGATOR START HERE
    def tree_aggregator(self, table, table_display_column, table_row_id):
        """
        Builds a string, starting at the current index and working it's way back up to the root of the tree.
        Useful for expressing the position of an element in a tree in a single string.
        For example, used with series it would produce ....: series_grandfather: series_father: series
        :param table: The table to search in
        :param table_display_column: The column to be used as a display column
        :param table_row_id: The id to start at
        :return return_str: ....: row_grandfather: row_father: row - All being the display columns at each level
        """
        start_row = self.direct_get_row_dict_from_id(table, table_row_id)
        row_column_index = self.__get_linear_index_of_columns(start_row, table_display_column)

        row_column_index = [six_unicode(force_unicode(_)) for _ in row_column_index]
        return_str = ": ".join(row_column_index)
        return return_str

    # Todo - Promote this to an actual method with tests
    def _get_linear_index_of_columns(self, start_row, display_column):
        """
        Takes a starting row. Calls get_linear_row_index to get a list of rows with order .......... ->
        grandparent_series -> parent_series -> series. Extracts the designated column from each of these rows to form a
        linear index of columns. Could be used, for example, in series to create a full series string.
        What is actually used is a stripped down version of these functions, which has been added directly to the
        connection.
        :param start_row:
        :param display_column: What column do you want as a display for the
        :return:
        """
        display_column = deepcopy(display_column)
        if display_column not in start_row:
            err_str = "Warning - get_linear_index_of_columns failed. \n"
            err_str += "display_column not found in start_row.\n"
            err_str += "start_row: " + repr(start_row) + "\n"
            err_str += "display_column: " + repr(display_column) + "\n"
            raise InputIntegrityError(err_str)
        row_index = self.get_linear_row_index(start_row)
        row_column_index = []

        for row in row_index:
            if display_column not in row:
                err_str = "Warning - get_linear_index_of_columns failed. \n"
                err_str += "display_column not found in a row.\n"
                err_str += "start_row: " + repr(start_row) + "\n"
                err_str += "display_column: " + repr(display_column) + "\n"
                err_str += "row_column_index: " + repr(row_column_index) + "\n"
                err_str += "row_index: " + repr(row_index) + "\n"
                raise InputIntegrityError(err_str)
            row_column_index.append(row[display_column])

        return row_column_index



    def get_linear_row_index(self, start_row):
        """
        Takes a starting row. Iterates up the tree building an index of all the rows_dicts as it goes.

        :param start_row: A Row that the method will iterate back from
        :return tree_row_index: An index of all the Rows in the tree forwards e.g.
        .......... -> grandparent_series -> parent_series -> series
        """
        start_row_dict = start_row
        row_table = self.identify_table_from_row(start_row_dict)
        row_parent_column = self.get_parent_column_name(row_table)
        linear_index = []
        current_row = start_row_dict
        try:
            current_parent = start_row_dict[row_parent_column]
            if current_parent is None:
                linear_index.append(start_row)
                return linear_index

            elif isinstance(current_parent, int):
                pass

            elif force_unicode(current_parent).upper() == "NONE":
                linear_index.append(start_row)
                return linear_index

        except KeyError:
            linear_index.append(start_row)
            return linear_index

        while force_unicode(six_unicode(current_parent)).upper() != "NONE":
            # extracting the current parent id
            try:
                current_parent = current_row[row_parent_column]
                if current_parent == "NONE":
                    linear_index = [current_row] + linear_index
                    return linear_index
            except KeyError:
                linear_index = [current_row] + linear_index
                return linear_index

            linear_index = [current_row] + linear_index
            if current_parent != "None" and current_parent is not None:
                current_row = self.direct_get_row_dict_from_id(table=row_table, row_id=current_parent)
            else:
                return linear_index

        # If the program ever reaches this point something has gone badly wrong
        if VERBOSE_DEBUG:
            err_str = "get_linear_row_string has failed.\n"
            err_str += "start_row: " + repr(start_row_dict) + "\n"
            raise DatabaseIntegrityError(err_str)
        else:
            raise DatabaseIntegrityError
