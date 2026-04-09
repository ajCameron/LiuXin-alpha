

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.errors import InputIntegrityError


class DriverWrapperTreeMixin:
    """
    Methods to handle tree like structures in the database.
    """

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO DEAL WITH TREE STRUCTURES IN TABLES
    # ------------------------------------------------------------------------------------------------------------------
    # Todo: Needs to throw an error when used on a table without a tree structure
    def get_linear_row_list(self, start_row):
        """
        Takes a starting row. Iterates up the tree, making an index of rows as it goes.
        Starts from the highest entry, then proceeds down.
        .......... -> grandparent_series -> parent_series -> series
        :param start_row:
        :return tree_row_index:
        """
        table = self.identify_table_from_row_dict(start_row)
        table_parent_column = self.get_parent_column(table)

        linear_rows = []
        current_row = start_row
        try:
            current_parent_id = start_row[table_parent_column]
            if six_unicode(current_parent_id).lower() == "none" or current_parent_id is None:
                linear_rows.append(start_row)
                return linear_rows
        except KeyError:
            linear_rows.append(start_row)
            return linear_rows

        while six_unicode(current_parent_id).upper() != "NONE" and current_parent_id is not None:
            # extracting the current parent id
            try:
                current_parent_id = current_row[table_parent_column]
                if current_parent_id == "NONE":
                    linear_rows = [current_row] + linear_rows
                    return linear_rows
            except KeyError:
                linear_rows = [current_row] + linear_rows
                return linear_rows

            linear_rows = [current_row] + linear_rows

            if six_unicode(current_parent_id).lower() != "none" and current_parent_id is not None:
                current_row = self.get_row_from_id(table=table, row_id=current_parent_id)
            else:
                break

        return linear_rows

    # Todo: Again, should error when called on a table which does not have a tree structure
    def set_tree_ids(self, table):
        """
        Every tree should have a unique tree id - this goes through and makes sure it's been set for every tree in the
        given table.
        :param table:
        :return:
        """
        return self.driver.direct_set_tree_ids(table)

    def set_full_column(self, table):
        """
        Rows which are part of a tree structure have a _full column. This is a string representation of their place in
        the tree structure. This method populates the full column for the target table.
        :param table:
        :return:
        """
        return self.driver.direct_set_full_column(target_table=table)

    def walk(self, start_row):
        """
        Walk the tree yielding all the rows in it, starting with the start_row itself.
        :param start_row: Walk starts here.
        :return:
        """
        table = self.identify_table_from_row_dict(start_row)
        table_id_col = self.get_id_column(table)
        table_parent_col = self.get_parent_column(table)

        if table_parent_col is None or table_parent_col is False:
            err_str = "Given table does not have a tree structure - so can't be walked"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("start_row", start_row),
                ("table", table),
                ("table_id_col", table_id_col),
                ("table_parent_col", table_parent_col),
            )
            raise InputIntegrityError(err_str)

        return self._walk(start_row, table, table_id_col, table_parent_col)

    def _walk(self, start_row, table, table_id_col, table_parent_col):
        # Load the ids pool with the ids of parent rows - search for them in the parent column and yield those rows
        # If a row has no children (not referenced in any parent column) then it's a leaf row and we're done for that
        # branch
        ids_pool = set()
        ids_pool.add(int(start_row[table_id_col]))

        # Start the walk by yielding the start row - then working through the ids pool - take each id from it, find all
        # the children, yield them and add their ids for recursion on down. Continue until all rows have been yielded.
        yield start_row
        while ids_pool:

            working_id = ids_pool.pop()
            working_children = self.search(table=table, column=table_parent_col, search_term=working_id)

            for child_row in working_children:
                ids_pool.add(int(child_row[table_id_col]))
                yield child_row
