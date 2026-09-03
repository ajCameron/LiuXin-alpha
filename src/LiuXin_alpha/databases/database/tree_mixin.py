
"""
Mixin to handle dealing with tree like structures in the database.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.databases.row import Row

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.driver_wrapper_api.driver_wrapper_api import DatabaseDriverWrapperAPI


class DatabaseTreeMixin:
    """Delegate hierarchical row operations from the facade to its driver."""


    driver_wrapper: "DatabaseDriverWrapperAPI"

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO READ TREE STRUCTURES FROM TABLES START HERE

    def get_root_row(self, start_row):
        """
        Get the root series of a tree.

        ALWAYS USE THIS INSTEAD OF get_root_series
        :param start_row:
        :return:
        """
        return self.get_root_series(start_row=start_row)

    # Todo: This method is terribly names - should be merged with the above and removed
    def get_root_series(self, start_row):
        """
        Get the root series of a tree.
        :param start_row:
        :return:
        """
        row_dict_list = self.driver_wrapper.get_linear_row_list(start_row.row_dict)
        return Row(database=self, row_dict=row_dict_list[0])

    def get_children(self, src_row):
        """
        Returns the immediate children of a row.
        :param src_row:
        :return:
        """
        src_row_table = src_row.table
        src_row_id = src_row.row_id
        table_parent_column = self.driver_wrapper.get_parent_column(src_row_table)
        return self.search(table=src_row_table, column=table_parent_column, search_term=src_row_id)

    def get_linear_row_list(self, start_row):
        """
        Takes a starting row. Iterates up the tree, making an index of rows as it goes.
        Starts from the highest entry, then proceeds down.
        .......... -> grandparent_series -> parent_series -> series

        :param start_row:
        :return tree_row_index:
        """
        row_dict_list = self.driver_wrapper.get_linear_row_list(start_row.row_dict)
        return [Row(row_dict=r, database=self) for r in row_dict_list]

    def get_all_tree_rows(self, start_row, back_iterate=True):
        """
        if back_iterate - start from a row - walk back up the tree to the root - then  walks back down the tree - adding
        every row it finds to the row set which it then returns.
        :param start_row:
        :param back_iterate:
        :return:
        """
        row_table = start_row.table
        row_parent_column = self.driver_wrapper.get_parent_column(row_table)
        row_id_column = self.driver_wrapper.get_id_column(row_table)
        if back_iterate:
            root_series = self.get_root_series(start_row)
        else:
            root_series = start_row

        row_pool = set()
        row_pool.add(root_series)
        found_series = set()

        while len(row_pool) != 0:

            current_series = row_pool.pop()
            current_id = current_series[row_id_column]

            # finds all the series which refer to the current_series in the series_parent column
            child_rows = self.search(table=row_table, column=row_parent_column, search_term=current_id)
            for row in child_rows:
                row_pool.add(row)

            found_series.add(current_series)

        return found_series

    def walk(self, start_row):
        """
        Walk the tree - yielding all the rows as you go.
        :param start_row:
        :return:
        """
        start_row_dict = start_row.row_dict
        for table_row_dict in self.driver_wrapper.walk(start_row_dict):
            yield Row(row_dict=table_row_dict, database=self)

    def search_tree(self, root_row, for_ids):
        """
        Search a tree looking for any of the ids in the for_ids object - if one is found which is in the object return
        True, else return False.
        e.g. used when trying to find out if a row is in the tree that's rooted at the root row - for example if you
        want to find out if a folder is inside another folder.
        :param root_row: The row to start the search with
        :param for_ids: Every id in the tree will be checked against this object.
        :return:
        """
        root_row_dict = root_row.row_dict
        target_table = root_row.table
        target_table_id_col = self.driver_wrapper.get_id_column(target_table)

        matched_ids = set()
        for child_row in self.driver_wrapper.walk(start_row=root_row_dict):
            if child_row[target_table_id_col] in for_ids:
                matched_ids.add(child_row[target_table_id_col])
        return matched_ids

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO WRITE TREE STRUCTURES START HERE

    # Todo: What happens when you try and nest rows from different tables
    # Todo: What happens when you try and nest a row inside itself? (should fail - might not)
    def nest_rows(self, parent_row, child_rows):
        """
        Takes a container row and a collection of target_rows. The target_rows are placed inside the container row.
        :param parent_row: A row in the form of a dict which will end up being the stem for all the rows in
        target_rows
        :param child_rows: Either one row, or an iterable of rows
        :return True/False: Checks against the database and makes sure that the change has been made (optional)
        """
        container_table = parent_row.table
        # Deals with the case of child_rows being a single row
        if isinstance(child_rows, Row):
            child_rows = [child_rows]

        # extract the id from the container_row - then set the parent category in all the target_rows to be that id
        container_row_id = parent_row.row_id
        target_rows_parent_column = self.driver_wrapper.get_parent_column(container_table)
        for row in child_rows:
            row[target_rows_parent_column] = container_row_id
            self.driver_wrapper.update_row(row.row_dict)

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - TREE STRUCTURES - DELETE

    def delete_tree(self, parent_row):
        """
        Removes the tree rooted at the parent_row entirely - all entries in the tree are removed.
        :param parent_row:
        :return:
        """
        # Due to the foreign key constraints removing the parent of a bunch of folders should also take out all children
        # of those folders. So deleting the root row should be enough to take out all the folders associated with it
        self.delete(parent_row)

    #
    # ----------------------------------------------------------------------------------------------------------------------
