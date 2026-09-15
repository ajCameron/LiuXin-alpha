
"""
Wrap hierarchical database records as Rows and delegate parent/child mutations.

These helpers assume a compatible parent-column schema. Driver traversal determines path/walk behavior, while some facade loops do not detect cycles. Deletion relies on backend foreign-key actions rather than recursive facade traversal.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.databases.row import Row

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.driver_wrapper_api.driver_wrapper_api import DatabaseDriverWrapperAPI


class DatabaseTreeMixin:
    """
    Provide parent chains, child searches and tree mutations for facade Rows.

    Example:
        For a Row in a parent-linked table, db.get_linear_row_list(row) retrieves its root-to-row chain.
    """


    driver_wrapper: "DatabaseDriverWrapperAPI"

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO READ TREE STRUCTURES FROM TABLES START HERE

    def get_root_row(self, start_row):
        """
        Resolve a root through the legacy get_root_series implementation.

        Example:
            For a descendant Row, root = db.get_root_row(descendant) uses the same parent-chain path as the legacy series-named helper.


        :param start_row: Starting Row in a parent-linked table.
        :return: Root Row returned by get_root_series.
        """
        return self.get_root_series(start_row=start_row)

    # Todo: This method is terribly names - should be merged with the above and removed
    def get_root_series(self, start_row):
        """
        Wrap the first record of the wrapper root-to-row chain.

        Despite the name, the method works with any supported parent-linked table. It assumes the returned chain is nonempty.

        Example:
            For a parent-linked folder Row, db.get_root_series(folder) returns the first Row in the wrapper chain.


        :param start_row: Starting Row whose row_dict is passed to the wrapper.
        :return: Root Row bound to this facade.
        :raises IndexError: The wrapper returns an empty chain.
        """
        row_dict_list = self.driver_wrapper.get_linear_row_list(start_row.row_dict)
        return Row(database=self, row_dict=row_dict_list[0])

    def get_children(self, src_row):
        """
        Search the source table for rows whose parent ID equals the source ID.

        No recursive traversal or sibling sorting is performed here.

        Example:
            For a parent-linked Row, db.get_children(parent) retrieves its direct children.


        :param src_row: Parent Row providing its table and identity.
        :return: List of immediate child Rows in search order.
        """
        src_row_table = src_row.table
        src_row_id = src_row.row_id
        table_parent_column = self.driver_wrapper.get_parent_column(src_row_table)
        return self.search(table=src_row_table, column=table_parent_column, search_term=src_row_id)

    def get_linear_row_list(self, start_row):
        """
        Wrap the wrapper parent chain in root-to-start order.

        Example:
            For root, child and grandchild in one chain, db.get_linear_row_list(grandchild) returns those three Rows in that order.


        :param start_row: Starting Row whose row_dict supplies table and identity.
        :return: List of Rows in wrapper chain order.
        """
        row_dict_list = self.driver_wrapper.get_linear_row_list(start_row.row_dict)
        return [Row(row_dict=r, database=self) for r in row_dict_list]

    def get_all_tree_rows(self, start_row, back_iterate=True):
        """
        Collect a root or subtree and its descendants using an unordered work set.

        Each popped Row searches its immediate children. Previously visited Rows are not excluded from the work set; cycles can cause nontermination. Requires a finite acyclic parent structure, and Row hashing determines set identity.

        Example:
            For an acyclic subtree, db.get_all_tree_rows(parent, back_iterate=False) collects parent and its descendants.


        :param start_row: Row selecting the tree or subtree.
        :param back_iterate: Resolve the root first when True; otherwise start at the supplied Row.
        :return: Set of visited Rows, including the chosen root.
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
        Lazily wrap records produced by the wrapper tree walk.

        Example:
            For an open db and supported tree Row, for row in db.walk(root): consumes the wrapper traversal without an eager facade list.


        :param start_row: Row whose row_dict starts backend traversal.
        :return: Generator of Rows in backend traversal order.
        """
        start_row_dict = start_row.row_dict
        for table_row_dict in self.driver_wrapper.walk(start_row_dict):
            yield Row(row_dict=table_row_dict, database=self)

    def search_tree(self, root_row, for_ids):
        """
        Collect requested IDs encountered in the wrapper traversal.

        The whole traversal is consumed rather than stopping at the first match. No ID coercion is applied.

        Example:
            For a tree rooted at root, db.search_tree(root, {wanted_id}) returns either an empty set or a set containing that ID.


        :param root_row: Root Row selecting table and traversal start.
        :param for_ids: Container used for membership tests against visited IDs.
        :return: Set of matching IDs, not a boolean.
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
        Set child parent-column values and update each child record through the wrapper.

        The facade does not validate equal tables, prevent cycles or verify the final structure. A failed update can leave an input Row changed and earlier writes applied.

        Example:
            For compatible Rows in an acyclic tree, db.nest_rows(parent, [first, second]) assigns the parent ID to each child and writes it.


        :param parent_row: Parent Row supplying table, identity and parent-column naming.
        :param child_rows: Single concrete Row or iterable of child Rows.
        :return: None; input Rows are mutated before their individual database updates.
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
        Delete only the supplied root Row through the normal facade delete path.

        Descendant deletion depends entirely on configured database cascade rules; the facade does not walk or independently delete child Rows.

        Example:
            For a schema with cascading parent foreign keys, db.delete_tree(root) relies on those rules to remove descendants after deleting root.


        :param parent_row: Root Row to delete.
        :return: None.
        """
        # Due to the foreign key constraints removing the parent of a bunch of folders should also take out all children
        # of those folders. So deleting the root row should be enough to take out all the folders associated with it
        self.delete(parent_row)

    #
    # ----------------------------------------------------------------------------------------------------------------------
