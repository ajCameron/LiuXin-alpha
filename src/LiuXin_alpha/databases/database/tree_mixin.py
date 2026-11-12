"""
Wrap hierarchical database records as Rows and delegate parent/child mutations.

These helpers assume a compatible parent-column schema. Driver traversal rejects
cycles. Tree mutations use one macro transaction, including nested caller units.
"""

from __future__ import annotations

from LiuXin_alpha.databases.database._state import DatabaseMixinState

from collections.abc import Iterable, Iterator
from typing import Union

from LiuXin_alpha.databases.row import Row

from LiuXin_alpha.databases.api.row_api import RowAPI
from LiuXin_alpha.errors import InputIntegrityError


class DatabaseTreeMixin(DatabaseMixinState):
    """
    Provide parent chains, child searches and tree mutations for facade Rows.

    Example:
        For a Row in a parent-linked table, db.get_linear_row_list(row) retrieves its
        root-to-row chain.
    """

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO READ TREE STRUCTURES FROM TABLES START HERE

    def get_root_row(self, start_row: "RowAPI") -> "RowAPI":
        """
        Resolve a root through the legacy get_root_series implementation.

        Example:
            For a descendant Row, root = db.get_root_row(descendant) uses the same
            parent-chain path as the legacy series-named helper.


        :param start_row: Starting Row in a parent-linked table.
        :return: Root Row returned by get_root_series.
        """
        return self.get_root_series(start_row=start_row)

    # Todo: This method is terribly names - should be merged with the above and removed
    def get_root_series(self, start_row: "RowAPI") -> "RowAPI":
        """
        Wrap the first record of the wrapper root-to-row chain.

        Despite the name, the method works with any supported parent-linked table. It
        assumes the returned chain is nonempty.

        Example:
            For a parent-linked folder Row, db.get_root_series(folder) returns the first
            Row in the wrapper chain.


        :param start_row: Starting Row whose row_dict is passed to the wrapper.
        :return: Root Row bound to this facade.
        :raises IndexError: The wrapper returns an empty chain.
        """
        row_dict_list = self._database.driver_wrapper.get_linear_row_list(
            start_row._require_row_dict()
        )
        return Row(database=self._database, row_dict=row_dict_list[0])

    def get_children(self, src_row: "RowAPI") -> list["RowAPI"]:
        """
        Search the source table for rows whose parent ID equals the source ID.

        No recursive traversal or sibling sorting is performed here.

        Example:
            For a parent-linked Row, db.get_children(parent) retrieves its direct
            children.


        :param src_row: Parent Row providing its table and identity.
        :return: List of immediate child Rows in search order.
        """
        src_row_table = self._row_table(src_row)
        src_row_id = src_row.row_id
        table_parent_column = self._database.driver_wrapper.get_parent_column(
            src_row_table
        )
        if not table_parent_column:
            raise InputIntegrityError(
                "Tree operations require a physical parent column"
            )
        return self._database.search(
            table=src_row_table, column=table_parent_column, search_term=src_row_id
        )

    def get_linear_row_list(self, start_row: "RowAPI") -> list["RowAPI"]:
        """
        Wrap the wrapper parent chain in root-to-start order.

        Example:
            For root, child and grandchild in one chain,
            db.get_linear_row_list(grandchild) returns those three Rows in that order.


        :param start_row: Starting Row whose row_dict supplies table and identity.
        :return: List of Rows in wrapper chain order.
        """
        row_dict_list = self._database.driver_wrapper.get_linear_row_list(
            start_row._require_row_dict()
        )
        return [Row(row_dict=r, database=self._database) for r in row_dict_list]

    def get_all_tree_rows(
        self, start_row: "RowAPI", back_iterate: bool = True
    ) -> set["RowAPI"]:
        """
        Collect a root or subtree using the wrapper's cycle-detecting traversal.

        Both ancestor lookup and descendant traversal reject cycles with
        InputIntegrityError. Row hashing determines the returned set identity.

        Example:
            For an acyclic subtree, db.get_all_tree_rows(parent, back_iterate=False)
            collects parent and its descendants.


        :param start_row: Row selecting the tree or subtree.
        :param back_iterate: Resolve the root first when True; otherwise start at the
            supplied Row.
        :return: Set of visited Rows, including the chosen root.
        """
        row_table = self._row_table(start_row)
        if start_row.row_id is None:
            raise InputIntegrityError("Tree collection requires a persisted root")
        if not self._database.driver_wrapper.get_parent_column(row_table):
            raise InputIntegrityError(
                "Tree operations require a physical parent column"
            )
        root = self.get_root_series(start_row) if back_iterate else start_row
        return set(self.walk(root))

    def walk(self, start_row: "RowAPI") -> Iterator["RowAPI"]:
        """
        Lazily wrap records produced by the wrapper tree walk.

        Example:
            For an open db and supported tree Row, for row in db.walk(root): consumes
            the wrapper traversal without an eager facade list.


        :param start_row: Row whose row_dict starts backend traversal.
        :return: Generator of Rows in backend traversal order.
        """
        start_row_dict = start_row._require_row_dict()
        for table_row_dict in self._database.driver_wrapper.walk(start_row_dict):
            yield Row(row_dict=table_row_dict, database=self._database)

    def search_tree(self, root_row: "RowAPI", for_ids: Iterable[int]) -> set[int]:
        """Return requested IDs encountered in a complete tree traversal.

        Materialize the requested iterable once so lists, sets and one-shot
        generators have the same membership semantics. No ID coercion is applied.

        :param root_row: Root selecting the table and traversal start.
        :param for_ids: Iterable of wanted IDs; consumed once.
        :return: Set of matching IDs.
        """
        root_row_dict = root_row._require_row_dict()
        target_table = self._row_table(root_row)
        target_table_id_col = self._database.driver_wrapper.get_id_column(target_table)

        wanted_ids = set(for_ids)
        matched_ids = set()
        for child_row in self._database.driver_wrapper.walk(start_row=root_row_dict):
            if child_row[target_table_id_col] in wanted_ids:
                matched_ids.add(child_row[target_table_id_col])
        return matched_ids

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO WRITE TREE STRUCTURES START HERE

    def nest_rows(
        self, parent_row: "RowAPI", child_rows: Union["RowAPI", Iterable["RowAPI"]]
    ) -> None:
        """
        Atomically assign persisted children to a persisted parent in the same table.

        Reject missing rows and ancestor/self nesting before writing. Update only
        the parent column, preserving unrelated staged edits. Input Rows change
        after the write unit succeeds; reload them after a later caller rollback.

        Example:
            For compatible Rows in an acyclic tree, db.nest_rows(parent, [first,
            second]) assigns the parent ID to each child and writes it.


        :param parent_row: Parent Row supplying table, identity and parent-column
            naming.
        :param child_rows: Single RowAPI instance or iterable of child Rows.
        :return: None; input parent values change only after successful writes.
        """
        container_table = self._row_table(parent_row)
        children = [child_rows] if isinstance(child_rows, RowAPI) else list(child_rows)
        container_row_id = parent_row.row_id
        target_rows_parent_column = self._database.driver_wrapper.get_parent_column(
            container_table
        )
        if not target_rows_parent_column:
            raise InputIntegrityError(
                "Tree operations require a physical parent column"
            )
        if container_row_id is None:
            raise InputIntegrityError("Nesting requires a persisted parent")
        for row in children:
            if self._row_table(row) != container_table or row.row_id is None:
                raise InputIntegrityError(
                    "Children must be persisted in the parent's table"
                )

        macros = self._database.macros
        with self._write_transaction():
            parent = macros.get_row(container_table, container_row_id)
            if parent is None:
                raise InputIntegrityError("Nesting requires an existing parent")
            id_column = self._database.driver_wrapper.get_id_column(container_table)
            ancestors = {
                row[id_column]
                for row in self._database.driver_wrapper.get_linear_row_list(
                    dict(parent)
                )
            }
            for child in children:
                if child.row_id in ancestors:
                    raise InputIntegrityError("Nesting would create a tree cycle")
                if macros.get_row(container_table, child.row_id) is None:
                    raise InputIntegrityError("Nesting requires existing children")
            for child in children:
                macros.update_row(
                    container_table,
                    child.row_id,
                    {target_rows_parent_column: container_row_id},
                )
        for child in children:
            child[target_rows_parent_column] = container_row_id

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - TREE STRUCTURES - DELETE

    def delete_tree(self, parent_row: "RowAPI") -> None:
        """Delete a root and all of its descendants in one macro transaction.

        Read the persisted root, walk with cycle detection, then delete children
        before parents. This works with cascading and SET NULL parent foreign keys.
        Failures roll back every deletion; nested calls respect caller ownership.

        :param parent_row: Persisted root of the subtree, including a non-root branch.
        :return: None; a root already absent from storage is harmless.
        """
        table = self._row_table(parent_row)
        if parent_row.row_id is None:
            raise InputIntegrityError("Subtree deletion requires a persisted root")
        wrapper = self._database.driver_wrapper
        if not wrapper.get_parent_column(table):
            raise InputIntegrityError(
                "Tree operations require a physical parent column"
            )
        macros = self._database.macros
        with self._write_transaction():
            root = macros.get_row(table, parent_row.row_id)
            if root is None:
                return
            id_column = wrapper.get_id_column(table)
            # The wrapper detects cycles and always visits parents before children.
            descendants = list(wrapper.walk(dict(root)))
            for row in reversed(descendants):
                macros.delete_row(table, row[id_column])

    #
    # ----------------------------------------------------------------------------------------------------------------------
