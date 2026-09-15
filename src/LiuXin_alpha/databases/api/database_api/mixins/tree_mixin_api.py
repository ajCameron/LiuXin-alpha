"""
Declare DatabaseTreeMixinAPI operations for database facade implementations.

Repeated declarations are retained; the later definition supplies the runtime member. Abstract bodies perform no backend work. Concrete behavior and its limitations are described for callers without changing that implementation.
"""

from __future__ import annotations

import abc
from typing import Iterator, Iterable, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import RowAPI


class DatabaseTreeMixinAPI(abc.ABC):
    """
    Specify parent chains, traversal and hierarchical mutations.

    Implement every abstract member before instantiating this interface. Backend resource and transaction policies remain the concrete implementation responsibility.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DatabaseTreeMixinAPI)
        True
    """

    @abc.abstractmethod
    def get_root_row(self, start_row: "RowAPI") -> "RowAPI":
        """
        Resolve a root through the legacy get_root_series implementation.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Resolve a root through the legacy get_root_series implementation.

        Example:
            For a descendant Row, root = db.get_root_row(descendant) uses the same parent-chain path as the legacy series-named helper.


        :param start_row: Starting Row in a parent-linked table.
        :return: Root Row returned by get_root_series.
        """

    # Todo: Replace this with "get_root_row"
    @abc.abstractmethod
    def get_root_series(self, start_row: "RowAPI") -> "RowAPI":
        """
        Wrap the first record of the wrapper root-to-row chain.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Despite the name, the method works with any supported parent-linked table. It assumes the returned chain is nonempty.

        Example:
            For a parent-linked folder Row, db.get_root_series(folder) returns the first Row in the wrapper chain.


        :param start_row: Starting Row whose row_dict is passed to the wrapper.
        :return: Root Row bound to this facade.
        :raises IndexError: The wrapper returns an empty chain.
        """

    @abc.abstractmethod
    def get_children(self, src_row: "RowAPI") -> list["RowAPI"]:
        """
        Search the source table for rows whose parent ID equals the source ID.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: No recursive traversal or sibling sorting is performed here.

        Example:
            For a parent-linked Row, db.get_children(parent) retrieves its direct children.


        :param src_row: Parent Row providing its table and identity.
        :return: List of immediate child Rows in search order.
        """

    @abc.abstractmethod
    def get_linear_row_list(self, start_row: "RowAPI") -> list["RowAPI"]:
        """
        Wrap the wrapper parent chain in root-to-start order.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Wrap the wrapper parent chain in root-to-start order.

        Example:
            For root, child and grandchild in one chain, db.get_linear_row_list(grandchild) returns those three Rows in that order.


        :param start_row: Starting Row whose row_dict supplies table and identity.
        :return: List of Rows in wrapper chain order.
        """

    @abc.abstractmethod
    def get_all_tree_rows(self, start_row: "RowAPI", back_iterate: bool = True) -> set["RowAPI"]:
        """
        Collect a root or subtree and its descendants using an unordered work set.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Each popped Row searches its immediate children. Previously visited Rows are not excluded from the work set; cycles can cause nontermination. Requires a finite acyclic parent structure, and Row hashing determines set identity.

        Example:
            For an acyclic subtree, db.get_all_tree_rows(parent, back_iterate=False) collects parent and its descendants.


        :param start_row: Row selecting the tree or subtree.
        :param back_iterate: Resolve the root first when True; otherwise start at the supplied Row.
        :return: Set of visited Rows, including the chosen root.
        """

    @abc.abstractmethod
    def walk(self, start_row: "RowAPI") -> Iterator["RowAPI"]:
        """
        Lazily wrap records produced by the wrapper tree walk.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Lazily wrap records produced by the wrapper tree walk.

        Example:
            For an open db and supported tree Row, for row in db.walk(root): consumes the wrapper traversal without an eager facade list.


        :param start_row: Row whose row_dict starts backend traversal.
        :return: Generator of Rows in backend traversal order.
        """

    @abc.abstractmethod
    def search_tree(self, root_row: "RowAPI", for_ids: Iterable[int]) -> set[int]:
        """
        Collect requested IDs encountered in the wrapper traversal.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The whole traversal is consumed rather than stopping at the first match. No ID coercion is applied.

        Example:
            For a tree rooted at root, db.search_tree(root, {wanted_id}) returns either an empty set or a set containing that ID.


        :param root_row: Root Row selecting table and traversal start.
        :param for_ids: Container used for membership tests against visited IDs.
        :return: Set of matching IDs, not a boolean.
        """

    @abc.abstractmethod
    def nest_rows(self, parent_row: "RowAPI", child_rows: Union["RowAPI", Iterable["RowAPI"]]) -> None:
        """
        Set child parent-column values and update each child record through the wrapper.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The facade does not validate equal tables, prevent cycles or verify the final structure. A failed update can leave an input Row changed and earlier writes applied.

        Example:
            For compatible Rows in an acyclic tree, db.nest_rows(parent, [first, second]) assigns the parent ID to each child and writes it.


        :param parent_row: Parent Row supplying table, identity and parent-column naming.
        :param child_rows: Single concrete Row or iterable of child Rows.
        :return: None; input Rows are mutated before their individual database updates.
        """

    # Todo: The parent_row should be the root_row
    @abc.abstractmethod
    def delete_tree(self, parent_row: "RowAPI") -> None:
        """
        Delete only the supplied root Row through the normal facade delete path.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Descendant deletion depends entirely on configured database cascade rules; the facade does not walk or independently delete child Rows.

        Example:
            For a schema with cascading parent foreign keys, db.delete_tree(root) relies on those rules to remove descendants after deleting root.


        :param parent_row: Root Row to delete.
        :return: None.
        """

    # ---------------------------------------------------------------------------------------------
    # Tree helpers (hierarchies expressed via intralinks)
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_root_row(self, start_row: "RowAPI") -> "RowAPI":
        """
        Resolve a root through the legacy get_root_series implementation.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Resolve a root through the legacy get_root_series implementation.

        Example:
            For a descendant Row, root = db.get_root_row(descendant) uses the same parent-chain path as the legacy series-named helper.


        :param start_row: Starting Row in a parent-linked table.
        :return: Root Row returned by get_root_series.
        """

    @abc.abstractmethod
    def get_root_series(self, start_row: "RowAPI") -> "RowAPI":
        """
        Wrap the first record of the wrapper root-to-row chain.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Despite the name, the method works with any supported parent-linked table. It assumes the returned chain is nonempty.

        Example:
            For a parent-linked folder Row, db.get_root_series(folder) returns the first Row in the wrapper chain.


        :param start_row: Starting Row whose row_dict is passed to the wrapper.
        :return: Root Row bound to this facade.
        :raises IndexError: The wrapper returns an empty chain.
        """

    @abc.abstractmethod
    def get_children(self, src_row: "RowAPI") -> list["RowAPI"]:
        """
        Search the source table for rows whose parent ID equals the source ID.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: No recursive traversal or sibling sorting is performed here.

        Example:
            For a parent-linked Row, db.get_children(parent) retrieves its direct children.


        :param src_row: Parent Row providing its table and identity.
        :return: List of immediate child Rows in search order.
        """

    @abc.abstractmethod
    def get_linear_row_list(self, start_row: "RowAPI") -> list["RowAPI"]:
        """
        Wrap the wrapper parent chain in root-to-start order.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Wrap the wrapper parent chain in root-to-start order.

        Example:
            For root, child and grandchild in one chain, db.get_linear_row_list(grandchild) returns those three Rows in that order.


        :param start_row: Starting Row whose row_dict supplies table and identity.
        :return: List of Rows in wrapper chain order.
        """

    @abc.abstractmethod
    def get_all_tree_rows(self, start_row: "RowAPI", back_iterate: bool = True) -> set["RowAPI"]:
        """
        Collect a root or subtree and its descendants using an unordered work set.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Each popped Row searches its immediate children. Previously visited Rows are not excluded from the work set; cycles can cause nontermination. Requires a finite acyclic parent structure, and Row hashing determines set identity.

        Example:
            For an acyclic subtree, db.get_all_tree_rows(parent, back_iterate=False) collects parent and its descendants.


        :param start_row: Row selecting the tree or subtree.
        :param back_iterate: Resolve the root first when True; otherwise start at the supplied Row.
        :return: Set of visited Rows, including the chosen root.
        """

    @abc.abstractmethod
    def walk(self, start_row: "RowAPI") -> Iterator["RowAPI"]:
        """
        Lazily wrap records produced by the wrapper tree walk.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Lazily wrap records produced by the wrapper tree walk.

        Example:
            For an open db and supported tree Row, for row in db.walk(root): consumes the wrapper traversal without an eager facade list.


        :param start_row: Row whose row_dict starts backend traversal.
        :return: Generator of Rows in backend traversal order.
        """

    @abc.abstractmethod
    def search_tree(self, root_row: "RowAPI", for_ids: Iterable[int]) -> set[int]:
        """
        Collect requested IDs encountered in the wrapper traversal.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The whole traversal is consumed rather than stopping at the first match. No ID coercion is applied.

        Example:
            For a tree rooted at root, db.search_tree(root, {wanted_id}) returns either an empty set or a set containing that ID.


        :param root_row: Root Row selecting table and traversal start.
        :param for_ids: Container used for membership tests against visited IDs.
        :return: Set of matching IDs, not a boolean.
        """

    @abc.abstractmethod
    def nest_rows(self, parent_row: "RowAPI", child_rows: Union["RowAPI", Iterable["RowAPI"]]) -> None:
        """
        Set child parent-column values and update each child record through the wrapper.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The facade does not validate equal tables, prevent cycles or verify the final structure. A failed update can leave an input Row changed and earlier writes applied.

        Example:
            For compatible Rows in an acyclic tree, db.nest_rows(parent, [first, second]) assigns the parent ID to each child and writes it.


        :param parent_row: Parent Row supplying table, identity and parent-column naming.
        :param child_rows: Single concrete Row or iterable of child Rows.
        :return: None; input Rows are mutated before their individual database updates.
        """

    @abc.abstractmethod
    def delete_tree(self, parent_row: "RowAPI") -> None:
        """
        Delete only the supplied root Row through the normal facade delete path.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Descendant deletion depends entirely on configured database cascade rules; the facade does not walk or independently delete child Rows.

        Example:
            For a schema with cascading parent foreign keys, db.delete_tree(root) relies on those rules to remove descendants after deleting root.


        :param parent_row: Root Row to delete.
        :return: None.
        """
