"""
Declare DatabaseTreeMixinAPI operations for database facade implementations.

Abstract bodies perform no backend work. Concrete behavior and its limitations are
described for callers without changing that implementation.
"""

from __future__ import annotations

import abc
from typing import Iterator, Iterable, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import RowAPI


class DatabaseTreeMixinAPI(abc.ABC):
    """
    Specify parent chains, traversal and hierarchical mutations.

    Implement every abstract member before instantiating this interface. Backend
    resource and transaction policies remain the concrete implementation responsibility.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DatabaseTreeMixinAPI)
        True
    """


    # Todo: Replace this with "get_root_row"


    # Todo: The parent_row should be the root_row

    # ---------------------------------------------------------------------------------------------
    # Tree helpers (hierarchies expressed via intralinks)
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_root_row(self, start_row: "RowAPI") -> "RowAPI":
        """
        Resolve a root through the legacy get_root_series implementation.

        Abstract hook; subclasses supply the operation. Concrete Database behavior:
        Resolve a root through the legacy get_root_series implementation.

        Example:
            For a descendant Row, root = db.get_root_row(descendant) uses the same
            parent-chain path as the legacy series-named helper.


        :param start_row: Starting Row in a parent-linked table.
        :return: Root Row returned by get_root_series.
        """

    @abc.abstractmethod
    def get_root_series(self, start_row: "RowAPI") -> "RowAPI":
        """
        Wrap the first record of the wrapper root-to-row chain.

        Abstract hook; subclasses supply the operation. Concrete Database behavior:
        Despite the name, the method works with any supported parent-linked table. It
        assumes the returned chain is nonempty.

        Example:
            For a parent-linked folder Row, db.get_root_series(folder) returns the first
            Row in the wrapper chain.


        :param start_row: Starting Row whose row_dict is passed to the wrapper.
        :return: Root Row bound to this facade.
        :raises IndexError: The wrapper returns an empty chain.
        """

    @abc.abstractmethod
    def get_children(self, src_row: "RowAPI") -> list["RowAPI"]:
        """
        Search the source table for rows whose parent ID equals the source ID.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: No
        recursive traversal or sibling sorting is performed here.

        Example:
            For a parent-linked Row, db.get_children(parent) retrieves its direct
            children.


        :param src_row: Parent Row providing its table and identity.
        :return: List of immediate child Rows in search order.
        """

    @abc.abstractmethod
    def get_linear_row_list(self, start_row: "RowAPI") -> list["RowAPI"]:
        """
        Wrap the wrapper parent chain in root-to-start order.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Wrap
        the wrapper parent chain in root-to-start order.

        Example:
            For root, child and grandchild in one chain,
            db.get_linear_row_list(grandchild) returns those three Rows in that order.


        :param start_row: Starting Row whose row_dict supplies table and identity.
        :return: List of Rows in wrapper chain order.
        """

    @abc.abstractmethod
    def get_all_tree_rows(self, start_row: "RowAPI", back_iterate: bool = True) -> set["RowAPI"]:
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

    @abc.abstractmethod
    def walk(self, start_row: "RowAPI") -> Iterator["RowAPI"]:
        """
        Lazily wrap records produced by the wrapper tree walk.

        Abstract hook; subclasses supply the operation. Concrete Database behavior:
        Lazily wrap records produced by the wrapper tree walk.

        Example:
            For an open db and supported tree Row, for row in db.walk(root): consumes
            the wrapper traversal without an eager facade list.


        :param start_row: Row whose row_dict starts backend traversal.
        :return: Generator of Rows in backend traversal order.
        """

    @abc.abstractmethod
    def search_tree(self, root_row: "RowAPI", for_ids: Iterable[int]) -> set[int]:
        """Return requested IDs encountered in a complete tree traversal.

        Materialize the requested iterable once so lists, sets and one-shot
        generators have the same membership semantics. No ID coercion is applied.

        :param root_row: Root selecting the table and traversal start.
        :param for_ids: Iterable of wanted IDs; consumed once.
        :return: Set of matching IDs.
        """

    @abc.abstractmethod
    def nest_rows(self, parent_row: "RowAPI", child_rows: Union["RowAPI", Iterable["RowAPI"]]) -> None:
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

    @abc.abstractmethod
    def delete_tree(self, parent_row: "RowAPI") -> None:
        """Delete a root and all of its descendants in one macro transaction.

        Read the persisted root, walk with cycle detection, then delete children
        before parents. This works with cascading and SET NULL parent foreign keys.
        Failures roll back every deletion; nested calls respect caller ownership.

        :param parent_row: Persisted root of the subtree, including a non-root branch.
        :return: None; a root already absent from storage is harmless.
        """
