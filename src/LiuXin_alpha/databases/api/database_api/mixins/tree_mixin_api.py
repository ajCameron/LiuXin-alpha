"""Database facade contracts for hierarchical catalogue relationships."""

from __future__ import annotations

import abc
from typing import Iterator, Iterable, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import RowAPI


class DatabaseTreeMixinAPI(abc.ABC):
    """
    Typed API for ``DatabaseTreeMixin``.

    Methods for dealing with trees in the database.
    """

    @abc.abstractmethod
    def get_root_row(self, start_row: "RowAPI") -> "RowAPI":
        """
        Get the root row of a tree the start_row is in.

        :param start_row:
        :return:
        """

    # Todo: Replace this with "get_root_row"
    @abc.abstractmethod
    def get_root_series(self, start_row: "RowAPI") -> "RowAPI":
        """
        Get the root series of the series tree we're in.

        :param start_row:
        :return:
        """

    @abc.abstractmethod
    def get_children(self, src_row: "RowAPI") -> list["RowAPI"]:
        """
        Get all the child rows of the tree we're in.

        :param src_row:
        :return:
        """

    @abc.abstractmethod
    def get_linear_row_list(self, start_row: "RowAPI") -> list["RowAPI"]:
        """
        Get all the rows in the tree we're in as a list.

        :param start_row:
        :return:
        """

    @abc.abstractmethod
    def get_all_tree_rows(self, start_row: "RowAPI", back_iterate: bool = True) -> set["RowAPI"]:
        """
        Get all the rows in the tree the start_row is in - from the start row down.

        :param start_row:
        :param back_iterate:
        :return:
        """

    @abc.abstractmethod
    def walk(self, start_row: "RowAPI") -> Iterator["RowAPI"]:
        """
        Walk a tree from the start_row down.

        :param start_row:
        :return:
        """

    @abc.abstractmethod
    def search_tree(self, root_row: "RowAPI", for_ids: Iterable[int]) -> set[int]:
        """
        Search a tree rooted in the root for any instances of the given ids.

        :param root_row:
        :param for_ids:
        :return:
        """

    @abc.abstractmethod
    def nest_rows(self, parent_row: "RowAPI", child_rows: Union["RowAPI", Iterable["RowAPI"]]) -> None:
        """
        Place the child rows under the parent row.

        :param parent_row:
        :param child_rows:
        :return:
        """

    # Todo: The parent_row should be the root_row
    @abc.abstractmethod
    def delete_tree(self, parent_row: "RowAPI") -> None:
        """
        Delete an entire tree

        :param parent_row:
        :return:
        """

    # ---------------------------------------------------------------------------------------------
    # Tree helpers (hierarchies expressed via intralinks)
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_root_row(self, start_row: "RowAPI") -> "RowAPI":
        """Return the root row for a tree anchored at start_row."""

    @abc.abstractmethod
    def get_root_series(self, start_row: "RowAPI") -> "RowAPI":
        """Return the lineage from start_row up to the root (inclusive)."""

    @abc.abstractmethod
    def get_children(self, src_row: "RowAPI") -> list["RowAPI"]:
        """Return immediate children of src_row in the tree."""

    @abc.abstractmethod
    def get_linear_row_list(self, start_row: "RowAPI") -> list["RowAPI"]:
        """Return a linearized list of tree rows starting from start_row."""

    @abc.abstractmethod
    def get_all_tree_rows(self, start_row: "RowAPI", back_iterate: bool = True) -> set["RowAPI"]:
        """Return all rows in the tree under start_row (optionally iterating 'backwards')."""

    @abc.abstractmethod
    def walk(self, start_row: "RowAPI") -> Iterator["RowAPI"]:
        """Yield rows in the tree in a driver-defined walk order."""

    @abc.abstractmethod
    def search_tree(self, root_row: "RowAPI", for_ids: Iterable[int]) -> set[int]:
        """Search a tree for particular row ids."""

    @abc.abstractmethod
    def nest_rows(self, parent_row: "RowAPI", child_rows: Union["RowAPI", Iterable["RowAPI"]]) -> None:
        """Nest child rows under parent_row in the tree."""

    @abc.abstractmethod
    def delete_tree(self, parent_row: "RowAPI") -> None:
        """Delete parent_row and all its descendants from the tree."""
