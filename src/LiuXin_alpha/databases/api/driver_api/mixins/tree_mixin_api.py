"""Driver-level contracts for direct hierarchical data operations."""

from __future__ import annotations

import abc
from typing import Any


class DriverTreeMixinAPI(abc.ABC):
    """
    Mixin containing API for the tree specific driver methods.
    """

    @abc.abstractmethod
    def direct_get_linear_index_of_columns(self, start_row: dict[str, Any], display_column: str) -> list[str]:
        """
        Return a liner index of the display column entries for every row in a tree.

        E.g. This will give you a series
        :param start_row:
        :param display_column:
        :return:
        """

    # Todo: should just be the above
    @abc.abstractmethod
    def direct_get_root_series(self, start_row: dict[str, Any]) -> dict[str, Any] :
        """
        Return the root row for a series.

        :param start_row:
        :return:
        """

    @abc.abstractmethod
    def direct_set_full_column(self, target_table):
        ...

    # Todo: This also should - probably - be a macros
    @abc.abstractmethod
    def direct_set_tree_ids(self, table: str) -> bool:
        """
        Swt the trees ids structure for the entire tree.

        :param table:
        :return:
        """

    # Todo: Make sure all methods are exposed via the driver wrapper.
    @abc.abstractmethod
    def direct_get_all_tree_rows(self, start_row: dict[str, Any]) -> dict[str, Any] :
        """
        Get all rows of a tree.

        :param start_row:
        :return:
        """

    @abc.abstractmethod
    def get_linear_row_index(self, start_row: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Get a linear index of rows in the given tree - from the start row going down.

        :param start_row:
        :return:
        """

    @abc.abstractmethod
    def direct_get_tree_aggregation_str(self, table: str, table_display_column: str, table_row_id: int) -> str:
        """
        Construct a string representing the entry's position in the tree.

        :param table:
        :param table_display_column:
        :param table_row_id:
        :return:
        """
