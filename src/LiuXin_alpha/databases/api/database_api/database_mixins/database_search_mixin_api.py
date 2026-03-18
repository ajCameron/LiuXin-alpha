from __future__ import annotations

import abc
from typing import Any, Optional, Union, Iterator, TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import RowAPI


class DatabaseSearchMixinAPI(abc.ABC):
    """
    Typed API for ``DatabaseSearchMixin``.

    API for preforming searches on the database.
    """

    @abc.abstractmethod
    def search(self, table: str, column: str, search_term: Any) -> list["RowAPI"]:
        """
        Search in a single column in a single table.

        :param table:
        :param column:
        :param search_term:
        :return:
        """

    @abc.abstractmethod
    def multi_column_search(self, search_index: Any, iterator_return: bool = False) -> Any:
        """
        Search in multiple columns in a single table.

        :param search_index:
        :param iterator_return:
        :return:
        """

    @abc.abstractmethod
    def get_unique(self, target_column: str) -> Any:
        """
        Return all the unique values for the given column.

        :param target_column:
        :return:
        """

    @abc.abstractmethod
    def get_values_set(self, target_column: str, iterator_return: bool = False) -> Any:
        """
        Return a set of values for the given column.

        :param target_column:
        :param iterator_return:
        :return:
        """

    @abc.abstractmethod
    def get_row_from_id(self, table: str, row_id: int) -> Optional["RowAPI"]:
        """
        Get a row from the given table by id.

        :param table:
        :param row_id:
        :return:
        """

    @abc.abstractmethod
    def get_random_row(self, table: str) -> "RowAPI":
        """
        Get a random row off the database.

        :param table:
        :return:
        """

    # Todo: Split this down into iterator and list

    @abc.abstractmethod
    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = True,
        sort_column: Optional[str] = None,
        reverse: bool = False,
    ) -> Union[list["RowAPI"], Iterator["RowAPI"]]:
        """
        Get all rows from the database.

        :param table:
        :param iterator_return:
        :param sort_column:
        :param reverse:
        :return:
        """

    # Todo: Add chunk size
    @abc.abstractmethod
    def chunk_iterator(self, column: str, target_table: Optional[str] = None) -> Iterator[list["RowAPI"]]:
        """
        Iterate over all rows in the database in chunks.

        :param column:
        :param target_table:
        :return:
        """


    # ---------------------------------------------------------------------------------------------
    # Search / retrieval
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def search(self, table: str, column: str, search_term: Any) -> list["RowAPI"]:
        """Search a table for rows matching the given column == search_term (driver-specific matching)."""

    @abc.abstractmethod
    def multi_column_search(self, search_index: Any, iterator_return: bool = False) -> Any:
        """Multi-column search helper (driver-dependent / may be incomplete)."""

    @abc.abstractmethod
    def get_unique(self, target_column: str) -> Any:
        """Convenience wrapper for get_values_set()."""

    @abc.abstractmethod
    def get_values_set(self, target_column: str, iterator_return: bool = False) -> Any:
        """Return the unique values for a column (as a set or iterator)."""

    @abc.abstractmethod
    def get_row_from_id(self, table: str, row_id: int) -> Optional["RowAPI"]:
        """Return the row with the given id from table, or None if not found."""

    @abc.abstractmethod
    def get_random_row(self, table: str) -> "RowAPI":
        """Return a randomly chosen row from a table."""

    @abc.abstractmethod
    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = True,
        sort_column: Optional[str] = None,
        reverse: bool = False,
    ) -> Union[list["RowAPI"], Iterator["RowAPI"]]:
        """Return all rows from a table as list or iterator."""

    @abc.abstractmethod
    def chunk_iterator(self, column: str, target_table: Optional[str] = None) -> Iterator[list["RowAPI"]]:
        """Iterate over grouped chunks of rows based on unique values in a column."""
