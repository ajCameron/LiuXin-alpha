"""Driver-level contracts for direct structured search operations."""

from __future__ import annotations

import abc
from typing import Iterable, Iterator, Optional, Union, Any


class DriverSearchMixinAPI(abc.ABC):
    """
    API for driver level search methods.
    """

    @staticmethod
    @abc.abstractmethod
    def can_index_be_transformed(target_index) -> bool:
        """
        Tests to see if an index can be transformed into pure string form.

        :param target_index:
        :return:
        """

    # Todo: Check actually returning an iterable
    @abc.abstractmethod
    def direct_get_all_hashes(self) -> set[str]:
        """
        Retrieve all the hashes from the database.

        :return:
        """

    @abc.abstractmethod
    def direct_get_all_rows(
            self,
            table: str,
            sort_column: Optional[str] = None,
            reverse: bool = False) -> list[dict[str, Any]]:
        """
        Method to get all the rows from a table as row dicts.

        :param table:
        :param sort_column:
        :param reverse:
        :return:
        """

    @abc.abstractmethod
    def direct_get_all_values(self, table: str, column: str) -> set[Any]:
        """
        Yield all the values present for a column in a table.

        :param table:
        :param column:
        :return:
        """

    @abc.abstractmethod
    def direct_get_highest_id(self, target_table: str) -> Optional[dict[str, Any]]:
        """
        Direct get the highest id from the database.

        :param target_table:
        :return:
        """

    @abc.abstractmethod
    def direct_get_max(self, column: str) -> Optional[int]:
        """
        Direct get the max value from the database.

        We don't know what this value is going to be, because we don't know what the column holds.
        :param column:
        :return:
        """

    @abc.abstractmethod
    def direct_get_min(self, column: str) -> Optional[int]:
        """
        Direct get the min value from the database.

        :param column:
        :return:
        """

    @abc.abstractmethod
    def direct_get_random_row_dict(self, target_table: str, direct: bool = False) -> Optional[dict[str, Any]]:
        """
        Pull a random row off the database in the form of a dict.

        :param target_table:
        :param direct:
        :return:
        """

    @abc.abstractmethod
    def direct_get_row_dict_from_id(self, table: str, row_id: int) -> Optional[dict[str, Any]] | bool:
        """
        Retrieve a row dict from a table.

        :param table:
        :param row_id:
        :return:
        """

    @abc.abstractmethod
    def direct_get_row_dict_iterator(
            self,
            table: str,
            sort_column: Optional[str] = None,
            reverse: bool = False) -> Iterator[dict[str, Any]]:
        """
        Yield row dicts from the table.

        :param table:
        :param sort_column:
        :param reverse:
        :return:
        """

    @abc.abstractmethod
    def direct_get_unique_values_iterator(self, target_column: str) -> Iterator[str]:
        """
        Yield a values iterator for the given target column.

        :param target_column:
        :return:
        """

    @abc.abstractmethod
    def direct_get_unique_values_set(self, target_column: str) -> set[str]:
        """
        Get the unique values set for a given target column from the database.

        :param target_column:
        :return:
        """

    # Todo: We need to pass the search in as values - this is not secure.
    @abc.abstractmethod
    def direct_multi_column_search(
            self,
            search_index: str,
            iterator_return: bool = False) -> Optional[Union[Iterator[dict[str, Any]], list[dict[str, Any]]]]:
        """
        Preform a mutli column search on the database.

        :param search_index:
        :param iterator_return:
        :return:
        """


    @abc.abstractmethod
    def direct_search_table(
            self,
            table: Optional[str] = None,
            column: Optional[str] = None,
            search_term: Optional[Any] = None) -> list[dict[str, Any]]:
        """
        Preform a search on a single table in the database.

        :param table:
        :param column:
        :param search_term:
        :return:
        """

    # Todo: Kill this along with locational_search
    @staticmethod
    @abc.abstractmethod
    def transform_index(target_index):
        """
        Part of building out a full locational search string.

        :param target_index:
        :return:
        """

    # Todo: Just remove this entirely
    @abc.abstractmethod
    def direct_locational_search(self, parsed_query):
        ...
