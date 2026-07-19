

"""
API for the bits of the driver connected with deleting entries to the database.
"""

import abc

from typing import Iterable, Any


class DriverDeleteMixinAPI(abc.ABC):
    """
    Mixin methods to add to the database.
    """

    @abc.abstractmethod
    def direct_clear_table(self, target_table: str) -> bool:
        """
        Clear the specified table entirely.

        :param target_table:
        :return:
        """

    @abc.abstractmethod
    def direct_delete(
            self,
            target_table: str,
            column: str,
            value: Any,
            many: bool = False) -> bool:
        """
        Delete one or more entries from the specified table matching the given value.

        :param target_table:
        :param column:
        :param value:
        :param many:
        :return:
        """


    # Todo: Switch over to returning the affected ids?
    @abc.abstractmethod
    def direct_delete_many(
            self,
            target_table: str,
            column: str,
            values: Iterable[Any]) -> None:
        """
        Direct delete every instance of many entries from the table.

        :param target_table:
        :param column:
        :param values:
        :return:
        """

    @abc.abstractmethod
    def direct_delete_many_by_ids(self, target_table: str, row_ids: Iterable[int]) -> bool:
        """
        Directly delete many entries from the target table by their ids.

        :param target_table:
        :param row_ids:
        :return:
        """

    @abc.abstractmethod
    def direct_delete_row_by_id(self, target_table: str, row_id: int) -> bool:
        """
        Delete a single row by its id - if it exists.

        :param target_table:
        :param row_id:
        :return:
        """
