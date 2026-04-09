
"""
API for the bits of the driver connected with CRUD custom columns.
"""

import abc

from typing import Iterable, Any


class DriverCustomColumnsMixinAPI(abc.ABC):
    """
    Mixin methods to add to the database.
    """

    @abc.abstractmethod
    def direct_get_custom_column_table_name(self, table: str, column_name: str) -> str:
        """
        Get the table name for a custom column to add the

        :param table:
        :param column_name:
        :return:
        """

    @abc.abstractmethod
    def direct_create_custom_column(
            self,
            in_table: str,
            column_name: str,
            data_type: str = 'TEXT',
            multi: bool = False) -> str:
        """
        Direct create a custom column in the database.

        :param in_table:
        :param column_name:
        :param data_type:
        :param multi:
        :return new_table_name: The name of the new custom column table.
        """

    @abc.abstractmethod
    def direct_create_many_many_custom_column(
            self,
            target_table: str,
            custom_column_name: str
    ) -> str:
        """
        Direct create a many-many custom column in the database.

        :param target_table:
        :param custom_column_name:
        :return:
        """

    @abc.abstractmethod
    def direct_create_many_to_one_custom_column(
            self,
            target_table: str,
            custom_column_name: str
    ) -> str:
        """
        Directly create a many-to one custom column in the database.

        :param target_table:
        :param custom_column_name:
        :return:
        """

    @abc.abstractmethod
    def direct_create_one_to_many_custom_column(
            self,
            target_table: str,
            custom_column_name: str,
            datatype: str = 'TEXT') -> None:
        """
        Directly create a one to many custom column in the database attatched to the given table.

        :param target_table:
        :param custom_column_name:
        :param datatype:
        :return:
        """

    @abc.abstractmethod
    def direct_create_one_to_one_custom_column(
            self,
            target_table: str,
            custom_column_name: str,
            datatype: str = 'TEXT',
            normalized: bool = False) -> None:
        """
        Direct create a one to one custom column in the database attatched to the given table.

        :param target_table:
        :param custom_column_name:
        :param datatype:
        :param normalized:
        :return:
        """
