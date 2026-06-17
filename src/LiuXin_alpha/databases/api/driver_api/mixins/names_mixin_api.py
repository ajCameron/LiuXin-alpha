from __future__ import annotations

import abc

from typing import Any, Callable, Dict, List, Optional, Tuple, Union, Iterable

# Todo: I suspect this is used EVERYWHERE. So let's try and dry out the code base.
class DriverNamesMixinAPI(abc.ABC):
    """
    Names manipulation methods for the drivers.
    """

    # Todo: First class method. Should be used in more places. Including custom columns.
    @staticmethod
    @abc.abstractmethod
    def _validate_table_name(table_name: str) -> bool:
        """
        Check that the trial table name is actually a valid SQL(e.t.c) table name.

        :param table_name:
        :return:
        """

    @abc.abstractmethod
    def direct_validate_existing_table_name(self, test_name: str) -> bool:
        """
        Check the trial table name is actually valid SQL.

        :param test_name:
        :return:
        """

    @staticmethod
    @abc.abstractmethod
    def direct_get_column_base(table_name: str) -> str:
        """
        Get the base name for the column.

        E.g in "titles" it's "title" - column names are made by adding to the end of it.
        :param table_name:
        :return:
        """

    # Todo: The two methods seem to be doing the same thing
    @staticmethod
    @abc.abstractmethod
    def _get_table_col_base(table_name: str) -> str:
        """
        Get the base column name for a given table.

        :param table_name:
        :return:
        """

    @abc.abstractmethod
    def direct_get_column_name(self, table_name: str) -> str:
        """
        Get the column name for a given table.

        :param table_name:
        :return:
        """

    @abc.abstractmethod
    def direct_get_datestamp_column(
            self,
            table: str,
            tables_and_columns: Optional[dict[str, list[str]]] = None) -> str:
        """
        Direct get the datestamp column for a given table.

        :param table:
        :param tables_and_columns:
        :return:
        """

    @abc.abstractmethod
    def _get_id_column(
            self,
            table: str,
            tables_and_columns: Optional[dict[str, Iterable[str]]] = None) -> None:
        """
        Get the id column for a given table.

        :param table:
        :param tables_and_columns:
        :return:
        """


    @abc.abstractmethod
    def direct_get_id_column(
            self,
            table: str,
            tables_and_columns: Optional[dict[str, list[str]]] = None) -> str:
        """
        Get the id column for a given table.

        :param table:
        :param tables_and_columns:
        :return:
        """

    @staticmethod
    @abc.abstractmethod
    def get_allowed_types_table_name(for_table: str) -> str:
        """
        Generate and return the names for the allowed table to attach to a given interlink table.

        :param for_table:
        :return:
        """

    @abc.abstractmethod
    def get_allowed_types_table_name_intralinks(self, for_table: str) -> str:
        """
        Generate and return the names for the allowed table to attach to a given intralink table.

        :param for_table:
        :return:
        """

    @abc.abstractmethod
    def get_display_column(self, table_name: str) -> str:
        """
        Return the display column for the given table - if it exists.

        :param table_name:
        :return:
        """

    # Todo: This sounds like several others methods...
    @abc.abstractmethod
    def get_full_column_name(self, target_table):
        """
        Return thw full column name for the given table.

        :param target_table:
        :return:
        """

    @staticmethod
    @abc.abstractmethod
    def get_interlink_table_name(table1: str, table2: str) -> tuple[str, str]:
        """
        Get the interlink table name/column name for a given table.

        :param table1:
        :param table2:
        :return:
        """

    @abc.abstractmethod
    def get_parent_column_name(self, table_name: str) -> str:
        """
        Get the parent column name for a given table.

        :param table_name:
        :return:
        """

    @abc.abstractmethod
    def get_tree_id_column(self, target_table: str) -> str:
        """
        Get the tree structure id column for the given table.

        :param target_table:
        :return:
        """

    @abc.abstractmethod
    def validate_existing_table_name(self, test_name: str) -> bool:
        """
        Check that the given test name is a valid table name of the table.

        :param test_name:
        :return:
        """


    @staticmethod
    @abc.abstractmethod
    def _get_link_table_name_col_name(primary_table: str, secondary_table: str) -> tuple[str, str]:
        """
        Get the column name for a link table linking the two given tables.

        :param primary_table:
        :param secondary_table:
        :return:
        """