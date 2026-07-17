from __future__ import annotations

import abc
from typing import Optional, Iterable, Any


class DriverDatabasePropertiesMixinAPI(abc.ABC):
    """
    Contains the API for driver level database properties mixins.
    """

    @abc.abstractmethod
    def direct_get_declared_types_for_table(self, table: str) -> dict[str, str]:
        """
        Get the declared column/type pairs for the given table.

        :param table:
        :return:
        """

    @abc.abstractmethod
    def _invalidate_schema_caches(self) -> None:
        """
        Invalidate and clear the internal database schema caches.

        These are the tables/columns caches.
        :return:
        """

    @abc.abstractmethod
    def direct_get_column_headings(self, table: str, normalize: bool = False) -> list[str]:
        """
        Direct get the column headings for a given table.

        :param table:
        :param normalize:
        :return:
        """

    @abc.abstractmethod
    def direct_get_record_count(self, target_table: str) -> int:
        """
        Get the number of records for a given table.

        :param target_table:
        :return:
        """

    # Todo: Merge with the above method
    @abc.abstractmethod
    def direct_get_row_count(self, table: str) -> int:
        """
        Get the number of rows for a given table.

        :param table:
        :return:
        """

    @abc.abstractmethod
    def direct_get_tables(self, force_refresh: bool = False) -> dict[str, list[str]]:
        """
        Get all tables.

        :param force_refresh:
        :return:
        """

    @abc.abstractmethod
    def direct_get_tables_and_columns(self, force_refresh: bool = False) -> dict[str, list[str]]:
        """
        Direct get all tables and columns.

        :param force_refresh:
        :return:
        """

    # Todo: direct_*
    @abc.abstractmethod
    def direct_get_table_sqlite(self, table: str, conn: Any = None) -> str:
        """
        Get the SQLite which defines a table.

        :param table:
        :param conn:
        :return:
        """
