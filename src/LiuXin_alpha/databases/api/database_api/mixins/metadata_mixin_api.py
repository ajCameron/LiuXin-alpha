from __future__ import annotations

import abc
from typing import Iterable, Any


class DatabaseMetadataMixinAPI(abc.ABC):
    """
    Typed API for ``DatabaseMetadataMixin``.
    """

    @property
    @abc.abstractmethod
    def uuid(self) -> str:
        """
        Return the uuid for the database.

        :return:
        """

    @uuid.setter
    @abc.abstractmethod
    def uuid(self, value: str) -> None:
        """
        Set the uuid for the database.

        :param value:
        :return:
        """

    @property
    @abc.abstractmethod
    def library_id(self) -> str:
        """
        Get the library id for the database.

        :return:
        """

    @library_id.setter
    @abc.abstractmethod
    def library_id(self, value: str) -> None:
        """
        Set the library id for the database.

        :param value:
        :return:
        """

    @property
    @abc.abstractmethod
    def database_version(self) -> str:
        """
        Get the current database version.

        :return:
        """

    @database_version.setter
    @abc.abstractmethod
    def database_version(self, value: str) -> None:
        """
        Set the database version for the database.

        :param value:
        :return:
        """

    @abc.abstractmethod
    def get_tables(self, force_refresh: bool = False) -> Iterable[str]:
        """
        Get all the tables in the database.

        :param force_refresh: Bypass and refresh the cache
        :return:
        """

    @abc.abstractmethod
    def get_column_headings(self, table: str) -> list[str]:
        """
        Get the column headings for the table of the database.

        :param table:
        :return:
        """

    @abc.abstractmethod
    def get_view_column_headings(self, view: str) -> list[str]:
        """
        Get the column headings for a view.

        :param view:
        :return:
        """

    @abc.abstractmethod
    def get_tables_and_columns(self) -> dict[str, list[str]]:
        """
        Get all the tables and columns for the database.

        :return:
        """

    @abc.abstractmethod
    def get_record_count(self, target_table: str) -> int:
        """
        Get the raw record count for the table.

        :param target_table:
        :return:
        """

    @abc.abstractmethod
    def get_max(self, column: str) -> Any:
        """
        Return the max value for the given column.

        :param column:
        :return:
        """

    @abc.abstractmethod
    def get_min(self, column: str) -> Any:
        """
        Return the min value for the given column.

        :param column:
        :return:
        """

    @abc.abstractmethod
    def row_counts(self) -> str:
        """
        Get the raw record count for the table.

        :return:
        """

    # ---------------------------------------------------------------------------------------------
    # Database metadata (uuid/library_id/version)
    # ---------------------------------------------------------------------------------------------
    @property
    @abc.abstractmethod
    def uuid(self) -> str:
        """Database UUID (used for cache keys, change detection, etc.)."""

    @uuid.setter
    @abc.abstractmethod
    def uuid(self, value: str) -> None:
        ...

    @property
    @abc.abstractmethod
    def library_id(self) -> str:
        """Library UUID (unique identifier for the library itself)."""

    @library_id.setter
    @abc.abstractmethod
    def library_id(self, value: str) -> None:
        ...

    @property
    @abc.abstractmethod
    def database_version(self) -> str:
        """Schema version string stored in the database."""

    @database_version.setter
    @abc.abstractmethod
    def database_version(self, value: str) -> None:
        ...
