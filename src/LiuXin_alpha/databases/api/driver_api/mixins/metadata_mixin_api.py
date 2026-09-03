"""Driver-level contracts for database identity and metadata operations."""

from __future__ import annotations

import abc

from typing import Any, Optional


class DriverMetadataMixinAPI(abc.ABC):
    """
    Mixin of driver methods to deal with the metadata off the database.
    """

    # Toodo: not clear how different to database version.
    @property
    @abc.abstractmethod
    def user_version(self) -> Optional[str]:
        """
        Get the user version of the database.

        :return:
        """


    @abc.abstractmethod
    def set_database_version(self) -> None:
        """
        Set the version of the database.

        Used during schema upgrades.
        :return:
        """

    @abc.abstractmethod
    def _initialize_md(self) -> None:
        """
        Read the metadata off the database and update the internal cache.

        :return:
        """

    @abc.abstractmethod
    def direct_get_schema_version(self) -> Optional[int]:
        """
        Return the schema version of the database.

        :return:
        """

    @abc.abstractmethod
    def direct_last_modified(self):
        """
        Check when the database was last modified.

        :return:
        """

    @abc.abstractmethod
    def direct_read_metadata(self, md_field_name: str) -> Any:
        """
        Directly read a metadata field from the database.

        :param md_field_name:
        :return:
        """

    @abc.abstractmethod
    def direct_set_db_unique_id(self, force_value: Optional[str] = None) -> None:
        """
        Direct set the databases unique id.

        :param force_value:
        :return:
        """

    @abc.abstractmethod
    def direct_write_metadata(self, md_field_name: str, md_field_value: Any) -> None:
        """
        Write a metadata field to the database.

        :param md_field_name:
        :param md_field_value:
        :return:
        """

    @abc.abstractmethod
    def last_modified(self):
        """
        Return the last modified date of the database.

        :return:
        """
