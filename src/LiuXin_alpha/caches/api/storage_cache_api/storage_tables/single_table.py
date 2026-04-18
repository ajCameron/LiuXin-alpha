"""
Represents a single table on the database.
"""

from __future__ import annotations

import abc

from typing import TYPE_CHECKING, Mapping, Sequence, Any, Optional, Iterable

from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.base_table import StorageCacheBaseTableAPI

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI
    from LiuXin_alpha.databases.db_types import MainTableID


class StorageCacheSingleTableAPI(StorageCacheBaseTableAPI):
    """
    Represents a single table on the database.

    Concrete cache tables are expected to hold a live database reference until
    the parent cache detaches or closes them.
    """

    # -----------------
    # - LINK PROPERTIES

    @abc.abstractmethod
    def linked_to(self) -> Iterable[str]:
        """
        Return an iterable of all the tables this table links to.

        :return:
        """

    # ----------------
    # - CREATE METHODS
    # - May be, largely, the same as update. But create in case they aren't.
    @abc.abstractmethod
    def create(self,
               table_id_val_map: Mapping[int, Any],
               db: "DatabaseAPI",
               target_column: Optional[str] = None,
               allow_case_change: bool = False) -> None:
        """
        Add new values to the cache and db at the same time.

        As a rule, when updating this table, call _this_ method.

        :param table_id_val_map: Keyed with the id and valued with the new value
        :param db: The database to read the table off.
        :param target_column:
        :param allow_case_change: If the value is the same up to a case change, skip update

        :return:
        """

    @abc.abstractmethod
    def _create_to_cache(self,
                        table_id_val_map: Mapping[int, Any],
                        target_column: Optional[str] = None,
                        allow_case_change: bool = False) -> None:
        """
        Update the internal cache.

        :param table_id_val_map:
        :param target_column:
        :param allow_case_change:

        :return:
        """

    @abc.abstractmethod
    def _create_to_db(self,
                      table_id_val_map: Mapping[int, Any],
                      db: "DatabaseAPI",
                      target_column: Optional[str] = None,
                      allow_case_change: bool = False) -> None:
        """
        Update the database.

        :param table_id_val_map:
        :param db:
        :param target_column:
        :param allow_case_change:
        :return:
        """

    # ----------------
    # --------------
    # - READ METHODS

    @abc.abstractmethod
    def get_values_for(self, column: str) -> Sequence[Any]:
        """
        Returns all the values for the given column - in their order.

        Elsewhere, in Views, you can sort. Here you just get the raw values.
        :param column:
        :return:
        """

    @abc.abstractmethod
    def get_unique_values(self, column: str) -> set[Any]:
        """
        Returns all the unique values for the given column.

        :param column:
        :return:
        """

    @abc.abstractmethod
    def get_ids_for_value(self, column: str, value: str) -> set[int]:
        """
        Returns all the ids for the given column and value.

        :param column:
        :param value:
        :return:
        """

    @abc.abstractmethod
    def get_col_value_from_id(self, table_id: MainTableID) -> Any:
        """
        Get the column value for a specific id.

        :param table_id:
        :return:
        """

    # --------------
    # ----------------
    # - UPDATE METHODS

    @abc.abstractmethod
    def update(self,
               table_id_val_map: Mapping[int, Any],
               db: "DatabaseAPI",
               target_column: Optional[str] = None,
               allow_case_change: bool = False) -> None:
        """
        Update both the cache and the database at the same time.

        As a rule, when updating this table, call _this_ method.

        :param table_id_val_map: Keyed with the id and valued with the new value
        :param db: The database to read the table off.
        :param target_column:
        :param allow_case_change: If the value is the same up to a case change, skip update

        :return:
        """

    @abc.abstractmethod
    def _update_cache(self,
                      table_id_val_map: Mapping[int, Any],
                      target_column: Optional[str] = None,
                      allow_case_change: bool = False) -> None:
        """
        Update the internal cache.

        :param table_id_val_map:
        :param target_column:
        :param allow_case_change:

        :return:
        """

    @abc.abstractmethod
    def _update_db(self,
                   table_id_val_map: Mapping[int, Any],
                   db: "DatabaseAPI",
                   target_column: Optional[str] = None,
                   allow_case_change: bool = False) -> None:
        """
        Update the database.

        :param table_id_val_map:
        :param db:
        :param target_column:
        :param allow_case_change:
        :return:
        """

    # ----------------
    # - DELETE METHODS
    @abc.abstractmethod
    def delete(
            self,
            table_ids: Iterable[int],
            db: "DatabaseAPI") -> None:
        """
        Delete from the cache and the database.

        :param table_ids:
        :param db:
        :return:
        """

    @abc.abstractmethod
    def _delete_from_cache(self, table_ids: Iterable[int]) -> None:
        """
        Delete entries from the cache by id.

        :param table_ids:
        :return:
        """

    @abc.abstractmethod
    def _delete_from_db(self, table_ids: Iterable[str], db: "DatabaseAPI") -> None:
        """
        Delete entries from the database.

        :param table_ids:
        :param db:
        :return:
        """


# Backwards-compatible alias while the typo is cleaned out elsewhere.
StorageStorageCacheSingleTableAPI = StorageCacheSingleTableAPI
