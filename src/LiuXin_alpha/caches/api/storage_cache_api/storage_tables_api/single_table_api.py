"""
Represents a single table on the database.
"""

from __future__ import annotations

import abc

from typing import TYPE_CHECKING, Sequence, Any, Iterable

from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.base_table_api import StorageCacheBaseTableAPI

if TYPE_CHECKING:
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

    # Storage components intentionally expose no public database mutation
    # contract. Application writes enter through the composed Cache facade,
    # delegate semantic persistence to Catalog, and then reconcile storage.


# Backwards-compatible alias while the typo is cleaned out elsewhere.
StorageStorageCacheSingleTableAPI = StorageCacheSingleTableAPI
