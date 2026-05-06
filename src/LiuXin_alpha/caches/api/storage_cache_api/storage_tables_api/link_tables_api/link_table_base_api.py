

"""
Represents a cache link table.
"""


from __future__ import annotations

import abc

from typing import TYPE_CHECKING, TypeVar, Generic

from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.base_table import StorageCacheBaseTableAPI

if TYPE_CHECKING:
    from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.base_table import TableTypes

T = TypeVar("T")



class StorageCacheLinkTableBaseAPI(StorageCacheBaseTableAPI, Generic[T]):
    """
    Represents a cached link table.

    Concrete cache tables are expected to hold a live database reference until
    the parent cache detaches or closes them.
    """

    _table_type: TableTypes
    _priority = False
    _typed = False

    @property
    def table_type(self) -> TableTypes:
        """
        The table type this class will represent.

        :return:
        """
        return self._table_type

    @property
    def priority(self) -> bool:
        """
        Is the table a priority table.

        :return:
        """
        return self._priority

    @property
    def typed(self) -> bool:
        """
        Is the table a typed table.

        :return:
        """
        return self._typed

    @property
    @abc.abstractmethod
    def primary_table(self) -> str:
        """
        Interlinks join two tables - this is the one on the left.

        :return:
        """

    @property
    @abc.abstractmethod
    def secondary_table(self) -> str:
        """
        Interlinks join two tables - this is the one on the left.

        :return:
        """

    @property
    @abc.abstractmethod
    def designated_secondary_col(self) -> str:
        """
        Which columns has been designated the value one in the other table.

        :return:
        """

    @abc.abstractmethod
    def get_primary_id_secondary_value_id_map(self) -> dict[int, int]:
        """
        Get a map keyed with the primary id and valued with the secondary id.

        :return:
        """

    @abc.abstractmethod
    def get_secondary_id_primary_id_map(self) -> dict[int, int]:
        """
        Dict keyed with the secondary id and valued with the primary id.-

        :return:
        """
