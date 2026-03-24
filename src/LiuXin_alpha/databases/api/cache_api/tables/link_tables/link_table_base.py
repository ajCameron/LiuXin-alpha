

"""
Represents a cache link table.
"""


from __future__ import annotations

import abc

from typing import TYPE_CHECKING, TypeVar, Generic, Literal, Union

from LiuXin_alpha.databases.api.cache_api.tables.base_table import CacheBaseTableAPI

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI
    from LiuXin_alpha.databases.api.cache_api.tables.base_table import TableTypes
    from LiuXin_alpha.databases.db_types import SrcTableID, DstTableID

T = TypeVar("T")



class CacheLinkTableBaseAPI(CacheBaseTableAPI, Generic[T]):
    """
    Represents a single table on the database.

    We try not to hold references to the database, as it makes shutdown harder.
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
