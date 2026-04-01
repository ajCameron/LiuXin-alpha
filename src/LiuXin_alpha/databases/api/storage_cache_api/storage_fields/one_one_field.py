
"""
Fields within and without tables on the database.
"""

import abc
import dataclasses

from typing import TYPE_CHECKING, Union, TypeVar, Generic, Optional

from LiuXin_alpha.databases.api.storage_cache_api.storage_fields.base_field import FieldBasicInterfaceAPI

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.storage_cache_api.storage_tables.single_table import StorageStorageCacheSingleTableAPI
    from LiuXin_alpha.databases.db_types import MainTableID, MainTableName, InterlinkExtraTypes
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI

T = TypeVar('T')


@dataclasses.dataclass
class OneOneInOneTableFieldUpdate(Generic[T]):
    """
    Update for a one to one field.
    """

    added_maps: dict[MainTableID, Optional[T]]

    updated_maps: dict[MainTableID, Optional[T]]

    deleted_ids: set[MainTableID]

    dirtied: set[MainTableID]

    # Are the values in this field unique?
    unique: bool = False


class CacheOneOneInSameTableFieldAPI(FieldBasicInterfaceAPI):
    """
    Represents a field in a single field in the database.
    """
    # The table the column is in
    in_table: "StorageStorageCacheSingleTableAPI"

    _table_id_col: "MainTableName"
    _table_cached_col: "MainTableName"

    _db: "DatabaseAPI"

    def __init__(
            self,
            in_table: Union["StorageStorageCacheSingleTableAPI", MainTableName],
            db: "DatabaseAPI") -> None:
        """
        Startup the cache.

        :param in_table:
        :param db:
        """
        self.in_table = self.get_main_table(in_table)

        self._db = db


    @abc.abstractmethod
    def update(self, update: OneOneInOneTableFieldUpdate[T]) -> None:
        """
        Update the field, and the underlying table/db.

        :param update:
        :return:
        """

    @property
    def table_name(self) -> MainTableName:
        """
        Get the name of the table we're in.

        :return:
        """
        return self.in_table.table

    @property
    @abc.abstractmethod
    def ids(self) -> set[MainTableID]:
        """
        Return all the ids known to this table.

        :return:
        """

    @property
    @abc.abstractmethod
    def values(self) -> list[T]:
        """
        Return all the values known to this table.

        :return:
        """

    @property
    @abc.abstractmethod
    def values_set(self) -> set[T]:
        """
        Return all the values known to this table.

        :return:
        """

    @property
    @abc.abstractmethod
    def ids_values_map(self) -> dict[MainTableID, Optional[T]]:
        """
        Return all the ids-values known to this table.

        :return:
        """

    @abc.abstractmethod
    def get_value_from_id(self, table_id: MainTableID) -> Optional[T]:
        """
        Get the value from the id.

        :param table_id:
        :return:
        """

    @abc.abstractmethod
    def get_ids_from_value(self, value: T) -> list[MainTableID]:
        """
        Get the ids from the value.

        Uniqueness is not guaranteed.
        :param value:
        :return:
        """


class CacheOneOneInSameTableFieldUniqueAPI(CacheOneOneInSameTableFieldAPI):
    """
    Field in one table where the values are unique.
    """

    @abc.abstractmethod
    def get_id_from_value(self, value: T) -> Optional[MainTableID]:
        """
        Match an id to an existing value.

        :param value:
        :return:
        """