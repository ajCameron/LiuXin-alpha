"""
Fields within and without tables on the database.
"""
from __future__ import annotations

import abc
import dataclasses

from typing import TYPE_CHECKING, Union, TypeVar, Generic, Optional

from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.base_field import FieldBasicInterfaceAPI

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI
    from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.link_tables.one_one_tables import (
        StorageCacheOneToOneLinkTableAPI,
    )
    from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.single_table import (
        StorageStorageCacheSingleTableAPI,
    )
    from LiuXin_alpha.databases.db_types import (
        MainTableColumnName,
        MainTableID,
        MainTableName,
    )

T = TypeVar("T")


@dataclasses.dataclass
class OneOneInOneTableFieldUpdate(Generic[T]):
    """
    Update for a one-to-one field stored in a single table.
    """

    added_maps: dict[MainTableID, Optional[T]]
    updated_maps: dict[MainTableID, Optional[T]]
    deleted_ids: set[MainTableID]
    dirtied: set[MainTableID]

    # Are the values in this field unique?
    unique: bool = False


class CacheOneOneInSameTableFieldAPI(FieldBasicInterfaceAPI[T]):
    """
    Represents a one-to-one field stored directly in a single table.
    """

    # The table the column is in.
    in_table: "StorageStorageCacheSingleTableAPI"

    _table_id_col: "MainTableName"
    _table_cached_col: "MainTableName"

    _db: "DatabaseAPI"

    def __init__(
        self,
        in_table: Union["StorageStorageCacheSingleTableAPI", "MainTableName"],
        db: "DatabaseAPI",
    ) -> None:
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
        Return all the ids known to this field.

        :return:
        """

    @property
    @abc.abstractmethod
    def values(self) -> list[T]:
        """
        Return all the values known to this field.

        :return:
        """

    @property
    @abc.abstractmethod
    def values_set(self) -> set[T]:
        """
        Return all the values known to this field.

        :return:
        """

    @property
    @abc.abstractmethod
    def ids_values_map(self) -> dict[MainTableID, Optional[T]]:
        """
        Return all known ids mapped to their current value.

        :return:
        """

    @abc.abstractmethod
    def get_value_from_id(self, table_id: MainTableID) -> Optional[T]:
        """
        Get the cached value for the given id.

        :param table_id:
        :return:
        """

    @abc.abstractmethod
    def get_ids_from_value(self, value: T) -> list[MainTableID]:
        """
        Get ids matching the given value.

        Uniqueness is not guaranteed.

        :param value:
        :return:
        """


class CacheOneOneInSameTableFieldUniqueAPI(CacheOneOneInSameTableFieldAPI[T]):
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


@dataclasses.dataclass
class OneOneInTwoTableFieldUpdate(Generic[T]):
    """
    Update for a one-to-one field stored across a link table and a dst table.

    The mapping is keyed by the src table id and valued with the value to be
    written into the dst table target column.
    """

    src_table: MainTableName
    dst_table: MainTableName
    dst_table_target_column: MainTableColumnName

    added_maps: dict[MainTableID, Optional[T]]
    updated_maps: dict[MainTableID, Optional[T]]
    deleted_ids: set[MainTableID]
    dirtied: set[MainTableID]

    # Are the values in this field unique?
    unique: bool = False


class CacheOneOneInTwoTableFieldAPI(FieldBasicInterfaceAPI[T]):
    """
    Represents a one-to-one field where the value lives in a dst table linked
    to a src table by a one-to-one link table.
    """

    src_table: "StorageStorageCacheSingleTableAPI"
    dst_table: "StorageStorageCacheSingleTableAPI"

    # We identify the src row by this column and cache the value from this dst column.
    src_table_id_col: MainTableColumnName
    dst_table_cache_col: MainTableColumnName

    # Connecting the two tables.
    link_table: "StorageCacheOneToOneLinkTableAPI"

    _db: "DatabaseAPI"

    def __init__(
        self,
        src_table: Union["StorageStorageCacheSingleTableAPI", "MainTableName"],
        src_table_id_col: MainTableColumnName,
        dst_table: Union["StorageStorageCacheSingleTableAPI", "MainTableName"],
        dst_table_cache_col: MainTableColumnName,
        db: "DatabaseAPI",
    ) -> None:
        """
        Startup the cache for the given field.

        :param src_table:
        :param src_table_id_col:
        :param dst_table:
        :param dst_table_cache_col:
        :param db:
        """
        self.src_table = self.get_main_table(src_table)
        self.dst_table = self.get_main_table(dst_table)

        self.link_table = self.get_link_table(self.src_table, self.dst_table)

        self.src_table_id_col = src_table_id_col
        self.dst_table_cache_col = dst_table_cache_col

        self._db = db

    @abc.abstractmethod
    def get_link_table(
        self,
        src_table: Union["StorageStorageCacheSingleTableAPI", MainTableName],
        dst_table: Union["StorageStorageCacheSingleTableAPI", MainTableName],
    ) -> "StorageCacheOneToOneLinkTableAPI":
        """
        Resolve the one-to-one link table connecting the two tables.

        :param src_table:
        :param dst_table:
        :return:
        """

    @abc.abstractmethod
    def update(self, update: OneOneInTwoTableFieldUpdate[T]) -> None:
        """
        Update the field, and the underlying tables/db.

        :param update:
        :return:
        """

    @property
    def src_table_name(self) -> MainTableName:
        """
        Get the name of the src table.

        :return:
        """
        return self.src_table.table

    @property
    def dst_table_name(self) -> MainTableName:
        """
        Get the name of the dst table.

        :return:
        """
        return self.dst_table.table

    @property
    @abc.abstractmethod
    def ids(self) -> set[MainTableID]:
        """
        Get all src ids known to this field.

        :return:
        """

    @property
    @abc.abstractmethod
    def values(self) -> list[T]:
        """
        Get all values in the cache.

        :return:
        """

    @property
    @abc.abstractmethod
    def values_set(self) -> set[T]:
        """
        Return all distinct values known to this field.

        :return:
        """

    @property
    @abc.abstractmethod
    def ids_values_map(self) -> dict[MainTableID, Optional[T]]:
        """
        Return the src-id to value map for this field.

        :return:
        """

    @property
    @abc.abstractmethod
    def dst_ids_values_map(self) -> dict[MainTableID, Optional[T]]:
        """
        Return the dst-id to value map for this field.

        :return:
        """

    @abc.abstractmethod
    def get_value_from_src_id(self, src_id: MainTableID) -> Optional[T]:
        """
        Get the field value from the src id.

        :param src_id:
        :return:
        """

    def get_value_from_id(self, table_id: MainTableID) -> Optional[T]:
        """
        Compatibility alias for src-keyed callers.

        :param table_id:
        :return:
        """
        return self.get_value_from_src_id(table_id)

    @abc.abstractmethod
    def get_value_from_dst_id(self, dst_id: MainTableID) -> Optional[T]:
        """
        Get the field value from the dst id.

        :param dst_id:
        :return:
        """

    @abc.abstractmethod
    def get_dst_id_from_src_id(self, src_id: MainTableID) -> Optional[MainTableID]:
        """
        Resolve the linked dst id for the given src id.

        :param src_id:
        :return:
        """

    @abc.abstractmethod
    def get_src_id_from_dst_id(self, dst_id: MainTableID) -> Optional[MainTableID]:
        """
        Resolve the linked src id for the given dst id.

        :param dst_id:
        :return:
        """

    @abc.abstractmethod
    def get_src_ids_from_value(self, value: T) -> list[MainTableID]:
        """
        Get src ids matching the given value.

        Uniqueness is not guaranteed.

        :param value:
        :return:
        """

    @abc.abstractmethod
    def get_dst_ids_from_value(self, value: T) -> list[MainTableID]:
        """
        Get dst ids matching the given value.

        Uniqueness is not guaranteed.

        :param value:
        :return:
        """


class CacheOneOneInTwoTableFieldUniqueAPI(CacheOneOneInTwoTableFieldAPI[T]):
    """
    Field across two tables where the values are unique.
    """

    @abc.abstractmethod
    def get_src_id_from_unique_value(self, value: T) -> Optional[MainTableID]:
        """
        Match a src id to an existing unique value.

        :param value:
        :return:
        """

    @abc.abstractmethod
    def get_dst_id_from_unique_value(self, value: T) -> Optional[MainTableID]:
        """
        Match a dst id to an existing unique value.

        :param value:
        :return:
        """
