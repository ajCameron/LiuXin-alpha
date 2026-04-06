from __future__ import annotations

import abc
from typing import Generic, TYPE_CHECKING, Union, TypeVar

from LiuXin_alpha.databases.api import DatabaseAPI

if TYPE_CHECKING:
    from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.single_table import (
        StorageStorageCacheSingleTableAPI,
    )
    from LiuXin_alpha.databases.db_types import MainTableName

T = TypeVar("T")


class FieldBasicInterfaceAPI(abc.ABC, Generic[T]):
    """
    Basic interface for the field system.
    """

    @abc.abstractmethod
    def read(self, db: "DatabaseAPI") -> None:
        """
        Read off the database into the internal cache.

        :param db:
        :return:
        """

    @abc.abstractmethod
    def get_main_table(
        self,
        name: Union[MainTableName, "StorageStorageCacheSingleTableAPI"],
    ) -> "StorageStorageCacheSingleTableAPI":
        """
        Get the cached table.

        :param name:
        :return:
        """
