import abc
from typing import Generic, TYPE_CHECKING, Union

from LiuXin_alpha.databases.api import DatabaseAPI
from LiuXin_alpha.databases.api.cache_api.fields.one_one_field import T

if TYPE_CHECKING:
    from LiuXin_alpha.databases.db_types import MainTableName
    from LiuXin_alpha.databases.api.cache_api.tables.single_table import CacheSingleTableAPI



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
    def get_main_table(self, name: Union[MainTableName, "CacheSingleTableAPI"]) -> "CacheSingleTableAPI":
        """
        Get the cached table.

        :param name:
        :return:
        """

