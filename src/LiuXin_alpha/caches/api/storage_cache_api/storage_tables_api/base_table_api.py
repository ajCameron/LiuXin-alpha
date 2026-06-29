
"""
Base class for all cache table APIS.
"""

import abc

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI

ONE_ONE, MANY_ONE, MANY_MANY, ONE_MANY = range(4)

null = object()

from dataclasses import dataclass

from enum import Enum

class TableTypes(Enum):
    """
    Available table types.
    """
    ONE_ONE = ONE_ONE
    MANY_ONE = MANY_ONE
    MANY_MANY = MANY_MANY
    ONE_MANY = ONE_MANY


@dataclass(frozen=True, slots=True)
class TableMetadata:
    """
    Defines a table for the cache.
    """
    table_name: str

    main_table: bool
    
    is_interlink: bool = False
    is_intralink: bool = False



class StorageCacheBaseTableAPI(abc.ABC):
    """
    Base table from which all other tables should descend.
    """

    table: str
    db: "DatabaseAPI"
    metadata: TableMetadata

    def __init__(self, table: str, db: "DatabaseAPI", metadata: TableMetadata) -> None:
        """
        Constructor.

        :param table: The table this class will represent.
        :param db: The database to read the table off.
        """
        self.table = table
        self.db = db
        self.metadata = metadata


    @abc.abstractmethod
    def read(self, db: "DatabaseAPI") -> None:
        """
        Load the table from the database.

        :return:
        """

    @abc.abstractmethod
    def reload(self, db: "DatabaseAPI") -> None:
        """
        Preform a reload of the table from the database.

        :param db:
        :return:
        """

    # ------------------
    # - TABLE PROPERTIES

    @property
    @abc.abstractmethod
    def column_headings(self) -> list[str]:
        """
        Get the current column headings for the loaded table.

        :return:
        """

    @property
    @abc.abstractmethod
    def column_types(self) -> dict[str, str]:
        """
        Get a dict keyed with the column headings and value with their type strings.

        :return:
        """

