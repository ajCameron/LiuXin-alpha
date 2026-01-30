
"""
Containers for one to one links.
"""


import abc

from typing import Iterable, Optional, Union

from pygments.lexers import q

from LiuXin_alpha.databases.api import RowAPI, DatabaseAPI


class OneToOneMetadataContainerAPI(abc.ABC):
    """
    Container representing a one-to-one metadata link between two tables.

    E.g. An isbn can only be assigned to a single item.
    (though these are not common).
    """

    _db: DatabaseAPI
    _primary_table_row: RowAPI
    _secondary_table_row: RowAPI

    def __init__(
            self,
            db: DatabaseAPI,
            primary_table_row: Union[int, str, RowAPI],
            secondary_table_row: Union[int, str, RowAPI]
    ) -> None:
        """
        Initialize the container and attach it to a database.

        :param primary_table_row:
        :param secondary_table_row:
        """
        self._db = db
        self._primary_table_row = self.ensure_row(primary_table_row)
        self._secondary_table_row = self.ensure_row(secondary_table_row)

    @abc.abstractmethod
    def ensure_row(self, row_like: Union[int, str, RowAPI]) -> RowAPI:
        """
        Front end for the ensure mechanism - takes a row like thing and gives you back the row.

        :param row_like:
        :return:
        """

    @property
    def db(self) -> DatabaseAPI:
        """
        Return the current database this row is attatched to.

        :return:
        """
        return self._db

    @property
    def primary_table_row(self) -> RowAPI:
        """
        The primary table row.

        :return:
        """
        return self._primary_table_row

    @property
    def secondary_table_row(self) -> RowAPI:
        """
        The secondary table row.

        :return:
        """
        return self._secondary_table_row

    @property
    @abc.abstractmethod
    def link_table_nane(self) -> str:
        """
        Return the name of the link table between the two tables.

        :return:
        """

    @abc.abstractmethod
    def sync(self) -> None:
        """
        Sync row changes back to the database.

        :return:
        """


class InTableMetadataLinkContainerAPI:
    """
    Container representing an in-table metadata link between two tables.

    Some tables have foreign key columns in them. This container represents those links.
    """
    _db: DatabaseAPI
    _primary_table_row: RowAPI
    _secondary_table_row: RowAPI
    _link_table_column: str

    def __init__(
            self,
            db: DatabaseAPI,
            primary_table_row: Union[int, str, RowAPI],
            secondary_table_row: Union[int, str, RowAPI]
    ) -> None:
        """
        Initialize the container and attach it to a database.

        :param primary_table_row:
        :param secondary_table_row:
        """
        self._db = db
        self._primary_table_row = self.ensure_row(primary_table_row)
        self._secondary_table_row = self.ensure_row(secondary_table_row)

    @abc.abstractmethod
    def ensure_row(self, row_like: Union[int, str, RowAPI]) -> RowAPI:
        """
        Front end for the ensure mechanism - takes a row like thing and gives you back the row.

        :param row_like:
        :return:
        """

    @property
    def db(self) -> DatabaseAPI:
        """
        Return the current database this row is attatched to.

        :return:
        """
        return self._db

    @property
    def primary_table_row(self) -> RowAPI:
        """
        The primary table row.

        :return:
        """
        return self._primary_table_row

    @property
    def secondary_table_row(self) -> RowAPI:
        """
        The secondary table row.

        :return:
        """
        return self._secondary_table_row

    @property
    @abc.abstractmethod
    def link_column_nane(self) -> str:
        """
        Return the name of the link table between the two tables.

        :return:
        """

    @abc.abstractmethod
    def sync(self) -> None:
        """
        Sync row changes back to the database.

        :return:
        """
