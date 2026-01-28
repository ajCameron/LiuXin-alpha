
import abc

from LiuXin_alpha.metadata.containers.metadata_containers.creator_container import CreatorContainer

from LiuXin_alpha.databases.api import RowAPI, DatabaseAPI



class ManyToManyPriorityTypedMetadataContainer(abc.ABC):
    """
    Contains metadata represented by a many-to-many link with priority and type info.
    """

    _db: DatabaseAPI
    _primary_table_row: RowAPI
    _secondary_table: str

    def __init__(self, db: DatabaseAPI, primary_table_row: RowAPI, secondary_table: str) -> None:
        """
        Initialize the container and attach it to a database.

        :param primary_table_row:
        :param secondary_table:
        """
        self._db = db
        self._primary_table_row = primary_table_row
        self._secondary_table = secondary_table

    @property
    def db(self) -> DatabaseAPI:
        """
        Protect the database from change.

        :return:
        """
        return self._db

    @property
    def primary_table_row(self) -> RowAPI:
        """
        Protect the primary table row from change.

        :return:
        """
        return self._primary_table_row

    @property
    def secondary_table(self) -> str:
        """
        Protect the secondary table from change.

        :return:
        """
        return self._secondary_table

    @abc.abstractmethod
    def get_link_types(self) -> frozenset[str]:
        """
        Return a frozenset of all possible link types.

        :return:
        """

    @abc.abstractmethod
    def get_rows(self) -> set[RowAPI]:
        """
        Return a set of all linked rows.

        :return:
        """

    @abc.abstractmethod
    def get_priority_rows(self) -> list[RowAPI]:
        """
        Return all the rows in the database linked to the primary row.

        :return:
        """

    @abc.abstractmethod
    def get_typed_rows(self) -> dict[str, frozenset[RowAPI]]:
        """
        Return a dictionary keyed with the row type and valued with a set of Rows.

        :return:
        """

    @abc.abstractmethod
    def get_priority_typed_rows(self) -> dict[str, list[RowAPI]]:
        """
        Return a dictionary keyed with the row type and valued with a list of Rows.

        Thus conveying both priority and type information.
        :return:
        """

