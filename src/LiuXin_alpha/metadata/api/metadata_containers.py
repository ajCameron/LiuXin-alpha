
import abc

from LiuXin_alpha.databases.api import RowAPI, DatabaseAPI


class ManyToManyMetadataContainerAPI(abc.ABC):
    """
    Container representing a many-to-many metadata link.
    """
    _db: DatabaseAPI
    _primary_table_row: RowAPI
    _secondary_table: str

    # ---------------------------------
    # - CONTAINER PROPERTIES START HERE

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

    #
    # ---------------------------------
    # -------------------------------
    # - ROW ACCESS METHODS START HERE


    @abc.abstractmethod
    def get_rows(self) -> frozenset[RowAPI]:
        """
        Return a set of all linked rows.

        :return:
        """

    def get_row_ids(self) -> frozenset[int]:
        """
        Return a set of all linked rows.

        :return:
        """
        return frozenset([_.id for _ in self.get_rows()])

    #
    # -------------------------------




class ManyToManyPriorityTypedMetadataContainerAPI(ManyToManyMetadataContainerAPI):
    """
    Contains metadata represented by a many-to-many link with priority and type info.
    """

    def __init__(self, db: DatabaseAPI, primary_table_row: RowAPI, secondary_table: str) -> None:
        """
        Initialize the container and attach it to a database.

        :param primary_table_row:
        :param secondary_table:
        """
        self._db = db
        self._primary_table_row = primary_table_row
        self._secondary_table = secondary_table

    @abc.abstractmethod
    def get_link_types(self) -> frozenset[str]:
        """
        Return a frozenset of all possible link types.

        :return:
        """

    # -------------------------------
    # - ROW ACCESS METHODS START HERE

    @abc.abstractmethod
    def get_priority_rows(self) -> list[RowAPI]:
        """
        Return all the rows in the database linked to the primary row.

        :return:
        """

    def get_priority_row_ids(self) -> list[int]:
        """
        Return a list of all the ids in the database linked to the primary row.

        :return:
        """
        return [_.id for _ in self.get_priority_rows()]

    @abc.abstractmethod
    def get_typed_rows(self) -> dict[str, frozenset[RowAPI]]:
        """
        Return a dictionary keyed with the row type and valued with a set of Rows.

        :return:
        """

    def get_typed_row_ids(self) -> dict[str, frozenset[int]]:
        """
        Return a dictionary keyed with the row type and valued with a set of row ids.

        :return:
        """
        row_ids_dict = dict()
        rows_dict = self.get_typed_rows()

        for rt in rows_dict:
            row_ids_dict[rt] = frozenset([_.id for _ in rows_dict[rt]])

        return row_ids_dict

    @abc.abstractmethod
    def get_priority_typed_rows(self) -> dict[str, list[RowAPI]]:
        """
        Returns a dictionary keyed with the row type and valued with a list of Rows.

        Thus conveying both priority and type information.
        :return:
        """

    def get_priority_typed_row_ids(self) -> dict[str, list[int]]:
        """
        Returns a dictionary keyed with the row type and valued with a list of row ids.

        :return:
        """
        row_ids_dict = dict()
        rows_dict = self.get_typed_rows()

        for rt in rows_dict:
            row_ids_dict[rt] = [_.id for _ in rows_dict[rt]]

        return row_ids_dict

    @abc.abstractmethod
    def rows_for_type(self, target_type: str) -> list[RowAPI]:
        """
        Return all the rows for a given type.

        :param target_type:
        :return:
        """

    @abc.abstractmethod
    def row_ids_for_type(self, target_type: str) -> list[RowAPI]:
        """
        Return all the rows for a given type.

        :param target_type:
        :return:
        """
        return [_.id for _ in self.rows_for_type(target_type)]

    #
    # -------------------------------


