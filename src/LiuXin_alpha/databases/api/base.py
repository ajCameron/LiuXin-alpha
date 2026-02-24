"""Base database API contracts."""

from __future__ import annotations

import abc
import datetime

from typing import Any, Iterable, Iterator, Optional, Union

class DatabaseBuilderAPI(abc.ABC):
    """
    API for the fundamental database builder class.
    """

    @abc.abstractmethod
    def set_database_version(self) -> None:
        """
        Set the database version.

        :return:
        """

class RowAPI(abc.ABC):
    """
    API for a row off the database.
    """

    def __init__(self, database: DatabaseAPI, row_dict: Optional[dict[str, str]] = None,
                 read_only: bool = False) -> None:
        """
        Represents a single row from the LiuXin database.

        :param database: A LiuXin database object
        :param row_dict: Keyed with the column names and valued with their values.
        :param read_only: If True then the row is loaded in read only mode
        :return:
        """

    @abc.abstractmethod
    def sync(self) -> None:
        """
        Synchronize the row with the database.

        :return:
        """

    @property
    @abc.abstractmethod
    def table(self) -> str:
        """
        Return the name of the table this row is in.

        :return:
        """

    @abc.abstractmethod
    def make_read_only(self) -> None:
        """
        Convert this object to a read only row.

        :return:
        """

    @abc.abstractmethod
    def refresh_db_properties(self) -> None:
        """
        Read the properties for the row off the database.

        :return:
        """

    @property
    @abc.abstractmethod
    def row_dict(self) -> Optional[dict[str, str]]:
        """
        Return the row dict stored in this row.

        :return:
        """
        raise NotImplementedError("You need to define this property.")

    @row_dict.setter
    @abc.abstractmethod
    def row_dict(self, val: Optional[dict[str, str]]) -> None:
        """
        Set the row dict stored in this row.

        :param val:
        :return:
        """
        raise NotImplementedError("You need to define this property.")

    # Todo: Validation for the convert for the individual row entry
    @abc.abstractmethod
    def __setitem__(self, key: str, value: Union[str, int, float, datetime.datetime]) -> None:
        """
        Allows a dictionary like interface to the row.

        :param key:
        :param value:
        :return:
        """

    @abc.abstractmethod
    def __getitem__(self, item: str) -> Union[str, int, float, datetime.datetime]:
        """
        Allows a dictionary like interface to the row.

        :param item:
        :return:
        """

    @abc.abstractmethod
    def update_and_check(self) -> None:
        """
        Updates the metadata stored about the row in the class.

        :return:
        """

    @abc.abstractmethod
    def load_row_from_id(self, row_id: Optional[int] = None, table: Optional[str] = None) -> None:
        """
        If an id is present, load or reload the row_dict from it.

        :param row_id: The id of the row to load - if None, tries to use the id already present
        :param table: The name of the table to load the row from
        :return:
        """

    @abc.abstractmethod
    def load_blank_row(self, table: Optional[str] = None) -> None:
        """
        Load a blank row off the given database - will block if the table or row_dict fields are already full.

        :param table:
        :return:
        """

    @abc.abstractmethod
    def ensure_row_has_id(self) -> None:
        """
        Makes sure that the row_dict has an id in it.

        :return:
        """

    @abc.abstractmethod
    def no_sync(self) -> None:
        """
        Method to replace sync if we're in read only mode.

        :return:
        """

    # -------------------------------
    # - COMPARISON METHODS START HERE

    @abc.abstractmethod
    def __hash__(self) -> int:
        """
        A hash for the row based on the table, id and database - will fail unless all three of these are filled.

        :return:
        """

    @abc.abstractmethod
    def __eq__(self, other: RowAPI) -> bool:
        """
        Uses the hash function to test equality.

        :param other:
        :return:
        """

    # -------------------------------
    # -----------------------------------------------
    #
    # - DICTIONARY EMULATION MAGIC METHODS START HERE
    @abc.abstractmethod
    def keys(self) -> Iterable[str]:
        """
        Returns the keys from the row_dict dictionary.

        :return:
        """

    @abc.abstractmethod
    def __iter__(self) -> Iterator[str]:
        """
        Allows use of the in statement in content of a for loop.

        Iterates over all the column headings in the row.
        If the row has been loaded from the database then all column headings will be set - including if the row is
        black. If the row is being constructed rom the invididual keys, only the keys that have been set will be
        returned.
        :return:
        """

    @abc.abstractmethod
    def __contains__(self, item: str) -> bool:
        """
        Allows use of the in statement - returns true if the item is in the row_dict - false otherwise.

        :param item:
        :return:
        """

    # -----------------------------------------------
    # ------------------------
    #
    # - COPY MAGIC STARTS HERE

    @abc.abstractmethod
    def __deepcopy__(self, memo: dict[Any, Any]) -> "RowAPI":
        """
        Allows for deep copying.

        :param memo:
        :return:
        """

    # ------------------------
