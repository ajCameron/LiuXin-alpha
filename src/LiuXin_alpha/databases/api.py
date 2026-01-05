
"""
API for the database and related classes.
"""

import abc
import datetime

from typing import Any, Callable, Dict, List, Optional, Tuple, Union, Iterator, Iterable


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
    def __init__(self, database: DatabaseAPI, row_dict: Optional[dict[str, str]] = None, read_only: bool = False) -> None:
        """
        Represents a single row from the LiuXin database.

        :param database: A LiuXin database object
        :param row_dict: Keyed with the column names and valued with their values.
        :param read_only: If True then the row is loaded in read only mode
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
    def row_dict(self) -> Optional[dict[str, str]]:
        """
        Return the row dict stored in this row.

        :return:
        """
        raise NotImplementedError("You need to define this property.")

    @row_dict.setter
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
    def load_row_from_id(self, row_id: int = None, table: str = None) -> None:
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
    def sync(self) -> None:
        """
        Sync the current contents of the row to the database.

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
    def keys(self) -> None:
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


class DatabaseDriverAPI(abc.ABC):
    """
    Every database drive must descend from this class.
    """



class DatabaseAPI(abc.ABC):
    """
    API for the Database itself.
    """



class DatabaseCacheAPI(abc.ABC):
    """
    Every local cache containing data from the database must descend from this class.
    """



class DatabaseMaintainerAPI(abc.ABC):
    """
    Maintenance bot which runs on the database.
    """
    def __init__(self, db: DatabaseAPI) -> None:
        """
        Attach the database to the maintainer which will work on it.

        :param db:
        """
        # Weakref to make sure the class doesn't block shutdown of the database
        self.db = db

    @abc.abstractmethod
    def dirty_record(self, table: str, row_id: int) -> None:
        """
        Notify the maintenance bot that a change has occurred to the table (put it in the maintain queue).

        :param table:
        :param row_id:
        :return:
        """

    @abc.abstractmethod
    def new_dirty_record(self, table: str, row_id: int) -> None:
        """
        Replacement for the dirty record method for testing.

        :param table:
        :param row_id:
        :return:
        """

    @abc.abstractmethod
    def dirty_interlink_record(
        self, update_type: str, table1: str, table2: str, table1_id: int, table2_id: int
    ) -> None:
        """
        Notify the maintenance bot that an interlink record has been changed.

        Used for updating the books_aggregate table when stuff happens to the relevant other tables.
        :param update_type:
        :param table1:
        :param table2:
        :param table1_id:
        :param table2_id:
        :return:
        """

    @abc.abstractmethod
    def clean(self, table: str, item_ids: Iterable[int]) -> None:
        """
        Clean the relevant table of the relevant item_ids

        :param table:
        :param item_ids:
        :return:
        """

    @abc.abstractmethod
    def merge(self, table: str, item_1_id: int, item_2_id: int) -> None:
        """
        Consider merging two items on the database.

        :param table:
        :param item_1_id:
        :param item_2_id: All the item 2 ids will be repointed to item_1_id - then it'll be deleted
        :return:
        """


class MaintenanceBotAPI(abc.ABC):
    """
    API for the maintenance bot thread itself.
    """
    @abc.abstractmethod
    def stop(self) -> None:
        """
        Preform thread shutdown.

        :return:
        """

    @abc.abstractmethod
    def rename_item(
            self,
            item_id: int,
            table: str,
            value: bool,
            now: bool = True,
            db: Optional[DatabaseAPI] = None) -> None:
        """
        Register a rename action has occurred on an item.

        :param item_id:
        :param table:
        :param value: The item value will be renamed to this
        :param now:
        :param db:
        :return:
        """