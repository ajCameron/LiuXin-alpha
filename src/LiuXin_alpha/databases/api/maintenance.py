"""Maintenance/cache related API contracts."""

from __future__ import annotations

import abc

from typing import Iterable, Optional

from .database import DatabaseAPI

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
