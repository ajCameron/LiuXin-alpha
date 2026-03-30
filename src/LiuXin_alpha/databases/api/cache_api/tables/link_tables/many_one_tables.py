
"""
API for many-to-one tables.
"""

import abc
from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:
    from LiuXin_alpha.databases.db_types import SrcTableID, DstTableID
    from LiuXin_alpha.databases.api.cache_api.tables.table_updates import (
        ManyOneInterlinkTableUpdate,
        ManyOneInterLinkTableUpdateResults)

from LiuXin_alpha.databases.api.cache_api.tables.link_tables.link_table_base import CacheLinkTableBaseAPI, TableTypes, T

from LiuXin_alpha.errors import WrongTypeOfCacheTable


ManyOneLinkUpdateType = dict[Optional[set[SrcTableID]], DstTableID]


class CacheManyToOneLinkTableAPI(CacheLinkTableBaseAPI):
    """
    Represents data which is uniquely linked to one other thing.

    E.g. notes - they should be linked to one, and only one, other thing.
    """
    table_type: TableTypes = TableTypes.MANY_ONE

    _typed: bool = False
    _priority: bool = False

    @abc.abstractmethod
    def update(
        self,
        update: ManyOneInterlinkTableUpdate
    ) -> ManyOneInterLinkTableUpdateResults:
        """
        Preform an update of the database and cache.

        This goes in the following order.

        - update_preflight - brings the update object into standard form
        - update_precheck - checks the update is actually valid
        (these two should be done with a lock)
        - update_cache - updates this object
        - update_db - write the update out to the db

        :param update: Use this update object to update the db and cache

        :return:
        """

    @abc.abstractmethod
    def update_preflight(
        self,
        update: ManyOneInterlinkTableUpdate
    ) -> ManyOneInterlinkTableUpdate:
        """
        Bring the update into a form where it can be more easily written out to the database.

        :param update: Use this update object as a base for the final update

        :return:
        """

    @abc.abstractmethod
    def update_precheck(
        self,
        update: ManyOneInterlinkTableUpdate
    ) -> bool:
        """
        Check that an update is of a valid form before writing it out to the cache and the database.

        :param update:

        :return:
        """

    @abc.abstractmethod
    def update_db(
        self,
        update: ManyOneInterlinkTableUpdate
    ) -> bool:
        """
        Preform an update on the database itself.

        :param update: Should contain all infomation required for the update

        :return:
        """

    @abc.abstractmethod
    def update_cache(
        self,
        update: ManyOneInterlinkTableUpdate
    ) -> bool:
        """
        Preform an update on the database itself.

        :param update:

        :return bool:
        """

    def get_secondary_value(self) -> Optional[T]:
        """
        This table has no concept of type or priority - so this is the only one you can call.

        :return:
        """

    def get_secondary_values_priority(self) -> list[T]:
        """
        Get the secondary values in priority order.

        :return:
        """
        raise WrongTypeOfCacheTable(
            "This is a basic one-to-many table - it has no concept of priority."
        )

    def get_secondary_values_typed(self) -> dict[DstTableID, dict[str, set[T]]]:
        """
        Return the typed values from the secondary table.

        :return:
        """
        raise WrongTypeOfCacheTable(
            "This is a basic one-to-many table - it has no concept of types."
        )

    def get_secondary_values_typed_priority(self) -> dict[DstTableID, dict[str, set[T]]]:
        """
        Return the typed, priority values from the secondary table.

        :return:
        """
        raise WrongTypeOfCacheTable(
            """
            This is a basic one-to-many table - it has no concept of priority or types.
            """
        )


ManyOneTypedLinkUpdateType = dict[Optional[dict[str, set[DstTableID]]], DstTableID]


class CacheManyToOneTypedLinkTableAPI(CacheLinkTableBaseAPI):
    """
    Represents data that can have many values, stored in another table, with a type.

    E.g. comments can have types
    """
    table_type: TableTypes = TableTypes.MANY_ONE

    _typed: bool = True
    _priority: bool = False

    @abc.abstractmethod
    def get_secondary_values(self) -> set[T]:
        """
        This table has no concept of type or priority - so this is the only one you can call.

        :return:
        """

    def get_secondary_values_priority(self) -> list[T]:
        """
        Get the secondary values in priority order.

        :return:
        """
        raise WrongTypeOfCacheTable(
            "This is a basic one-to-many table - it has no concept of priority."
        )

    @abc.abstractmethod
    def get_secondary_values_typed(self) -> dict[DstTableID, dict[str, set[T]]]:
        """
        Return the typed values from the secondary table.

        :return:
        """

    def get_secondary_values_typed_priority(self) -> dict[DstTableID, dict[str, set[T]]]:
        """
        Return the typed, priority values from the secondary table.

        :return:
        """
        raise WrongTypeOfCacheTable(
            """
            This is a basic one-to-many table - it has no concept of priority or types.
            """
        )


ManyOnePriorityLinkUpdateType = dict[tuple[DstTableID], DstTableID]


class CacheManyToOnePriorityLinkTableAPI(CacheLinkTableBaseAPI):
    """
    Represents data that can have many values, stored in another table, with a type.

    E.g. comments can have types
    """
    table_type: TableTypes = TableTypes.MANY_ONE

    _typed: bool = False
    _priority: bool = True

    @abc.abstractmethod
    def get_secondary_values(self) -> set[T]:
        """
        This table has no concept of type or priority - so this is the only one you can call.

        :return:
        """

    @abc.abstractmethod
    def get_secondary_values_priority(self) -> list[T]:
        """
        Get the secondary values in priority order.

        :return:
        """

    def get_secondary_values_typed(self) -> dict[DstTableID, dict[str, set[T]]]:
        """
        Return the typed values from the secondary table.

        :return:
        """
        raise WrongTypeOfCacheTable(
            "This is a basic one-to-many table - it has no concept of priority."
        )

    def get_secondary_values_typed_priority(self) -> dict[DstTableID, dict[str, set[T]]]:
        """
        Return the typed, priority values from the secondary table.

        :return:
        """
        raise WrongTypeOfCacheTable(
            """
            This is a basic one-to-many table - it has no concept of priority or types.
            """
        )


ManyOnePriorityTypedLinkUpdateType = dict[Optional[dict[str, list[DstTableID]]], DstTableID]


class CacheManyToOnePriorityTypedLinkTableAPI(CacheLinkTableBaseAPI):
    """
    Represents data that can have many values, stored in another table, with a type.

    E.g. comments can have types
    """
    table_type: TableTypes = TableTypes.MANY_ONE

    _typed: bool = True
    _priority: bool = True
    @abc.abstractmethod
    def get_secondary_values(self) -> set[T]:
        """
        This table has no concept of type or priority - so this is the only one you can call.

        :return:
        """

    @abc.abstractmethod
    def get_secondary_values_priority(self) -> list[T]:
        """
        Get the secondary values in priority order.

        :return:
        """

    @abc.abstractmethod
    def get_secondary_values_typed(self) -> dict[DstTableID, dict[str, set[T]]]:
        """
        Return the typed values from the secondary table.

        :return:
        """

    @abc.abstractmethod
    def get_secondary_values_typed_priority(self) -> dict[DstTableID, dict[str, set[T]]]:
        """
        Return the typed, priority values from the secondary table.

        :return:
        """
