
"""
API for many-to-one tables.
"""

import abc
from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:
    from LiuXin_alpha.databases.db_types import SrcTableID, DstTableID

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
            primary_id_secondary_id_map: ManyOneLinkUpdateType,
            secondary_id_map_update: dict[DstTableID, Optional[T]],
            dirtied: Optional[set[SrcTableID]] = None,
    ) -> None:
        """
        Preform an update of the database and cache.

        This goes in the following order.

        - update_preflight - brings the update object into standard form
        - update_precheck - checks the update is actually valid
        (these two should be done with a lock)
        - update_cache - updates this object
        - update_db - write the update out to the db

        :param primary_id_secondary_id_map:
        :param secondary_id_map_update:
        :param dirtied:
        :return:
        """

    @abc.abstractmethod
    def update_preflight(
        self,
        primary_id_secondary_id_map: ManyOneLinkUpdateType,
        secondary_id_map_update: dict[DstTableID, Optional[T]],
        dirtied: Optional[set[SrcTableID]] = None,
    ) -> tuple[dict[SrcTableID, set[DstTableID]], set[SrcTableID]]:
        """
        Bring the update into a form where it can be more easily written out to the database.

        :param primary_id_secondary_id_map:
        :param secondary_id_map_update:
        :param dirtied:

        :return:
        """

    @abc.abstractmethod
    def update_precheck(
        self,
        primary_id_secondary_id_map: ManyOneLinkUpdateType,
        secondary_id_map_update: Optional[DstTableID, Optional[T]] = None
    ) -> bool:
        """
        Check that an update is of a valid form before writing it out to the cache and the database.

        :param primary_id_secondary_id_map:
        :param secondary_id_map_update:

        :return:
        """

    @abc.abstractmethod
    def update_db(
        self,
        primary_id_secondary_id_map: ManyOneLinkUpdateType,
        secondary_id_map_update: Optional[DstTableID, Optional[T]] = None
    ) -> bool:
        """
        Preform an update on the database itself.

        We expect the update in the form of a dict
         - keyed with the value in one table
         - valued with the
        :param primary_id_secondary_id_map:
        :param secondary_id_map_update:
        :return:
        """

    @abc.abstractmethod
    def update_cache(
        self,
        primary_id_secondary_id_map: ManyOneLinkUpdateType,
        secondary_id_map_update: Optional[DstTableID, Optional[T]] = None
    ) -> bool:
        """
        Preform an update on the database itself.

        :param primary_id_secondary_id_map:
        :param secondary_id_map_update:
        :return:
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
    table_type: TableTypes = TableTypes.ONE_MANY

    _typed: bool = True
    _priority: bool = False

    @abc.abstractmethod
    def update(
        self,
        primary_id_secondary_id_map: ManyOneTypedLinkUpdateType,
        secondary_id_map_update: dict[DstTableID, Optional[T]],
        dirtied: Optional[set[SrcTableID]] = None,
    ) -> None:
        """
        Preform an update of the database and cache.

        Update maps are expected in the form of a dict
         - keyed with the id in the primary table
         - valued with a set of ids in the secondary table

        # Todo: Is this a good idea?
        Optionally, you can update the secondary table at the same time?

        This goes in the following order.
        - update_preflight - brings the update object into standard form
        - update_precheck - checks the update is actually valid
        (these two should be done with a lock)
        - update_cache - updates this object
        - update_db - write the update out to the db

        :param primary_id_secondary_id_map:
        :param secondary_id_map_update:
        :param dirtied:
        :return:
        """

    @abc.abstractmethod
    def update_preflight(
        self,
        primary_id_secondary_id_map: OneManyTypedLinkUpdateType,
        secondary_id_map_update: dict[DstTableID, Optional[T]],
        dirtied: Optional[set[SrcTableID]] = None,
    ) -> tuple[dict[SrcTableID, set[DstTableID]], set[SrcTableID]]:
        """
        Bring the update into a form where it can be more easily written out to the database.

        :param primary_id_secondary_id_map:
        :param secondary_id_map_update:
        :param dirtied:

        :return:
        """

    @abc.abstractmethod
    def update_precheck(
        self,
        primary_id_secondary_id_map: OneManyTypedLinkUpdateType,
        secondary_id_map_update: dict[DstTableID, Optional[T]]
    ) -> bool:
        """
        Check that an update is of a valid form before writing it out to the cache and the database.

        :param primary_id_secondary_id_map:
        :param secondary_id_map_update:

        :return:
        """

    @abc.abstractmethod
    def update_db(
        self,
        primary_id_secondary_id_map: OneManyTypedLinkUpdateType,
        secondary_id_map_update: dict[DstTableID, Optional[T]]
    ) -> bool:
        """
        Preform an update on the database itself.

        :param primary_id_secondary_id_map:
        :param secondary_id_map_update:
        :return:
        """

    @abc.abstractmethod
    def update_cache(
        self,
        primary_id_secondary_id_map: OneManyTypedLinkUpdateType,
        secondary_id_map_update: dict[DstTableID, Optional[T]]
    ) -> bool:
        """
        Preform an update on the database itself.

        :param primary_id_secondary_id_map:
        :param secondary_id_map_update:
        :return:
        """

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
