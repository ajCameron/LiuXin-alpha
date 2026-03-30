
"""
API for one-to-many tables.
"""
import abc
import dataclasses
from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:
    from LiuXin_alpha.databases.db_types import (
        SrcTableID,
        DstTableID,
        MainTableName,
        InterLinkTableName,
        InterlinkTableID)
    from LiuXin_alpha.databases.api.cache_api.tables.table_updates import (
        OneManyInterlinkTableUpdate,
        OneManyInterLinkTableUpdateResults)

from LiuXin_alpha.databases.api.cache_api.tables.link_tables.link_table_base import (
    CacheLinkTableBaseAPI,
    TableTypes,
    T)

from LiuXin_alpha.errors import WrongTypeOfCacheTable


@dataclasses.dataclass(slots=True)
class OneManyLink:
    """
    Represents an element in a one-many link.
    """
    # Todo: Make this literals for all the tables
    src_table: MainTableName
    dst_table: MainTableName

    link_table: InterLinkTableName
    link_table_id: InterlinkTableID

    src_table_id: Optional[SrcTableID]
    dst_table_id: Optional[DstTableID]

    # Properties of the link
    has_priority: bool = False
    priority: Optional[int] = None

    has_primary: bool = False
    primary: Optional[bool] = False

    has_type: bool = False
    type: Optional[str] = None

    has_origin: bool = False
    origin: Optional[str] = None

    has_policy: bool = False
    policy: Optional[str] = None

    has_data: bool = False
    data: Optional[str] = None

    has_index: bool = False
    index: Optional[str] = None





class CacheOneToManyLinkTableBaseAPI(CacheLinkTableBaseAPI):
    """
    Base class for the One-to-Many cache tables.
    """

    @abc.abstractmethod
    def update(
        self,
        update: OneManyInterlinkTableUpdate
    ) -> OneManyInterLinkTableUpdateResults:
        """
        Preform an update of the database and cache.

        This goes in the following order.

        - update_preflight - brings the update object into standard form
        - update_precheck - checks the update is actually valid
        (these two should be done with a lock)
        - update_cache - updates this object
        - update_db - write the update out to the db

        :param update:
        :return:
        """

    @abc.abstractmethod
    def update_preflight(
        self,
        update: OneManyInterlinkTableUpdate
    ) -> OneManyInterlinkTableUpdate:
        """
        Bring the update into a form where it can be more easily written out to the database.

        :param update:
        :return:
        """

    @abc.abstractmethod
    def update_precheck(
        self,
        update: OneManyInterlinkTableUpdate
    ) -> bool:
        """
        Check that an update is of a valid form before writing it out to the cache and the database.

        :param update: OneManyInterlinkTableUpdate - containing
        :return:
        """

    @abc.abstractmethod
    def update_db(
        self,
        update: OneManyInterlinkTableUpdate
    ) -> bool:
        """
        Preform an update on the database itself.

        :param update:
        :return:
        """

    @abc.abstractmethod
    def update_cache(
        self,
        update: OneManyInterlinkTableUpdate
    ) -> bool:
        """
        Preform an update on the database itself.

        :param update:
        :return:
        """

    # -----------------------
    # - PRIMARY VALUES GETTER

    @abc.abstractmethod
    def get_primary_id(self, secondary_id: DstTableID) -> Optional[SrcTableID]:
        """
        Get the primary id for the secondary table entry - if it exists.

        :param secondary_id:
        :return:
        """

    @abc.abstractmethod
    def get_primary_id_type(self, secondary_id: DstTableID, link_type: str) -> Optional[SrcTableID]:
        """
        Get the primary id from the secondary table id iff the link type matches.

        :param secondary_id:
        :param link_type:
        :return:
        """

    @abc.abstractmethod
    def get_primary_id_from_value(self, secondary_value: T) -> Optional[SrcTableID]:
        """
        Get the primary ids from the secondary value - if any.

        :param secondary_value:
        :return:
        """

    # -----------------------
    # ------------------------
    # - SECONDARY VALUE GETTER

    @abc.abstractmethod
    def get_secondary_values(self, primary_id: SrcTableID) -> set[T]:
        """
        Return the values from the secondary table.

        :return:
        """

    @abc.abstractmethod
    def get_secondary_values_priority(self, primary_id: SrcTableID) -> list[T]:
        """
        Get the secondary values in priority order.

        :return:
        """

    @abc.abstractmethod
    def get_secondary_values_typed(self, primary_id: SrcTableID) -> dict[DstTableID, dict[str, set[T]]]:
        """
        Return the typed values from the secondary table.

        :return:
        """

    @abc.abstractmethod
    def get_secondary_values_typed_priority(self, primary_id: SrcTableID) -> dict[DstTableID, dict[str, set[T]]]:
        """
        Return the typed, priority values from the secondary table.

        :return:
        """

    # ------------------------


OneManyLinkUpdateType = dict[SrcTableID, Optional[set[DstTableID]]]

class CacheOneToManyLinkTableAPI(CacheOneToManyLinkTableBaseAPI):
    """
    Represents data which is uniquely linked to one other thing.

    E.g. notes - they should be linked to one, and only one, other thing.
    """
    table_type: TableTypes = TableTypes.ONE_MANY

    _typed: bool = False
    _priority: bool = False

    @abc.abstractmethod
    def get_secondary_values(self, primary_id: SrcTableID) -> set[T]:
        """
        This table has no concept of type or priority - so this is the only one you can call.

        :return:
        """

    def get_secondary_values_priority(self, primary_id: SrcTableID) -> list[T]:
        """
        Get the secondary values in priority order.

        :return:
        """
        raise WrongTypeOfCacheTable(
            "This is a basic one-to-many table - it has no concept of priority."
        )

    def get_secondary_values_typed(self, primary_id: SrcTableID) -> dict[DstTableID, dict[str, set[T]]]:
        """
        Return the typed values from the secondary table.

        :return:
        """
        raise WrongTypeOfCacheTable(
            "This is a basic one-to-many table - it has no concept of types."
        )

    def get_secondary_values_typed_priority(self, primary_id: SrcTableID) -> dict[DstTableID, dict[str, set[T]]]:
        """
        Return the typed, priority values from the secondary table.

        :return:
        """
        raise WrongTypeOfCacheTable(
            """
            This is a basic one-to-many table - it has no concept of priority or types.
            """
        )


OneManyTypedLinkUpdateType = dict[SrcTableID, Optional[dict[str, set[DstTableID]]]]


class CacheOneToManyTypedLinkTableAPI(CacheOneToManyLinkTableBaseAPI):
    """
    Represents data that can have many values, stored in another table, with a type.

    E.g. comments can have types
    """
    table_type: TableTypes = TableTypes.ONE_MANY

    _typed: bool = True
    _priority: bool = False

    @abc.abstractmethod
    def get_secondary_values(self, primary_id: SrcTableID) -> set[T]:
        """
        This table has no concept of type or priority - so this is the only one you can call.

        :return:
        """

    def get_secondary_values_priority(self, primary_id: SrcTableID) -> list[T]:
        """
        Get the secondary values in priority order.

        :return:
        """
        raise WrongTypeOfCacheTable(
            "This is a basic one-to-many table - it has no concept of priority."
        )

    @abc.abstractmethod
    def get_secondary_values_typed(self, primary_id: SrcTableID) -> dict[DstTableID, dict[str, set[T]]]:
        """
        Return the typed values from the secondary table.

        :return:
        """

    def get_secondary_values_typed_priority(self, primary_id: SrcTableID) -> dict[DstTableID, dict[str, set[T]]]:
        """
        Return the typed, priority values from the secondary table.

        :return:
        """
        raise WrongTypeOfCacheTable(
            """
            This is a basic one-to-many table - it has no concept of priority or types.
            """
        )



OneManyPriorityLinkUpdateType = dict[SrcTableID, Optional[list[DstTableID]]]


class CacheOneToManyPriorityLinkTableAPI(CacheOneToManyLinkTableBaseAPI):
    """
    Represents data that can have many values, stored in another table, with a priority.

    E.g. notes can have a priority.
    """
    table_type: TableTypes = TableTypes.ONE_MANY

    _typed: bool = False
    _priority: bool = True

    @abc.abstractmethod
    def get_secondary_values(self, primary_id: SrcTableID) -> set[T]:
        """
        This table has a concept of priority - but you can ignore it.

        :return:
        """

    @abc.abstractmethod
    def get_secondary_values_priority(self, primary_id: SrcTableID) -> list[T]:
        """
        Get the secondary values in priority order.

        :return:
        """

    def get_secondary_values_typed(
            self,
            primary_id: SrcTableID) -> dict[DstTableID, dict[str, set[T]]]:
        """
        Return the typed values from the secondary table.

        :return:
        """
        raise WrongTypeOfCacheTable(
            "This is a basic one-to-many table - it has no concept of type."
        )

    def get_secondary_values_typed_priority(
            self,
            primary_id: SrcTableID) -> dict[DstTableID, dict[str, set[T]]]:
        """
        Return the typed, priority values from the secondary table.

        :return:
        """
        raise WrongTypeOfCacheTable(
            """
            This is a basic one-to-many table - it has no concept of priority or types.
            """
        )



OneManyPriorityTypedLinkUpdateType = dict[SrcTableID, Optional[dict[str, list[DstTableID]]]]


class CacheOneToManyPriorityTypedLinkTableAPI(CacheOneToManyLinkTableBaseAPI):
    """
    Represents data that can have many values, stored in another table, with a priority.

    E.g. notes can have a priority.
    """
    table_type: TableTypes = TableTypes.ONE_MANY

    _typed: bool = True
    _priority: bool = True

    @abc.abstractmethod
    def get_secondary_values(self, primary_id: SrcTableID) -> set[T]:
        """
        This table has a concept of priority - but you can ignore it.

        :return:
        """

    @abc.abstractmethod
    def get_secondary_values_priority(self, primary_id: SrcTableID) -> list[T]:
        """
        Get the secondary values in priority order.

        :return:
        """

    @abc.abstractmethod
    def get_secondary_values_typed(
            self, primary_id: SrcTableID) -> dict[DstTableID, dict[str, set[T]]]:
        """
        Return the typed values from the secondary table.

        :return:
        """

    @abc.abstractmethod
    def get_secondary_values_typed_priority(
            self, primary_id: SrcTableID) -> dict[DstTableID, dict[str, set[T]]]:
        """
        Return the typed, priority values from the secondary table.

        :return:
        """
