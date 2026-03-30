
"""
one-many fields represent items such as work notes.
"""

from __future__ import annotations

import abc
import dataclasses
from typing import TYPE_CHECKING, Union, Generic, TypeVar, Optional

from collections import OrderedDict

from LiuXin_alpha.databases.api.cache_api.fields.base_field import FieldBasicInterfaceAPI

from LiuXin_alpha.errors import WrongTypeOfCacheTable

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI
    from LiuXin_alpha.databases.api.cache_api.tables.single_table import CacheSingleTableAPI
    from LiuXin_alpha.databases.api.cache_api.tables.link_tables.one_many_tables import CacheOneToManyLinkTableBaseAPI
    from LiuXin_alpha.databases.api.cache_api.tables.link_tables.one_many_tables import CacheOneToManyLinkTableAPI
    from LiuXin_alpha.databases.db_types import MainTableName, InterLinkTableName, MainTableColumnName, MainTableID, InterlinkExtraTypes
    from LiuXin_alpha.databases.api.cache_api.tables.link_tables.one_many_tables import OneManyInterlinkTableUpdate

T = TypeVar('T')


@dataclasses.dataclass
class SrcDstIDMixin:
    """
    Getting around a database default limitation.
    """
    src_table: MainTableName
    src_table_id: MainTableID

    dst_table: MainTableName
    dst_table_id: MainTableID


@dataclasses.dataclass
class LinkPropertiesMixin:
    """
    Contains all the link properties, as a number of classes need them to be knowable.
    """

    priority: Optional[int] = None
    primary: Optional[bool] = None
    type: Optional[str] = None
    origin: Optional[str] = None
    policy: Optional[str] = None
    data: Optional[str] = None
    index: Optional[int] = None


@dataclasses.dataclass
class IndividualLinkProperties(LinkPropertiesMixin, SrcDstIDMixin):
    """
    Properties of a link between two tables.

    Property is for a single connection.
    """



@dataclasses.dataclass
class OneManyInTwoTableaFieldUpdate(Generic[T]):
    """
    Update for a one to one field.

    Update for the basic field - with no prioirty/type info.

    This will be used to generate the update objects for
     - each of the two main tables
     - the link table
    """
    src_table: MainTableName
    dst_table: MainTableName

    dst_table_target_column: MainTableColumnName

    # We need to carry info for the types of
    added_maps: dict["MainTableID", Optional[T]]

    updated_maps: dict["MainTableID", Optional[T]]

    deleted_ids: set[MainTableID]

    dirtied: set[MainTableID]

    # Are the values in this field unique?
    unique: bool = False


@dataclasses.dataclass
class LinkDstUpdateMixin(Generic[T]):
    """
    We're adding/updating a dst with optional link properties.
    """

    dst_table: MainTableName
    dst_table_target_column: MainTableColumnName

    dst_col_val: Optional[T]


@dataclasses.dataclass
class LinkDstUpdate(LinkPropertiesMixin, LinkDstUpdateMixin):
    """
    Update for a link between two tables.
    """
    ...


class OneToManyFieldAPI(FieldBasicInterfaceAPI):
    """
    One to many field.
    """
    # One to many fields have one entry in one table and many entries in another table
    src_table: "CacheSingleTableAPI"
    dst_table: "CacheSingleTableAPI"

    # We're caching the results of this column in the other table
    src_table_id_col: MainTableColumnName
    dst_table_cache_col: MainTableColumnName

    # connecting the two tables
    link_table: "CacheOneToManyLinkTableAPI"

    _db: "DatabaseAPI"

    def __init__(
        self,
        src_table: Union["CacheSingleTableAPI", MainTableName],
        src_table_id_col: MainTableColumnName,
        dst_table: Union["CacheSingleTableAPI", MainTableName],
        dst_table_cache_col: MainTableColumnName,
        db: "DatabaseAPI"
    ) -> None:
        """
        Startup the cache for the given field.

        :param src_table:
        :param src_table_id_col:
        :param dst_table:
        :param dst_table_cache_col:
        :param db:
        """
        self.src_table = self.get_main_table(src_table)
        self.dst_table = self.get_main_table(dst_table)

        self.get_link_table(src_table, dst_table)

        self.src_table_id_col = src_table_id_col
        self.dst_table_cache_col = dst_table_cache_col

        self._db = db

    @abc.abstractmethod
    def get_link_table(
        self,
        src_table: Union["CacheSingleTableAPI", MainTableName],
        dst_table: Union["CacheSingleTableAPI", MainTableName]
    ) -> "CacheOneToManyLinkTableAPI":
        """


        :param src_table:
        :param src_table_id_col:
        :param dst_table:
        :param dst_table_id_col:
        :return:
        """

    @property
    def src_table_name(self) -> MainTableName:
        """
        Get the name of the src table.

        :return:
        """
        return self.src_table.table

    @property
    def dst_table_name(self) -> MainTableName:
        """
        Get the name of the dst table.

        :return:
        """
        return self.dst_table.table

    @property
    @abc.abstractmethod
    def ids(self) -> set[MainTableID]:
        """
        Get all the ids-values known to this table - from the src table.

        :return:
        """

    @property
    @abc.abstractmethod
    def values(self) -> list[T]:
        """
        Get all the values in the cache.

        :return:
        """

    @property
    @abc.abstractmethod
    def values_set(self) -> set[T]:
        """
        Return all the values known to this table.

        :return:
        """

    @abc.abstractmethod
    def dst_ids_values_map(self) -> dict[MainTableID, Optional[T]]:
        """
        Ids to values map in the dst table.

        :return:
        """

    @property
    @abc.abstractmethod
    def ids_values_map(self) -> dict[MainTableID, set[Optional[T]]]:
        """
        Return all the ids-values known to this table.

        By default, they're unordered.
        :return:
        """

    def ids_values_map_typed(self) -> dict[MainTableID, dict[str, set[Optional[T]]]]:
        """
        Return an id-values map - with type info.

        :return:
        """
        raise WrongTypeOfCacheTable(f"{self.link_table = } does not support types.")

    def ids_values_map_priority(self) -> dict[MainTableID, set[Optional[T]]]:
        """
        Return a id-values map - with priority info.

        :return:
        """
        raise WrongTypeOfCacheTable(f"{self.link_table = } does not support priority.")

    def ids_values_map_priority_typed(self) -> dict[MainTableID, dict[str, list[Optional[T]]]]:
        """
        Return a id-values map - with priority info.

        :return:
        """
        raise WrongTypeOfCacheTable(f"{self.link_table = } does not support priority and type info.")

    @abc.abstractmethod
    def get_values_from_id(self, table_id: MainTableID) -> set[Optional[T]]:
        """
        Get the values from the id.

        :param table_id:
        :return:
        """

    @abc.abstractmethod
    def get_ids_from_value(self, value: T) -> set[MainTableID]:
        """
        Get the ids from the value.

        Uniqueness is not guaranteed.
        :param value:
        :return:
        """

    @abc.abstractmethod
    def get_link_properties(self, src_id: MainTableID, dst_id: MainTableID) -> IndividualLinkProperties:
        """
        Return the extra dict for the given link.

        :param src_id:
        :param dst_id:
        :return:
        """

    @abc.abstractmethod
    def set_link_properties(self, updated_link_properties: IndividualLinkProperties) -> None:
        """
        Write link properties out to the cache.

        :param updated_link_properties:
        :return:
        """

    # Todo: Think about having a sync method
    @abc.abstractmethod
    def get_extra(
            self,
            src_id: MainTableID,
            dst_id: MainTableID,
            extra_type: InterlinkExtraTypes
    ) -> Optional[T]:
        """
        Get the specific value for the extra type.

        :param src_id:
        :param dst_id:
        :param extra_type:
        :return:
        """

    @abc.abstractmethod
    def set_extra(
        self,
        src_id: MainTableID,
        dst_id: MainTableID,
        extra_type: InterlinkExtraTypes,
        new_extra_value: Optional[Union[str, bool, int]]
    ):
        """
        Write extra for a link out to the cache.

        :param src_id:
        :param dst_id:
        :param extra_type:
        :param new_extra_value:
        :return:
        """


class OneToManyFieldTypedAPI(OneToManyFieldAPI):
    """
    One-to-many field, with type info API.
    """
    @property
    @abc.abstractmethod
    def ids_values_map(self) -> dict[MainTableID, set[Optional[T]]]:
        """
        Return all the ids-values known to this table.

        By default, they're unordered.
        :return:
        """

    @property
    @abc.abstractmethod
    def ids_values_map_typed(self) -> dict[MainTableID, dict[str, set[Optional[T]]]]:
        """
        Return an id-values map - with type info.

        :return:
        """

    def ids_values_map_priority(self) -> dict[MainTableID, set[Optional[T]]]:
        """
        Return a id-values map - with priority info.

        :return:
        """
        raise WrongTypeOfCacheTable(f"{self.link_table = } does not support priority.")

    def ids_values_map_priority_typed(self) -> dict[MainTableID, dict[str, list[Optional[T]]]]:
        """
        Return a id-values map - with priority info.

        :return:
        """
        raise WrongTypeOfCacheTable(f"{self.link_table = } does not support priority and type info.")


class OneToManyFieldPriorityAPI(OneToManyFieldAPI):
    """
    One-to-many field, with type info API.
    """

    @property
    @abc.abstractmethod
    def ids_values_map(self) -> dict[MainTableID, set[Optional[T]]]:
        """
        Return all the ids-values known to this table.

        By default, they're unordered.
        :return:
        """

    def ids_values_map_typed(self) -> dict[MainTableID, dict[str, set[Optional[T]]]]:
        """
        Return an id-values map - with type info.

        :return:
        """
        raise WrongTypeOfCacheTable(f"{self.link_table = } does not support types.")

    @property
    @abc.abstractmethod
    def ids_values_map_priority(self) -> dict[MainTableID, set[Optional[T]]]:
        """
        Return a id-values map - with priority info.

        :return:
        """

    def ids_values_map_priority_typed(self) -> dict[MainTableID, dict[str, list[Optional[T]]]]:
        """
        Return a id-values map - with priority info.

        :return:
        """
        raise WrongTypeOfCacheTable(f"{self.link_table = } does not support priority and type info.")


class OneToManyFieldPriorityTypedAPI(OneToManyFieldAPI):
    """
    One-to-many field, with type info API.
    """

    @property
    @abc.abstractmethod
    def ids_values_map(self) -> dict[MainTableID, set[Optional[T]]]:
        """
        Return all the ids-values known to this table.

        By default, they're unordered.
        :return:
        """

    @property
    @abc.abstractmethod
    def ids_values_map_typed(self) -> dict[MainTableID, dict[str, set[Optional[T]]]]:
        """
        Return an id-values map - with type info.

        :return:
        """
        raise WrongTypeOfCacheTable(f"{self.link_table = } does not support types.")

    @property
    @abc.abstractmethod
    def ids_values_map_priority(self) -> dict[MainTableID, set[Optional[T]]]:
        """
        Return a id-values map - with priority info.

        :return:
        """

    @property
    @abc.abstractmethod
    def ids_values_map_priority_typed(self) -> dict[MainTableID, dict[str, list[Optional[T]]]]:
        """
        Return a id-values map - with priority info.

        :return:
        """
