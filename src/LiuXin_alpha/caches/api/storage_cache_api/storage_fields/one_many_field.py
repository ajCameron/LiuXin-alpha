"""
one-many fields represent items such as work notes.
"""
from __future__ import annotations

import abc
import dataclasses

from typing import TYPE_CHECKING, Union, Generic, TypeVar, Optional, Sequence

from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.base_field import (
    RelationFieldBasicInterfaceAPI,
)

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI
    from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.single_table import (
        StorageStorageCacheSingleTableAPI,
    )
    from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.link_tables.one_many_tables import (
        StorageCacheOneToManyLinkTable,
    )
    from LiuXin_alpha.databases.db_types import (
        MainTableName,
        MainTableColumnName,
        MainTableID,
        InterlinkExtraTypes,
    )

T = TypeVar("T")


@dataclasses.dataclass
class SrcDstIDMixin:
    """
    Identify one concrete src/dst edge.
    """

    src_table: MainTableName
    src_table_id: MainTableID

    dst_table: MainTableName
    dst_table_id: MainTableID


@dataclasses.dataclass
class LinkPropertiesMixin:
    """
    Link-level properties for one-many relationships.
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
    Properties of one concrete link between two tables.
    """


@dataclasses.dataclass
class OneManyInTwoTableFieldUpdate(Generic[T]):
    """
    Update for a one-to-many field stored across a link table and a dst table.

    The mapping is keyed by the src table id and valued with the values to be
    written into the dst table target column.

    ``deleted_ids`` means "detach/clear this field from these src rows", not
    "delete the src rows themselves". Implementations may mutate links and, if
    explicitly supported, create or remove related dst rows.
    """

    src_table: MainTableName
    dst_table: MainTableName

    dst_table_target_column: MainTableColumnName

    added_maps: dict[MainTableID, Sequence[Optional[T]]]
    updated_maps: dict[MainTableID, Sequence[Optional[T]]]
    deleted_ids: set[MainTableID]
    dirtied: set[MainTableID]

    # Are the values in this field unique?
    unique: bool = False


@dataclasses.dataclass
class LinkDstUpdateMixin(Generic[T]):
    """
    We're adding/updating a dst row with optional link properties.
    """

    dst_table: MainTableName
    dst_table_target_column: MainTableColumnName
    dst_col_val: Optional[T]


@dataclasses.dataclass
class LinkDstUpdate(LinkPropertiesMixin, LinkDstUpdateMixin[T]):
    """
    Update for one concrete linked dst row.
    """


class OneToManyFieldAPI(RelationFieldBasicInterfaceAPI[T]):
    """
    One-to-many field over a src table, a dst table, and a one-to-many link table.

    The field is keyed by src ids and exposes values from the dst table.
    """

    # One-to-many fields have one entry in one table and many entries in another table.
    src_table: "StorageStorageCacheSingleTableAPI"
    dst_table: "StorageStorageCacheSingleTableAPI"

    # We key by a src id column and cache values from this dst column.
    src_table_id_col: MainTableColumnName
    dst_table_cache_col: MainTableColumnName

    # Connecting the two tables.
    link_table: "StorageCacheOneToManyLinkTable"

    _db: "DatabaseAPI"

    def __init__(
        self,
        src_table: Union["StorageStorageCacheSingleTableAPI", MainTableName],
        src_table_id_col: MainTableColumnName,
        dst_table: Union["StorageStorageCacheSingleTableAPI", MainTableName],
        dst_table_cache_col: MainTableColumnName,
        db: "DatabaseAPI",
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

        self.link_table = self.get_link_table(self.src_table, self.dst_table)

        self.src_table_id_col = src_table_id_col
        self.dst_table_cache_col = dst_table_cache_col

        self._db = db

    @abc.abstractmethod
    def get_link_table(
        self,
        src_table: Union["StorageStorageCacheSingleTableAPI", MainTableName],
        dst_table: Union["StorageStorageCacheSingleTableAPI", MainTableName],
    ) -> "StorageCacheOneToManyLinkTable":
        """
        Resolve the one-to-many link table connecting the two tables.

        :param src_table:
        :param dst_table:
        :return:
        """

    @abc.abstractmethod
    def update(self, update: OneManyInTwoTableFieldUpdate[T]) -> None:
        """
        Update the field, and the underlying tables/db.

        :param update:
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
        Get all src ids known to this field.

        :return:
        """

    @property
    @abc.abstractmethod
    def values(self) -> list[T]:
        """
        Get all values in the cache.

        :return:
        """

    @property
    @abc.abstractmethod
    def values_set(self) -> set[T]:
        """
        Return all distinct values known to this field.

        :return:
        """

    @property
    @abc.abstractmethod
    def ids_values_map(self) -> dict[MainTableID, Sequence[Optional[T]]]:
        """
        Return the src-id to values map for this field.

        The concrete sequence may be ordered or unordered depending on the
        linked table's capabilities. Callers should not infer semantic ordering
        unless they explicitly know the field supports it.

        :return:
        """

    @property
    @abc.abstractmethod
    def dst_ids_values_map(self) -> dict[MainTableID, Optional[T]]:
        """
        Return the dst-id to value map for this field.

        :return:
        """

    @abc.abstractmethod
    def get_values_from_src_id(
        self,
        src_id: MainTableID,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Optional[T]]:
        """
        Get field values for the given src id.

        :param src_id:
        :param require_ordering:
        :param type_filter:
        :return:
        """

    def get_values_from_id(
        self,
        table_id: MainTableID,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Optional[T]]:
        """
        Compatibility alias for src-keyed callers.

        :param table_id:
        :param require_ordering:
        :param type_filter:
        :return:
        """
        return self.get_values_from_src_id(
            table_id,
            require_ordering=require_ordering,
            type_filter=type_filter,
        )

    @abc.abstractmethod
    def get_value_from_dst_id(self, dst_id: MainTableID) -> Optional[T]:
        """
        Get the field value from the dst id.

        :param dst_id:
        :return:
        """

    @abc.abstractmethod
    def get_dst_ids_from_src_id(
        self,
        src_id: MainTableID,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[MainTableID]:
        """
        Resolve linked dst ids for the given src id.

        :param src_id:
        :param require_ordering:
        :param type_filter:
        :return:
        """

    @abc.abstractmethod
    def get_src_id_from_dst_id(
        self,
        dst_id: MainTableID,
        type_filter: Optional[str] = None,
    ) -> Optional[MainTableID]:
        """
        Resolve the linked src id for the given dst id.

        :param dst_id:
        :param type_filter:
        :return:
        """

    @abc.abstractmethod
    def get_src_ids_from_value(self, value: T) -> list[MainTableID]:
        """
        Get src ids whose linked dst rows expose the given value.

        Uniqueness is not guaranteed.

        :param value:
        :return:
        """

    @abc.abstractmethod
    def get_dst_ids_from_value(self, value: T) -> list[MainTableID]:
        """
        Get dst ids matching the given value.

        Uniqueness is not guaranteed.

        :param value:
        :return:
        """

    @abc.abstractmethod
    def get_link_properties(
        self,
        src_id: MainTableID,
        dst_id: MainTableID,
    ) -> IndividualLinkProperties:
        """
        Return link properties for the given src/dst pair.

        :param src_id:
        :param dst_id:
        :return:
        """

    @abc.abstractmethod
    def set_link_properties(
        self,
        updated_link_properties: IndividualLinkProperties,
    ) -> None:
        """
        Write link properties out to the cache.

        :param updated_link_properties:
        :return:
        """

    @abc.abstractmethod
    def get_extra(
        self,
        src_id: MainTableID,
        dst_id: MainTableID,
        extra_type: InterlinkExtraTypes,
    ) -> Optional[str | bool | int]:
        """
        Get one extra value from the link row.

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
        new_extra_value: Optional[str | bool | int],
    ) -> None:
        """
        Write one extra value to the cache.

        :param src_id:
        :param dst_id:
        :param extra_type:
        :param new_extra_value:
        :return:
        """
