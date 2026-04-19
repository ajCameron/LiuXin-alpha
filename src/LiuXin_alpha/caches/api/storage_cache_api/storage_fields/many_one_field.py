"""
many-one fields represent items such as a manifestation's publisher.
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
    from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.link_tables.many_one_tables import (
        StorageCacheManyToOneLinkTable,
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
    Link-level properties for many-one relationships.
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
class ManyOneInTwoTableFieldUpdate(Generic[T]):
    """
    Update for a many-to-one field stored across a link table and a dst table.

    The mapping is keyed by the src table id and valued with the value to be
    written into the dst table target column.

    ``deleted_ids`` means "detach/clear this field from these src rows", not
    "delete the src rows themselves". Implementations may mutate links and, if
    explicitly supported, create or remove related dst rows.
    """

    src_table: MainTableName
    dst_table: MainTableName

    dst_table_target_column: MainTableColumnName

    added_maps: dict[MainTableID, Optional[T]]
    updated_maps: dict[MainTableID, Optional[T]]
    deleted_ids: set[MainTableID]
    dirtied: set[MainTableID]

    # Are the values in this field unique?
    unique: bool = False

    # If True, missing src->dst links may be created when a src row currently
    # has no linked dst row for this field.
    create_missing_links: bool = False

    # If True, and no existing dst row can be matched for a missing link, a new
    # dst row may be created and then linked. This requires
    # ``create_missing_links=True``.
    create_missing_related_rows: bool = False


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


class ManyToOneFieldAPI(RelationFieldBasicInterfaceAPI[T]):
    """
    Many-to-one field over a src table, a dst table, and a many-to-one link table.

    The field is keyed by src ids and exposes values from the dst table.
    Each src id resolves to at most one dst row/value, while one dst row may be
    shared by many src rows.
    """

    # Many-to-one fields have many entries in one table and one entry in another.
    src_table: "StorageStorageCacheSingleTableAPI"
    dst_table: "StorageStorageCacheSingleTableAPI"

    # We key by a src id column and cache values from this dst column.
    src_table_id_col: MainTableColumnName
    dst_table_cache_col: MainTableColumnName

    # Connecting the two tables.
    link_table: "StorageCacheManyToOneLinkTable"

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
    ) -> "StorageCacheManyToOneLinkTable":
        """
        Resolve the many-to-one link table connecting the two tables.

        :param src_table:
        :param dst_table:
        :return:
        """

    @abc.abstractmethod
    def update(self, update: ManyOneInTwoTableFieldUpdate[T]) -> None:
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
    def ids_values_map(self) -> dict[MainTableID, Optional[T]]:
        """
        Return the src-id to value map for this field.

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
    def get_value_from_src_id(self, src_id: MainTableID) -> Optional[T]:
        """
        Get the field value from the src id.

        :param src_id:
        :return:
        """

    def get_value_from_id(self, table_id: MainTableID) -> Optional[T]:
        """
        Compatibility alias for src-keyed callers.

        :param table_id:
        :return:
        """
        return self.get_value_from_src_id(table_id)

    @abc.abstractmethod
    def get_value_from_dst_id(self, dst_id: MainTableID) -> Optional[T]:
        """
        Get the field value from the dst id.

        :param dst_id:
        :return:
        """

    @abc.abstractmethod
    def get_dst_id_from_src_id(
        self,
        src_id: MainTableID,
        type_filter: Optional[str] = None,
    ) -> Optional[MainTableID]:
        """
        Resolve the linked dst id for the given src id.

        :param src_id:
        :param type_filter:
        :return:
        """

    @abc.abstractmethod
    def get_src_ids_from_dst_id(
        self,
        dst_id: MainTableID,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[MainTableID]:
        """
        Resolve linked src ids for the given dst id.

        :param dst_id:
        :param require_ordering:
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
