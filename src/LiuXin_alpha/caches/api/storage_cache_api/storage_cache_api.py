"""
Top-level API for a storage cache.

The StorageCache is responsible for raw cached values and relationships as read
from the database. Higher-level concerns such as search presentation,
user-facing sort semantics, and views belong in the InterfaceCache layer.
"""
from __future__ import annotations

import abc

from typing import TYPE_CHECKING, Any, Iterable, Mapping, Optional, Sequence, Union

from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.base_field import (
    FieldBasicInterfaceAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.base_table import (
    StorageCacheBaseTableAPI,
    TableTypes,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.link_tables.link_table_base import (
    StorageCacheLinkTableBaseAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.link_tables.many_many_tables import (
    StorageCacheManyToManyLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.link_tables.many_one_tables import (
    StorageCacheManyToOneLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.link_tables.one_many_tables import (
    StorageCacheOneToManyLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.link_tables.one_one_tables import (
    StorageCacheOneToOneLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.single_table import (
    StorageCacheSingleTableAPI,
)

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI
    from LiuXin_alpha.databases.db_types import MainTableName


FieldKey = str
LinkTableKey = tuple[str, str]


class StorageCacheAPI(abc.ABC):
    """
    Top-level storage cache API.

    A StorageCache owns cached single-table objects, cached link-table objects,
    and storage-facing field objects built on top of them. It is intentionally
    lower-level than any interface/library cache.
    """

    db: Optional["DatabaseAPI"]

    #: Main cached tables, keyed by database table name.
    main_tables: Mapping["MainTableName", StorageCacheSingleTableAPI]

    #: All cached link tables, keyed however the implementation prefers.
    #: A canonical choice is ``(src_table_name, dst_table_name)``.
    link_tables: Mapping[LinkTableKey, StorageCacheLinkTableBaseAPI[Any]]

    #: Storage-facing fields, keyed by field name.
    fields: Mapping[FieldKey, FieldBasicInterfaceAPI[Any]]

    def __init__(self, db: Optional["DatabaseAPI"]) -> None:
        """
        Create the storage cache.

        Implementations may choose to defer all actual reading until ``read()``.

        :param db:
        :return:
        """
        self.db = db

    # ------------------------------------------------------------------
    # - LIFECYCLE / STATE

    @property
    @abc.abstractmethod
    def is_loaded(self) -> bool:
        """
        Has the cache been loaded from storage at least once?

        :return:
        """

    @property
    @abc.abstractmethod
    def is_initialized(self) -> bool:
        """
        Is the cache fully initialized and safe for normal use?

        This is stricter than ``is_loaded``; for example, a cache might have
        discovered table objects but not yet populated them.

        :return:
        """

    def assert_ready(self) -> None:
        """
        Raise if the cache is not yet fully initialized.

        :return:
        """
        if not self.is_initialized:
            raise RuntimeError("StorageCache is not fully initialized")

    @abc.abstractmethod
    def read(self, db: Optional["DatabaseAPI"] = None) -> None:
        """
        Fully initialize the storage cache from the database.

        A typical implementation will:
        - read/build table objects
        - populate table caches
        - read/build field objects
        - populate field caches

        :param db:
        :return:
        """

    @abc.abstractmethod
    def reload(self, db: Optional["DatabaseAPI"] = None) -> None:
        """
        Reload the whole storage cache from the database.

        :param db:
        :return:
        """

    @abc.abstractmethod
    def clear(self) -> None:
        """
        Drop all in-memory cached state.

        :return:
        """

    @abc.abstractmethod
    def detach_db(self) -> Optional["DatabaseAPI"]:
        """
        Detach and return the currently attached database, if any.

        Implementations may use this to make shutdown and teardown cheaper by
        breaking references to the live database object.

        :return:
        """

    @abc.abstractmethod
    def close(self) -> None:
        """
        Close the cache and release live references.

        This should leave the cache in a detached / inert state.

        :return:
        """

    # ------------------------------------------------------------------
    # - BOOTSTRAP / BUILD STEPS

    @abc.abstractmethod
    def read_tables(self, db: Optional["DatabaseAPI"] = None) -> None:
        """
        Build cached table objects from the database schema.

        This should define table objects, but need not populate them yet.

        :param db:
        :return:
        """

    @abc.abstractmethod
    def initialize_tables(self, db: Optional["DatabaseAPI"] = None) -> None:
        """
        Populate the cached table objects with data from the database.

        :param db:
        :return:
        """

    @abc.abstractmethod
    def read_fields(self, db: Optional["DatabaseAPI"] = None) -> None:
        """
        Build storage field objects from the schema / configured metadata.

        :param db:
        :return:
        """

    @abc.abstractmethod
    def initialize_fields(self, db: Optional["DatabaseAPI"] = None) -> None:
        """
        Populate storage field objects from the cached tables.

        :param db:
        :return:
        """

    # ------------------------------------------------------------------
    # - TABLE ACCESS

    @abc.abstractmethod
    def has_main_table(
        self,
        name: "MainTableName",
    ) -> bool:
        """
        Return True if the named main table is cached.

        :param name:
        :return:
        """

    @abc.abstractmethod
    def get_main_table(
        self,
        name: Union["MainTableName", StorageCacheSingleTableAPI],
    ) -> StorageCacheSingleTableAPI:
        """
        Resolve a cached main table.

        Passing an already-resolved cached table should return it unchanged.

        :param name:
        :return:
        """

    @abc.abstractmethod
    def iter_main_tables(self) -> Iterable[StorageCacheSingleTableAPI]:
        """
        Iterate all cached main tables.

        :return:
        """

    @abc.abstractmethod
    def get_table(
        self,
        name: str,
    ) -> StorageCacheBaseTableAPI:
        """
        Resolve any cached table by name.

        This may return a main table or a link table.

        :param name:
        :return:
        """

    @abc.abstractmethod
    def iter_tables(self) -> Iterable[StorageCacheBaseTableAPI]:
        """
        Iterate all cached tables, including link tables.

        :return:
        """

    # ------------------------------------------------------------------
    # - LINK TABLE ACCESS

    @abc.abstractmethod
    def has_link_table(
        self,
        src_table: Union["MainTableName", StorageCacheSingleTableAPI],
        dst_table: Union["MainTableName", StorageCacheSingleTableAPI],
        table_type: Optional[TableTypes] = None,
    ) -> bool:
        """
        Return True if a cached link table exists between the given tables.

        If ``table_type`` is provided, the relation must also match that type.

        :param src_table:
        :param dst_table:
        :param table_type:
        :return:
        """

    @abc.abstractmethod
    def get_link_table(
        self,
        src_table: Union["MainTableName", StorageCacheSingleTableAPI],
        dst_table: Union["MainTableName", StorageCacheSingleTableAPI],
        table_type: Optional[TableTypes] = None,
    ) -> StorageCacheLinkTableBaseAPI[Any]:
        """
        Resolve the cached link table connecting the given tables.

        If ``table_type`` is provided, the resolved table must match that type.

        :param src_table:
        :param dst_table:
        :param table_type:
        :return:
        """

    @abc.abstractmethod
    def get_one_one_link_table(
        self,
        src_table: Union["MainTableName", StorageCacheSingleTableAPI],
        dst_table: Union["MainTableName", StorageCacheSingleTableAPI],
    ) -> StorageCacheOneToOneLinkTable[Any]:
        """
        Resolve a cached one-to-one link table.

        :param src_table:
        :param dst_table:
        :return:
        """

    @abc.abstractmethod
    def get_one_many_link_table(
        self,
        src_table: Union["MainTableName", StorageCacheSingleTableAPI],
        dst_table: Union["MainTableName", StorageCacheSingleTableAPI],
    ) -> StorageCacheOneToManyLinkTable:
        """
        Resolve a cached one-to-many link table.

        :param src_table:
        :param dst_table:
        :return:
        """

    @abc.abstractmethod
    def get_many_one_link_table(
        self,
        src_table: Union["MainTableName", StorageCacheSingleTableAPI],
        dst_table: Union["MainTableName", StorageCacheSingleTableAPI],
    ) -> StorageCacheManyToOneLinkTable:
        """
        Resolve a cached many-to-one link table.

        :param src_table:
        :param dst_table:
        :return:
        """

    @abc.abstractmethod
    def get_many_many_link_table(
        self,
        src_table: Union["MainTableName", StorageCacheSingleTableAPI],
        dst_table: Union["MainTableName", StorageCacheSingleTableAPI],
    ) -> StorageCacheManyToManyLinkTable:
        """
        Resolve a cached many-to-many link table.

        :param src_table:
        :param dst_table:
        :return:
        """

    @abc.abstractmethod
    def iter_link_tables(self) -> Iterable[StorageCacheLinkTableBaseAPI[Any]]:
        """
        Iterate all cached link tables.

        :return:
        """

    # ------------------------------------------------------------------
    # - FIELD ACCESS

    @abc.abstractmethod
    def has_field(self, name: FieldKey) -> bool:
        """
        Return True if the named storage field is cached.

        :param name:
        :return:
        """

    @abc.abstractmethod
    def get_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
    ) -> FieldBasicInterfaceAPI[Any]:
        """
        Resolve one cached storage field.

        Passing an already-resolved cached field should return it unchanged.

        :param name:
        :return:
        """

    @abc.abstractmethod
    def iter_fields(self) -> Iterable[FieldBasicInterfaceAPI[Any]]:
        """
        Iterate all cached storage fields.

        :return:
        """

    @abc.abstractmethod
    def get_fields_for_table(
        self,
        table: Union["MainTableName", StorageCacheSingleTableAPI],
    ) -> Sequence[FieldBasicInterfaceAPI[Any]]:
        """
        Return fields whose source table is the given table.

        :param table:
        :return:
        """

    # ------------------------------------------------------------------
    # - TARGETED REFRESH / INVALIDATION

    @abc.abstractmethod
    def reload_main_table(
        self,
        name: Union["MainTableName", StorageCacheSingleTableAPI],
        db: Optional["DatabaseAPI"] = None,
    ) -> None:
        """
        Reload one cached main table.

        :param name:
        :param db:
        :return:
        """

    @abc.abstractmethod
    def reload_link_table(
        self,
        src_table: Union["MainTableName", StorageCacheSingleTableAPI],
        dst_table: Union["MainTableName", StorageCacheSingleTableAPI],
        db: Optional["DatabaseAPI"] = None,
        table_type: Optional[TableTypes] = None,
    ) -> None:
        """
        Reload one cached link table.

        :param src_table:
        :param dst_table:
        :param db:
        :param table_type:
        :return:
        """

    @abc.abstractmethod
    def reload_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
        db: Optional["DatabaseAPI"] = None,
    ) -> None:
        """
        Reload one cached field.

        :param name:
        :param db:
        :return:
        """

    @abc.abstractmethod
    def invalidate_table(
        self,
        table: Union["MainTableName", StorageCacheSingleTableAPI],
    ) -> None:
        """
        Mark one whole main table as stale.

        :param table:
        :return:
        """

    @abc.abstractmethod
    def invalidate_link_table(
        self,
        src_table: Union["MainTableName", StorageCacheSingleTableAPI],
        dst_table: Union["MainTableName", StorageCacheSingleTableAPI],
        table_type: Optional[TableTypes] = None,
    ) -> None:
        """
        Mark one whole link table as stale.

        :param src_table:
        :param dst_table:
        :param table_type:
        :return:
        """

    @abc.abstractmethod
    def invalidate_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
    ) -> None:
        """
        Mark one whole field as stale.

        :param name:
        :return:
        """

    @abc.abstractmethod
    def invalidate_ids(
        self,
        table: Union["MainTableName", StorageCacheSingleTableAPI],
        ids: Iterable[int],
    ) -> None:
        """
        Mark the given ids as stale in the cache.

        Implementations may eagerly refresh them, lazily refresh them on next
        access, or simply note them as dirty.

        :param table:
        :param ids:
        :return:
        """
