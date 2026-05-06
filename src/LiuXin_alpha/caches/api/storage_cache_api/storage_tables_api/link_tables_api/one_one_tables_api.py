"""
Represents a one-to-one table in the cache.
"""
from __future__ import annotations

import abc
import dataclasses

from typing import Optional, TYPE_CHECKING, Sequence, Any

from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.base_table import TableTypes
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.link_table_base_api import (
    StorageCacheLinkTableBaseAPI,
    T,
)

if TYPE_CHECKING:
    from LiuXin_alpha.caches.updates.table_updates import (
        OneOneInterLinkTableUpdate,
        OneOneInterLinkTableUpdateResults,
    )
    from LiuXin_alpha.databases.db_types import (
        SrcTableID,
        DstTableID,
        InterlinkTableID,
        TableColumnName,
    )
    from LiuXin_alpha.databases.api.row import RowAPI, InterlinkRowAPI


@dataclasses.dataclass(slots=True)
class OneOneLink:
    """
    Represents one edge in a one-to-one relationship.
    """

    src_id: "SrcTableID"
    dst_id: "DstTableID"
    link_row_id: Optional["InterlinkTableID"] = None
    link_type: Optional[str] = None


class StorageCacheOneOneGetterAPI(abc.ABC):
    """
    Common read/query methods for one-to-one link tables.

    One-to-one is intentionally strict and singular in both directions.
    The only naturally plural helpers are the "from_value" lookups, since a
    value search may match multiple rows even when the underlying relation is
    one-to-one.
    """

    @abc.abstractmethod
    def has_link(
        self,
        src_id: "SrcTableID",
        dst_id: "DstTableID",
        type_filter: Optional[str] = None,
    ) -> bool:
        """
        Return True if a link exists between the given src and dst ids.
        """

    @abc.abstractmethod
    def has_src(
        self,
        dst_id: "DstTableID",
        type_filter: Optional[str] = None,
    ) -> bool:
        """
        Return True if the given dst id is linked to a src id.
        """

    @abc.abstractmethod
    def has_dst(
        self,
        src_id: "SrcTableID",
        type_filter: Optional[str] = None,
    ) -> bool:
        """
        Return True if the given src id is linked to a dst id.
        """

    # -------------------
    # - NORMALIZED LINK OBJECT GETTERS

    @abc.abstractmethod
    def get_link(
        self,
        src_id: "SrcTableID",
        dst_id: "DstTableID",
        type_filter: Optional[str] = None,
    ) -> Optional[OneOneLink]:
        """
        Return the normalized link object between the given src and dst ids.
        """

    @abc.abstractmethod
    def get_link_for_src(
        self,
        src_id: "SrcTableID",
        type_filter: Optional[str] = None,
    ) -> Optional[OneOneLink]:
        """
        Return the normalized link object for the given src id.
        """

    @abc.abstractmethod
    def get_link_for_dst(
        self,
        dst_id: "DstTableID",
        type_filter: Optional[str] = None,
    ) -> Optional[OneOneLink]:
        """
        Return the normalized link object for the given dst id.
        """

    # -------------------
    # - RAW LINK ROW GETTERS

    @abc.abstractmethod
    def get_link_row(
        self,
        src_id: "SrcTableID",
        dst_id: "DstTableID",
        type_filter: Optional[str] = None,
    ) -> Optional["InterlinkRowAPI"]:
        """
        Return the raw interlink row between the given src and dst ids.
        """

    @abc.abstractmethod
    def get_link_row_for_src(
        self,
        src_id: "SrcTableID",
        type_filter: Optional[str] = None,
    ) -> Optional["InterlinkRowAPI"]:
        """
        Return the raw interlink row for the given src id.
        """

    @abc.abstractmethod
    def get_link_row_for_dst(
        self,
        dst_id: "DstTableID",
        type_filter: Optional[str] = None,
    ) -> Optional["InterlinkRowAPI"]:
        """
        Return the raw interlink row for the given dst id.
        """

    # -------------------
    # - DST -> SRC (SINGULAR) GETTERS

    @abc.abstractmethod
    def get_src_id(
        self,
        dst_id: "DstTableID",
        type_filter: Optional[str] = None,
    ) -> Optional["SrcTableID"]:
        """
        Return the src id linked to a dst id.
        """

    @abc.abstractmethod
    def get_src_row(
        self,
        dst_id: "DstTableID",
        type_filter: Optional[str] = None,
    ) -> Optional["RowAPI"]:
        """
        Return the src row linked to a dst id.
        """

    @abc.abstractmethod
    def get_src_value(
        self,
        dst_id: "DstTableID",
        src_column: "TableColumnName",
        type_filter: Optional[str] = None,
    ) -> Any:
        """
        Return the value from src_column for the src row linked to the given dst id.
        """

    @abc.abstractmethod
    def get_src_ids_from_value(
        self,
        dst_value: Any,
        dst_column: "TableColumnName",
        type_filter: Optional[str] = None,
    ) -> Sequence["SrcTableID"]:
        """
        Search the dst table for a value, and return the src ids corresponding to it.

        Multiple dst rows may match the search value, so this method is plural.
        """

    @abc.abstractmethod
    def get_src_rows_from_value(
        self,
        dst_value: Any,
        dst_column: "TableColumnName",
        type_filter: Optional[str] = None,
    ) -> Sequence["RowAPI"]:
        """
        Search the dst table for a value, and return the src rows corresponding to it.
        """

    # -------------------
    # - SRC -> DST (SINGULAR) GETTERS

    @abc.abstractmethod
    def get_dst_id(
        self,
        src_id: "SrcTableID",
        type_filter: Optional[str] = None,
    ) -> Optional["DstTableID"]:
        """
        Return the dst id linked to a src id.
        """

    @abc.abstractmethod
    def get_dst_row(
        self,
        src_id: "SrcTableID",
        type_filter: Optional[str] = None,
    ) -> Optional["RowAPI"]:
        """
        Return the dst row linked to a src id.
        """

    @abc.abstractmethod
    def get_dst_value(
        self,
        src_id: "SrcTableID",
        dst_column: "TableColumnName",
        type_filter: Optional[str] = None,
    ) -> Any:
        """
        Return the value from dst_column for the dst row linked to the given src id.
        """

    @abc.abstractmethod
    def get_dst_ids_from_value(
        self,
        src_value: Any,
        src_column: "TableColumnName",
        type_filter: Optional[str] = None,
    ) -> Sequence["DstTableID"]:
        """
        Search the src table for a value, and return the dst ids corresponding to it.

        Multiple src rows may match the search value, so this method is plural.
        """

    @abc.abstractmethod
    def get_dst_rows_from_value(
        self,
        src_value: Any,
        src_column: "TableColumnName",
        type_filter: Optional[str] = None,
    ) -> Sequence["RowAPI"]:
        """
        Search the src table for a value, and return the dst rows corresponding to it.
        """


class StorageCacheOneToOneLinkTable(StorageCacheLinkTableBaseAPI[T], StorageCacheOneOneGetterAPI):
    """
    Represents data that is unique per table, but stored in another table.

    This is the strict one-to-one link-table API. Typing remains a capability
    of the concrete table/schema, not of this API class identity.
    """

    _table_type: TableTypes = TableTypes.ONE_ONE
    _typed: bool = False
    _priority: bool = False

    @abc.abstractmethod
    def update(
        self,
        update: "OneOneInterLinkTableUpdate",
    ) -> "OneOneInterLinkTableUpdateResults":
        """
        Perform an update of the database and cache.
        """

    @abc.abstractmethod
    def update_preflight(
        self,
        update: "OneOneInterLinkTableUpdate",
    ) -> "OneOneInterLinkTableUpdate":
        """
        Bring the update into a form where it can be more easily written out.
        """

    @abc.abstractmethod
    def update_precheck(
        self,
        update: "OneOneInterLinkTableUpdate",
    ) -> bool:
        """
        Check that an update is valid before writing it out.
        """

    @abc.abstractmethod
    def update_db(
        self,
        update: "OneOneInterLinkTableUpdate",
    ) -> bool:
        """
        Perform the update on the database itself.
        """

    @abc.abstractmethod
    def update_cache(
        self,
        update: "OneOneInterLinkTableUpdate",
    ) -> bool:
        """
        Perform the update on the cache itself.
        """

    @abc.abstractmethod
    def get_primary_id_secondary_value_map(self) -> dict[int, T]:
        """
        Compatibility/convenience helper.

        Get a map keyed with the primary id and valued with the value from the
        designated secondary column.
        """


class StorageCacheOneToOneLinkTableAPI(StorageCacheOneToOneLinkTable[T]):
    """
    Backwards-compatible alias while the rest of the API settles.
    """


class StorageCacheItemCalibreUUIDTableAPI(StorageCacheLinkTableBaseAPI):
    """
    Represents a calibre uuid linked to its item.

    The closest direct analogue to a calibre book is the item at the end of the
    WEMI stack. This links calibre uuids to those items.
    """

    @abc.abstractmethod
    def lookup_by_uuid(self, uuid: str) -> int:
        """
        Lookup an item id by its uuid and return the id of the item as an int.
        """
