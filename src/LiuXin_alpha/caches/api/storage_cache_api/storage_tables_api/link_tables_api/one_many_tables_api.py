"""
One-to-many tables link one src item to many dst items.
Each dst item may be linked to at most one src item.
"""
from __future__ import annotations

import abc
import dataclasses

from typing import Optional, TYPE_CHECKING, Sequence, Any

from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.link_table_base import (
    StorageCacheLinkTableBaseAPI,
)

if TYPE_CHECKING:
    from LiuXin_alpha.databases.db_types import (
        InterlinkTableID,
        SrcTableID,
        DstTableID,
        TableColumnName,
    )
    from LiuXin_alpha.caches.updates.table_updates import (
        OneManyInterlinkTableUpdate,
        OneManyInterLinkTableUpdateResults,
    )
    from LiuXin_alpha.databases.api.row import RowAPI, InterlinkRowAPI


@dataclasses.dataclass(slots=True)
class OneManyLink:
    """
    Represents one edge in a one-to-many relationship.
    """

    src_id: "SrcTableID"
    dst_id: "DstTableID"
    link_row_id: Optional["InterlinkTableID"] = None
    link_type: Optional[str] = None
    priority: Optional[int | float] = None


class StorageCacheOneManyGetterAPI(abc.ABC):
    """
    Common read/query methods for one-to-many link tables.
    """

    # -------------------
    # - EXISTENCE / PREDICATES

    @abc.abstractmethod
    def has_link(
        self,
        src_id: "SrcTableID",
        dst_id: "DstTableID",
        type_filter: Optional[str] = None,
    ) -> bool:
        """
        Return True if a link exists between the given src and dst ids.

        :param src_id:
        :param dst_id:
        :param type_filter:
        :return:
        """

    @abc.abstractmethod
    def has_src(
        self,
        dst_id: "DstTableID",
        type_filter: Optional[str] = None,
    ) -> bool:
        """
        Return True if the given dst id is linked to any src id.

        :param dst_id:
        :param type_filter:
        :return:
        """

    @abc.abstractmethod
    def has_dsts(
        self,
        src_id: "SrcTableID",
        type_filter: Optional[str] = None,
    ) -> bool:
        """
        Return True if the given src id is linked to any dst ids.

        :param src_id:
        :param type_filter:
        :return:
        """

    # -------------------
    # - NORMALIZED LINK OBJECT GETTERS

    @abc.abstractmethod
    def get_link(
        self,
        src_id: "SrcTableID",
        dst_id: "DstTableID",
    ) -> Optional[OneManyLink]:
        """
        Return the normalized link object between the given src and dst ids.

        :param src_id:
        :param dst_id:
        :return:
        """

    @abc.abstractmethod
    def get_links_for_src(
        self,
        src_id: "SrcTableID",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[OneManyLink]:
        """
        Return normalized link objects for all links originating at src_id.

        :param src_id:
        :param require_ordering:
        :param type_filter:
        :return:
        """

    @abc.abstractmethod
    def get_link_for_dst(
        self,
        dst_id: "DstTableID",
        type_filter: Optional[str] = None,
    ) -> Optional[OneManyLink]:
        """
        Return the normalized link object for the given dst id.

        Because this is a one-to-many relation, a dst may have at most one src.

        :param dst_id:
        :param type_filter:
        :return:
        """

    # -------------------
    # - RAW LINK ROW GETTERS

    @abc.abstractmethod
    def get_link_row(
        self,
        src_id: "SrcTableID",
        dst_id: "DstTableID",
    ) -> Optional["InterlinkRowAPI"]:
        """
        Return the raw interlink row between the given src and dst ids.

        :param src_id:
        :param dst_id:
        :return:
        """

    @abc.abstractmethod
    def get_link_rows_for_src(
        self,
        src_id: "SrcTableID",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence["InterlinkRowAPI"]:
        """
        Return raw interlink rows for all links originating at src_id.

        :param src_id:
        :param require_ordering:
        :param type_filter:
        :return:
        """

    @abc.abstractmethod
    def get_link_row_for_dst(
        self,
        dst_id: "DstTableID",
        type_filter: Optional[str] = None,
    ) -> Optional["InterlinkRowAPI"]:
        """
        Return the raw interlink row for the given dst id.

        Because this is a one-to-many relation, a dst may have at most one src.

        :param dst_id:
        :param type_filter:
        :return:
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

        :param dst_id:
        :param type_filter:
        :return:
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

        Note that multiple dst rows may match the given value, so this method is plural.

        :param dst_value:
        :param dst_column:
        :param type_filter:
        :return:
        """

    @abc.abstractmethod
    def get_src_row(
        self,
        dst_id: "DstTableID",
        type_filter: Optional[str] = None,
    ) -> Optional["RowAPI"]:
        """
        Return the src row linked to a dst id.

        :param dst_id:
        :param type_filter:
        :return:
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

        :param dst_value:
        :param dst_column:
        :param type_filter:
        :return:
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

        :param dst_id:
        :param src_column:
        :param type_filter:
        :return:
        """

    # -------------------
    # - SRC -> DST (PLURAL) GETTERS

    @abc.abstractmethod
    def get_dst_ids(
        self,
        src_id: "SrcTableID",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence["DstTableID"]:
        """
        Return the dst ids linked to a src id.

        :param src_id:
        :param require_ordering:
        :param type_filter:
        :return:
        """

    @abc.abstractmethod
    def get_dst_ids_from_value(
        self,
        src_value: Any,
        src_column: "TableColumnName",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence["DstTableID"]:
        """
        Search the src table for a value, and return the dst ids corresponding to it.

        :param src_value:
        :param src_column:
        :param require_ordering:
        :param type_filter:
        :return:
        """

    @abc.abstractmethod
    def get_dst_rows(
        self,
        src_id: "SrcTableID",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence["RowAPI"]:
        """
        Return the dst rows linked to a src id.

        :param src_id:
        :param require_ordering:
        :param type_filter:
        :return:
        """

    @abc.abstractmethod
    def get_dst_rows_from_value(
        self,
        src_value: Any,
        src_column: "TableColumnName",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence["RowAPI"]:
        """
        Search the src table for a value, and return the dst rows corresponding to it.

        :param src_value:
        :param src_column:
        :param require_ordering:
        :param type_filter:
        :return:
        """

    @abc.abstractmethod
    def get_dst_values(
        self,
        src_id: "SrcTableID",
        dst_column: "TableColumnName",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Any]:
        """
        Return values from dst_column for rows linked to the given src id.

        :param src_id:
        :param dst_column:
        :param require_ordering:
        :param type_filter:
        :return:
        """


class StorageCacheOneToManyLinkTable(
    StorageCacheLinkTableBaseAPI,
    StorageCacheOneManyGetterAPI,
):
    """
    Represents a one-to-many table that links one src item to many dst items.

    Ordering and typing are capabilities of the concrete table/schema, not of
    this API class identity.
    """

    @abc.abstractmethod
    def update(
        self,
        update: "OneManyInterlinkTableUpdate",
    ) -> "OneManyInterLinkTableUpdateResults":
        """
        Perform an update of the database and cache.

        This goes in the following order.

        - update_preflight - brings the update object into standard form
        - update_precheck - checks the update is actually valid
        - update_cache - updates this object
        - update_db - writes the update out to the database

        :param update:
        :return:
        """

    @abc.abstractmethod
    def update_preflight(
        self,
        update: "OneManyInterlinkTableUpdate",
    ) -> "OneManyInterlinkTableUpdate":
        """
        Bring the update into a form where it can be more easily written out.

        :param update:
        :return:
        """

    @abc.abstractmethod
    def update_precheck(
        self,
        update: "OneManyInterlinkTableUpdate",
    ) -> bool:
        """
        Check that an update is valid before writing it out.

        :param update:
        :return:
        """

    @abc.abstractmethod
    def update_db(
        self,
        update: "OneManyInterlinkTableUpdate",
    ) -> bool:
        """
        Perform the update on the database itself.

        :param update:
        :return:
        """

    @abc.abstractmethod
    def update_cache(
        self,
        update: "OneManyInterlinkTableUpdate",
    ) -> bool:
        """
        Perform the update on the cache itself.

        :param update:
        :return:
        """