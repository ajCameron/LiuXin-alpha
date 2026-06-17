"""
Many-to-one tables link many src items to one dst item.
Each src item may be linked to at most one dst item.
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
        ManyOneInterlinkTableUpdate,
        ManyOneInterLinkTableUpdateResults,
    )
    from LiuXin_alpha.databases.api.row_api import RowAPI, InterlinkRowAPI


@dataclasses.dataclass(slots=True)
class ManyOneLink:
    """
    Represents one link in a many-to-one relationship.
    """

    src_id: "SrcTableID"
    dst_id: "DstTableID"
    link_row_id: Optional["InterlinkTableID"] = None
    link_type: Optional[str] = None
    priority: Optional[int | float] = None


class StorageCacheManyOneGetterAPI(abc.ABC):
    """
    Common read/query methods for many-to-one link tables.
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
        """

    @abc.abstractmethod
    def has_dst(
        self,
        src_id: "SrcTableID",
        type_filter: Optional[str] = None,
    ) -> bool:
        """
        Return True if the given src id is linked to any dst id.
        """

    @abc.abstractmethod
    def has_srcs(
        self,
        dst_id: "DstTableID",
        type_filter: Optional[str] = None,
    ) -> bool:
        """
        Return True if the given dst id is linked to any src ids.
        """

    # -------------------
    # - NORMALIZED LINK OBJECT GETTERS

    @abc.abstractmethod
    def get_link(
        self,
        src_id: "SrcTableID",
        dst_id: "DstTableID",
    ) -> Optional[ManyOneLink]:
        """
        Return the normalized link object between the given src and dst ids.
        """

    @abc.abstractmethod
    def get_link_for_src(
        self,
        src_id: "SrcTableID",
        type_filter: Optional[str] = None,
    ) -> Optional[ManyOneLink]:
        """
        Return the normalized link object for the given src id.

        Because this is a many-to-one relation, a src may have at most one dst.
        """

    @abc.abstractmethod
    def get_links_for_dst(
        self,
        dst_id: "DstTableID",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[ManyOneLink]:
        """
        Return normalized link objects for all links terminating at dst_id.
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
        """

    @abc.abstractmethod
    def get_link_row_for_src(
        self,
        src_id: "SrcTableID",
        type_filter: Optional[str] = None,
    ) -> Optional["InterlinkRowAPI"]:
        """
        Return the raw interlink row for the given src id.

        Because this is a many-to-one relation, a src may have at most one dst.
        """

    @abc.abstractmethod
    def get_link_rows_for_dst(
        self,
        dst_id: "DstTableID",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence["InterlinkRowAPI"]:
        """
        Return raw interlink rows for all links terminating at dst_id.
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
    def get_dst_ids_from_value(
        self,
        src_value: Any,
        src_column: "TableColumnName",
        type_filter: Optional[str] = None,
    ) -> Sequence["DstTableID"]:
        """
        Search the src table for a value, and return the dst ids corresponding to it.

        Note that multiple src rows may match the given value, so this method is plural.
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
    def get_dst_rows_from_value(
        self,
        src_value: Any,
        src_column: "TableColumnName",
        type_filter: Optional[str] = None,
    ) -> Sequence["RowAPI"]:
        """
        Search the src table for a value, and return the dst rows corresponding to it.
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

    # -------------------
    # - DST -> SRC (PLURAL) GETTERS

    @abc.abstractmethod
    def get_src_ids(
        self,
        dst_id: "DstTableID",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence["SrcTableID"]:
        """
        Return the src ids linked to a dst id.
        """

    @abc.abstractmethod
    def get_src_ids_from_value(
        self,
        dst_value: Any,
        dst_column: "TableColumnName",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence["SrcTableID"]:
        """
        Search the dst table for a value, and return the src ids corresponding to it.
        """

    @abc.abstractmethod
    def get_src_rows(
        self,
        dst_id: "DstTableID",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence["RowAPI"]:
        """
        Return the src rows linked to a dst id.
        """

    @abc.abstractmethod
    def get_src_rows_from_value(
        self,
        dst_value: Any,
        dst_column: "TableColumnName",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence["RowAPI"]:
        """
        Search the dst table for a value, and return the src rows corresponding to it.
        """

    @abc.abstractmethod
    def get_src_values(
        self,
        dst_id: "DstTableID",
        src_column: "TableColumnName",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Any]:
        """
        Return values from src_column for rows linked to the given dst id.
        """


class StorageCacheManyToOneLinkTable(
    StorageCacheLinkTableBaseAPI,
    StorageCacheManyOneGetterAPI,
):
    """
    Represents a many-to-one table that links many src items to one dst item.

    Ordering and typing are capabilities of the concrete table/schema, not of
    this API class identity.
    """

    @abc.abstractmethod
    def update(
        self,
        update: "ManyOneInterlinkTableUpdate",
    ) -> "ManyOneInterLinkTableUpdateResults":
        """
        Perform an update of the database and cache.

        This goes in the following order.

        - update_preflight - brings the update object into standard form
        - update_precheck - checks the update is actually valid
        - update_cache - updates this object
        - update_db - writes the update out to the database
        """

    @abc.abstractmethod
    def update_preflight(
        self,
        update: "ManyOneInterlinkTableUpdate",
    ) -> "ManyOneInterlinkTableUpdate":
        """
        Bring the update into a form where it can be more easily written out.
        """

    @abc.abstractmethod
    def update_precheck(
        self,
        update: "ManyOneInterlinkTableUpdate",
    ) -> bool:
        """
        Check that an update is valid before writing it out.
        """

    @abc.abstractmethod
    def update_db(
        self,
        update: "ManyOneInterlinkTableUpdate",
    ) -> bool:
        """
        Perform the update on the database itself.
        """

    @abc.abstractmethod
    def update_cache(
        self,
        update: "ManyOneInterlinkTableUpdate",
    ) -> bool:
        """
        Perform the update on the cache itself.
        """
