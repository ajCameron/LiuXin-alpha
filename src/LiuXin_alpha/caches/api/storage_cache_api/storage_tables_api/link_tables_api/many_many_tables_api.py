"""
Many-to-many tables link many items to many others.
"""
from __future__ import annotations

import abc
import dataclasses

from typing import Optional, TYPE_CHECKING, Sequence, Any

from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.link_table_base_api import (
    StorageCacheLinkTableBaseAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.one_many_tables_api import (
    OneManyLink,
)

if TYPE_CHECKING:
    from LiuXin_alpha.databases.db_types import (
        SrcTableID,
        DstTableID,
        TableColumnName,
    )
    from LiuXin_alpha.caches.updates.table_updates import (
        ManyManyInterlinkTableUpdate,
        ManyManyInterLinkTableUpdateResults,
    )
    from LiuXin_alpha.databases.api.row import RowAPI, InterlinkRowAPI


@dataclasses.dataclass(slots=True)
class ManyManyLink(OneManyLink):
    """
    Represents one edge in a many-to-many relationship.

    This is the normalized object-level view of a link, distinct from the raw
    interlink row.
    """




class StorageCacheManyManyGetterAPI(abc.ABC):
    """
    Common read/query methods for many-to-many link tables.
    """

    # -------------------
    # - EXISTENCE / LINK OBJECT GETTERS

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
    def get_link(
        self,
        src_id: "SrcTableID",
        dst_id: "DstTableID",
        insist_on_singular: bool = True,
    ) -> Optional[ManyManyLink]:
        """
        Return a normalized link object between the given src and dst ids.

        If duplicate link rows are permitted by the table and multiple matches
        exist, implementations should error when insist_on_singular is True.

        :param src_id:
        :param dst_id:
        :param insist_on_singular:
        :return:
        """

    @abc.abstractmethod
    def get_links(
        self,
        src_id: "SrcTableID",
        dst_id: "DstTableID",
    ) -> Sequence[ManyManyLink]:
        """
        Return all normalized link objects between the given src and dst ids.

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
    ) -> Sequence[ManyManyLink]:
        """
        Return normalized link objects for all links originating at src_id.

        :param src_id:
        :param require_ordering:
        :param type_filter:
        :return:
        """

    @abc.abstractmethod
    def get_links_for_dst(
        self,
        dst_id: "DstTableID",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[ManyManyLink]:
        """
        Return normalized link objects for all links terminating at dst_id.

        :param dst_id:
        :param require_ordering:
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
        insist_on_singular: bool = True,
    ) -> Optional["InterlinkRowAPI"]:
        """
        Get a single raw link row between the given src and dst ids.

        If duplicate link rows are permitted by the table and multiple matches
        exist, implementations should error when insist_on_singular is True.

        :param src_id:
        :param dst_id:
        :param insist_on_singular:
        :return:
        """

    @abc.abstractmethod
    def get_link_rows(
        self,
        src_id: "SrcTableID",
        dst_id: "DstTableID",
    ) -> Sequence["InterlinkRowAPI"]:
        """
        Get all raw link rows between the given src and dst ids.

        :param src_id:
        :param dst_id:
        :return:
        """

    # -------------------
    # - SRC TABLE GETTERS

    @abc.abstractmethod
    def get_src_ids(
        self,
        dst_id: "DstTableID",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence["SrcTableID"]:
        """
        Return the src ids linked to a dst id.

        :param dst_id:
        :param require_ordering:
        :param type_filter:
        :return:
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

        :param dst_value:
        :param dst_column:
        :param require_ordering:
        :param type_filter:
        :return:
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

        :param dst_id:
        :param require_ordering:
        :param type_filter:
        :return:
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

        :param dst_value:
        :param dst_column:
        :param require_ordering:
        :param type_filter:
        :return:
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

        :param dst_id:
        :param src_column:
        :param require_ordering:
        :param type_filter:
        :return:
        """

    # -------------------
    # - DST TABLE GETTERS

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


class StorageCacheManyToManyLinkTable(
    StorageCacheLinkTableBaseAPI,
    StorageCacheManyManyGetterAPI,
):
    """
    Represents a many-to-many table that links many items to many others.

    These are very common; a basic example would be tags.
    Ordering and typing are capabilities of the concrete table/schema, not of
    this API class identity.
    """

    @abc.abstractmethod
    def update(
        self,
        update: "ManyManyInterlinkTableUpdate",
    ) -> "ManyManyInterLinkTableUpdateResults":
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
        update: "ManyManyInterlinkTableUpdate",
    ) -> "ManyManyInterlinkTableUpdate":
        """
        Bring the update into a form where it can be more easily written out.

        :param update:
        :return:
        """

    @abc.abstractmethod
    def update_precheck(
        self,
        update: "ManyManyInterlinkTableUpdate",
    ) -> bool:
        """
        Check that an update is valid before writing it out.

        :param update:
        :return:
        """

    @abc.abstractmethod
    def update_db(
        self,
        update: "ManyManyInterlinkTableUpdate",
    ) -> bool:
        """
        Perform the update on the database itself.

        :param update:
        :return:
        """

    @abc.abstractmethod
    def update_cache(
        self,
        update: "ManyManyInterlinkTableUpdate",
    ) -> bool:
        """
        Perform the update on the cache itself.

        :param update:
        :return:
        """