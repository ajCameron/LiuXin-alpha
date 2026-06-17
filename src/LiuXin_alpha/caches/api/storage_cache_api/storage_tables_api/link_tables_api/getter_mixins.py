from __future__ import annotations

import abc
from typing import Optional, Sequence, Any

from LiuXin_alpha.databases.api import RowAPI
from LiuXin_alpha.databases.api.row_api import InterlinkRowAPI
from LiuXin_alpha.databases.db_types import DstTableID, SrcTableID, TableColumnName


class StorageCacheGetterMixinAPI(abc.ABC):
    """
    Common methods for getting infomation about linked tables.
    """

    # -------------------
    # - SRC TABLE GETTERS

    @abc.abstractmethod
    def get_src_ids(
            self,
            dst_id: DstTableID,
            require_ordering: bool = False,
            type_filter: Optional[str] = None) -> Sequence[SrcTableID]:
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
            dst_column: TableColumnName,
            require_ordering: bool = False,
            type_filter: Optional[str] = None) -> Sequence[SrcTableID]:
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
            dst_id: DstTableID,
            require_ordering: bool = False,
            type_filter: Optional[str] = None) -> Sequence["RowAPI"]:
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
            dst_column: TableColumnName,
            require_ordering: bool = False,
            type_filter: Optional[str] = None) -> Sequence["RowAPI"]:
        """
        Search the dst table for a value, and return the src ids corresponding to it.

        :param dst_value:
        :param dst_column:
        :param require_ordering:
        :param type_filter:
        :return:
        """

    @abc.abstractmethod
    def get_src_values(
            self,
            dst_id: DstTableID,
            src_column: TableColumnName,
            require_ordering: bool = False,
            type_filter: Optional[str] = None) -> Sequence[Any]:
        """
        Return the values linked to a given src id.

        :param dst_id:
        :param src_column:
        :param require_ordering:
        :param type_filter:
        :return:
        """

    # -------------------
    # -------------------
    # - DST TABLE GETTERS

    @abc.abstractmethod
    def get_dst_ids(
            self,
            src_id: SrcTableID,
            require_ordering: bool = False,
            type_filter: Optional[str] = None) -> Sequence[DstTableID]:
        """
        Return the src ids linked to a dst id.

        :param src_id:
        :param require_ordering:
        :param type_filter:

        :return:
        """

    @abc.abstractmethod
    def get_dst_ids_from_value(
            self,
            src_value: Any,
            src_column: TableColumnName,
            require_ordering: bool = False,
            type_filter: Optional[str] = None) -> Sequence[DstTableID]:
        """
        Search the dst table for a value, and return the src ids corresponding to it.

        :param src_value:
        :param src_column:
        :param require_ordering:
        :param type_filter:
        :return:
        """

    @abc.abstractmethod
    def get_dst_rows(
            self,
            src_id: SrcTableID,
            require_ordering: bool = False,
            type_filter: Optional[str] = None) -> Sequence["RowAPI"]:
        """
        Return the src rows linked to a dst id.

        :param src_id:
        :param require_ordering:
        :param type_filter:

        :return:
        """

    @abc.abstractmethod
    def get_dst_rows_from_value(
            self,
            src_value: Any,
            src_column: TableColumnName,
            require_ordering: bool = False,
            type_filter: Optional[str] = None) -> Sequence["RowAPI"]:
        """
        Search the dst table for a value, and return the src ids corresponding to it.

        :param src_value:
        :param src_column:
        :param require_ordering:
        :param type_filter:
        :return:
        """

    @abc.abstractmethod
    def get_dst_values(
            self,
            src_id: SrcTableID,
            dst_column: TableColumnName,
            require_ordering: bool = False,
            type_filter: Optional[str] = None) -> Sequence[Any]:
        """
        Return the values linked to a given src id.

        :param src_id:
        :param dst_column:
        :param require_ordering:
        :param type_filter:
        :return:
        """

    # -------------------
    # --------------
    # - LINK GETTERS

    @abc.abstractmethod
    def get_link_row(
            self,
            src_id: SrcTableID,
            dst_id: DstTableID,
            insist_on_singular: bool = True) -> Optional["InterlinkRowAPI"]:
        """
        Get a link row between the given src and dst.

        Always singular, or None.
        :param src_id:
        :param dst_id:
        :param insist_on_singular: If True, and the table allows multiple src/dst links, error.
        :return:
        """

    @abc.abstractmethod
    def get_link_rows(
            self,
            src_id: SrcTableID,
            dst_id: DstTableID) -> Sequence["InterlinkRowAPI"]:
        """
        Get the link rows between the given src and dst.

        May be multiple, in the case where multiple srcs could connect to multiple dsts.
        :param src_id:
        :param dst_id:
        :return:
        """

    # --------------
