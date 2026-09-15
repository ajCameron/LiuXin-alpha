"""
Define common plural endpoint projections and raw pair-row lookup contracts.

The mixin keeps source and destination traversal symmetric. Value searches
match the opposite table before crossing links; column projections read
the requested endpoint side. Concrete classes supply all lookup behavior.
"""

from __future__ import annotations

import abc
from typing import Optional, Sequence, Any

from LiuXin_alpha.databases.api import RowAPI
from LiuXin_alpha.databases.api.row_api import InterlinkRowAPI
from LiuXin_alpha.databases.db_types import DstTableID, SrcTableID, TableColumnName


class StorageCacheGetterMixinAPI(abc.ABC):
    """
    Specify shared plural directed link queries independently of cache storage.

    Abstract getters separate projected link values, raw association Rows
    and endpoint IDs/Rows/column values. Cardinality determines scalar versus
    plural lookup shapes; value searches can be plural even for singular links.
    Concrete backends define freshness, copying, ordering and ambiguity errors.

    Example:
        Retrieve a physical link Row when its stored metadata columns are needed,
        or an endpoint Row when the linked entity payload is needed.
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
        Find source identities linked to the selected destination identity.

        Abstract contract. Traverse accepted links in the requested direction. Snapshot freshness
        and endpoint-existence checks belong to the concrete backend.

        Example:
            Use this direction to traverse from destination rows back to source rows.


        :param dst_id: Destination row identity for reverse link selection.
        :param require_ordering: Request supported link ordering; concrete priority rules may order even when false.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Sequence of linked source identities; repeated links/value matches may remain repeated.
        """

    @abc.abstractmethod
    def get_src_ids_from_value(
            self,
            dst_value: Any,
            dst_column: TableColumnName,
            require_ordering: bool = False,
            type_filter: Optional[str] = None) -> Sequence[SrcTableID]:
        """
        Find source identities linked to destination-column value matches.

        Abstract contract. Value matching searches the opposite endpoint table before traversing
        links; several matching rows can make the result plural even for one-to-one
        relations.

        Example:
            Several equal destination values can contribute several source results.


        :param dst_value: Value matched against the specified destination-table column.
        :param dst_column: Destination column to search or project, according to the getter.
        :param require_ordering: Request supported link ordering; concrete priority rules may order even when false.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Sequence of linked source identities; repeated links/value matches may remain repeated.
        """

    @abc.abstractmethod
    def get_src_rows(
            self,
            dst_id: DstTableID,
            require_ordering: bool = False,
            type_filter: Optional[str] = None) -> Sequence["RowAPI"]:
        """
        Find source Rows linked to the selected destination identity.

        Abstract contract. Traverse accepted links in the requested direction. Snapshot freshness
        and endpoint-existence checks belong to the concrete backend.
        Row payload ownership and treatment of dangling endpoints are backend-specific.

        Example:
            Use this direction to traverse from destination rows back to source rows.


        :param dst_id: Destination row identity for reverse link selection.
        :param require_ordering: Request supported link ordering; concrete priority rules may order even when false.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Sequence of linked source Rows; repeated links/value matches may remain repeated.
        """

    @abc.abstractmethod
    def get_src_rows_from_value(
            self,
            dst_value: Any,
            dst_column: TableColumnName,
            require_ordering: bool = False,
            type_filter: Optional[str] = None) -> Sequence["RowAPI"]:
        """
        Find source Rows linked to destination-column value matches.

        Abstract contract. Value matching searches the opposite endpoint table before traversing
        links; several matching rows can make the result plural even for one-to-one
        relations.
        Row payload ownership and treatment of dangling endpoints are backend-specific.

        Example:
            Several equal destination values can contribute several source results.


        :param dst_value: Value matched against the specified destination-table column.
        :param dst_column: Destination column to search or project, according to the getter.
        :param require_ordering: Request supported link ordering; concrete priority rules may order even when false.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Sequence of linked source Rows; repeated links/value matches may remain repeated.
        """

    @abc.abstractmethod
    def get_src_values(
            self,
            dst_id: DstTableID,
            src_column: TableColumnName,
            require_ordering: bool = False,
            type_filter: Optional[str] = None) -> Sequence[Any]:
        """
        Find source column values linked to the selected destination identity.

        Abstract contract. Traverse accepted links in the requested direction. Snapshot freshness
        and endpoint-existence checks belong to the concrete backend.
        Read the named column on the returned endpoint side; null/default handling
        is supplied by the implementation.

        Example:
            Use this direction to traverse from destination rows back to source rows.


        :param dst_id: Destination row identity for reverse link selection.
        :param src_column: Source column to search or project, according to the getter.
        :param require_ordering: Request supported link ordering; concrete priority rules may order even when false.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Sequence of linked source column values; repeated links/value matches may remain repeated.
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
        Find destination identities linked to the selected source identity.

        Abstract contract. Traverse accepted links in the requested direction. Snapshot freshness
        and endpoint-existence checks belong to the concrete backend.

        Example:
            Use this direction to traverse from source rows back to destination rows.


        :param src_id: Source row identity for directed link selection.
        :param require_ordering: Request supported link ordering; concrete priority rules may order even when false.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Sequence of linked destination identities; repeated links/value matches may remain repeated.
        """

    @abc.abstractmethod
    def get_dst_ids_from_value(
            self,
            src_value: Any,
            src_column: TableColumnName,
            require_ordering: bool = False,
            type_filter: Optional[str] = None) -> Sequence[DstTableID]:
        """
        Find destination identities linked to source-column value matches.

        Abstract contract. Value matching searches the opposite endpoint table before traversing
        links; several matching rows can make the result plural even for one-to-one
        relations.

        Example:
            Several equal source values can contribute several destination results.


        :param src_value: Value matched against the specified source-table column.
        :param src_column: Source column to search or project, according to the getter.
        :param require_ordering: Request supported link ordering; concrete priority rules may order even when false.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Sequence of linked destination identities; repeated links/value matches may remain repeated.
        """

    @abc.abstractmethod
    def get_dst_rows(
            self,
            src_id: SrcTableID,
            require_ordering: bool = False,
            type_filter: Optional[str] = None) -> Sequence["RowAPI"]:
        """
        Find destination Rows linked to the selected source identity.

        Abstract contract. Traverse accepted links in the requested direction. Snapshot freshness
        and endpoint-existence checks belong to the concrete backend.
        Row payload ownership and treatment of dangling endpoints are backend-specific.

        Example:
            Use this direction to traverse from source rows back to destination rows.


        :param src_id: Source row identity for directed link selection.
        :param require_ordering: Request supported link ordering; concrete priority rules may order even when false.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Sequence of linked destination Rows; repeated links/value matches may remain repeated.
        """

    @abc.abstractmethod
    def get_dst_rows_from_value(
            self,
            src_value: Any,
            src_column: TableColumnName,
            require_ordering: bool = False,
            type_filter: Optional[str] = None) -> Sequence["RowAPI"]:
        """
        Find destination Rows linked to source-column value matches.

        Abstract contract. Value matching searches the opposite endpoint table before traversing
        links; several matching rows can make the result plural even for one-to-one
        relations.
        Row payload ownership and treatment of dangling endpoints are backend-specific.

        Example:
            Several equal source values can contribute several destination results.


        :param src_value: Value matched against the specified source-table column.
        :param src_column: Source column to search or project, according to the getter.
        :param require_ordering: Request supported link ordering; concrete priority rules may order even when false.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Sequence of linked destination Rows; repeated links/value matches may remain repeated.
        """

    @abc.abstractmethod
    def get_dst_values(
            self,
            src_id: SrcTableID,
            dst_column: TableColumnName,
            require_ordering: bool = False,
            type_filter: Optional[str] = None) -> Sequence[Any]:
        """
        Find destination column values linked to the selected source identity.

        Abstract contract. Traverse accepted links in the requested direction. Snapshot freshness
        and endpoint-existence checks belong to the concrete backend.
        Read the named column on the returned endpoint side; null/default handling
        is supplied by the implementation.

        Example:
            Use this direction to traverse from source rows back to destination rows.


        :param src_id: Source row identity for directed link selection.
        :param dst_column: Destination column to search or project, according to the getter.
        :param require_ordering: Request supported link ordering; concrete priority rules may order even when false.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Sequence of linked destination column values; repeated links/value matches may remain repeated.
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
        Read one optional physical association Rows selected by the directed endpoint pair.

        Abstract contract. Raw Rows expose physical link columns.
        Concrete implementations define singularity errors for ambiguous records.
        The optional singularity flag, where supplied, requests strict checking.

        Example:
            An absent association can yield None without creating a new link.


        :param src_id: Source row identity for directed link selection.
        :param dst_id: Destination row identity for reverse link selection.
        :param insist_on_singular: True requests an error when several physical records match instead of selecting one.
        :return: Physical link Row, or None when no accepted link exists.
        """

    @abc.abstractmethod
    def get_link_rows(
            self,
            src_id: SrcTableID,
            dst_id: DstTableID) -> Sequence["InterlinkRowAPI"]:
        """
        Read all physical association Rows selected by the directed endpoint pair.

        Abstract contract. Raw Rows expose physical link columns.
        Repeated physical links can remain repeated; ordering follows the backend.

        Example:
            Two physical links for a pair can yield two separate records.


        :param src_id: Source row identity for directed link selection.
        :param dst_id: Destination row identity for reverse link selection.
        :return: Sequence of physical association Rows, empty when no records match.
        """

    # --------------
