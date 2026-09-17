"""
Define many-to-one link values, directed reads and legacy update hooks.

Projected link dataclasses carry endpoint identities and optional physical
row/type metadata plus priority.
The API classes prescribe read shapes while concrete backends implement
cardinality enforcement, ordering, storage refresh and update sequencing.
"""
from __future__ import annotations

import abc
import dataclasses
from typing import TYPE_CHECKING, Any, Optional, Sequence

from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.link_table_base_api import (
    StorageCacheLinkTableBaseAPI,
)

if TYPE_CHECKING:
    from LiuXin_alpha.caches.updates.table_updates import (
        ManyOneInterlinkTableUpdate,
        ManyOneInterLinkTableUpdateResults,
    )
    from LiuXin_alpha.databases.api.row_api import InterlinkRowAPI, RowAPI
    from LiuXin_alpha.databases.db_types import (
        DstTableID,
        InterlinkTableID,
        SrcTableID,
        TableColumnName,
    )


@dataclasses.dataclass(slots=True)
class ManyOneLink:
    """
    Carry one many-to-one association as a mutable slots value.

    Require src_id and dst_id; link_row_id and link_type default to None.
    priority also defaults to None and accepts integer/float annotations.
    Construction neither coerces IDs nor checks database membership/cardinality.
    The payload contains no physical Row object.

    Example:
        >>> ManyOneLink(1, 7).link_row_id is None
        True
    """

    src_id: "SrcTableID"
    dst_id: "DstTableID"
    link_row_id: Optional["InterlinkTableID"] = None
    link_type: Optional[str] = None
    priority: Optional[int | float] = None


class StorageCacheManyOneGetterAPI(abc.ABC):
    """
    Specify many-to-one directed link queries independently of cache storage.

    Abstract getters separate projected link values, raw association Rows
    and endpoint IDs/Rows/column values. Cardinality determines scalar versus
    plural lookup shapes; value searches can be plural even for singular links.
    Concrete backends define freshness, copying, ordering and ambiguity errors.

    Example:
        Retrieve a physical link Row when its stored metadata columns are needed,
        or an endpoint Row when the linked entity payload is needed.
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
        Test for accepted cached associations matching a directed endpoint pair.

        Abstract contract. Presence concerns link records; endpoint row existence
        and freshness checks depend on the concrete backend.

        Example:
            A dangling physical link may satisfy presence even when its endpoint
            Row cannot be retrieved.


        :param src_id: Source row identity for directed link selection.
        :param dst_id: Destination row identity for reverse link selection.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Boolean presence of at least one accepted association.
        """

    @abc.abstractmethod
    def has_dst(
        self,
        src_id: "SrcTableID",
        type_filter: Optional[str] = None,
    ) -> bool:
        """
        Test for accepted cached associations matching a source's outgoing links.

        Abstract contract. Presence concerns link records; endpoint row existence
        and freshness checks depend on the concrete backend.

        Example:
            A dangling physical link may satisfy presence even when its endpoint
            Row cannot be retrieved.


        :param src_id: Source row identity for directed link selection.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Boolean presence of at least one accepted association.
        """

    @abc.abstractmethod
    def has_srcs(
        self,
        dst_id: "DstTableID",
        type_filter: Optional[str] = None,
    ) -> bool:
        """
        Test for accepted cached associations matching a destination's incoming links.

        Abstract contract. Presence concerns link records; endpoint row existence
        and freshness checks depend on the concrete backend.

        Example:
            A dangling physical link may satisfy presence even when its endpoint
            Row cannot be retrieved.


        :param dst_id: Destination row identity for reverse link selection.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Boolean presence of at least one accepted association.
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
        Read one optional ManyOneLink values selected by the directed endpoint pair.

        Abstract contract. Link values carry endpoint identities and supported association metadata.
        Concrete implementations define singularity errors for ambiguous records.
        The optional singularity flag, where supplied, requests strict checking.

        Example:
            An absent association can yield None without creating a new link.


        :param src_id: Source row identity for directed link selection.
        :param dst_id: Destination row identity for reverse link selection.
        :return: ManyOneLink value, or None when no accepted link exists.
        """

    @abc.abstractmethod
    def get_link_for_src(
        self,
        src_id: "SrcTableID",
        type_filter: Optional[str] = None,
    ) -> Optional[ManyOneLink]:
        """
        Read one optional ManyOneLink values selected by the source ID.

        Abstract contract. Link values carry endpoint identities and supported association metadata.
        Concrete implementations define singularity errors for ambiguous records.
        The optional singularity flag, where supplied, requests strict checking.

        Example:
            An absent association can yield None without creating a new link.


        :param src_id: Source row identity for directed link selection.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: ManyOneLink value, or None when no accepted link exists.
        """

    @abc.abstractmethod
    def get_links_for_dst(
        self,
        dst_id: "DstTableID",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[ManyOneLink]:
        """
        Read all ManyOneLink values selected by the destination ID.

        Abstract contract. Link values carry endpoint identities and supported association metadata.
        Repeated physical links can remain repeated; ordering follows the backend.

        Example:
            Two physical links for a pair can yield two separate records.


        :param dst_id: Destination row identity for reverse link selection.
        :param require_ordering: Request supported link ordering; concrete priority rules may order even when false.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Sequence of ManyOneLink values, empty when no records match.
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
        Read one optional physical association Rows selected by the directed endpoint pair.

        Abstract contract. Raw Rows expose physical link columns.
        Concrete implementations define singularity errors for ambiguous records.
        The optional singularity flag, where supplied, requests strict checking.

        Example:
            An absent association can yield None without creating a new link.


        :param src_id: Source row identity for directed link selection.
        :param dst_id: Destination row identity for reverse link selection.
        :return: Physical link Row, or None when no accepted link exists.
        """

    @abc.abstractmethod
    def get_link_row_for_src(
        self,
        src_id: "SrcTableID",
        type_filter: Optional[str] = None,
    ) -> Optional["InterlinkRowAPI"]:
        """
        Read one optional physical association Rows selected by the source ID.

        Abstract contract. Raw Rows expose physical link columns.
        Concrete implementations define singularity errors for ambiguous records.
        The optional singularity flag, where supplied, requests strict checking.

        Example:
            An absent association can yield None without creating a new link.


        :param src_id: Source row identity for directed link selection.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Physical link Row, or None when no accepted link exists.
        """

    @abc.abstractmethod
    def get_link_rows_for_dst(
        self,
        dst_id: "DstTableID",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence["InterlinkRowAPI"]:
        """
        Read all physical association Rows selected by the destination ID.

        Abstract contract. Raw Rows expose physical link columns.
        Repeated physical links can remain repeated; ordering follows the backend.

        Example:
            Two physical links for a pair can yield two separate records.


        :param dst_id: Destination row identity for reverse link selection.
        :param require_ordering: Request supported link ordering; concrete priority rules may order even when false.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Sequence of physical association Rows, empty when no records match.
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
        Find destination identity linked to the selected source identity.

        Abstract contract. Traverse accepted links in the requested direction. Snapshot freshness
        and endpoint-existence checks belong to the concrete backend.

        Example:
            Use this direction to traverse from source rows back to destination rows.


        :param src_id: Source row identity for directed link selection.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Linked destination identity, or None according to backend missing-link semantics.
        """

    @abc.abstractmethod
    def get_dst_ids_from_value(
        self,
        src_value: Any,
        src_column: "TableColumnName",
        type_filter: Optional[str] = None,
    ) -> Sequence["DstTableID"]:
        """
        Find destination identities linked to source-column value matches.

        Abstract contract. Value matching searches the opposite endpoint table before traversing
        links; several matching rows can make the result plural even for one-to-one
        relations.

        Example:
            Several equal source values can contribute several destination results.


        :param src_value: Value matched against the specified source-table column.
        :param src_column: Source column to search or project, according to the getter.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Sequence of linked destination identities; repeated links/value matches may remain repeated.
        """

    @abc.abstractmethod
    def get_dst_row(
        self,
        src_id: "SrcTableID",
        type_filter: Optional[str] = None,
    ) -> Optional["RowAPI"]:
        """
        Find destination Row linked to the selected source identity.

        Abstract contract. Traverse accepted links in the requested direction. Snapshot freshness
        and endpoint-existence checks belong to the concrete backend.
        Row payload ownership and treatment of dangling endpoints are backend-specific.

        Example:
            Use this direction to traverse from source rows back to destination rows.


        :param src_id: Source row identity for directed link selection.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Linked destination Row, or None according to backend missing-link semantics.
        """

    @abc.abstractmethod
    def get_dst_rows_from_value(
        self,
        src_value: Any,
        src_column: "TableColumnName",
        type_filter: Optional[str] = None,
    ) -> Sequence["RowAPI"]:
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
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Sequence of linked destination Rows; repeated links/value matches may remain repeated.
        """

    @abc.abstractmethod
    def get_dst_value(
        self,
        src_id: "SrcTableID",
        dst_column: "TableColumnName",
        type_filter: Optional[str] = None,
    ) -> Any:
        """
        Find destination column value linked to the selected source identity.

        Abstract contract. Traverse accepted links in the requested direction. Snapshot freshness
        and endpoint-existence checks belong to the concrete backend.
        Read the named column on the returned endpoint side; null/default handling
        is supplied by the implementation.

        Example:
            Use this direction to traverse from source rows back to destination rows.


        :param src_id: Source row identity for directed link selection.
        :param dst_column: Destination column to search or project, according to the getter.
        :param type_filter: Optional exact link-type restriction interpreted by the backend.
        :return: Linked destination column value, or None according to backend missing-link semantics.
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
        dst_column: "TableColumnName",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence["SrcTableID"]:
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
        dst_id: "DstTableID",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence["RowAPI"]:
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
        dst_column: "TableColumnName",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence["RowAPI"]:
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
        dst_id: "DstTableID",
        src_column: "TableColumnName",
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Any]:
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


class StorageCacheManyToOneLinkTable(
    StorageCacheLinkTableBaseAPI,
    StorageCacheManyOneGetterAPI,
):
    """
    Combine many-to-one read contracts with table lifecycle and legacy update hooks.

    Concrete backends implement persistence and cache repair. Ordering/type
    support comes from the table schema and flags, not merely this class name.
    This class adds no concrete cardinality initialization or update pipeline.

    Example:
        A concrete implementation can write database links before refreshing
        its cached records, with transaction ownership managed by its caller.
    """

    @abc.abstractmethod
    def update(
        self,
        update: "ManyOneInterlinkTableUpdate",
    ) -> "ManyOneInterLinkTableUpdateResults":
        """
        Apply a link update through a concrete backend's full update pipeline.

        This abstract method does not order or execute hooks. Existing schema and
        NumPy backends call preflight, precheck, database writes, then cache refresh;
        return annotations alone do not enforce the concrete result shape.

        Example:
            A post-write cache-refresh error can occur after persistence; this API
            does not supply rollback.


        :param update: Cardinality-specific update payload interpreted by the backend.
        :return: Backend update result following its implementation-specific contract.
        """

    @abc.abstractmethod
    def update_preflight(
        self,
        update: "ManyOneInterlinkTableUpdate",
    ) -> "ManyOneInterlinkTableUpdate":
        """
        Prepare a link update for later validation and writes.

        Abstract contract; pass-through implementations may retain object identity.

        Example:
            A backend can return the input unchanged when it needs no normalization.


        :param update: Cardinality-specific update payload interpreted by the backend.
        :return: Normalized or original update object chosen by the backend.
        """

    @abc.abstractmethod
    def update_precheck(
        self,
        update: "ManyOneInterlinkTableUpdate",
    ) -> bool:
        """
        Check a prepared link update according to backend rules.

        Abstract contract. Whether a false result stops update is decided by the
        concrete pipeline; callers cannot infer that behavior from this signature.

        Example:
            An implementation whose update ignores hook booleans must raise to stop writes.


        :param update: Cardinality-specific update payload interpreted by the backend.
        :return: Boolean precheck result under the concrete backend's convention.
        """

    @abc.abstractmethod
    def update_db(
        self,
        update: "ManyOneInterlinkTableUpdate",
    ) -> bool:
        """
        Apply prepared link intentions to persistent storage.

        Abstract contract. Validation, transaction ownership and partial-write
        recovery are not implemented here. Cache refresh is a separate hook.

        Example:
            A link insert can persist before a later cache refresh runs.


        :param update: Cardinality-specific update payload interpreted by the backend.
        :return: Boolean result under the concrete backend's convention.
        """

    @abc.abstractmethod
    def update_cache(
        self,
        update: "ManyOneInterlinkTableUpdate",
    ) -> bool:
        """
        Reconcile cached link state after the prepared update.

        Abstract contract. Backends may reload the entire link table or repair
        selected state; this hook does not prescribe rollback of database changes.

        Example:
            A schema link cache reloads all physical link records after mutation.


        :param update: Cardinality-specific update payload interpreted by the backend.
        :return: Boolean result under the concrete backend's convention.
        """
