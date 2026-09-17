"""
Define source-keyed many-to-one field projections and legacy update contracts.

Values come from a destination column reached through a directed link
table. Concrete backends supply reads, value matching and update behavior;
this module provides endpoint binding, names and compatibility forwarding.
"""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Optional, Sequence, TypeVar, Union

from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.base_field_api import (
    RelationFieldBasicInterfaceAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.util_mixins import (
    IndividualLinkProperties,
)
from LiuXin_alpha.caches.updates.field_updates import ManyOneInTwoTableFieldUpdate

if TYPE_CHECKING:
    from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.many_one_tables_api import (
        StorageCacheManyToOneLinkTable,
    )
    from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.single_table_api import (
        StorageCacheSingleTableAPI,
    )
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.db_types import (
        InterlinkExtraTypes,
        MainTableColumnName,
        MainTableID,
        MainTableName,
    )

T = TypeVar("T")


class ManyToOneFieldAPI(RelationFieldBasicInterfaceAPI[T]):
    """
    Expose many-to-one destination values through source-keyed field access.

    Each source projects at most one destination value under the declared shape.
    Destinations may be shared by several sources.
    Concrete implementations choose storage, refresh and malformed-link handling;
    this API does not enforce cardinality, normalize values or own transactions.

    Example:
        >>> ManyToOneFieldAPI.field_storage_shape, ManyToOneFieldAPI.deletes_owner_rows
        ('relation', False)
    """

    # Many-to-one fields have many entries in one table and one entry in another.
    src_table: "StorageCacheSingleTableAPI"
    dst_table: "StorageCacheSingleTableAPI"

    # We key by a src id column and cache values from this dst column.
    src_table_id_col: MainTableColumnName
    dst_table_cache_col: MainTableColumnName

    # Connecting the two tables.
    link_table: "StorageCacheManyToOneLinkTable"

    _db: "DatabaseAPI"

    def __init__(
        self,
        src_table: Union["StorageCacheSingleTableAPI", MainTableName],
        src_table_id_col: MainTableColumnName,
        dst_table: Union["StorageCacheSingleTableAPI", MainTableName],
        dst_table_cache_col: MainTableColumnName,
        db: "DatabaseAPI",
    ) -> None:
        """
        Resolve source, destination and link objects before retaining field metadata.

        Call get_main_table for source then destination, then get_link_table.
        Only afterward assign column names and _db; lookup failures can leave
        partially assigned endpoint state. Concrete subclasses supply these hooks.

        Example:
            A subclass establishes its owning root cache before this constructor
            uses the table-resolution hooks.


        :param src_table: Source table name or API reference.
        :param src_table_id_col: Source identity column retained unchanged.
        :param dst_table: Destination table name or API reference.
        :param dst_table_cache_col: Destination column whose values will be projected.
        :param db: Database reference retained after endpoint/link resolution.
        :return: None; binds the route without invoking read or creating projection storage.
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
        src_table: Union["StorageCacheSingleTableAPI", MainTableName],
        dst_table: Union["StorageCacheSingleTableAPI", MainTableName],
    ) -> "StorageCacheManyToOneLinkTable":
        """
        Resolve the many-to-one link cache connecting the endpoints.

        Abstract contract; concrete implementations validate route availability
        and cardinality according to their backend.

        Example:
            A many-to-many tags field requires a route that permits shared destinations.


        :param src_table: Source table name or table API reference.
        :param dst_table: Destination table name or table API reference.
        :return: Directed link object with the required cardinality API.
        """

    @abc.abstractmethod
    def update(self, update: ManyOneInTwoTableFieldUpdate[T]) -> None:
        """
        Apply field values and relationships through the concrete many-to-one backend.

        Abstract contract. Deleted IDs clear field mappings rather than delete
        source rows. Creation policy, validation order, transaction handling and
        related-row cleanup belong to the implementation and update payload.

        Example:
            A source deletion request can unlink its relation while retaining the owner row.


        :param update: Cardinality-specific update containing source-keyed value/link intentions.
        :return: None; the backend applies its supported changes and refresh policy.
        """

    @property
    def src_table_name(self) -> MainTableName:
        """
        Expose the bound source table name.

        Example:
            A books-to-tags projection exposes books through this property.


        :return: The src_table.table value without additional resolution.
        """
        return self.src_table.table

    @property
    def dst_table_name(self) -> MainTableName:
        """
        Expose the bound destination table name.

        Example:
            A books-to-tags projection exposes tags through this property.


        :return: The dst_table.table value without additional resolution.
        """
        return self.dst_table.table

    @property
    @abc.abstractmethod
    def ids(self) -> set[MainTableID]:
        """
        Expose source identities known to this field.

        Abstract contract. Snapshot freshness and copying versus shared objects
        are defined by the concrete backend.

        Example:
            A snapshot relation can omit a source that has no projected links.


        :return: Set of cached source IDs; coverage of unlinked sources is backend-specific.
        """

    @property
    @abc.abstractmethod
    def values(self) -> list[T]:
        """
        Expose all values represented by the field projection.

        Abstract contract. Snapshot freshness and copying versus shared objects
        are defined by the concrete backend.

        Example:
            Several sources can contribute the same value to this list.


        :return: List of values; order and duplicate retention follow the backend.
        """

    @property
    @abc.abstractmethod
    def values_set(self) -> set[T]:
        """
        Expose distinct values represented by this field.

        Abstract contract. Snapshot freshness and copying versus shared objects
        are defined by the concrete backend.

        Example:
            Repeated projected strings can contribute one distinct value.


        :return: Set of projected values using the backend's supported value semantics.
        """

    @property
    @abc.abstractmethod
    def ids_values_map(self) -> dict[MainTableID, Optional[T]]:
        """
        Expose source identities mapped to projected scalar values.

        Abstract contract. Snapshot freshness and copying versus shared objects
        are defined by the concrete backend.

        Example:
            An unlinked source need not have an entry in the mapping.


        :return: Mapping from source IDs to optional destination values.
        """

    @property
    @abc.abstractmethod
    def dst_ids_values_map(self) -> dict[MainTableID, Optional[T]]:
        """
        Expose known destination identities and their projected values.

        Abstract contract. Snapshot freshness and copying versus shared objects
        are defined by the concrete backend.

        Example:
            Several sources sharing one destination can refer to one destination-value entry.


        :return: Mapping from destination IDs to optional values.
        """

    @abc.abstractmethod
    def get_value_from_src_id(self, src_id: MainTableID) -> Optional[T]:
        """
        Read the optional projected value for one source identity.

        Abstract contract. Concrete backends decide how malformed multiple links
        are handled; this declaration adds no singularity check.

        Example:
            An unlinked source can return None without deleting its owner row.


        :param src_id: Source row identity.
        :return: Destination value or None according to backend missing/null semantics.
        """

    def get_value_from_id(self, table_id: MainTableID) -> Optional[T]:
        """
        Forward the compatibility owner-ID spelling to scalar source lookup.

        Example:
            get_value_from_id(7) delegates directly to get_value_from_src_id(7).


        :param table_id: Owner identity forwarded unchanged as the source ID.
        :return: The get_value_from_src_id result without copying or default substitution.
        """
        return self.get_value_from_src_id(table_id)

    @abc.abstractmethod
    def get_value_from_dst_id(self, dst_id: MainTableID) -> Optional[T]:
        """
        Read a projected column value by destination identity.

        Abstract contract; whether lookup requires link membership is defined
        by the implementation rather than this declaration.

        Example:
            A backend can expose a cached tag name directly by its tag ID.


        :param dst_id: Destination row identity.
        :return: Destination value or None according to backend missing/null semantics.
        """

    @abc.abstractmethod
    def get_dst_id_from_src_id(
        self,
        src_id: MainTableID,
        type_filter: Optional[str] = None,
    ) -> Optional[MainTableID]:
        """
        Resolve one optional linked dst identity.

        Abstract contract. Endpoint existence checks, ordering and malformed-link
        singularity behavior belong to the concrete backend.

        Example:
            An unlinked endpoint can yield None.


        :param src_id: Source row identity.
        :param type_filter: Optional link-type restriction interpreted by the backend.
        :return: Linked endpoint identity, or None when no accepted link exists.
        """

    @abc.abstractmethod
    def get_src_ids_from_dst_id(
        self,
        dst_id: MainTableID,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[MainTableID]:
        """
        Resolve linked src identities.

        Abstract contract. Endpoint existence checks, ordering and malformed-link
        singularity behavior belong to the concrete backend.

        Example:
            Several links can yield several endpoint IDs for this lookup.


        :param dst_id: Destination row identity.
        :param require_ordering: Request relation ordering supported by the concrete backend.
        :param type_filter: Optional link-type restriction interpreted by the backend.
        :return: Sequence of endpoint identities; repeated physical links may remain repeated.
        """

    @abc.abstractmethod
    def get_src_ids_from_value(self, value: T) -> list[MainTableID]:
        """
        Find source identities whose projected destination values match.

        Abstract contract. Hashability requirements, normalization and result
        ordering depend on the concrete field.

        Example:
            Two destinations with the same value can both contribute matching IDs.


        :param value: Value interpreted by the backend's matching/index rules.
        :return: List of matching identities; uniqueness is not guaranteed.
        """

    @abc.abstractmethod
    def get_dst_ids_from_value(self, value: T) -> list[MainTableID]:
        """
        Find destination identities whose projected destination values match.

        Abstract contract. Hashability requirements, normalization and result
        ordering depend on the concrete field.

        Example:
            Two destinations with the same value can both contribute matching IDs.


        :param value: Value interpreted by the backend's matching/index rules.
        :return: List of matching identities; uniqueness is not guaranteed.
        """

    @abc.abstractmethod
    def get_link_properties(
        self,
        src_id: MainTableID,
        dst_id: MainTableID,
    ) -> IndividualLinkProperties:
        """
        Read association properties for a directed endpoint pair.

        Abstract contract. Unsupported columns and ambiguous physical pairs
        are handled by the concrete implementation.

        Example:
            Read priority or type metadata for a book-to-tag association.


        :param src_id: Source row identity.
        :param dst_id: Destination row identity.
        :return: IndividualLinkProperties value populated by the backend.
        """

    @abc.abstractmethod
    def set_link_properties(
        self,
        updated_link_properties: IndividualLinkProperties,
    ) -> None:
        """
        Apply supplied association properties through the field backend.

        Abstract contract. None handling and supported columns vary by backend;
        this interface adds no validation or transaction.

        Example:
            A supported priority change can alter the order of projected relation values.


        :param updated_link_properties: Property object carrying endpoint identities and optional metadata values.
        :return: None; the backend writes supported properties and updates its cache as needed.
        """

    @abc.abstractmethod
    def get_extra(
        self,
        src_id: MainTableID,
        dst_id: MainTableID,
        extra_type: InterlinkExtraTypes,
    ) -> Optional[str | bool | int]:
        """
        Read one logical property from an association.

        Abstract contract; unsupported selectors and absent pairs are resolved
        by the implementation.

        Example:
            Read an origin property when the link schema exposes it.


        :param src_id: Source row identity.
        :param dst_id: Destination row identity.
        :param extra_type: Logical link-property selector supported by the backend.
        :return: Optional string, boolean or integer property value under the backend contract.
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
        Write one logical property on an association.

        Abstract contract. A concrete setter can interpret None as an explicit
        clear, distinct from a bulk property update that ignores None.

        Example:
            A schema-backed field can clear its supported origin property with None.


        :param src_id: Source row identity.
        :param dst_id: Destination row identity.
        :param extra_type: Logical link-property selector supported by the backend.
        :param new_extra_value: New optional property value interpreted by the concrete setter.
        :return: None; the backend writes and refreshes supported association state.
        """
