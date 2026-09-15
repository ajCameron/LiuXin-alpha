"""
Define same-table scalars, two-table one-to-one fields and unique lookup contracts.

The relation update dataclass carries mutable source-keyed intentions and
creation flags without validation. Concrete backends implement persistence,
null handling, refresh and uniqueness checks; these APIs preserve owner-row
lifecycle rather than deleting owners when field values are cleared.
"""
from __future__ import annotations

import abc
import dataclasses

from typing import TYPE_CHECKING, Union, TypeVar, Generic, Optional

from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.base_field_api import (
    RelationFieldBasicInterfaceAPI,
    ScalarFieldBasicInterfaceAPI,
)

from LiuXin_alpha.caches.updates.field_updates import OneOneInOneTableFieldUpdate

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
    from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.one_one_tables_api import (
        StorageCacheOneToOneLinkTableAPI,
    )
    from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.single_table_api import (
        StorageStorageCacheSingleTableAPI,
    )
    from LiuXin_alpha.databases.db_types import (
        MainTableColumnName,
        MainTableID,
        MainTableName,
    )

T = TypeVar("T")


class CacheOneOneInSameTableFieldAPI(ScalarFieldBasicInterfaceAPI[T]):
    """
    Represent a scalar value stored directly on one owner table.

    Resolve the owner table in the constructor. Concrete fields supply value
    storage, reads and updates; clearing a value must retain its owner row.
    The inherited scalar shape declares no link or related-row mutations.

    Example:
        >>> CacheOneOneInSameTableFieldAPI.field_storage_shape
        'scalar'
    """

    # The table the column is in.
    in_table: "StorageStorageCacheSingleTableAPI"

    _table_id_col: "MainTableName"
    _table_cached_col: "MainTableName"

    _db: "DatabaseAPI"

    def __init__(
        self,
        in_table: Union["StorageStorageCacheSingleTableAPI", "MainTableName"],
        db: "DatabaseAPI",
    ) -> None:
        """
        Resolve the owner table before retaining the database reference.

        Example:
            A concrete scalar field sets its owner cache before calling this constructor.


        :param in_table: Owner table name or API reference passed to get_main_table.
        :param db: Database reference retained after table resolution.
        :return: None; attaches the table without loading or allocating field values.
        """
        self.in_table = self.get_main_table(in_table)
        self._db = db

    @abc.abstractmethod
    def update(self, update: OneOneInOneTableFieldUpdate[T]) -> None:
        """
        Apply scalar field-value mutations while preserving owner rows.

        Abstract contract. Deleted IDs express clearing values, subject to column
        constraints; they must not implicitly delete the table rows.

        Example:
            Clearing a nullable title writes None while the book row remains.


        :param update: Same-table update containing value maps, cleared IDs and dirty IDs.
        :return: None; the concrete backend validates, writes and refreshes field values.
        """

    @property
    def table_name(self) -> MainTableName:
        """
        Expose the name of the resolved owner table.

        Example:
            A title field bound to books returns books.


        :return: The in_table.table value without another table lookup.
        """
        return self.in_table.table

    @property
    @abc.abstractmethod
    def ids(self) -> set[MainTableID]:
        """
        Expose owner identities known to the scalar field.

        Abstract contract; this property does not prescribe a database refresh.

        Example:
            A loaded field can expose IDs {1, 2} for two stored owner values.


        :return: Set of cached owner IDs, with coverage determined by the backend.
        """

    @property
    @abc.abstractmethod
    def values(self) -> list[T]:
        """
        Expose scalar values represented by this field.

        Abstract contract; callers must not infer unique values from this list.

        Example:
            Two owners may contribute the same title twice.


        :return: List of values; ordering and duplicate retention follow the backend.
        """

    @property
    @abc.abstractmethod
    def values_set(self) -> set[T]:
        """
        Expose distinct scalar values represented by this field.

        Abstract contract; support for unhashable stored values is backend-specific.

        Example:
            Repeated title strings contribute one distinct title.


        :return: Set of known values using supported backend equality/hash semantics.
        """

    @property
    @abc.abstractmethod
    def ids_values_map(self) -> dict[MainTableID, Optional[T]]:
        """
        Expose known owners mapped to optional scalar values.

        Abstract contract. Copy ownership and whether deleted IDs have been
        evicted depend on the backend's refresh state.

        Example:
            An owner with a stored null title can map to None.


        :return: Dictionary of owner IDs to stored values or None.
        """

    @abc.abstractmethod
    def get_value_from_id(self, table_id: MainTableID) -> Optional[T]:
        """
        Read the optional scalar value for an owner identity.

        Abstract contract; this method adds no application-level default value.

        Example:
            An absent owner and a stored null may both return None.


        :param table_id: Owner row identity interpreted by the field backend.
        :return: Stored value or None according to backend missing/null semantics.
        """

    @abc.abstractmethod
    def get_ids_from_value(self, value: T) -> list[MainTableID]:
        """
        Find owners whose scalar values match a query.

        Abstract contract. Text normalization and result ordering are not fixed
        by this interface.

        Example:
            A non-unique title can match several owner IDs.


        :param value: Value interpreted by backend matching/index rules.
        :return: List of matching owner identities; uniqueness is not guaranteed.
        """


class CacheOneOneInSameTableFieldUniqueAPI(CacheOneOneInSameTableFieldAPI[T]):
    """
    Specialize same-table scalar fields with a unique-value owner lookup.

    The extra abstract getter expresses uniqueness but does not add a schema
    constraint or validate stored data itself.

    Example:
        A unique identifier value can resolve one owner ID or no match.
    """

    @abc.abstractmethod
    def get_id_from_value(self, value: T) -> Optional[MainTableID]:
        """
        Resolve an owner identity from a declared-unique scalar value.

        Abstract contract; violations of the declared uniqueness are handled
        by the implementation.

        Example:
            Look up the owner of one unique identifier value.


        :param value: Value matched by the concrete unique-value index.
        :return: Matching owner identity, or None when no match exists.
        """


@dataclasses.dataclass
class OneOneInTwoTableFieldUpdate(Generic[T]):
    """
    Carry mutable source-keyed intentions for a one-to-one relation update.

    Required table/column labels describe the destination projection.
    added_maps and updated_maps carry optional destination values; deleted_ids
    request clearing/unlinking source mappings and dirtied supplies refresh IDs.
    unique and both creation flags default false. Related-row creation requires
    link creation under the intended policy, but this dataclass performs no
    validation, copying or database work; consumers enforce their own rules.

    Example:
        >>> change = OneOneInTwoTableFieldUpdate("books", "covers", "path", {}, {}, {1}, set())
        >>> change.deleted_ids, change.create_missing_links
        ({1}, False)
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


class CacheOneOneInTwoTableFieldAPI(RelationFieldBasicInterfaceAPI[T]):
    """
    Expose one-to-one destination values through source-keyed field access.

    Each source projects at most one destination value under the declared shape.
    Each destination belongs to at most one source under the declared shape.
    Concrete implementations choose storage, refresh and malformed-link handling;
    this API does not enforce cardinality, normalize values or own transactions.

    Example:
        >>> CacheOneOneInTwoTableFieldAPI.field_storage_shape, CacheOneOneInTwoTableFieldAPI.deletes_owner_rows
        ('relation', False)
    """

    src_table: "StorageStorageCacheSingleTableAPI"
    dst_table: "StorageStorageCacheSingleTableAPI"

    # We identify the src row by this column and cache the value from this dst column.
    src_table_id_col: MainTableColumnName
    dst_table_cache_col: MainTableColumnName

    # Connecting the two tables.
    link_table: "StorageCacheOneToOneLinkTableAPI"

    _db: "DatabaseAPI"

    def __init__(
        self,
        src_table: Union["StorageStorageCacheSingleTableAPI", "MainTableName"],
        src_table_id_col: MainTableColumnName,
        dst_table: Union["StorageStorageCacheSingleTableAPI", "MainTableName"],
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
        src_table: Union["StorageStorageCacheSingleTableAPI", MainTableName],
        dst_table: Union["StorageStorageCacheSingleTableAPI", MainTableName],
    ) -> "StorageCacheOneToOneLinkTableAPI":
        """
        Resolve the one-to-one link cache connecting the endpoints.

        Abstract contract; concrete implementations validate route availability
        and cardinality according to their backend.

        Example:
            A many-to-many tags field requires a route that permits shared destinations.


        :param src_table: Source table name or table API reference.
        :param dst_table: Destination table name or table API reference.
        :return: Directed link object with the required cardinality API.
        """

    @abc.abstractmethod
    def update(self, update: OneOneInTwoTableFieldUpdate[T]) -> None:
        """
        Apply field values and relationships through the concrete one-to-one backend.

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
    def get_dst_id_from_src_id(self, src_id: MainTableID) -> Optional[MainTableID]:
        """
        Resolve one optional linked dst identity.

        Abstract contract. Endpoint existence checks, ordering and malformed-link
        singularity behavior belong to the concrete backend.

        Example:
            An unlinked endpoint can yield None.


        :param src_id: Source row identity.
        :return: Linked endpoint identity, or None when no accepted link exists.
        """

    @abc.abstractmethod
    def get_src_id_from_dst_id(self, dst_id: MainTableID) -> Optional[MainTableID]:
        """
        Resolve one optional linked src identity.

        Abstract contract. Endpoint existence checks, ordering and malformed-link
        singularity behavior belong to the concrete backend.

        Example:
            An unlinked endpoint can yield None.


        :param dst_id: Destination row identity.
        :return: Linked endpoint identity, or None when no accepted link exists.
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


class CacheOneOneInTwoTableFieldUniqueAPI(CacheOneOneInTwoTableFieldAPI[T]):
    """
    Specialize one-to-one relation fields with unique-value endpoint lookups.

    Concrete implementations provide source and destination lookup.
    This class does not enforce database uniqueness or implement either index.

    Example:
        A unique linked cover path can identify both its cover row and owning book.
    """

    @abc.abstractmethod
    def get_src_id_from_unique_value(self, value: T) -> Optional[MainTableID]:
        """
        Resolve the source identity for a declared-unique projected value.

        Abstract contract; uniqueness validation and errors belong to the backend.

        Example:
            A unique cover path can resolve its book owner identity.


        :param value: Destination-column value matched under the backend's uniqueness rules.
        :return: Matching source identity, or None for no match.
        """

    @abc.abstractmethod
    def get_dst_id_from_unique_value(self, value: T) -> Optional[MainTableID]:
        """
        Resolve the destination identity for a declared-unique projected value.

        Abstract contract; uniqueness validation and errors belong to the backend.

        Example:
            A unique cover path can resolve its cover row identity.


        :param value: Destination-column value matched under the backend's uniqueness rules.
        :return: Matching destination identity, or None for no match.
        """
