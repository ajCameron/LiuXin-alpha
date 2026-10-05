"""
Project cached destination values through schema-backed many-to-one relations.

Relation getters use cached endpoint/link objects; bulk mappings use the
last rebuilt field projection. Legacy update helpers write through the database
and refresh affected caches without adding a transaction boundary.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Sequence, Union, cast

from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.many_one_field_api import (
    IndividualLinkProperties,
    ManyOneInTwoTableFieldUpdate,
    ManyToOneFieldAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.single_table_api import (
    StorageCacheSingleTableAPI,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.common import _ensure_db
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.relation_base import (
    _SchemaBackedRelationFieldBase,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.link_tables.link_table import (
    SchemaBackedLinkTable,
)

if TYPE_CHECKING:
    from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_cache import (
        SchemaBackedStorageCache,
    )


class SchemaBackedManyOneField(
    _SchemaBackedRelationFieldBase[Any],
    ManyToOneFieldAPI[Any],
):
    """
    Expose many-to-one destination values and legacy relation updates.

    Construction resolves the endpoint tables and required directed link type.
    read builds field dictionaries from those snapshots. Bulk mappings need a
    field refresh after underlying data changes; direct endpoint/link getters
    can observe the newer table caches before that refresh.
    Scalar source results select the first projected value without enforcing
    singularity on that read path.

    Example:
        >>> issubclass(SchemaBackedManyOneField, ManyToOneFieldAPI)
        True
    """

    def __init__(
        self,
        cache: "SchemaBackedStorageCache",
        src_table: Union[StorageCacheSingleTableAPI, str],
        src_table_id_col: str,
        dst_table: Union[StorageCacheSingleTableAPI, str],
        dst_table_cache_col: str,
        db: Any,
    ) -> None:
        """
        Resolve endpoint/link caches and initialize empty many-to-one projections.

        Initialize empty relation dictionaries before the API constructor resolves
        source, destination and link objects. Root lookups can refresh stale tables
        and reject unknown routes or mismatched cardinality.

        Example:
            Root initialize_fields calls read after construction to populate the
            new field from initialized table caches.


        :param cache: Owning schema-backed cache used for table and link resolution.
        :param src_table: Source table name or table API reference.
        :param src_table_id_col: Source identity column retained by the relation API.
        :param dst_table: Destination table name or table API reference.
        :param dst_table_cache_col: Destination column to project.
        :param db: Database reference retained for later reads and writes.
        :return: None; establishes the route without building field value dictionaries.
        """

        self._init_relation_field(cache, db)
        ManyToOneFieldAPI.__init__(
            self,
            src_table=src_table,
            src_table_id_col=src_table_id_col,
            dst_table=dst_table,
            dst_table_cache_col=dst_table_cache_col,
            db=db,
        )

    def get_link_table(
        self,
        src_table: Union[StorageCacheSingleTableAPI, str],
        dst_table: Union[StorageCacheSingleTableAPI, str],
    ) -> SchemaBackedLinkTable:
        """
        Resolve the many-to-one directed link view through the owning cache.

        Delegate to get_many_one_link_table; cast supplies a type annotation
        without adding runtime validation.

        Example:
            A route with another cardinality is rejected by the root accessor.


        :param src_table: Source table name or table API reference.
        :param dst_table: Destination table name or table API reference.
        :return: Schema-backed link object after root freshness and cardinality checks.
        """

        return cast(SchemaBackedLinkTable, self._cache.get_many_one_link_table(src_table, dst_table))

    def update(self, update: ManyOneInTwoTableFieldUpdate[Any]) -> None:
        """
        Apply scalar relation updates with explicit unlink and missing-link creation policy.

        Require an attached database and validate creation-policy flags. Merge
        added_maps then updated_maps (updated values win identical keys), normalize
        IDs, and collect deleted_ids plus None-valued updates as source unlink requests.
        For each non-None value, update an existing linked destination in place, or
        resolve/create a destination when both the relevant creation flags permit it.
        New destination rows may be created before a later missing-link error is
        reported; no encompassing transaction or rollback is added.

        After validation, unlink selected sources, create planned links, write
        destination values and rebind if anything changed or dirtied is truthy.
        Combining a non-None update with an explicit deletion does not globally cancel
        that update: it can still update a former destination or create a planned
        link after unlinking. Multiple updates to the same destination keep the last
        collected value. A failure can follow earlier durable work.

        Exact-value destinations can be reused across sources. Updating an already
        linked shared destination changes the value observed by its other sources
        after refresh; this path does not retarget the source merely because another
        destination already has the desired value.

        Example:
            A None-valued update unlinks its source while retaining the destination
            row; a non-None update to an existing link writes that destination value.


        :param update: Relation update carrying added/updated maps, deleted IDs, creation flags and dirtied.
        :return: None; updates destination values/links and conditionally rebuilds projection state.
        :raises KeyError: Required linked destinations are unavailable under the creation policy.
        :raises ValueError: Creation flags conflict, exact matches are ambiguous, or ownership checks fail.
        """

        self._db = _ensure_db(self._db)
        create_missing_links = bool(update.create_missing_links)
        create_missing_related_rows = bool(update.create_missing_related_rows)
        self._validate_create_policy(
            create_missing_links=create_missing_links,
            create_missing_related_rows=create_missing_related_rows,
        )

        raw_updates = {
            int(src_id): value
            for src_id, value in {
                **dict(update.added_maps),
                **dict(update.updated_maps),
            }.items()
        }

        deleted_src_ids = {
            int(src_id)
            for src_id in update.deleted_ids
        } | {
            int(src_id)
            for src_id, value in raw_updates.items()
            if value is None
        }

        missing_link_src_ids: list[int] = []
        dst_updates: dict[int, Any] = {}
        links_to_create: dict[int, int] = {}

        for src_id, value in raw_updates.items():
            if value is None:
                continue

            existing_dst_id = cast(Any, self.link_table).get_dst_id(int(src_id))
            if existing_dst_id is not None:
                dst_updates[int(existing_dst_id)] = value
                continue

            if not create_missing_links:
                missing_link_src_ids.append(int(src_id))
                continue

            dst_id = self._get_unique_dst_id_for_value(value)
            if dst_id is None:
                if not create_missing_related_rows:
                    missing_link_src_ids.append(int(src_id))
                    continue
                dst_id = self._create_related_dst_row(value)

            links_to_create[int(src_id)] = int(dst_id)

        if missing_link_src_ids:
            raise KeyError(
                f"Field {self.field_key!r} cannot update missing linked rows for src ids: {sorted(missing_link_src_ids)}"
            )

        if deleted_src_ids:
            self._unlink_src_ids(deleted_src_ids)

        for src_id, dst_id in links_to_create.items():
            self._create_link(src_id, dst_id)

        if dst_updates:
            self._update_dst_values(dst_updates)

        if deleted_src_ids or links_to_create or dst_updates or update.dirtied:
            self.read(self._db)

    @property
    def ids(self) -> set[int]:
        """
        Copy source IDs present in the stored field projection.

        Example:
            A source row with no links contributes no projection ID.


        :return: New set of keys from _src_to_values; unlinked owners are normally omitted.
        """

        return set(self._src_to_values.keys())

    @property
    def values(self) -> list[Any]:
        """
        Flatten stored field values in ascending source-ID order.

        This reads _src_to_values without refreshing destination or link data.
        Mutable values can remain shared with the stored projection.

        Example:
            Sources projecting ("A", "B") and ("B",) contribute ["A", "B", "B"].


        :return: New list retaining link order within each source, duplicates and None.
        """

        return self._flattened_values()

    @property
    def values_set(self) -> set[Any]:
        """
        Collect distinct values from the stored field projection.

        Example:
            Repeated links projecting "tag" contribute one set entry.


        :return: Set using Python hash/equality, including None when present.
        :raises TypeError: A projected value is unhashable.
        """

        return set(self.values)

    @property
    def ids_values_map(self) -> dict[int, Optional[Any]]:
        """
        Copy each stored source projection as its first value.

        Read _src_to_values without table refresh or type filtering. Only stored
        source keys are included; returned mutable elements are not deep-copied.

        Example:
            An unlinked source is omitted from the mapping.


        :return: New dict of source IDs to first values, with None for empty groups.
        """

        return {
            src_id: values[0] if values else None
            for src_id, values in self._src_to_values.items()
        }

    @property
    def dst_ids_values_map(self) -> dict[int, Optional[Any]]:
        """
        Copy the stored destination-value projection.

        This does not query the destination table. A full field read includes
        linked destinations from existing sources, including dangling IDs with None.
        Bounded destination repair can add an unlinked changed row or remove a
        deleted destination from this dictionary.

        Example:
            Refreshing an unlinked destination ID can add its value here without
            adding any source-to-destination link.


        :return: New dict of cached destination IDs to projected values; elements are shared.
        """

        return dict(self._dst_to_values)

    def get_value_from_src_id(self, src_id: int) -> Optional[Any]:
        """
        Project the first value through current cached endpoint and link records.

        Build the full accepted value tuple and select its first element.
        Multiple physical links do not trigger a singularity error here.

        Example:
            A source projecting (None, "later") returns None.


        :param src_id: Source ID converted with int.
        :return: First projected value, or None for an absent source, no links or a null first value.
        """

        return self._single_value_for_src_id(int(src_id))

    def get_value_from_dst_id(self, dst_id: int) -> Optional[Any]:
        """
        Read a value directly from a cached destination row.

        No link membership is required. This uses the current endpoint snapshot
        and can differ from the stored destination-value dictionary until refresh.

        Example:
            A cached destination with no links can still be read by its ID.


        :param dst_id: Destination ID converted with int.
        :return: Value from a row snapshot, or None for an absent row or missing column.
        """

        return self._value_for_dst_id(int(dst_id))

    def get_dst_id_from_src_id(
        self,
        src_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[int]:
        """
        Select the first destination from current cached link records.

        This first-item selection adds no singularity or endpoint-existence check.

        Example:
            Malformed links to IDs 7 and 8 select the first accepted ID.


        :param src_id: Source ID converted with int.
        :param type_filter: Optional exact link-type filter.
        :return: First destination ID, or None when no accepted link exists.
        """

        dst_ids = self._ordered_dst_ids_for_src(int(src_id), type_filter=type_filter)
        return dst_ids[0] if dst_ids else None

    def get_src_ids_from_dst_id(
        self,
        dst_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[int]:
        """
        Read reverse source IDs directly from cached link records.

        No endpoint membership check or field-dictionary refresh is performed.
        Priority-enabled views sort by descending priority regardless of the hint.

        Example:
            Repeated physical links can produce the same source ID more than once.


        :param dst_id: Destination ID converted with int.
        :param require_ordering: Ordering hint forwarded to link selection.
        :param type_filter: Optional exact link-type filter.
        :return: Tuple of source IDs in link order, retaining duplicates.
        """

        return self._ordered_src_ids_for_dst(
            int(dst_id),
            require_ordering=require_ordering,
            type_filter=type_filter,
        )

    def get_src_ids_from_value(self, value: Any) -> list[int]:
        """
        Find source IDs by equality against stored destination values.

        Scan _dst_to_values without refreshing tables or normalizing text.
        Expand matching destinations through _dst_to_src_ids; repeated source
        entries are retained.

        Example:
            A source linked to two matching destinations appears twice.


        :param value: Value compared with == to each stored destination value; no hashing is required.
        :return: Sorted source-ID list, retaining repetitions across matching destinations.
        """

        src_ids: list[int] = []
        for dst_id, dst_value in sorted(self._dst_to_values.items()):
            if dst_value == value:
                src_ids.extend(self._dst_to_src_ids.get(dst_id, ()))
        return sorted(src_ids)

    def get_dst_ids_from_value(self, value: Any) -> list[int]:
        """
        Find destination IDs by equality against stored destination values.

        Scan _dst_to_values without refreshing tables or normalizing text.
        Bounded repair may have added an unlinked destination to this dictionary;
        this lookup does not separately require reverse-link membership.

        Example:
            An unlinked destination inserted by bounded repair can appear in this result.


        :param value: Value compared with == to each stored destination value; no hashing is required.
        :return: Sorted list of matching destination IDs from the stored value dictionary.
        """

        return sorted(
            dst_id
            for dst_id, dst_value in self._dst_to_values.items()
            if dst_value == value
        )

    def get_link_properties(
        self,
        src_id: int,
        dst_id: int,
    ) -> IndividualLinkProperties:
        """
        Build many-to-one link metadata from a unique cached pair Row.

        Fetch a singular pair Row without a type filter. Unsupported/absent
        properties become None; casts do not coerce property values.

        Example:
            Two typed physical links for one pair can make this metadata lookup
            ambiguous even though filtered link traversal is available elsewhere.


        :param src_id: Source identity converted with int.
        :param dst_id: Destination identity converted with int.
        :return: IndividualLinkProperties value populated with endpoints and supported logical properties.
        :raises KeyError: The pair has no cached Row.
        :raises RuntimeError: The pair has multiple cached link records.
        """

        return cast(
            IndividualLinkProperties,
            self._build_link_properties(IndividualLinkProperties, int(src_id), int(dst_id)),
        )

    def set_link_properties(
        self,
        updated_link_properties: IndividualLinkProperties,
    ) -> None:
        """
        Write supported non-None many-to-one link properties and rebind the field.

        Delegate to the common setter. Table-name labels are not validated, None
        properties are skipped, and false/zero values are retained. Later refresh
        failures can follow successful writes; no enclosing transaction is added.

        Example:
            Use set_extra when a supported property must explicitly be cleared to
            None rather than skipped by this bulk setter.


        :param updated_link_properties: Property object whose endpoint IDs select the pair and whose values supply updates.
        :return: None; writes selected columns, refreshes links and reads the relation projection.
        """

        self._set_link_properties(updated_link_properties)


__all__ = ["SchemaBackedManyOneField"]
