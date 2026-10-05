"""
Project cached destination values through schema-backed one-to-many relations.

Relation getters use cached endpoint/link objects; bulk mappings use the
last rebuilt field projection. Legacy update helpers write through the database
and refresh affected caches without adding a transaction boundary.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Sequence, Union, cast

from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.one_many_field_api import (
    IndividualLinkProperties,
    OneManyInTwoTableFieldUpdate,
    OneToManyFieldAPI,
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


class SchemaBackedOneManyField(
    _SchemaBackedRelationFieldBase[Any],
    OneToManyFieldAPI[Any],
):
    """
    Expose one-to-many destination values and legacy relation updates.

    Construction resolves the endpoint tables and required directed link type.
    read builds field dictionaries from those snapshots. Bulk mappings need a
    field refresh after underlying data changes; direct endpoint/link getters
    can observe the newer table caches before that refresh.
    Plural source results retain repeated physical links.

    Example:
        >>> issubclass(SchemaBackedOneManyField, OneToManyFieldAPI)
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
        Resolve endpoint/link caches and initialize empty one-to-many projections.

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
        OneToManyFieldAPI.__init__(
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
        Resolve the one-to-many directed link view through the owning cache.

        Delegate to get_one_many_link_table; cast supplies a type annotation
        without adding runtime validation.

        Example:
            A route with another cardinality is rejected by the root accessor.


        :param src_table: Source table name or table API reference.
        :param dst_table: Destination table name or table API reference.
        :return: Schema-backed link object after root freshness and cardinality checks.
        """

        return cast(SchemaBackedLinkTable, self._cache.get_one_many_link_table(src_table, dst_table))

    def update(self, update: OneManyInTwoTableFieldUpdate[Any]) -> None:
        """
        Apply sequence-value changes, source unlinking and explicit link replacements.

        Require a database, merge added/updated maps and materialize value and
        replacement sequences. Reject value-update/replacement overlap first, then
        delete/replacement overlap. Check existing link-sequence lengths before any
        unlinking. Value updates may still overlap deletions: destinations are resolved
        again after unlinking, so those deleted sources can contribute no value writes.

        Unlink deleted sources, gather/write destination values in current link order,
        then apply explicit replacements per source with exclusive destination ownership.
        Several sources targeting one destination keep the last collected value.
        Replacement resolution can create rows before later errors. No encompassing
        transaction or rollback is added. Rebind after requested changes or truthy
        dirtied; failures can leave durable changes with stale field projection.

        Example:
            An explicit replacement sequence changes both link membership/order and
            its supplied destination values; a plain value sequence requires matching
            existing link count.


        :param update: Relation update with added/updated value sequences, replacements, deleted IDs and dirtied.
        :return: None; writes requested changes and conditionally rebuilds relation projection.
        :raises ValueError: Update categories overlap or sequence/replacement validation fails.
        :raises KeyError: Nonempty values have no existing links or an explicit target is absent.
        """

        self._db = _ensure_db(self._db)

        value_updates = {
            int(src_id): tuple(values)
            for src_id, values in {
                **dict(update.added_maps),
                **dict(update.updated_maps),
            }.items()
        }
        explicit_replacements = {
            int(src_id): tuple(replacements)
            for src_id, replacements in dict(update.link_replacements).items()
        }
        overlap = set(value_updates) & set(explicit_replacements)
        if overlap:
            raise ValueError(
                f"Field {self.field_key!r} cannot mix value updates and explicit link replacements "
                f"for the same src ids: {sorted(overlap)}"
            )
        overlap = {int(src_id) for src_id in update.deleted_ids} & set(explicit_replacements)
        if overlap:
            raise ValueError(
                f"Field {self.field_key!r} cannot delete and replace links for the same src ids: {sorted(overlap)}"
            )

        self._ensure_existing_sequence_targets(value_updates)

        deleted_src_ids = {int(src_id) for src_id in update.deleted_ids}
        if deleted_src_ids:
            self._unlink_src_ids(deleted_src_ids)

        dst_updates: dict[int, Any] = {}
        for src_id, values in value_updates.items():
            for dst_id, value in zip(self._existing_ordered_dst_ids_for_src(src_id), values):
                dst_updates[int(dst_id)] = value
        if dst_updates:
            self._update_dst_values(dst_updates)

        for src_id, replacements in explicit_replacements.items():
            self._replace_links_for_src(
                int(src_id),
                replacements,
                allow_shared_dst=False,
            )

        if deleted_src_ids or dst_updates or explicit_replacements or update.dirtied:
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
    def ids_values_map(self) -> dict[int, Sequence[Optional[Any]]]:
        """
        Copy each stored source projection as a value tuple.

        Read _src_to_values without table refresh or type filtering. Only stored
        source keys are included; returned mutable elements are not deep-copied.

        Example:
            An unlinked source is omitted from the mapping.


        :return: New dict of source IDs to value tuples.
        """

        return {
            src_id: tuple(values)
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

    def get_values_from_src_id(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Optional[Any]]:
        """
        Read a stored value tuple or traverse current cached links when requested.

        With false ordering and no filter, read _src_to_values. Otherwise check
        source existence and project through the current endpoint/link caches. These
        paths can differ after table refresh until field dictionaries are repaired.

        Example:
            require_ordering=True selects traversal even without a type filter.


        :param src_id: Source ID converted with int.
        :param require_ordering: True selects link traversal and forwards the ordering hint.
        :param type_filter: Non-None selects traversal with an exact link-type filter.
        :return: Tuple of projected values, including repeats and None for absent destinations.
        """

        if require_ordering or type_filter is not None:
            return self._values_for_src_id(
                int(src_id),
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
        return tuple(self._src_to_values.get(int(src_id), ()))

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

    def get_dst_ids_from_src_id(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[int]:
        """
        Read destination IDs from current cached link records.

        This always uses the link table, without consulting stored field
        dictionaries or checking endpoint existence. Priority-enabled link views
        sort by descending priority even when require_ordering is false.

        Example:
            A dangling link can return its destination ID while its projected value is None.


        :param src_id: Source ID converted with int.
        :param require_ordering: Ordering hint forwarded to link selection.
        :param type_filter: Optional exact link-type filter.
        :return: Tuple of destination IDs retaining repeated links and dangling endpoints.
        """

        return self._ordered_dst_ids_for_src(
            int(src_id),
            require_ordering=require_ordering,
            type_filter=type_filter,
        )

    def get_src_id_from_dst_id(
        self,
        dst_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[int]:
        """
        Select the first source from current reverse link records.

        This first-item selection does not validate source membership or enforce
        unique ownership if malformed link data contains several sources.

        Example:
            Several accepted source links return their first source rather than raising.


        :param dst_id: Destination ID converted with int.
        :param type_filter: Optional exact link-type filter.
        :return: First source ID, or None when no accepted link exists.
        """

        src_ids = self._ordered_src_ids_for_dst(int(dst_id), type_filter=type_filter)
        return src_ids[0] if src_ids else None

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
        Build one-to-many link metadata from a unique cached pair Row.

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
        Write supported non-None one-to-many link properties and rebind the field.

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


__all__ = ["SchemaBackedOneManyField"]
