"""
Implement scalar same-table and relational one-to-one schema-backed fields.

Scalar fields copy one table column and support bounded ID repair.
Two-table fields combine cached relation projections with direct endpoint
and link reads. Legacy update methods preserve owner rows while writing
values or links; callers own any enclosing transaction.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable, Optional, Union, cast

from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.one_one_field import (
    CacheOneOneInTwoTableFieldAPI,
    CacheOneOneInSameTableFieldAPI,
    OneOneInTwoTableFieldUpdate,
    OneOneInOneTableFieldUpdate,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.single_table import (
    StorageCacheSingleTableAPI,
)

from LiuXin_alpha.caches.cache_plugins.schema_backed.common import (
    _canonical_field_key,
    _ensure_db,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.relation_base import (
    _SchemaBackedRelationFieldBase,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.link_tables.link_table import (
    SchemaBackedLinkTable,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.single_table import (
    SchemaBackedMainTableCache,
)

if TYPE_CHECKING:
    from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_cache import SchemaBackedStorageCache


class SchemaBackedSameTableField(CacheOneOneInSameTableFieldAPI[Any]):
    """
    Cache one scalar column by owner ID and support bounded value repair.

    read copies IDs and values from the loaded table into a separate mapping.
    Field getters use that mapping until read, refresh_ids or refresh_from_table
    repairs it. Legacy updates write existing rows and clear values with None;
    they neither create nor delete owner rows.

    Example:
        >>> issubclass(SchemaBackedSameTableField, CacheOneOneInSameTableFieldAPI)
        True
    """

    def __init__(
        self,
        cache: "SchemaBackedStorageCache",
        in_table: Union[StorageCacheSingleTableAPI, str],
        column_name: str,
        db: Any,
    ) -> None:
        """
        Resolve the owner table and allocate an empty scalar value mapping.

        Example:
            Call read after table initialization to populate the new field.


        :param cache: Root cache used to resolve the table.
        :param in_table: Owner table name or table API reference.
        :param column_name: Column name retained without schema validation here.
        :param db: Database reference retained by the API constructor.
        :return: None; sets field identity and resolves its table without copying values.
        """

        self._cache = cache
        self.column_name = column_name
        self._ids_values_map: dict[int, Any] = {}
        super().__init__(in_table=in_table, db=db)

    @property
    def field_key(self) -> str:
        """
        Qualify the scalar column name with its owning table name.

        Example:
            A title column on books has field_key books.title.


        :return: table.column key from _canonical_field_key without escaping or validation.
        """

        return _canonical_field_key(self.table_name, self.column_name)

    @property
    def table_name(self) -> str:
        """
        Expose the bound owner table name.

        Example:
            A field for books.title is owned by books.


        :return: The in_table.table name without another root lookup.
        """

        return self.in_table.table

    def get_main_table(
        self,
        name: Union[str, StorageCacheSingleTableAPI],
    ) -> StorageCacheSingleTableAPI:
        """
        Resolve a table reference through the owning root cache.

        Example:
            The inherited field constructor calls this to bind in_table from a name.


        :param name: Table name or table API reference forwarded unchanged.
        :return: Root cache main-table object; dependency refresh follows the root's policy.
        """

        return self._cache.get_main_table(name)

    def read(self, db: Any) -> None:
        """
        Copy the loaded table column into an owner-ID-to-value mapping.

        Read table.row_ids and table.get_values_for rather than deep-copying
        each complete row. The underlying table is not reloaded or reattached here.
        Values are shared with the table result; duplicate IDs keep their last value.

        Example:
            Reload the table first when an external write should be visible to this field.


        :param db: Database override retained on the field; None selects its current attachment.
        :return: None; replaces the mapping after strict ID/value pairing succeeds.
        :raises RuntimeError: No field database is available.
        :raises ValueError: The ID and value sequences differ in length.
        """

        db = _ensure_db(self._db, db)
        self._db = db
        table = self.in_table
        # The table cache already owns a column index. Re-reading and
        # deep-copying every complete row once per field makes cache startup
        # quadratic in the number of columns for wide schema tables.
        self._ids_values_map = dict(
            zip(
                table.row_ids,
                table.get_values_for(self.column_name),
                strict=True,
            )
        )

    def _table_cache(self) -> SchemaBackedMainTableCache:
        """
        Return the bound owner table under its concrete annotation.

        Example:
            Bounded scalar refresh uses this object to reload selected rows.


        :return: The same in_table object; cast performs no validation or refresh.
        """

        return cast(SchemaBackedMainTableCache, self.in_table)

    def _column_spec(self):
        """
        Find the first schema column matching the field's stored column name.

        Example:
            Nullability checks use the stored table schema, not a fresh database
            schema-discovery call.


        :return: Column specification from the bound table's spec.columns.
        :raises KeyError: No column in the bound table specification has the requested name.
        """

        table = self._table_cache()
        for column in table.spec.columns:
            if column.name == self.column_name:
                return column
        raise KeyError(self.column_name)

    def _assert_can_write_value(self, value: Any) -> None:
        """
        Reject clearing a primary-key or non-nullable scalar column.

        Resolve the stored column specification first. Non-None values are not
        type-checked, uniqueness-checked or rejected merely because the column is a
        primary key; remaining constraints belong to the driver.

        Example:
            None is rejected for a NOT NULL title, while an empty string passes
            this local nullability guard.


        :param value: Proposed scalar value; only identity with None triggers this guard.
        :return: None when the value passes the limited nullability check.
        :raises ValueError: The value is None and the column is primary-key or non-nullable.
        :raises KeyError: The stored column cannot be resolved.
        """

        column = self._column_spec()
        if value is None and (column.is_primary_key or not column.nullable):
            raise ValueError(
                f"Field {self.field_key!r} cannot be cleared because the column is not nullable"
            )

    def _assert_rows_exist(self, ids: Iterable[int]) -> None:
        """
        Check all supplied owner IDs against the attached database before writing.

        Collect missing IDs and report them sorted, retaining duplicates if the
        input repeats missing IDs. This uses database rows rather than cached
        membership; lookup and conversion errors propagate immediately.

        Example:
            A cached row deleted externally is rejected here before scalar updates
            are delegated to the table.


        :param ids: Iterable of row IDs converted with int for database lookup.
        :return: None when every get_row_from_id result is non-None.
        :raises KeyError: One or more owner rows are missing from the attached database.
        """

        missing = [
            int(row_id)
            for row_id in ids
            if self._db.get_row_from_id(self.table_name, int(row_id)) is None
        ]
        if missing:
            raise KeyError(f"Cannot update field {self.field_key!r}; missing row ids: {sorted(missing)}")

    def _write_values(self, id_value_map: dict[int, Any]) -> None:
        """
        Validate one scalar update group and delegate it to the bound main table.

        Normalize all IDs, check every owner row in the database, then validate all
        values against column nullability before calling in_table.update with this
        column as target. The table refreshes the selected owner rows. This helper does
        not repair the field's value mapping or add a transaction.

        Example:
            If one value in a group is an invalid None, none of that group reaches
            in_table.update; earlier groups in a larger field update may already be written.


        :param id_value_map: ID-to-value dict; IDs are normalized with int and colliding IDs keep the last value.
        :return: None; validates and writes a nonempty group, otherwise returns immediately.
        """

        if not id_value_map:
            return
        normalized = {int(row_id): value for row_id, value in id_value_map.items()}
        self._assert_rows_exist(normalized.keys())
        for value in normalized.values():
            self._assert_can_write_value(value)
        self.in_table.update(
            normalized,
            self._db,
            target_column=self.column_name,
        )

    def update(self, update: OneOneInOneTableFieldUpdate[Any]) -> None:
        """
        Write added, changed and cleared values, then refresh changed or dirty IDs.

        Require a database before processing added maps, updated maps, then
        deleted IDs as None writes. Each nonempty group validates existing rows and
        nullability before writing. Earlier groups may persist if a later one fails;
        there is no encompassing transaction. Added maps do not create owner rows.

        On success refresh the union of changed and dirtied IDs. With no such IDs,
        read the entire stored table column without reloading its database rows.

        Example:
            Deleting a nullable field value clears its column while retaining the owner row.


        :param update: Update with added_maps, updated_maps, deleted_ids and dirtied owner IDs.
        :return: None; writes requested groups and repairs selected values, or reads all when empty.
        """

        self._db = _ensure_db(self._db)
        changed_ids: set[int] = set()
        if update.added_maps:
            changed_ids.update(int(row_id) for row_id in update.added_maps)
            self._write_values(dict(update.added_maps))
        if update.updated_maps:
            changed_ids.update(int(row_id) for row_id in update.updated_maps)
            self._write_values(dict(update.updated_maps))
        if update.deleted_ids:
            changed_ids.update(int(row_id) for row_id in update.deleted_ids)
            self._write_values({int(row_id): None for row_id in update.deleted_ids})
        refresh_ids = changed_ids | {int(row_id) for row_id in update.dirtied}
        if refresh_ids:
            self.refresh_ids(refresh_ids)
        else:
            self.read(self._db)

    def refresh_ids(
        self,
        ids: Iterable[int],
        db: Any = None,
    ) -> None:
        """
        Reload selected owner rows and repair this field from the table cache.

        Require and retain a field database even for empty IDs. The table helper
        uses the table's own attached database; the override is not forwarded to it.
        Table and field repair can leave partial changes on a later failure.

        Example:
            After an external title change, refresh_ids({7}) rereads row 7 and repairs its title.


        :param ids: Owner IDs consumed into a deduplicated integer set.
        :param db: Database override retained on the field; None uses current attachment.
        :return: None; refreshes table rows and inserts/removes corresponding field values.
        """

        db = _ensure_db(self._db, db)
        self._db = db
        ids = {int(row_id) for row_id in ids}
        table = self._table_cache()
        table._refresh_ids(ids)
        self.refresh_from_table(ids)

    def refresh_from_table(self, ids: Iterable[int]) -> None:
        """
        Repair selected field values from already refreshed owner rows.

        Perform no database reads or attachment checks. Missing column keys
        produce None; unrelated field entries remain unchanged.

        Example:
            An owner removed from the table cache is evicted from this field on repair.


        :param ids: Owner IDs consumed into a deduplicated integer set.
        :return: None; inserts snapshot column values for present rows and evicts absent IDs.
        """

        ids = {int(row_id) for row_id in ids}
        table = self._table_cache()
        for row_id in ids:
            if table.has_id(row_id):
                self._ids_values_map[row_id] = table.get_row_snapshot(row_id).get(self.column_name)
            else:
                self._ids_values_map.pop(row_id, None)

    def remove_ids(self, ids: Iterable[int]) -> None:
        """
        Evict selected owner IDs from this field mapping only.

        Do not delete database rows, change table caches or require attachment.
        A subsequent full read can restore entries still present in the table.

        Example:
            remove_ids({7}) makes the field return None for ID 7 until it is populated again.


        :param ids: Owner IDs consumed into a deduplicated integer set.
        :return: None; ignores IDs already absent.
        """

        for row_id in {int(row_id) for row_id in ids}:
            self._ids_values_map.pop(row_id, None)

    @property
    def ids(self) -> set[int]:
        """
        Copy the owner IDs currently stored in this field.

        Example:
            Removing ID 7 from this field removes 7 from this set.


        :return: New set of mapping keys without refreshing table data.
        """

        return set(self._ids_values_map.keys())

    @property
    def values(self) -> list[Any]:
        """
        List stored scalar values in ascending owner-ID order.

        Example:
            Owners 2 and 7 contribute their values in that order.


        :return: New list of mapped values, including None and repeated values.
        """

        return [self._ids_values_map[row_id] for row_id in sorted(self._ids_values_map)]

    @property
    def values_set(self) -> set[Any]:
        """
        Collect distinct stored scalar values.

        Example:
            Two owners storing the same string contribute one set entry.


        :return: Set using Python hash/equality, including None when stored.
        :raises TypeError: A stored value is unhashable.
        """

        return set(self._ids_values_map.values())

    @property
    def ids_values_map(self) -> dict[int, Optional[Any]]:
        """
        Copy the owner-ID-to-value mapping.

        Example:
            Removing a key from the returned dictionary leaves the field mapping intact.


        :return: New dict with shared value references; no table refresh is performed.
        """

        return dict(self._ids_values_map)

    def get_value_from_id(self, table_id: int) -> Optional[Any]:
        """
        Look up one value in this field's stored mapping.

        Example:
            A stored None and an absent owner both return None.


        :param table_id: Owner identity converted with int.
        :return: Stored value, or None when the ID is absent.
        """

        return self._ids_values_map.get(int(table_id))

    def get_ids_from_value(self, value: Any) -> list[int]:
        """
        Find owner IDs whose stored values compare equal to the query.

        Example:
            A query of None finds only stored null values, not absent owner IDs.


        :param value: Value compared with ==; no hashing or text normalization is required.
        :return: Sorted list of matching owner IDs.
        """

        return sorted(row_id for row_id, row_value in self._ids_values_map.items() if row_value == value)


class SchemaBackedTwoTableOneOneField(
    _SchemaBackedRelationFieldBase[Any],
    CacheOneOneInTwoTableFieldAPI[Any],
):
    """
    Expose one-to-one destination values and legacy relation updates.

    Construction resolves the endpoint tables and required directed link type.
    read builds field dictionaries from those snapshots. Bulk mappings need a
    field refresh after underlying data changes; direct endpoint/link getters
    can observe the newer table caches before that refresh.
    Scalar source results select the first projected value without enforcing
    singularity on that read path.

    Example:
        >>> issubclass(SchemaBackedTwoTableOneOneField, CacheOneOneInTwoTableFieldAPI)
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
        Resolve endpoint/link caches and initialize empty one-to-one projections.

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
        CacheOneOneInTwoTableFieldAPI.__init__(
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
        Resolve the one-to-one directed link view through the owning cache.

        Delegate to get_one_one_link_table; cast supplies a type annotation
        without adding runtime validation.

        Example:
            A route with another cardinality is rejected by the root accessor.


        :param src_table: Source table name or table API reference.
        :param dst_table: Destination table name or table API reference.
        :return: Schema-backed link object after root freshness and cardinality checks.
        """

        return cast(SchemaBackedLinkTable, self._cache.get_one_one_link_table(src_table, dst_table))

    def update(self, update: OneOneInTwoTableFieldUpdate[Any]) -> None:
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

        When reusing an exact-value destination for a missing link, reject ownership
        by a different source. Report collected missing-link errors before collected
        ownership conflicts. Singular link lookups can raise on duplicate records.

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
        linked_dst_conflicts: list[tuple[int, int, int]] = []
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
            else:
                existing_src_id = cast(Any, self.link_table).get_src_id(int(dst_id))
                if existing_src_id is not None and int(existing_src_id) != int(src_id):
                    linked_dst_conflicts.append((int(src_id), int(dst_id), int(existing_src_id)))
                    continue

            links_to_create[int(src_id)] = int(dst_id)

        if missing_link_src_ids:
            raise KeyError(
                f"Field {self.field_key!r} cannot update missing linked rows for src ids: {sorted(missing_link_src_ids)}"
            )
        if linked_dst_conflicts:
            details = ", ".join(
                f"src {src_id} -> dst {dst_id} already linked to src {existing_src_id}"
                for src_id, dst_id, existing_src_id in linked_dst_conflicts
            )
            raise ValueError(
                f"Field {self.field_key!r} cannot reuse already-linked dst rows in one-to-one mode: {details}"
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

    def get_dst_id_from_src_id(self, src_id: int) -> Optional[int]:
        """
        Select the first destination from current cached link records.

        This first-item selection adds no singularity or endpoint-existence check.

        Example:
            Malformed links to IDs 7 and 8 select the first accepted ID.


        :param src_id: Source ID converted with int.
        :return: First destination ID, or None when no accepted link exists.
        """

        dst_ids = self._ordered_dst_ids_for_src(int(src_id))
        return dst_ids[0] if dst_ids else None

    def get_src_id_from_dst_id(self, dst_id: int) -> Optional[int]:
        """
        Select the first source from current reverse link records.

        This first-item selection does not validate source membership or enforce
        unique ownership if malformed link data contains several sources.

        Example:
            Several accepted source links return their first source rather than raising.


        :param dst_id: Destination ID converted with int.
        :return: First source ID, or None when no accepted link exists.
        """

        src_ids = self._ordered_src_ids_for_dst(int(dst_id))
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


__all__ = [
    "SchemaBackedSameTableField",
    "SchemaBackedTwoTableOneOneField",
]
