"""
Share schema-backed relation projections, bounded repair and legacy writes.

Concrete cardinality fields supply endpoint/link objects and column names.
These helpers maintain independent value dictionaries, project through table
snapshots and route writes to table/driver APIs. Creation is supported where
requested by the concrete update policy; no encompassing transaction is added.
"""

from __future__ import annotations

import dataclasses

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Generic, Iterable, Optional, Sequence, TypeVar, Union, cast

from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.single_table import (
    StorageCacheSingleTableAPI,
)

from LiuXin_alpha.caches.cache_plugins.schema_backed.common import (
    _canonical_field_key,
    _ensure_db,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.link_tables.link_table import (
    SchemaBackedLinkTable,
)

if TYPE_CHECKING:
    from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_cache import SchemaBackedStorageCache

T = TypeVar("T")


class _SchemaBackedRelationFieldBase(Generic[T]):
    """
    Maintain relation dictionaries and shared endpoint/link mutation helpers.

    Concrete API constructors establish source, destination and directed link
    objects after _init_relation_field. read rebuilds projections from those
    caches without reloading them. Partial repair and local removal can change
    field dictionaries independently of link-table records.

    Legacy writes may create related rows, edit values or replace links. They
    refresh affected table caches but rely on their caller to rebuild field
    projections and own any transaction spanning multiple operations.

    Example:
        A one-to-many field reuses these helpers for ordered replacements,
        while its public API returns sequences of projected destination values.
    """

    def _init_relation_field(
        self,
        cache: "SchemaBackedStorageCache",
        db: Any,
    ) -> None:
        """
        Retain the owner/database and allocate empty relation dictionaries.

        Example:
            >>> field = _SchemaBackedRelationFieldBase()
            >>> field._init_relation_field(object(), None)
            >>> field._src_to_values, field._dst_to_values
            ({}, {})


        :param cache: Root schema-backed cache used by endpoint resolution.
        :param db: Database reference retained without validation.
        :return: None; initializes source/destination ID and value mappings without reading data.
        """

        self._cache = cache
        self._db = db
        self._src_to_dst_ids: dict[int, tuple[int, ...]] = {}
        self._dst_to_src_ids: dict[int, tuple[int, ...]] = {}
        self._src_to_values: dict[int, tuple[Optional[T], ...]] = {}
        self._dst_to_values: dict[int, Optional[T]] = {}

    @property
    def field_key(self) -> str:
        """
        Qualify a destination column by both endpoint table names.

        Example:
            A books-to-tags projection of tag_name has key books.tags.tag_name.


        :return: source.destination.column string without escaping or validation.
        """

        return _canonical_field_key(
            self.src_table_name,
            f"{self.dst_table_name}.{self.dst_table_cache_col}",
        )

    @property
    def table_name(self) -> str:
        """
        Expose the source table as the owner of this relation field.

        Example:
            A book field projecting tag values is owned by books, not tags.


        :return: Configured src_table_name, without resolving a database table.
        """

        return self.src_table_name

    @property
    def column_name(self) -> str:
        """
        Expose the destination column whose values the field projects.

        Example:
            For books.tags.tag_name, column_name is tag_name.


        :return: Configured dst_table_cache_col unchanged.
        """

        return self.dst_table_cache_col

    def get_main_table(
        self,
        name: Union[str, StorageCacheSingleTableAPI],
    ) -> StorageCacheSingleTableAPI:
        """
        Resolve a field endpoint through the owning root cache.

        Example:
            The inherited relation constructor uses this to establish both endpoint tables.


        :param name: Table name or table API reference passed unchanged to the root.
        :return: Resolved main-table object according to root cache refresh policy.
        """

        return self._cache.get_main_table(name)

    def _link_table_cache(self) -> SchemaBackedLinkTable:
        """
        Expose the bound link object under the concrete schema-backed annotation.

        Example:
            Property helpers use this view to inspect the stored link specification.


        :return: The existing link_table object without lookup, validation or refresh.
        """

        return cast(SchemaBackedLinkTable, self.link_table)

    def _value_for_dst_id(self, dst_id: int) -> Optional[T]:
        """
        Read the projected column from a cached destination-row snapshot.

        Check endpoint membership before requesting its row snapshot. No link
        membership or field-dictionary lookup is required.

        Example:
            An unlinked destination can still supply a value through this helper.


        :param dst_id: Destination ID converted with int.
        :return: Snapshot column value, or None for an absent row or missing column.
        """

        dst_id = int(dst_id)
        if self.dst_table.has_id(dst_id):
            return cast(Optional[T], self.dst_table.get_row_snapshot(dst_id).get(self.dst_table_cache_col))
        return None

    def _ordered_dst_ids_for_src(
        self,
        src_id: int,
        *,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> tuple[int, ...]:
        """
        Read ordered destination IDs through the bound directed link table.

        Use current cached link indexes without checking endpoint rows or
        refreshing field dictionaries. Priority-enabled views sort descending even
        when the ordering hint is false; repeated physical links remain repeated.

        Example:
            A repeated physical link remains a repeated endpoint ID in the result.


        :param src_id: Source ID converted with int.
        :param require_ordering: Ordering hint forwarded to the link table; its priority policy determines order.
        :param type_filter: Optional exact type filter forwarded to link selection.
        :return: Tuple of integer endpoint IDs in link order, preserving duplicate links.
        """

        link_table = cast(Any, self.link_table)
        return tuple(
            int(dst_id)
            for dst_id in link_table.get_dst_ids(
                int(src_id),
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
        )

    def _ordered_src_ids_for_dst(
        self,
        dst_id: int,
        *,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> tuple[int, ...]:
        """
        Read ordered source IDs through the bound directed link table.

        Use current cached link indexes without checking endpoint rows or
        refreshing field dictionaries. Priority-enabled views sort descending even
        when the ordering hint is false; repeated physical links remain repeated.

        Example:
            A repeated physical link remains a repeated endpoint ID in the result.


        :param dst_id: Destination ID converted with int.
        :param require_ordering: Ordering hint forwarded to the link table; its priority policy determines order.
        :param type_filter: Optional exact type filter forwarded to link selection.
        :return: Tuple of integer endpoint IDs in link order, preserving duplicate links.
        """

        link_table = cast(Any, self.link_table)
        return tuple(
            int(src_id)
            for src_id in link_table.get_src_ids(
                int(dst_id),
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
        )

    def _values_for_src_id(
        self,
        src_id: int,
        *,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> tuple[Optional[T], ...]:
        """
        Project current cached destination values for a source's accepted links.

        Return empty when the source row is absent from its table cache.
        Otherwise read link IDs and destination snapshots; do not use or repair
        _src_to_values.

        Example:
            A source deleted from the table cache yields () even if stale links remain.


        :param src_id: Source identity converted with int.
        :param require_ordering: Ordering hint passed to the directed link view.
        :param type_filter: Optional exact type filter passed to link selection.
        :return: Tuple retaining link order, duplicate links and None for absent destinations.
        """

        if not self.src_table.has_id(int(src_id)):
            return ()
        return tuple(
            self._value_for_dst_id(dst_id)
            for dst_id in self._ordered_dst_ids_for_src(
                int(src_id),
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
        )

    def _single_value_for_src_id(
        self,
        src_id: int,
        *,
        type_filter: Optional[str] = None,
    ) -> Optional[T]:
        """
        Return the first accepted projected value without enforcing relation singularity.

        Compute the entire ordered value tuple before taking its first member.
        Multiple links do not raise merely because this helper selects one value;
        a missing first destination can yield None even if later destinations exist.

        Example:
            For projected values (None, "later"), this returns None rather than
            searching for the first non-null value.


        :param src_id: Source ID converted with int.
        :param type_filter: Optional exact type filter passed to link selection.
        :return: First value in link order, or None when no accepted links exist.
        """

        values = self._values_for_src_id(int(src_id), type_filter=type_filter)
        return values[0] if values else None

    def _read_relation_cache(self) -> None:
        """
        Rebuild all field dictionaries from current endpoint and link snapshots.

        Visit existing source IDs in sorted order and request ordered links.
        Omit unlinked sources; retain repeated links and dangling destination IDs
        with None values. Reverse source tuples retain repeated links. Build local
        dictionaries before publishing them; this performs no database scan.

        Example:
            Two physical links from source 1 to destination 7 produce reverse
            sources (1, 1) for 7 after a full read.


        :return: None; replaces the four relation dictionaries after projection succeeds.
        """

        src_to_dst_ids: dict[int, tuple[int, ...]] = {}
        dst_to_src_ids: dict[int, list[int]] = {}
        src_to_values: dict[int, tuple[Optional[T], ...]] = {}
        dst_to_values: dict[int, Optional[T]] = {}

        for src_id in sorted(int(row_id) for row_id in self.src_table.row_ids):
            dst_ids = self._ordered_dst_ids_for_src(src_id, require_ordering=True)
            if not dst_ids:
                continue

            values = tuple(self._value_for_dst_id(dst_id) for dst_id in dst_ids)
            src_to_dst_ids[src_id] = dst_ids
            src_to_values[src_id] = values

            for dst_id, value in zip(dst_ids, values):
                dst_to_src_ids.setdefault(dst_id, []).append(src_id)
                dst_to_values[dst_id] = value

        self._src_to_dst_ids = src_to_dst_ids
        self._src_to_values = src_to_values
        self._dst_to_src_ids = {
            dst_id: tuple(src_ids)
            for dst_id, src_ids in dst_to_src_ids.items()
        }
        self._dst_to_values = dst_to_values

    def read(self, db: Any) -> None:
        """
        Attach relation database references and rebuild field value dictionaries.

        Retain _db and assign catalog on the three bound table/link objects.
        These assignments do not reload those caches. A later projection failure
        can leave new database references with previous field dictionaries.

        Example:
            Reload endpoint/link data before calling read to make external writes
            available to the rebuilt projection.


        :param db: Database override; None selects the field's current attachment.
        :return: None; updates references before projecting existing table/link snapshots.
        :raises RuntimeError: No explicit or attached field database is available.
        """

        db = _ensure_db(self._db, db)
        self._db = db
        self.src_table.catalog = db
        self.dst_table.catalog = db
        self.link_table.catalog = db
        self._read_relation_cache()

    def refresh_ids(
        self,
        ids: Iterable[int],
        db: Any = None,
    ) -> None:
        """
        Rebuild the complete field projection for an ignored ID refresh hint.

        Example:
            A one-ID hint still visits every cached source; use refresh_table_ids
            for bounded repair after the owning table has refreshed.


        :param ids: ID iterable ignored without consumption.
        :param db: Database override; None selects current field attachment.
        :return: None; delegates to read without reloading endpoint or link caches.
        """

        del ids
        self.read(db)

    def refresh_table_ids(
        self,
        table_name: str,
        ids: Iterable[int],
    ) -> None:
        """
        Repair field dictionaries for changed source or destination rows.

        Empty IDs return immediately. For changed sources, remove old projections
        and reverse memberships, then reproject surviving rows through current cached
        links. Rebuilt reverse memberships are sorted sets, so this path can remove
        duplicates that a full read retains.

        For changed destinations, gather previously linked sources, store values
        for surviving rows (including unlinked ones), evict absent destination values,
        and reproject affected source tuples without changing their destination IDs.
        Deleted destinations can thus remain as None in source projections while
        being absent from _dst_to_values. Both branches run for a self-relation;
        unknown table names leave dictionaries unchanged after ID conversion.
        Errors may leave partial repairs; no atomic replacement is attempted.

        Example:
            After destination 7 is deleted from the table cache, repairing its ID
            keeps existing source link IDs but projects None for their missing value.


        :param table_name: Table name converted with str and compared to both endpoint names.
        :param ids: Changed IDs consumed into a deduplicated integer set.
        :return: None; repairs affected dictionaries without database reads or link-table reload.
        """

        changed_ids = {int(row_id) for row_id in ids}
        if not changed_ids:
            return

        if str(table_name) == self.src_table_name:
            for src_id in changed_ids:
                old_dst_ids = self._src_to_dst_ids.pop(src_id, ())
                self._src_to_values.pop(src_id, None)
                for dst_id in old_dst_ids:
                    remaining = tuple(
                        value
                        for value in self._dst_to_src_ids.get(dst_id, ())
                        if value != src_id
                    )
                    if remaining:
                        self._dst_to_src_ids[dst_id] = remaining
                    else:
                        self._dst_to_src_ids.pop(dst_id, None)
                        self._dst_to_values.pop(dst_id, None)

                if not self.src_table.has_id(src_id):
                    continue
                dst_ids = self._ordered_dst_ids_for_src(
                    src_id,
                    require_ordering=True,
                )
                if not dst_ids:
                    continue
                values = tuple(self._value_for_dst_id(dst_id) for dst_id in dst_ids)
                self._src_to_dst_ids[src_id] = dst_ids
                self._src_to_values[src_id] = values
                for dst_id, value in zip(dst_ids, values):
                    src_ids = set(self._dst_to_src_ids.get(dst_id, ()))
                    src_ids.add(src_id)
                    self._dst_to_src_ids[dst_id] = tuple(sorted(src_ids))
                    self._dst_to_values[dst_id] = value

        if str(table_name) == self.dst_table_name:
            affected_sources: set[int] = set()
            for dst_id in changed_ids:
                affected_sources.update(self._dst_to_src_ids.get(dst_id, ()))
                if self.dst_table.has_id(dst_id):
                    self._dst_to_values[dst_id] = self._value_for_dst_id(dst_id)
                else:
                    self._dst_to_values.pop(dst_id, None)
            for src_id in affected_sources:
                dst_ids = self._src_to_dst_ids.get(src_id, ())
                self._src_to_values[src_id] = tuple(
                    self._value_for_dst_id(dst_id) for dst_id in dst_ids
                )

    def remove_ids(self, ids: Iterable[int]) -> None:
        """
        Prune selected sources from field dictionaries and rebuild reverse projections.

        Retain remaining source ID/value tuples and rebuild reverse dictionaries
        by zipping them. Repeated links remain repeated. Empty input is a no-op;
        a full read can restore pruned sources if table/link snapshots still contain
        them. Direct link getters can continue to expose those links meanwhile.

        Example:
            Local removal hides source 1 from bulk field mappings while leaving
            its underlying database rows and cached links intact.


        :param ids: Source IDs consumed into a deduplicated integer set.
        :return: None; nonempty input replaces field dictionaries without touching table caches.
        """

        removed_ids = {int(row_id) for row_id in ids}
        if not removed_ids:
            return

        self._src_to_dst_ids = {
            src_id: dst_ids
            for src_id, dst_ids in self._src_to_dst_ids.items()
            if src_id not in removed_ids
        }
        self._src_to_values = {
            src_id: values
            for src_id, values in self._src_to_values.items()
            if src_id not in removed_ids
        }

        dst_to_src_ids: dict[int, list[int]] = {}
        dst_to_values: dict[int, Optional[T]] = {}
        for src_id, dst_ids in self._src_to_dst_ids.items():
            values = self._src_to_values.get(src_id, ())
            for dst_id, value in zip(dst_ids, values):
                dst_to_src_ids.setdefault(dst_id, []).append(src_id)
                dst_to_values[dst_id] = value

        self._dst_to_src_ids = {
            dst_id: tuple(src_ids)
            for dst_id, src_ids in dst_to_src_ids.items()
        }
        self._dst_to_values = dst_to_values

    def _flattened_values(self) -> list[Optional[T]]:
        """
        Flatten stored projections in ascending source-ID order.

        Read field dictionaries without refreshing table data. Mutable value
        references are not deep-copied.

        Example:
            >>> field = _SchemaBackedRelationFieldBase()
            >>> field._init_relation_field(object(), None)
            >>> field._src_to_values = {2: ("B",), 1: ("A", "B")}
            >>> field._flattened_values()
            ['A', 'B', 'B']


        :return: New list retaining each source tuple's order, duplicate links and None.
        """

        values: list[Optional[T]] = []
        for src_id in sorted(self._src_to_values):
            values.extend(self._src_to_values[src_id])
        return values

    def _unlink_src_ids(self, src_ids: Iterable[int]) -> None:
        """
        Delete cached-record-backed links for selected source IDs through the link updater.

        Empty input returns before requiring a database. The link updater selects
        physical deletions from its current cached records; this does not delete
        source or destination entity rows or rebind the relation field afterward.

        Example:
            Clearing a book's relation links leaves its tag rows available for
            other books.


        :param src_ids: Source IDs consumed into a deduplicated set of integers.
        :return: None; nonempty input runs a src_ids_deleted update and its link-cache reload.
        """

        deleted_ids = {int(src_id) for src_id in src_ids}
        if not deleted_ids:
            return
        self._db = _ensure_db(self._db)
        cast(Any, self.link_table).update(SimpleNamespace(src_ids_deleted=deleted_ids))

    def _validate_create_policy(
        self,
        *,
        create_missing_links: bool,
        create_missing_related_rows: bool,
    ) -> None:
        """
        Require link creation permission whenever related-row creation is enabled.

        This checks only the relationship between flags, not their runtime types
        or any particular source/destination row.

        Example:
            >>> _SchemaBackedRelationFieldBase._validate_create_policy(
            ...     None, create_missing_links=True, create_missing_related_rows=True)


        :param create_missing_links: Whether callers allow missing links to be created.
        :param create_missing_related_rows: Whether callers allow missing destination rows to be created.
        :return: None when the two truth-valued flags form an allowed combination.
        :raises ValueError: Related-row creation is truthy while link creation is false-valued.
        """

        if create_missing_related_rows and not create_missing_links:
            raise ValueError(
                f"Field {self.field_key!r} cannot create related rows without also creating links"
            )

    def _update_dst_values(self, dst_values_map: dict[int, Optional[T]]) -> None:
        """
        Update destination scalar values through the bound main-table helper.

        Return immediately for empty input; otherwise require a database and
        delegate to dst_table.update with the projected column as target. Default
        scalar case-change suppression applies in the main-table helper. No relation
        rebind or cross-operation transaction is added here.

        Example:
            Updating a shared destination value affects every source that links
            to that destination after their projections are refreshed.


        :param dst_values_map: Destination-ID-to-value dict; IDs are converted with int.
        :return: None; a nonempty mapping writes values and refreshes selected destination rows.
        """

        if not dst_values_map:
            return
        self._db = _ensure_db(self._db)
        self.dst_table.update(
            {int(dst_id): value for dst_id, value in dst_values_map.items()},
            self._db,
            target_column=self.dst_table_cache_col,
        )

    def _existing_ordered_dst_ids_for_src(self, src_id: int) -> tuple[int, ...]:
        """
        Read existing destination IDs with the specification's ordering hint.

        Forward bool(link_spec.ordered) as require_ordering. The schema-backed link table
        still sorts whenever priority is enabled, regardless of that hint.

        Example:
            This full unfiltered link sequence defines the positions expected by
            sequence-value updates.


        :param src_id: Source ID converted with int.
        :return: Tuple of destination IDs from the bound link table, without a type filter.
        """

        return self._ordered_dst_ids_for_src(
            int(src_id),
            require_ordering=bool(self._link_table_cache().link_spec.ordered),
        )

    def _get_unique_dst_id_for_value(self, value: T) -> Optional[int]:
        """
        Resolve an exact destination-column value to at most one cached ID.

        Sort converted matching IDs and reject multiple matches. Matching is not
        restricted to already linked destinations and does not normalize text.

        Example:
            Two separate destination rows with the same requested value are
            ambiguous even if only one is currently linked to this source.


        :param value: Hashable value passed to the destination table's exact value index.
        :return: Matching integer ID, or None when no destination row matches.
        :raises ValueError: More than one destination ID matches the value.
        """

        matches = sorted(int(dst_id) for dst_id in self.dst_table.get_ids_for_value(self.dst_table_cache_col, value))
        if not matches:
            return None
        if len(matches) > 1:
            raise ValueError(
                f"Field {self.field_key!r} found multiple dst rows for value {value!r}: {matches}"
            )
        return matches[0]

    def _create_related_dst_row(self, value: T) -> int:
        """
        Create a destination row and refresh its table through the available driver path.

        Require a database. Prefer callable get_blank_row: obtain its payload, set
        the value, call update_row and read the ID from the payload. This path assumes
        the blank-row helper provides an identified row and does not fall back after
        a failure. Otherwise call add_row with just the value column and reject a
        None returned identity. Refresh only the new destination ID afterward through the table's own
        attached database. A failure can follow durable row
        creation, and no link is created by this helper.

        Example:
            Creating a destination succeeds before the later caller creates its
            link; a later error can leave that destination unlinked.


        :param value: Value assigned to the projected destination column.
        :return: Integer ID obtained from the blank-row payload or add_row return value.
        :raises RuntimeError: No database is attached or add_row returns no identity.
        """

        self._db = _ensure_db(self._db)
        driver_wrapper = self._db.driver_wrapper
        if callable(getattr(driver_wrapper, "get_blank_row", None)):
            blank_row = driver_wrapper.get_blank_row(self.dst_table_name)
            payload = dict(getattr(blank_row, "row_dict", blank_row))
            payload[self.dst_table_cache_col] = value
            driver_wrapper.update_row(payload)
            new_id = int(payload[self.dst_table.id_column])
            cast(Any, self.dst_table)._refresh_ids({new_id})
            return new_id

        new_id = driver_wrapper.add_row({self.dst_table_cache_col: value})
        if new_id is None:
            raise RuntimeError(
                f"Field {self.field_key!r} failed to create a related row for value {value!r}"
            )
        new_id = int(new_id)
        cast(Any, self.dst_table)._refresh_ids({new_id})
        return new_id

    def _create_link(self, src_id: int, dst_id: int) -> None:
        """
        Ensure one source/destination link and reload its link-table cache.

        Require a database and delegate cardinality handling and physical writes
        to the link updater. This does not rebind the relation projection afterward.

        Example:
            A new book-to-tag association is requested as a one-pair link update.


        :param src_id: Source identity converted with int.
        :param dst_id: Destination identity converted with int.
        :return: None; sends a create_these_links update to the bound link table.
        """

        self._db = _ensure_db(self._db)
        cast(Any, self.link_table).update(
            SimpleNamespace(create_these_links={int(src_id): int(dst_id)})
        )

    def _validate_link_dst_update(self, link_update: Any) -> None:
        """
        Check an explicit replacement's optional destination table and column labels.

        Missing labels default to the field's own destination names. Check table
        first, then column; this does not validate IDs, values or link properties.

        Example:
            An omitted destination label is accepted, while an explicit None becomes
            "None" and normally fails the corresponding name check.


        :param link_update: Replacement-like object with optional dst_table and dst_table_target_column.
        :return: None when provided labels match this field after string conversion.
        :raises ValueError: A supplied destination table or target-column label differs from this field.
        """

        if str(getattr(link_update, "dst_table", self.dst_table_name)) != self.dst_table_name:
            raise ValueError(
                f"Field {self.field_key!r} received a link update for dst table "
                f"{getattr(link_update, 'dst_table', None)!r}, expected {self.dst_table_name!r}"
            )
        if (
            str(getattr(link_update, "dst_table_target_column", self.dst_table_cache_col))
            != self.dst_table_cache_col
        ):
            raise ValueError(
                f"Field {self.field_key!r} received a link update for dst column "
                f"{getattr(link_update, 'dst_table_target_column', None)!r}, "
                f"expected {self.dst_table_cache_col!r}"
            )

    def _resolve_explicit_dst_target(
        self,
        src_id: int,
        link_update: Any,
        *,
        allow_shared_dst: bool,
    ) -> int:
        """
        Resolve a replacement destination by explicit ID, exact value or row creation.

        Validate destination labels first. An explicit ID must exist in the table
        cache; when sharing is disabled, reject it if its unique existing source is
        another source. Explicit ID takes precedence over the desired value.

        Without an ID, try an exact unique match for a non-None desired value. Reuse
        it when sharing is allowed or it is unowned/already owned by this source. If
        an exact-value match is owned elsewhere and sharing is disabled, create a new
        row instead of retargeting it. With no usable match, create a row even when
        the desired value is None. This helper does not check creation-policy flags
        or create the association itself.

        Example:
            An explicit ID owned by another source raises when sharing is disabled;
            an unowned exact-value match can be reused.


        :param src_id: Source ID used when checking exclusive destination ownership.
        :param link_update: Replacement object with optional destination identity/value and labels.
        :param allow_shared_dst: True permits reusing a destination linked to other sources.
        :return: Integer destination ID; resolving can create a new database row.
        :raises KeyError: An explicit destination ID is absent from the table cache.
        :raises ValueError: Labels disagree, exact value matching is ambiguous, or explicit ownership conflicts.
        :raises RuntimeError: A singular ownership lookup encounters multiple links, or row creation fails.
        """

        self._validate_link_dst_update(link_update)

        explicit_dst_id = getattr(link_update, "dst_table_id", None)
        if explicit_dst_id is not None:
            dst_id = int(explicit_dst_id)
            if not self.dst_table.has_id(dst_id):
                raise KeyError(
                    f"Field {self.field_key!r} cannot target missing dst id {dst_id}"
                )
            if not allow_shared_dst:
                existing_src_id = cast(Any, self.link_table).get_src_id(dst_id)
                if existing_src_id is not None and int(existing_src_id) != int(src_id):
                    raise ValueError(
                        f"Field {self.field_key!r} cannot retarget dst id {dst_id} "
                        f"because it is already linked to src id {int(existing_src_id)}"
                    )
            return dst_id

        desired_value = getattr(link_update, "dst_col_val", None)
        if desired_value is not None:
            matched_dst_id = self._get_unique_dst_id_for_value(desired_value)
            if matched_dst_id is not None:
                if allow_shared_dst:
                    return matched_dst_id
                existing_src_id = cast(Any, self.link_table).get_src_id(matched_dst_id)
                if existing_src_id is None or int(existing_src_id) == int(src_id):
                    return matched_dst_id

        return self._create_related_dst_row(desired_value)

    def _link_property_updates(self, link_update: Any) -> dict[str, Any]:
        """
        Collect supported non-None link properties from a replacement object.

        Resolve each supported property column first. Skip absent columns, absent
        attributes and None values; false/zero values are retained. No endpoint or
        property-type validation is performed, and None cannot clear a property here.

        Example:
            An explicit priority=0 is retained, while priority=None requests no
            priority-column update.


        :param link_update: Replacement object optionally carrying priority/type/primary/origin/policy/data/index.
        :return: New dict mapping recognized physical columns to supplied property values.
        """

        updates: dict[str, Any] = {}
        for property_name in ("priority", "type", "primary", "origin", "policy", "data", "index"):
            column_name = self._column_for_extra(property_name)
            if column_name is None or not hasattr(link_update, property_name):
                continue
            value = getattr(link_update, property_name)
            if value is None:
                continue
            updates[column_name] = value
        return updates

    def _replace_links_for_src(
        self,
        src_id: int,
        replacements: Sequence[Any],
        *,
        allow_shared_dst: bool,
    ) -> None:
        """
        Resolve replacement targets, replace links, then write values and link metadata.

        Resolve all target IDs first, potentially creating destination rows before
        checking later replacements. Gather desired values with a None default, then
        unlink the source. Recreate nonempty replacements through the priority-order
        link updater, write every destination value, then write non-None properties
        per link. An explicit destination ID with no desired value can therefore
        request clearing its destination column. Empty replacements only unlink.

        No source-existence precheck, encompassing transaction or final field rebind
        is added here. Errors can leave newly created unlinked destinations, partly
        replaced links or updated values; earlier changes are not rolled back.

        Example:
            Replacing with [] removes the source's cached-record-backed links while
            retaining destination rows.


        :param src_id: Source ID converted with int for resolution and each write stage.
        :param replacements: Ordered replacement objects; duplicate resolved destination IDs are rejected.
        :param allow_shared_dst: Whether destination resolution may reuse rows linked to other sources.
        :return: None; applies replacement order, destination values and supported properties.
        :raises ValueError: A destination resolves twice or target validation/value matching fails.
        """

        resolved: list[tuple[int, Any]] = []
        seen_dst_ids: set[int] = set()
        dst_updates: dict[int, Optional[T]] = {}

        for link_update in replacements:
            dst_id = self._resolve_explicit_dst_target(
                int(src_id),
                link_update,
                allow_shared_dst=allow_shared_dst,
            )
            if dst_id in seen_dst_ids:
                raise ValueError(
                    f"Field {self.field_key!r} cannot replace src id {int(src_id)} "
                    f"with duplicate dst id {dst_id}"
                )
            seen_dst_ids.add(dst_id)

            desired_value = cast(Optional[T], getattr(link_update, "dst_col_val", None))
            if dst_id in dst_updates and dst_updates[dst_id] != desired_value:
                raise ValueError(
                    f"Field {self.field_key!r} received conflicting values for dst id {dst_id}"
                )
            dst_updates[dst_id] = desired_value
            resolved.append((dst_id, link_update))

        self._unlink_src_ids({int(src_id)})

        if resolved:
            cast(Any, self.link_table).update(
                SimpleNamespace(
                    src_dst_priority_update={
                        int(src_id): [dst_id for dst_id, _link_update in resolved]
                    }
                )
            )

        if dst_updates:
            self._update_dst_values(dst_updates)

        for dst_id, link_update in resolved:
            property_updates = self._link_property_updates(link_update)
            if property_updates:
                self._update_link_row_columns(int(src_id), int(dst_id), property_updates)

    def _ensure_existing_singular_targets(
        self,
        src_ids: Iterable[int],
    ) -> dict[int, int]:
        """
        Require one cached destination for each selected source.

        Use the link table's singular getter. Collect missing links and report
        them after all lookups, while duplicate-link or conversion errors propagate
        immediately. Do not require endpoint rows or write data.

        Example:
            An unlinked source is reported as missing rather than assigned a new destination.


        :param src_ids: Source IDs consumed into a deduplicated integer set and visited in sorted order.
        :return: Dict of source IDs to integer destination IDs when every lookup succeeds.
        :raises KeyError: One or more source IDs have no cached destination link.
        :raises RuntimeError: A singular link lookup encounters multiple accepted records.
        """

        mapping: dict[int, int] = {}
        missing: list[int] = []
        for src_id in sorted({int(value) for value in src_ids}):
            dst_id = cast(Any, self.link_table).get_dst_id(src_id)
            if dst_id is None:
                missing.append(src_id)
                continue
            mapping[src_id] = int(dst_id)
        if missing:
            raise KeyError(
                f"Field {self.field_key!r} cannot update missing linked rows for src ids: {missing}"
            )
        return mapping

    def _ensure_existing_sequence_targets(
        self,
        updates: dict[int, Sequence[Optional[T]]],
    ) -> dict[int, tuple[int, ...]]:
        """
        Match each supplied value sequence to the existing ordered destination sequence.

        Collect missing-link sources and length mismatches across the input.
        Nonempty values with no links are reported as missing before any length
        mismatch errors. Empty values and no links are accepted. The method does
        not create links, validate individual values or check endpoint row existence.

        Example:
            Two existing links require exactly two supplied values, including None
            placeholders when the caller intends to clear a nullable value.


        :param updates: Source-ID-to-value-sequence dict, consumed without writing values.
        :return: Dict of converted source IDs to existing ordered destination-ID tuples.
        :raises KeyError: One or more sources have nonempty values but no existing links.
        :raises ValueError: After missing-link checks, a sequence length differs from its link count.
        """

        mapping: dict[int, tuple[int, ...]] = {}
        missing: list[int] = []
        length_mismatches: list[tuple[int, int, int]] = []

        for src_id, values in updates.items():
            dst_ids = self._existing_ordered_dst_ids_for_src(src_id)
            if not dst_ids and values:
                missing.append(int(src_id))
                continue
            if len(dst_ids) != len(values):
                length_mismatches.append((int(src_id), len(dst_ids), len(values)))
                continue
            mapping[int(src_id)] = dst_ids

        if missing:
            raise KeyError(
                f"Field {self.field_key!r} cannot update missing linked rows for src ids: {sorted(missing)}"
            )
        if length_mismatches:
            mismatch_text = ", ".join(
                f"{src_id} (linked={linked_count}, values={value_count})"
                for src_id, linked_count, value_count in length_mismatches
            )
            raise ValueError(
                f"Field {self.field_key!r} requires one value per existing linked row: {mismatch_text}"
            )
        return mapping

    def _link_row_snapshot(self, src_id: int, dst_id: int) -> dict[str, Any]:
        """
        Copy the payload of a unique cached physical link Row for a directed pair.

        Use get_link_row with default singularity enforcement and no type filter.
        The link helper already deep-copies its record payload into a read-only Row;
        this method adds a top-level dict copy.

        Example:
            Two typed rows for one pair can make this lookup ambiguous because no
            link-type selector is supplied.


        :param src_id: Source ID converted with int.
        :param dst_id: Destination ID converted with int.
        :return: New dict from the link Row's snapshot payload.
        :raises KeyError: The directed pair has no cached link Row.
        :raises RuntimeError: The pair has several cached link records.
        """

        row = cast(Any, self.link_table).get_link_row(int(src_id), int(dst_id))
        if row is None:
            raise KeyError((int(src_id), int(dst_id)))
        return dict(row.row_dict)

    def _column_for_extra(self, extra_name: str) -> Optional[str]:
        """
        Resolve a supported logical link property to its physical column name.

        priority and type use the link specification directly without checking
        headings. Other recognized names use the first heading with the corresponding
        underscore suffix. Unknown names return None. Heading lookup is best-effort
        in the link table and can hide database errors with an empty list.

        Example:
            origin resolves the first column ending in _origin; a property named
            unknown has no matching suffix rule.


        :param extra_name: Logical name such as priority, type, primary, origin, policy, data or index.
        :return: Configured/matching column name, or None when unsupported or unavailable.
        """

        link_table = self._link_table_cache()
        if extra_name == "priority":
            return link_table.link_spec.priority_link_col
        if extra_name == "type":
            return link_table.link_spec.type_link_col

        suffixes = {
            "primary": "_primary",
            "origin": "_origin",
            "policy": "_policy",
            "data": "_data",
            "index": "_index",
        }
        suffix = suffixes.get(str(extra_name))
        if suffix is None:
            return None
        for candidate in link_table.column_headings:
            if candidate.endswith(suffix):
                return candidate
        return None

    def _update_link_row_columns(
        self,
        src_id: int,
        dst_id: int,
        updates: dict[str, Any],
    ) -> None:
        """
        Merge physical column updates into one cached link Row and reload link records.

        An empty mapping returns before database or link validation. Otherwise
        require a database and a unique cached pair Row, merge the supplied values
        and write. Reload failure can follow a successful database update; relation
        field dictionaries are not rebuilt here.

        Example:
            A successful priority-column write reloads link ordering before the
            caller later rebinds this relation field.


        :param src_id: Source ID used for singular pair lookup.
        :param dst_id: Destination ID used for singular pair lookup.
        :param updates: Column/value dict merged without filtering identity or endpoint columns.
        :return: None; a nonempty mapping calls driver update_row then link_table.read.
        """

        if not updates:
            return
        self._db = _ensure_db(self._db)
        row_dict = self._link_row_snapshot(src_id, dst_id)
        row_dict.update(updates)
        self._db.driver_wrapper.update_row(row_dict)
        self.link_table.read(self._db)

    def _value_from_link_property(
        self,
        row_dict: dict[str, Any],
        property_name: str,
    ) -> Any:
        """
        Read a logical property from an existing physical link-row dictionary.

        No coercion, value validation or link-row lookup is added.

        Example:
            A recognized column whose stored value is False returns False rather
            than the None fallback.


        :param row_dict: Physical link payload already selected by the caller.
        :param property_name: Logical property name resolved through _column_for_extra.
        :return: Stored value, or None for unsupported properties or absent column keys.
        """

        column_name = self._column_for_extra(property_name)
        if column_name is None:
            return None
        return row_dict.get(column_name)

    def _build_link_properties(
        self,
        props_cls: type[Any],
        src_id: int,
        dst_id: int,
    ) -> Any:
        """
        Construct a property dataclass from one cached link Row and endpoint identities.

        Fetch a singular link-row snapshot first, then inspect dataclasses.fields.
        Fill src_table/src_table_id/dst_table/dst_table_id from this field and supplied
        IDs; every other field uses property resolution, possibly None. Defaults are
        not used to replace missing property values. Unsupported dataclass or
        constructor shapes and row-selection errors propagate.

        Example:
            A property dataclass's origin field receives None when no origin column
            is supported, even if that dataclass declares another default.


        :param props_cls: Dataclass type accepting its discovered fields as keyword arguments.
        :param src_id: Source ID used for row selection and src_table_id.
        :param dst_id: Destination ID used for row selection and dst_table_id.
        :return: New props_cls instance populated from endpoint names/IDs and logical properties.
        """

        row_dict = self._link_row_snapshot(src_id, dst_id)
        kwargs: dict[str, Any] = {}
        for dc_field in dataclasses.fields(props_cls):
            if dc_field.name == "src_table":
                kwargs[dc_field.name] = self.src_table_name
            elif dc_field.name == "src_table_id":
                kwargs[dc_field.name] = int(src_id)
            elif dc_field.name == "dst_table":
                kwargs[dc_field.name] = self.dst_table_name
            elif dc_field.name == "dst_table_id":
                kwargs[dc_field.name] = int(dst_id)
            else:
                kwargs[dc_field.name] = self._value_from_link_property(row_dict, dc_field.name)
        return props_cls(**kwargs)

    def _set_link_properties(self, updated_link_properties: Any) -> None:
        """
        Write supported non-None properties for supplied endpoint IDs and rebind values.

        Ignore None properties, preserving existing values; false and zero remain
        updates. Resolve IDs from the object but do not check its table-name labels
        against this field. The physical row update/reload precedes the final relation
        read, so later failures can follow persistence. Use set_extra to explicitly
        write None to a supported column.

        Example:
            An object containing only endpoint IDs performs no column write but
            still requests the final relation read.


        :param updated_link_properties: Property object carrying src_table_id/dst_table_id and optional logical values.
        :return: None; applies nonempty column updates then calls read even when none were selected.
        """

        updates: dict[str, Any] = {}
        for property_name in ("priority", "type", "primary", "origin", "policy", "data", "index"):
            column_name = self._column_for_extra(property_name)
            if column_name is None or not hasattr(updated_link_properties, property_name):
                continue
            value = getattr(updated_link_properties, property_name)
            if value is None:
                continue
            updates[column_name] = value

        self._update_link_row_columns(
            int(updated_link_properties.src_table_id),
            int(updated_link_properties.dst_table_id),
            updates,
        )
        self.read(self._db)

    def get_extra(
        self,
        src_id: int,
        dst_id: int,
        extra_type: Any,
    ) -> Optional[str | bool | int]:
        """
        Read a supported logical property from one unique cached association.

        Resolve the pair Row before checking whether the property name is
        supported. A missing/ambiguous pair therefore raises even for an unknown
        property; a valid pair with an unsupported property returns None.

        Example:
            Reading extra type origin returns the stored origin value if its suffix
            column exists, otherwise None after validating the pair.


        :param src_id: Source ID converted with int.
        :param dst_id: Destination ID converted with int.
        :param extra_type: Logical property selector converted with str.
        :return: Stored property value or None; the return annotation does not coerce the value.
        """

        row_dict = self._link_row_snapshot(int(src_id), int(dst_id))
        column_name = self._column_for_extra(str(extra_type))
        if column_name is None:
            return None
        return cast(Optional[str | bool | int], row_dict.get(column_name))

    def set_extra(
        self,
        src_id: int,
        dst_id: int,
        extra_type: Any,
        new_extra_value: Optional[str | bool | int],
    ) -> None:
        """
        Write one supported logical property, including None, then rebind relation values.

        Resolve the property column first and reject unsupported names before
        pair lookup. Unlike bulk property setters, None is forwarded as an update.
        Driver constraints and post-write reload/rebind failures propagate.

        Example:
            ``field.set_extra(1, 7, "origin", None)`` requests clearing a supported
            origin column rather than skipping it.


        :param src_id: Source ID converted with int after property-column resolution.
        :param dst_id: Destination ID converted with int after property-column resolution.
        :param extra_type: Logical property selector converted with str.
        :param new_extra_value: Value written unchanged, including None for an explicit clear.
        :return: None; writes and reloads the unique link Row, then calls read on this field.
        :raises KeyError: The logical property is unsupported or the pair has no cached Row.
        :raises RuntimeError: The pair is ambiguous or no database is attached.
        """

        column_name = self._column_for_extra(str(extra_type))
        if column_name is None:
            raise KeyError(str(extra_type))
        self._update_link_row_columns(int(src_id), int(dst_id), {column_name: new_extra_value})
        self.read(self._db)
