"""
Cache oriented link records with eager source, destination and pair indexes.

Physical rows are deep-copied into records and projected into cardinality
values or read-only Row snapshots. Endpoint names/columns define the view
orientation independently of the physical table. Legacy update hooks write
through the driver and reload afterward without an enclosing transaction.
"""

from __future__ import annotations

from collections import defaultdict
from copy import deepcopy

from typing import Any, Mapping, Optional, Sequence, cast

from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.base_table import (
    TableMetadata,
    TableTypes,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.many_many_tables import (
    ManyManyLink,
    StorageCacheManyToManyLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.many_one_tables import (
    ManyOneLink,
    StorageCacheManyToOneLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.one_many_tables import (
    OneManyLink,
    StorageCacheOneToManyLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.one_one_tables import (
    OneOneLink,
    StorageCacheOneToOneLinkTable,
)
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.databases.schema_specs import LinkCardinality, StorageLinkSpec

from LiuXin_alpha.caches.cache_plugins.schema_backed.common import (
    _CachedLinkRecord,
    _column_type_map,
    _ensure_db,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.single_table import (
    SchemaBackedMainTableCache,
)


class SchemaBackedLinkTable(
    StorageCacheOneToOneLinkTable[Any],
    StorageCacheOneToManyLinkTable,
    StorageCacheManyToOneLinkTable,
    StorageCacheManyToManyLinkTable,
):
    """
    Expose one directed view of a physical link table through all cardinality APIs.

    Retain endpoint tables and physical link-column names at construction.
    read eagerly builds three record indexes and determines cardinality from
    explicit schema, SQLite uniqueness metadata or observed endpoint repeats.
    A reverse view must supply swapped endpoints/columns; the stored cardinality
    declaration is not reversed automatically.

    Getters read snapshots until reload. Metadata headings/types can still query
    the attached database. Legacy writes consult existing cached records even
    after issuing mutations, with no transaction or rollback around the sequence.

    Example:
        >>> issubclass(SchemaBackedLinkTable, StorageCacheManyToManyLinkTable)
        True
    """

    def __init__(
        self,
        *,
        db: Any,
        link_spec: StorageLinkSpec,
        src_table: SchemaBackedMainTableCache,
        dst_table: SchemaBackedMainTableCache,
        src_table_name: str,
        dst_table_name: str,
        src_link_col: str,
        dst_link_col: str,
    ) -> None:
        """
        Attach directed endpoint metadata and initialize empty eager link indexes.

        Set initial type to MANY_MANY until read infers the actual type. Priority
        and typed flags reflect whether corresponding schema columns are truthy.
        Mark interlink versus intralink metadata by comparing endpoint names.

        Example:
            A newly constructed view has empty pair/source/destination dictionaries
            even when its physical database table already contains links.


        :param db: Database reference retained by the base table API.
        :param link_spec: Physical link schema retained by reference.
        :param src_table: Source main-table cache for this orientation.
        :param dst_table: Destination main-table cache for this orientation.
        :param src_table_name: Source table name used by this view.
        :param dst_table_name: Destination table name used by this view.
        :param src_link_col: Physical column containing this view's source IDs.
        :param dst_link_col: Physical column containing this view's destination IDs.
        :return: None; allocates empty records/indexes without reading physical rows.
        """

        metadata = TableMetadata(
            table_name=link_spec.link_table,
            main_table=False,
            is_interlink=src_table_name != dst_table_name,
            is_intralink=src_table_name == dst_table_name,
        )
        super().__init__(table=link_spec.link_table, db=db, metadata=metadata)
        self.link_spec = link_spec
        self._src_table = src_table
        self._dst_table = dst_table
        self._src_table_name = src_table_name
        self._dst_table_name = dst_table_name
        self._src_link_col = src_link_col
        self._dst_link_col = dst_link_col
        self._records: list[_CachedLinkRecord] = []
        self._by_src: dict[int, list[_CachedLinkRecord]] = {}
        self._by_dst: dict[int, list[_CachedLinkRecord]] = {}
        self._by_pair: dict[tuple[int, int], list[_CachedLinkRecord]] = {}
        self._id_column: Optional[str] = None
        self._table_type = TableTypes.MANY_MANY
        self._priority = bool(link_spec.priority_link_col)
        self._typed = bool(link_spec.type_link_col)

    @property
    def primary_table(self) -> str:
        """
        Expose the source table name of this directed view.

        Example:
            For the books-to-tags view, primary_table is books even if another view
            of the same physical table uses the reverse orientation.


        :return: Configured source-table name, without querying the database.
        """

        return self._src_table_name

    @property
    def secondary_table(self) -> str:
        """
        Expose the destination table name of this directed view.

        Example:
            For the books-to-tags view, secondary_table is tags.


        :return: Configured destination-table name, without querying the database.
        """

        return self._dst_table_name

    @property
    def designated_secondary_col(self) -> str:
        """
        Choose the destination display/value column with an ID-column fallback.

        The fallback is taken from the stored link specification and is not
        reoriented or validated by this property.

        Example:
            A destination cache whose default_value_column is name uses name in
            the primary-ID-to-secondary-value mapping.


        :return: Truthy destination default_value_column, otherwise link_spec.secondary_id_col.
        """

        return self._dst_table.default_value_column or self.link_spec.secondary_id_col

    @property
    def column_headings(self) -> list[str]:
        """
        Read physical link-table column names with a best-effort empty fallback.

        This queries the attached database rather than a cached schema snapshot.
        Both retrieval and list-conversion failures are suppressed.

        Example:
            A detached or failing database produces [] here, allowing optional-column
            updates to be skipped.


        :return: New list of database column headings, or an empty list on any Exception.
        """

        try:
            return list(self.db.get_column_headings(self.table))
        except Exception:
            return []

    @property
    def column_types(self) -> dict[str, str]:
        """
        Read the physical link-table schema and project its column type names.

        Unlike column_headings, schema-discovery and type-mapping errors propagate.

        Example:
            A failed driver get_table_spec call remains an error rather than an
            empty type mapping.


        :return: New mapping of column name to declared type, affinity or UNKNOWN.
        """

        spec = self.db.driver_wrapper.get_table_spec(self.table)
        return _column_type_map(spec)

    def _row_from_snapshot(self, row_dict: Mapping[str, Any]) -> Row:
        """
        Wrap a deep-copied link payload as a read-only database Row.

        Copy the mapping and recursively copy its contents before Row construction.
        Row validation and copying failures propagate; this does not insert a row.

        Example:
            Changing a nested container in the original cached payload does not change
            the independently copied payload supplied to the returned Row.


        :param row_dict: Mapping of physical link-column values.
        :return: Read-only Row attached to the current database, with an independent payload.
        """

        return Row(database=self.db, row_dict=deepcopy(dict(row_dict)), read_only=True)

    def _record_matches(self, record: _CachedLinkRecord, type_filter: Optional[str]) -> bool:
        """
        Apply an optional exact link-type restriction to one cached record.

        Example:
            An explicit filter on an untyped table returns False even if a record
            happens to carry a link_type value.


        :param record: Cached record whose link_type is compared when filtering is active.
        :param type_filter: None accepts every record; another value requires a typed table and equality.
        :return: True when the record is accepted by the type filter.
        """

        if type_filter is None:
            return True
        if not self.typed:
            return False
        return record.link_type == type_filter

    def _ordered_records(
        self,
        records: Sequence[_CachedLinkRecord],
        *,
        require_ordering: bool = False,
    ) -> list[_CachedLinkRecord]:
        """
        Copy and order link records by priority or requested input sequence.

        With priority enabled, sort by negative float priority then sequence;
        None priority uses zero. This applies regardless of require_ordering.
        Otherwise preserve input list order unless sequence ordering was requested.

        Example:
            Priorities 3, None and -1 sort in that order; equal priorities use sequence.


        :param records: Records copied into a new list; record objects remain shared.
        :param require_ordering: True sorts by sequence when priority is disabled.
        :return: New ordered list preserving duplicate record entries.
        """

        ordered = list(records)
        if self.priority:
            ordered.sort(
                key=lambda record: (
                    -float(record.priority) if record.priority is not None else 0.0,
                    record.sequence,
                )
            )
        elif require_ordering:
            ordered.sort(key=lambda record: record.sequence)
        return ordered

    def _to_row_dicts(self) -> list[dict[str, Any]]:
        """
        Read all physical link rows as independent dictionaries.

        Prefer driver_wrapper.get_all_rows and convert each result with dict.
        Any AttributeError in that try block, including conversion/copying, selects
        db.get_all_rows(iterator_return=False) and copies each Row.row_dict. Other
        errors propagate; fallback errors are not retried.

        Example:
            A database lacking the driver bulk-row helper can use its Row-returning
            get_all_rows facade.


        :return: List of deep-copied row payload dictionaries from the selected database API.
        """

        try:
            rows = self.db.driver_wrapper.get_all_rows(self.table)
            return [deepcopy(dict(row)) for row in rows]
        except AttributeError:
            rows = self.db.get_all_rows(self.table, iterator_return=False)
            return [deepcopy(row.row_dict) for row in rows]

    def _unique_single_columns(self) -> set[str]:
        """
        Discover SQLite single-column unique indexes for cardinality fallback.

        Use database.conn cursor PRAGMA index_list/index_info. Multi-column and
        non-unique indexes are ignored. Any Exception discards even previously found
        columns; the cursor is not explicitly closed here. This best-effort probe
        does not constitute portable schema discovery or primary-key inference.

        Example:
            A two-column unique index over both endpoint columns does not mark either
            column individually unique.


        :return: Set of uniquely indexed column names, or empty when unavailable or any probe fails.
        """

        conn = getattr(self.db, "conn", None)
        if conn is None:
            return set()

        unique_columns: set[str] = set()
        try:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA index_list('{self.table}')")
            indexes = cursor.fetchall()
            for index in indexes:
                is_unique = bool(index[2])
                if not is_unique:
                    continue
                index_name = index[1]
                cursor.execute(f"PRAGMA index_info('{index_name}')")
                columns = [row[2] for row in cursor.fetchall()]
                if len(columns) == 1:
                    unique_columns.add(columns[0])
        except Exception:
            return set()
        return unique_columns

    def _infer_table_type(self, records: Sequence[_CachedLinkRecord]) -> TableTypes:
        """
        Select cardinality from the link specification, uniqueness or observed repeats.

        Honor ONE_TO_ONE, ONE_TO_MANY, MANY_TO_ONE and MANY_TO_MANY declarations
        without validating rows or reversing the declaration. Otherwise unique source
        and destination columns imply ONE_ONE; only source uniqueness implies MANY_ONE,
        only destination uniqueness ONE_MANY. Finally classify repeated endpoint IDs
        in the supplied records; an empty sample therefore infers ONE_ONE.

        Example:
            Unknown cardinality with repeated source IDs and unique destination IDs
            infers ONE_MANY when no earlier unique-index rule applies.


        :param records: Cached link records used only after explicit metadata and unique-index checks.
        :return: TableTypes value for this view.
        """

        cardinality = self.link_spec.cardinality
        if cardinality == LinkCardinality.ONE_TO_ONE:
            return TableTypes.ONE_ONE
        if cardinality == LinkCardinality.ONE_TO_MANY:
            return TableTypes.ONE_MANY
        if cardinality == LinkCardinality.MANY_TO_ONE:
            return TableTypes.MANY_ONE
        if cardinality == LinkCardinality.MANY_TO_MANY:
            return TableTypes.MANY_MANY

        unique_columns = self._unique_single_columns()
        src_unique = self._src_link_col in unique_columns
        dst_unique = self._dst_link_col in unique_columns
        if src_unique and dst_unique:
            return TableTypes.ONE_ONE
        if src_unique:
            return TableTypes.MANY_ONE
        if dst_unique:
            return TableTypes.ONE_MANY

        src_seen: set[int] = set()
        dst_seen: set[int] = set()
        src_duplicate = False
        dst_duplicate = False

        for record in records:
            if record.src_id in src_seen:
                src_duplicate = True
            src_seen.add(record.src_id)
            if record.dst_id in dst_seen:
                dst_duplicate = True
            dst_seen.add(record.dst_id)

        if not src_duplicate and not dst_duplicate:
            return TableTypes.ONE_ONE
        if src_duplicate and not dst_duplicate:
            return TableTypes.ONE_MANY
        if not src_duplicate and dst_duplicate:
            return TableTypes.MANY_ONE
        return TableTypes.MANY_MANY

    def _rebuild_indices(self, row_dicts: Sequence[Mapping[str, Any]]) -> None:
        """
        Copy physical link payloads into records and rebuild all directed indexes.

        Discover the physical ID column first. Skip rows with missing/None
        endpoints; convert accepted endpoints and non-None row IDs with int. Priority
        conversion failures become None, and non-None link types become strings.
        Retain each original input position as sequence and preserve duplicate links.

        Build source, destination and pair lists eagerly in input order. Publish
        records/indexes before cardinality inference; inference failure can leave
        new records paired with the previous type. The ID-column reference can
        already change before earlier conversion failures.

        Example:
            Two accepted physical rows for one pair produce two pair-index records;
            a later singular getter can reject that ambiguity.


        :param row_dicts: Physical row mappings copied recursively before endpoint extraction.
        :return: None; replaces records/indexes and then infers the view's cardinality.
        """

        self._id_column = self.db.driver_wrapper.get_id_column(self.table)
        records: list[_CachedLinkRecord] = []
        for sequence, row_dict in enumerate(row_dicts):
            row_copy = deepcopy(dict(row_dict))
            src_id = row_copy.get(self._src_link_col)
            dst_id = row_copy.get(self._dst_link_col)
            if src_id is None or dst_id is None:
                continue
            row_id = row_copy.get(self._id_column) if self._id_column else None
            priority = None
            if self.link_spec.priority_link_col:
                priority_value = row_copy.get(self.link_spec.priority_link_col)
                if priority_value is not None:
                    try:
                        priority = float(priority_value)
                    except (TypeError, ValueError):
                        priority = None
            link_type = None
            if self.link_spec.type_link_col:
                raw_type = row_copy.get(self.link_spec.type_link_col)
                if raw_type is not None:
                    link_type = str(raw_type)
            records.append(
                _CachedLinkRecord(
                    src_id=int(src_id),
                    dst_id=int(dst_id),
                    row_dict=row_copy,
                    row_id=int(row_id) if row_id is not None else None,
                    link_type=link_type,
                    priority=priority,
                    sequence=sequence,
                )
            )

        by_src: dict[int, list[_CachedLinkRecord]] = defaultdict(list)
        by_dst: dict[int, list[_CachedLinkRecord]] = defaultdict(list)
        by_pair: dict[tuple[int, int], list[_CachedLinkRecord]] = defaultdict(list)

        for record in records:
            by_src[record.src_id].append(record)
            by_dst[record.dst_id].append(record)
            by_pair[(record.src_id, record.dst_id)].append(record)

        self._records = records
        self._by_src = {key: list(value) for key, value in by_src.items()}
        self._by_dst = {key: list(value) for key, value in by_dst.items()}
        self._by_pair = {key: list(value) for key, value in by_pair.items()}
        self._table_type = self._infer_table_type(records)

    def read(self, db: Any) -> None:
        """
        Attach the selected database and rebuild records from all physical link rows.

        Retain the database before reading. _to_row_dicts supplies deep-copied
        rows; _rebuild_indices copies them again into independent record payloads.
        No endpoint-table or relation-field refresh is performed.

        Example:
            After an external link insert, read makes it visible to pair lookups.


        :param db: Database override; None selects the current table attachment.
        :return: None; replaces records and eager indexes and determines cardinality.
        :raises RuntimeError: No explicit or attached database is available.
        """

        db = _ensure_db(self.db, db)
        self.db = db
        self._rebuild_indices(self._to_row_dicts())

    def reload(self, db: Any) -> None:
        """
        Perform the same complete link-table read as read.

        Example:
            ``link_table.reload(link_table.db)`` observes external link-table writes.


        :param db: Database override; None reuses the current database.
        :return: None; rebuilds records, eager indexes and cardinality through read.
        """

        self.read(db)

    def _records_for_src(
        self,
        src_id: int,
        *,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> list[_CachedLinkRecord]:
        """
        Filter and order cached records for one source ID.

        Use the eager source index without a database read or endpoint
        existence check, then apply _ordered_records. Configured priority ordering
        applies even when require_ordering is false.

        Example:
            An unmatched type filter produces [] while leaving cached records intact.


        :param src_id: Source ID converted with int.
        :param require_ordering: Request sequence ordering when priority is disabled.
        :param type_filter: Optional exact type filter; untyped tables reject non-None filters.
        :return: New list of accepted shared record objects; empty for an absent endpoint key.
        """

        matches = [record for record in self._by_src.get(int(src_id), []) if self._record_matches(record, type_filter)]
        return self._ordered_records(matches, require_ordering=require_ordering)

    def _records_for_dst(
        self,
        dst_id: int,
        *,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> list[_CachedLinkRecord]:
        """
        Filter and order cached records for one destination ID.

        Use the eager destination index without a database read or endpoint
        existence check, then apply _ordered_records. Configured priority ordering
        applies even when require_ordering is false.

        Example:
            An unmatched type filter produces [] while leaving cached records intact.


        :param dst_id: Destination ID converted with int.
        :param require_ordering: Request sequence ordering when priority is disabled.
        :param type_filter: Optional exact type filter; untyped tables reject non-None filters.
        :return: New list of accepted shared record objects; empty for an absent endpoint key.
        """

        matches = [record for record in self._by_dst.get(int(dst_id), []) if self._record_matches(record, type_filter)]
        return self._ordered_records(matches, require_ordering=require_ordering)

    def _records_for_pair(
        self,
        src_id: int,
        dst_id: int,
        *,
        type_filter: Optional[str] = None,
    ) -> list[_CachedLinkRecord]:
        """
        Filter and order cached records for one directed endpoint pair.

        Use the eager pair index. Without priority, preserve its original row
        sequence. Do not check endpoint existence or enforce singularity.

        Example:
            Two typed rows can be narrowed by a type filter before a caller checks singularity.


        :param src_id: Source identity converted with int.
        :param dst_id: Destination identity converted with int.
        :param type_filter: Optional exact type filter; untyped tables reject non-None filters.
        :return: New list of shared pair records, ordered by priority when enabled.
        """

        matches = [
            record
            for record in self._by_pair.get((int(src_id), int(dst_id)), [])
            if self._record_matches(record, type_filter)
        ]
        return self._ordered_records(matches)

    def _make_link_object(self, record: _CachedLinkRecord) -> Any:
        """
        Project a cached record into the link value type for the current cardinality.

        ONE_ONE omits priority; other recognized cardinalities include it. Any
        remaining table type uses ManyManyLink. The physical row payload is not
        attached to the result and constructor errors propagate.

        Example:
            A ONE_ONE record with a stored priority still produces a OneOneLink
            without a priority argument.


        :param record: Cached record supplying endpoint, row identity, type and priority values.
        :return: OneOneLink, OneManyLink, ManyOneLink or ManyManyLink value.
        """

        if self.table_type == TableTypes.ONE_ONE:
            return OneOneLink(
                src_id=record.src_id,
                dst_id=record.dst_id,
                link_row_id=record.row_id,
                link_type=record.link_type,
            )
        if self.table_type == TableTypes.ONE_MANY:
            return OneManyLink(
                src_id=record.src_id,
                dst_id=record.dst_id,
                link_row_id=record.row_id,
                link_type=record.link_type,
                priority=record.priority,
            )
        if self.table_type == TableTypes.MANY_ONE:
            return ManyOneLink(
                src_id=record.src_id,
                dst_id=record.dst_id,
                link_row_id=record.row_id,
                link_type=record.link_type,
                priority=record.priority,
            )
        return ManyManyLink(
            src_id=record.src_id,
            dst_id=record.dst_id,
            link_row_id=record.row_id,
            link_type=record.link_type,
            priority=record.priority,
        )

    def _optional_single(
        self,
        records: Sequence[_CachedLinkRecord],
        *,
        insist_on_singular: bool = True,
    ) -> Optional[_CachedLinkRecord]:
        """
        Return an optional first record while enforcing singularity when requested.

        Example:
            >>> SchemaBackedLinkTable._optional_single(None, []) is None
            True
            >>> SchemaBackedLinkTable._optional_single(None, ["first", "next"], insist_on_singular=False)
            'first'


        :param records: Ordered record sequence to inspect.
        :param insist_on_singular: True rejects more than one record; false selects the first.
        :return: None for an empty sequence, otherwise the first record object unchanged.
        :raises RuntimeError: Several records exist and insist_on_singular is true.
        """

        if not records:
            return None
        if insist_on_singular and len(records) > 1:
            raise RuntimeError(f"Expected one link row, found {len(records)}")
        return records[0]

    def _src_row(self, src_id: int) -> Optional[Row]:
        """
        Read a source Row only when its main-table cache contains the ID.

        The endpoint table owns Row snapshot construction. Errors after a positive
        membership check still propagate; no database fallback is performed here.

        Example:
            A dangling cached link can yield no source Row even though the link
            record itself is still present.


        :param src_id: Source ID forwarded to the endpoint table helpers.
        :return: Endpoint Row, or None when has_id is false.
        """

        if not self._src_table.has_id(src_id):
            return None
        return self._src_table.get_row(src_id)

    def _dst_row(self, dst_id: int) -> Optional[Row]:
        """
        Read a destination Row only when its main-table cache contains the ID.

        The endpoint table owns Row snapshot construction. Errors after a positive
        membership check still propagate; no database fallback is performed here.

        Example:
            A dangling cached link can yield no destination Row even though the link
            record itself is still present.


        :param dst_id: Destination ID forwarded to the endpoint table helpers.
        :return: Endpoint Row, or None when has_id is false.
        """

        if not self._dst_table.has_id(dst_id):
            return None
        return self._dst_table.get_row(dst_id)

    def has_link(
        self,
        src_id: int,
        dst_id: int,
        type_filter: Optional[str] = None,
    ) -> bool:
        """
        Test whether any cached row matches a directed endpoint pair.

        Example:
            A duplicate pair still returns True; use get_link with singularity
            enforcement when exactly one physical association is required.


        :param src_id: Source row identity.
        :param dst_id: Destination row identity.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: Boolean presence of accepted pair records, without endpoint existence checks.
        """

        return bool(self._records_for_pair(src_id, dst_id, type_filter=type_filter))

    def has_src(
        self,
        dst_id: int,
        type_filter: Optional[str] = None,
    ) -> bool:
        """
        Test whether the given destination has any accepted source links.

        This tests link records, not the presence of the opposite endpoint in its
        main-table cache. Singular and plural spellings have the same boolean behavior.

        Example:
            A dangling link can satisfy this predicate even when retrieving the
            linked endpoint Row would return None.


        :param dst_id: Destination ID used for cached record lookup.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: True when at least one accepted cached link row exists.
        """

        return bool(self._records_for_dst(dst_id, type_filter=type_filter))

    def has_dst(
        self,
        src_id: int,
        type_filter: Optional[str] = None,
    ) -> bool:
        """
        Test whether the given source has any accepted destination links.

        This tests link records, not the presence of the opposite endpoint in its
        main-table cache. Singular and plural spellings have the same boolean behavior.

        Example:
            A dangling link can satisfy this predicate even when retrieving the
            linked endpoint Row would return None.


        :param src_id: Source ID used for cached record lookup.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: True when at least one accepted cached link row exists.
        """

        return bool(self._records_for_src(src_id, type_filter=type_filter))

    def has_dsts(
        self,
        src_id: int,
        type_filter: Optional[str] = None,
    ) -> bool:
        """
        Test whether the given source has any accepted destination links.

        This tests link records, not the presence of the opposite endpoint in its
        main-table cache. Singular and plural spellings have the same boolean behavior.

        Example:
            A dangling link can satisfy this predicate even when retrieving the
            linked endpoint Row would return None.


        :param src_id: Source ID used for cached record lookup.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: True when at least one accepted cached link row exists.
        """

        return self.has_dst(src_id, type_filter=type_filter)

    def has_srcs(
        self,
        dst_id: int,
        type_filter: Optional[str] = None,
    ) -> bool:
        """
        Test whether the given destination has any accepted source links.

        This tests link records, not the presence of the opposite endpoint in its
        main-table cache. Singular and plural spellings have the same boolean behavior.

        Example:
            A dangling link can satisfy this predicate even when retrieving the
            linked endpoint Row would return None.


        :param dst_id: Destination ID used for cached record lookup.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: True when at least one accepted cached link row exists.
        """

        return self.has_src(dst_id, type_filter=type_filter)

    def get_link(
        self,
        src_id: int,
        dst_id: int,
        insist_on_singular: bool = True,
        type_filter: Optional[str] = None,
    ) -> Optional[Any]:
        """
        Return one optional link value for a directed pair.

        Selection uses cached pair records in priority order when enabled.
        Filter by optional type before enforcing singularity. With singularity
        disabled, select the first ordered record; endpoint existence is not checked.

        Example:
            Request link-row snapshots when physical association columns are needed;
            link values carry endpoint identities and the supported association metadata.


        :param src_id: Source ID for the directed pair.
        :param dst_id: Destination ID for the directed pair.
        :param insist_on_singular: True rejects multiple matching rows; false selects the first.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: Cardinality-specific link value, or None when no accepted link exists.
        :raises RuntimeError: Multiple accepted pair records exist and singularity is required.
        """

        record = self._optional_single(
            self._records_for_pair(src_id, dst_id, type_filter=type_filter),
            insist_on_singular=insist_on_singular,
        )
        if record is None:
            return None
        return self._make_link_object(record)

    def get_links(
        self,
        src_id: int,
        dst_id: int,
    ) -> Sequence[Any]:
        """
        Return all link values for a directed pair.

        Selection uses cached pair records in priority order when enabled.
        All link types are included; this plural pair method has no type-filter argument.

        Example:
            Request link-row snapshots when physical association columns are needed;
            link values carry endpoint identities and the supported association metadata.


        :param src_id: Source ID for the directed pair.
        :param dst_id: Destination ID for the directed pair.
        :return: List of cardinality-specific link values, empty for no pair matches.
        """

        return [self._make_link_object(record) for record in self._records_for_pair(src_id, dst_id)]

    def get_link_for_src(
        self,
        src_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[Any]:
        """
        Project one optional source-selected link record.

        Read cached link records with exact optional type filtering. Priority
        ordering applies whenever enabled, independent of the compatibility hint.
        Require at most one matching record; this method has no option to pick
        an arbitrary first row when several associations exist.

        Example:
            A non-None type filter on an untyped table yields None.


        :param src_id: Source ID for cached record selection.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: One cardinality-specific link value, or None when no record matches.
        :raises RuntimeError: More than one accepted link record exists for this endpoint.
        """

        record = self._optional_single(self._records_for_src(src_id, type_filter=type_filter))
        if record is None:
            return None
        return self._make_link_object(record)

    def get_links_for_src(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Any]:
        """
        Project all source-selected link records.

        Read cached link records with exact optional type filtering. Priority
        ordering applies whenever enabled, independent of the compatibility hint.
        Retain repeated physical links; do not deduplicate endpoint IDs.

        Example:
            A non-None type filter on an untyped table yields an empty list.


        :param src_id: Source ID for cached record selection.
        :param require_ordering: Request sequence order when unprioritized; configured priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of cardinality-specific link values, empty when no records match.
        """

        return [
            self._make_link_object(record)
            for record in self._records_for_src(src_id, require_ordering=require_ordering, type_filter=type_filter)
        ]

    def get_link_for_dst(
        self,
        dst_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[Any]:
        """
        Project one optional destination-selected link record.

        Read cached link records with exact optional type filtering. Priority
        ordering applies whenever enabled, independent of the compatibility hint.
        Require at most one matching record; this method has no option to pick
        an arbitrary first row when several associations exist.

        Example:
            A non-None type filter on an untyped table yields None.


        :param dst_id: Destination ID for cached record selection.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: One cardinality-specific link value, or None when no record matches.
        :raises RuntimeError: More than one accepted link record exists for this endpoint.
        """

        record = self._optional_single(self._records_for_dst(dst_id, type_filter=type_filter))
        if record is None:
            return None
        return self._make_link_object(record)

    def get_links_for_dst(
        self,
        dst_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Any]:
        """
        Project all destination-selected link records.

        Read cached link records with exact optional type filtering. Priority
        ordering applies whenever enabled, independent of the compatibility hint.
        Retain repeated physical links; do not deduplicate endpoint IDs.

        Example:
            A non-None type filter on an untyped table yields an empty list.


        :param dst_id: Destination ID for cached record selection.
        :param require_ordering: Request sequence order when unprioritized; configured priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of cardinality-specific link values, empty when no records match.
        """

        return [
            self._make_link_object(record)
            for record in self._records_for_dst(dst_id, require_ordering=require_ordering, type_filter=type_filter)
        ]

    def get_link_row(
        self,
        src_id: int,
        dst_id: int,
        insist_on_singular: bool = True,
        type_filter: Optional[str] = None,
    ) -> Optional[Row]:
        """
        Return one optional link-row snapshot for a directed pair.

        Selection uses cached pair records in priority order when enabled.
        Filter by optional type before enforcing singularity. With singularity
        disabled, select the first ordered record; endpoint existence is not checked.

        Example:
            Request link-row snapshots when physical association columns are needed;
            link values carry endpoint identities and the supported association metadata.


        :param src_id: Source ID for the directed pair.
        :param dst_id: Destination ID for the directed pair.
        :param insist_on_singular: True rejects multiple matching rows; false selects the first.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: Deep-copied read-only Row, or None when no accepted link exists.
        :raises RuntimeError: Multiple accepted pair records exist and singularity is required.
        """

        record = self._optional_single(
            self._records_for_pair(src_id, dst_id, type_filter=type_filter),
            insist_on_singular=insist_on_singular,
        )
        if record is None:
            return None
        return self._row_from_snapshot(record.row_dict)

    def get_link_rows(
        self,
        src_id: int,
        dst_id: int,
    ) -> Sequence[Row]:
        """
        Return all link-row snapshots for a directed pair.

        Selection uses cached pair records in priority order when enabled.
        All link types are included; this plural pair method has no type-filter argument.

        Example:
            Request link-row snapshots when physical association columns are needed;
            link values carry endpoint identities and the supported association metadata.


        :param src_id: Source ID for the directed pair.
        :param dst_id: Destination ID for the directed pair.
        :return: List of deep-copied read-only Rows, empty for no pair matches.
        """

        return [self._row_from_snapshot(record.row_dict) for record in self._records_for_pair(src_id, dst_id)]

    def get_link_row_for_src(
        self,
        src_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[Row]:
        """
        Project one optional source-selected link record.

        Read cached link records with exact optional type filtering. Priority
        ordering applies whenever enabled, independent of the compatibility hint.
        Require at most one matching record; this method has no option to pick
        an arbitrary first row when several associations exist.

        Example:
            A non-None type filter on an untyped table yields None.


        :param src_id: Source ID for cached record selection.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: One read-only Row snapshot, or None when no record matches.
        :raises RuntimeError: More than one accepted link record exists for this endpoint.
        """

        record = self._optional_single(self._records_for_src(src_id, type_filter=type_filter))
        if record is None:
            return None
        return self._row_from_snapshot(record.row_dict)

    def get_link_rows_for_src(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Row]:
        """
        Project all source-selected link records.

        Read cached link records with exact optional type filtering. Priority
        ordering applies whenever enabled, independent of the compatibility hint.
        Retain repeated physical links; do not deduplicate endpoint IDs.

        Example:
            A non-None type filter on an untyped table yields an empty list.


        :param src_id: Source ID for cached record selection.
        :param require_ordering: Request sequence order when unprioritized; configured priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of read-only Row snapshots with deep-copied payloads, empty when no records match.
        """

        return [
            self._row_from_snapshot(record.row_dict)
            for record in self._records_for_src(src_id, require_ordering=require_ordering, type_filter=type_filter)
        ]

    def get_link_row_for_dst(
        self,
        dst_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[Row]:
        """
        Project one optional destination-selected link record.

        Read cached link records with exact optional type filtering. Priority
        ordering applies whenever enabled, independent of the compatibility hint.
        Require at most one matching record; this method has no option to pick
        an arbitrary first row when several associations exist.

        Example:
            A non-None type filter on an untyped table yields None.


        :param dst_id: Destination ID for cached record selection.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: One read-only Row snapshot, or None when no record matches.
        :raises RuntimeError: More than one accepted link record exists for this endpoint.
        """

        record = self._optional_single(self._records_for_dst(dst_id, type_filter=type_filter))
        if record is None:
            return None
        return self._row_from_snapshot(record.row_dict)

    def get_link_rows_for_dst(
        self,
        dst_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Row]:
        """
        Project all destination-selected link records.

        Read cached link records with exact optional type filtering. Priority
        ordering applies whenever enabled, independent of the compatibility hint.
        Retain repeated physical links; do not deduplicate endpoint IDs.

        Example:
            A non-None type filter on an untyped table yields an empty list.


        :param dst_id: Destination ID for cached record selection.
        :param require_ordering: Request sequence order when unprioritized; configured priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of read-only Row snapshots with deep-copied payloads, empty when no records match.
        """

        return [
            self._row_from_snapshot(record.row_dict)
            for record in self._records_for_dst(dst_id, require_ordering=require_ordering, type_filter=type_filter)
        ]

    def get_src_id(
        self,
        dst_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[int]:
        """
        Read the unique optional source ID linked to a destination.

        Singularity counts physical cached records, so duplicate links to the
        same endpoint still raise. The returned ID need not exist in its main table.

        Example:
            Two accepted links to the same source still violate this singular
            lookup; use the plural ID method to retain both.


        :param dst_id: Destination ID whose linked source values are requested.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: Integer endpoint ID, or None when no accepted link exists.
        :raises RuntimeError: More than one accepted link record exists.
        """

        record = self._optional_single(self._records_for_dst(dst_id, type_filter=type_filter))
        return None if record is None else record.src_id

    def get_src_ids(
        self,
        dst_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[int]:
        """
        List linked source IDs in cached association order.

        Priority order is descending when enabled even with require_ordering=False.
        An absent endpoint key or unmatched type filter gives an empty list.

        Example:
            Two links to source ID 7 produce [7, 7], without deduplication.


        :param dst_id: Destination ID whose linked source values are requested.
        :param require_ordering: Request sequence order when unprioritized; configured priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of integer IDs, retaining duplicate links and dangling endpoints.
        """

        return [
            record.src_id
            for record in self._records_for_dst(dst_id, require_ordering=require_ordering, type_filter=type_filter)
        ]

    def get_src_row(
        self,
        dst_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[Row]:
        """
        Read the unique linked source Row when its main-table cache still contains it.

        Resolve a singular link before checking endpoint membership. Row creation
        and endpoint-table failures propagate.

        Example:
            A dangling unique link returns None here even though get_src_id
            can still return its endpoint ID.


        :param dst_id: Destination ID whose linked source values are requested.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: Source Row, or None for no link or a missing linked endpoint.
        :raises RuntimeError: More than one accepted link record exists before endpoint lookup.
        """

        src_id = self.get_src_id(dst_id, type_filter=type_filter)
        if src_id is None:
            return None
        return self._src_row(src_id)

    def get_src_rows(
        self,
        dst_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Row]:
        """
        Materialize linked source Rows and omit absent endpoint rows.

        Use the plural ID lookup, then check each endpoint through its main-table
        cache. Missing endpoint Rows are skipped; failures for existing rows propagate.

        Example:
            If one linked ID has been deleted from its table cache, the returned Row
            list can be shorter than the corresponding plural ID list.


        :param dst_id: Destination ID whose linked source values are requested.
        :param require_ordering: Request sequence order when unprioritized; configured priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of endpoint Rows in link order, preserving duplicates for existing rows.
        """

        rows = []
        for src_id in self.get_src_ids(dst_id, require_ordering=require_ordering, type_filter=type_filter):
            row = self._src_row(src_id)
            if row is not None:
                rows.append(row)
        return rows

    def get_src_value(
        self,
        dst_id: int,
        src_column: str,
        type_filter: Optional[str] = None,
    ) -> Any:
        """
        Project one column from the unique linked source row snapshot.

        Enforce link singularity first, then get the endpoint snapshot without a
        has_id check. A dangling endpoint can raise KeyError; stored None and absent
        columns both return None.

        Example:
            A linked row lacking the requested column returns None, but a linked ID
            missing from the main-table cache can raise instead.


        :param dst_id: Destination ID whose linked source values are requested.
        :param src_column: Source column read with dict.get from its row snapshot.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: Column value, or None for no accepted link or an absent column.
        :raises RuntimeError: More than one accepted link record exists.
        :raises KeyError: The endpoint table cannot provide the linked row snapshot.
        """

        src_id = self.get_src_id(dst_id, type_filter=type_filter)
        if src_id is None:
            return None
        return self._src_table.get_row_snapshot(src_id).get(src_column)

    def get_src_values(
        self,
        dst_id: int,
        src_column: str,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Any]:
        """
        Project a column from each linked source row snapshot in link order.

        Unlike the Row-list method, do not filter out absent endpoint IDs before
        snapshot lookup. A dangling endpoint can raise, aborting the projection.

        Example:
            Two linked rows with values Alpha and None produce ["Alpha", None].


        :param dst_id: Destination ID whose linked source values are requested.
        :param src_column: Source column read with dict.get.
        :param require_ordering: Request sequence order when unprioritized; configured priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of column values, retaining duplicates and None for missing columns.
        :raises KeyError: A linked endpoint snapshot cannot be read.
        """

        return [
            self._src_table.get_row_snapshot(src_id).get(src_column)
            for src_id in self.get_src_ids(dst_id, require_ordering=require_ordering, type_filter=type_filter)
        ]

    def get_src_ids_from_value(
        self,
        dst_value: Any,
        dst_column: str,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[int]:
        """
        Find source IDs linked to opposite-table rows matching a column value.

        Sort matching opposite endpoint IDs, then concatenate each endpoint's
        linked IDs with optional type filtering. Ordering is grouped by those sorted
        matching IDs, not a global priority sort. Endpoint index errors propagate.

        Example:
            If matching opposite IDs 2 and 5 link to [7, 8] and [7], return
            [7, 8, 7] in grouped traversal order.


        :param dst_value: Destination value passed to the opposite table's exact value index.
        :param dst_column: Destination column used for the value-index lookup.
        :param require_ordering: Request sequence order when unprioritized; configured priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of linked source IDs, retaining repetitions across rows and links.
        """

        src_ids: list[int] = []
        for dst_id in sorted(self._dst_table.get_ids_for_value(dst_column, dst_value)):
            src_ids.extend(self.get_src_ids(dst_id, require_ordering=require_ordering, type_filter=type_filter))
        return src_ids

    def get_src_rows_from_value(
        self,
        dst_value: Any,
        dst_column: str,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Row]:
        """
        Materialize source Rows linked to opposite-table value matches.

        Use get_src_ids_from_value and omit IDs absent from the source table.
        Repeated links still produce repeated Rows; no global ordering or deduplication
        is added. Snapshot construction failures propagate.

        Example:
            A matching value can lead to repeated Rows when several opposite
            endpoints share the same source link.


        :param dst_value: Destination value passed to the opposite table's exact value index.
        :param dst_column: Destination column used for the value-index lookup.
        :param require_ordering: Request sequence order when unprioritized; configured priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of existing endpoint Rows in grouped value-match/link order.
        """

        return [
            row
            for src_id in self.get_src_ids_from_value(
                dst_value,
                dst_column,
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
            for row in [self._src_row(src_id)]
            if row is not None
        ]

    def get_dst_id(
        self,
        src_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[int]:
        """
        Read the unique optional destination ID linked to a source.

        Singularity counts physical cached records, so duplicate links to the
        same endpoint still raise. The returned ID need not exist in its main table.

        Example:
            Two accepted links to the same destination still violate this singular
            lookup; use the plural ID method to retain both.


        :param src_id: Source ID whose linked destination values are requested.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: Integer endpoint ID, or None when no accepted link exists.
        :raises RuntimeError: More than one accepted link record exists.
        """

        record = self._optional_single(self._records_for_src(src_id, type_filter=type_filter))
        return None if record is None else record.dst_id

    def get_dst_ids(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[int]:
        """
        List linked destination IDs in cached association order.

        Priority order is descending when enabled even with require_ordering=False.
        An absent endpoint key or unmatched type filter gives an empty list.

        Example:
            Two links to destination ID 7 produce [7, 7], without deduplication.


        :param src_id: Source ID whose linked destination values are requested.
        :param require_ordering: Request sequence order when unprioritized; configured priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of integer IDs, retaining duplicate links and dangling endpoints.
        """

        return [
            record.dst_id
            for record in self._records_for_src(src_id, require_ordering=require_ordering, type_filter=type_filter)
        ]

    def get_dst_row(
        self,
        src_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[Row]:
        """
        Read the unique linked destination Row when its main-table cache still contains it.

        Resolve a singular link before checking endpoint membership. Row creation
        and endpoint-table failures propagate.

        Example:
            A dangling unique link returns None here even though get_dst_id
            can still return its endpoint ID.


        :param src_id: Source ID whose linked destination values are requested.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: Destination Row, or None for no link or a missing linked endpoint.
        :raises RuntimeError: More than one accepted link record exists before endpoint lookup.
        """

        dst_id = self.get_dst_id(src_id, type_filter=type_filter)
        if dst_id is None:
            return None
        return self._dst_row(dst_id)

    def get_dst_rows(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Row]:
        """
        Materialize linked destination Rows and omit absent endpoint rows.

        Use the plural ID lookup, then check each endpoint through its main-table
        cache. Missing endpoint Rows are skipped; failures for existing rows propagate.

        Example:
            If one linked ID has been deleted from its table cache, the returned Row
            list can be shorter than the corresponding plural ID list.


        :param src_id: Source ID whose linked destination values are requested.
        :param require_ordering: Request sequence order when unprioritized; configured priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of endpoint Rows in link order, preserving duplicates for existing rows.
        """

        rows = []
        for dst_id in self.get_dst_ids(src_id, require_ordering=require_ordering, type_filter=type_filter):
            row = self._dst_row(dst_id)
            if row is not None:
                rows.append(row)
        return rows

    def get_dst_value(
        self,
        src_id: int,
        dst_column: str,
        type_filter: Optional[str] = None,
    ) -> Any:
        """
        Project one column from the unique linked destination row snapshot.

        Enforce link singularity first, then get the endpoint snapshot without a
        has_id check. A dangling endpoint can raise KeyError; stored None and absent
        columns both return None.

        Example:
            A linked row lacking the requested column returns None, but a linked ID
            missing from the main-table cache can raise instead.


        :param src_id: Source ID whose linked destination values are requested.
        :param dst_column: Destination column read with dict.get from its row snapshot.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: Column value, or None for no accepted link or an absent column.
        :raises RuntimeError: More than one accepted link record exists.
        :raises KeyError: The endpoint table cannot provide the linked row snapshot.
        """

        dst_id = self.get_dst_id(src_id, type_filter=type_filter)
        if dst_id is None:
            return None
        return self._dst_table.get_row_snapshot(dst_id).get(dst_column)

    def get_dst_values(
        self,
        src_id: int,
        dst_column: str,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Any]:
        """
        Project a column from each linked destination row snapshot in link order.

        Unlike the Row-list method, do not filter out absent endpoint IDs before
        snapshot lookup. A dangling endpoint can raise, aborting the projection.

        Example:
            Two linked rows with values Alpha and None produce ["Alpha", None].


        :param src_id: Source ID whose linked destination values are requested.
        :param dst_column: Destination column read with dict.get.
        :param require_ordering: Request sequence order when unprioritized; configured priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of column values, retaining duplicates and None for missing columns.
        :raises KeyError: A linked endpoint snapshot cannot be read.
        """

        return [
            self._dst_table.get_row_snapshot(dst_id).get(dst_column)
            for dst_id in self.get_dst_ids(src_id, require_ordering=require_ordering, type_filter=type_filter)
        ]

    def get_dst_ids_from_value(
        self,
        src_value: Any,
        src_column: str,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[int]:
        """
        Find destination IDs linked to opposite-table rows matching a column value.

        Sort matching opposite endpoint IDs, then concatenate each endpoint's
        linked IDs with optional type filtering. Ordering is grouped by those sorted
        matching IDs, not a global priority sort. Endpoint index errors propagate.

        Example:
            If matching opposite IDs 2 and 5 link to [7, 8] and [7], return
            [7, 8, 7] in grouped traversal order.


        :param src_value: Source value passed to the opposite table's exact value index.
        :param src_column: Source column used for the value-index lookup.
        :param require_ordering: Request sequence order when unprioritized; configured priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of linked destination IDs, retaining repetitions across rows and links.
        """

        dst_ids: list[int] = []
        for src_id in sorted(self._src_table.get_ids_for_value(src_column, src_value)):
            dst_ids.extend(self.get_dst_ids(src_id, require_ordering=require_ordering, type_filter=type_filter))
        return dst_ids

    def get_dst_rows_from_value(
        self,
        src_value: Any,
        src_column: str,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Row]:
        """
        Materialize destination Rows linked to opposite-table value matches.

        Use get_dst_ids_from_value and omit IDs absent from the destination table.
        Repeated links still produce repeated Rows; no global ordering or deduplication
        is added. Snapshot construction failures propagate.

        Example:
            A matching value can lead to repeated Rows when several opposite
            endpoints share the same destination link.


        :param src_value: Source value passed to the opposite table's exact value index.
        :param src_column: Source column used for the value-index lookup.
        :param require_ordering: Request sequence order when unprioritized; configured priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of existing endpoint Rows in grouped value-match/link order.
        """

        return [
            row
            for dst_id in self.get_dst_ids_from_value(
                src_value,
                src_column,
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
            for row in [self._dst_row(dst_id)]
            if row is not None
        ]

    def get_primary_id_secondary_value_map(self) -> dict[int, Any]:
        """
        Map each linked source ID to its unique destination's designated value.

        Require singular destination links for every source in the eager index.
        Use designated_secondary_col for projection without filtering types. Dangling
        destination snapshots can raise rather than being omitted.

        Example:
            A one-to-one books/cover mapping can yield {1: "/covers/one.jpg"}.


        :return: Dict of source ID to destination value, including None for a missing column.
        :raises RuntimeError: Any source has multiple cached link records.
        :raises KeyError: A selected destination snapshot cannot be read.
        """

        return {
            src_id: self.get_dst_value(src_id, self.designated_secondary_col)
            for src_id in self._by_src
            if self.get_dst_id(src_id) is not None
        }

    def get_primary_id_secondary_value_id_map(self) -> dict[int, int]:
        """
        Map linked source IDs to their singular destination IDs.

        Traverse eager source-index keys and enforce singularity without a type
        filter. Duplicate physical links still violate singularity.

        Example:
            A one-to-one link cache can yield {1: 10, 2: 11}.


        :return: New dict of source ID to destination ID, without endpoint existence checks.
        :raises RuntimeError: Any source has more than one cached link record.
        """

        return {
            src_id: dst_id
            for src_id in self._by_src
            for dst_id in [self.get_dst_id(src_id)]
            if dst_id is not None
        }

    def get_secondary_id_primary_id_map(self) -> dict[int, int]:
        """
        Map linked destination IDs to their singular source IDs.

        Traverse eager destination-index keys and enforce singularity without a
        type filter. Record order does not resolve multiple associations here.

        Example:
            The reverse of source links {1: 10, 2: 11} is {10: 1, 11: 2}.


        :return: New dict of destination ID to source ID, without endpoint existence checks.
        :raises RuntimeError: Any destination has more than one cached link record.
        """

        return {
            dst_id: src_id
            for dst_id in self._by_dst
            for src_id in [self.get_src_id(dst_id)]
            if src_id is not None
        }

    def _delete_matching_records(
        self,
        *,
        src_ids: Optional[set[int]] = None,
        dst_ids: Optional[set[int]] = None,
        row_ids: Optional[set[int]] = None,
    ) -> None:
        """
        Delete physical row IDs selected from cached records by any supplied criterion.

        Criteria are combined as alternatives, not an intersection. If no physical
        ID column was discovered, return without doing anything. Cached records and
        indexes remain unchanged until reload. Endpoint matches can include a record
        whose row_id is None; the cast does not convert or validate it before passing
        that value to the driver. Database errors propagate.

        Example:
            Supplying source IDs {1} and destination IDs {7} deletes links matching
            either endpoint criterion, including links that match only one.


        :param src_ids: Optional source-ID set; matching records are deleted.
        :param dst_ids: Optional destination-ID set; matching records are deleted.
        :param row_ids: Optional physical row-ID set; matching records are deleted.
        :return: None; calls driver delete_by_id once when selected IDs are nonempty.
        """

        if self._id_column is None:
            return
        ids_to_delete: set[int] = set()
        for record in self._records:
            if row_ids is not None and record.row_id in row_ids:
                ids_to_delete.add(cast(int, record.row_id))
                continue
            if src_ids is not None and record.src_id in src_ids:
                ids_to_delete.add(cast(int, record.row_id))
                continue
            if dst_ids is not None and record.dst_id in dst_ids:
                ids_to_delete.add(cast(int, record.row_id))
                continue
        if ids_to_delete:
            self.db.driver_wrapper.delete_by_id(self.table, ids_to_delete)

    def _delete_existing_for_src(self, src_id: int) -> None:
        """
        Delete cached-record-backed physical links for one source ID.

        Only cached records can be selected. A missing link ID column makes the
        delegated deletion a no-op; driver and conversion failures propagate.

        Example:
            External links absent from the current cache are not discovered by this
            delete helper before it calls the driver.


        :param src_id: Source ID converted with int before selecting records.
        :return: None; delegates deletion without updating the cached record indexes.
        """

        self._delete_matching_records(src_ids={int(src_id)})

    def _delete_existing_for_dst(self, dst_id: int) -> None:
        """
        Delete cached-record-backed physical links for one destination ID.

        Only cached records can be selected. A missing link ID column makes the
        delegated deletion a no-op; driver and conversion failures propagate.

        Example:
            External links absent from the current cache are not discovered by this
            delete helper before it calls the driver.


        :param dst_id: Destination ID converted with int before selecting records.
        :return: None; delegates deletion without updating the cached record indexes.
        """

        self._delete_matching_records(dst_ids={int(dst_id)})

    def _find_existing_pair_row(self, src_id: int, dst_id: int) -> Optional[dict[str, Any]]:
        """
        Deep-copy the first cached row payload for a directed pair.

        No type filter or singularity check is used. This consults the cached pair
        index, so earlier driver mutations in the same update sequence are not
        automatically reflected.

        Example:
            With two typed rows for one pair, this selects the first ordered row
            rather than requiring the pair to be unique.


        :param src_id: Source ID for pair lookup.
        :param dst_id: Destination ID for pair lookup.
        :return: Independent dict payload of the first ordered pair record, or None if absent.
        """

        existing = self._records_for_pair(src_id, dst_id)
        if not existing:
            return None
        return deepcopy(existing[0].row_dict)

    def _ensure_link(
        self,
        src_id: int,
        dst_id: int,
        *,
        updates: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """
        Apply cardinality deletions then add or update a pair using cached existence.

        ONE_ONE/MANY_ONE delete existing source links; ONE_ONE/ONE_MANY delete
        existing destination links. Then look up the pair in the unchanged cache:
        add a new endpoint payload if absent, otherwise update the cached row payload.
        This lookup can still describe a row just deleted by the earlier step. Repeated
        ensures do not discover newly inserted pairs until reload, and updates may
        override endpoint or identity fields. No transaction or rollback is added.

        Example:
            Caller-supplied priority updates are merged into the physical row payload
            when this helper adds or updates a pair.


        :param src_id: Source ID converted with int.
        :param dst_id: Destination ID converted with int.
        :param updates: Optional column updates copied into a dict; later keys override base payload keys.
        :return: None; performs driver mutations without refreshing cached records.
        """

        src_id = int(src_id)
        dst_id = int(dst_id)
        updates = dict(updates or {})

        if self.table_type in {TableTypes.ONE_ONE, TableTypes.MANY_ONE}:
            self._delete_existing_for_src(src_id)
        if self.table_type in {TableTypes.ONE_ONE, TableTypes.ONE_MANY}:
            self._delete_existing_for_dst(dst_id)

        current = self._find_existing_pair_row(src_id, dst_id)
        if current is None:
            payload = {
                self._src_link_col: src_id,
                self._dst_link_col: dst_id,
            }
            payload.update(updates)
            self.db.driver_wrapper.add_row(payload)
            return

        current.update(updates)
        self.db.driver_wrapper.update_row(current)

    def _set_pair_column(
        self,
        src_id: int,
        dst_id: int,
        column: Optional[str],
        value: Any,
    ) -> None:
        """
        Set an optional physical link column when current headings include it.

        column_headings suppresses lookup failures, so a metadata failure can make
        this operation silently skip. A valid column can cause link creation and
        cardinality-driven deletions through _ensure_link.

        Example:
            Setting an unsupported origin column does nothing rather than creating
            a new schema column.


        :param src_id: Source ID for the pair to ensure.
        :param dst_id: Destination ID for the pair to ensure.
        :param column: Optional physical column; None or an absent heading skips the operation.
        :param value: Value merged into the ensured pair payload.
        :return: None; either skips the unsupported column or delegates to _ensure_link.
        """

        if column is None or column not in self.column_headings:
            return
        self._ensure_link(src_id, dst_id, updates={column: value})

    def _apply_priority_order(self, src_id: int, dst_ids: Sequence[int]) -> None:
        """
        Ensure destination links and assign descending priorities when configured.

        Without a priority column, only ensure each pair. Otherwise assign
        len(dst_ids)-index, so earlier inputs receive larger priorities. Existing
        links not named by the sequence are not explicitly removed here, though
        _ensure_link may delete links to enforce cardinality.

        Example:
            Destinations [7, 8, 9] receive priorities 3, 2 and 1 when a priority
            column is configured.


        :param src_id: Source ID shared by all requested destination links.
        :param dst_ids: Ordered destination IDs; duplicates are retained and each is converted with int.
        :return: None; performs per-pair driver writes without refreshing link indexes.
        """

        if self.link_spec.priority_link_col is None:
            for dst_id in dst_ids:
                self._ensure_link(src_id, int(dst_id))
            return
        total = len(dst_ids)
        for index, dst_id in enumerate(dst_ids):
            self._ensure_link(
                src_id,
                int(dst_id),
                updates={self.link_spec.priority_link_col: total - index},
            )

    def update(self, update: Any) -> Any:
        """
        Run the legacy update hooks then refresh cached link state.

        Call preflight, precheck, update_db and update_cache in that order. Ignore
        boolean hook return values; failures must raise to stop the sequence. No
        transaction is opened, so a database or refresh error can follow earlier
        successful writes and leave stale cache state.

        Example:
            An update_cache failure propagates after update_db has already issued
            its writes; this wrapper does not roll them back.


        :param update: Update payload passed through preflight before all later hooks.
        :return: Payload returned by update_preflight, unchanged by this wrapper.
        """

        update = self.update_preflight(update)
        self.update_precheck(update)
        self.update_db(update)
        self.update_cache(update)
        return update

    def update_preflight(self, update: Any) -> Any:
        """
        Pass the legacy update payload through without normalization.

        Example:
            >>> payload = object()
            >>> SchemaBackedLinkTable.update_preflight(None, payload) is payload
            True


        :param update: Caller update object retained by identity.
        :return: The same update object.
        """

        return update

    def update_precheck(self, update: Any) -> bool:
        """
        Accept the legacy update payload without validation.

        Example:
            >>> SchemaBackedLinkTable.update_precheck(None, None)
            True


        :param update: Update object ignored by this default hook.
        :return: True for every payload; update ignores this return value.
        """

        del update
        return True

    def update_db(self, update: Any) -> bool:
        """
        Apply legacy link deletion, creation, ordering and metadata update attributes.

        Apply stages in order: explicit link-row deletions; source and destination
        deletions (including their compatibility aliases); create_these_links;
        source/destination priority maps; type maps; combined priority/type maps;
        explicit pair priority/type values; optional origin/policy/data/index values;
        then primary flags. Convert endpoint IDs with int as each stage is reached.

        Source-oriented order lists receive descending priorities. dst_src_priority_update
        only ensures its source group and does not assign priorities; the combined
        destination type/priority map does assign them. Missing type/priority columns
        are omitted in creation maps, while explicit column setters skip unsupported
        columns. Optional metadata uses the first heading ending in the relevant
        suffix. Primary requests set matching cached links to 1 without clearing
        other flags. Unknown update attributes are ignored.

        Every selection/ensure still consults the pre-refresh cache, even after this
        method has deleted or inserted rows. Stage errors propagate after any earlier
        writes; there is no local transaction, rollback, validation pass or cache
        refresh. update_cache is a separate operation.

        Example:
            An object with create_these_links={1: 7} requests a pair ensure; an object
            with no supported attributes performs no mutations but still returns True.


        :param update: Attribute-based update object; missing supported attributes default to empty containers.
        :return: True after every requested operation completes, including an empty update.
        """

        link_row_ids = {
            int(value)
            for value in getattr(update, "delete_these_link_ids", set())
            if value is not None
        }
        if link_row_ids:
            self._delete_matching_records(row_ids=link_row_ids)

        src_ids = {int(value) for value in getattr(update, "src_ids_deleted", set())}
        src_ids |= {int(value) for value in getattr(update, "delete_these_src_ids", set())}
        src_ids |= {int(value) for value in getattr(update, "delete_links_with_this_src_id", set())}
        if src_ids:
            self._delete_matching_records(src_ids=src_ids)

        dst_ids = {int(value) for value in getattr(update, "dst_ids_deleted", set())}
        dst_ids |= {int(value) for value in getattr(update, "delete_these_dst_ids", set())}
        dst_ids |= {int(value) for value in getattr(update, "delete_links_with_this_dst_id", set())}
        if dst_ids:
            self._delete_matching_records(dst_ids=dst_ids)

        for src_id, dst_id in getattr(update, "create_these_links", {}).items():
            self._ensure_link(int(src_id), int(dst_id))

        for src_id, dst_ids_for_src in getattr(update, "src_dst_priority_update", {}).items():
            self._apply_priority_order(int(src_id), [int(dst_id) for dst_id in dst_ids_for_src])

        for dst_srcs, dst_id in getattr(update, "dst_src_priority_update", {}).items():
            for src_id in dst_srcs:
                self._ensure_link(int(src_id), int(dst_id))

        for src_id, typed_values in getattr(update, "src_dst_type_update", {}).items():
            for link_type, dst_ids_for_type in typed_values.items():
                for dst_id in dst_ids_for_type:
                    self._ensure_link(
                        int(src_id),
                        int(dst_id),
                        updates={self.link_spec.type_link_col: link_type} if self.link_spec.type_link_col else None,
                    )

        for type_and_srcs, dst_id in getattr(update, "dst_src_type_update", {}).items():
            link_type, src_ids_for_type = type_and_srcs
            for src_id in src_ids_for_type:
                self._ensure_link(
                    int(src_id),
                    int(dst_id),
                    updates={self.link_spec.type_link_col: link_type} if self.link_spec.type_link_col else None,
                )

        for src_id, typed_values in getattr(update, "src_dst_priority_type_update", {}).items():
            for link_type, ordered_dsts in typed_values.items():
                total = len(ordered_dsts)
                for index, dst_id in enumerate(ordered_dsts):
                    updates = {}
                    if self.link_spec.type_link_col:
                        updates[self.link_spec.type_link_col] = link_type
                    if self.link_spec.priority_link_col:
                        updates[self.link_spec.priority_link_col] = total - index
                    self._ensure_link(int(src_id), int(dst_id), updates=updates)

        for type_and_srcs, dst_id in getattr(update, "dst_src_priority_type_update", {}).items():
            link_type, src_ids_for_type = type_and_srcs
            total = len(src_ids_for_type)
            for index, src_id in enumerate(src_ids_for_type):
                updates = {}
                if self.link_spec.type_link_col:
                    updates[self.link_spec.type_link_col] = link_type
                if self.link_spec.priority_link_col:
                    updates[self.link_spec.priority_link_col] = total - index
                self._ensure_link(int(src_id), int(dst_id), updates=updates)

        for (src_id, dst_id), priority in getattr(update, "set_link_priority", {}).items():
            self._set_pair_column(int(src_id), int(dst_id), self.link_spec.priority_link_col, priority)

        for (src_id, dst_id), link_type in getattr(update, "set_link_type", {}).items():
            self._set_pair_column(int(src_id), int(dst_id), self.link_spec.type_link_col, link_type)

        optional_columns = {
            "set_link_origin": "origin",
            "set_link_policy": "policy",
            "set_link_data": "data",
            "set_link_index": "index",
        }
        for attr_name, suffix in optional_columns.items():
            column_name = None
            for candidate in self.column_headings:
                if candidate.endswith(f"_{suffix}"):
                    column_name = candidate
                    break
            for (src_id, dst_id), value in getattr(update, attr_name, {}).items():
                self._set_pair_column(int(src_id), int(dst_id), column_name, value)

        primary_column = None
        for candidate in self.column_headings:
            if candidate.endswith("_primary"):
                primary_column = candidate
                break
        for dst_id in getattr(update, "set_these_dst_as_primary", set()):
            if primary_column is None:
                break
            for record in self._records_for_dst(int(dst_id)):
                self._set_pair_column(record.src_id, record.dst_id, primary_column, 1)
        for src_id in getattr(update, "set_these_src_as_primary", set()):
            if primary_column is None:
                break
            for record in self._records_for_src(int(src_id)):
                self._set_pair_column(record.src_id, record.dst_id, primary_column, 1)

        return True

    def update_cache(self, update: Any) -> bool:
        """
        Reload the entire link cache after legacy database mutations.

        Use the current attachment to rebuild all records and eager indexes.
        Transaction ownership remains with the caller; a read failure does not
        roll back earlier database writes.

        Example:
            Even an empty update triggers a full read when update_cache is called.


        :param update: Update payload ignored because refresh is unconditional.
        :return: True after read succeeds; read failures propagate.
        """

        del update
        self.read(self.db)
        return True


__all__ = ["SchemaBackedLinkTable"]
