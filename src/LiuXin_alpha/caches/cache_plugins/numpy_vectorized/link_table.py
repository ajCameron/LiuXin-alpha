"""
Cache directed link records and packed relation topology for the NumPy backend.

Read physical link rows into oriented records, build source/reverse/pair
lookups, and expose cardinality-specific link values and read-only Row
snapshots. NumPy is optional: integer topology buffers use tuples when import
fails. Reads use cached link records until reload; metadata properties can
still query the attached database. The legacy update pipeline writes through
the driver and reloads afterward without adding an enclosing transaction.
"""

from __future__ import annotations

import dataclasses
from collections import defaultdict
from copy import deepcopy
from typing import Any, Iterable, Mapping, Optional, Sequence, cast

from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.base_table_api import (
    TableMetadata,
    TableTypes,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.many_many_tables_api import (
    ManyManyLink,
    StorageCacheManyToManyLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.many_one_tables_api import (
    ManyOneLink,
    StorageCacheManyToOneLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.one_many_tables_api import (
    OneManyLink,
    StorageCacheOneToManyLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.one_one_tables_api import (
    OneOneLink,
    StorageCacheOneToOneLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.single_table_api import (
    StorageCacheSingleTableAPI,
)
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.databases.schema_specs import (
    LinkCardinality,
    StorageLinkSpec,
    StorageTableSpec,
)

try:
    import numpy as _np
except Exception:  # pragma: no cover - availability is environment-specific
    _np = None


def _ensure_db(current_db: Any, passed_db: Any = None) -> Any:
    """
    Choose an explicitly supplied database or retain the current attachment.

    Example:
        >>> current = object()
        >>> _ensure_db(current) is current
        True
        >>> _ensure_db(current, False)
        False


    :param current_db: Currently attached database, possibly None.
    :param passed_db: Override used whenever it is not None, including false-valued objects.
    :return: Selected database object unchanged; does not modify an attachment.
    :raises RuntimeError: Both database arguments are None.
    """

    db = passed_db if passed_db is not None else current_db
    if db is None:
        raise RuntimeError("Storage cache requires an attached database")
    return db


def _column_type_map(spec: StorageTableSpec) -> dict[str, str]:
    """
    Describe each schema column using its declared type, affinity or fallback.

    Duplicate column names overwrite earlier entries. Type names are retained
    as supplied rather than normalized for a particular database driver.

    Example:
        A column with no declared type and affinity TEXT is reported as TEXT.


    :param spec: StorageTableSpec whose columns supply names and type metadata.
    :return: New dict from column name to first truthy declared_type/affinity, otherwise UNKNOWN.
    """

    return {
        col.name: col.declared_type or col.affinity or "UNKNOWN"
        for col in spec.columns
    }


def _as_int_array(values: Iterable[int]) -> Any:
    """
    Consume integer-convertible values into an int64 buffer or tuple fallback.

    NumPy import availability determines the representation. Integer conversion
    errors propagate; values outside int64 range can fail only on the array path.

    Example:
        >>> tuple(int(value) for value in _as_int_array(["2", 3]))
        (2, 3)


    :param values: Iterable consumed once with int conversion for each element.
    :return: NumPy int64 array when available, otherwise a tuple of Python integers.
    """

    normalized = tuple(int(value) for value in values)
    if _np is None:
        return normalized
    return _np.asarray(normalized, dtype=_np.int64)


@dataclasses.dataclass(slots=True)
class _CachedLinkRecord:
    """
    Store one oriented link row with lookup and ordering metadata.

    src_id/dst_id are endpoint identities, row_dict is the retained row payload,
    and row_id is the optional physical link identity. link_type and priority
    are optional values normalized by the reader; sequence is the original row
    iteration index and breaks priority ties. This slotted dataclass is mutable
    and does not validate or copy constructor arguments.

    Example:
        >>> record = _CachedLinkRecord(1, 7, {"id": 9}, 9, "tag", 2.0, 0)
        >>> record.src_id, record.dst_id, record.sequence
        (1, 7, 0)
    """

    src_id: int
    dst_id: int
    row_dict: dict[str, Any]
    row_id: Optional[int]
    link_type: Optional[str]
    priority: Optional[float]
    sequence: int


@dataclasses.dataclass(slots=True)
class _CachedRelationTopology:
    """
    Hold grouped link adjacency and destination positions for vectorized fields.

    src_ids and src_ids_array enumerate source rows with links; src_positions
    maps each source ID to its group index. src_offsets delimits that group in
    flat_dst_ids and flat_dst_positions, with one final sentinel offset. A missing
    destination position is -1 and sets has_missing_dst. dst_to_src_ids supports
    reverse traversal and retains duplicates from repeated links. Buffers may be
    NumPy arrays or tuple fallbacks. The container does not freeze its mutable arrays or dictionaries.

    Example:
        For source groups of sizes two and one, offsets are (0, 2, 3); slice
        flat destination buffers between adjacent offsets to recover each group.
    """

    src_ids: tuple[int, ...]
    src_ids_array: Any
    src_positions: dict[int, int]
    src_offsets: Any
    flat_dst_ids: Any
    flat_dst_positions: Any
    has_missing_dst: bool
    dst_to_src_ids: dict[int, tuple[int, ...]]


def _empty_relation_topology() -> _CachedRelationTopology:
    """
    Create independent empty topology containers with a zero sentinel offset.

    Use _as_int_array for buffers, preserving the NumPy-optional representation.

    Example:
        >>> topology = _empty_relation_topology()
        >>> topology.src_ids, tuple(int(v) for v in topology.src_offsets)
        ((), (0,))


    :return: New _CachedRelationTopology with no sources/destinations and has_missing_dst=False.
    """

    return _CachedRelationTopology(
        src_ids=(),
        src_ids_array=_as_int_array(()),
        src_positions={},
        src_offsets=_as_int_array((0,)),
        flat_dst_ids=_as_int_array(()),
        flat_dst_positions=_as_int_array(()),
        has_missing_dst=False,
        dst_to_src_ids={},
    )


class NumpyVectorizedLinkTable(
    StorageCacheOneToOneLinkTable[Any],
    StorageCacheOneToManyLinkTable,
    StorageCacheManyToOneLinkTable,
    StorageCacheManyToManyLinkTable,
):
    """
    Represent one directed view of a physical link table with cached records.

    Explicit endpoint names and link columns establish orientation independently
    of the stored StorageLinkSpec. Construction starts with MANY_MANY type and
    empty records; read infers the effective type, honoring declared cardinality
    first. Priority-bearing links sort by descending numeric priority, using zero
    for missing priorities and input sequence for ties, even when callers do not
    request ordering. Typed filters on an untyped table match nothing.

    Source lookups are built eagerly, destination/pair indexes lazily. Topology
    includes only source IDs present in the source table and records missing
    destination positions. Mutations use cached records to locate existing rows
    until update_cache reloads them; no database transaction is opened here.

    Example:
        An oriented books-to-tags view interprets book_id as src_id and tag_id as
        dst_id; callers request the reverse view separately when needed.
    """

    def __init__(
        self,
        *,
        db: Any,
        link_spec: StorageLinkSpec,
        src_table: StorageCacheSingleTableAPI,
        dst_table: StorageCacheSingleTableAPI,
        src_table_name: str,
        dst_table_name: str,
        src_link_col: str,
        dst_link_col: str,
    ) -> None:
        """
        Attach directed endpoint tables and create empty link indexes/topology.

        Metadata classifies equal endpoint names as intralinks and different names
        as interlinks. Type/priority flags depend on truthy configured column names,
        not a database probe. Initial table_type is MANY_MANY until read rebuilds it.

        Example:
            Constructing a reverse view requires swapping endpoint tables, names and
            link-column arguments; the constructor does not infer that swap.


        :param db: Database retained by the table base; no read is performed here.
        :param link_spec: Physical link specification supplying table and optional type/priority columns.
        :param src_table: Source main-table cache retained by reference.
        :param dst_table: Destination main-table cache retained by reference.
        :param src_table_name: Name of this view's source table.
        :param dst_table_name: Name of this view's destination table.
        :param src_link_col: Physical column storing source IDs for this orientation.
        :param dst_link_col: Physical column storing destination IDs for this orientation.
        :return: None; records orientation and initializes empty caches without validating the schema.
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
        self._by_dst: Optional[dict[int, list[_CachedLinkRecord]]] = None
        self._by_pair: Optional[dict[tuple[int, int], list[_CachedLinkRecord]]] = None
        self._id_column: Optional[str] = None
        self._table_type = TableTypes.MANY_MANY
        self._priority = bool(link_spec.priority_link_col)
        self._typed = bool(link_spec.type_link_col)
        self._relation_topology = _empty_relation_topology()

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

        return (
            getattr(self._dst_table, "default_value_column", None)
            or self.link_spec.secondary_id_col
        )

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
        Copy records and sort priority-bearing links regardless of the ordering hint.

        Missing priority sorts as zero. Equal numeric keys retain original sequence
        order. Negative priorities follow missing/zero priorities, and invalid manual
        priority values can raise during float conversion.

        Example:
            Priorities 3, None and -1 appear in that order; require_ordering=False
            does not disable priority sorting.


        :param records: Sequence of cached record references to order.
        :param require_ordering: Compatibility hint; it does not change ordering behavior.
        :return: New list sharing records, priority-descending when enabled, otherwise input order.
        """

        if not self.priority:
            return list(records)
        ordered = list(records)
        ordered.sort(
            key=lambda record: (
                -float(record.priority) if record.priority is not None else 0.0,
                record.sequence,
            )
        )
        return ordered

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

    def _rebuild_indices(self, rows: Iterable[Any]) -> None:
        """
        Normalize link rows, publish source lookups and rebuild packed topology.

        Discover the physical ID column first. Skip rows with None source or
        destination IDs, convert other IDs with int, optional types with str, and
        priorities with float. Invalid TypeError/ValueError priorities become None.
        Retain dict payloads by reference, copying only non-dict mappings. Record
        sequence preserves the original input position, including skipped rows.

        Sort source groups when priority is enabled, then publish records and indexes
        before cardinality/topology construction. A later failure can leave partial
        new state; earlier conversion failures still leave the newly discovered ID
        column but do not publish the locally accumulated records.

        Example:
            A row missing dst_id contributes no link, while priority="unknown" keeps
            the row with priority=None instead of dropping it.


        :param rows: Iterable of Row-like objects or mappings in desired tie-break sequence.
        :return: None; replaces records/source index, clears lazy indexes and derives type/topology.
        """

        self._id_column = self.db.driver_wrapper.get_id_column(self.table)
        records: list[_CachedLinkRecord] = []
        records_append = records.append
        by_src: dict[int, list[_CachedLinkRecord]] = defaultdict(list)
        for sequence, row in enumerate(rows):
            row_payload = getattr(row, "row_dict", None)
            if row_payload is None:
                row_payload = row if isinstance(row, dict) else dict(row)
            src_id = row_payload.get(self._src_link_col)
            dst_id = row_payload.get(self._dst_link_col)
            if src_id is None or dst_id is None:
                continue
            row_id = row_payload.get(self._id_column) if self._id_column else None
            priority = None
            if self.link_spec.priority_link_col:
                priority_value = row_payload.get(self.link_spec.priority_link_col)
                if priority_value is not None:
                    try:
                        priority = float(priority_value)
                    except (TypeError, ValueError):
                        priority = None
            link_type = None
            if self.link_spec.type_link_col:
                raw_type = row_payload.get(self.link_spec.type_link_col)
                if raw_type is not None:
                    link_type = str(raw_type)
            record = _CachedLinkRecord(
                src_id=int(src_id),
                dst_id=int(dst_id),
                row_dict=row_payload if isinstance(row_payload, dict) else dict(row_payload),
                row_id=int(row_id) if row_id is not None else None,
                link_type=link_type,
                priority=priority,
                sequence=sequence,
            )
            records_append(record)
            by_src[record.src_id].append(record)

        if self._priority:
            for same_src_records in by_src.values():
                same_src_records.sort(
                    key=lambda record: (
                        -float(record.priority) if record.priority is not None else 0.0,
                        record.sequence,
                    )
                )

        self._records = records
        self._by_src = {key: list(value) for key, value in by_src.items()}
        self._by_dst = None
        self._by_pair = None
        self._table_type = self._infer_table_type(records)
        self._rebuild_relation_topology()

    def _ensure_dst_index(self) -> dict[int, list[_CachedLinkRecord]]:
        """
        Lazily group cached records by destination ID in original record order.

        Build only while _by_dst is None. This does not query the database or sort
        by priority; destination selection applies ordering afterward. Returned
        containers are internal and must not be mutated by callers.

        Example:
            The first reverse lookup builds this index; later reverse lookups reuse
            it until a link-record rebuild sets it back to None.


        :return: Internal dict mapping destination IDs to mutable record lists.
        """

        if self._by_dst is None:
            by_dst: dict[int, list[_CachedLinkRecord]] = defaultdict(list)
            for record in self._records:
                by_dst[int(record.dst_id)].append(record)
            self._by_dst = {key: list(value) for key, value in by_dst.items()}
        return self._by_dst

    def _ensure_pair_index(self) -> dict[tuple[int, int], list[_CachedLinkRecord]]:
        """
        Lazily group cached records by directed endpoint pair.

        Preserve original record order and duplicate pair rows. Do not query the
        database; pair selection sorts matches afterward when priority is enabled.

        Example:
            Two typed rows for the same endpoints share a pair bucket rather than
            being collapsed into one record.


        :return: Internal dict from (source ID, destination ID) to mutable record lists.
        """

        if self._by_pair is None:
            by_pair: dict[tuple[int, int], list[_CachedLinkRecord]] = defaultdict(list)
            for record in self._records:
                by_pair[(int(record.src_id), int(record.dst_id))].append(record)
            self._by_pair = {key: list(value) for key, value in by_pair.items()}
        return self._by_pair

    def _rebuild_relation_topology(self) -> None:
        """
        Pack source-visible links into offset-delimited destination buffers.

        Visit source table row_ids in their supplied order and omit sources with no
        links. Append every record in each preordered source group, retaining duplicate
        destinations. Resolve destination positions through _row_id_positions; absent
        positions become -1 and set has_missing_dst. Reverse destination-to-source
        tuples also retain duplicate links. Link records for absent source rows do
        not enter this topology. Arrays use _as_int_array and may fail on int64 overflow.

        Example:
            Two links from source 1 to destination 7 occupy two flat entries and
            produce two source-1 entries in the reverse mapping for destination 7.


        :return: None; replaces the cached topology after constructing its complete value.
        """

        src_ids: list[int] = []
        offsets: list[int] = [0]
        flat_dst_ids: list[int] = []
        flat_dst_positions: list[int] = []
        dst_to_src_ids: dict[int, list[int]] = defaultdict(list)
        has_missing_dst = False

        src_row_ids = getattr(cast(Any, self._src_table), "row_ids", ())
        dst_positions = getattr(cast(Any, self._dst_table), "_row_id_positions", {})
        by_src = self._by_src
        src_ids_append = src_ids.append
        flat_dst_ids_append = flat_dst_ids.append
        flat_dst_positions_append = flat_dst_positions.append
        offsets_append = offsets.append
        dst_positions_get = dst_positions.get

        for src_id in src_row_ids:
            src_id = int(src_id)
            records = by_src.get(src_id, ())
            if not records:
                continue

            src_ids_append(src_id)
            for record in records:
                dst_id = int(record.dst_id)
                position = dst_positions_get(dst_id)
                flat_dst_ids_append(dst_id)
                flat_dst_positions_append(-1 if position is None else int(position))
                has_missing_dst = has_missing_dst or position is None
                dst_to_src_ids[dst_id].append(src_id)
            offsets_append(len(flat_dst_ids))

        self._relation_topology = _CachedRelationTopology(
            src_ids=tuple(src_ids),
            src_ids_array=_as_int_array(src_ids),
            src_positions={int(src_id): index for index, src_id in enumerate(src_ids)},
            src_offsets=_as_int_array(offsets),
            flat_dst_ids=_as_int_array(flat_dst_ids),
            flat_dst_positions=_as_int_array(flat_dst_positions),
            has_missing_dst=has_missing_dst,
            dst_to_src_ids={
                int(dst_id): tuple(src_ids)
                for dst_id, src_ids in dst_to_src_ids.items()
            },
        )

    def refresh_relation_topology(self) -> None:
        """
        Repack existing links against current endpoint row IDs and destination positions.

        Use this after endpoint arrays change position. Source/reverse/pair record
        indexes and inferred table type are not refreshed by this operation.

        Example:
            After deleting a destination row from its table cache, refresh topology
            to mark that cached link's destination position as missing.


        :return: None; replaces topology without reading or rebuilding link records.
        """

        self._rebuild_relation_topology()

    def get_relation_topology(self) -> _CachedRelationTopology:
        """
        Expose the current packed topology object without refreshing or copying it.

        Callers should treat the container, arrays and dictionaries as read-only
        and request refresh_relation_topology after endpoint-position changes.

        Example:
            Read adjacent src_offsets to select a source group from flat_dst_ids.


        :return: Internal mutable _CachedRelationTopology shared with vectorized field consumers.
        """

        return self._relation_topology

    def read(self, db: Any) -> None:
        """
        Attach the selected database and rebuild all link records from its physical table.

        Call get_all_rows with iterator_return=False. The database reference is
        updated before fetching; fetch/rebuild failures propagate without restoring
        the prior attachment or rolling back partial cache changes.

        Example:
            ``link_table.read(None)`` uses its current database and refreshes every
            cached link row.


        :param db: Database override; pass None to reuse the current attachment.
        :return: None; replaces link indexes and topology after reading all table rows.
        :raises RuntimeError: Both explicit and current database references are None.
        """

        db = _ensure_db(self.db, db)
        self.db = db
        rows = self.db.get_all_rows(self.table, iterator_return=False)
        self._rebuild_indices(rows)

    def reload(self, db: Any) -> None:
        """
        Perform the same complete link-table read as read.

        Example:
            ``link_table.reload(link_table.db)`` observes external link-table writes.


        :param db: Database override; None reuses the current database.
        :return: None; rebuilds records, indexes and topology through read.
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
        Select cached records for one source ID with optional type filtering.

        Use the eager source index whose groups were sorted at rebuild time.
        The ordering hint is accepted but not inspected.

        Example:
            Duplicate link rows remain separate results; filtering does not change
            the stored record lists.


        :param src_id: Source ID converted with int.
        :param require_ordering: Compatibility ordering hint; enabled priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: New list sharing cached record objects, empty when no accepted links exist.
        """

        matches = self._by_src.get(int(src_id), [])
        if type_filter is None:
            return list(matches)
        return [
            record
            for record in matches
            if self._record_matches(record, type_filter)
        ]

    def _records_for_dst(
        self,
        dst_id: int,
        *,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> list[_CachedLinkRecord]:
        """
        Select cached records for one destination ID with optional type filtering.

        Build the destination index lazily and sort accepted records by priority
        when enabled. The ordering hint does not disable that sorting.

        Example:
            Duplicate link rows remain separate results; filtering does not change
            the stored record lists.


        :param dst_id: Destination ID converted with int.
        :param require_ordering: Compatibility ordering hint; enabled priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: New list sharing cached record objects, empty when no accepted links exist.
        """

        by_dst = self._ensure_dst_index()
        matches = [
            record
            for record in by_dst.get(int(dst_id), [])
            if self._record_matches(record, type_filter)
        ]
        return self._ordered_records(matches, require_ordering=require_ordering)

    def _records_for_pair(
        self,
        src_id: int,
        dst_id: int,
        *,
        type_filter: Optional[str] = None,
    ) -> list[_CachedLinkRecord]:
        """
        Select every accepted cached row for a directed pair and apply priority order.

        Build the pair index lazily. Endpoint existence in main-table caches is
        not checked; matching is against cached link rows.

        Example:
            A pair can yield several records when distinct typed or duplicate physical
            links exist for those endpoints.


        :param src_id: Source ID converted with int.
        :param dst_id: Destination ID converted with int.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: New list sharing matching cached records, including duplicate pair rows.
        """

        by_pair = self._ensure_pair_index()
        matches = [
            record
            for record in by_pair.get((int(src_id), int(dst_id)), [])
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
            >>> NumpyVectorizedLinkTable._optional_single(None, []) is None
            True
            >>> NumpyVectorizedLinkTable._optional_single(None, ["first", "next"], insist_on_singular=False)
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

        if not cast(Any, self._src_table).has_id(src_id):
            return None
        return cast(Any, self._src_table).get_row(src_id)

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

        if not cast(Any, self._dst_table).has_id(dst_id):
            return None
        return cast(Any, self._dst_table).get_row(dst_id)

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
        :param require_ordering: Compatibility ordering hint; enabled priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of cardinality-specific link values, empty when no records match.
        """

        return [
            self._make_link_object(record)
            for record in self._records_for_src(
                src_id,
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
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
        :param require_ordering: Compatibility ordering hint; enabled priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of cardinality-specific link values, empty when no records match.
        """

        return [
            self._make_link_object(record)
            for record in self._records_for_dst(
                dst_id,
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
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
        :param require_ordering: Compatibility ordering hint; enabled priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of read-only Row snapshots with deep-copied payloads, empty when no records match.
        """

        return [
            self._row_from_snapshot(record.row_dict)
            for record in self._records_for_src(
                src_id,
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
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
        :param require_ordering: Compatibility ordering hint; enabled priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of read-only Row snapshots with deep-copied payloads, empty when no records match.
        """

        return [
            self._row_from_snapshot(record.row_dict)
            for record in self._records_for_dst(
                dst_id,
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
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
        :param require_ordering: Compatibility ordering hint; enabled priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of integer IDs, retaining duplicate links and dangling endpoints.
        """

        return [
            record.src_id
            for record in self._records_for_dst(
                dst_id,
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
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
        :param require_ordering: Compatibility ordering hint; enabled priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of endpoint Rows in link order, preserving duplicates for existing rows.
        """

        rows = []
        for src_id in self.get_src_ids(
            dst_id,
            require_ordering=require_ordering,
            type_filter=type_filter,
        ):
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
        return cast(Any, self._src_table).get_row_snapshot(src_id).get(src_column)

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
        :param require_ordering: Compatibility ordering hint; enabled priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of column values, retaining duplicates and None for missing columns.
        :raises KeyError: A linked endpoint snapshot cannot be read.
        """

        return [
            cast(Any, self._src_table).get_row_snapshot(src_id).get(src_column)
            for src_id in self.get_src_ids(
                dst_id,
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
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
        :param require_ordering: Compatibility ordering hint; enabled priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of linked source IDs, retaining repetitions across rows and links.
        """

        src_ids: list[int] = []
        for dst_id in sorted(cast(Any, self._dst_table).get_ids_for_value(dst_column, dst_value)):
            src_ids.extend(
                self.get_src_ids(
                    dst_id,
                    require_ordering=require_ordering,
                    type_filter=type_filter,
                )
            )
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
        :param require_ordering: Compatibility ordering hint; enabled priorities are always honored.
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
        :param require_ordering: Compatibility ordering hint; enabled priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of integer IDs, retaining duplicate links and dangling endpoints.
        """

        return [
            record.dst_id
            for record in self._records_for_src(
                src_id,
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
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
        :param require_ordering: Compatibility ordering hint; enabled priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of endpoint Rows in link order, preserving duplicates for existing rows.
        """

        rows = []
        for dst_id in self.get_dst_ids(
            src_id,
            require_ordering=require_ordering,
            type_filter=type_filter,
        ):
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
        return cast(Any, self._dst_table).get_row_snapshot(dst_id).get(dst_column)

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
        :param require_ordering: Compatibility ordering hint; enabled priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of column values, retaining duplicates and None for missing columns.
        :raises KeyError: A linked endpoint snapshot cannot be read.
        """

        return [
            cast(Any, self._dst_table).get_row_snapshot(dst_id).get(dst_column)
            for dst_id in self.get_dst_ids(
                src_id,
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
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
        :param require_ordering: Compatibility ordering hint; enabled priorities are always honored.
        :param type_filter: Optional exact type filter; a non-None filter on an untyped table matches nothing.
        :return: List of linked destination IDs, retaining repetitions across rows and links.
        """

        dst_ids: list[int] = []
        for src_id in sorted(cast(Any, self._src_table).get_ids_for_value(src_column, src_value)):
            dst_ids.extend(
                self.get_dst_ids(
                    src_id,
                    require_ordering=require_ordering,
                    type_filter=type_filter,
                )
            )
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
        :param require_ordering: Compatibility ordering hint; enabled priorities are always honored.
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

        Build the destination index lazily and enforce singularity without a type
        filter. Record order does not resolve multiple associations in this method.

        Example:
            The reverse of source links {1: 10, 2: 11} is {10: 1, 11: 2}.


        :return: New dict of destination ID to source ID, without endpoint existence checks.
        :raises RuntimeError: Any destination has more than one cached link record.
        """

        by_dst = self._ensure_dst_index()
        return {
            dst_id: src_id
            for dst_id in by_dst
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
            >>> NumpyVectorizedLinkTable.update_preflight(None, payload) is payload
            True


        :param update: Caller update object retained by identity.
        :return: The same update object.
        """

        return update

    def update_precheck(self, update: Any) -> bool:
        """
        Accept the legacy update payload without validation.

        Example:
            >>> NumpyVectorizedLinkTable.update_precheck(None, None)
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
                        updates={self.link_spec.type_link_col: link_type}
                        if self.link_spec.type_link_col
                        else None,
                    )

        for type_and_srcs, dst_id in getattr(update, "dst_src_type_update", {}).items():
            link_type, src_ids_for_type = type_and_srcs
            for src_id in src_ids_for_type:
                self._ensure_link(
                    int(src_id),
                    int(dst_id),
                    updates={self.link_spec.type_link_col: link_type}
                    if self.link_spec.type_link_col
                    else None,
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

        Use the currently attached database, rebuild all records/indexes/topology,
        and leave transaction ownership to the caller. This does not roll back the
        database update if refreshing fails.

        Example:
            Even an empty update triggers a full read when update_cache is called.


        :param update: Update payload ignored because refresh is unconditional.
        :return: True after read succeeds; read failures propagate.
        """

        del update
        self.read(self.db)
        return True


__all__ = ["NumpyVectorizedLinkTable"]
