"""
Cache schema-declared main-table rows with exact value indexes and bounded repair.

Loaded rows are deep-copied and ordered by integer ID. Secondary indexes
use Python hash/equality. Legacy writes call the driver and then refresh
selected IDs or evict deleted entries; they do not refresh relation fields
or provide an encompassing transaction.
"""

from __future__ import annotations

import bisect

from collections import defaultdict
from copy import deepcopy

from typing import Any, Iterable, Mapping, Optional, Sequence

from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.base_table import (
    TableMetadata,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.single_table import (
    StorageCacheSingleTableAPI,
)
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.databases.schema_specs import StorageTableSpec

from LiuXin_alpha.caches.cache_plugins.schema_backed.common import (
    _column_type_map,
    _default_value_column,
    _ensure_db,
)


class SchemaBackedMainTableCache(StorageCacheSingleTableAPI):
    """
    Own one main table's row snapshots, ID order and column value indexes.

    Construction retains a schema/database without loading rows. Full reads
    keep duplicate IDs in row order while the ID dictionary stores the last
    row for each ID. Normal operation expects unique database identities.

    Bounded refresh patches individual rows and index buckets. Snapshot
    getters return deep copies, while column lists share stored value objects.
    Callers own database transactions and coordination with related caches.

    Example:
        After loading IDs 3 and 1, row_ids is (1, 3) and get_values_for follows that order.
    """

    spec: StorageTableSpec

    def __init__(self, spec: StorageTableSpec, db: Any) -> None:
        """
        Attach schema/table metadata and allocate empty row and index state.

        No rows are read or schema validity checked. Column headings and types
        are later derived from the retained specification rather than copied here.

        Example:
            A newly constructed table has row_ids == () before read.


        :param spec: Table specification retained by reference.
        :param db: Database reference retained by the base table API.
        :return: None; sets main-table metadata and unloaded empty dictionaries/list.
        """

        metadata = TableMetadata(
            table_name=spec.name,
            main_table=True,
            is_interlink=False,
            is_intralink=False,
        )
        super().__init__(table=spec.name, db=db, metadata=metadata)
        self.spec = spec
        self._rows_by_id: dict[int, dict[str, Any]] = {}
        self._row_order: list[int] = []
        self._value_indexes: dict[str, dict[Any, set[int]]] = {}
        self._loaded = False

    @property
    def id_column(self) -> str:
        """
        Expose the ID column declared by this table specification.

        Example:
            A schema declaring id_column="book_id" reports book_id.


        :return: Declared ID column name without a database lookup.
        :raises RuntimeError: The specification has no ID column.
        """

        if self.spec.id_column is None:
            raise RuntimeError(f"Table {self.table!r} does not expose an id column")
        return self.spec.id_column

    @property
    def default_value_column(self) -> Optional[str]:
        """
        Choose the first non-bookkeeping column with an ID fallback.

        Example:
            A table containing only its identity column uses that column as its
            default value column.


        :return: Value column selected by _default_value_column, or None when no fallback exists.
        """

        return _default_value_column(self.spec)

    @property
    def row_ids(self) -> tuple[int, ...]:
        """
        Copy the current ordered row identities into a tuple.

        No refresh or readiness check is performed. Bounded insertion assumes
        this order remains sorted; ordinary database tables supply unique IDs.

        Example:
            An unloaded table returns (), while a full load of IDs 3 and 1 returns (1, 3).


        :return: Tuple of integer IDs, including duplicates retained by a full load.
        """

        return tuple(self._row_order)

    @property
    def column_headings(self) -> list[str]:
        """
        List column names in the retained schema's declaration order.

        Example:
            An empty table can still report headings such as ["id", "title"].


        :return: New list of column names, independent of whether rows were loaded.
        """

        return [col.name for col in self.spec.columns]

    @property
    def column_types(self) -> dict[str, str]:
        """
        Map declared column names to schema type descriptions.

        Example:
            A column with neither declared type nor affinity reports UNKNOWN.


        :return: New dict using declared type, affinity fallback or UNKNOWN.
        """

        return _column_type_map(self.spec)

    def _row_from_snapshot(self, row_dict: Mapping[str, Any]) -> Row:
        """
        Wrap a deep-copied main-table payload in a read-only database Row.

        Copy the mapping and recursively copy its contents before Row construction.
        Row validation and copying failures propagate; this does not insert a row.

        Example:
            Changing a nested container in the original cached payload does not change
            the independently copied payload supplied to the returned Row.


        :param row_dict: Mapping of main-table column values.
        :return: Read-only Row attached to the current database, with an independent payload.
        """

        return Row(database=self.db, row_dict=deepcopy(dict(row_dict)), read_only=True)

    def _refresh_indexes(self) -> None:
        """
        Rebuild exact value-to-ID sets for every schema column.

        Visit _row_order and read each row from _rows_by_id. Missing column keys
        are indexed as None. Duplicate IDs collapse into sets and use the last row
        payload; values must support dictionary hashing.

        Example:
            Two owners with title "Dune" share one value bucket containing both IDs.


        :return: None; replaces all value indexes after building them from cached rows.
        :raises TypeError: A cached column value is unhashable.
        """

        value_indexes: dict[str, dict[Any, set[int]]] = {
            column: defaultdict(set)
            for column in self.column_headings
        }
        for row_id in self._row_order:
            row_dict = self._rows_by_id[row_id]
            for column in self.column_headings:
                value_indexes[column][row_dict.get(column)].add(row_id)
        self._value_indexes = {
            column: {value: set(ids) for value, ids in values.items()}
            for column, values in value_indexes.items()
        }

    def _replace_rows(self, row_dicts: Sequence[Mapping[str, Any]]) -> None:
        """
        Deep-copy identified rows, sort their IDs and rebuild all value indexes.

        Skip rows with a None/missing ID and convert others with int. Sort by ID
        then input position, retaining duplicate IDs in _row_order while the last
        payload wins _rows_by_id. Publish row state before rebuilding indexes: an
        index error can leave new rows with old indexes and the previous load flag.

        Example:
            Duplicate ID 7 rows retain two positions, both resolving to the last
            ID-7 payload through the row dictionary.


        :param row_dicts: Row mappings copied before identity extraction.
        :return: None; replaces row state, builds indexes and sets _loaded true on success.
        """

        rows_by_id: dict[int, dict[str, Any]] = {}
        ordered_ids: list[int] = []

        sortable = []
        for index, row_dict in enumerate(row_dicts):
            row_copy = deepcopy(dict(row_dict))
            row_id = row_copy.get(self.id_column)
            if row_id is None:
                continue
            sortable.append((int(row_id), index, row_copy))

        sortable.sort(key=lambda item: (item[0], item[1]))

        for row_id, _index, row_copy in sortable:
            rows_by_id[int(row_id)] = row_copy
            ordered_ids.append(int(row_id))

        self._rows_by_id = rows_by_id
        self._row_order = ordered_ids
        self._refresh_indexes()
        self._loaded = True

    def read(self, db: Any) -> None:
        """
        Attach the selected database and replace snapshots from all physical rows.

        Retain the selected database before fetching rows. Prefer
        driver_wrapper.get_all_rows; an AttributeError from that call selects
        db.get_all_rows(iterator_return=False) and extracts each Row.row_dict.
        Other errors propagate. Copying/indexing happens in _replace_rows, outside
        the fallback handler, and can leave partial replacement state.

        Example:
            ``table.read(None)`` refreshes the current attachment; supplying another
            database changes which connection supplies the rows.


        :param db: Database override; None reuses the current attachment.
        :return: None; loads ordered row dictionaries and exact column-value indexes.
        :raises RuntimeError: No explicit or attached database is available.
        """

        db = _ensure_db(self.db, db)
        self.db = db
        try:
            row_dicts = db.driver_wrapper.get_all_rows(self.table)
        except AttributeError:
            rows = db.get_all_rows(self.table, iterator_return=False)
            row_dicts = [row.row_dict for row in rows]
        self._replace_rows(row_dicts)

    def reload(self, db: Any) -> None:
        """
        Delegate a complete table replacement to read.

        Example:
            Reload after an external change to rebuild every row snapshot and value bucket.


        :param db: Database override; None selects the current table attachment.
        :return: None; reloads all rows and indexes using read's fallback/error behavior.
        """

        self.read(db=db)

    def linked_to(self) -> Iterable[str]:
        """
        Expose endpoint table names declared by the stored schema.

        Example:
            A schema naming covers and tags returns those names in its declared order.


        :return: Tuple of linked table names, without discovering current database links.
        """

        return tuple(self.spec.linked_tables)

    def get_values_for(self, column: str) -> Sequence[Any]:
        """
        List one indexed column's values in cached row order.

        Column presence is checked against _value_indexes before row traversal.
        Missing keys in a stored row contribute None.

        Example:
            A column declared in the schema can still raise before the first read builds its index.


        :param column: Column name requiring a built value index.
        :return: New list of row values, including None and repeated IDs; elements are shared.
        :raises KeyError: The column has no built value index.
        """

        if column not in self._value_indexes:
            raise KeyError(column)
        return [self._rows_by_id[row_id].get(column) for row_id in self._row_order]

    def get_unique_values(self, column: str) -> set[Any]:
        """
        Copy the value-index keys for one column.

        Example:
            Distinct Unicode normalization forms remain distinct keys when Python equality differs.


        :param column: Column name requiring a built value index.
        :return: New set of distinct values using Python hash/equality.
        :raises KeyError: The column has no built value index.
        """

        if column not in self._value_indexes:
            raise KeyError(column)
        return set(self._value_indexes[column].keys())

    def get_ids_for_value(self, column: str, value: str) -> set[int]:
        """
        Look up row IDs using exact Python value-index equality.

        Deduplicate repeated IDs from the value bucket. Do not normalize text or
        coerce lookup values to str; dictionary hashing/equality defines matching.

        Example:
            A column with two rows sharing the same title returns both row IDs.


        :param column: Column whose value index should be searched.
        :param value: Hashable lookup value passed unchanged, despite the string annotation.
        :return: New set of matching integer IDs, empty for a known column with no match.
        :raises KeyError: The column has no value index.
        :raises TypeError: The lookup value is unhashable.
        """

        if column not in self._value_indexes:
            raise KeyError(column)
        return set(self._value_indexes[column].get(value, set()))

    def get_col_value_from_id(self, table_id: int) -> Any:
        """
        Read a default-column value from a deep-copied row snapshot.

        Resolve and copy the row before choosing the default column. Missing
        column keys return None; the schema helper can fall back to the ID column.

        Example:
            A table whose default column is title returns the copied title value.


        :param table_id: Requested row identity passed to get_row_snapshot.
        :return: Default-column value, or the complete snapshot dict when no default exists.
        :raises KeyError: The requested row ID is absent from the cache.
        """

        row_dict = self.get_row_snapshot(table_id)
        default_column = self.default_value_column
        if default_column is None:
            return row_dict
        return row_dict.get(default_column)

    def has_id(self, table_id: int) -> bool:
        """
        Test an integer-converted ID against the cached row dictionary.

        Example:
            A row added externally remains absent until an explicit refresh.


        :param table_id: Requested row ID converted with int.
        :return: True if the ID is present, without database fallback or refresh.
        """

        return int(table_id) in self._rows_by_id

    def get_row_snapshot(self, table_id: int) -> dict[str, Any]:
        """
        Deep-copy the complete cached payload for one row identity.

        A duplicate loaded ID resolves to its last stored payload. No database
        read or field dependency refresh occurs.

        Example:
            Changing a nested value in the returned snapshot leaves cached row data intact.


        :param table_id: Requested row identity converted with int.
        :return: Independent dictionary including stored keys outside schema headings.
        :raises KeyError: The integer row ID is absent from the cached dictionary.
        """

        row_id = int(table_id)
        if row_id not in self._rows_by_id:
            raise KeyError(row_id)
        return deepcopy(self._rows_by_id[row_id])

    def get_row(self, table_id: int) -> Row:
        """
        Wrap the selected main-table snapshot as a deep-copied read-only Row.

        Example:
            Use this when a caller expects the database Row interface instead of
            a plain snapshot dictionary.


        :param table_id: Row identity resolved by get_row_snapshot.
        :return: Read-only Row attached to the database with an independent copied payload.
        :raises KeyError: The row ID is not present in the cache.
        """

        return self._row_from_snapshot(self.get_row_snapshot(table_id))

    def _normalize_row_payload(
        self,
        table_id_val_map: Mapping[int, Any],
        target_column: Optional[str],
    ) -> dict[int, dict[str, Any]]:
        """
        Convert ID-keyed scalar or mapping updates into physical row dictionaries.

        Choose a non-None target column before processing any entries, even when
        every value is a mapping or input is empty. Deep-copy mapping payloads and
        overwrite their ID column with the converted key. Scalar payloads retain the
        value reference under the chosen column. Keys that convert to the same ID
        overwrite earlier payloads. If chosen column equals the ID column, the scalar
        value overwrites the identity entry in that payload.

        Example:
            An update keyed by 7 with payload {"id": 99, "title": "Dune"} stores
            id=7 in the normalized mapping payload.


        :param table_id_val_map: Mapping of integer-convertible IDs to scalar values or column mappings.
        :param target_column: Truthy explicit scalar target column, otherwise the default value column.
        :return: Dict keyed by converted integer ID with one payload dict per surviving key.
        :raises RuntimeError: No target/default value column exists, or a needed ID column is absent.
        """

        payloads: dict[int, dict[str, Any]] = {}
        chosen_column = target_column or self.default_value_column
        if chosen_column is None:
            raise RuntimeError(f"Table {self.table!r} has no sensible default value column")

        for table_id, value in table_id_val_map.items():
            row_id = int(table_id)
            if isinstance(value, Mapping):
                payload = deepcopy(dict(value))
                payload[self.id_column] = row_id
            else:
                payload = {self.id_column: row_id, chosen_column: value}
            payloads[row_id] = payload
        return payloads

    def _refresh_ids(self, table_ids: Iterable[int]) -> None:
        """
        Read and repair only the selected database rows and their index buckets.

        Require the table's current database before ID conversion, including for
        empty input. For each ID read get_row_from_id, remove old value-index entries,
        and deep-copy/reindex a surviving row or evict an absent one. Insert new IDs
        with bisect into sorted row order. No full scan or field refresh is performed.

        The load flag is unchanged. Iteration order follows a set, and failures can
        leave some rows repaired or indexes partially changed. Deletion removes one
        row-order occurrence, assuming identities were unique in the source table.

        Example:
            Refreshing {7} after a title edit reads row 7 and updates its old/new
            title buckets without reloading unrelated rows.


        :param table_ids: Row IDs consumed into a deduplicated integer set.
        :return: None; inserts/replaces present rows and evicts cached rows missing from the database.
        """

        db = _ensure_db(self.db)
        for table_id in {int(value) for value in table_ids}:
            row_id = int(table_id)
            old_row = self._rows_by_id.get(row_id)
            row = db.get_row_from_id(self.table, row_id)
            if row is None:
                if old_row is None:
                    continue
                self._remove_row_from_indexes(row_id, old_row)
                self._rows_by_id.pop(row_id, None)
                self._row_order.remove(row_id)
                continue
            new_row = deepcopy(row.row_dict)
            if old_row is not None:
                self._remove_row_from_indexes(row_id, old_row)
            else:
                bisect.insort(self._row_order, row_id)
            self._rows_by_id[row_id] = new_row
            self._add_row_to_indexes(row_id, new_row)

    def _remove_row_from_indexes(
        self,
        row_id: int,
        row: Mapping[str, Any],
    ) -> None:
        """
        Discard one row ID from buckets for its previous column values.

        Visit schema columns only. Row storage and ordering are not changed here.

        Example:
            Removing the last owner of title "Dune" also removes that title bucket.


        :param row_id: Row identity converted with int when removing membership.
        :param row: Previous payload whose missing column keys are treated as None.
        :return: None; removes emptied value buckets and ignores absent buckets.
        """

        for column in self.column_headings:
            value_ids = self._value_indexes.get(column, {}).get(row.get(column))
            if value_ids is None:
                continue
            value_ids.discard(int(row_id))
            if not value_ids:
                self._value_indexes[column].pop(row.get(column), None)

    def _add_row_to_indexes(
        self,
        row_id: int,
        row: Mapping[str, Any],
    ) -> None:
        """
        Add one row ID to each schema column's current value bucket.

        No row storage or ordering is changed here; values must be hashable.
        A later unhashable column can fail after earlier buckets were updated.

        Example:
            A row with no title key is indexed under None for the title column.


        :param row_id: Row identity converted with int before adding membership.
        :param row: New payload whose missing column keys are indexed as None.
        :return: None; creates missing column/value dictionaries and set entries.
        """

        for column in self.column_headings:
            self._value_indexes.setdefault(column, {}).setdefault(
                row.get(column), set()
            ).add(int(row_id))

    def create(
        self,
        table_id_val_map: Mapping[int, Any],
        db: Any,
        target_column: Optional[str] = None,
        allow_case_change: bool = False,
    ) -> None:
        """
        Write normalized row creations then refresh selected IDs.

        The write helper selects its database locally without changing self.db.
        The cache hook uses self.db, so an explicit write override can differ from
        the refresh database. No transaction or relation-field refresh is added;
        earlier writes can survive later write or cache-repair failures.

        Example:
            A one-row insert refreshes that owner ID instead of rereading every table row.


        :param table_id_val_map: Mapping of IDs to scalar values or column mappings.
        :param db: Database override used for writes; None selects the table attachment.
        :param target_column: Truthy scalar target column, otherwise the default value column.
        :param allow_case_change: Compatibility flag ignored by creation.
        :return: None; after writes succeed, repairs input IDs from the table's attached database.
        """

        del allow_case_change
        self._create_to_db(table_id_val_map, db, target_column=target_column)
        self._create_to_cache(table_id_val_map, target_column=target_column)

    def _create_to_cache(
        self,
        table_id_val_map: Mapping[int, Any],
        target_column: Optional[str] = None,
        allow_case_change: bool = False,
    ) -> None:
        """
        Refresh the input mapping's IDs from the table's attached database.

        No scalar payload is copied directly into the cache. Even an empty
        mapping requires a database through _refresh_ids.

        Example:
            Driver-assigned data is observed by rereading the supplied IDs after a write.


        :param table_id_val_map: Mapping whose keys select rows; values are ignored.
        :param target_column: Unused compatibility target-column hint.
        :param allow_case_change: Unused compatibility case-change hint.
        :return: None; delegates mapping keys to bounded _refresh_ids.
        """

        del target_column, allow_case_change
        self._refresh_ids(table_id_val_map.keys())

    def _create_to_db(
        self,
        table_id_val_map: Mapping[int, Any],
        db: Any,
        target_column: Optional[str] = None,
        allow_case_change: bool = False,
    ) -> None:
        """
        Insert each normalized row payload without refreshing or reattaching the cache.

        Ignore allow_case_change. Resolve an explicit/current database locally,
        normalize every payload, then insert in mapping order. Do not save an explicit
        database override on self until a later read. Driver-returned identities are
        ignored and partial inserts are not rolled back here.

        Example:
            An empty mapping can still fail target-column normalization before any
            insert is attempted.


        :param table_id_val_map: Mapping of row IDs to scalar values or column mappings.
        :param db: Database override; None uses the current attachment.
        :param target_column: Truthy scalar target column, otherwise the default value column.
        :param allow_case_change: Whether case-only scalar text changes are allowed; create ignores this flag.
        :return: None; calls driver add_row for each normalized payload.
        """

        del allow_case_change
        db = _ensure_db(self.db, db)
        payloads = self._normalize_row_payload(table_id_val_map, target_column)
        for payload in payloads.values():
            db.driver_wrapper.add_row(payload)

    def update(
        self,
        table_id_val_map: Mapping[int, Any],
        db: Any,
        target_column: Optional[str] = None,
        allow_case_change: bool = False,
    ) -> None:
        """
        Write normalized row updates then refresh selected IDs.

        The write helper selects its database locally without changing self.db.
        The cache hook uses self.db, so an explicit write override can differ from
        the refresh database. No transaction or relation-field refresh is added;
        earlier writes can survive later write or cache-repair failures.

        Example:
            A one-row update refreshes that owner ID instead of rereading every table row.


        :param table_id_val_map: Mapping of IDs to scalar values or column mappings.
        :param db: Database override used for writes; None selects the table attachment.
        :param target_column: Truthy scalar target column, otherwise the default value column.
        :param allow_case_change: True permits scalar case-only string changes relative to cached values.
        :return: None; after writes succeed, repairs input IDs from the table's attached database.
        """

        self._update_db(
            table_id_val_map,
            db,
            target_column=target_column,
            allow_case_change=allow_case_change,
        )
        self._update_cache(
            table_id_val_map,
            target_column=target_column,
            allow_case_change=allow_case_change,
        )

    def _update_cache(
        self,
        table_id_val_map: Mapping[int, Any],
        target_column: Optional[str] = None,
        allow_case_change: bool = False,
    ) -> None:
        """
        Refresh the input mapping's IDs from the table's attached database.

        No scalar payload is copied directly into the cache. Even an empty
        mapping requires a database through _refresh_ids.

        Example:
            Driver-assigned data is observed by rereading the supplied IDs after a write.


        :param table_id_val_map: Mapping whose keys select rows; values are ignored.
        :param target_column: Unused compatibility target-column hint.
        :param allow_case_change: Unused compatibility case-change hint.
        :return: None; delegates mapping keys to bounded _refresh_ids.
        """

        del target_column, allow_case_change
        self._refresh_ids(table_id_val_map.keys())

    def _update_db(
        self,
        table_id_val_map: Mapping[int, Any],
        db: Any,
        target_column: Optional[str] = None,
        allow_case_change: bool = False,
    ) -> None:
        """
        Apply mapping merges or scalar column updates through the selected driver.

        Normalize all payloads, then index the original input mapping using each
        converted integer ID to decide the branch. String keys accepted during
        normalization can therefore raise KeyError at this later lookup.

        Mapping updates fetch the current database Row, reject absence, deep-copy it,
        merge the normalized payload and call update_row. Scalar updates compare
        against the cached column value when the ID is cached; with allow_case_change
        false, equal lowercase string forms skip the driver write. Other scalars use
        update_column without a prior database existence check. No transaction or
        rollback is added, and earlier writes can survive a later error.

        Example:
            With cached title "Dune", scalar "DUNE" is skipped by default; a mapping
            payload containing title="DUNE" still follows the full-row update branch.


        :param table_id_val_map: Mapping of row IDs to scalar values or column mappings.
        :param db: Database override; None uses the current attachment.
        :param target_column: Truthy scalar target column, otherwise the default value column.
        :param allow_case_change: True permits scalar case-only changes; mapping updates ignore this flag.
        :return: None; writes accepted updates without refreshing cached rows or changing self.db.
        :raises KeyError: A normalized integer key is absent from the original mapping or a mapping-update row is missing.
        """

        db = _ensure_db(self.db, db)
        payloads = self._normalize_row_payload(table_id_val_map, target_column)
        chosen_column = target_column or self.default_value_column

        for row_id, payload in payloads.items():
            if isinstance(table_id_val_map[row_id], Mapping):
                current = db.get_row_from_id(self.table, row_id)
                if current is None:
                    raise KeyError(f"No such row in {self.table!r}: {row_id}")
                merged = deepcopy(current.row_dict)
                merged.update(payload)
                db.driver_wrapper.update_row(merged)
                continue

            if chosen_column is None:
                raise RuntimeError(f"Table {self.table!r} has no sensible default value column")

            current_value = None
            if self.has_id(row_id):
                current_value = self._rows_by_id[row_id].get(chosen_column)

            new_value = payload.get(chosen_column)
            if (
                not allow_case_change
                and isinstance(current_value, str)
                and isinstance(new_value, str)
                and current_value.lower() == new_value.lower()
            ):
                continue

            db.driver_wrapper.update_column(self.table, row_id, chosen_column, new_value)

    def delete(
        self,
        table_ids: Iterable[int],
        db: Any,
    ) -> None:
        """
        Delete physical rows, then evict selected IDs and rebuild local value indexes.

        Pass the same iterable to both phases. A one-shot iterator consumed by
        the database helper can leave no IDs for cache eviction, so callers needing
        both effects should supply a reusable collection. No attachment change,
        transaction or related-field refresh is added.

        Example:
            A set {7} is available to both phases, allowing the cached row and its
            value memberships to be removed after the physical deletion.


        :param table_ids: ID iterable consumed separately by database deletion and cache eviction.
        :param db: Database override for deletion; None selects the current attachment.
        :return: None; performs driver deletion before local eviction without a database reload.
        """

        self._delete_from_db(table_ids, db)
        self._delete_from_cache(table_ids)

    def _delete_from_cache(self, table_ids: Iterable[int]) -> None:
        """
        Evict selected cached rows and rebuild indexes from the survivors.

        This does not query or modify the database and does not reset _loaded.
        Even empty input rebuilds indexes. Index failures can follow row eviction.

        Example:
            Evicting ID 7 removes every occurrence of 7 retained in row order.


        :param table_ids: Row IDs consumed into a deduplicated integer set.
        :return: None; removes all selected ID occurrences from rows/order and rebuilds indexes.
        """

        deleted = {int(table_id) for table_id in table_ids}
        self._rows_by_id = {
            row_id: row_dict
            for row_id, row_dict in self._rows_by_id.items()
            if row_id not in deleted
        }
        self._row_order = [row_id for row_id in self._row_order if row_id not in deleted]
        self._refresh_indexes()

    def _delete_from_db(self, table_ids: Iterable[str], db: Any) -> None:
        """
        Delete a deduplicated integer-ID set through the selected database driver.

        Resolve the database before consuming IDs. No cache refresh or attachment
        change occurs here, and conversion/driver failures propagate.

        Example:
            Repeated IDs [7, 7] produce one deletion selector; an empty iterable
            issues no driver delete call.


        :param table_ids: ID iterable consumed into a set with int conversion.
        :param db: Database override; None uses the current attachment.
        :return: None; calls delete_by_id only for a nonempty set.
        """

        db = _ensure_db(self.db, db)
        ids = {int(table_id) for table_id in table_ids}
        if ids:
            db.driver_wrapper.delete_by_id(self.table, ids)


__all__ = ["SchemaBackedMainTableCache"]
