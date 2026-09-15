"""
Implement independent array-backed main tables, fields and storage-cache composition.

NumPy arrays hold row IDs and column values when available; tuple fallbacks
keep the backend importable and usable when require_numpy is disabled. Main
tables and directed link topology feed scalar and relation-field projections.
The root backend owns schema discovery, dependency invalidation and reloads.

Returned internal arrays are shared unless a method explicitly copies them.
Snapshot consistency requires reload or invalidation for external changes.
Legacy table/field mutation helpers perform driver writes and refresh afterward
without adding an encompassing transaction; the modern Cache/Catalog boundary
provides the application-facing write coordination.
"""

from __future__ import annotations

import dataclasses

from collections import defaultdict
from copy import deepcopy
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Optional, Sequence, Union, cast

from LiuXin_alpha.caches.api.storage_cache_api.storage_cache_api import (
    FieldKey,
    StorageCacheAPI,
    StorageCacheCapabilities,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.base_field import (
    FieldBasicInterfaceAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.many_many_field import (
    ManyManyInTwoTableFieldUpdate,
    ManyToManyFieldAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.util_mixins import \
    IndividualLinkProperties as ManyManyIndividualLinkProperties
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.many_one_field import (
    IndividualLinkProperties as ManyOneIndividualLinkProperties,
    ManyOneInTwoTableFieldUpdate,
    ManyToOneFieldAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.one_many_field import (
    IndividualLinkProperties as OneManyIndividualLinkProperties,
    OneManyInTwoTableFieldUpdate,
    OneToManyFieldAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.one_one_field import (
    CacheOneOneInSameTableFieldAPI,
    CacheOneOneInTwoTableFieldAPI,
    OneOneInOneTableFieldUpdate,
    OneOneInTwoTableFieldUpdate,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.base_table import (
    TableMetadata,
    TableTypes,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.many_many_tables import (
    StorageCacheManyToManyLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.many_one_tables import (
    StorageCacheManyToOneLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.one_many_tables import (
    StorageCacheOneToManyLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.one_one_tables import (
    StorageCacheOneToOneLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.single_table import (
    StorageCacheSingleTableAPI,
)
from LiuXin_alpha.caches.cache_plugins.numpy_vectorized.link_table import (
    NumpyVectorizedLinkTable,
)
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.databases.schema_specs import LinkCardinality, StorageTableSpec

try:
    import numpy as _np
except Exception:  # pragma: no cover - availability is environment-specific
    _np = None

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.db_types import MainTableName
    from LiuXin_alpha.databases.schema_specs import StorageSchemaSpec


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


def _canonical_field_key(table_name: str, column_name: str) -> str:
    """
    Join table and column string forms into a qualified field key.

    Example:
        >>> _canonical_field_key("books", "title")
        'books.title'


    :param table_name: Table component inserted by string formatting.
    :param column_name: Column component inserted by string formatting.
    :return: table.column string without escaping, validation or whitespace stripping.
    """

    return f"{table_name}.{column_name}"


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


def _default_value_column(spec: StorageTableSpec) -> Optional[str]:
    """
    Choose the first column outside the table's identity and bookkeeping roles.

    Skip names equal to id_column, parent_column, datestamp_column or
    scratch_column. Do not inspect data types or validate the fallback column.

    Example:
        A schema ordered as id, stamp, title with id/stamp assigned bookkeeping
        roles selects title; a table with only id falls back to id.


    :param spec: Table specification with ordered columns and optional role names.
    :return: First non-role column name, otherwise spec.id_column, possibly None.
    """

    skip = {
        spec.id_column,
        spec.parent_column,
        spec.datestamp_column,
        spec.scratch_column,
    }
    for col in spec.columns:
        if col.name not in skip:
            return col.name
    return spec.id_column


def _normalize_numpy_scalar(value: Any) -> Any:
    """
    Extract a NumPy scalar's item representation while preserving other objects.

    This is not recursive container conversion. NumPy determines the item
    representation, which need not be a built-in Python scalar for every dtype.

    Example:
        >>> _normalize_numpy_scalar(7), _normalize_numpy_scalar(None)
        (7, None)


    :param value: Scalar or arbitrary object returned by array access.
    :return: value.item() for a NumPy generic scalar when available; otherwise the same object.
    """

    if _np is not None and isinstance(value, _np.generic):
        return value.item()
    return value


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


def _column_array_dtype(values: Sequence[Any], declared_type: str) -> Any:
    """
    Select a conservative column dtype from declared type and all observed values.

    With NumPy absent return None. Empty/all-null columns use object. All-present
    Python integers excluding bool use int64 when the type contains INT; real-like
    declarations use float64 for all-present Python int/float values excluding bool.
    All-present strings use inferred dtype regardless of declaration. Other values,
    including nullable numeric columns, use object. This selects representation,
    not a schema or numeric-range validation policy.

    Example:
        A nullable INTEGER column uses object storage to retain None rather than
        converting missing values into an integer sentinel.


    :param values: Sequence inspected for nulls and homogeneous supported Python value types.
    :param declared_type: Declared type text inspected case-insensitively for numeric substrings.
    :return: NumPy int64/float64 type, object, or None for inference/unavailable NumPy.
    """

    if _np is None:
        return None
    non_null = [value for value in values if value is not None]
    if not values:
        return object
    if not non_null:
        return object

    normalized_declared = str(declared_type or "").upper()
    if (
        "INT" in normalized_declared
        and len(non_null) == len(values)
        and all(isinstance(value, int) and not isinstance(value, bool) for value in non_null)
    ):
        return _np.int64
    if (
        any(token in normalized_declared for token in ("REAL", "FLOAT", "DOUBLE"))
        and len(non_null) == len(values)
        and all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in non_null)
    ):
        return _np.float64
    if len(non_null) == len(values) and all(isinstance(value, str) for value in non_null):
        return None
    return object


def _as_column_array(values: Sequence[Any], declared_type: str) -> Any:
    """
    Materialize column values using the selected NumPy dtype or a tuple fallback.

    None from dtype selection requests NumPy inference; other dtypes are passed
    explicitly. Elements are not recursively copied. Nested sequences may produce
    additional array dimensions; this helper does not enforce one-dimensional
    scalar data or catch conversion/overflow errors.

    Example:
        >>> _array_values(_as_column_array([1, None], "INTEGER"))
        [1, None]


    :param values: Sequence copied into an outer tuple before dtype selection.
    :param declared_type: Declared type used by the conservative dtype selector.
    :return: NumPy array when available, otherwise the tuple of supplied values.
    """

    normalized = tuple(values)
    if _np is None:
        return normalized
    dtype = _column_array_dtype(normalized, declared_type)
    if dtype is None:
        return _np.asarray(normalized)
    return _np.asarray(normalized, dtype=dtype)


def _array_value(array_obj: Any, index: int) -> Any:
    """
    Read one array or sequence position and normalize a NumPy scalar result.

    Example:
        >>> _array_value(("first", "last"), -1)
        'last'


    :param array_obj: Indexable array/sequence object.
    :param index: Index forwarded unchanged to subscription.
    :return: Selected value after _normalize_numpy_scalar, without a recursive copy.
    :raises IndexError: A sequence position is out of range; backend indexing errors also propagate.
    """

    return _normalize_numpy_scalar(array_obj[index])


def _array_values(array_obj: Any, *, start: int = 0, end: Optional[int] = None) -> list[Any]:
    """
    Copy a slice into a list while normalizing each NumPy scalar element.

    Example:
        >>> _array_values((1, 2, 3, 4), start=1, end=3)
        [2, 3]


    :param array_obj: Sliceable array or sequence.
    :param start: Start index following the object's slicing semantics.
    :param end: Exclusive end index, or None to continue to the end.
    :return: New list of slice values; nested objects remain shared.
    """

    slice_obj = array_obj[start:end] if end is not None else array_obj[start:]
    return [_normalize_numpy_scalar(value) for value in slice_obj]


class NumpyVectorizedMainTableCache(StorageCacheSingleTableAPI):
    """
    Cache one main table as ordered column arrays and exact value indexes.

    Retain a StorageTableSpec and database without loading at construction.
    Read sorts rows by integer ID, preserving input order for duplicate IDs;
    row_ids retains duplicates while direct ID lookup resolves the last position.
    Column arrays can be NumPy buffers or tuples and exact value indexes use
    Python hash/equality without text normalization. Internal arrays are exposed
    by some accessors, so callers must not mutate them independently of indexes.

    Create/update/delete helpers write through the driver and then reload the
    complete table. They do not add transactions or refresh related field/link
    objects by themselves; the root cache coordinates those dependencies.

    Example:
        After loading rows with IDs 3 and 1, row_ids is (1, 3) and each column
        array follows that same order.
    """

    spec: StorageTableSpec

    def __init__(self, spec: StorageTableSpec, db: Any) -> None:
        """
        Attach table metadata and allocate empty array/index state without reading.

        Copy ordered column names and type descriptions. The schema need not expose
        an ID until an operation requires it; no database validation happens here.

        Example:
            A constructed table reports row_ids=() before read populates its arrays.


        :param spec: Table schema retained by reference and used to derive headings/types.
        :param db: Database reference retained by the table base.
        :return: None; initializes unloaded state with no row or column data.
        """

        metadata = TableMetadata(
            table_name=spec.name,
            main_table=True,
            is_interlink=False,
            is_intralink=False,
        )
        super().__init__(table=spec.name, db=db, metadata=metadata)
        self.spec = spec
        self._column_headings: tuple[str, ...] = tuple(col.name for col in self.spec.columns)
        self._column_types: dict[str, str] = _column_type_map(self.spec)
        self._row_id_array: Any = _as_int_array(())
        self._row_ids: tuple[int, ...] = ()
        self._row_id_positions: dict[int, int] = {}
        self._column_arrays: dict[str, Any] = {}
        self._value_indexes: dict[str, dict[Any, tuple[int, ...]]] = {}
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
        Expose integer row identities in the current array order.

        No copy, refresh or readiness check is performed. Normal loading sorts
        ascending by ID with original input order for ties.

        Example:
            Two loaded rows whose IDs both convert to 7 leave row_ids=(7, 7).


        :return: Stored tuple of row IDs, including duplicates retained during loading.
        """

        return self._row_ids

    @property
    def row_id_array(self) -> Any:
        """
        Expose the internal row-ID buffer without copying or refreshing it.

        Treat the returned buffer as read-only. Mutating a NumPy buffer does not
        rebuild row_ids, position mappings or field projections.

        Example:
            Use row_ids when only an immutable tuple of identities is needed.


        :return: Shared NumPy int64 array or tuple fallback in row_ids order.
        """

        return self._row_id_array

    @property
    def column_headings(self) -> list[str]:
        """
        Copy the schema's ordered column names into a list.

        Example:
            Modifying the returned list does not change which columns the next read loads.


        :return: New list independent of the stored headings tuple.
        """

        return list(self._column_headings)

    @property
    def column_types(self) -> dict[str, str]:
        """
        Copy the stored declared-type/affinity descriptions.

        Example:
            Updating a returned type mapping does not change the table's dtype choices.


        :return: New dict from column name to the cached type description.
        """

        return dict(self._column_types)

    def linked_to(self) -> Iterable[str]:
        """
        Expose endpoint table names declared by the stored schema.

        Example:
            A schema naming covers and tags returns those names in its declared order.


        :return: Tuple of linked table names, without discovering current database links.
        """

        return tuple(self.spec.linked_tables)

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

    def _build_value_indexes(self, row_ids: Sequence[int], column_values: dict[str, Sequence[Any]]) -> None:
        """
        Build exact column-value buckets from aligned row IDs and raw values.

        Use Python hash/equality, retaining duplicate row IDs within buckets. zip
        truncates mismatched sequences rather than validating their lengths. Values
        are not normalized; unhashable values fail before the new index mapping is
        published. Missing column sequences raise KeyError.

        Example:
            Values "Café" and its decomposed Unicode spelling occupy different
            exact-match buckets even though a normalized text query can match both.


        :param row_ids: Row IDs converted to int and materialized once.
        :param column_values: Mapping with a value sequence for each configured column.
        :return: None; publishes new value-to-ID-tuples mappings after all columns succeed.
        :raises TypeError: A raw column value is unhashable or row-ID conversion is unsupported.
        :raises KeyError: column_values lacks a configured column.
        """

        normalized_row_ids = tuple(int(row_id) for row_id in row_ids)
        indexes: dict[str, dict[Any, tuple[int, ...]]] = {}
        for column in self._column_headings:
            values_for_column = column_values[column]
            grouped_ids: dict[Any, list[int]] = defaultdict(list)
            for row_id, value in zip(normalized_row_ids, values_for_column):
                grouped_ids[value].append(row_id)
            indexes[column] = {
                value: tuple(ids)
                for value, ids in grouped_ids.items()
            }
        self._value_indexes = indexes

    def _replace_rows(self, row_dicts: Iterable[Mapping[str, Any]]) -> None:
        """
        Rebuild sorted row identities, column arrays and exact value indexes.

        Require an ID column while processing each input row, convert identity
        values with int, and sort by identity then original input position. Preserve
        raw column values, using None for missing columns. Duplicate identities stay
        in row_ids/arrays, while _row_id_positions keeps their last position.

        Publish row IDs, ID buffer and positions before constructing column arrays
        and value indexes. Later conversion or hashing failures can leave partially
        updated state. Empty input does not execute the per-row missing-ID-column
        check. No database reads or dependency refreshes occur here.

        Example:
            If two rows share ID 7, get_row_snapshot(7) reads the later one in input
            order, while the column-value lists still contain both rows.


        :param row_dicts: Iterable of row mappings; rows with a None ID value are skipped.
        :return: None; publishes arrays/indexes and marks the table loaded after full success.
        """

        column_headings = self._column_headings
        column_types = self._column_types
        id_column = self.spec.id_column
        sortable: list[tuple[int, int, Mapping[str, Any]]] = []
        for index, row_dict in enumerate(row_dicts):
            if id_column is None:
                raise RuntimeError(f"Table {self.table!r} does not expose an id column")
            row_id = row_dict.get(id_column)
            if row_id is None:
                continue
            sortable.append((int(row_id), index, row_dict))
        sortable.sort(key=lambda item: (item[0], item[1]))

        row_ids = tuple(int(row_id) for row_id, _index, _row in sortable)
        column_values: dict[str, list[Any]] = {
            column: []
            for column in column_headings
        }
        for _row_id, _index, row_data in sortable:
            row_get = row_data.get
            for column in column_headings:
                column_values[column].append(row_get(column))

        self._row_ids = row_ids
        self._row_id_array = _as_int_array(row_ids)
        self._row_id_positions = {row_id: position for position, row_id in enumerate(row_ids)}
        self._column_arrays = {
            column: _as_column_array(column_values[column], column_types[column])
            for column in column_headings
        }
        self._build_value_indexes(row_ids, column_values)
        self._loaded = True

    def read(self, db: Any) -> None:
        """
        Attach the selected database and replace table arrays from all physical rows.

        Fetch get_all_rows(iterator_return=False) and require each returned row to
        expose row_dict. Set the database reference before fetching. Read/conversion
        failures propagate and do not restore prior attachment or partial cache state.

        Example:
            ``table.read(None)`` refreshes the current attachment; supplying another
            database changes which connection supplies the rows.


        :param db: Database override; None reuses the current attachment.
        :return: None; loads ordered arrays and exact indexes from row.row_dict payloads.
        :raises RuntimeError: No explicit or attached database is available.
        """

        db = _ensure_db(self.db, db)
        self.db = db
        rows = db.get_all_rows(self.table, iterator_return=False)
        self._replace_rows(row.row_dict for row in rows)

    def reload(self, db: Any) -> None:
        """
        Delegate a complete table reload to read.

        Example:
            Reload after an external table change to rebuild both arrays and value
            indexes from that database.


        :param db: Database override; None reuses the current attachment.
        :return: None; same full-table replacement and error behavior as read.
        """

        self.read(db=db)

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

    def _refresh_ids(self, ids: Iterable[int]) -> None:
        """
        Reload the whole table for a request to refresh particular IDs.

        This backend does not use the supplied IDs to bound work. A detached cache
        raises through _ensure_db before reading.

        Example:
            Even an empty ID iterable requests a full-table reload.


        :param ids: ID iterable ignored without consumption or conversion.
        :return: None; reads the attached database and replaces every cached row.
        """

        del ids
        self.read(_ensure_db(self.db))

    def has_id(self, table_id: int) -> bool:
        """
        Check an integer-converted identity against the current position mapping.

        Example:
            An empty newly constructed table reports false for every valid integer ID.


        :param table_id: Requested row identity converted with int.
        :return: True when an indexed row position exists; no refresh or database fallback.
        """

        return int(table_id) in self._row_id_positions

    def get_column_value_from_id(self, table_id: int, column: str) -> Any:
        """
        Read one column value at the indexed position for a row ID.

        Check row presence before column presence. Duplicate loaded IDs select the
        last indexed position. Errors do not trigger database fallback.

        Example:
            Reading a missing row and a missing column reports the row error first.


        :param table_id: Row identity converted with int.
        :param column: Exact configured column name.
        :return: Stored value with a NumPy scalar converted through item(), without deep copying.
        :raises KeyError: The row ID is absent, or the row exists but the column is not loaded.
        """

        row_id = int(table_id)
        if row_id not in self._row_id_positions:
            raise KeyError(row_id)
        if column not in self._column_arrays:
            raise KeyError(column)
        return _array_value(self._column_arrays[column], self._row_id_positions[row_id])

    def get_values_for(self, column: str) -> Sequence[Any]:
        """
        Copy one loaded column into a list in current row-array order.

        Example:
            A column with two loaded rows containing None and "Dune" returns
            [None, "Dune"] in row-ID order.


        :param column: Exact loaded column name.
        :return: List of scalar-normalized values, preserving duplicate rows and nested references.
        :raises KeyError: The column has no loaded array.
        """

        if column not in self._column_arrays:
            raise KeyError(column)
        return _array_values(self._column_arrays[column])

    def get_unique_values(self, column: str) -> set[Any]:
        """
        Copy the exact value-index keys for one column into a set.

        This reads the index, not the current array. Mutating exposed arrays does
        not make this set reflect those unsupported independent edits.

        Example:
            Two identical title values appear once; differently normalized Unicode
            strings can remain distinct.


        :param column: Configured column whose value index has been built.
        :return: Set of raw distinct values according to Python hash/equality.
        :raises KeyError: The column has no value index.
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
        return set(self._value_indexes[column].get(value, ()))

    def get_col_value_from_id(self, table_id: int) -> Any:
        """
        Read a row's default value column, or its complete snapshot when no default exists.

        The helper's ID-column fallback often makes the default non-None. Row and
        column errors from the selected read path propagate.

        Example:
            A table whose default value column is title returns that title rather
            than the complete row dictionary.


        :param table_id: Requested row identity passed to the selected read helper.
        :return: Default-column value or a new row snapshot dict.
        """

        default_column = self.default_value_column
        if default_column is None:
            return self.get_row_snapshot(table_id)
        return self.get_column_value_from_id(int(table_id), default_column)

    def get_row_snapshot(self, table_id: int) -> dict[str, Any]:
        """
        Copy all column values at one indexed row position into a dictionary.

        Duplicate IDs select their last position. Use get_row for the additional
        deep-copy/read-only Row wrapper.

        Example:
            The returned dictionary can be changed without changing column arrays,
            although mutable objects held inside it may still be shared.


        :param table_id: Row ID converted with int.
        :return: New dict of scalar-normalized values; nested stored objects are not deep-copied.
        :raises KeyError: The integer row ID is not in the position mapping.
        """

        row_id = int(table_id)
        if row_id not in self._row_id_positions:
            raise KeyError(row_id)
        position = self._row_id_positions[row_id]
        return {
            column: _array_value(array_obj, position)
            for column, array_obj in self._column_arrays.items()
        }

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

    def create(
        self,
        table_id_val_map: Mapping[int, Any],
        db: Any,
        target_column: Optional[str] = None,
        allow_case_change: bool = False,
    ) -> None:
        """
        Write normalized row creations then reload the complete table.

        Normalize payloads before iterating driver writes. No transaction is added;
        a later write or full reload can fail after earlier writes succeeded. The
        operation does not itself refresh related link or field cache objects.

        Example:
            After a successful scalar create for one title, the whole table cache
            is reloaded rather than patching only that row in its arrays.


        :param table_id_val_map: Mapping of row IDs to scalar values or column mappings.
        :param db: Database override; None uses the current attachment.
        :param target_column: Truthy scalar target column, otherwise the default value column.
        :param allow_case_change: Compatibility flag accepted but ignored by creation.
        :return: None; after successful writes, read the selected database to rebuild cache state.
        """

        self._create_to_db(
            table_id_val_map,
            db,
            target_column=target_column,
            allow_case_change=allow_case_change,
        )
        self.read(db)

    def _create_to_cache(
        self,
        table_id_val_map: Mapping[int, Any],
        target_column: Optional[str] = None,
        allow_case_change: bool = False,
    ) -> None:
        """
        Refresh all cached rows after a legacy write hook, ignoring the supplied payload.

        No local array patch is applied. Even an empty payload requests a full
        read; detached database and read failures propagate.

        Example:
            The hook relies on the database already containing the intended changes
            and reconstructs the cache from that state.


        :param table_id_val_map: Update payload ignored without normalization.
        :param target_column: Target-column hint ignored.
        :param allow_case_change: Case-change hint ignored.
        :return: None; fully reloads the attached database.
        """

        del table_id_val_map, target_column, allow_case_change
        self.read(_ensure_db(self.db))

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
        Write normalized row updates then reload the complete table.

        Normalize payloads before iterating driver writes. No transaction is added;
        a later write or full reload can fail after earlier writes succeeded. The
        operation does not itself refresh related link or field cache objects.

        Example:
            After a successful scalar update for one title, the whole table cache
            is reloaded rather than patching only that row in its arrays.


        :param table_id_val_map: Mapping of row IDs to scalar values or column mappings.
        :param db: Database override; None uses the current attachment.
        :param target_column: Truthy scalar target column, otherwise the default value column.
        :param allow_case_change: Allow scalar string changes whose lowercased forms equal the cached value.
        :return: None; after successful writes, read the selected database to rebuild cache state.
        """

        self._update_db(
            table_id_val_map,
            db,
            target_column=target_column,
            allow_case_change=allow_case_change,
        )
        self.read(db)

    def _update_cache(
        self,
        table_id_val_map: Mapping[int, Any],
        target_column: Optional[str] = None,
        allow_case_change: bool = False,
    ) -> None:
        """
        Refresh all cached rows after a legacy write hook, ignoring the supplied payload.

        No local array patch is applied. Even an empty payload requests a full
        read; detached database and read failures propagate.

        Example:
            The hook relies on the database already containing the intended changes
            and reconstructs the cache from that state.


        :param table_id_val_map: Update payload ignored without normalization.
        :param target_column: Target-column hint ignored.
        :param allow_case_change: Case-change hint ignored.
        :return: None; fully reloads the attached database.
        """

        del table_id_val_map, target_column, allow_case_change
        self.read(_ensure_db(self.db))

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
        :return: None; writes accepted updates without refreshing arrays or changing self.db.
        :raises KeyError: A normalized integer key is absent from the original mapping, a mapping update row is missing, or a cached column lookup fails.
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
                current_value = self.get_column_value_from_id(row_id, chosen_column)

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
        Delete selected physical rows then reload the complete table cache.

        Even an empty ID set is followed by a full read. No transaction or related
        field/link refresh is added; refresh errors can follow successful deletion.

        Example:
            Deleting a single row rebuilds the remaining ID positions and all column
            arrays rather than leaving a hole in their old positions.


        :param table_ids: Iterable of IDs converted to a deduplicated integer set by the write helper.
        :param db: Database override; None uses the current attachment.
        :return: None; after deletion, rebuilds all table arrays/indexes from the selected database.
        """

        self._delete_from_db(table_ids, db)
        self.read(db)

    def _delete_from_cache(self, table_ids: Iterable[int]) -> None:
        """
        Reload the whole table instead of locally evicting requested IDs.

        Example:
            This hook observes database deletions already performed by its caller.


        :param table_ids: ID iterable ignored without consuming it.
        :return: None; reads the currently attached database.
        """

        del table_ids
        self.read(_ensure_db(self.db))

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


class NumpyVectorizedSameTableField(CacheOneOneInSameTableFieldAPI[Any]):
    """
    Expose a scalar column using a main table and bound array/index references.

    Construction resolves in_table through the root cache but starts with empty
    field buffers. read binds references to that table's current positions,
    column array and value index without reading the database. Direct value/index
    getters use these bound references, while ids, values and ids_values_map
    consult the table object. After table arrays are replaced, read must rebind
    the field; refresh_ids alone changes only its database reference and
    remove_ids is a no-op.

    Legacy updates validate row existence and nullability before each group of
    writes, update the main table, then rebind after all groups. Clearing values
    must not delete their owning rows; no transaction is added here.

    Example:
        After a table reload replaces its title array, call field.read(db) to bind
        direct field lookups to that new array.
    """

    def __init__(
        self,
        cache: "NumpyVectorizedStorageCache",
        in_table: Union[StorageCacheSingleTableAPI, str],
        column_name: str,
        db: Any,
    ) -> None:
        """
        Resolve the owning table and allocate empty scalar-field references.

        Initialize empty positions, value array and reverse index before delegating
        to the scalar API constructor. Table resolution errors propagate; column
        existence is not checked by this constructor.

        Example:
            A newly constructed field can refer to a loaded table while direct
            get_value_from_id still returns None until read binds its buffers.


        :param cache: Root NumPy storage cache used to resolve table references.
        :param in_table: Table name or table API object resolved by the inherited constructor.
        :param column_name: Column name converted with str.
        :param db: Database reference retained for later reads and writes.
        :return: None; attaches the table but does not bind its loaded arrays until read.
        """

        self._cache = cache
        self.column_name = str(column_name)
        self._row_id_positions: dict[int, int] = {}
        self._value_array: Any = _as_column_array((), "TEXT")
        self._value_index: dict[Any, tuple[int, ...]] = {}
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

    def _table_cache(self) -> NumpyVectorizedMainTableCache:
        """
        Return the already bound table under its concrete NumPy type annotation.

        Example:
            This helper does not fetch a replacement table from the root cache after
            a root-level schema rebuild.


        :return: The same in_table object; cast performs no runtime validation or lookup.
        """

        return cast(NumpyVectorizedMainTableCache, self.in_table)

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
        column as target. The table performs its own full reload. This helper does
        not rebind the field's cached references or add a transaction.

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

    def read(self, db: "DatabaseAPI") -> None:
        """
        Bind scalar lookup references to the main table's current loaded buffers.

        Do not read or reload the table and do not reattach its database. Missing
        reverse indexes default to an empty dict, but a missing column array raises
        after the database and position references have already changed.

        Example:
            Rebinding a field after table.read makes direct value lookups use the
            newly allocated column array.


        :param db: Database override; None retains the field's current database reference.
        :return: None; stores the database and shares table positions, column array and reverse index.
        :raises RuntimeError: No explicit or attached field database is available.
        :raises KeyError: The bound table has no array for this column.
        """

        self._db = _ensure_db(self._db, db)
        table = self._table_cache()
        self._row_id_positions = table._row_id_positions
        self._value_array = table._column_arrays[self.column_name]
        self._value_index = table._value_indexes.get(self.column_name, {})

    def refresh_ids(
        self,
        ids: Iterable[int],
        db: Optional["DatabaseAPI"] = None,
    ) -> None:
        """
        Accept a bounded refresh hint while updating only the field database reference.

        Example:
            Call read to rebind replaced table arrays; refresh_ids does not perform
            that work even when the supplied IDs are nonempty.


        :param ids: ID iterable ignored without consumption.
        :param db: Database override; None retains the current field attachment.
        :return: None; stores the selected database without reloading or rebinding buffers.
        :raises RuntimeError: Neither an explicit nor current database is available.
        """

        del ids
        self._db = _ensure_db(self._db, db)

    def remove_ids(self, ids: Iterable[int]) -> None:
        """
        Accept an ID-removal hint without changing this scalar view.

        Example:
            Removing a row from the main table requires the table operation and a
            later field read; this hint alone does not evict it.


        :param ids: ID iterable ignored without consumption or conversion.
        :return: None; no rows, array values or index entries are removed.
        """

        del ids

    @property
    def ids(self) -> set[int]:
        """
        Copy distinct integer owner IDs from the bound table's current row tuple.

        This consults the table rather than the field's last-bound positions.

        Example:
            Duplicate table identities (7, 7) produce the field ID set {7}.


        :return: Set of table row IDs, deduplicating any duplicate loaded identities.
        """

        return set(int(row_id) for row_id in self.in_table.row_ids)

    @property
    def values(self) -> list[Any]:
        """
        Copy the bound table's current scalar column values in row-array order.

        Use the table's live-in-object column lookup rather than the field's
        last-bound _value_array. This does not imply live database consistency.

        Example:
            After the table replaces its arrays, values can observe the new column
            while an unrebound direct field getter still holds the previous array.


        :return: List of values with NumPy scalar normalization and nested references retained.
        """

        return list(self._table_cache().get_values_for(self.column_name))

    @property
    def values_set(self) -> set[Any]:
        """
        Copy distinct values from the field's last-bound reverse index.

        Example:
            The value set stays tied to the bound index until read rebinds the field.


        :return: Set of index keys using Python hash/equality without text normalization.
        """

        return set(self._value_index.keys())

    @property
    def ids_values_map(self) -> dict[int, Optional[Any]]:
        """
        Project each current table ID to its scalar column value.

        Consult the bound table directly for each ID. Duplicate IDs collapse into
        one mapping entry and select the table's last indexed row position.

        Example:
            If the table has duplicate ID 7 rows, this mapping retains only the
            value retrieved by table.get_column_value_from_id(7, column_name).


        :return: New dict of integer ID to current table value, including None.
        """

        table = self._table_cache()
        return {
            int(row_id): table.get_column_value_from_id(int(row_id), self.column_name)
            for row_id in table.row_ids
        }

    def get_value_from_id(self, table_id: int) -> Optional[Any]:
        """
        Read a scalar value through the field's last-bound position and value buffers.

        A stored None and an absent row have the same result. No refresh, column
        resolution or database fallback is performed.

        Example:
            Before the first field read, every valid integer ID is absent from the
            initial empty position mapping.


        :param table_id: Owner row ID converted with int.
        :return: Scalar-normalized value, or None when the bound positions lack the ID.
        """

        position = self._row_id_positions.get(int(table_id))
        if position is None:
            return None
        return _array_value(self._value_array, position)

    def get_ids_from_value(self, value: Any) -> list[int]:
        """
        Read owner IDs from the field's last-bound exact value index.

        Python hash/equality determines matching. The method neither normalizes
        Unicode text nor resolves the table's newer index after a reload.

        Example:
            A repeated owner ID within a value bucket remains repeated in this list.


        :param value: Hashable lookup value retained unchanged.
        :return: New list of matching IDs in index order, retaining duplicates; empty on no match.
        :raises TypeError: The lookup value is unhashable.
        """

        return list(self._value_index.get(value, ()))

    def get_numpy_owner_ids_array(self) -> Any:
        """
        Expose the bound table's current row-ID buffer without copying.

        This can be newer than the field's value buffer after an unrebound table
        reload. Treat the buffer as read-only to preserve index consistency.

        Example:
            Rebind after table refresh before pairing this owner-ID buffer with
            get_numpy_values_array.


        :return: Shared NumPy integer array or tuple fallback from the bound table.
        """

        return self._table_cache().row_id_array

    def get_numpy_values_array(self) -> Any:
        """
        Expose the scalar value buffer last bound by read.

        No copy, refresh or table lookup is performed. Independent array mutation
        does not update reverse indexes.

        Example:
            Repeated calls return the same bound buffer until read replaces its reference.


        :return: Shared NumPy array or tuple fallback retained by this field.
        """

        return self._value_array

    def update(self, update: OneOneInOneTableFieldUpdate[Any]) -> None:
        """
        Apply added, changed and cleared scalar values then rebind field buffers.

        Require an attached database, then process added maps, updated maps and
        deleted IDs in that order. Added maps update existing owners rather than
        creating rows; deleted IDs write None rather than deleting owners. Each
        nonempty group has its own row/nullability prechecks and table update/reload.
        Earlier groups can persist if a later group fails; no transaction or rollback
        is added, and failure before the final read can leave field buffers stale.

        Example:
            Deleting a field value on a nullable column clears the column while
            keeping its owner row; clearing a primary-key value is rejected.


        :param update: OneOneInOneTableFieldUpdate with added_maps, updated_maps and deleted_ids.
        :return: None; after all requested groups succeed, read rebinds the scalar field.
        """

        self._db = _ensure_db(self._db)
        if update.added_maps:
            self._write_values({int(row_id): value for row_id, value in update.added_maps.items()})
        if update.updated_maps:
            self._write_values({int(row_id): value for row_id, value in update.updated_maps.items()})
        if update.deleted_ids:
            self._write_values({int(row_id): None for row_id in update.deleted_ids})
        self.read(self._db)


class _NumpyVectorizedRelationFieldBase:
    """
    Share packed relation projections and legacy write helpers across cardinalities.

    The concrete field/API constructor must supply source/destination tables,
    column names and a directed link table. This mixin combines bound topology
    arrays with direct endpoint/link reads; they can differ until the root cache
    refreshes and rebinds all dependencies. Returned internal arrays and dictionaries
    are shared, and no concurrency or transaction boundary is added here.

    Example:
        Concrete one-to-one and many-valued relation fields reuse offset-delimited
        destination buffers while exposing different public scalar/plural APIs.
    """

    def _init_relation_field(
        self,
        cache: "NumpyVectorizedStorageCache",
        db: Any,
    ) -> None:
        """
        Retain the owner/database and initialize empty relation projection buffers.

        Endpoint tables and link table are established separately by the concrete
        API constructor. This helper performs no database read.

        Example:
            >>> field = _NumpyVectorizedRelationFieldBase()
            >>> field._init_relation_field(object(), None)
            >>> field._src_ids, tuple(int(v) for v in field._src_offsets)
            ((), (0,))


        :param cache: Root NumPy storage cache used by field table resolution.
        :param db: Database reference retained without validation or loading.
        :return: None; creates empty topology references and a zero offset sentinel.
        """

        self._cache = cache
        self._db = db
        self._src_ids_array: Any = _as_int_array(())
        self._src_ids: tuple[int, ...] = ()
        self._src_positions: dict[int, int] = {}
        self._src_offsets: Any = _as_int_array((0,))
        self._flat_dst_ids: Any = _as_int_array(())
        self._flat_dst_positions: Any = _as_int_array(())
        self._flat_values: Any = _as_column_array((), "TEXT")
        self._has_missing_dst = False
        self._dst_to_src_ids: dict[int, tuple[int, ...]] = {}

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

    def _link_table_cache(self) -> NumpyVectorizedLinkTable:
        """
        Return the bound link table under the concrete NumPy annotation.

        Example:
            Topology helpers access the directed link view already chosen by the
            concrete relation field constructor.


        :return: The same link_table object; cast adds no validation, lookup or refresh.
        """

        return cast(NumpyVectorizedLinkTable, self.link_table)

    def _value_for_dst_id(self, dst_id: int) -> Optional[Any]:
        """
        Read a destination column value when its endpoint row is cached.

        Check destination table membership before reading the configured column.
        An absent endpoint and a stored None both return None; column errors propagate.

        Example:
            A dangling link projects None here rather than requiring a destination
            Row snapshot to exist.


        :param dst_id: Destination ID converted with int.
        :return: Scalar-normalized destination value, or None when the endpoint is absent.
        """

        dst_id = int(dst_id)
        if self.dst_table.has_id(dst_id):
            return cast(NumpyVectorizedMainTableCache, self.dst_table).get_column_value_from_id(
                dst_id,
                self.dst_table_cache_col,
            )
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

        This uses current link-table record indexes rather than the field's
        previously bound flat topology. It does not verify opposite endpoint rows.

        Example:
            A repeated physical link remains a repeated endpoint ID in the result.


        :param src_id: Source ID converted with int.
        :param require_ordering: Ordering hint forwarded to the link table; its priority policy determines order.
        :param type_filter: Optional exact type filter forwarded to link selection.
        :return: Tuple of integer endpoint IDs in link order, preserving duplicate links.
        """

        return tuple(
            int(dst_id)
            for dst_id in cast(Any, self.link_table).get_dst_ids(
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

        This uses current link-table record indexes rather than the field's
        previously bound flat topology. It does not verify opposite endpoint rows.

        Example:
            A repeated physical link remains a repeated endpoint ID in the result.


        :param dst_id: Destination ID converted with int.
        :param require_ordering: Ordering hint forwarded to the link table; its priority policy determines order.
        :param type_filter: Optional exact type filter forwarded to link selection.
        :return: Tuple of integer endpoint IDs in link order, preserving duplicate links.
        """

        return tuple(
            int(src_id)
            for src_id in cast(Any, self.link_table).get_src_ids(
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
    ) -> tuple[Optional[Any], ...]:
        """
        Project destination values for every accepted link from a source.

        Read link IDs first, then read each destination value through endpoint
        membership checking. This path does not use the bound flat-value buffer.

        Example:
            If a source links to an existing destination and a deleted destination,
            the second projected value is None.


        :param src_id: Source ID converted with int.
        :param require_ordering: Ordering hint forwarded to the link-table lookup.
        :param type_filter: Optional exact type filter forwarded to link selection.
        :return: Tuple of values in link order, retaining duplicates and None for absent destinations.
        """

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
    ) -> Optional[Any]:
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

    def _src_slice(self, src_id: int) -> Optional[tuple[int, int]]:
        """
        Resolve one source group's half-open bounds in the bound flat buffers.

        Use the stored positions and adjacent offset entries. This does not inspect
        the current link table or validate offset consistency.

        Example:
            >>> field = _NumpyVectorizedRelationFieldBase()
            >>> field._init_relation_field(object(), None)
            >>> field._src_slice(7) is None
            True


        :param src_id: Source ID converted with int for group-position lookup.
        :return: (start, end) integer offsets, or None when the source has no bound group.
        """

        position = self._src_positions.get(int(src_id))
        if position is None:
            return None
        start = int(_array_value(self._src_offsets, position))
        end = int(_array_value(self._src_offsets, position + 1))
        return (start, end)

    def _cached_dst_ids_for_src(self, src_id: int) -> tuple[int, ...]:
        """
        Copy destination IDs from one source's bound topology slice.

        No type filter or new link lookup is applied to the bound topology.

        Example:
            A bound group with duplicate destination ID 7 returns (7, 7).


        :param src_id: Source ID converted with int.
        :return: Tuple of integer destination IDs, retaining duplicates; empty for no source group.
        """

        slice_bounds = self._src_slice(int(src_id))
        if slice_bounds is None:
            return ()
        start, end = slice_bounds
        return tuple(int(value) for value in _array_values(self._flat_dst_ids, start=start, end=end))

    def _cached_values_for_src(self, src_id: int) -> tuple[Optional[Any], ...]:
        """
        Copy projected values from one source's bound flat-value slice.

        Read _flat_values without refreshing destination data or reapplying type
        filters. Mutable element references can remain shared.

        Example:
            A missing destination position represented during projection remains
            None in this tuple until the relation cache is refreshed.


        :param src_id: Source ID converted with int.
        :return: Tuple of scalar-normalized values, including None; empty for no source group.
        """

        slice_bounds = self._src_slice(int(src_id))
        if slice_bounds is None:
            return ()
        start, end = slice_bounds
        return tuple(_array_values(self._flat_values, start=start, end=end))

    def _project_flat_values(self) -> Any:
        """
        Gather destination-column values in the bound topology's flat link order.

        With NumPy present and no missing destination flag, use advanced integer
        indexing into the destination column array. Otherwise iterate positions,
        replace negative positions with None and build a column buffer using its
        declared type. Missing columns, invalid positions and conversion failures
        propagate. The correctness of the fast path relies on the bound missing flag
        and positions remaining consistent.

        Example:
            Positions (2, -1) yield the third destination value followed by None
            when missing-destination handling is active.


        :return: Projected NumPy array or tuple fallback aligned with _flat_dst_positions.
        """

        dst_table = cast(NumpyVectorizedMainTableCache, self.dst_table)
        column_array = dst_table._column_arrays[self.dst_table_cache_col]
        if _np is not None and not self._has_missing_dst:
            return column_array[self._flat_dst_positions]

        positions = _array_values(self._flat_dst_positions)
        column_type = dst_table.column_types.get(self.dst_table_cache_col, "UNKNOWN")
        values = [
            None if int(position) < 0 else _array_value(column_array, int(position))
            for position in positions
        ]
        return _as_column_array(values, column_type)

    def _dst_ids_values_map(self) -> dict[int, Optional[Any]]:
        """
        Project current endpoint values for destination IDs present in bound reverse topology.

        Keys come from _dst_to_src_ids while values are read from the bound table
        object. This does not include unlinked destination rows.

        Example:
            A destination appearing in several source groups contributes one mapping
            key even though its reverse topology can list several sources.


        :return: New dict from linked destination ID to value, with None for missing endpoints.
        """

        return {
            int(dst_id): self._value_for_dst_id(int(dst_id))
            for dst_id in self._dst_to_src_ids
        }

    def _get_dst_ids_from_value(self, value: Any) -> list[int]:
        """
        Find sorted destination value matches that also appear in bound relation topology.

        Use table hash/equality without Unicode normalization, then restrict to
        _dst_to_src_ids keys. This can mix a newer table index with older topology
        until the field is rebound.

        Example:
            An unlinked destination with the requested value is excluded even when
            the destination table's value index finds it.


        :param value: Hashable value passed unchanged to the destination table's exact index.
        :return: Sorted list of matching integer destination IDs retained by reverse topology.
        """

        dst_ids = [
            int(dst_id)
            for dst_id in self.dst_table.get_ids_for_value(self.dst_table_cache_col, value)
            if int(dst_id) in self._dst_to_src_ids
        ]
        dst_ids.sort()
        return dst_ids

    def _get_src_ids_from_value(self, value: Any) -> list[int]:
        """
        Expand linked destination value matches into sorted source IDs.

        Find linked destination IDs through _get_dst_ids_from_value, concatenate
        their bound reverse source tuples, then sort globally. Do not deduplicate.

        Example:
            If two matching destinations both point back to source 1, this returns
            [1, 1] rather than [1].


        :param value: Hashable destination-column value used for exact matching.
        :return: Sorted source-ID list retaining duplicate links and repeated source matches.
        """

        src_ids: list[int] = []
        for dst_id in self._get_dst_ids_from_value(value):
            src_ids.extend(int(src_id) for src_id in self._dst_to_src_ids.get(int(dst_id), ()))
        return sorted(src_ids)

    def _read_relation_cache(self) -> None:
        """
        Bind current link topology and rebuild projected destination values.

        Copy references from get_relation_topology without refreshing the link
        table. Bind source IDs, positions, offsets, destination buffers, missing flag
        and reverse mapping before projecting values. Projection failure can leave
        new topology references paired with the previous flat-value buffer.

        Example:
            After a link-table rebuild, this helper rebinds the new group offsets
            and gathers destination values in the new flat-link order.


        :return: None; shares topology members and replaces this field's flat-value buffer.
        """

        topology = self._link_table_cache().get_relation_topology()
        self._src_ids = topology.src_ids
        self._src_ids_array = topology.src_ids_array
        self._src_positions = topology.src_positions
        self._src_offsets = topology.src_offsets
        self._flat_dst_ids = topology.flat_dst_ids
        self._flat_dst_positions = topology.flat_dst_positions
        self._has_missing_dst = topology.has_missing_dst
        self._dst_to_src_ids = topology.dst_to_src_ids
        self._flat_values = self._project_flat_values()

    def read(self, db: Any) -> None:
        """
        Attach field and endpoint/link table database references, then bind relation values.

        Set this field's _db and each bound table's catalog attribute before
        _read_relation_cache. It assumes table/link data has already been refreshed.
        Attachment or projection failures propagate without restoring old references.

        Example:
            The root cache refreshes endpoint/link data before asking relation
            fields to read and bind their projections.


        :param db: Database override; None uses the field's current attachment.
        :return: None; rebinds topology and projects values without reloading underlying tables.
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
        Rebind the whole relation field for a bounded ID refresh hint.

        This does not itself reload endpoint tables or link records. The selector
        does not limit the projection work.

        Example:
            Even a one-ID hint reprojects the complete bound relation topology.


        :param ids: ID iterable ignored without consumption.
        :param db: Database override; None uses the current attachment.
        :return: None; calls read for the complete relation projection.
        """

        del ids
        self.read(_ensure_db(self._db, db))

    def remove_ids(self, ids: Iterable[int]) -> None:
        """
        Rebind relation values after an ID-removal hint when attached.

        This does not delete rows, remove links or locally prune topology entries.
        Underlying caches must already reflect the intended deletion.

        Example:
            A detached relation field ignores remove_ids without raising for a
            missing database.


        :param ids: ID iterable ignored without consumption or conversion.
        :return: None; calls read when attached, otherwise does nothing.
        """

        del ids
        if self._db is not None:
            self.read(self._db)

    def _flattened_values(self) -> list[Optional[Any]]:
        """
        Copy the complete bound flat-value buffer into a scalar-normalized list.

        Mutable element references can remain shared. There is no type filter,
        deduplication or destination refresh in this conversion.

        Example:
            Two source groups projecting ("A", "B") and ("B",) flatten to
            ["A", "B", "B"].


        :return: New list in packed link order, including duplicates and None placeholders.
        """

        return list(_array_values(self._flat_values))

    def get_numpy_owner_ids_array(self) -> Any:
        """
        Expose the bound buffer of source IDs that have topology groups.

        The buffer length can differ from the flat-value buffer length for plural
        relations. Group offsets provide their alignment; do not pair them elementwise
        without applying those offsets.

        Example:
            A source with three destination links contributes one owner-ID entry
            and three flat-value entries.


        :return: Shared NumPy integer array or tuple fallback, one entry per source group.
        """

        return self._src_ids_array

    def get_numpy_values_array(self) -> Any:
        """
        Expose the bound flat destination-value buffer without copying.

        Treat internal arrays as read-only. They are neither a scalar-per-owner
        array for plural relations nor automatically refreshed destination values.

        Example:
            Use the source offsets to slice a source's values from this buffer.


        :return: Shared NumPy array or tuple fallback, one value per packed link entry.
        """

        return self._flat_values

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
            >>> _NumpyVectorizedRelationFieldBase._validate_create_policy(
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

    def _update_dst_values(self, dst_values_map: dict[int, Optional[Any]]) -> None:
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
        :return: None; a nonempty mapping writes values and reloads the destination table.
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

        Forward bool(link_spec.ordered) as require_ordering. The NumPy link table
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

    def _get_unique_dst_id_for_value(self, value: Any) -> Optional[int]:
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

        matches = sorted(
            int(dst_id)
            for dst_id in self.dst_table.get_ids_for_value(self.dst_table_cache_col, value)
        )
        if not matches:
            return None
        if len(matches) > 1:
            raise ValueError(
                f"Field {self.field_key!r} found multiple dst rows for value {value!r}: {matches}"
            )
        return matches[0]

    def _create_related_dst_row(self, value: Any) -> int:
        """
        Create a destination row and refresh its table through the available driver path.

        Require a database. Prefer callable get_blank_row: obtain its payload, set
        the value, call update_row and read the ID from the payload. This path assumes
        the blank-row helper provides an identified row and does not fall back after
        a failure. Otherwise call add_row with just the value column and reject a
        None returned identity. Refresh destination IDs afterward; this backend
        implements that as a complete table read. A failure can follow durable row
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
        dst_updates: dict[int, Optional[Any]] = {}

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

            desired_value = cast(Optional[Any], getattr(link_update, "dst_col_val", None))
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

    def _ensure_existing_sequence_targets(
        self,
        updates: dict[int, Sequence[Optional[Any]]],
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
        field topology/projections are not rebound here.

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


class NumpyVectorizedTwoTableOneOneField(
    _NumpyVectorizedRelationFieldBase,
    CacheOneOneInTwoTableFieldAPI[Any],
):
    """
    Project destination values for a one-to-one relation through bound topology.

    The inherited API resolves endpoint tables and the required directed link
    type; read binds topology and value buffers afterward. Unfiltered cached
    reads use those bound buffers, while selected filtered/reverse paths consult
    the link table. External writes require coordinated cache refresh.
    Source value lookups select the first bound value without separately
    checking cardinality; malformed duplicate links need not raise on this path.

    Example:
        >>> issubclass(NumpyVectorizedTwoTableOneOneField, CacheOneOneInTwoTableFieldAPI)
        True
    """

    def __init__(
        self,
        cache: "NumpyVectorizedStorageCache",
        src_table: Union[StorageCacheSingleTableAPI, str],
        src_table_id_col: str,
        dst_table: Union[StorageCacheSingleTableAPI, str],
        dst_table_cache_col: str,
        db: Any,
    ) -> None:
        """
        Resolve one-to-one endpoint/link objects and initialize empty projection state.

        Initialize common empty buffers first, then invoke the specific relation
        API constructor. Table/link resolution and type-check failures propagate.
        Call read after endpoint and link data have been initialized.

        Example:
            Construction establishes the field route; root initialize_fields later
            binds the loaded link topology and destination values.


        :param cache: Owning NumPy storage cache for endpoint/link resolution.
        :param src_table: Source table name or table API reference.
        :param src_table_id_col: Source identity column retained by the relation API.
        :param dst_table: Destination table name or table API reference.
        :param dst_table_cache_col: Destination column whose values are projected.
        :param db: Database reference retained for later binding and writes.
        :return: None; attaches the relation without reading its topology/value buffers.
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
    ) -> NumpyVectorizedLinkTable:
        """
        Resolve the directed one-to-one link table through the owning cache.

        Delegate to root.get_one_one_link_table. The cast adds no new runtime
        validation; unknown tables/routes or mismatched cardinality propagate as errors.

        Example:
            An explicit cardinality mismatch is rejected instead of silently using
            a different relation shape.


        :param src_table: Source table name or table API reference.
        :param dst_table: Destination table name or table API reference.
        :return: Bound NumPy link-table object after root freshness and cardinality checks.
        """

        return cast(NumpyVectorizedLinkTable, self._cache.get_one_one_link_table(src_table, dst_table))

    @property
    def ids(self) -> set[int]:
        """
        Copy distinct source IDs represented in the field's bound topology.

        Example:
            A source table row without links does not appear in this relation ID set.


        :return: Set of source IDs with cached relation groups; unlinked source rows are omitted.
        """

        return set(self._src_ids)

    @property
    def values(self) -> list[Any]:
        """
        Copy all projected values in packed link order across source groups.

        This is not a per-owner dictionary or a unique destination-value list.
        It uses the last-bound flat-value buffer without refreshing.

        Example:
            Two sources linked to the same destination contribute that value twice.


        :return: Flat list of scalar-normalized values, retaining duplicate links and None.
        """

        return self._flattened_values()

    @property
    def values_set(self) -> set[Any]:
        """
        Collect distinct values from the field's bound flat projection.

        Example:
            Several links projecting the same string contribute one set entry.


        :return: Set of values using Python hash/equality, including None when present.
        :raises TypeError: A projected value is unhashable.
        """

        return set(self.values)

    @property
    def ids_values_map(self) -> dict[int, Optional[Any]]:
        """
        Map bound source IDs to their first projected value.

        Only sources present in _src_ids are included. Read bound flat-value
        slices without reloading destination data or filtering link types.

        Example:
            Unlinked owners are omitted rather than inserted with a None value.


        :return: New dict of source ID to first value, or None for an empty group.
        """

        return {src_id: (values[0] if values else None) for src_id, values in (
            (src_id, self._cached_values_for_src(src_id))
            for src_id in self._src_ids
        )}

    @property
    def dst_ids_values_map(self) -> dict[int, Optional[Any]]:
        """
        Map topology-linked destination IDs to current cached endpoint values.

        Keys come from bound reverse topology; values use current endpoint-table
        lookups. Unlinked destination rows are excluded.

        Example:
            A dangling destination retained in topology appears with value None.


        :return: New dict with None for destination IDs missing from the bound destination table.
        """

        return self._dst_ids_values_map()

    def get_value_from_src_id(self, src_id: int) -> Optional[Any]:
        """
        Read the first bound projected value for a source without a fresh link lookup.

        This path does not enforce singularity or search later values for a
        non-null result. It does not refresh stale topology or destination buffers.

        Example:
            A malformed group projecting (None, "later") returns None.


        :param src_id: Source ID converted with int.
        :return: First projected value, or None for no bound group or a None first value.
        """

        values = self._cached_values_for_src(int(src_id))
        return values[0] if values else None

    def get_value_from_dst_id(self, dst_id: int) -> Optional[Any]:
        """
        Read the projected column directly from a cached destination row.

        This path does not restrict the ID to the field's bound topology. A
        missing projected column on an existing row can still raise.

        Example:
            A destination with no source links can still be read by its ID here.


        :param dst_id: Destination ID converted with int.
        :return: Destination value or None for an absent endpoint, without requiring a link.
        """

        return self._value_for_dst_id(int(dst_id))

    def get_dst_id_from_src_id(self, src_id: int) -> Optional[int]:
        """
        Select the first destination ID from a source's bound topology group.

        No cardinality validation or endpoint-existence check is added to this
        first-item projection.

        Example:
            If a malformed group contains (7, 8), this method selects 7.


        :param src_id: Source ID converted with int.
        :return: First destination ID, or None for no group.
        """

        dst_ids = self._cached_dst_ids_for_src(int(src_id))
        return dst_ids[0] if dst_ids else None

    def get_src_id_from_dst_id(self, dst_id: int) -> Optional[int]:
        """
        Select the first source ID from the bound destination reverse topology.

        Return the first bound entry without checking uniqueness or source-table
        membership again.

        Example:
            A repeated reverse source tuple (1, 1) returns 1 rather than raising.


        :param dst_id: Destination ID converted with int.
        :return: First integer source ID, or None when no reverse entry exists.
        """

        src_ids = self._dst_to_src_ids.get(int(dst_id), ())
        return int(src_ids[0]) if src_ids else None

    def get_src_ids_from_value(self, value: Any) -> list[int]:
        """
        Find source IDs linked to exact destination-column value matches.

        Restrict destination matches to the bound reverse topology, expand its
        source tuples and sort. No Unicode normalization or deduplication is added.

        Example:
            A source linked to two matching destinations appears twice.


        :param value: Hashable value passed unchanged to the destination table index.
        :return: Sorted source-ID list, retaining repetitions across matching links/destinations.
        """

        return self._get_src_ids_from_value(value)

    def get_dst_ids_from_value(self, value: Any) -> list[int]:
        """
        Find linked destination IDs whose cached column value matches exactly.

        Unlinked value matches are excluded; Python index hash/equality determines
        matching rather than the facade's normalized text rules.

        Example:
            An unlinked tag with the same name is excluded from this field lookup.


        :param value: Hashable value passed unchanged to the destination table index.
        :return: Sorted list of matching destination IDs present in bound reverse topology.
        """

        return self._get_dst_ids_from_value(value)

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
        :return: None; updates destination values/links and conditionally rebinds projection state.
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


class NumpyVectorizedManyOneField(
    _NumpyVectorizedRelationFieldBase,
    ManyToOneFieldAPI[Any],
):
    """
    Project destination values for a many-to-one relation through bound topology.

    The inherited API resolves endpoint tables and the required directed link
    type; read binds topology and value buffers afterward. Unfiltered cached
    reads use those bound buffers, while selected filtered/reverse paths consult
    the link table. External writes require coordinated cache refresh.
    Source value lookups select the first bound value without separately
    checking cardinality; malformed duplicate links need not raise on this path.

    Example:
        >>> issubclass(NumpyVectorizedManyOneField, ManyToOneFieldAPI)
        True
    """

    def __init__(
        self,
        cache: "NumpyVectorizedStorageCache",
        src_table: Union[StorageCacheSingleTableAPI, str],
        src_table_id_col: str,
        dst_table: Union[StorageCacheSingleTableAPI, str],
        dst_table_cache_col: str,
        db: Any,
    ) -> None:
        """
        Resolve many-to-one endpoint/link objects and initialize empty projection state.

        Initialize common empty buffers first, then invoke the specific relation
        API constructor. Table/link resolution and type-check failures propagate.
        Call read after endpoint and link data have been initialized.

        Example:
            Construction establishes the field route; root initialize_fields later
            binds the loaded link topology and destination values.


        :param cache: Owning NumPy storage cache for endpoint/link resolution.
        :param src_table: Source table name or table API reference.
        :param src_table_id_col: Source identity column retained by the relation API.
        :param dst_table: Destination table name or table API reference.
        :param dst_table_cache_col: Destination column whose values are projected.
        :param db: Database reference retained for later binding and writes.
        :return: None; attaches the relation without reading its topology/value buffers.
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
    ) -> NumpyVectorizedLinkTable:
        """
        Resolve the directed many-to-one link table through the owning cache.

        Delegate to root.get_many_one_link_table. The cast adds no new runtime
        validation; unknown tables/routes or mismatched cardinality propagate as errors.

        Example:
            An explicit cardinality mismatch is rejected instead of silently using
            a different relation shape.


        :param src_table: Source table name or table API reference.
        :param dst_table: Destination table name or table API reference.
        :return: Bound NumPy link-table object after root freshness and cardinality checks.
        """

        return cast(NumpyVectorizedLinkTable, self._cache.get_many_one_link_table(src_table, dst_table))

    @property
    def ids(self) -> set[int]:
        """
        Copy distinct source IDs represented in the field's bound topology.

        Example:
            A source table row without links does not appear in this relation ID set.


        :return: Set of source IDs with cached relation groups; unlinked source rows are omitted.
        """

        return set(self._src_ids)

    @property
    def values(self) -> list[Any]:
        """
        Copy all projected values in packed link order across source groups.

        This is not a per-owner dictionary or a unique destination-value list.
        It uses the last-bound flat-value buffer without refreshing.

        Example:
            Two sources linked to the same destination contribute that value twice.


        :return: Flat list of scalar-normalized values, retaining duplicate links and None.
        """

        return self._flattened_values()

    @property
    def values_set(self) -> set[Any]:
        """
        Collect distinct values from the field's bound flat projection.

        Example:
            Several links projecting the same string contribute one set entry.


        :return: Set of values using Python hash/equality, including None when present.
        :raises TypeError: A projected value is unhashable.
        """

        return set(self.values)

    @property
    def ids_values_map(self) -> dict[int, Optional[Any]]:
        """
        Map bound source IDs to their first projected value.

        Only sources present in _src_ids are included. Read bound flat-value
        slices without reloading destination data or filtering link types.

        Example:
            Unlinked owners are omitted rather than inserted with a None value.


        :return: New dict of source ID to first value, or None for an empty group.
        """

        return {src_id: (values[0] if values else None) for src_id, values in (
            (src_id, self._cached_values_for_src(src_id))
            for src_id in self._src_ids
        )}

    @property
    def dst_ids_values_map(self) -> dict[int, Optional[Any]]:
        """
        Map topology-linked destination IDs to current cached endpoint values.

        Keys come from bound reverse topology; values use current endpoint-table
        lookups. Unlinked destination rows are excluded.

        Example:
            A dangling destination retained in topology appears with value None.


        :return: New dict with None for destination IDs missing from the bound destination table.
        """

        return self._dst_ids_values_map()

    def get_value_from_src_id(self, src_id: int) -> Optional[Any]:
        """
        Read the first bound projected value for a source without a fresh link lookup.

        This path does not enforce singularity or search later values for a
        non-null result. It does not refresh stale topology or destination buffers.

        Example:
            A malformed group projecting (None, "later") returns None.


        :param src_id: Source ID converted with int.
        :return: First projected value, or None for no bound group or a None first value.
        """

        values = self._cached_values_for_src(int(src_id))
        return values[0] if values else None

    def get_value_from_dst_id(self, dst_id: int) -> Optional[Any]:
        """
        Read the projected column directly from a cached destination row.

        This path does not restrict the ID to the field's bound topology. A
        missing projected column on an existing row can still raise.

        Example:
            A destination with no source links can still be read by its ID here.


        :param dst_id: Destination ID converted with int.
        :return: Destination value or None for an absent endpoint, without requiring a link.
        """

        return self._value_for_dst_id(int(dst_id))

    def get_dst_id_from_src_id(
        self,
        src_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[int]:
        """
        Select one destination from bound topology or a filtered link lookup.

        A filtered call consults current link records. Neither path enforces
        singularity beyond selecting the first ID.

        Example:
            A non-None type filter on an untyped link table returns no destination.


        :param src_id: Source ID converted with int.
        :param type_filter: Optional exact type filter; None reads the bound topology.
        :return: First destination ID in the selected path, or None for no accepted link.
        """

        if type_filter is not None:
            dst_ids = self._ordered_dst_ids_for_src(int(src_id), type_filter=type_filter)
        else:
            dst_ids = self._cached_dst_ids_for_src(int(src_id))
        return dst_ids[0] if dst_ids else None

    def get_src_ids_from_dst_id(
        self,
        dst_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[int]:
        """
        Read reverse source IDs from topology or a requested ordered/filtered traversal.

        With false ordering and no filter, return the bound reverse tuple. Other
        calls delegate to link getters; ordering and freshness can differ from the
        bound topology path.

        Example:
            Setting require_ordering=True changes the lookup path even when no
            link-type filter is supplied.


        :param dst_id: Destination ID converted with int.
        :param require_ordering: True selects the current link-table traversal path.
        :param type_filter: Non-None selects filtered current link-table traversal.
        :return: Tuple of source IDs, retaining duplicates in the selected path's order.
        """

        if require_ordering or type_filter is not None:
            return self._ordered_src_ids_for_dst(
                int(dst_id),
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
        return tuple(self._dst_to_src_ids.get(int(dst_id), ()))

    def get_src_ids_from_value(self, value: Any) -> list[int]:
        """
        Find source IDs linked to exact destination-column value matches.

        Restrict destination matches to the bound reverse topology, expand its
        source tuples and sort. No Unicode normalization or deduplication is added.

        Example:
            A source linked to two matching destinations appears twice.


        :param value: Hashable value passed unchanged to the destination table index.
        :return: Sorted source-ID list, retaining repetitions across matching links/destinations.
        """

        return self._get_src_ids_from_value(value)

    def get_dst_ids_from_value(self, value: Any) -> list[int]:
        """
        Find linked destination IDs whose cached column value matches exactly.

        Unlinked value matches are excluded; Python index hash/equality determines
        matching rather than the facade's normalized text rules.

        Example:
            An unlinked tag with the same name is excluded from this field lookup.


        :param value: Hashable value passed unchanged to the destination table index.
        :return: Sorted list of matching destination IDs present in bound reverse topology.
        """

        return self._get_dst_ids_from_value(value)

    def get_link_properties(
        self,
        src_id: int,
        dst_id: int,
    ) -> ManyOneIndividualLinkProperties:
        """
        Build many-to-one link metadata from a unique cached pair Row.

        Fetch a singular pair Row without a type filter. Unsupported/absent
        properties become None; casts do not coerce property values.

        Example:
            Two typed physical links for one pair can make this metadata lookup
            ambiguous even though filtered link traversal is available elsewhere.


        :param src_id: Source identity converted with int.
        :param dst_id: Destination identity converted with int.
        :return: ManyOneIndividualLinkProperties value populated with endpoints and supported logical properties.
        :raises KeyError: The pair has no cached Row.
        :raises RuntimeError: The pair has multiple cached link records.
        """

        return cast(
            ManyOneIndividualLinkProperties,
            self._build_link_properties(ManyOneIndividualLinkProperties, int(src_id), int(dst_id)),
        )

    def set_link_properties(
        self,
        updated_link_properties: ManyOneIndividualLinkProperties,
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
        :return: None; updates destination values/links and conditionally rebinds projection state.
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


class NumpyVectorizedOneManyField(
    _NumpyVectorizedRelationFieldBase,
    OneToManyFieldAPI[Any],
):
    """
    Project destination values for a one-to-many relation through bound topology.

    The inherited API resolves endpoint tables and the required directed link
    type; read binds topology and value buffers afterward. Unfiltered cached
    reads use those bound buffers, while selected filtered/reverse paths consult
    the link table. External writes require coordinated cache refresh.
    Source value lookups return sequences and retain repeated links.

    Example:
        >>> issubclass(NumpyVectorizedOneManyField, OneToManyFieldAPI)
        True
    """

    def __init__(
        self,
        cache: "NumpyVectorizedStorageCache",
        src_table: Union[StorageCacheSingleTableAPI, str],
        src_table_id_col: str,
        dst_table: Union[StorageCacheSingleTableAPI, str],
        dst_table_cache_col: str,
        db: Any,
    ) -> None:
        """
        Resolve one-to-many endpoint/link objects and initialize empty projection state.

        Initialize common empty buffers first, then invoke the specific relation
        API constructor. Table/link resolution and type-check failures propagate.
        Call read after endpoint and link data have been initialized.

        Example:
            Construction establishes the field route; root initialize_fields later
            binds the loaded link topology and destination values.


        :param cache: Owning NumPy storage cache for endpoint/link resolution.
        :param src_table: Source table name or table API reference.
        :param src_table_id_col: Source identity column retained by the relation API.
        :param dst_table: Destination table name or table API reference.
        :param dst_table_cache_col: Destination column whose values are projected.
        :param db: Database reference retained for later binding and writes.
        :return: None; attaches the relation without reading its topology/value buffers.
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
    ) -> NumpyVectorizedLinkTable:
        """
        Resolve the directed one-to-many link table through the owning cache.

        Delegate to root.get_one_many_link_table. The cast adds no new runtime
        validation; unknown tables/routes or mismatched cardinality propagate as errors.

        Example:
            An explicit cardinality mismatch is rejected instead of silently using
            a different relation shape.


        :param src_table: Source table name or table API reference.
        :param dst_table: Destination table name or table API reference.
        :return: Bound NumPy link-table object after root freshness and cardinality checks.
        """

        return cast(NumpyVectorizedLinkTable, self._cache.get_one_many_link_table(src_table, dst_table))

    @property
    def ids(self) -> set[int]:
        """
        Copy distinct source IDs represented in the field's bound topology.

        Example:
            A source table row without links does not appear in this relation ID set.


        :return: Set of source IDs with cached relation groups; unlinked source rows are omitted.
        """

        return set(self._src_ids)

    @property
    def values(self) -> list[Any]:
        """
        Copy all projected values in packed link order across source groups.

        This is not a per-owner dictionary or a unique destination-value list.
        It uses the last-bound flat-value buffer without refreshing.

        Example:
            Two sources linked to the same destination contribute that value twice.


        :return: Flat list of scalar-normalized values, retaining duplicate links and None.
        """

        return self._flattened_values()

    @property
    def values_set(self) -> set[Any]:
        """
        Collect distinct values from the field's bound flat projection.

        Example:
            Several links projecting the same string contribute one set entry.


        :return: Set of values using Python hash/equality, including None when present.
        :raises TypeError: A projected value is unhashable.
        """

        return set(self.values)

    @property
    def ids_values_map(self) -> dict[int, Sequence[Optional[Any]]]:
        """
        Map bound source IDs to their projected value tuples.

        Only sources present in _src_ids are included. Read bound flat-value
        slices without reloading destination data or filtering link types.

        Example:
            Unlinked owners are omitted rather than inserted with an empty tuple.


        :return: New dict of source ID to value tuple, retaining duplicate links and None.
        """

        return {
            src_id: self._cached_values_for_src(src_id)
            for src_id in self._src_ids
        }

    @property
    def dst_ids_values_map(self) -> dict[int, Optional[Any]]:
        """
        Map topology-linked destination IDs to current cached endpoint values.

        Keys come from bound reverse topology; values use current endpoint-table
        lookups. Unlinked destination rows are excluded.

        Example:
            A dangling destination retained in topology appears with value None.


        :return: New dict with None for destination IDs missing from the bound destination table.
        """

        return self._dst_ids_values_map()

    def get_values_from_src_id(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Optional[Any]]:
        """
        Read source values from bound projection or an explicitly filtered link traversal.

        Without a type filter, ignore require_ordering and preserve bound topology
        order. With a filter, use current link selection and endpoint values; freshness
        can therefore differ if the field has not been rebound after table changes.

        Example:
            Requesting require_ordering=True alone still reads the bound unfiltered
            value tuple rather than rebuilding or sorting it.


        :param src_id: Source ID converted with int.
        :param require_ordering: Ordering hint forwarded only when a non-None type filter selects traversal.
        :param type_filter: Optional exact type filter; None selects the bound value-buffer path.
        :return: Tuple of projected values, retaining duplicates and None for missing destinations.
        """

        if type_filter is not None:
            return self._values_for_src_id(
                int(src_id),
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
        return self._cached_values_for_src(int(src_id))

    def get_value_from_dst_id(self, dst_id: int) -> Optional[Any]:
        """
        Read the projected column directly from a cached destination row.

        This path does not restrict the ID to the field's bound topology. A
        missing projected column on an existing row can still raise.

        Example:
            A destination with no source links can still be read by its ID here.


        :param dst_id: Destination ID converted with int.
        :return: Destination value or None for an absent endpoint, without requiring a link.
        """

        return self._value_for_dst_id(int(dst_id))

    def get_dst_ids_from_src_id(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[int]:
        """
        Read destination IDs from bound topology or a filtered link traversal.

        A non-None filter resolves through current link indexes. Unfiltered calls
        ignore the ordering hint and do not validate destination row existence.

        Example:
            Dangling destination IDs remain in unfiltered topology results until
            link state is rebuilt; their value projection can be None.


        :param src_id: Source ID converted with int.
        :param require_ordering: Ordering hint forwarded only on the filtered traversal path.
        :param type_filter: Optional exact type filter; None selects cached topology.
        :return: Tuple of integer destination IDs in the selected path's order, retaining duplicates.
        """

        if type_filter is not None:
            return self._ordered_dst_ids_for_src(
                int(src_id),
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
        return self._cached_dst_ids_for_src(int(src_id))

    def get_src_id_from_dst_id(
        self,
        dst_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[int]:
        """
        Select one source from bound reverse topology or a filtered link lookup.

        No additional uniqueness check is performed even if malformed data has
        several sources for a supposedly exclusive destination.

        Example:
            A filtered lookup can observe current link records while an unfiltered
            lookup remains tied to previously bound reverse topology.


        :param dst_id: Destination ID converted with int.
        :param type_filter: Optional exact type filter; None reads bound reverse topology.
        :return: First source ID in the selected path, or None when none is accepted.
        """

        if type_filter is not None:
            src_ids = self._ordered_src_ids_for_dst(int(dst_id), type_filter=type_filter)
        else:
            src_ids = tuple(self._dst_to_src_ids.get(int(dst_id), ()))
        return src_ids[0] if src_ids else None

    def get_src_ids_from_value(self, value: Any) -> list[int]:
        """
        Find source IDs linked to exact destination-column value matches.

        Restrict destination matches to the bound reverse topology, expand its
        source tuples and sort. No Unicode normalization or deduplication is added.

        Example:
            A source linked to two matching destinations appears twice.


        :param value: Hashable value passed unchanged to the destination table index.
        :return: Sorted source-ID list, retaining repetitions across matching links/destinations.
        """

        return self._get_src_ids_from_value(value)

    def get_dst_ids_from_value(self, value: Any) -> list[int]:
        """
        Find linked destination IDs whose cached column value matches exactly.

        Unlinked value matches are excluded; Python index hash/equality determines
        matching rather than the facade's normalized text rules.

        Example:
            An unlinked tag with the same name is excluded from this field lookup.


        :param value: Hashable value passed unchanged to the destination table index.
        :return: Sorted list of matching destination IDs present in bound reverse topology.
        """

        return self._get_dst_ids_from_value(value)

    def get_link_properties(
        self,
        src_id: int,
        dst_id: int,
    ) -> OneManyIndividualLinkProperties:
        """
        Build one-to-many link metadata from a unique cached pair Row.

        Fetch a singular pair Row without a type filter. Unsupported/absent
        properties become None; casts do not coerce property values.

        Example:
            Two typed physical links for one pair can make this metadata lookup
            ambiguous even though filtered link traversal is available elsewhere.


        :param src_id: Source identity converted with int.
        :param dst_id: Destination identity converted with int.
        :return: OneManyIndividualLinkProperties value populated with endpoints and supported logical properties.
        :raises KeyError: The pair has no cached Row.
        :raises RuntimeError: The pair has multiple cached link records.
        """

        return cast(
            OneManyIndividualLinkProperties,
            self._build_link_properties(OneManyIndividualLinkProperties, int(src_id), int(dst_id)),
        )

    def set_link_properties(
        self,
        updated_link_properties: OneManyIndividualLinkProperties,
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
        :return: None; writes requested changes and conditionally rebinds relation projection.
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


class NumpyVectorizedManyManyField(
    _NumpyVectorizedRelationFieldBase,
    ManyToManyFieldAPI[Any],
):
    """
    Project destination values for a many-to-many relation through bound topology.

    The inherited API resolves endpoint tables and the required directed link
    type; read binds topology and value buffers afterward. Unfiltered cached
    reads use those bound buffers, while selected filtered/reverse paths consult
    the link table. External writes require coordinated cache refresh.
    Source value lookups return sequences and retain repeated links.

    Example:
        >>> issubclass(NumpyVectorizedManyManyField, ManyToManyFieldAPI)
        True
    """

    def __init__(
        self,
        cache: "NumpyVectorizedStorageCache",
        src_table: Union[StorageCacheSingleTableAPI, str],
        src_table_id_col: str,
        dst_table: Union[StorageCacheSingleTableAPI, str],
        dst_table_cache_col: str,
        db: Any,
    ) -> None:
        """
        Resolve many-to-many endpoint/link objects and initialize empty projection state.

        Initialize common empty buffers first, then invoke the specific relation
        API constructor. Table/link resolution and type-check failures propagate.
        Call read after endpoint and link data have been initialized.

        Example:
            Construction establishes the field route; root initialize_fields later
            binds the loaded link topology and destination values.


        :param cache: Owning NumPy storage cache for endpoint/link resolution.
        :param src_table: Source table name or table API reference.
        :param src_table_id_col: Source identity column retained by the relation API.
        :param dst_table: Destination table name or table API reference.
        :param dst_table_cache_col: Destination column whose values are projected.
        :param db: Database reference retained for later binding and writes.
        :return: None; attaches the relation without reading its topology/value buffers.
        """

        self._init_relation_field(cache, db)
        ManyToManyFieldAPI.__init__(
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
    ) -> NumpyVectorizedLinkTable:
        """
        Resolve the directed many-to-many link table through the owning cache.

        Delegate to root.get_many_many_link_table. The cast adds no new runtime
        validation; unknown tables/routes or mismatched cardinality propagate as errors.

        Example:
            An explicit cardinality mismatch is rejected instead of silently using
            a different relation shape.


        :param src_table: Source table name or table API reference.
        :param dst_table: Destination table name or table API reference.
        :return: Bound NumPy link-table object after root freshness and cardinality checks.
        """

        return cast(NumpyVectorizedLinkTable, self._cache.get_many_many_link_table(src_table, dst_table))

    @property
    def ids(self) -> set[int]:
        """
        Copy distinct source IDs represented in the field's bound topology.

        Example:
            A source table row without links does not appear in this relation ID set.


        :return: Set of source IDs with cached relation groups; unlinked source rows are omitted.
        """

        return set(self._src_ids)

    @property
    def values(self) -> list[Any]:
        """
        Copy all projected values in packed link order across source groups.

        This is not a per-owner dictionary or a unique destination-value list.
        It uses the last-bound flat-value buffer without refreshing.

        Example:
            Two sources linked to the same destination contribute that value twice.


        :return: Flat list of scalar-normalized values, retaining duplicate links and None.
        """

        return self._flattened_values()

    @property
    def values_set(self) -> set[Any]:
        """
        Collect distinct values from the field's bound flat projection.

        Example:
            Several links projecting the same string contribute one set entry.


        :return: Set of values using Python hash/equality, including None when present.
        :raises TypeError: A projected value is unhashable.
        """

        return set(self.values)

    @property
    def ids_values_map(self) -> dict[int, Sequence[Optional[Any]]]:
        """
        Map bound source IDs to their projected value tuples.

        Only sources present in _src_ids are included. Read bound flat-value
        slices without reloading destination data or filtering link types.

        Example:
            Unlinked owners are omitted rather than inserted with an empty tuple.


        :return: New dict of source ID to value tuple, retaining duplicate links and None.
        """

        return {
            src_id: self._cached_values_for_src(src_id)
            for src_id in self._src_ids
        }

    @property
    def dst_ids_values_map(self) -> dict[int, Optional[Any]]:
        """
        Map topology-linked destination IDs to current cached endpoint values.

        Keys come from bound reverse topology; values use current endpoint-table
        lookups. Unlinked destination rows are excluded.

        Example:
            A dangling destination retained in topology appears with value None.


        :return: New dict with None for destination IDs missing from the bound destination table.
        """

        return self._dst_ids_values_map()

    def get_values_from_src_id(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Optional[Any]]:
        """
        Read source values from bound projection or an explicitly filtered link traversal.

        Without a type filter, ignore require_ordering and preserve bound topology
        order. With a filter, use current link selection and endpoint values; freshness
        can therefore differ if the field has not been rebound after table changes.

        Example:
            Requesting require_ordering=True alone still reads the bound unfiltered
            value tuple rather than rebuilding or sorting it.


        :param src_id: Source ID converted with int.
        :param require_ordering: Ordering hint forwarded only when a non-None type filter selects traversal.
        :param type_filter: Optional exact type filter; None selects the bound value-buffer path.
        :return: Tuple of projected values, retaining duplicates and None for missing destinations.
        """

        if type_filter is not None:
            return self._values_for_src_id(
                int(src_id),
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
        return self._cached_values_for_src(int(src_id))

    def get_value_from_dst_id(self, dst_id: int) -> Optional[Any]:
        """
        Read the projected column directly from a cached destination row.

        This path does not restrict the ID to the field's bound topology. A
        missing projected column on an existing row can still raise.

        Example:
            A destination with no source links can still be read by its ID here.


        :param dst_id: Destination ID converted with int.
        :return: Destination value or None for an absent endpoint, without requiring a link.
        """

        return self._value_for_dst_id(int(dst_id))

    def get_dst_ids_from_src_id(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[int]:
        """
        Read destination IDs from bound topology or a filtered link traversal.

        A non-None filter resolves through current link indexes. Unfiltered calls
        ignore the ordering hint and do not validate destination row existence.

        Example:
            Dangling destination IDs remain in unfiltered topology results until
            link state is rebuilt; their value projection can be None.


        :param src_id: Source ID converted with int.
        :param require_ordering: Ordering hint forwarded only on the filtered traversal path.
        :param type_filter: Optional exact type filter; None selects cached topology.
        :return: Tuple of integer destination IDs in the selected path's order, retaining duplicates.
        """

        if type_filter is not None:
            return self._ordered_dst_ids_for_src(
                int(src_id),
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
        return self._cached_dst_ids_for_src(int(src_id))

    def get_src_ids_from_dst_id(
        self,
        dst_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[int]:
        """
        Read reverse source IDs from topology or a requested ordered/filtered traversal.

        With false ordering and no filter, return the bound reverse tuple. Other
        calls delegate to link getters; ordering and freshness can differ from the
        bound topology path.

        Example:
            Setting require_ordering=True changes the lookup path even when no
            link-type filter is supplied.


        :param dst_id: Destination ID converted with int.
        :param require_ordering: True selects the current link-table traversal path.
        :param type_filter: Non-None selects filtered current link-table traversal.
        :return: Tuple of source IDs, retaining duplicates in the selected path's order.
        """

        if require_ordering or type_filter is not None:
            return self._ordered_src_ids_for_dst(
                int(dst_id),
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
        return tuple(self._dst_to_src_ids.get(int(dst_id), ()))

    def get_src_ids_from_value(self, value: Any) -> list[int]:
        """
        Find source IDs linked to exact destination-column value matches.

        Restrict destination matches to the bound reverse topology, expand its
        source tuples and sort. No Unicode normalization or deduplication is added.

        Example:
            A source linked to two matching destinations appears twice.


        :param value: Hashable value passed unchanged to the destination table index.
        :return: Sorted source-ID list, retaining repetitions across matching links/destinations.
        """

        return self._get_src_ids_from_value(value)

    def get_dst_ids_from_value(self, value: Any) -> list[int]:
        """
        Find linked destination IDs whose cached column value matches exactly.

        Unlinked value matches are excluded; Python index hash/equality determines
        matching rather than the facade's normalized text rules.

        Example:
            An unlinked tag with the same name is excluded from this field lookup.


        :param value: Hashable value passed unchanged to the destination table index.
        :return: Sorted list of matching destination IDs present in bound reverse topology.
        """

        return self._get_dst_ids_from_value(value)

    def get_link_properties(
        self,
        src_id: int,
        dst_id: int,
    ) -> ManyManyIndividualLinkProperties:
        """
        Build many-to-many link metadata from a unique cached pair Row.

        Fetch a singular pair Row without a type filter. Unsupported/absent
        properties become None; casts do not coerce property values.

        Example:
            Two typed physical links for one pair can make this metadata lookup
            ambiguous even though filtered link traversal is available elsewhere.


        :param src_id: Source identity converted with int.
        :param dst_id: Destination identity converted with int.
        :return: ManyManyIndividualLinkProperties value populated with endpoints and supported logical properties.
        :raises KeyError: The pair has no cached Row.
        :raises RuntimeError: The pair has multiple cached link records.
        """

        return cast(
            ManyManyIndividualLinkProperties,
            self._build_link_properties(ManyManyIndividualLinkProperties, int(src_id), int(dst_id)),
        )

    def set_link_properties(
        self,
        updated_link_properties: ManyManyIndividualLinkProperties,
    ) -> None:
        """
        Write supported non-None many-to-many link properties and rebind the field.

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

    def update(self, update: ManyManyInTwoTableFieldUpdate[Any]) -> None:
        """
        Apply sequence-value changes, source unlinking and explicit link replacements.

        Require a database, merge added/updated maps and materialize value and
        replacement sequences. Reject value-update/replacement overlap first, then
        delete/replacement overlap. Check existing link-sequence lengths before any
        unlinking. Value updates may still overlap deletions: destinations are resolved
        again after unlinking, so those deleted sources can contribute no value writes.

        Unlink deleted sources, gather/write destination values in current link order,
        then apply explicit replacements per source with shared destinations allowed.
        Several sources targeting one destination keep the last collected value.
        Replacement resolution can create rows before later errors. No encompassing
        transaction or rollback is added. Rebind after requested changes or truthy
        dirtied; failures can leave durable changes with stale field projection.

        Example:
            An explicit replacement sequence changes both link membership/order and
            its supplied destination values; a plain value sequence requires matching
            existing link count.


        :param update: Relation update with added/updated value sequences, replacements, deleted IDs and dirtied.
        :return: None; writes requested changes and conditionally rebinds relation projection.
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
                allow_shared_dst=True,
            )

        if deleted_src_ids or dst_updates or explicit_replacements or update.dirtied:
            self.read(self._db)


class NumpyVectorizedStorageCache(StorageCacheAPI):
    """
    Own snapshot arrays, directed links and field projections independently of schema-backed storage.

    Construct unloaded state; read performs schema discovery, table reads, field
    construction and projection binding. Direct accessors refresh declared stale
    dependencies but do not detect external changes automatically. Held child
    objects are snapshots and may be replaced by a full rebuild.

    NumPy is required by default. require_numpy=False permits tuple fallback
    when import failed; capabilities then disables vectorized_helpers while
    retaining snapshot semantics. The root adds neither application write
    reconciliation nor database transaction ownership; use Cache for that boundary.

    Example:
        >>> cache = NumpyVectorizedStorageCache(None, require_numpy=False)
        >>> cache.is_loaded, cache.is_initialized
        (False, False)
    """

    plugin_name = "numpy_vectorized"
    plugin_capabilities = StorageCacheCapabilities(
        live_reads=False,
        live_child_objects=False,
        vectorized_helpers=True,
        requires_reload_for_external_changes=True,
    )

    def __init__(self, db: Any, *, require_numpy: bool = True) -> None:
        """
        Check NumPy requirements and allocate empty root cache state.

        The flag does not force tuple mode when NumPy is available. Validate the
        NumPy requirement before creating registries or initializing the base cache.

        Example:
            Use require_numpy=False to construct the optional backend in an
            environment whose NumPy import failed.


        :param db: Database reference retained by StorageCacheAPI; None permits detached construction.
        :param require_numpy: True rejects unavailable NumPy; false allows tuple-backed operation.
        :return: None; creates empty registries and stale sets without loading data.
        :raises RuntimeError: NumPy is unavailable and require_numpy is truthy.
        """

        if require_numpy and _np is None:
            raise RuntimeError(
                "The numpy_vectorized cache plugin requires numpy to be installed"
            )
        self._require_numpy = require_numpy
        self.main_tables: dict[str, NumpyVectorizedMainTableCache] = {}
        self.link_tables: dict[tuple[str, str], NumpyVectorizedLinkTable] = {}
        self.fields: dict[str, FieldBasicInterfaceAPI[Any]] = {}
        self._field_objects: dict[str, FieldBasicInterfaceAPI[Any]] = {}
        self._schema: Optional["StorageSchemaSpec"] = None
        self._is_loaded = False
        self._is_initialized = False
        self._stale_main_tables: set[str] = set()
        self._stale_link_tables: set[tuple[str, str]] = set()
        self._stale_fields: set[str] = set()
        self._stale_ids: dict[str, set[int]] = defaultdict(set)
        super().__init__(db)

    @classmethod
    def numpy_available(cls) -> bool:
        """
        Report whether this module's optional NumPy import succeeded.

        This does not retry import or probe package installation again.

        Example:
            >>> isinstance(NumpyVectorizedStorageCache.numpy_available(), bool)
            True


        :return: True when the module-level _np reference is non-None.
        """

        return _np is not None

    @property
    def capabilities(self) -> StorageCacheCapabilities:
        """
        Describe snapshot semantics with vectorized support adjusted for NumPy availability.

        Fallback keeps live_reads/live_child_objects false and requires external
        reload, while disabling vectorized_helpers. It does not depend on the
        constructor flag once import availability is known.

        Example:
            >>> cache = NumpyVectorizedStorageCache(None, require_numpy=False)
            >>> cache.capabilities.vectorized_helpers == cache.numpy_available()
            True


        :return: Plugin capabilities when NumPy is available, otherwise a new fallback capabilities value.
        """

        if _np is None:
            return StorageCacheCapabilities(
                live_reads=False,
                live_child_objects=False,
                vectorized_helpers=False,
                requires_reload_for_external_changes=True,
            )
        return self.plugin_capabilities

    @property
    def is_loaded(self) -> bool:
        """
        Report the root cache's recorded load flag.

        A complete read sets both flags true and clear resets them. Detaching
        the database alone does not reset these flags.

        Example:
            A newly constructed root returns False before its first complete read.


        :return: Stored boolean flag, without attachment validation or refresh.
        """

        return self._is_loaded

    @property
    def is_initialized(self) -> bool:
        """
        Report the root cache's recorded initialization flag.

        A complete read sets both flags true and clear resets them. Detaching
        the database alone does not reset these flags.

        Example:
            A newly constructed root returns False before its first complete read.


        :return: Stored boolean flag, without attachment validation or refresh.
        """

        return self._is_initialized

    def _require_db(self, db: Any = None) -> Any:
        """
        Resolve an explicit/current database and retain it on the root cache.

        Example:
            Passing an explicit database changes the root attachment even before
            the caller begins reading tables.


        :param db: Database override used when non-None; otherwise retain the current attachment.
        :return: Selected database object, also stored as self.db.
        :raises RuntimeError: Neither an explicit nor current database is available.
        """

        resolved = _ensure_db(self.db, db)
        self.db = resolved
        return resolved

    def clear(self) -> None:
        """
        Discard root table/field registries, schema, flags and stale dependencies.

        Replace public/canonical dictionaries and clear all stale sets, including
        bounded IDs. Previously returned child objects are not walked or cleared and
        can retain their old snapshots and database references.

        Example:
            After clear, get_main_table cannot find earlier tables until they are
            rediscovered, while an externally held old table object still exists.


        :return: None; leaves the root unloaded while retaining its database reference.
        """

        self.main_tables = {}
        self.link_tables = {}
        self.fields = {}
        self._field_objects = {}
        self._schema = None
        self._is_loaded = False
        self._is_initialized = False
        self._stale_main_tables.clear()
        self._stale_link_tables.clear()
        self._stale_fields.clear()
        self._stale_ids.clear()

    def detach_db(self) -> Optional[Any]:
        """
        Remove database references from the root and its currently registered children.

        Set table.db and existing field._db attributes to None without closing the
        database. Keep loaded/initialized flags, cached data and stale markers.
        Unregistered old child objects are not visited.

        Example:
            A detached root can retain is_initialized=True, but a requested stale
            refresh then requires an explicit database.


        :return: Previously attached database, or None when already detached.
        """

        old_db = self.db
        self.db = None
        for table in self.main_tables.values():
            table.db = None
        for table in self.link_tables.values():
            table.db = None
        for field in self._field_objects.values():
            if hasattr(field, "_db"):
                field._db = None
        return old_db

    def close(self) -> None:
        """
        Clear root cache state and detach its database without closing the database object.

        Clear registries before detach_db, so that call no longer walks old child
        objects. This backend has no terminal CLOSED state of its own.

        Example:
            Save the database separately when it is needed to reattach after close.


        :return: None; leaves an unloaded, detached cache that can be read with a supplied database.
        """

        self.clear()
        self.detach_db()

    def read(self, db: Any = None) -> None:
        """
        Rebuild the entire schema, table data and field projections against the selected database.

        Require/retain the database, clear previous state, discover tables, read
        main then link tables, construct fields, then bind every field. No read
        transaction or rollback is added; failures can leave partial new registries
        with flags still false and prior held objects unchanged.

        Example:
            Use read(database) to attach and initialize a newly constructed backend.


        :param db: Optional database override; None reuses the current attachment.
        :return: None; sets loaded and initialized true only after every phase succeeds.
        :raises RuntimeError: No explicit or attached database is available.
        """

        self._require_db(db)
        self.clear()
        self.read_tables(self.db)
        self.initialize_tables(self.db)
        self.read_fields(self.db)
        self.initialize_fields(self.db)
        self._is_loaded = True
        self._is_initialized = True

    def reload(self, db: Any = None) -> None:
        """
        Delegate a full schema/data/projection rebuild to read.

        Example:
            A full reload replaces root child registries rather than promising
            that previously held table/field objects remain current.


        :param db: Optional database override; None retains the current attachment.
        :return: None; same lifecycle and failure behavior as read.
        """

        self.read(db=db)

    def read_tables(self, db: Any = None) -> None:
        """
        Force schema discovery and construct unloaded main and directed link tables.

        Call get_schema_spec(force_refresh=True). Include every spec flagged as a
        main table without an additional ID-column check. For interlinks and intralinks,
        skip routes whose endpoints are not included. Build a forward view and, for
        different endpoint names, a reverse view with swapped tables and columns.
        Both retain the same link specification, including its declared cardinality;
        this method does not invert that declaration. Duplicate directed keys overwrite
        earlier views. No table rows or field projections are loaded here.

        Example:
            A self-link receives one directed view; a link between distinct tables
            receives separate forward and reverse objects.


        :param db: Database override; None uses the current root attachment.
        :return: None; publishes the schema and new main/link table registries.
        """

        db = self._require_db(db)
        schema = db.driver_wrapper.get_schema_spec(force_refresh=True)
        self._schema = schema
        self.main_tables = {
            table_name: NumpyVectorizedMainTableCache(spec, db)
            for table_name, spec in schema.tables.items()
            if spec.is_main_table
        }

        link_tables: dict[tuple[str, str], NumpyVectorizedLinkTable] = {}
        for link_spec in schema.interlinks + schema.intralinks:
            src_table = self.main_tables.get(link_spec.primary_table)
            dst_table = self.main_tables.get(link_spec.secondary_table)
            if src_table is None or dst_table is None:
                continue

            forward = NumpyVectorizedLinkTable(
                db=db,
                link_spec=link_spec,
                src_table=src_table,
                dst_table=dst_table,
                src_table_name=link_spec.primary_table,
                dst_table_name=link_spec.secondary_table,
                src_link_col=link_spec.primary_link_col,
                dst_link_col=link_spec.secondary_link_col,
            )
            link_tables[(link_spec.primary_table, link_spec.secondary_table)] = forward

            if link_spec.primary_table != link_spec.secondary_table:
                reverse = NumpyVectorizedLinkTable(
                    db=db,
                    link_spec=link_spec,
                    src_table=dst_table,
                    dst_table=src_table,
                    src_table_name=link_spec.secondary_table,
                    dst_table_name=link_spec.primary_table,
                    src_link_col=link_spec.secondary_link_col,
                    dst_link_col=link_spec.primary_link_col,
                )
                link_tables[(link_spec.secondary_table, link_spec.primary_table)] = reverse

        self.link_tables = link_tables

    def initialize_tables(self, db: Any = None) -> None:
        """
        Read every registered main table before reading directed link tables.

        Use registry insertion order within each phase. Earlier tables remain
        loaded if a later read fails. Link views can read the same physical table
        separately for its two orientations.

        Example:
            Destination row positions are available before link-table topology
            construction because main-table reads run first.


        :param db: Database override retained on the root and passed to table readers.
        :return: None; populates table arrays and link topology without setting root completion flags.
        """

        db = self._require_db(db)
        for table in self.main_tables.values():
            table.read(db)
        for table in self.link_tables.values():
            table.read(db)

    def read_fields(self, db: Any = None) -> None:
        """
        Construct canonical scalar/relation fields and unambiguous scalar aliases.

        Create a scalar field for every main-table column. In sorted directed-link
        order, project every destination column except its ID through the link
        cardinality's field class, defaulting to many-to-many for other types.

        Public fields starts with canonical keys. Add a bare alias only for scalar
        columns whose name occurs once among main tables; aliases share field object
        identity. Relation columns are not counted for this alias decision. This
        constructs fields but does not bind their data buffers; initialize_fields
        performs that later. Constructor/freshness errors propagate.

        Example:
            A globally unique scalar title column gains alias title as well as
            books.title; duplicate scalar name columns remain qualified.


        :param db: Database override used by field constructors and root attachment.
        :return: None; replaces canonical _field_objects and public fields after construction succeeds.
        """

        db = self._require_db(db)
        field_objects: dict[str, FieldBasicInterfaceAPI[Any]] = {}
        raw_name_counts: dict[str, int] = defaultdict(int)

        for table_name, table in self.main_tables.items():
            for column in table.column_headings:
                field = NumpyVectorizedSameTableField(self, table_name, column, db)
                field_objects[field.field_key] = field
                raw_name_counts[column] += 1

        for (src_table_name, dst_table_name), link_table in sorted(self.link_tables.items()):
            dst_table = self.main_tables[dst_table_name]
            dst_columns = [
                column
                for column in dst_table.column_headings
                if column != dst_table.id_column
            ]
            for column in dst_columns:
                if link_table.table_type == TableTypes.ONE_ONE:
                    field: FieldBasicInterfaceAPI[Any] = NumpyVectorizedTwoTableOneOneField(
                        self,
                        src_table_name,
                        self.main_tables[src_table_name].id_column,
                        dst_table_name,
                        column,
                        db,
                    )
                elif link_table.table_type == TableTypes.ONE_MANY:
                    field = NumpyVectorizedOneManyField(
                        self,
                        src_table_name,
                        self.main_tables[src_table_name].id_column,
                        dst_table_name,
                        column,
                        db,
                    )
                elif link_table.table_type == TableTypes.MANY_ONE:
                    field = NumpyVectorizedManyOneField(
                        self,
                        src_table_name,
                        self.main_tables[src_table_name].id_column,
                        dst_table_name,
                        column,
                        db,
                    )
                else:
                    field = NumpyVectorizedManyManyField(
                        self,
                        src_table_name,
                        self.main_tables[src_table_name].id_column,
                        dst_table_name,
                        column,
                        db,
                    )
                field_objects[field.field_key] = field

        fields: dict[str, FieldBasicInterfaceAPI[Any]] = dict(field_objects)
        for field in field_objects.values():
            column_name = getattr(field, "column_name", None)
            if (
                isinstance(field, NumpyVectorizedSameTableField)
                and column_name is not None
                and raw_name_counts.get(column_name, 0) == 1
            ):
                fields[str(column_name)] = field

        self._field_objects = field_objects
        self.fields = fields

    def initialize_fields(self, db: Any = None) -> None:
        """
        Bind each canonical field once to its already loaded endpoint/link state.

        Iterate canonical objects rather than alias entries. A later field failure
        leaves earlier bindings in place; underlying table reads are not repeated here.

        Example:
            A scalar alias does not cause the same field to be initialized twice.


        :param db: Database override retained on the root and passed to each field.read.
        :return: None; initializes field projections without setting root completion flags.
        """

        db = self._require_db(db)
        for field in self._field_objects.values():
            field.read(db)

    def _resolve_field_name(self, name: Union[FieldKey, FieldBasicInterfaceAPI[Any]]) -> str:
        """
        Resolve a field reference, existing public key or unique bare column match.

        A field_key attribute is returned without a membership check. Otherwise
        retain existing public keys, including bare aliases, unchanged. A non-dotted
        unknown name may resolve to exactly one canonical object with that column_name.
        This helper does not refresh fields or check ownership.

        Example:
            A registered bare title alias stays title here rather than being
            rewritten to books.title.


        :param name: Field-like object with field_key, or a string-convertible name.
        :return: String key accepted by the selected resolution path.
        :raises KeyError: The name is neither an existing public key nor a unique bare column match.
        """

        if hasattr(name, "field_key"):
            return str(getattr(name, "field_key"))
        requested = str(name)
        if requested in self.fields:
            return requested
        if "." not in requested:
            matches = [
                field.field_key
                for field in self._field_objects.values()
                if getattr(field, "column_name", None) == requested
            ]
            if len(matches) == 1:
                return matches[0]
        raise KeyError(requested)

    def _field_owner_table(self, field: FieldBasicInterfaceAPI[Any]) -> Optional[str]:
        """
        Read a field's first declared owner table name.

        An empty string is a non-None owner and is retained. Attribute access
        errors propagate rather than falling through to the next owner.

        Example:
            A relation field's table_name identifies its source table as owner.


        :param field: Field object inspected for table_name then src_table_name.
        :return: String form of the first non-None owner attribute, otherwise None.
        """

        owner = getattr(field, "table_name", None)
        if owner is not None:
            return str(owner)
        owner = getattr(field, "src_table_name", None)
        if owner is not None:
            return str(owner)
        return None

    def _field_tables(self, field: FieldBasicInterfaceAPI[Any]) -> set[str]:
        """
        Collect nonempty table dependencies exposed by a field's naming attributes.

        Example:
            A relation field depending on books and tags contributes both names
            even though only books owns its source IDs.


        :param field: Field object inspected for table_name, src_table_name and dst_table_name.
        :return: Set of string table names for truthy attributes, with duplicates removed.
        """

        tables: set[str] = set()
        for attr_name in ("table_name", "src_table_name", "dst_table_name"):
            table_name = getattr(field, attr_name, None)
            if table_name:
                tables.add(str(table_name))
        return tables

    def _field_link_key(self, field: FieldBasicInterfaceAPI[Any]) -> Optional[tuple[str, str]]:
        """
        Extract a directed endpoint key when a field declares both endpoint names.

        Unlike _field_tables, empty names are accepted because the check uses
        identity with None rather than truthiness.

        Example:
            A scalar field with no endpoint-name pair has no link dependency key.


        :param field: Field object inspected for src_table_name and dst_table_name.
        :return: Pair of string endpoint names, or None if either attribute is None/missing.
        """

        src_table_name = getattr(field, "src_table_name", None)
        dst_table_name = getattr(field, "dst_table_name", None)
        if src_table_name is None or dst_table_name is None:
            return None
        return (str(src_table_name), str(dst_table_name))

    def _ensure_main_table_fresh(self, table_name: str) -> None:
        """
        Reload a whole main table when it has table-wide or bounded stale markers.

        Bounded IDs still trigger reload_main_table for this backend; the helper
        does not refresh only those rows.

        Example:
            A nonempty _stale_ids entry triggers the same full-table refresh as
            a table-wide marker.


        :param table_name: Exact main-table key used for stale lookups.
        :return: None; performs no work when neither stale condition is present.
        """

        if table_name in self._stale_main_tables or self._stale_ids.get(table_name):
            self.reload_main_table(table_name)

    def _ensure_link_table_fresh(self, key: tuple[str, str]) -> None:
        """
        Reload a directed link view only when its exact key is marked stale.

        Do not check or implicitly refresh the reverse orientation.

        Example:
            A stale books-to-tags key does not by itself refresh tags-to-books.


        :param key: Directed (source table, destination table) key.
        :return: None; calls reload_link_table for a stale key and otherwise returns.
        """

        if key in self._stale_link_tables:
            self.reload_link_table(*key)

    def _ensure_field_fresh(self, field_name: str) -> None:
        """
        Refresh a field's stale table dependencies, directed link and own binding in order.

        Reload sorted table dependencies intersecting _stale_main_tables, then
        the field's exact stale link key, then reload the field if its supplied name
        is marked stale. Bounded-ID sets are not checked independently here; normal
        ID invalidation also sets the table-wide marker. Earlier refreshes can clear
        markers or rebind fields before a later operation is considered.

        Example:
            Refreshing an invalidated destination table can rebind its relation
            fields before their own stale-field check is reached.


        :param field_name: Public field key used to find the field and its stale marker.
        :return: None; applies the applicable dependency refresh operations.
        """

        field = self.fields[field_name]
        for table_name in sorted(
            self._field_tables(field) & self._stale_main_tables
        ):
            self.reload_main_table(table_name)
        link_key = self._field_link_key(field)
        if link_key is not None and link_key in self._stale_link_tables:
            self.reload_link_table(*link_key)
        if field_name in self._stale_fields:
            self.reload_field(field_name)

    def has_main_table(self, name: str) -> bool:
        """
        Test main-table registry membership without a freshness check.

        Example:
            A stale registered table still returns True without being reloaded.


        :param name: Table name converted with str.
        :return: Whether the current main_tables dictionary contains the key.
        """

        return str(name) in self.main_tables

    def get_main_table(
        self,
        name: Union[str, StorageCacheSingleTableAPI],
    ) -> NumpyVectorizedMainTableCache:
        """
        Resolve a table reference, refresh its stale data and return the cached object.

        Refresh occurs before final dictionary access. This does not force schema
        discovery or replace the object merely because its row data was stale.

        Example:
            Getting a table after ID invalidation rebuilds its entire row arrays.


        :param name: Table API instance whose table attribute is used, or a string-convertible name.
        :return: Registered NumPy main-table cache after any required full-table reload.
        :raises KeyError: The resolved main-table key is unknown.
        """

        table_name = name.table if isinstance(name, StorageCacheSingleTableAPI) else str(name)
        self._ensure_main_table_fresh(table_name)
        return self.main_tables[table_name]

    def iter_main_tables(self) -> Iterable[NumpyVectorizedMainTableCache]:
        """
        Yield registered main tables in sorted name order through freshness-aware lookup.

        Capture sorted names for the loop, then call get_main_table per name.
        This does not wrap the sequence in a database transaction.

        Example:
            An early yielded table can be available even if a later stale-table
            refresh raises during iteration.


        :return: Generator whose table refreshes occur as iteration advances.
        """

        for table_name in sorted(self.main_tables):
            yield self.get_main_table(table_name)

    def get_table(self, name: str):
        """
        Resolve a main table or the first directed view with a matching physical link name.

        Main tables take precedence. Link views are searched in insertion order,
        so a physical name shared by forward/reverse objects selects the first one.
        The input is not generally converted with str before membership checks.

        Example:
            A physical book_tags table name can return one oriented view; use
            get_link_table with endpoints when orientation matters.


        :param name: Exact name checked against main keys then link-table physical names.
        :return: Main/link table cache object after its applicable freshness check.
        :raises KeyError: Neither a main key nor a physical link-table name matches.
        """

        if name in self.main_tables:
            return self.get_main_table(name)
        for key, table in self.link_tables.items():
            if table.table == name:
                self._ensure_link_table_fresh(key)
                return table
        raise KeyError(name)

    def iter_tables(self) -> Iterable[Any]:
        """
        Yield fresh main tables then directed link views, deduplicating object identities.

        Distinct forward/reverse views can both be yielded even when they share
        a physical table name. Deduplication uses id(object), not table names.

        Example:
            One physical association table can contribute two different directed
            cache objects to this iterator.


        :return: Generator ordered by main-table names then directed link keys.
        """

        yielded: set[int] = set()
        for table in self.iter_main_tables():
            yielded.add(id(table))
            yield table
        for key in sorted(self.link_tables):
            table = self.get_link_table(*key)
            if id(table) in yielded:
                continue
            yielded.add(id(table))
            yield table

    def has_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
        table_type: Optional[TableTypes] = None,
    ) -> bool:
        """
        Resolve fresh endpoints and test a directed link route with an optional type filter.

        Unknown endpoint tables raise before route absence can return false.
        Refresh an existing directed link view before checking its type; do not
        retry the reverse route automatically.

        Example:
            An absent route between two known tables returns False; an unknown
            source table is an error.


        :param src_table: Source table name or API object resolved by get_main_table.
        :param dst_table: Destination table name or API object resolved by get_main_table.
        :param table_type: Optional required TableTypes value.
        :return: True for an existing route whose refreshed type matches, otherwise false.
        :raises KeyError: An endpoint table cannot be resolved.
        """

        src_name = self.get_main_table(src_table).table
        dst_name = self.get_main_table(dst_table).table
        table = self.link_tables.get((src_name, dst_name))
        if table is None:
            return False
        self._ensure_link_table_fresh((src_name, dst_name))
        return table_type is None or table.table_type == table_type

    def get_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
        table_type: Optional[TableTypes] = None,
    ) -> NumpyVectorizedLinkTable:
        """
        Return a fresh directed link view after resolving endpoints and validating its type.

        Endpoint table refreshes precede link refresh and type comparison. No
        reverse-route fallback is added.

        Example:
            Requesting ONE_ONE for a MANY_MANY route raises instead of coercing
            the table into a different interface.


        :param src_table: Source table name or API object resolved through the root.
        :param dst_table: Destination table name or API object resolved through the root.
        :param table_type: Optional required TableTypes value.
        :return: Registered directed NumPy link-table object.
        :raises KeyError: An endpoint/route is absent or the refreshed route has a different required type.
        """

        src_name = self.get_main_table(src_table).table
        dst_name = self.get_main_table(dst_table).table
        key = (src_name, dst_name)
        self._ensure_link_table_fresh(key)
        table = self.link_tables[key]
        if table_type is not None and table.table_type != table_type:
            raise KeyError(f"Link table {src_name!r}->{dst_name!r} is {table.table_type}, not {table_type}")
        return table

    def get_one_one_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
    ) -> StorageCacheOneToOneLinkTable[Any]:
        """
        Resolve a directed link view requiring TableTypes.ONE_ONE.

        Endpoint/link freshness and error handling are delegated to the generic
        link lookup; no reverse-route fallback or cardinality conversion is added.

        Example:
            A route of another table type raises KeyError rather than returning
            a differently shaped link interface.


        :param src_table: Source table name or API reference.
        :param dst_table: Destination table name or API reference.
        :return: Link cache returned by get_link_table with the required cardinality.
        :raises KeyError: An endpoint/route is absent or its current type differs from ONE_ONE.
        """

        return self.get_link_table(src_table, dst_table, table_type=TableTypes.ONE_ONE)

    def get_one_many_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
    ) -> StorageCacheOneToManyLinkTable:
        """
        Resolve a directed link view requiring TableTypes.ONE_MANY.

        Endpoint/link freshness and error handling are delegated to the generic
        link lookup; no reverse-route fallback or cardinality conversion is added.

        Example:
            A route of another table type raises KeyError rather than returning
            a differently shaped link interface.


        :param src_table: Source table name or API reference.
        :param dst_table: Destination table name or API reference.
        :return: Link cache returned by get_link_table with the required cardinality.
        :raises KeyError: An endpoint/route is absent or its current type differs from ONE_MANY.
        """

        return self.get_link_table(src_table, dst_table, table_type=TableTypes.ONE_MANY)

    def get_many_one_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
    ) -> StorageCacheManyToOneLinkTable:
        """
        Resolve a directed link view requiring TableTypes.MANY_ONE.

        Endpoint/link freshness and error handling are delegated to the generic
        link lookup; no reverse-route fallback or cardinality conversion is added.

        Example:
            A route of another table type raises KeyError rather than returning
            a differently shaped link interface.


        :param src_table: Source table name or API reference.
        :param dst_table: Destination table name or API reference.
        :return: Link cache returned by get_link_table with the required cardinality.
        :raises KeyError: An endpoint/route is absent or its current type differs from MANY_ONE.
        """

        return self.get_link_table(src_table, dst_table, table_type=TableTypes.MANY_ONE)

    def get_many_many_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
    ) -> StorageCacheManyToManyLinkTable:
        """
        Resolve a directed link view requiring TableTypes.MANY_MANY.

        Endpoint/link freshness and error handling are delegated to the generic
        link lookup; no reverse-route fallback or cardinality conversion is added.

        Example:
            A route of another table type raises KeyError rather than returning
            a differently shaped link interface.


        :param src_table: Source table name or API reference.
        :param dst_table: Destination table name or API reference.
        :return: Link cache returned by get_link_table with the required cardinality.
        :raises KeyError: An endpoint/route is absent or its current type differs from MANY_MANY.
        """

        return self.get_link_table(src_table, dst_table, table_type=TableTypes.MANY_MANY)

    def iter_link_tables(self) -> Iterable[NumpyVectorizedLinkTable]:
        """
        Yield directed link views in sorted endpoint-key order through fresh lookup.

        Each lookup can refresh endpoint tables as well as link records. Work
        happens during iteration rather than generator construction.

        Example:
            Forward and reverse views appear separately when both keys are registered.


        :return: Generator over registered views, without deduplication by physical table.
        """

        for key in sorted(self.link_tables):
            yield self.get_link_table(*key)

    def has_field(self, name: FieldKey) -> bool:
        """
        Test whether field-name resolution succeeds without refreshing data.

        Other errors propagate. A field object carrying field_key can pass
        resolution without registry membership; this method is not a full validation
        of object ownership or its buffers.

        Example:
            A registered but stale field still returns True without a reload.


        :param name: Field name/key accepted by _resolve_field_name.
        :return: False for resolution KeyError, otherwise true.
        """

        try:
            self._resolve_field_name(name)
        except KeyError:
            return False
        return True

    def get_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
    ) -> FieldBasicInterfaceAPI[Any]:
        """
        Resolve a public field key and refresh declared dependencies before returning it.

        Check freshness only if table/link/field stale sets are nonempty. Existing
        scalar aliases retain their spelling during resolution. A stale bare alias
        can reach reload_field, which requires a canonical dictionary key.

        Example:
            Canonical books.title is a reliable key for explicit field reload and
            invalidation paths that must address _field_objects.


        :param name: Field name, alias or object accepted by _resolve_field_name.
        :return: Field object from the public fields dictionary after applicable refresh work.
        :raises KeyError: Name resolution, dependency reload or final public-key lookup fails.
        """

        field_name = self._resolve_field_name(name)
        if self._stale_fields or self._stale_main_tables or self._stale_link_tables:
            self._ensure_field_fresh(field_name)
        return self.fields[field_name]

    def iter_fields(self) -> Iterable[FieldBasicInterfaceAPI[Any]]:
        """
        Yield canonical fields in sorted key order with dependency refresh per lookup.

        Example:
            A title alias and its qualified key contribute one canonical field
            object to iteration.


        :return: Generator over canonical objects, excluding duplicate public alias entries.
        """

        for field_name in sorted(self._field_objects):
            yield self.get_field(field_name)

    def get_fields_for_table(
        self,
        table: Union[str, StorageCacheSingleTableAPI],
    ) -> Sequence[FieldBasicInterfaceAPI[Any]]:
        """
        Collect canonical fields whose declared owner matches a resolved main table.

        Iterate all fields through get_field before filtering by ownership.
        Consequently stale unrelated fields may also be refreshed or raise errors.

        Example:
            A books-to-tags projection is included for books because its source
            table owns the field.


        :param table: Table name or API reference resolved through get_main_table.
        :return: Tuple of matching field objects in canonical-key order.
        """

        table_name = self.get_main_table(table).table
        return tuple(
            field for field in self.iter_fields() if self._field_owner_table(field) == table_name
        )

    def reload_main_table(
        self,
        name: Union[str, StorageCacheSingleTableAPI],
        db: Any = None,
    ) -> None:
        """
        Reload one table, rebuild non-stale related topology and rebind dependent fields.

        Resolve the table before attaching the database. Reload its data, then
        refresh topology for touching link views unless a view is already marked
        stale. Clear table/ID markers before rebinding dependent fields, skipping
        fields whose touching link view was deferred. Discard canonical field markers
        as each binding succeeds. This does not read non-stale link records anew,
        refresh unrelated tables or roll back partial refresh work on error.

        Example:
            Deleting a destination row then reloading its table updates positions
            and missing-destination projections without re-reading unchanged link rows.


        :param name: Main-table name or table API reference.
        :param db: Database override retained on the root; None reuses its attachment.
        :return: None; refreshes complete table arrays and their applicable dependent projections.
        """

        table_name = (
            name.table
            if isinstance(name, StorageCacheSingleTableAPI)
            else str(name)
        )
        table = self.main_tables[table_name]
        table.reload(self._require_db(db))
        skipped_link_keys: set[tuple[str, str]] = set()
        for key, link_table in self.link_tables.items():
            if table.table not in (link_table.primary_table, link_table.secondary_table):
                continue
            if key in self._stale_link_tables:
                skipped_link_keys.add(key)
                continue
            link_table.refresh_relation_topology()
        self._stale_main_tables.discard(table.table)
        self._stale_ids.pop(table.table, None)
        for field in self._field_objects.values():
            if table.table not in self._field_tables(field):
                continue
            link_key = self._field_link_key(field)
            if link_key is not None and link_key in skipped_link_keys:
                continue
            field.read(self.db)
            self._stale_fields.discard(field.field_key)

    def reload_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
        db: Any = None,
        table_type: Optional[TableTypes] = None,
    ) -> None:
        """
        Reload one directed link view and rebind fields using that exact route.

        Resolve names without freshness-aware endpoint lookup. Validate optional
        cardinality before attaching/reading, then clear the link marker before
        rebinding fields. The reverse route and endpoint table data are not reloaded
        here. Later field errors can follow a successful link refresh.

        Example:
            Reloading books-to-tags does not automatically reload the separate
            tags-to-books link object.


        :param src_table: Source table name or API reference.
        :param dst_table: Destination table name or API reference.
        :param db: Database override retained on the root; None uses current attachment.
        :param table_type: Optional type checked against the current view before reload.
        :return: None; reloads link records/topology and clears matching canonical field markers.
        :raises KeyError: The directed route is absent or its pre-reload type fails the requested check.
        """

        src_name = (
            src_table.table
            if isinstance(src_table, StorageCacheSingleTableAPI)
            else str(src_table)
        )
        dst_name = (
            dst_table.table
            if isinstance(dst_table, StorageCacheSingleTableAPI)
            else str(dst_table)
        )
        table = self.link_tables[(src_name, dst_name)]
        if table_type is not None and table.table_type != table_type:
            raise KeyError(
                f"Link table {src_name!r}->{dst_name!r} is "
                f"{table.table_type}, not {table_type}"
            )
        table.reload(self._require_db(db))
        key = (table.primary_table, table.secondary_table)
        self._stale_link_tables.discard(key)
        for field in self._field_objects.values():
            if self._field_link_key(field) == key:
                field.read(self.db)
                self._stale_fields.discard(field.field_key)

    def reload_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
        db: Any = None,
    ) -> None:
        """
        Rebind one canonical field from current table/link state and clear its marker.

        Lookup uses _field_objects after name resolution. A registered bare alias
        can remain unqualified and fail that lookup; pass a canonical key for direct
        reload. Clear the canonical field marker only after read succeeds.

        Example:
            Reload books.title after its table data has been refreshed to rebind
            the scalar array and exact value index.


        :param name: Canonical field name or field reference resolved by _resolve_field_name.
        :param db: Database override retained on the root before field.read.
        :return: None; calls field.read without refreshing its underlying table/link records.
        :raises KeyError: The resolved name is absent from the canonical field dictionary.
        """

        field_name = self._resolve_field_name(name)
        field = self._field_objects[field_name]
        field.read(self._require_db(db))
        self._stale_fields.discard(field.field_key)

    def invalidate_table(
        self,
        table: Union[str, StorageCacheSingleTableAPI],
    ) -> None:
        """
        Resolve a main table then mark its complete data stale.

        Resolution can refresh an already stale table before it is marked stale
        again. This is not unconditionally free of database reads.

        Example:
            A fresh table is only marked stale; repeating invalidation before a
            read can first refresh the earlier invalidation.


        :param table: Table name or API reference resolved through get_main_table.
        :return: None; adds the resolved table name to _stale_main_tables.
        """

        table_name = self.get_main_table(table).table
        self._stale_main_tables.add(table_name)

    def invalidate_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
        table_type: Optional[TableTypes] = None,
    ) -> None:
        """
        Resolve a directed link view and mark its exact key stale.

        The resolving lookup can refresh stale endpoints or the link view before
        marking it. No reverse key is automatically added.

        Example:
            Invalidate both directions explicitly when both oriented link caches
            need to observe an external association change.


        :param src_table: Source table name or API reference.
        :param dst_table: Destination table name or API reference.
        :param table_type: Optional type requirement enforced during link lookup.
        :return: None; records the resolved directed endpoint key as stale.
        """

        table = self.get_link_table(src_table, dst_table, table_type=table_type)
        self._stale_link_tables.add((table.primary_table, table.secondary_table))

    def invalidate_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
    ) -> None:
        """
        Resolve a field name and retain that exact spelling as a stale marker.

        Existing bare aliases remain bare. Later reload_field addresses canonical
        objects, so canonical keys avoid the alias mismatch in direct reload paths.

        Example:
            Use books.title as the invalidation key for the canonical scalar field.


        :param name: Field name, alias or reference accepted by _resolve_field_name.
        :return: None; adds the resolved name without reading field data.
        """

        field_name = self._resolve_field_name(name)
        self._stale_fields.add(field_name)

    def invalidate_ids(
        self,
        table: Union[str, StorageCacheSingleTableAPI],
        ids: Iterable[int],
    ) -> None:
        """
        Record integer row IDs and mark the entire owning table stale.

        Resolution may first refresh earlier stale state. Conversion can partly
        update the ID set before failing; the table-wide marker is added afterward.
        Normal successful ID invalidation triggers a full-table reload in this backend.

        Example:
            An empty direct backend ID invalidation still marks its table stale;
            the modern facade filters empty ID groups before calling this method.


        :param table: Table name or API object resolved with get_main_table before consuming IDs.
        :param ids: ID iterable consumed into the table's stale set with int conversion.
        :return: None; marks the table stale even when the ID iterable is empty.
        """

        table_name = self.get_main_table(table).table
        self._stale_ids[table_name].update(int(row_id) for row_id in ids)
        self._stale_main_tables.add(table_name)

    def get_numpy_row_id_array(self, table_name: str) -> Any:
        """
        Expose a main table's row-ID buffer after applicable dependency refresh.

        Treat the buffer as read-only; mutation does not maintain other row/index
        structures.

        Example:
            A stale table may be fully reloaded before this method returns its buffer.


        :param table_name: Main-table name resolved by get_main_table.
        :return: Shared NumPy integer array or tuple fallback, without copying.
        """

        return self.get_main_table(table_name).row_id_array

    def get_numpy_field_owner_ids(self, field_name: str) -> Any:
        """
        Expose a field's owner-ID buffer through its optional vectorized getter.

        For plural relation fields, owner IDs enumerate groups and are not
        elementwise aligned with the flat value buffer; offsets describe the groups.

        Example:
            A source linked to three destinations contributes one owner-ID entry.


        :param field_name: Field key or alias resolved with dependency refresh.
        :return: Getter result unchanged, commonly a shared array or tuple fallback.
        :raises KeyError: Field resolution fails or the resolved field has no callable owner-ID getter.
        """

        field = self.get_field(field_name)
        getter = getattr(field, "get_numpy_owner_ids_array", None)
        if callable(getter):
            return getter()
        raise KeyError(str(field_name))

    def get_numpy_field_array(self, field_name: str) -> Any:
        """
        Expose a field's value buffer through its optional vectorized getter.

        Plural relation fields return flat link-order values. No copy or dtype
        coercion is added and independently mutating a buffer can desynchronize indexes.

        Example:
            Use relation offsets when grouping a plural field's flat values by owner.


        :param field_name: Field key or alias resolved with dependency refresh.
        :return: Getter result unchanged, commonly a shared array or tuple fallback.
        :raises KeyError: Field resolution fails or the field has no callable values-array getter.
        """

        field = self.get_field(field_name)
        getter = getattr(field, "get_numpy_values_array", None)
        if callable(getter):
            return getter()
        raise KeyError(str(field_name))

    def get_cached_value(
        self,
        owner_id: int,
        field_key: str,
        default_value: Any = None,
    ) -> Any:
        """
        Read one field value after explicit dependency refresh and apply a None fallback.

        Use a direct registered string key when possible. Refresh when table/link/
        field stale sets are nonempty. Prefer concrete scalar and relation methods,
        then callable generic scalar/source getters, then the StorageCacheAPI fallback.
        Unknown fields and getter errors propagate; a missing known row and a stored
        None share the default result on the supported scalar path.

        Example:
            For a stored None title, get_cached_value(1, "title", "missing") returns
            "missing" rather than None.


        :param owner_id: Owner ID converted with int before field resolution.
        :param field_key: Public field key, alias or reference accepted by the resolver.
        :param default_value: Fallback returned when a supported getter returns None.
        :return: Field value, including scalar/sequence shapes, or the supplied None fallback.
        """

        owner_id = int(owner_id)
        field_name = field_key if isinstance(field_key, str) and field_key in self.fields else self._resolve_field_name(field_key)
        if self._stale_fields or self._stale_main_tables or self._stale_link_tables:
            self._ensure_field_fresh(field_name)
        field = self.fields[field_name]
        if isinstance(field, NumpyVectorizedSameTableField):
            value = field.get_value_from_id(owner_id)
            return default_value if value is None else value
        if isinstance(field, _NumpyVectorizedRelationFieldBase):
            value = field.get_value_from_src_id(owner_id)
            return default_value if value is None else value
        getter = getattr(field, "get_value_from_id", None)
        if callable(getter):
            value = getter(owner_id)
            return default_value if value is None else value
        getter = getattr(field, "get_value_from_src_id", None)
        if callable(getter):
            value = getter(owner_id)
            return default_value if value is None else value
        return super().get_cached_value(owner_id, field_key, default_value=default_value)

    def get_cached_row_values(
        self,
        owner_id: int,
        field_keys: tuple[str, ...] | list[str],
        default_value: Any = None,
    ) -> tuple[Any, ...]:
        """
        Read ordered field values with a same-table scalar shortcut when possible.

        Resolve every field with get_field. Mixed relation/scalar fields or scalar
        fields from different tables use get_cached_value for each, substituting the
        default for None. All scalar fields from one table use direct column reads:
        missing owners return defaults, but stored None values are preserved. This
        shortcut therefore differs from the single-value getter's None handling.
        Resolution and backend errors propagate; no read transaction is opened.

        Example:
            For an existing row with title=None, the all-scalar shortcut returns
            (None,) even with default_value="missing"; a missing row returns ("missing",).


        :param owner_id: Owner ID interpreted after resolving fields; empty field input leaves it unused.
        :param field_keys: Ordered field keys/aliases resolved first, retaining repeated requests.
        :param default_value: Fallback for absent owners or fallback-path None values.
        :return: Tuple of values in requested order; empty field input returns ().
        """

        resolved_fields = tuple(self.get_field(field_key) for field_key in field_keys)
        if not resolved_fields:
            return ()

        table_name: str | None = None
        scalar_fields: list[NumpyVectorizedSameTableField] = []
        for field in resolved_fields:
            if not isinstance(field, NumpyVectorizedSameTableField):
                return tuple(
                    self.get_cached_value(owner_id, str(getattr(field, "field_key", field)), default_value=default_value)
                    for field in resolved_fields
                )
            if table_name is None:
                table_name = field.table_name
            elif field.table_name != table_name:
                return tuple(
                    self.get_cached_value(owner_id, str(getattr(field, "field_key", field)), default_value=default_value)
                    for field in resolved_fields
                )
            scalar_fields.append(field)

        table = self.get_main_table(cast(str, table_name))
        if not table.has_id(int(owner_id)):
            return tuple(default_value for _field in scalar_fields)
        return tuple(
            (table.get_column_value_from_id(int(owner_id), field.column_name) if table.has_id(int(owner_id)) else default_value)
            for field in scalar_fields
        )


StorageCache = NumpyVectorizedStorageCache

__all__ = [
    "NumpyVectorizedMainTableCache",
    "NumpyVectorizedLinkTable",
    "NumpyVectorizedManyManyField",
    "NumpyVectorizedManyOneField",
    "NumpyVectorizedOneManyField",
    "NumpyVectorizedSameTableField",
    "NumpyVectorizedStorageCache",
    "NumpyVectorizedTwoTableOneOneField",
    "StorageCache",
]
