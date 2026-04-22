"""NumPy-oriented storage cache plugin with independent array-backed storage."""

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
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.base_field import (
    FieldBasicInterfaceAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.many_many_field import (
    IndividualLinkProperties as ManyManyIndividualLinkProperties,
    ManyManyInTwoTableFieldUpdate,
    ManyToManyFieldAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.many_one_field import (
    IndividualLinkProperties as ManyOneIndividualLinkProperties,
    ManyOneInTwoTableFieldUpdate,
    ManyToOneFieldAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.one_many_field import (
    IndividualLinkProperties as OneManyIndividualLinkProperties,
    OneManyInTwoTableFieldUpdate,
    OneToManyFieldAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.one_one_field import (
    CacheOneOneInSameTableFieldAPI,
    CacheOneOneInTwoTableFieldAPI,
    OneOneInOneTableFieldUpdate,
    OneOneInTwoTableFieldUpdate,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.base_table import (
    TableMetadata,
    TableTypes,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.link_tables.many_many_tables import (
    StorageCacheManyToManyLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.link_tables.many_one_tables import (
    StorageCacheManyToOneLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.link_tables.one_many_tables import (
    StorageCacheOneToManyLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.link_tables.one_one_tables import (
    StorageCacheOneToOneLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.single_table import (
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
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI
    from LiuXin_alpha.databases.db_types import MainTableName
    from LiuXin_alpha.databases.schema_specs import StorageSchemaSpec


def _ensure_db(current_db: Any, passed_db: Any = None) -> Any:
    db = passed_db if passed_db is not None else current_db
    if db is None:
        raise RuntimeError("Storage cache requires an attached database")
    return db


def _canonical_field_key(table_name: str, column_name: str) -> str:
    return f"{table_name}.{column_name}"


def _column_type_map(spec: StorageTableSpec) -> dict[str, str]:
    return {
        col.name: col.declared_type or col.affinity or "UNKNOWN"
        for col in spec.columns
    }


def _default_value_column(spec: StorageTableSpec) -> Optional[str]:
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
    if _np is not None and isinstance(value, _np.generic):
        return value.item()
    return value


def _as_int_array(values: Iterable[int]) -> Any:
    normalized = tuple(int(value) for value in values)
    if _np is None:
        return normalized
    return _np.asarray(normalized, dtype=_np.int64)


def _column_array_dtype(values: Sequence[Any], declared_type: str) -> Any:
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
    normalized = tuple(values)
    if _np is None:
        return normalized
    dtype = _column_array_dtype(normalized, declared_type)
    if dtype is None:
        return _np.asarray(normalized)
    return _np.asarray(normalized, dtype=dtype)


def _array_value(array_obj: Any, index: int) -> Any:
    return _normalize_numpy_scalar(array_obj[index])


def _array_values(array_obj: Any, *, start: int = 0, end: Optional[int] = None) -> list[Any]:
    slice_obj = array_obj[start:end] if end is not None else array_obj[start:]
    return [_normalize_numpy_scalar(value) for value in slice_obj]


class NumpyVectorizedMainTableCache(StorageCacheSingleTableAPI):
    """Array-backed cache for one main table."""

    spec: StorageTableSpec

    def __init__(self, spec: StorageTableSpec, db: Any) -> None:
        metadata = TableMetadata(
            table_name=spec.name,
            main_table=True,
            is_interlink=False,
            is_intralink=False,
        )
        super().__init__(table=spec.name, db=db, metadata=metadata)
        self.spec = spec
        self._row_id_array: Any = _as_int_array(())
        self._row_ids: tuple[int, ...] = ()
        self._row_id_positions: dict[int, int] = {}
        self._column_arrays: dict[str, Any] = {}
        self._value_indexes: dict[str, dict[Any, tuple[int, ...]]] = {}
        self._loaded = False

    @property
    def id_column(self) -> str:
        if self.spec.id_column is None:
            raise RuntimeError(f"Table {self.table!r} does not expose an id column")
        return self.spec.id_column

    @property
    def default_value_column(self) -> Optional[str]:
        return _default_value_column(self.spec)

    @property
    def row_ids(self) -> tuple[int, ...]:
        return self._row_ids

    @property
    def row_id_array(self) -> Any:
        return self._row_id_array

    @property
    def column_headings(self) -> list[str]:
        return [col.name for col in self.spec.columns]

    @property
    def column_types(self) -> dict[str, str]:
        return _column_type_map(self.spec)

    def linked_to(self) -> Iterable[str]:
        return tuple(self.spec.linked_tables)

    def _row_from_snapshot(self, row_dict: Mapping[str, Any]) -> Row:
        return Row(database=self.db, row_dict=deepcopy(dict(row_dict)), read_only=True)

    def _build_value_indexes(self, row_ids: Sequence[int], column_values: dict[str, Sequence[Any]]) -> None:
        indexes: dict[str, dict[Any, list[int]]] = {
            column: defaultdict(list)
            for column in self.column_headings
        }
        for row_position, row_id in enumerate(row_ids):
            for column in self.column_headings:
                value = column_values[column][row_position]
                indexes[column][value].append(int(row_id))
        self._value_indexes = {
            column: {value: tuple(ids) for value, ids in values.items()}
            for column, values in indexes.items()
        }

    def _replace_rows(self, row_dicts: Sequence[Mapping[str, Any]]) -> None:
        sortable: list[tuple[int, int, dict[str, Any]]] = []
        for index, row_dict in enumerate(row_dicts):
            row_copy = deepcopy(dict(row_dict))
            row_id = row_copy.get(self.id_column)
            if row_id is None:
                continue
            sortable.append((int(row_id), index, row_copy))
        sortable.sort(key=lambda item: (item[0], item[1]))

        row_ids = tuple(int(row_id) for row_id, _index, _row in sortable)
        column_values: dict[str, list[Any]] = {
            column: []
            for column in self.column_headings
        }
        for _row_id, _index, row_copy in sortable:
            for column in self.column_headings:
                column_values[column].append(row_copy.get(column))

        self._row_ids = row_ids
        self._row_id_array = _as_int_array(row_ids)
        self._row_id_positions = {row_id: position for position, row_id in enumerate(row_ids)}
        self._column_arrays = {
            column: _as_column_array(values, self.column_types[column])
            for column, values in column_values.items()
        }
        self._build_value_indexes(row_ids, column_values)
        self._loaded = True

    def read(self, db: Any) -> None:
        db = _ensure_db(self.db, db)
        self.db = db
        rows = db.get_all_rows(self.table, iterator_return=False)
        self._replace_rows([row.row_dict for row in rows])

    def reload(self, db: Any) -> None:
        self.read(db=db)

    def _normalize_row_payload(
        self,
        table_id_val_map: Mapping[int, Any],
        target_column: Optional[str],
    ) -> dict[int, dict[str, Any]]:
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
        del ids
        self.read(_ensure_db(self.db))

    def has_id(self, table_id: int) -> bool:
        return int(table_id) in self._row_id_positions

    def get_column_value_from_id(self, table_id: int, column: str) -> Any:
        row_id = int(table_id)
        if row_id not in self._row_id_positions:
            raise KeyError(row_id)
        if column not in self._column_arrays:
            raise KeyError(column)
        return _array_value(self._column_arrays[column], self._row_id_positions[row_id])

    def get_values_for(self, column: str) -> Sequence[Any]:
        if column not in self._column_arrays:
            raise KeyError(column)
        return _array_values(self._column_arrays[column])

    def get_unique_values(self, column: str) -> set[Any]:
        if column not in self._value_indexes:
            raise KeyError(column)
        return set(self._value_indexes[column].keys())

    def get_ids_for_value(self, column: str, value: str) -> set[int]:
        if column not in self._value_indexes:
            raise KeyError(column)
        return set(self._value_indexes[column].get(value, ()))

    def get_col_value_from_id(self, table_id: int) -> Any:
        default_column = self.default_value_column
        if default_column is None:
            return self.get_row_snapshot(table_id)
        return self.get_column_value_from_id(int(table_id), default_column)

    def get_row_snapshot(self, table_id: int) -> dict[str, Any]:
        row_id = int(table_id)
        if row_id not in self._row_id_positions:
            raise KeyError(row_id)
        position = self._row_id_positions[row_id]
        return {
            column: _array_value(array_obj, position)
            for column, array_obj in self._column_arrays.items()
        }

    def get_row(self, table_id: int) -> Row:
        return self._row_from_snapshot(self.get_row_snapshot(table_id))

    def create(
        self,
        table_id_val_map: Mapping[int, Any],
        db: Any,
        target_column: Optional[str] = None,
        allow_case_change: bool = False,
    ) -> None:
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
        del table_id_val_map, target_column, allow_case_change
        self.read(_ensure_db(self.db))

    def _create_to_db(
        self,
        table_id_val_map: Mapping[int, Any],
        db: Any,
        target_column: Optional[str] = None,
        allow_case_change: bool = False,
    ) -> None:
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
        del table_id_val_map, target_column, allow_case_change
        self.read(_ensure_db(self.db))

    def _update_db(
        self,
        table_id_val_map: Mapping[int, Any],
        db: Any,
        target_column: Optional[str] = None,
        allow_case_change: bool = False,
    ) -> None:
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
        self._delete_from_db(table_ids, db)
        self.read(db)

    def _delete_from_cache(self, table_ids: Iterable[int]) -> None:
        del table_ids
        self.read(_ensure_db(self.db))

    def _delete_from_db(self, table_ids: Iterable[str], db: Any) -> None:
        db = _ensure_db(self.db, db)
        ids = {int(table_id) for table_id in table_ids}
        if ids:
            db.driver_wrapper.delete_by_id(self.table, ids)


class NumpyVectorizedSameTableField(CacheOneOneInSameTableFieldAPI[Any]):
    """Scalar field view over an array-backed main table."""

    def __init__(
        self,
        cache: "NumpyVectorizedStorageCache",
        in_table: Union[StorageCacheSingleTableAPI, str],
        column_name: str,
        db: Any,
    ) -> None:
        self._cache = cache
        self.column_name = str(column_name)
        super().__init__(in_table=in_table, db=db)

    @property
    def field_key(self) -> str:
        return _canonical_field_key(self.table_name, self.column_name)

    def get_main_table(
        self,
        name: Union[str, StorageCacheSingleTableAPI],
    ) -> StorageCacheSingleTableAPI:
        return self._cache.get_main_table(name)

    def _table_cache(self) -> NumpyVectorizedMainTableCache:
        return cast(NumpyVectorizedMainTableCache, self.in_table)

    def _column_spec(self):
        table = self._table_cache()
        for column in table.spec.columns:
            if column.name == self.column_name:
                return column
        raise KeyError(self.column_name)

    def _assert_can_write_value(self, value: Any) -> None:
        column = self._column_spec()
        if value is None and (column.is_primary_key or not column.nullable):
            raise ValueError(
                f"Field {self.field_key!r} cannot be cleared because the column is not nullable"
            )

    def _assert_rows_exist(self, ids: Iterable[int]) -> None:
        missing = [
            int(row_id)
            for row_id in ids
            if self._db.get_row_from_id(self.table_name, int(row_id)) is None
        ]
        if missing:
            raise KeyError(f"Cannot update field {self.field_key!r}; missing row ids: {sorted(missing)}")

    def _write_values(self, id_value_map: dict[int, Any]) -> None:
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
        self._db = _ensure_db(self._db, db)

    def refresh_ids(
        self,
        ids: Iterable[int],
        db: Optional["DatabaseAPI"] = None,
    ) -> None:
        del ids
        self._db = _ensure_db(self._db, db)

    def remove_ids(self, ids: Iterable[int]) -> None:
        del ids

    @property
    def ids(self) -> set[int]:
        return set(int(row_id) for row_id in self.in_table.row_ids)

    @property
    def values(self) -> list[Any]:
        return list(self._table_cache().get_values_for(self.column_name))

    @property
    def values_set(self) -> set[Any]:
        return self._table_cache().get_unique_values(self.column_name)

    @property
    def ids_values_map(self) -> dict[int, Optional[Any]]:
        table = self._table_cache()
        return {
            int(row_id): table.get_column_value_from_id(int(row_id), self.column_name)
            for row_id in table.row_ids
        }

    def get_value_from_id(self, table_id: int) -> Optional[Any]:
        table = self._table_cache()
        row_id = int(table_id)
        if not table.has_id(row_id):
            return None
        return table.get_column_value_from_id(row_id, self.column_name)

    def get_ids_from_value(self, value: Any) -> list[int]:
        return sorted(self._table_cache().get_ids_for_value(self.column_name, value))

    def get_numpy_owner_ids_array(self) -> Any:
        return self._table_cache().row_id_array

    def get_numpy_values_array(self) -> Any:
        return self._table_cache()._column_arrays[self.column_name]

    def update(self, update: OneOneInOneTableFieldUpdate[Any]) -> None:
        self._db = _ensure_db(self._db)
        if update.added_maps:
            self._write_values({int(row_id): value for row_id, value in update.added_maps.items()})
        if update.updated_maps:
            self._write_values({int(row_id): value for row_id, value in update.updated_maps.items()})
        if update.deleted_ids:
            self._write_values({int(row_id): None for row_id in update.deleted_ids})
        self.read(self._db)


class _NumpyVectorizedRelationFieldBase:
    """Shared helpers for NumPy-backed relation fields."""

    def _init_relation_field(
        self,
        cache: "NumpyVectorizedStorageCache",
        db: Any,
    ) -> None:
        self._cache = cache
        self._db = db
        self._src_ids_array: Any = _as_int_array(())
        self._src_ids: tuple[int, ...] = ()
        self._src_positions: dict[int, int] = {}
        self._src_offsets: Any = _as_int_array((0,))
        self._flat_dst_ids: Any = _as_int_array(())
        self._flat_values: Any = _as_column_array((), "TEXT")
        self._dst_to_src_ids: dict[int, tuple[int, ...]] = {}
        self._dst_to_values: dict[int, Optional[Any]] = {}
        self._value_to_src_ids: dict[Any, tuple[int, ...]] = {}
        self._value_to_dst_ids: dict[Any, tuple[int, ...]] = {}

    @property
    def field_key(self) -> str:
        return _canonical_field_key(
            self.src_table_name,
            f"{self.dst_table_name}.{self.dst_table_cache_col}",
        )

    @property
    def table_name(self) -> str:
        return self.src_table_name

    @property
    def column_name(self) -> str:
        return self.dst_table_cache_col

    def get_main_table(
        self,
        name: Union[str, StorageCacheSingleTableAPI],
    ) -> StorageCacheSingleTableAPI:
        return self._cache.get_main_table(name)

    def _link_table_cache(self) -> NumpyVectorizedLinkTable:
        return cast(NumpyVectorizedLinkTable, self.link_table)

    def _value_for_dst_id(self, dst_id: int) -> Optional[Any]:
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
        values = self._values_for_src_id(int(src_id), type_filter=type_filter)
        return values[0] if values else None

    def _src_slice(self, src_id: int) -> Optional[tuple[int, int]]:
        position = self._src_positions.get(int(src_id))
        if position is None:
            return None
        start = int(_array_value(self._src_offsets, position))
        end = int(_array_value(self._src_offsets, position + 1))
        return (start, end)

    def _cached_dst_ids_for_src(self, src_id: int) -> tuple[int, ...]:
        slice_bounds = self._src_slice(int(src_id))
        if slice_bounds is None:
            return ()
        start, end = slice_bounds
        return tuple(int(value) for value in _array_values(self._flat_dst_ids, start=start, end=end))

    def _cached_values_for_src(self, src_id: int) -> tuple[Optional[Any], ...]:
        slice_bounds = self._src_slice(int(src_id))
        if slice_bounds is None:
            return ()
        start, end = slice_bounds
        return tuple(_array_values(self._flat_values, start=start, end=end))

    def _read_relation_cache(self) -> None:
        src_ids: list[int] = []
        offsets: list[int] = [0]
        flat_dst_ids: list[int] = []
        flat_values: list[Optional[Any]] = []
        dst_to_src_ids: dict[int, list[int]] = defaultdict(list)
        dst_to_values: dict[int, Optional[Any]] = {}
        value_to_src_ids: dict[Any, list[int]] = defaultdict(list)
        value_to_dst_ids: dict[Any, list[int]] = defaultdict(list)

        for src_id in sorted(int(row_id) for row_id in self.src_table.row_ids):
            dst_ids = self._ordered_dst_ids_for_src(src_id, require_ordering=True)
            if not dst_ids:
                continue
            src_ids.append(src_id)
            for dst_id in dst_ids:
                value = self._value_for_dst_id(dst_id)
                flat_dst_ids.append(int(dst_id))
                flat_values.append(value)
                dst_to_src_ids[int(dst_id)].append(int(src_id))
                dst_to_values[int(dst_id)] = value
                value_to_src_ids[value].append(int(src_id))
                value_to_dst_ids[value].append(int(dst_id))
            offsets.append(len(flat_dst_ids))

        self._src_ids = tuple(src_ids)
        self._src_ids_array = _as_int_array(src_ids)
        self._src_positions = {int(src_id): index for index, src_id in enumerate(src_ids)}
        self._src_offsets = _as_int_array(offsets)
        self._flat_dst_ids = _as_int_array(flat_dst_ids)
        self._flat_values = _as_column_array(flat_values, "TEXT")
        self._dst_to_src_ids = {
            dst_id: tuple(src_values)
            for dst_id, src_values in dst_to_src_ids.items()
        }
        self._dst_to_values = dict(dst_to_values)
        self._value_to_src_ids = {
            value: tuple(sorted(src_values))
            for value, src_values in value_to_src_ids.items()
        }
        self._value_to_dst_ids = {
            value: tuple(sorted(dst_values))
            for value, dst_values in value_to_dst_ids.items()
        }

    def read(self, db: Any) -> None:
        db = _ensure_db(self._db, db)
        self._db = db
        self.src_table.db = db
        self.dst_table.db = db
        self.link_table.db = db
        self._read_relation_cache()

    def refresh_ids(
        self,
        ids: Iterable[int],
        db: Any = None,
    ) -> None:
        del ids
        self.read(_ensure_db(self._db, db))

    def remove_ids(self, ids: Iterable[int]) -> None:
        del ids
        if self._db is not None:
            self.read(self._db)

    def _flattened_values(self) -> list[Optional[Any]]:
        return list(_array_values(self._flat_values))

    def get_numpy_owner_ids_array(self) -> Any:
        return self._src_ids_array

    def get_numpy_values_array(self) -> Any:
        return self._flat_values

    def _unlink_src_ids(self, src_ids: Iterable[int]) -> None:
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
        if create_missing_related_rows and not create_missing_links:
            raise ValueError(
                f"Field {self.field_key!r} cannot create related rows without also creating links"
            )

    def _update_dst_values(self, dst_values_map: dict[int, Optional[Any]]) -> None:
        if not dst_values_map:
            return
        self._db = _ensure_db(self._db)
        self.dst_table.update(
            {int(dst_id): value for dst_id, value in dst_values_map.items()},
            self._db,
            target_column=self.dst_table_cache_col,
        )

    def _existing_ordered_dst_ids_for_src(self, src_id: int) -> tuple[int, ...]:
        return self._ordered_dst_ids_for_src(
            int(src_id),
            require_ordering=bool(self._link_table_cache().link_spec.ordered),
        )

    def _get_unique_dst_id_for_value(self, value: Any) -> Optional[int]:
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
        self._db = _ensure_db(self._db)
        cast(Any, self.link_table).update(
            SimpleNamespace(create_these_links={int(src_id): int(dst_id)})
        )

    def _validate_link_dst_update(self, link_update: Any) -> None:
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
        row = cast(Any, self.link_table).get_link_row(int(src_id), int(dst_id))
        if row is None:
            raise KeyError((int(src_id), int(dst_id)))
        return dict(row.row_dict)

    def _column_for_extra(self, extra_name: str) -> Optional[str]:
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
        column_name = self._column_for_extra(str(extra_type))
        if column_name is None:
            raise KeyError(str(extra_type))
        self._update_link_row_columns(int(src_id), int(dst_id), {column_name: new_extra_value})
        self.read(self._db)


class NumpyVectorizedTwoTableOneOneField(
    _NumpyVectorizedRelationFieldBase,
    CacheOneOneInTwoTableFieldAPI[Any],
):
    def __init__(
        self,
        cache: "NumpyVectorizedStorageCache",
        src_table: Union[StorageCacheSingleTableAPI, str],
        src_table_id_col: str,
        dst_table: Union[StorageCacheSingleTableAPI, str],
        dst_table_cache_col: str,
        db: Any,
    ) -> None:
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
        return cast(NumpyVectorizedLinkTable, self._cache.get_one_one_link_table(src_table, dst_table))

    @property
    def ids(self) -> set[int]:
        return set(self._src_ids)

    @property
    def values(self) -> list[Any]:
        return self._flattened_values()

    @property
    def values_set(self) -> set[Any]:
        return set(self.values)

    @property
    def ids_values_map(self) -> dict[int, Optional[Any]]:
        return {src_id: (values[0] if values else None) for src_id, values in (
            (src_id, self._cached_values_for_src(src_id))
            for src_id in self._src_ids
        )}

    @property
    def dst_ids_values_map(self) -> dict[int, Optional[Any]]:
        return dict(self._dst_to_values)

    def get_value_from_src_id(self, src_id: int) -> Optional[Any]:
        values = self._cached_values_for_src(int(src_id))
        return values[0] if values else None

    def get_value_from_dst_id(self, dst_id: int) -> Optional[Any]:
        return self._dst_to_values.get(int(dst_id))

    def get_dst_id_from_src_id(self, src_id: int) -> Optional[int]:
        dst_ids = self._cached_dst_ids_for_src(int(src_id))
        return dst_ids[0] if dst_ids else None

    def get_src_id_from_dst_id(self, dst_id: int) -> Optional[int]:
        src_ids = self._dst_to_src_ids.get(int(dst_id), ())
        return int(src_ids[0]) if src_ids else None

    def get_src_ids_from_value(self, value: Any) -> list[int]:
        return list(self._value_to_src_ids.get(value, ()))

    def get_dst_ids_from_value(self, value: Any) -> list[int]:
        return list(self._value_to_dst_ids.get(value, ()))

    def update(self, update: OneOneInTwoTableFieldUpdate[Any]) -> None:
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
    def __init__(
        self,
        cache: "NumpyVectorizedStorageCache",
        src_table: Union[StorageCacheSingleTableAPI, str],
        src_table_id_col: str,
        dst_table: Union[StorageCacheSingleTableAPI, str],
        dst_table_cache_col: str,
        db: Any,
    ) -> None:
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
        return cast(NumpyVectorizedLinkTable, self._cache.get_many_one_link_table(src_table, dst_table))

    @property
    def ids(self) -> set[int]:
        return set(self._src_ids)

    @property
    def values(self) -> list[Any]:
        return self._flattened_values()

    @property
    def values_set(self) -> set[Any]:
        return set(self.values)

    @property
    def ids_values_map(self) -> dict[int, Optional[Any]]:
        return {src_id: (values[0] if values else None) for src_id, values in (
            (src_id, self._cached_values_for_src(src_id))
            for src_id in self._src_ids
        )}

    @property
    def dst_ids_values_map(self) -> dict[int, Optional[Any]]:
        return dict(self._dst_to_values)

    def get_value_from_src_id(self, src_id: int) -> Optional[Any]:
        values = self._cached_values_for_src(int(src_id))
        return values[0] if values else None

    def get_value_from_dst_id(self, dst_id: int) -> Optional[Any]:
        return self._dst_to_values.get(int(dst_id))

    def get_dst_id_from_src_id(
        self,
        src_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[int]:
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
        if require_ordering or type_filter is not None:
            return self._ordered_src_ids_for_dst(
                int(dst_id),
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
        return tuple(self._dst_to_src_ids.get(int(dst_id), ()))

    def get_src_ids_from_value(self, value: Any) -> list[int]:
        return list(self._value_to_src_ids.get(value, ()))

    def get_dst_ids_from_value(self, value: Any) -> list[int]:
        return list(self._value_to_dst_ids.get(value, ()))

    def get_link_properties(
        self,
        src_id: int,
        dst_id: int,
    ) -> ManyOneIndividualLinkProperties:
        return cast(
            ManyOneIndividualLinkProperties,
            self._build_link_properties(ManyOneIndividualLinkProperties, int(src_id), int(dst_id)),
        )

    def set_link_properties(
        self,
        updated_link_properties: ManyOneIndividualLinkProperties,
    ) -> None:
        self._set_link_properties(updated_link_properties)

    def update(self, update: ManyOneInTwoTableFieldUpdate[Any]) -> None:
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
    def __init__(
        self,
        cache: "NumpyVectorizedStorageCache",
        src_table: Union[StorageCacheSingleTableAPI, str],
        src_table_id_col: str,
        dst_table: Union[StorageCacheSingleTableAPI, str],
        dst_table_cache_col: str,
        db: Any,
    ) -> None:
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
        return cast(NumpyVectorizedLinkTable, self._cache.get_one_many_link_table(src_table, dst_table))

    @property
    def ids(self) -> set[int]:
        return set(self._src_ids)

    @property
    def values(self) -> list[Any]:
        return self._flattened_values()

    @property
    def values_set(self) -> set[Any]:
        return set(self.values)

    @property
    def ids_values_map(self) -> dict[int, Sequence[Optional[Any]]]:
        return {
            src_id: self._cached_values_for_src(src_id)
            for src_id in self._src_ids
        }

    @property
    def dst_ids_values_map(self) -> dict[int, Optional[Any]]:
        return dict(self._dst_to_values)

    def get_values_from_src_id(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Optional[Any]]:
        if type_filter is not None:
            return self._values_for_src_id(
                int(src_id),
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
        return self._cached_values_for_src(int(src_id))

    def get_value_from_dst_id(self, dst_id: int) -> Optional[Any]:
        return self._dst_to_values.get(int(dst_id))

    def get_dst_ids_from_src_id(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[int]:
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
        if type_filter is not None:
            src_ids = self._ordered_src_ids_for_dst(int(dst_id), type_filter=type_filter)
        else:
            src_ids = tuple(self._dst_to_src_ids.get(int(dst_id), ()))
        return src_ids[0] if src_ids else None

    def get_src_ids_from_value(self, value: Any) -> list[int]:
        return list(self._value_to_src_ids.get(value, ()))

    def get_dst_ids_from_value(self, value: Any) -> list[int]:
        return list(self._value_to_dst_ids.get(value, ()))

    def get_link_properties(
        self,
        src_id: int,
        dst_id: int,
    ) -> OneManyIndividualLinkProperties:
        return cast(
            OneManyIndividualLinkProperties,
            self._build_link_properties(OneManyIndividualLinkProperties, int(src_id), int(dst_id)),
        )

    def set_link_properties(
        self,
        updated_link_properties: OneManyIndividualLinkProperties,
    ) -> None:
        self._set_link_properties(updated_link_properties)

    def update(self, update: OneManyInTwoTableFieldUpdate[Any]) -> None:
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
    def __init__(
        self,
        cache: "NumpyVectorizedStorageCache",
        src_table: Union[StorageCacheSingleTableAPI, str],
        src_table_id_col: str,
        dst_table: Union[StorageCacheSingleTableAPI, str],
        dst_table_cache_col: str,
        db: Any,
    ) -> None:
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
        return cast(NumpyVectorizedLinkTable, self._cache.get_many_many_link_table(src_table, dst_table))

    @property
    def ids(self) -> set[int]:
        return set(self._src_ids)

    @property
    def values(self) -> list[Any]:
        return self._flattened_values()

    @property
    def values_set(self) -> set[Any]:
        return set(self.values)

    @property
    def ids_values_map(self) -> dict[int, Sequence[Optional[Any]]]:
        return {
            src_id: self._cached_values_for_src(src_id)
            for src_id in self._src_ids
        }

    @property
    def dst_ids_values_map(self) -> dict[int, Optional[Any]]:
        return dict(self._dst_to_values)

    def get_values_from_src_id(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Optional[Any]]:
        if type_filter is not None:
            return self._values_for_src_id(
                int(src_id),
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
        return self._cached_values_for_src(int(src_id))

    def get_value_from_dst_id(self, dst_id: int) -> Optional[Any]:
        return self._dst_to_values.get(int(dst_id))

    def get_dst_ids_from_src_id(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[int]:
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
        if require_ordering or type_filter is not None:
            return self._ordered_src_ids_for_dst(
                int(dst_id),
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
        return tuple(self._dst_to_src_ids.get(int(dst_id), ()))

    def get_src_ids_from_value(self, value: Any) -> list[int]:
        return list(self._value_to_src_ids.get(value, ()))

    def get_dst_ids_from_value(self, value: Any) -> list[int]:
        return list(self._value_to_dst_ids.get(value, ()))

    def get_link_properties(
        self,
        src_id: int,
        dst_id: int,
    ) -> ManyManyIndividualLinkProperties:
        return cast(
            ManyManyIndividualLinkProperties,
            self._build_link_properties(ManyManyIndividualLinkProperties, int(src_id), int(dst_id)),
        )

    def set_link_properties(
        self,
        updated_link_properties: ManyManyIndividualLinkProperties,
    ) -> None:
        self._set_link_properties(updated_link_properties)

    def update(self, update: ManyManyInTwoTableFieldUpdate[Any]) -> None:
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
    """Independent array-backed cache with NumPy-first read paths."""

    plugin_name = "numpy_vectorized"
    plugin_capabilities = StorageCacheCapabilities(
        live_reads=False,
        live_child_objects=False,
        vectorized_helpers=True,
        requires_reload_for_external_changes=True,
    )

    def __init__(self, db: Any, *, require_numpy: bool = True) -> None:
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
        return _np is not None

    @property
    def capabilities(self) -> StorageCacheCapabilities:
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
        return self._is_loaded

    @property
    def is_initialized(self) -> bool:
        return self._is_initialized

    def _require_db(self, db: Any = None) -> Any:
        resolved = _ensure_db(self.db, db)
        self.db = resolved
        return resolved

    def clear(self) -> None:
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
        self.clear()
        self.detach_db()

    def read(self, db: Any = None) -> None:
        self._require_db(db)
        self.clear()
        self.read_tables(self.db)
        self.initialize_tables(self.db)
        self.read_fields(self.db)
        self.initialize_fields(self.db)
        self._is_loaded = True
        self._is_initialized = True

    def reload(self, db: Any = None) -> None:
        self.read(db=db)

    def read_tables(self, db: Any = None) -> None:
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
        db = self._require_db(db)
        for table in self.main_tables.values():
            table.read(db)
        for table in self.link_tables.values():
            table.read(db)

    def read_fields(self, db: Any = None) -> None:
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
        db = self._require_db(db)
        for field in self._field_objects.values():
            field.read(db)

    def _resolve_field_name(self, name: Union[FieldKey, FieldBasicInterfaceAPI[Any]]) -> str:
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
        owner = getattr(field, "table_name", None)
        if owner is not None:
            return str(owner)
        owner = getattr(field, "src_table_name", None)
        if owner is not None:
            return str(owner)
        return None

    def _field_tables(self, field: FieldBasicInterfaceAPI[Any]) -> set[str]:
        tables: set[str] = set()
        for attr_name in ("table_name", "src_table_name", "dst_table_name"):
            table_name = getattr(field, attr_name, None)
            if table_name:
                tables.add(str(table_name))
        return tables

    def _field_link_key(self, field: FieldBasicInterfaceAPI[Any]) -> Optional[tuple[str, str]]:
        src_table_name = getattr(field, "src_table_name", None)
        dst_table_name = getattr(field, "dst_table_name", None)
        if src_table_name is None or dst_table_name is None:
            return None
        return (str(src_table_name), str(dst_table_name))

    def _ensure_main_table_fresh(self, table_name: str) -> None:
        if table_name in self._stale_main_tables or self._stale_ids.get(table_name):
            self.reload_main_table(table_name)

    def _ensure_link_table_fresh(self, key: tuple[str, str]) -> None:
        if key in self._stale_link_tables:
            self.reload_link_table(*key)

    def _ensure_field_fresh(self, field_name: str) -> None:
        field = self.fields[field_name]
        link_key = self._field_link_key(field)
        if (
            field_name in self._stale_fields
            or bool(self._field_tables(field) & self._stale_main_tables)
            or (link_key is not None and link_key in self._stale_link_tables)
        ):
            self.reload_field(field_name)

    def has_main_table(self, name: str) -> bool:
        return str(name) in self.main_tables

    def get_main_table(
        self,
        name: Union[str, StorageCacheSingleTableAPI],
    ) -> NumpyVectorizedMainTableCache:
        table_name = name.table if isinstance(name, StorageCacheSingleTableAPI) else str(name)
        self._ensure_main_table_fresh(table_name)
        return self.main_tables[table_name]

    def iter_main_tables(self) -> Iterable[NumpyVectorizedMainTableCache]:
        for table_name in sorted(self.main_tables):
            yield self.get_main_table(table_name)

    def get_table(self, name: str):
        if name in self.main_tables:
            return self.get_main_table(name)
        for key, table in self.link_tables.items():
            if table.table == name:
                self._ensure_link_table_fresh(key)
                return table
        raise KeyError(name)

    def iter_tables(self) -> Iterable[Any]:
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
        return self.get_link_table(src_table, dst_table, table_type=TableTypes.ONE_ONE)

    def get_one_many_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
    ) -> StorageCacheOneToManyLinkTable:
        return self.get_link_table(src_table, dst_table, table_type=TableTypes.ONE_MANY)

    def get_many_one_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
    ) -> StorageCacheManyToOneLinkTable:
        return self.get_link_table(src_table, dst_table, table_type=TableTypes.MANY_ONE)

    def get_many_many_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
    ) -> StorageCacheManyToManyLinkTable:
        return self.get_link_table(src_table, dst_table, table_type=TableTypes.MANY_MANY)

    def iter_link_tables(self) -> Iterable[NumpyVectorizedLinkTable]:
        for key in sorted(self.link_tables):
            yield self.get_link_table(*key)

    def has_field(self, name: FieldKey) -> bool:
        try:
            self._resolve_field_name(name)
        except KeyError:
            return False
        return True

    def get_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
    ) -> FieldBasicInterfaceAPI[Any]:
        field_name = self._resolve_field_name(name)
        self._ensure_field_fresh(field_name)
        return self.fields[field_name]

    def iter_fields(self) -> Iterable[FieldBasicInterfaceAPI[Any]]:
        for field_name in sorted(self._field_objects):
            yield self.get_field(field_name)

    def get_fields_for_table(
        self,
        table: Union[str, StorageCacheSingleTableAPI],
    ) -> Sequence[FieldBasicInterfaceAPI[Any]]:
        table_name = self.get_main_table(table).table
        return tuple(
            field for field in self.iter_fields() if self._field_owner_table(field) == table_name
        )

    def reload_main_table(
        self,
        name: Union[str, StorageCacheSingleTableAPI],
        db: Any = None,
    ) -> None:
        table = self.get_main_table(name if not isinstance(name, StorageCacheSingleTableAPI) else name.table)
        table.reload(self._require_db(db))
        self._stale_main_tables.discard(table.table)
        self._stale_ids.pop(table.table, None)
        for field in self._field_objects.values():
            if table.table in self._field_tables(field):
                field.read(self.db)
                self._stale_fields.discard(field.field_key)

    def reload_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
        db: Any = None,
        table_type: Optional[TableTypes] = None,
    ) -> None:
        table = self.get_link_table(src_table, dst_table, table_type=table_type)
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
        field_name = self._resolve_field_name(name)
        field = self._field_objects[field_name]
        field.read(self._require_db(db))
        self._stale_fields.discard(field.field_key)

    def invalidate_table(
        self,
        table: Union[str, StorageCacheSingleTableAPI],
    ) -> None:
        table_name = self.get_main_table(table).table
        self._stale_main_tables.add(table_name)

    def invalidate_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
        table_type: Optional[TableTypes] = None,
    ) -> None:
        table = self.get_link_table(src_table, dst_table, table_type=table_type)
        self._stale_link_tables.add((table.primary_table, table.secondary_table))

    def invalidate_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
    ) -> None:
        field_name = self._resolve_field_name(name)
        self._stale_fields.add(field_name)

    def invalidate_ids(
        self,
        table: Union[str, StorageCacheSingleTableAPI],
        ids: Iterable[int],
    ) -> None:
        table_name = self.get_main_table(table).table
        self._stale_ids[table_name].update(int(row_id) for row_id in ids)
        self._stale_main_tables.add(table_name)

    def get_numpy_row_id_array(self, table_name: str) -> Any:
        return self.get_main_table(table_name).row_id_array

    def get_numpy_field_owner_ids(self, field_name: str) -> Any:
        field = self.get_field(field_name)
        getter = getattr(field, "get_numpy_owner_ids_array", None)
        if callable(getter):
            return getter()
        raise KeyError(str(field_name))

    def get_numpy_field_array(self, field_name: str) -> Any:
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
        field = self.get_field(field_key)
        getter = getattr(field, "get_value_from_id", None)
        if callable(getter):
            value = getter(int(owner_id))
            return default_value if value is None else value
        getter = getattr(field, "get_value_from_src_id", None)
        if callable(getter):
            value = getter(int(owner_id))
            return default_value if value is None else value
        return super().get_cached_value(owner_id, field_key, default_value=default_value)

    def get_cached_row_values(
        self,
        owner_id: int,
        field_keys: tuple[str, ...] | list[str],
        default_value: Any = None,
    ) -> tuple[Any, ...]:
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
