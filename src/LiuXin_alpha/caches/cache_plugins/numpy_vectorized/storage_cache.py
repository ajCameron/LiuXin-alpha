"""Numpy-oriented cache plugin built on top of the schema-backed cache."""

from __future__ import annotations

from typing import Any

from LiuXin_alpha.caches.api.storage_cache_api.storage_cache_api import (
    StorageCacheCapabilities,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.base_field import (
    FieldBasicInterfaceAPI,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed import (
    SchemaBackedSameTableField,
    SchemaBackedStorageCache,
)

try:
    import numpy as _np
except Exception:  # pragma: no cover - availability is environment-specific
    _np = None


class NumpyVectorizedStorageCache(SchemaBackedStorageCache):
    """
    Schema-backed cache with numpy-built owner-id and value arrays.

    This is intentionally a first plugin step, not a second full cache stack:
    it reuses the schema-backed cache semantics and layers vectorizable arrays
    over the loaded cache state for read-heavy or analysis-heavy callers.
    """

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
        self._table_row_id_arrays: dict[str, Any] = {}
        self._table_row_id_positions: dict[str, dict[int, int]] = {}
        self._field_owner_id_arrays: dict[str, Any] = {}
        self._field_owner_id_positions: dict[str, dict[int, int]] = {}
        self._field_value_arrays: dict[str, Any] = {}
        super().__init__(db)

    @classmethod
    def numpy_available(cls) -> bool:
        return _np is not None

    def clear(self) -> None:
        self._table_row_id_arrays = {}
        self._table_row_id_positions = {}
        self._field_owner_id_arrays = {}
        self._field_owner_id_positions = {}
        self._field_value_arrays = {}
        super().clear()

    def read(self, db: Any = None) -> None:
        super().read(db=db)
        self._build_numpy_arrays()

    def reload(self, db: Any = None) -> None:
        self.read(db=db)

    def _build_numpy_arrays(self) -> None:
        if _np is None:
            self._table_row_id_arrays = {}
            self._table_row_id_positions = {}
            self._field_owner_id_arrays = {}
            self._field_owner_id_positions = {}
            self._field_value_arrays = {}
            return

        table_row_id_arrays: dict[str, Any] = {}
        table_row_id_positions: dict[str, dict[int, int]] = {}
        for table_name, table in self.main_tables.items():
            owner_ids = tuple(int(row_id) for row_id in table.row_ids)
            table_row_id_arrays[table_name] = _np.asarray(owner_ids, dtype=_np.int64)
            table_row_id_positions[table_name] = {
                row_id: index for index, row_id in enumerate(owner_ids)
            }

        field_owner_id_arrays: dict[str, Any] = {}
        field_owner_id_positions: dict[str, dict[int, int]] = {}
        field_value_arrays: dict[str, Any] = {}
        for field_key, field in self._field_objects.items():
            owner_ids = self._owner_ids_for_field(field)
            values = self._values_for_field(field, owner_ids)
            field_owner_id_arrays[field_key] = _np.asarray(owner_ids, dtype=_np.int64)
            field_owner_id_positions[field_key] = {
                row_id: index for index, row_id in enumerate(owner_ids)
            }
            field_value_arrays[field_key] = _np.asarray(values, dtype=object)

        self._table_row_id_arrays = table_row_id_arrays
        self._table_row_id_positions = table_row_id_positions
        self._field_owner_id_arrays = field_owner_id_arrays
        self._field_owner_id_positions = field_owner_id_positions
        self._field_value_arrays = field_value_arrays

    def _owner_ids_for_field(self, field: FieldBasicInterfaceAPI[Any]) -> tuple[int, ...]:
        if isinstance(field, SchemaBackedSameTableField):
            table = self.get_main_table(field.table_name)
            return tuple(int(row_id) for row_id in table.row_ids)
        return tuple(sorted(int(row_id) for row_id in getattr(field, "ids", set())))

    def _values_for_field(
        self,
        field: FieldBasicInterfaceAPI[Any],
        owner_ids: tuple[int, ...],
    ) -> tuple[Any, ...]:
        if isinstance(field, SchemaBackedSameTableField):
            return tuple(field.get_value_from_id(row_id) for row_id in owner_ids)

        ids_values_map = getattr(field, "ids_values_map", {})
        return tuple(ids_values_map.get(row_id) for row_id in owner_ids)

    def get_numpy_row_id_array(self, table_name: str) -> Any:
        return self._table_row_id_arrays[str(table_name)]

    def get_numpy_field_owner_ids(self, field_name: str) -> Any:
        field_key = self._resolve_field_name(field_name)
        return self._field_owner_id_arrays[field_key]

    def get_numpy_field_array(self, field_name: str) -> Any:
        field_key = self._resolve_field_name(field_name)
        return self._field_value_arrays[field_key]

    def _normalize_numpy_scalar(self, value: Any) -> Any:
        if _np is not None and isinstance(value, _np.generic):
            return value.item()
        return value

    def get_cached_value(
        self,
        owner_id: int,
        field_key: str,
        default_value: Any = None,
    ) -> Any:
        field = self.get_field(field_key)
        resolved_field_key = str(field.field_key)
        if isinstance(field, SchemaBackedSameTableField):
            positions = self._field_owner_id_positions.get(resolved_field_key)
            values = self._field_value_arrays.get(resolved_field_key)
            if positions is not None and values is not None:
                position = positions.get(int(owner_id))
                if position is None:
                    return default_value
                value = self._normalize_numpy_scalar(values[position])
                return default_value if value is None else value
        return super().get_cached_value(owner_id, resolved_field_key, default_value=default_value)

    def get_cached_row_values(
        self,
        owner_id: int,
        field_keys: tuple[str, ...] | list[str],
        default_value: Any = None,
    ) -> tuple[Any, ...]:
        resolved_fields = tuple(self.get_field(field_key) for field_key in field_keys)
        resolved_field_keys = tuple(str(field.field_key) for field in resolved_fields)
        if not resolved_field_keys:
            return ()

        table_name: str | None = None
        for field in resolved_fields:
            if not isinstance(field, SchemaBackedSameTableField):
                return super().get_cached_row_values(
                    owner_id,
                    resolved_field_keys,
                    default_value=default_value,
                )
            if table_name is None:
                table_name = field.table_name
                continue
            if field.table_name != table_name:
                return super().get_cached_row_values(
                    owner_id,
                    resolved_field_keys,
                    default_value=default_value,
                )

        if table_name is None:
            return ()

        positions = self._table_row_id_positions.get(table_name)
        if positions is None:
            return super().get_cached_row_values(
                owner_id,
                resolved_field_keys,
                default_value=default_value,
            )

        position = positions.get(int(owner_id))
        if position is None:
            return tuple(default_value for _field_key in resolved_field_keys)

        values: list[Any] = []
        for field_key in resolved_field_keys:
            raw_value = self._field_value_arrays[field_key][position]
            value = self._normalize_numpy_scalar(raw_value)
            values.append(default_value if value is None else value)
        return tuple(values)


StorageCache = NumpyVectorizedStorageCache

__all__ = [
    "NumpyVectorizedStorageCache",
    "StorageCache",
]
