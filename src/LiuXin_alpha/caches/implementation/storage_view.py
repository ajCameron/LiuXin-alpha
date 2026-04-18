"""
Concrete schema-backed implementation of the storage view API.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable, Iterator, Optional, Sequence, cast

from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.base_field import (
    FieldBasicInterfaceAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_view_api import (
    CacheViewAPI,
    CacheViewColumnSpec,
    CacheViewRowAPI,
    CacheViewSortSpec,
    CacheViewSpec,
    CacheViewState,
)

from LiuXin_alpha.caches.implementation.common import _sort_key
from LiuXin_alpha.caches.implementation.storage_fields.one_one_field import (
    SchemaBackedSameTableField,
)
from LiuXin_alpha.caches.implementation.storage_tables.single_table import (
    SchemaBackedMainTableCache,
)

if TYPE_CHECKING:
    from LiuXin_alpha.caches.implementation.storage_cache import SchemaBackedStorageCache


class SchemaBackedCacheViewRow(CacheViewRowAPI):
    """
    Row wrapper over one cache-view id.
    """

    def __init__(self, view: "SchemaBackedCacheView", row_id: int) -> None:
        self._view = view
        self.row_id = int(row_id)
        self.column_count = view.column_count

    def __getitem__(self, obj: int | slice) -> Any:
        values = self._view.row_values_for_id(self.row_id)
        return values[obj]

    def __len__(self) -> int:
        return self.column_count

    def __iter__(self) -> Iterator[Any]:
        yield from self._view.row_values_for_id(self.row_id)


class SchemaBackedCacheView(CacheViewAPI):
    """
    Minimal in-memory ordered view over cached fields.
    """

    def __init__(
        self,
        cache: "SchemaBackedStorageCache",
        spec: CacheViewSpec,
    ) -> None:
        self._cache = cache
        self.spec = spec
        self.state = CacheViewState()
        self._field_map: dict[int, str] = {}
        self._fields: dict[str, SchemaBackedSameTableField] = {}
        self.refresh()

    @property
    def field_map(self) -> dict[int, str]:
        return dict(self._field_map)

    @property
    def fields(self) -> dict[str, FieldBasicInterfaceAPI[Any]]:
        return dict(self._fields)

    def _visible_columns(self) -> tuple[CacheViewColumnSpec, ...]:
        if self.spec.columns:
            return tuple(column for column in self.spec.columns if column.visible)

        table_fields = [
            field
            for field in self._cache.get_fields_for_table(self.spec.base_table)
            if isinstance(field, SchemaBackedSameTableField)
        ]
        return tuple(
            CacheViewColumnSpec(key=field.field_key, label=field.field_key, field_key=field.field_key)
            for field in table_fields
        )

    def _refresh_field_map(self) -> None:
        self._field_map = {}
        self._fields = {}
        for index, column in enumerate(self._visible_columns()):
            field_key = column.field_key or column.key
            field = cast(SchemaBackedSameTableField, self._cache.get_field(field_key))
            self._field_map[index] = field_key
            self._fields[field_key] = field

    def _matches_restriction(self, row_id: int, restriction: str) -> bool:
        text = restriction.strip().lower()
        if not text:
            return True
        for field_key in self._field_map.values():
            value = self.value_for(row_id, field_key, default_value="")
            if text in str(value).lower():
                return True
        return False

    def count(self) -> int:
        return len(self.state.visible_ids)

    def has_id(self, row_id: int) -> bool:
        return int(row_id) in self.state.visible_ids

    def iter_all_ids(self) -> Iterator[int]:
        yield from self.state.all_ids

    def iter_visible_ids(self) -> Iterator[int]:
        yield from self.state.visible_ids

    def row_for_id(self, row_id: int) -> CacheViewRowAPI:
        if not self.has_id(row_id):
            raise KeyError(row_id)
        return SchemaBackedCacheViewRow(self, row_id)

    def index_to_id(self, index: int) -> int:
        return self.state.visible_ids[index]

    def id_to_index(self, row_id: int) -> int:
        return self.state.visible_ids.index(int(row_id))

    def __getitem__(self, index: int) -> CacheViewRowAPI:
        return self.row_for_id(self.index_to_id(index))

    def __len__(self) -> int:
        return self.count()

    def __iter__(self) -> Iterator[CacheViewRowAPI]:
        for row_id in self.state.visible_ids:
            yield self.row_for_id(row_id)

    def value_for(
        self,
        row_id: int,
        field_key: str,
        default_value: Any = None,
    ) -> Any:
        field = cast(SchemaBackedSameTableField, self.get_field(field_key))
        value = field.get_value_from_id(int(row_id))
        if value is None:
            return default_value
        return value

    def value_for_index(
        self,
        index: int,
        field_key: str,
        default_value: Any = None,
    ) -> Any:
        return self.value_for(self.index_to_id(index), field_key, default_value=default_value)

    def row_values_for_id(self, row_id: int) -> Sequence[Any]:
        return tuple(self.value_for(row_id, field_key) for field_key in self._field_map.values())

    def get_field(self, key_or_index: str | int) -> FieldBasicInterfaceAPI[Any]:
        if isinstance(key_or_index, int):
            field_key = self._field_map[key_or_index]
            return self._fields[field_key]
        return self._fields[key_or_index]

    def sort(self, fields: Sequence[CacheViewSortSpec], subsort: bool = False) -> None:
        del subsort
        ids = list(self.state.visible_ids)
        for sort_spec in reversed(tuple(fields)):
            ids.sort(
                key=lambda row_id: _sort_key(self.value_for(row_id, sort_spec.key)),
                reverse=not sort_spec.ascending,
            )
        self.state.visible_ids = tuple(ids)
        self.state.sort_history = list(fields)
        self.state.full_map_is_sorted = True

    def search(self, query: str, return_ids: bool = False) -> Optional[tuple[int, ...]]:
        matches = tuple(
            row_id
            for row_id in self.state.visible_ids
            if self._matches_restriction(row_id, query)
        )
        if return_ids:
            return matches
        self.state.search_restriction = query
        self.state.search_restriction_count = len(matches)
        self.state.visible_ids = matches
        return None

    def set_search_restriction(self, restriction: str, name: str = "") -> None:
        self.state.search_restriction = restriction
        self.state.search_restriction_name = name
        self.refresh()

    def set_base_restriction(self, restriction: str, name: str = "") -> None:
        self.state.base_restriction = restriction
        self.state.base_restriction_name = name
        self.refresh()

    def search_restriction_applied(self) -> bool:
        return bool(self.state.search_restriction or self.state.base_restriction)

    def refresh(self, field: Optional[str] = None, ascending: bool = True) -> None:
        self._refresh_field_map()
        base_table = cast(SchemaBackedMainTableCache, self._cache.get_main_table(self.spec.base_table))
        ids = list(base_table.row_ids)

        if self.state.base_restriction:
            ids = [row_id for row_id in ids if self._matches_restriction(row_id, self.state.base_restriction)]

        if self.state.search_restriction:
            ids = [row_id for row_id in ids if self._matches_restriction(row_id, self.state.search_restriction)]
            self.state.search_restriction_count = len(ids)
        else:
            self.state.search_restriction_count = 0

        self.state.all_ids = tuple(base_table.row_ids)
        self.state.visible_ids = tuple(ids)

        if field is not None:
            self.sort((CacheViewSortSpec(field, ascending),))
        elif self.spec.default_sort:
            self.sort(self.spec.default_sort)

    def refresh_ids(self, ids: Iterable[int]) -> None:
        refreshed = {int(row_id) for row_id in ids}
        visible = [row_id for row_id in self.state.visible_ids if row_id not in refreshed]
        for row_id in refreshed:
            if row_id in self.state.all_ids:
                visible.append(row_id)
        self.state.visible_ids = tuple(visible)
        if self.state.sort_history:
            self.sort(self.state.sort_history)

    def remove_ids(self, ids: Iterable[int]) -> None:
        removed = {int(row_id) for row_id in ids}
        self.state.all_ids = tuple(row_id for row_id in self.state.all_ids if row_id not in removed)
        self.state.visible_ids = tuple(row_id for row_id in self.state.visible_ids if row_id not in removed)

    def add_ids(self, ids: Iterable[int]) -> None:
        additions = [int(row_id) for row_id in ids if int(row_id) not in self.state.all_ids]
        self.state.all_ids = tuple(list(self.state.all_ids) + additions)
        self.state.visible_ids = tuple(list(self.state.visible_ids) + additions)
        if self.state.sort_history:
            self.sort(self.state.sort_history)


__all__ = ["SchemaBackedCacheView", "SchemaBackedCacheViewRow"]
