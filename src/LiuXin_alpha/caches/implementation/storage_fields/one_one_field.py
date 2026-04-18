"""
Concrete schema-backed implementation of one-to-one storage fields.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable, Optional, Union, cast

from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.one_one_field import (
    CacheOneOneInSameTableFieldAPI,
    OneOneInOneTableFieldUpdate,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.single_table import (
    StorageCacheSingleTableAPI,
)

from LiuXin_alpha.caches.implementation.common import (
    _canonical_field_key,
    _ensure_db,
)
from LiuXin_alpha.caches.implementation.storage_tables.single_table import (
    SchemaBackedMainTableCache,
)

if TYPE_CHECKING:
    from LiuXin_alpha.caches.implementation.storage_cache import SchemaBackedStorageCache


class SchemaBackedSameTableField(CacheOneOneInSameTableFieldAPI[Any]):
    """
    Simple field wrapper over one cached main-table column.
    """

    def __init__(
        self,
        cache: "SchemaBackedStorageCache",
        in_table: Union[StorageCacheSingleTableAPI, str],
        column_name: str,
        db: Any,
    ) -> None:
        self._cache = cache
        self.column_name = column_name
        self._ids_values_map: dict[int, Any] = {}
        super().__init__(in_table=in_table, db=db)

    @property
    def field_key(self) -> str:
        return _canonical_field_key(self.table_name, self.column_name)

    @property
    def table_name(self) -> str:
        return self.in_table.table

    def get_main_table(
        self,
        name: Union[str, StorageCacheSingleTableAPI],
    ) -> StorageCacheSingleTableAPI:
        return self._cache.get_main_table(name)

    def read(self, db: Any) -> None:
        db = _ensure_db(self._db, db)
        self._db = db
        table = self.in_table
        self._ids_values_map = {
            row_id: table.get_row_snapshot(row_id).get(self.column_name)
            for row_id in table.row_ids
        }

    def _table_cache(self) -> SchemaBackedMainTableCache:
        return cast(SchemaBackedMainTableCache, self.in_table)

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

    def update(self, update: OneOneInOneTableFieldUpdate[Any]) -> None:
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
        db = _ensure_db(self._db, db)
        self._db = db
        ids = {int(row_id) for row_id in ids}
        table = self._table_cache()
        table._refresh_ids(ids)
        for row_id in ids:
            if table.has_id(row_id):
                self._ids_values_map[row_id] = table.get_row_snapshot(row_id).get(self.column_name)
            else:
                self._ids_values_map.pop(row_id, None)

    def remove_ids(self, ids: Iterable[int]) -> None:
        for row_id in {int(row_id) for row_id in ids}:
            self._ids_values_map.pop(row_id, None)

    @property
    def ids(self) -> set[int]:
        return set(self._ids_values_map.keys())

    @property
    def values(self) -> list[Any]:
        return [self._ids_values_map[row_id] for row_id in sorted(self._ids_values_map)]

    @property
    def values_set(self) -> set[Any]:
        return set(self._ids_values_map.values())

    @property
    def ids_values_map(self) -> dict[int, Optional[Any]]:
        return dict(self._ids_values_map)

    def get_value_from_id(self, table_id: int) -> Optional[Any]:
        return self._ids_values_map.get(int(table_id))

    def get_ids_from_value(self, value: Any) -> list[int]:
        return sorted(row_id for row_id, row_value in self._ids_values_map.items() if row_value == value)


__all__ = ["SchemaBackedSameTableField"]
