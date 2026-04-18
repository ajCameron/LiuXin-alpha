"""
Concrete schema-backed implementation of one-to-one storage fields.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Union

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

    def update(self, update: OneOneInOneTableFieldUpdate[Any]) -> None:
        if update.added_maps:
            self.in_table.create(
                update.added_maps,
                self._db,
                target_column=self.column_name,
            )
        if update.updated_maps:
            self.in_table.update(
                update.updated_maps,
                self._db,
                target_column=self.column_name,
            )
        if update.deleted_ids:
            self.in_table.delete(update.deleted_ids, self._db)
        self.read(self._db)

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
