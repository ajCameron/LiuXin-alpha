"""
Concrete schema-backed implementation of one-to-one storage fields.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable, Optional, Union, cast

from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.one_one_field import (
    CacheOneOneInTwoTableFieldAPI,
    CacheOneOneInSameTableFieldAPI,
    OneOneInTwoTableFieldUpdate,
    OneOneInOneTableFieldUpdate,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.single_table import (
    StorageCacheSingleTableAPI,
)

from LiuXin_alpha.caches.cache_plugins.schema_backed.common import (
    _canonical_field_key,
    _ensure_db,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.relation_base import (
    _SchemaBackedRelationFieldBase,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.link_tables.link_table import (
    SchemaBackedLinkTable,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.single_table import (
    SchemaBackedMainTableCache,
)

if TYPE_CHECKING:
    from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_cache import SchemaBackedStorageCache


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
        # The table cache already owns a column index. Re-reading and
        # deep-copying every complete row once per field makes cache startup
        # quadratic in the number of columns for wide schema tables.
        self._ids_values_map = dict(
            zip(
                table.row_ids,
                table.get_values_for(self.column_name),
                strict=True,
            )
        )

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
        self.refresh_from_table(ids)

    def refresh_from_table(self, ids: Iterable[int]) -> None:
        """Refresh selected values after the owning table was refreshed."""

        ids = {int(row_id) for row_id in ids}
        table = self._table_cache()
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


class SchemaBackedTwoTableOneOneField(
    _SchemaBackedRelationFieldBase[Any],
    CacheOneOneInTwoTableFieldAPI[Any],
):
    """
    Field wrapper over a one-to-one relation.
    """

    def __init__(
        self,
        cache: "SchemaBackedStorageCache",
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
    ) -> SchemaBackedLinkTable:
        return cast(SchemaBackedLinkTable, self._cache.get_one_one_link_table(src_table, dst_table))

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

    @property
    def ids(self) -> set[int]:
        return set(self._src_to_values.keys())

    @property
    def values(self) -> list[Any]:
        return self._flattened_values()

    @property
    def values_set(self) -> set[Any]:
        return set(self.values)

    @property
    def ids_values_map(self) -> dict[int, Optional[Any]]:
        return {
            src_id: values[0] if values else None
            for src_id, values in self._src_to_values.items()
        }

    @property
    def dst_ids_values_map(self) -> dict[int, Optional[Any]]:
        return dict(self._dst_to_values)

    def get_value_from_src_id(self, src_id: int) -> Optional[Any]:
        return self._single_value_for_src_id(int(src_id))

    def get_value_from_dst_id(self, dst_id: int) -> Optional[Any]:
        return self._value_for_dst_id(int(dst_id))

    def get_dst_id_from_src_id(self, src_id: int) -> Optional[int]:
        dst_ids = self._ordered_dst_ids_for_src(int(src_id))
        return dst_ids[0] if dst_ids else None

    def get_src_id_from_dst_id(self, dst_id: int) -> Optional[int]:
        src_ids = self._ordered_src_ids_for_dst(int(dst_id))
        return src_ids[0] if src_ids else None

    def get_src_ids_from_value(self, value: Any) -> list[int]:
        src_ids: list[int] = []
        for dst_id, dst_value in sorted(self._dst_to_values.items()):
            if dst_value == value:
                src_ids.extend(self._dst_to_src_ids.get(dst_id, ()))
        return sorted(src_ids)

    def get_dst_ids_from_value(self, value: Any) -> list[int]:
        return sorted(
            dst_id
            for dst_id, dst_value in self._dst_to_values.items()
            if dst_value == value
        )


__all__ = [
    "SchemaBackedSameTableField",
    "SchemaBackedTwoTableOneOneField",
]
