"""
Concrete schema-backed implementation of the top-level storage cache API.
"""

from __future__ import annotations

from collections import defaultdict

from typing import Any, Iterable, Optional, Sequence, Union, cast

from LiuXin_alpha.caches.api.storage_cache_api.storage_cache_api import (
    FieldKey,
    StorageCacheAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.base_field import (
    FieldBasicInterfaceAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.base_table import (
    StorageCacheBaseTableAPI,
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

from LiuXin_alpha.caches.implementation.common import _ensure_db
from LiuXin_alpha.caches.implementation.storage_fields.one_one_field import (
    SchemaBackedSameTableField,
)
from LiuXin_alpha.caches.implementation.storage_tables.link_tables.link_table import (
    SchemaBackedLinkTable,
)
from LiuXin_alpha.caches.implementation.storage_tables.single_table import (
    SchemaBackedMainTableCache,
)


class SchemaBackedStorageCache(StorageCacheAPI):
    """
    Concrete storage cache built from live database schema introspection.
    """

    def __init__(self, db: Any) -> None:
        super().__init__(db)
        self.main_tables: dict[str, SchemaBackedMainTableCache] = {}
        self.link_tables: dict[tuple[str, str], SchemaBackedLinkTable] = {}
        self.fields: dict[str, SchemaBackedSameTableField] = {}
        self._field_objects: dict[str, SchemaBackedSameTableField] = {}
        self._schema = None
        self._is_loaded = False
        self._is_initialized = False
        self._stale_main_tables: set[str] = set()
        self._stale_link_tables: set[tuple[str, str]] = set()
        self._stale_fields: set[str] = set()
        self._stale_ids: dict[str, set[int]] = defaultdict(set)

    @property
    def is_loaded(self) -> bool:
        return self._is_loaded

    @property
    def is_initialized(self) -> bool:
        return self._is_initialized

    def _require_db(self, db: Any = None):
        resolved = _ensure_db(self.db, db)
        self.db = resolved
        return resolved

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
            field._db = None
        return old_db

    def close(self) -> None:
        self.clear()
        self.detach_db()

    def read_tables(self, db: Any = None) -> None:
        db = self._require_db(db)
        schema = db.driver_wrapper.get_schema_spec(force_refresh=True)
        self._schema = schema
        self.main_tables = {
            table_name: SchemaBackedMainTableCache(spec, db)
            for table_name, spec in schema.tables.items()
            if spec.is_main_table
        }

        link_tables: dict[tuple[str, str], SchemaBackedLinkTable] = {}
        for link_spec in schema.interlinks + schema.intralinks:
            src_table = self.main_tables.get(link_spec.primary_table)
            dst_table = self.main_tables.get(link_spec.secondary_table)
            if src_table is None or dst_table is None:
                continue

            forward = SchemaBackedLinkTable(
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
                reverse = SchemaBackedLinkTable(
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
        field_objects: dict[str, SchemaBackedSameTableField] = {}
        raw_name_counts: dict[str, int] = defaultdict(int)
        for table_name, table in self.main_tables.items():
            for column in table.column_headings:
                field = SchemaBackedSameTableField(self, table_name, column, db)
                field_objects[field.field_key] = field
                raw_name_counts[column] += 1
        fields: dict[str, SchemaBackedSameTableField] = dict(field_objects)
        for field in field_objects.values():
            if raw_name_counts[field.column_name] == 1:
                fields[field.column_name] = field
        self._field_objects = field_objects
        self.fields = fields

    def initialize_fields(self, db: Any = None) -> None:
        db = self._require_db(db)
        for field in self._field_objects.values():
            field.read(db)

    def _resolve_field_name(self, name: Union[FieldKey, FieldBasicInterfaceAPI[Any]]) -> str:
        if isinstance(name, SchemaBackedSameTableField):
            return name.field_key
        requested = str(name)
        if requested in self.fields:
            return requested
        if "." not in requested:
            matches = [
                field.field_key
                for field in self._field_objects.values()
                if field.column_name == requested
            ]
            if len(matches) == 1:
                return matches[0]
        raise KeyError(requested)

    def _ensure_main_table_fresh(self, table_name: str) -> None:
        if table_name in self._stale_main_tables or self._stale_ids.get(table_name):
            self.reload_main_table(table_name)

    def _ensure_link_table_fresh(self, key: tuple[str, str]) -> None:
        if key in self._stale_link_tables:
            self.reload_link_table(*key)

    def _ensure_field_fresh(self, field_name: str) -> None:
        field = self.fields[field_name]
        if field_name in self._stale_fields or field.table_name in self._stale_main_tables:
            self.reload_field(field_name)

    def has_main_table(self, name: str) -> bool:
        return str(name) in self.main_tables

    def get_main_table(
        self,
        name: Union[str, StorageCacheSingleTableAPI],
    ) -> SchemaBackedMainTableCache:
        table_name = name.table if isinstance(name, StorageCacheSingleTableAPI) else str(name)
        self._ensure_main_table_fresh(table_name)
        return self.main_tables[table_name]

    def iter_main_tables(self) -> Iterable[SchemaBackedMainTableCache]:
        for table_name in sorted(self.main_tables):
            yield self.get_main_table(table_name)

    def get_table(self, name: str) -> StorageCacheBaseTableAPI:
        if name in self.main_tables:
            return self.get_main_table(name)
        for key, table in self.link_tables.items():
            if table.table == name:
                self._ensure_link_table_fresh(key)
                return table
        raise KeyError(name)

    def iter_tables(self) -> Iterable[StorageCacheBaseTableAPI]:
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
    ) -> SchemaBackedLinkTable:
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

    def iter_link_tables(self) -> Iterable[SchemaBackedLinkTable]:
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
            field for field in self.iter_fields() if cast(SchemaBackedSameTableField, field).table_name == table_name
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
            if field.table_name == table.table:
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
        self._stale_link_tables.discard((table.primary_table, table.secondary_table))

    def reload_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
        db: Any = None,
    ) -> None:
        field = cast(SchemaBackedSameTableField, self.get_field(name))
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


__all__ = ["SchemaBackedStorageCache"]
