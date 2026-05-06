"""
Concrete schema-backed implementation of the top-level storage cache API.
"""

from __future__ import annotations

from collections import defaultdict

from typing import Any, Iterable, Optional, Sequence, Union, cast

from LiuXin_alpha.caches.api.storage_cache_api.storage_cache_api import (
    FieldKey,
    StorageCacheAPI,
    StorageCacheCapabilities,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.base_field import (
    FieldBasicInterfaceAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.base_table import (
    StorageCacheBaseTableAPI,
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

from LiuXin_alpha.caches.cache_plugins.schema_backed.common import _ensure_db
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.many_many_field import (
    SchemaBackedManyManyField,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.many_one_field import (
    SchemaBackedManyOneField,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.one_one_field import (
    SchemaBackedSameTableField,
    SchemaBackedTwoTableOneOneField,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.one_many_field import (
    SchemaBackedOneManyField,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.link_tables.link_table import (
    SchemaBackedLinkTable,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.single_table import (
    SchemaBackedMainTableCache,
)


class SchemaBackedStorageCache(StorageCacheAPI):
    """
    Concrete storage cache built from live database schema introspection.
    """

    plugin_name = "schema_backed"
    plugin_capabilities = StorageCacheCapabilities(
        live_reads=False,
        live_child_objects=False,
        vectorized_helpers=False,
        requires_reload_for_external_changes=True,
    )

    def __init__(self, db: Any) -> None:
        super().__init__(db)
        self.main_tables: dict[str, SchemaBackedMainTableCache] = {}
        self.link_tables: dict[tuple[str, str], SchemaBackedLinkTable] = {}
        self.fields: dict[str, FieldBasicInterfaceAPI[Any]] = {}
        self._field_objects: dict[str, FieldBasicInterfaceAPI[Any]] = {}
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
        field_objects: dict[str, FieldBasicInterfaceAPI[Any]] = {}
        raw_name_counts: dict[str, int] = defaultdict(int)
        for table_name, table in self.main_tables.items():
            for column in table.column_headings:
                field = SchemaBackedSameTableField(self, table_name, column, db)
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
                    field: FieldBasicInterfaceAPI[Any] = SchemaBackedTwoTableOneOneField(
                        self,
                        src_table_name,
                        self.main_tables[src_table_name].id_column,
                        dst_table_name,
                        column,
                        db,
                    )
                elif link_table.table_type == TableTypes.ONE_MANY:
                    field = SchemaBackedOneManyField(
                        self,
                        src_table_name,
                        self.main_tables[src_table_name].id_column,
                        dst_table_name,
                        column,
                        db,
                    )
                elif link_table.table_type == TableTypes.MANY_ONE:
                    field = SchemaBackedManyOneField(
                        self,
                        src_table_name,
                        self.main_tables[src_table_name].id_column,
                        dst_table_name,
                        column,
                        db,
                    )
                else:
                    field = SchemaBackedManyManyField(
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
                isinstance(field, SchemaBackedSameTableField)
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


__all__ = ["SchemaBackedStorageCache"]
