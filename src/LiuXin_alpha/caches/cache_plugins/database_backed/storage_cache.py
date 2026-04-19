"""Storage-cache plugin that proxies reads through to the live database."""

from __future__ import annotations

from typing import Any, Iterable, Optional, Sequence, Union

from LiuXin_alpha.caches.api.storage_cache_api.storage_cache_api import FieldKey
from LiuXin_alpha.caches.api.storage_cache_api.storage_cache_api import (
    StorageCacheCapabilities,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.base_field import (
    FieldBasicInterfaceAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.base_table import (
    StorageCacheBaseTableAPI,
    TableTypes,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.single_table import (
    StorageCacheSingleTableAPI,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_cache import (
    SchemaBackedStorageCache,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.many_many_field import (
    SchemaBackedManyManyField,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.many_one_field import (
    SchemaBackedManyOneField,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.one_many_field import (
    SchemaBackedOneManyField,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.one_one_field import (
    SchemaBackedSameTableField,
    SchemaBackedTwoTableOneOneField,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.link_tables.link_table import (
    SchemaBackedLinkTable,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.single_table import (
    SchemaBackedMainTableCache,
)


class _LiveDelegatingProxyMixin:
    _LIVE_PROXY_INTERNALS = {
        "_live_cache",
        "_live_key",
        "_init_live_proxy",
        "_get_live_target",
        "__dict__",
        "__class__",
        "__weakref__",
        "__getattribute__",
        "__setattr__",
        "__repr__",
        "__iter__",
        "__len__",
        "__getitem__",
    }

    def _init_live_proxy(self, cache: "DatabaseBackedStorageCache", key: Any) -> None:
        object.__setattr__(self, "_live_cache", cache)
        object.__setattr__(self, "_live_key", key)

    def _get_live_target(self) -> Any:
        raise NotImplementedError

    def __getattribute__(self, name: str) -> Any:
        if name in _LiveDelegatingProxyMixin._LIVE_PROXY_INTERNALS or (
            name.startswith("__") and name.endswith("__")
        ):
            return object.__getattribute__(self, name)
        target = object.__getattribute__(self, "_get_live_target")()
        return getattr(target, name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name in _LiveDelegatingProxyMixin._LIVE_PROXY_INTERNALS or (
            name.startswith("__") and name.endswith("__")
        ):
            object.__setattr__(self, name, value)
            return
        setattr(object.__getattribute__(self, "_get_live_target")(), name, value)

    def __repr__(self) -> str:
        target = object.__getattribute__(self, "_get_live_target")()
        return repr(target)

    def __iter__(self):
        return iter(object.__getattribute__(self, "_get_live_target")())

    def __len__(self) -> int:
        return len(object.__getattribute__(self, "_get_live_target")())

    def __getitem__(self, key: Any) -> Any:
        return object.__getattribute__(self, "_get_live_target")()[key]


class _LiveMainTableProxy(_LiveDelegatingProxyMixin, SchemaBackedMainTableCache):
    def __init__(self, cache: "DatabaseBackedStorageCache", table_name: str) -> None:
        self._init_live_proxy(cache, str(table_name))

    def _get_live_target(self) -> SchemaBackedMainTableCache:
        return self._live_cache._current_main_table_target(self._live_key)


class _LiveLinkTableProxy(_LiveDelegatingProxyMixin, SchemaBackedLinkTable):
    def __init__(
        self,
        cache: "DatabaseBackedStorageCache",
        key: tuple[str, str],
    ) -> None:
        self._init_live_proxy(cache, key)

    def _get_live_target(self) -> SchemaBackedLinkTable:
        return self._live_cache._current_link_table_target(self._live_key)


class _LiveSameTableFieldProxy(_LiveDelegatingProxyMixin, SchemaBackedSameTableField):
    def __init__(self, cache: "DatabaseBackedStorageCache", field_key: str) -> None:
        self._init_live_proxy(cache, str(field_key))

    def _get_live_target(self) -> SchemaBackedSameTableField:
        return self._live_cache._current_field_target(self._live_key)


class _LiveTwoTableOneOneFieldProxy(
    _LiveDelegatingProxyMixin,
    SchemaBackedTwoTableOneOneField,
):
    def __init__(self, cache: "DatabaseBackedStorageCache", field_key: str) -> None:
        self._init_live_proxy(cache, str(field_key))

    def _get_live_target(self) -> SchemaBackedTwoTableOneOneField:
        return self._live_cache._current_field_target(self._live_key)


class _LiveOneManyFieldProxy(_LiveDelegatingProxyMixin, SchemaBackedOneManyField):
    def __init__(self, cache: "DatabaseBackedStorageCache", field_key: str) -> None:
        self._init_live_proxy(cache, str(field_key))

    def _get_live_target(self) -> SchemaBackedOneManyField:
        return self._live_cache._current_field_target(self._live_key)


class _LiveManyOneFieldProxy(_LiveDelegatingProxyMixin, SchemaBackedManyOneField):
    def __init__(self, cache: "DatabaseBackedStorageCache", field_key: str) -> None:
        self._init_live_proxy(cache, str(field_key))

    def _get_live_target(self) -> SchemaBackedManyOneField:
        return self._live_cache._current_field_target(self._live_key)


class _LiveManyManyFieldProxy(_LiveDelegatingProxyMixin, SchemaBackedManyManyField):
    def __init__(self, cache: "DatabaseBackedStorageCache", field_key: str) -> None:
        self._init_live_proxy(cache, str(field_key))

    def _get_live_target(self) -> SchemaBackedManyManyField:
        return self._live_cache._current_field_target(self._live_key)


class DatabaseBackedStorageCache(SchemaBackedStorageCache):
    """
    Storage-cache facade that exposes live table/link/field objects.

    The cache root and the objects returned from it proxy through to a freshly
    rebuilt schema-backed state on each access. This favors correctness and
    "what is in the database right now" semantics over performance.
    """

    plugin_name = "database_backed"
    plugin_capabilities = StorageCacheCapabilities(
        live_reads=True,
        live_child_objects=True,
        vectorized_helpers=False,
        requires_reload_for_external_changes=False,
    )

    def __init__(self, db: Any) -> None:
        self._building_live_state = False
        self._snapshot_main_tables: dict[str, SchemaBackedMainTableCache] = {}
        self._snapshot_link_tables: dict[tuple[str, str], SchemaBackedLinkTable] = {}
        self._snapshot_fields: dict[str, FieldBasicInterfaceAPI[Any]] = {}
        self._snapshot_field_objects: dict[str, FieldBasicInterfaceAPI[Any]] = {}
        self._main_table_proxies: dict[str, _LiveMainTableProxy] = {}
        self._link_table_proxies: dict[tuple[str, str], _LiveLinkTableProxy] = {}
        self._field_proxies: dict[str, FieldBasicInterfaceAPI[Any]] = {}
        super().__init__(db)

    def clear(self) -> None:
        super().clear()
        self._snapshot_main_tables = {}
        self._snapshot_link_tables = {}
        self._snapshot_fields = {}
        self._snapshot_field_objects = {}

    def detach_db(self) -> Optional[Any]:
        old_db = self.db
        self.db = None
        for table in self._snapshot_main_tables.values():
            table.db = None
        for table in self._snapshot_link_tables.values():
            table.db = None
        for field in self._snapshot_field_objects.values():
            field._db = None
        return old_db

    def close(self) -> None:
        self.clear()
        self.detach_db()

    def read(self, db: Any = None) -> None:
        self._building_live_state = True
        try:
            super().read(db=db)
            self._snapshot_main_tables = dict(self.main_tables)
            self._snapshot_link_tables = dict(self.link_tables)
            self._snapshot_fields = dict(self.fields)
            self._snapshot_field_objects = dict(self._field_objects)
            self._install_live_views()
        finally:
            self._building_live_state = False

    def reload(self, db: Any = None) -> None:
        self.read(db=db)

    def _refresh_live_state(self, db: Any = None) -> None:
        if self._building_live_state:
            return
        self.read(db=db)

    def _table_name_from_ref(
        self,
        name: Union[str, StorageCacheSingleTableAPI],
    ) -> str:
        return name.table if isinstance(name, StorageCacheSingleTableAPI) else str(name)

    def _make_field_proxy(self, field_key: str) -> FieldBasicInterfaceAPI[Any]:
        existing = self._field_proxies.get(field_key)
        if existing is not None:
            return existing

        field = self._snapshot_field_objects[field_key]
        if isinstance(field, SchemaBackedSameTableField):
            proxy = _LiveSameTableFieldProxy(self, field_key)
        elif isinstance(field, SchemaBackedTwoTableOneOneField):
            proxy = _LiveTwoTableOneOneFieldProxy(self, field_key)
        elif isinstance(field, SchemaBackedOneManyField):
            proxy = _LiveOneManyFieldProxy(self, field_key)
        elif isinstance(field, SchemaBackedManyOneField):
            proxy = _LiveManyOneFieldProxy(self, field_key)
        elif isinstance(field, SchemaBackedManyManyField):
            proxy = _LiveManyManyFieldProxy(self, field_key)
        else:
            raise TypeError(f"Unsupported live field type: {type(field)!r}")

        self._field_proxies[field_key] = proxy
        return proxy

    def _main_table_proxy(self, table_name: str) -> _LiveMainTableProxy:
        proxy = self._main_table_proxies.get(table_name)
        if proxy is None:
            proxy = _LiveMainTableProxy(self, table_name)
            self._main_table_proxies[table_name] = proxy
        return proxy

    def _link_table_proxy(self, key: tuple[str, str]) -> _LiveLinkTableProxy:
        proxy = self._link_table_proxies.get(key)
        if proxy is None:
            proxy = _LiveLinkTableProxy(self, key)
            self._link_table_proxies[key] = proxy
        return proxy

    def _install_live_views(self) -> None:
        self.main_tables = {
            table_name: self._main_table_proxy(table_name)
            for table_name in self._snapshot_main_tables
        }
        self.link_tables = {
            key: self._link_table_proxy(key)
            for key in self._snapshot_link_tables
        }
        self._field_objects = {
            field_key: self._make_field_proxy(field_key)
            for field_key in self._snapshot_field_objects
        }
        self.fields = {
            alias: self._make_field_proxy(actual.field_key)
            for alias, actual in self._snapshot_fields.items()
        }

    def _current_main_table_target(self, table_name: str) -> SchemaBackedMainTableCache:
        self._refresh_live_state()
        return self._snapshot_main_tables[str(table_name)]

    def _current_link_table_target(self, key: tuple[str, str]) -> SchemaBackedLinkTable:
        self._refresh_live_state()
        return self._snapshot_link_tables[key]

    def _current_field_target(self, field_key: str) -> FieldBasicInterfaceAPI[Any]:
        self._refresh_live_state()
        return self._snapshot_field_objects[str(field_key)]

    def _cached_value_from_loaded_state(
        self,
        owner_id: int,
        field_key: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
        default_value: Any = None,
    ) -> Any:
        field_name = self._resolve_field_name(field_key)
        field = self._snapshot_fields[field_name]
        row_id = int(owner_id)

        getter = getattr(field, "get_value_from_id", None)
        if callable(getter):
            value = getter(row_id)
            return default_value if value is None else value

        getter = getattr(field, "get_value_from_src_id", None)
        if callable(getter):
            value = getter(row_id)
            return default_value if value is None else value

        getter = getattr(field, "get_values_from_src_id", None)
        if callable(getter):
            return getter(row_id)

        ids_values_map = getattr(field, "ids_values_map", None)
        if isinstance(ids_values_map, dict):
            value = ids_values_map.get(row_id)
            return default_value if value is None else value

        raise TypeError(
            f"Field {field_name!r} does not expose a supported cached-value accessor"
        )

    def has_main_table(self, name: str) -> bool:
        if self._building_live_state:
            return super().has_main_table(name)
        self._refresh_live_state()
        return str(name) in self._snapshot_main_tables

    def get_main_table(
        self,
        name: Union[str, StorageCacheSingleTableAPI],
    ) -> SchemaBackedMainTableCache:
        if self._building_live_state:
            return super().get_main_table(name)
        self._refresh_live_state()
        return self.main_tables[self._table_name_from_ref(name)]

    def iter_main_tables(self) -> Iterable[SchemaBackedMainTableCache]:
        if self._building_live_state:
            yield from super().iter_main_tables()
            return
        self._refresh_live_state()
        for table_name in sorted(self.main_tables):
            yield self.main_tables[table_name]

    def get_table(self, name: str) -> StorageCacheBaseTableAPI:
        if self._building_live_state:
            return super().get_table(name)
        self._refresh_live_state()
        if name in self.main_tables:
            return self.main_tables[name]
        for key, table in self._snapshot_link_tables.items():
            if table.table == name:
                return self.link_tables[key]
        raise KeyError(name)

    def iter_tables(self) -> Iterable[StorageCacheBaseTableAPI]:
        if self._building_live_state:
            yield from super().iter_tables()
            return
        self._refresh_live_state()
        yielded: set[int] = set()
        for table_name in sorted(self.main_tables):
            table = self.main_tables[table_name]
            yielded.add(id(table))
            yield table
        for key in sorted(self.link_tables):
            table = self.link_tables[key]
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
        if self._building_live_state:
            return super().has_link_table(src_table, dst_table, table_type=table_type)
        self._refresh_live_state()
        src_name = self._table_name_from_ref(src_table)
        dst_name = self._table_name_from_ref(dst_table)
        table = self._snapshot_link_tables.get((src_name, dst_name))
        if table is None:
            return False
        return table_type is None or table.table_type == table_type

    def get_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
        table_type: Optional[TableTypes] = None,
    ) -> SchemaBackedLinkTable:
        if self._building_live_state:
            return super().get_link_table(src_table, dst_table, table_type=table_type)
        self._refresh_live_state()
        src_name = self._table_name_from_ref(src_table)
        dst_name = self._table_name_from_ref(dst_table)
        key = (src_name, dst_name)
        table = self._snapshot_link_tables[key]
        if table_type is not None and table.table_type != table_type:
            raise KeyError(
                f"Link table {src_name!r}->{dst_name!r} is {table.table_type}, not {table_type}"
            )
        return self.link_tables[key]

    def iter_link_tables(self) -> Iterable[SchemaBackedLinkTable]:
        if self._building_live_state:
            yield from super().iter_link_tables()
            return
        self._refresh_live_state()
        for key in sorted(self.link_tables):
            yield self.link_tables[key]

    def has_field(self, name: FieldKey) -> bool:
        if self._building_live_state:
            return super().has_field(name)
        self._refresh_live_state()
        return super().has_field(name)

    def get_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
    ) -> FieldBasicInterfaceAPI[Any]:
        if self._building_live_state:
            return super().get_field(name)
        self._refresh_live_state()
        field_name = self._resolve_field_name(name)
        return self.fields[field_name]

    def iter_fields(self) -> Iterable[FieldBasicInterfaceAPI[Any]]:
        if self._building_live_state:
            yield from super().iter_fields()
            return
        self._refresh_live_state()
        for field_name in sorted(self._field_objects):
            yield self._field_objects[field_name]

    def get_fields_for_table(
        self,
        table: Union[str, StorageCacheSingleTableAPI],
    ) -> Sequence[FieldBasicInterfaceAPI[Any]]:
        if self._building_live_state:
            return super().get_fields_for_table(table)
        self._refresh_live_state()
        table_name = self._table_name_from_ref(table)
        return tuple(
            self._field_objects[field_name]
            for field_name, field in sorted(
                self._snapshot_field_objects.items(),
                key=lambda item: item[0],
            )
            if self._field_owner_table(field) == table_name
        )

    def get_cached_value(
        self,
        owner_id: int,
        field_key: FieldKey,
        default_value: Any = None,
    ) -> Any:
        self._refresh_live_state()
        return self._cached_value_from_loaded_state(
            owner_id,
            field_key,
            default_value=default_value,
        )

    def get_cached_row_values(
        self,
        owner_id: int,
        field_keys: Sequence[FieldKey],
        default_value: Any = None,
    ) -> Sequence[Any]:
        self._refresh_live_state()
        resolved_field_keys = tuple(self._resolve_field_name(field_key) for field_key in field_keys)
        return tuple(
            self._cached_value_from_loaded_state(
                owner_id,
                field_key,
                default_value=default_value,
            )
            for field_key in resolved_field_keys
        )

    def reload_main_table(
        self,
        name: Union[str, StorageCacheSingleTableAPI],
        db: Any = None,
    ) -> None:
        del name
        self.read(db=db)

    def reload_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
        db: Any = None,
        table_type: Optional[TableTypes] = None,
    ) -> None:
        del src_table, dst_table, table_type
        self.read(db=db)

    def reload_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
        db: Any = None,
    ) -> None:
        del name
        self.read(db=db)

    def invalidate_table(
        self,
        table: Union[str, StorageCacheSingleTableAPI],
    ) -> None:
        del table

    def invalidate_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
        table_type: Optional[TableTypes] = None,
    ) -> None:
        del src_table, dst_table, table_type

    def invalidate_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
    ) -> None:
        del name

    def invalidate_ids(
        self,
        table: Union[str, StorageCacheSingleTableAPI],
        ids: Iterable[int],
    ) -> None:
        del table, ids


StorageCache = DatabaseBackedStorageCache

__all__ = [
    "DatabaseBackedStorageCache",
    "StorageCache",
]
