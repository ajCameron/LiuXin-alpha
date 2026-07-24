"""Concrete composed cache facade."""

from __future__ import annotations

import threading

from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import Any, Optional, cast

from LiuXin_alpha.caches.api.cache_api import (
    CacheAPI,
    CacheCapabilities,
    CacheClosedError,
    CacheConsistency,
    CacheDirtyError,
    CacheLookup,
    CacheLookupStatus,
    CacheNotReadyError,
    CacheQuery,
    CacheQueryResult,
    CacheRecord,
    CacheReconciliationError,
    CacheState,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_cache_api import (
    StorageCacheAPI,
)
from LiuXin_alpha.caches.cache_plugins.registry import create_storage_cache
from LiuXin_alpha.caches.query import CacheQueryEngine


class _CacheCatalogFacade:
    """Delegate persistence to Catalog and reconcile the owning cache."""

    def __init__(self, owner: "Cache") -> None:
        from LiuXin_alpha.catalog import Catalog

        self._owner = owner
        self._catalog = Catalog(owner.database)
        self.db = owner.database

    def __getattr__(self, name: str) -> Any:
        return getattr(self._catalog, name)

    def _assert_attached(self) -> None:
        self._owner._assert_open()
        if (
            self._owner.database is not self.db
            or self._owner.storage.db is not self.db
        ):
            raise RuntimeError(
                "cache writer cannot apply after its cache is closed, detached, "
                "or attached to a different database"
            )
        self._owner._assert_ready()

    def _apply(self, method_name: str, update: Any) -> Mapping[Any, Any]:
        with self._owner._lock:
            self._assert_attached()
            if (
                self._owner.state == CacheState.DIRTY
                and not self._owner._transaction_is_open()
            ):
                self._owner._refresh_dirty()
            result = cast(
                Mapping[Any, Any],
                getattr(self._catalog, method_name)(update),
            )
            self._owner._reconcile_catalog_update(update, result)
            return result

    def write_column_update(self, update: Any) -> Mapping[Any, Any]:
        return self._apply("write_column_update", update)

    def write_link_update(self, update: Any) -> Mapping[Any, Any]:
        return self._apply("write_link_update", update)

    def write_owned_row_update(self, update: Any) -> Mapping[Any, Any]:
        return self._apply("write_owned_row_update", update)


class Cache(CacheAPI):
    """Application-facing cache composed around one storage-cache plugin."""

    def __init__(
        self,
        database: Any,
        *,
        storage: Optional[StorageCacheAPI] = None,
        storage_type: str = "schema_backed",
        storage_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> None:
        if database is None:
            raise ValueError("Cache requires an attached database")
        self.database = database
        self.storage = storage or create_storage_cache(
            database,
            storage_type,
            **dict(storage_kwargs or {}),
        )
        if self.storage.db is not database:
            raise ValueError("Cache storage backend must be attached to the same database")
        self._lock = threading.RLock()
        self._state = CacheState.EMPTY
        self._generation = 0
        self._dirty_tables: set[str] = set()
        self._dirty_links: set[tuple[str, str]] = set()
        self._dirty_fields: set[str] = set()
        self._query_engine = CacheQueryEngine(self.storage)

    @classmethod
    def from_storage(cls, storage: StorageCacheAPI) -> "Cache":
        """Compose a facade around an existing storage cache."""

        if storage.db is None:
            raise ValueError("Cannot compose Cache around detached storage")
        instance = cls(storage.db, storage=storage)
        if storage.is_initialized:
            instance._state = CacheState.READY
            instance._generation = 1
        return instance

    @property
    def state(self) -> CacheState:
        return self._state

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def capabilities(self) -> CacheCapabilities:
        storage_caps = self.storage.capabilities
        consistency = (
            CacheConsistency.LIVE
            if storage_caps.live_reads
            else CacheConsistency.SNAPSHOT
        )
        return CacheCapabilities(
            consistency=consistency,
            live_child_objects=storage_caps.live_child_objects,
            vectorized_helpers=storage_caps.vectorized_helpers,
        )

    def _assert_open(self) -> None:
        if self._state == CacheState.CLOSED:
            raise CacheClosedError("Cache is closed")

    def _assert_ready(self) -> None:
        self._assert_open()
        if self._state == CacheState.EMPTY or not self.storage.is_initialized:
            raise CacheNotReadyError("Cache has not been loaded")

    def _advance_generation(self) -> None:
        self._generation += 1
        self._query_engine.reset()

    def load(self) -> None:
        with self._lock:
            self._assert_open()
            self.storage.read()
            self._dirty_tables.clear()
            self._dirty_links.clear()
            self._dirty_fields.clear()
            self._state = CacheState.READY
            self._advance_generation()

    def reload(self) -> None:
        with self._lock:
            self._assert_open()
            self.storage.reload()
            self._dirty_tables.clear()
            self._dirty_links.clear()
            self._dirty_fields.clear()
            self._state = CacheState.READY
            self._advance_generation()

    def clear(self) -> None:
        with self._lock:
            self._assert_open()
            self.storage.clear()
            self._dirty_tables.clear()
            self._dirty_links.clear()
            self._dirty_fields.clear()
            self._state = CacheState.EMPTY
            self._advance_generation()

    def close(self) -> None:
        with self._lock:
            if self._state == CacheState.CLOSED:
                return
            self.storage.close()
            self._dirty_tables.clear()
            self._dirty_links.clear()
            self._dirty_fields.clear()
            self._state = CacheState.CLOSED
            self._advance_generation()

    def table_columns(self) -> Mapping[str, tuple[str, ...]]:
        with self._lock:
            self._assert_ready()
            self._refresh_dirty()
            return MappingProxyType(
                {
                    str(table_name): tuple(
                        str(column)
                        for column in table_cache.column_headings
                    )
                    for table_name, table_cache in self.storage.main_tables.items()
                }
            )

    def _transaction_is_open(self) -> bool:
        macros = getattr(self.database, "macros", None)
        transaction_state = (
            vars(macros).get("_macro_transaction_state")
            if macros is not None
            else None
        )
        return bool(
            transaction_state is not None
            and getattr(transaction_state, "depth", 0)
        )

    def _refresh_dirty(self) -> None:
        if self._state != CacheState.DIRTY:
            return
        if self._transaction_is_open():
            raise CacheDirtyError(
                "Cache dependencies are dirty while an outer transaction is open"
            )

        try:
            for table in sorted(self._dirty_tables):
                self.storage.reload_main_table(table)
            for source, target in sorted(self._dirty_links):
                self.storage.reload_link_table(source, target)
            for field in sorted(self._dirty_fields):
                self.storage.reload_field(field)
        except Exception as exc:
            raise CacheDirtyError("Failed to refresh dirty cache dependencies") from exc

        self._dirty_tables.clear()
        self._dirty_links.clear()
        self._dirty_fields.clear()
        self._state = CacheState.READY
        self._advance_generation()

    def get(self, table: str, row_id: int) -> CacheLookup[CacheRecord]:
        with self._lock:
            self._assert_ready()
            self._refresh_dirty()
            if self.capabilities.consistency == CacheConsistency.LIVE:
                self._query_engine.reset()
            record = self._query_engine.get(str(table), int(row_id))
            return CacheLookup(
                status=(
                    CacheLookupStatus.HIT
                    if record is not None
                    else CacheLookupStatus.MISS
                ),
                value=record,
                complete=True,
                generation=self._generation,
            )

    def query(self, query: CacheQuery) -> CacheQueryResult:
        with self._lock:
            self._assert_ready()
            self._refresh_dirty()
            if self.capabilities.consistency == CacheConsistency.LIVE:
                self._query_engine.reset()
            return self._query_engine.query(query, generation=self._generation)

    def related(
        self,
        source_table: str,
        source_ids: Iterable[int],
        target_table: str,
        *,
        type_filter: Optional[str] = None,
    ) -> CacheQueryResult:
        with self._lock:
            self._assert_ready()
            self._refresh_dirty()
            if self.capabilities.consistency == CacheConsistency.LIVE:
                self._query_engine.reset()
            ids = self._query_engine.related_ids(
                str(source_table),
                source_ids,
                str(target_table),
                type_filter=type_filter,
            )
            records = tuple(
                record
                for row_id in ids
                if (record := self._query_engine.get(str(target_table), row_id))
                is not None
            )
            return CacheQueryResult(
                records=records,
                total_count=len(records),
                offset=0,
                limit=None,
                complete=True,
                generation=self._generation,
            )

    def link_records(
        self,
        source_table: str,
        source_id: int,
        target_table: str,
        *,
        type_filter: Optional[str] = None,
    ) -> tuple[CacheRecord, ...]:
        with self._lock:
            self._assert_ready()
            self._refresh_dirty()
            rows = self.storage.get_link_rows_for_source(
                str(source_table),
                int(source_id),
                str(target_table),
                require_ordering=True,
                type_filter=type_filter,
            )
            try:
                link_table = self.storage.get_link_table(
                    str(source_table),
                    str(target_table),
                )
            except KeyError:
                link_table = self.storage.get_link_table(
                    str(target_table),
                    str(source_table),
                )
            id_column = self.database.driver_wrapper.get_id_column(link_table.table)
            records: list[CacheRecord] = []
            for index, row in enumerate(rows):
                values = (
                    dict(row.row_dict)
                    if hasattr(row, "row_dict")
                    else dict(row)
                )
                raw_id = values.get(id_column)
                records.append(
                    CacheRecord(
                        table=str(link_table.table),
                        row_id=int(raw_id) if raw_id is not None else -(index + 1),
                        values=values,
                    )
                )
            return tuple(records)

    def invalidate(
        self,
        *,
        tables: Iterable[str] = (),
        links: Iterable[tuple[str, str]] = (),
        fields: Iterable[str] = (),
    ) -> None:
        with self._lock:
            self._assert_ready()
            table_names = {str(table) for table in tables}
            link_names = {
                (str(source), str(target)) for source, target in links
            }
            field_names = {str(field) for field in fields}

            if self.capabilities.consistency == CacheConsistency.LIVE:
                if table_names or link_names or field_names:
                    self._advance_generation()
                return

            for table in table_names:
                self.storage.invalidate_table(table)
            for source, target in link_names:
                self.storage.invalidate_link_table(source, target)
            for field in field_names:
                self.storage.invalidate_field(field)

            self._dirty_tables.update(table_names)
            self._dirty_links.update(link_names)
            self._dirty_fields.update(field_names)
            if table_names or link_names or field_names:
                self._state = CacheState.DIRTY
                self._advance_generation()

    @staticmethod
    def _update_dependencies(
        update: Any,
    ) -> tuple[set[str], set[tuple[str, str]], set[str]]:
        from LiuXin_alpha.catalog.write import (
            CatalogColumnUpdate,
            CatalogOwnedRowUpdate,
            LinkUpdate,
        )

        tables: set[str] = set()
        links: set[tuple[str, str]] = set()
        fields: set[str] = set()
        if isinstance(update, CatalogColumnUpdate):
            table = str(update.table_spec.name)
            tables.add(table)
            fields.add(f"{table}.{update.column_spec.name}")
            return tables, links, fields
        if isinstance(update, (CatalogOwnedRowUpdate, LinkUpdate)):
            link_spec = update.link_spec
            primary = str(link_spec.primary_table)
            secondary = str(link_spec.secondary_table)
            tables.add(secondary)
            links.add((primary, secondary))
            if primary != secondary:
                links.add((secondary, primary))
            return tables, links, fields
        raise TypeError(f"unsupported catalog update type: {type(update).__name__}")

    def _mark_dependencies_dirty(
        self,
        tables: Iterable[str],
        links: Iterable[tuple[str, str]],
        fields: Iterable[str],
    ) -> None:
        table_names = {str(table) for table in tables}
        link_names = {(str(source), str(target)) for source, target in links}
        field_names = {str(field) for field in fields}

        for table in table_names:
            try:
                self.storage.invalidate_table(table)
            except Exception:
                pass
        for source, target in link_names:
            try:
                self.storage.invalidate_link_table(source, target)
            except Exception:
                pass
        for field in field_names:
            try:
                self.storage.invalidate_field(field)
            except Exception:
                pass

        self._dirty_tables.update(table_names)
        self._dirty_links.update(link_names)
        self._dirty_fields.update(field_names)
        self._state = CacheState.DIRTY
        self._advance_generation()

    def _reconcile_catalog_update(
        self,
        update: Any,
        result: Mapping[Any, Any],
    ) -> None:
        if not result:
            return

        with self._lock:
            self._assert_ready()
            tables, links, fields = self._update_dependencies(update)
            if self.capabilities.consistency == CacheConsistency.LIVE:
                self._advance_generation()
                return

            if self._transaction_is_open():
                self._mark_dependencies_dirty(tables, links, fields)
                return

            try:
                for table in sorted(tables):
                    self.storage.reload_main_table(table)
                for source, target in sorted(links):
                    self.storage.reload_link_table(source, target)
                for field in sorted(fields):
                    if self.storage.has_field(field):
                        self.storage.reload_field(field)
            except Exception as exc:
                self._mark_dependencies_dirty(tables, links, fields)
                dependencies = set(tables)
                dependencies.update(f"{source}->{target}" for source, target in links)
                dependencies.update(fields)
                raise CacheReconciliationError(
                    "Catalog commit succeeded but cache reconciliation failed",
                    receipt=result,
                    dependencies=dependencies,
                ) from exc

            self._state = CacheState.READY
            self._advance_generation()

    def create_writer(
        self,
        src_table: str,
        dst_column: str,
        *,
        force_refresh: bool = False,
        destination_owned: bool | None = None,
    ) -> Any:
        from LiuXin_alpha.catalog.write import create_catalog_writer
        from LiuXin_alpha.catalog.api import CatalogAPI

        with self._lock:
            self._assert_ready()
            facade = _CacheCatalogFacade(self)
            return create_catalog_writer(
                cast(CatalogAPI, cast(object, facade)),
                src_table,
                dst_column,
                force_refresh=force_refresh,
                destination_owned=destination_owned,
            )

    def write(
        self,
        src_table: str,
        dst_column: str,
        *args: Any,
        force_refresh: bool = False,
        destination_owned: bool | None = None,
        **kwargs: Any,
    ) -> Mapping[Any, Any]:
        writer = self.create_writer(
            src_table,
            dst_column,
            force_refresh=force_refresh,
            destination_owned=destination_owned,
        )
        return cast(Mapping[Any, Any], writer.write(*args, **kwargs))

    def write_one(
        self,
        src_table: str,
        dst_column: str,
        src_id: Any,
        dst_value: Any,
        *,
        force_refresh: bool = False,
        destination_owned: bool | None = None,
        **kwargs: Any,
    ) -> Mapping[Any, Any]:
        writer = self.create_writer(
            src_table,
            dst_column,
            force_refresh=force_refresh,
            destination_owned=destination_owned,
        )
        return cast(
            Mapping[Any, Any],
            writer.write_one(src_id, dst_value, **kwargs),
        )


def create_cache(
    database: Any,
    storage_type: str = "schema_backed",
    *,
    load: bool = True,
    **storage_kwargs: Any,
) -> Cache:
    """Create the modern composed cache and load it by default."""

    cache = Cache(
        database,
        storage_type=storage_type,
        storage_kwargs=storage_kwargs,
    )
    if load:
        cache.load()
    return cache


__all__ = ["Cache", "create_cache"]
