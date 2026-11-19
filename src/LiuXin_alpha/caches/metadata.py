"""Bind native metadata operations to Catalog persistence and cache freshness.

Use ``cache.metadata`` for the ordinary loaded-cache path, or construct
``MetadataAPI(catalog, cache=cache)`` explicitly. Omitting cache uses Catalog
reads directly. The caller retains both lifetimes. Returned metadata is detached;
changes are persisted only by an explicit entity or compound mutation method.

Compound writes conservatively refresh the complete snapshot. Pending outer
transactions dirty the cache until commit or rollback; committed refresh failures
retain their result in MetadataReconciliationError and never retry persistence.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager, nullcontext
from dataclasses import replace
from typing import TYPE_CHECKING, cast

from LiuXin_alpha.caches.api import CacheConsistency, CacheQuery, CacheSort
from LiuXin_alpha.caches.metadata_reads import (
    MetadataReadSource,
    MetadataValue,
    detached,
)
from LiuXin_alpha.caches.metadata_search import MetadataSearch
from LiuXin_alpha.catalog.api.common import (
    CatalogNotFoundError,
    CreatedWemiStack,
    WemiLevel,
)
from LiuXin_alpha.catalog.api.metadata_access import (
    MetadataEntityInterface,
    MetadataMutationsInterface,
    MetadataReconciliationError,
    MetadataReference,
    MetadataSearchInterface,
    MetadataWriteReceipt,
)
from LiuXin_alpha.catalog.repositories.base import WEMI_TABLES, BaseRepository
from LiuXin_alpha.catalog.retrieval.hydration.expression_metadata_hydrator import (
    ExpressionMetadataHydrator,
)
from LiuXin_alpha.catalog.retrieval.hydration.item_metadata_hydrator import (
    ItemMetadataHydrator,
)
from LiuXin_alpha.catalog.retrieval.hydration.manifestation_metadata_hydrator import (
    ManifestationMetadataHydrator,
)
from LiuXin_alpha.catalog.retrieval.hydration.work_metadata_hydrator import (
    WorkMetadataHydrator,
)
from LiuXin_alpha.databases.transaction_participants import owned_transaction_active

if TYPE_CHECKING:
    from LiuXin_alpha.caches.read_cache import ReadCache
    from LiuXin_alpha.catalog.catalog import Catalog
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_metadata_api import (
        ExpressionMetadataAPI,
    )
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_metadata_api import (
        ItemMetadataAPI,
    )
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_metadata_api import (
        ManifestationMetadataAPI,
    )
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
        MetadataRecord,
    )
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.work_containers.work_metadata_api import (
        WorkMetadataAPI,
    )


class MetadataAPI:
    """Implement MetadataAPIInterface with one Catalog and optional loaded cache.

    Fallback is fixed at construction and disabled by default. Binding identity
    and readiness are checked on every operation, including retained child
    services. The read-cache lock spans hydration and the entire write/refresh
    boundary, preventing another facade operation from publishing half a change.
    Direct database writes still require owner-driven invalidation.
    """

    def __init__(
        self,
        catalog: Catalog,
        *,
        cache: ReadCache | None = None,
        allow_database_fallback: bool = False,
    ) -> None:
        self._catalog = catalog
        self._database = catalog.db
        self._cache = cache
        self._fallback = allow_database_fallback
        with self._lock():
            self._assert_attached()
        self._works: MetadataEntityInterface[WorkMetadataAPI] = _Entity(
            self,
            catalog.works,
            lambda source, entity_id: WorkMetadataHydrator(
                source, strict=True
            ).from_work_id(entity_id),
        )
        self._expressions: MetadataEntityInterface[ExpressionMetadataAPI] = _Entity(
            self,
            catalog.expressions,
            lambda source, entity_id: ExpressionMetadataHydrator(
                source, strict=True
            ).from_expression_id(entity_id),
        )
        self._manifestations: MetadataEntityInterface[ManifestationMetadataAPI] = (
            _Entity(
                self,
                catalog.manifestations,
                lambda source, entity_id: ManifestationMetadataHydrator(
                    source, strict=True
                ).from_manifestation_id(entity_id),
            )
        )
        self._items: MetadataEntityInterface[ItemMetadataAPI] = _Entity(
            self,
            catalog.items,
            lambda source, entity_id: ItemMetadataHydrator(
                source, strict=True
            ).from_item_id(entity_id),
        )
        self._search = MetadataSearch(self)
        self._mutations = _Mutations(self)

    @property
    def works(self) -> MetadataEntityInterface[WorkMetadataAPI]:
        """Read and mutate Works through the coordinating metadata service."""
        return self._works

    @property
    def expressions(self) -> MetadataEntityInterface[ExpressionMetadataAPI]:
        """Read and mutate Expressions through the coordinating metadata service."""
        return self._expressions

    @property
    def manifestations(self) -> MetadataEntityInterface[ManifestationMetadataAPI]:
        """Read and mutate Manifestations through the coordinating metadata service."""
        return self._manifestations

    @property
    def items(self) -> MetadataEntityInterface[ItemMetadataAPI]:
        """Read and mutate Items through the coordinating metadata service."""
        return self._items

    @property
    def search(self) -> MetadataSearchInterface:
        """Search with Catalog's semantics and this service's bound source policy."""
        return self._search

    @property
    def mutations(self) -> MetadataMutationsInterface:
        """Coordinate atomic compound Catalog mutations and cache publication."""
        return self._mutations

    def _lock(self) -> AbstractContextManager[object]:
        return nullcontext() if self._cache is None else self._cache._lock

    def _assert_attached(self) -> None:
        if self._catalog.db is not self._database:
            raise RuntimeError("Metadata Catalog was attached to a different database")
        cache = self._cache
        if cache is not None:
            cache._assert_ready()
            if (
                cache.database is not self._database
                or cache.storage.db is not self._database
            ):
                raise RuntimeError(
                    "Metadata cache is detached or attached to another database"
                )

    def _reads(self, *, require_rows: bool = False) -> MetadataReadSource:
        self._assert_attached()
        cache = self._cache
        if cache is not None:
            cache._refresh_dirty()
            if cache.capabilities.consistency == CacheConsistency.LIVE:
                # A metadata projection is one composed read. Reuse the same
                # bounded capture used by live queries instead of rebuilding the
                # complete catalogue for each traversed row/link proxy.
                from LiuXin_alpha.caches.api import StorageCacheAPI
                from LiuXin_alpha.caches.read_cache import ReadCache

                capture = getattr(cache.storage, "query_view", None)
                if callable(capture):
                    cache = ReadCache.from_storage(cast(StorageCacheAPI, capture()))
        return MetadataReadSource(
            self._database,
            cache,
            allow_database_fallback=self._fallback,
            require_rows=require_rows,
        )

    def _write[ResultT](
        self, operation: Callable[[], tuple[ResultT, bool]]
    ) -> MetadataWriteReceipt[ResultT]:
        with self._lock():
            self._assert_attached()
            macros = self._database.macros
            # Requiring ownership outside an existing managed transaction avoids
            # labelling a borrowed raw-driver savepoint as a durable commit.
            pending = macros.current_transaction_connection() is not None
            if pending and not owned_transaction_active(macros):
                raise RuntimeError(
                    "Metadata mutations require an owned managed transaction; "
                    "a borrowed raw transaction has no observable completion"
                )
            cache = self._cache
            if cache is not None and not pending:
                cache._refresh_dirty()
            with macros.transaction(require_outermost=not pending):
                result, changed = operation()
            receipt = MetadataWriteReceipt(
                result=result,
                changed=changed,
                persistence="pending" if pending else "committed",
                cache="not_configured"
                if cache is None
                else "deferred"
                if pending
                else "current",
                generation=None if cache is None else cache.generation,
            )
            if cache is None:
                return receipt
            try:
                if pending:
                    # Thread-local macro depth alone cannot stop another thread
                    # from publishing an old snapshot before the outer commit.
                    cache._full_reload_required = True
                    cache._mark_dependencies_dirty((), (), ())
                    cache._defer_refresh_until_transaction_ends()
                elif changed:
                    if cache.capabilities.consistency == CacheConsistency.LIVE:
                        cache._advance_generation()
                    else:
                        cache.reload_data()
            except Exception as exc:
                cache._full_reload_required = True
                cache._mark_dependencies_dirty((), (), ())
                raise MetadataReconciliationError(
                    "Metadata mutation applied but cache reconciliation failed; "
                    "inspect the receipt before retrying",
                    receipt=replace(
                        receipt, cache="dirty", generation=cache.generation
                    ),
                ) from exc
            return replace(receipt, generation=cache.generation)


class _Entity[MetadataT: MetadataValue]:
    """Bind a WEMI repository without exposing its uncoordinated methods."""

    def __init__(
        self,
        owner: MetadataAPI,
        repository: BaseRepository,
        hydrate: Callable[[MetadataReadSource, int], MetadataT],
    ) -> None:
        self._owner = owner
        self._repository = repository
        self._hydrate = hydrate

    def get(self, entity_id: int) -> MetadataT | None:
        """Return detached native metadata, or None for a complete root miss."""
        BaseRepository._validate_entity_id(entity_id)
        with self._owner._lock():
            source = self._owner._reads()
            if source.get_row_from_id(self._repository.table_name, entity_id) is None:
                return None
            source.require_rows = True
            return detached(self._hydrate(source, entity_id))

    def require(self, entity_id: int) -> MetadataT:
        """Raise CatalogNotFoundError only for an absent root or referenced entity."""
        result = self.get(entity_id)
        if result is None:
            raise CatalogNotFoundError(
                f"{self._repository.table_name} row not found: {entity_id}"
            )
        return result

    def list(self, *, limit: int = 100, offset: int = 0) -> tuple[MetadataT, ...]:
        """Hydrate a complete ID-ordered page under the cache generation lock."""
        BaseRepository._validate_page(limit, offset)
        with self._owner._lock():
            source = self._owner._reads()
            if limit == 0:
                return ()
            rows = source.rows(
                CacheQuery(
                    table=self._repository.table_name,
                    sort=(CacheSort(self._repository.id_column),),
                    limit=limit,
                    offset=offset,
                ),
                lambda: self._repository.list(limit=limit, offset=offset),
            )
            source.require_rows = True
            return tuple(
                detached(
                    self._hydrate(source, self._row_id(row, self._repository.id_column))
                )
                for row in rows
            )

    @staticmethod
    def _row_id(row: Mapping[str, object], column: str) -> int:
        value = row[column]
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError(f"Invalid metadata identity in {column}: {value!r}")
        return value

    def create(self, data: MetadataRecord) -> MetadataWriteReceipt[int]:
        """Create through Catalog, then publish or defer the cache generation."""
        return self._owner._write(lambda: (self._repository.create(data), True))

    def update(self, entity_id: int, data: MetadataRecord) -> MetadataWriteReceipt[int]:
        """Update only supplied fields, preserving Catalog validation and aliases."""

        def operation() -> tuple[int, bool]:
            self._repository.update(entity_id, data)
            return entity_id, bool(data)

        return self._owner._write(operation)

    def delete(self, entity_id: int) -> MetadataWriteReceipt[int]:
        """Delete an entity and refresh relationships affected by Catalog policy."""

        def operation() -> tuple[int, bool]:
            self._repository.delete(entity_id)
            return entity_id, True

        return self._owner._write(operation)


class _Mutations:
    """Preserve the Catalog writer's compound operation and validation boundary."""

    def __init__(self, owner: MetadataAPI) -> None:
        self._owner = owner

    def create_wemi_stack(
        self,
        *,
        work: MetadataRecord,
        expression: MetadataRecord,
        manifestation: MetadataRecord,
        items: Sequence[MetadataRecord] = (),
        origin: str | None = None,
        work_id: int | None = None,
    ) -> MetadataWriteReceipt[CreatedWemiStack]:
        """Create one atomic WEMI path and reconcile it once."""
        return self._owner._write(
            lambda: (
                self._owner._catalog.mutations.writer.create_wemi_stack(
                    work=work,
                    expression=expression,
                    manifestation=manifestation,
                    items=items,
                    origin=origin,
                    work_id=work_id,
                ),
                True,
            )
        )

    def _metadata(
        self,
        *,
        level: WemiLevel,
        entity_id: int,
        data: MetadataRecord,
        replace_groups: bool,
    ) -> MetadataWriteReceipt[MetadataReference]:
        def operation() -> tuple[MetadataReference, bool]:
            if level not in WEMI_TABLES:
                raise ValueError(f"unknown WEMI level: {level!r}")
            # Catalog replacement of an empty payload does not itself require
            # the root. The metadata API never reports success for a missing ID.
            repository = self._owner._catalog.repositories.for_table(WEMI_TABLES[level])
            if repository is None:
                raise ValueError(f"No Catalog repository for {level!r}")
            repository.require(entity_id)
            writer = self._owner._catalog.mutations.writer
            method = (
                writer.replace_metadata if replace_groups else writer.attach_metadata
            )
            method(level=level, entity_id=entity_id, data=data)
            return MetadataReference(level, entity_id), bool(data)

        return self._owner._write(operation)

    def attach_metadata(
        self, *, level: WemiLevel, entity_id: int, data: MetadataRecord
    ) -> MetadataWriteReceipt[MetadataReference]:
        """Apply direct fields and attachments through the Catalog writer."""
        return self._metadata(
            level=level, entity_id=entity_id, data=data, replace_groups=False
        )

    def replace_metadata(
        self, *, level: WemiLevel, entity_id: int, data: MetadataRecord
    ) -> MetadataWriteReceipt[MetadataReference]:
        """Replace supplied groups while retaining every omitted group."""
        return self._metadata(
            level=level, entity_id=entity_id, data=data, replace_groups=True
        )

    def link_wemi(
        self,
        *,
        parent_level: WemiLevel,
        parent_id: int,
        child_level: WemiLevel,
        child_id: int,
        primary: bool | None = None,
        priority: int | None = None,
        origin: str | None = None,
    ) -> MetadataWriteReceipt[Mapping[str, object]]:
        """Link adjacent WEMI entities, including primary-link reconciliation."""
        return self._owner._write(
            lambda: (
                self._owner._catalog.mutations.writer.link_wemi(
                    parent_level=parent_level,
                    parent_id=parent_id,
                    child_level=child_level,
                    child_id=child_id,
                    primary=primary,
                    priority=priority,
                    origin=origin,
                ),
                True,
            )
        )

    def unlink_wemi(
        self,
        *,
        parent_level: WemiLevel,
        parent_id: int,
        child_level: WemiLevel,
        child_id: int,
    ) -> MetadataWriteReceipt[bool]:
        """Remove the relationship and retain both endpoint entities."""

        def operation() -> tuple[bool, bool]:
            result = self._owner._catalog.mutations.writer.unlink_wemi(
                parent_level=parent_level,
                parent_id=parent_id,
                child_level=child_level,
                child_id=child_id,
            )
            return result, result

        return self._owner._write(operation)


__all__ = ["MetadataAPI"]
