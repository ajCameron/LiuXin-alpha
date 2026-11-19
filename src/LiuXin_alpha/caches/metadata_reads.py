"""Supply complete reads and detached values to the bound metadata service.

Catalog's existing hydrators own the projection. This source opts into their
strict mode and only falls back for explicitly unavailable cache operations.
Schema discovery still belongs to the database; a cache missing a known table
must not make that metadata disappear from the projection.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from typing import TYPE_CHECKING, Any, Protocol, Self, cast

from LiuXin_alpha.caches.api import (
    CacheFilterOperator,
    CachePredicate,
    CacheQuery,
    CacheQueryResult,
    CacheSort,
    UnknownCacheFieldError,
    UnknownCacheTableError,
    UnsupportedCacheQueryError,
)
from LiuXin_alpha.catalog.api.common import CatalogNotFoundError
from LiuXin_alpha.catalog.retrieval.hydration.read_sources import (
    DatabaseMetadataReadSource,
)
from LiuXin_alpha.databases.row import Row

if TYPE_CHECKING:
    from LiuXin_alpha.caches.read_cache import ReadCache
    from LiuXin_alpha.catalog.api.common import DatabaseHandle
    from LiuXin_alpha.catalog.api.retrieval.hydration.metadata_read_source_api import (
        MetadataRowSequence,
        MetadataSearchTerm,
    )
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
        MetadataRecord,
    )


UNAVAILABLE = (
    UnknownCacheTableError,
    UnknownCacheFieldError,
    UnsupportedCacheQueryError,
)


class MetadataValue(Protocol):
    """The native containers' serialization boundary, used to detach row targets."""

    def to_mapping(self, include_related: bool = True) -> MetadataRecord:
        """Serialize identity, relationships and link attributes."""
        ...

    @classmethod
    def from_mapping(cls, payload: MetadataRecord) -> Self:
        """Construct a value without retaining database-bound rows."""
        ...


def detached[ValueT: MetadataValue](value: ValueT) -> ValueT:
    """Copy nested payloads after serializing all database-bound relation targets."""
    return value.from_mapping(deepcopy(value.to_mapping()))


class MetadataReadSource:
    """Read one bound source without translating unavailable data into absence.

    The service owns the surrounding cache lock. Hydration uses ``require_rows``
    after checking the root, so dangling references cannot silently disappear.
    This source never owns or closes the borrowed Catalog database or cache.
    """

    def __init__(
        self,
        database: DatabaseHandle,
        cache: ReadCache | None,
        *,
        allow_database_fallback: bool,
        require_rows: bool = False,
    ) -> None:
        self._source = DatabaseMetadataReadSource(database)
        self.driver_wrapper = self._source.driver_wrapper
        self.cache = cache
        self.allow_database_fallback = allow_database_fallback
        self.require_rows = require_rows

    def get_tables(self, force_refresh: bool = False) -> Sequence[str]:
        """Discover schema independently of cache coverage; errors propagate."""
        return self._source.get_tables(force_refresh=force_refresh)

    def get_tables_and_columns(self) -> Mapping[str, Sequence[str]]:
        """Retain optional-table discovery without hiding unavailable cached data."""
        return self._source.get_tables_and_columns()

    def get_column_headings(self, table: str) -> set[str]:
        """Resolve physical row headings through the Catalog database."""
        return self._source.get_column_headings(table)

    def _read[ResultT](
        self, cached: Callable[[ReadCache], ResultT], database: Callable[[], ResultT]
    ) -> ResultT:
        if self.cache is None:
            return database()
        try:
            return cached(self.cache)
        except UNAVAILABLE:
            if not self.allow_database_fallback:
                raise
        return database()

    @staticmethod
    def complete(result: CacheQueryResult) -> CacheQueryResult:
        """Require a complete result even if some records are present."""
        if not result.complete:
            raise UnsupportedCacheQueryError(
                "Metadata requires a complete cache result"
            )
        return result

    def _row(self, values: Mapping[str, object]) -> Row:
        return Row(database=cast(Any, self), row_dict=dict(values), read_only=True)

    def get_row_from_id(self, table: str, row_id: int) -> Row | None:
        """Return a confirmed root miss; reject missing references during hydration."""

        def cached(cache: ReadCache) -> Row | None:
            lookup = cache.get(table, row_id)
            if not lookup.complete:
                raise UnsupportedCacheQueryError(
                    "Metadata requires a complete cache lookup"
                )
            return None if lookup.value is None else self._row(lookup.value.values)

        row = self._read(
            cached,
            lambda: self._source.get_row_from_id(table, row_id),
        )
        if row is None and self.require_rows:
            raise CatalogNotFoundError(f"Missing referenced {table} row: {row_id}")
        return row

    def rows(
        self, query: CacheQuery, fallback: Callable[[], Sequence[Mapping[str, object]]]
    ) -> tuple[Mapping[str, object], ...]:
        """Execute a complete page, retaining the configured fallback policy."""
        return self._read(
            lambda cache: tuple(
                record.values for record in self.complete(cache.query(query)).records
            ),
            lambda: tuple(fallback()),
        )

    def get_all_rows(
        self, table: str, iterator_return: bool = False
    ) -> MetadataRowSequence:
        """Return complete detached row wrappers for hydration."""
        del iterator_return
        return self._read(
            lambda cache: cast(
                "MetadataRowSequence",
                tuple(
                    self._row(record.values)
                    for record in self.complete(
                        cache.query(CacheQuery(table=table))
                    ).records
                ),
            ),
            lambda: tuple(self._source.get_all_rows(table)),
        )

    def search(
        self, table: str, column: str, search_term: MetadataSearchTerm
    ) -> MetadataRowSequence:
        """Read equality matches in ID order without suppressing query failures."""
        query = CacheQuery(
            table=table,
            predicates=(CachePredicate(column, CacheFilterOperator.EQ, search_term),),
            sort=(CacheSort(self.driver_wrapper.get_id_column(table)),),
        )
        return self._read(
            lambda cache: cast(
                "MetadataRowSequence",
                tuple(
                    self._row(record.values)
                    for record in self.complete(cache.query(query)).records
                ),
            ),
            lambda: tuple(self._source.search(table, column, search_term)),
        )

    def get_interlink_rows(
        self, primary_row: Any, secondary_table: str
    ) -> Sequence[Any]:
        """Distinguish absent schema relationships from unavailable cached ones."""
        if not self.driver_wrapper.get_link_table_name(
            primary_row.table, secondary_table
        ):
            return ()

        def cached(cache: ReadCache) -> Sequence[Any]:
            # Check declared availability separately: a KeyError from the actual
            # read may indicate corrupt data and must not trigger fallback.
            try:
                cache.storage.get_link_table(primary_row.table, secondary_table)
            except KeyError as exc:
                raise UnsupportedCacheQueryError("Relationship is not cached") from exc
            return cache.link_records(
                primary_row.table, primary_row.row_id, secondary_table
            )

        return self._read(
            cached,
            lambda: self._source.get_interlink_rows(primary_row, secondary_table),
        )


__all__ = ["MetadataReadSource"]
