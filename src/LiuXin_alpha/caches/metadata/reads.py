"""
Supply complete reads and detached values within the metadata service package.

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
        MetadataLinkRowSequence,
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
    """
    The native containers' serialization boundary, used to detach row targets.

    Example:
        Native WEMI containers round-trip through to_mapping and from_mapping.
    """

    def to_mapping(self, include_related: bool = True) -> MetadataRecord:
        """
        Serialize identity, relationships and link attributes.

        Example:
            ``value.to_mapping()`` includes the relationship data used for detachment.


        :param include_related: Include relation targets and link payloads when true.
        :return: Native serialization mapping for the metadata value.
        """
        ...

    @classmethod
    def from_mapping(cls, payload: MetadataRecord) -> Self:
        """
        Construct a value without retaining database-bound rows.

        Example:
            The detachment helper supplies a recursively copied mapping.


        :param payload: Native serialization mapping for this container family.
        :return: Container reconstructed from the supplied mapping.
        """
        ...


def detached[ValueT: MetadataValue](value: ValueT) -> ValueT:
    """
    Copy nested payloads after serializing all database-bound relation targets.

    Example:
        Editing a returned Work title leaves the bound cache unchanged.


    :param value: Native metadata container exposing the serialization contract.
    :return: Independent container of the same concrete family.
    """
    return value.from_mapping(deepcopy(value.to_mapping()))


class MetadataReadSource:
    """
    Read one bound source without translating unavailable data into absence.

    The service owns the surrounding cache lock. Hydration uses ``require_rows``
    after checking the root, so dangling references cannot silently disappear.
    This source never owns or closes the borrowed Catalog database or cache.

    Example:
        Check a root with require_rows=False, then enable strict missing-reference checks.
    """

    def __init__(
        self,
        database: DatabaseHandle,
        cache: ReadCache | None,
        *,
        allow_database_fallback: bool,
        require_rows: bool = False,
    ) -> None:
        """
        Bind database schema discovery and an optional complete cache read path.

        Example:
            A live metadata call supplies one captured cache view for its entire hydration.


        :param database: Borrowed Catalog database handle for schema discovery and
            permitted reads.
        :param cache: Selected cache/captured view, or None for direct database reads.
        :param allow_database_fallback: Permit unsupported cache operations to use their
            database equivalent.
        :param require_rows: Raise on missing rows instead of returning an optional root
            miss.
        :return: None; retains the read policy without taking resource ownership.
        """
        self._source = DatabaseMetadataReadSource(database)
        self.driver_wrapper = self._source.driver_wrapper
        self.cache = cache
        self.allow_database_fallback = allow_database_fallback
        self.require_rows = require_rows

    def get_tables(self, force_refresh: bool = False) -> Sequence[str]:
        """
        Discover schema independently of cache coverage; errors propagate.

        Example:
            A missing cached table remains a coverage error rather than absent schema.


        :param force_refresh: Request fresh database schema discovery when true.
        :return: Schema table names, independently of cache coverage.
        """
        return self._source.get_tables(force_refresh=force_refresh)

    def get_tables_and_columns(self) -> Mapping[str, Sequence[str]]:
        """
        Retain optional-table discovery without hiding unavailable cached data.

        Example:
            Hydrators use this mapping to detect optional metadata tables.


        :return: Physical table-to-column mapping from the database read source.
        """
        return self._source.get_tables_and_columns()

    def get_column_headings(self, table: str) -> set[str]:
        """
        Resolve physical row headings through the Catalog database.

        Example:
            Identity reconstruction resolves physical columns even on a cache read.


        :param table: Physical table to inspect.
        :return: Set of physical column names.
        """
        return self._source.get_column_headings(table)

    def _read[ResultT](
        self, cached: Callable[[ReadCache], ResultT], database: Callable[[], ResultT]
    ) -> ResultT:
        """
        Select one read path with fallback limited to declared unavailability.

        Example:
            A confirmed cache miss returns directly and never activates fallback.


        :param cached: Operation applied to the selected cache when configured.
        :param database: Equivalent database operation used without a cache or on
            allowed unavailability.
        :return: Selected operation's result; arbitrary corruption/read failures
            propagate.
        """
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
        """
        Require a complete result even if some records are present.

        Example:
            A nonempty partial page is still rejected as incomplete.


        :param result: Cache query result whose coverage must be complete.
        :return: The same result when complete; otherwise UnsupportedCacheQueryError.
        """
        if not result.complete:
            raise UnsupportedCacheQueryError(
                "Metadata requires a complete cache result"
            )
        return result

    def _row(self, values: Mapping[str, object]) -> Row:
        """
        Wrap copied physical values for read-only Catalog hydration.

        Example:
            The wrapper cannot persist a modified row back to SQL.


        :param values: Physical values copied into a read-only row wrapper.
        :return: Row bound to this source for hydration/schema access.
        """
        # Row's broad database annotation exceeds the read-source protocol used
        # by hydration. This adapter supplies that narrow read-only surface.
        return Row(database=cast(Any, self), row_dict=dict(values), read_only=True)

    def get_row_from_id(self, table: str, row_id: int) -> Row | None:
        """
        Return a confirmed root miss; reject missing references during hydration.

        Example:
            Once require_rows is enabled, a dangling reference raises CatalogNotFoundError.


        :param table: Physical root or referenced table.
        :param row_id: Requested row identity.
        :return: Read-only row, or None when a missing root is allowed.
        """

        def cached(cache: ReadCache) -> Row | None:
            """
            Require complete lookup coverage before constructing a row.

            Example:
                An incomplete lookup raises before fallback policy is considered.


            :param cache: Selected complete cache view.
            :return: Wrapped cached row or None for a complete miss.
            """
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
        """
        Execute a complete page, retaining the configured fallback policy.

        Example:
            A partial cache page never returns as a successful metadata page.


        :param query: Structured query to execute against the selected cache.
        :param fallback: Equivalent database page operation, called only when permitted.
        :return: Complete tuple of physical row mappings.
        """
        return self._read(
            lambda cache: tuple(
                record.values for record in self.complete(cache.query(query)).records
            ),
            lambda: tuple(fallback()),
        )

    def get_all_rows(
        self, table: str, iterator_return: bool = False
    ) -> MetadataRowSequence:
        """
        Return complete detached row wrappers for hydration.

        Example:
            Hydration receives the whole supported table population, not a partial iterator.


        :param table: Physical table whose rows are requested.
        :param iterator_return: Accepted for the read-source contract; this method
            always materializes rows.
        :return: Materialized row wrappers from a complete cache read or selected
            database path.
        """
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
        """
        Read equality matches in ID order without suppressing query failures.

        Example:
            A null search operand retains the underlying source's equality semantics.


        :param table: Owning physical table.
        :param column: Physical equality-search column.
        :param search_term: Native equality operand passed unchanged to the selected
            source.
        :return: Matching row wrappers in identity order on the cache path.
        """
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
    ) -> MetadataLinkRowSequence:
        """
        Distinguish absent schema relationships from unavailable cached ones.

        Hydrators supply identified row-like objects. Keep the input boundary
        broad to accommodate their adapters; RowAPI also permits unidentified
        drafts and does not express the identity required for this traversal.

        Example:
            Missing cached topology raises an availability error when schema declares the link.


        :param primary_row: Identified row-like object exposing a source table name and
            integer row ID.
        :param secondary_table: Target endpoint table.
        :return: Physical association rows; no schema relationship produces an empty
            tuple.
        """
        if not self.driver_wrapper.get_link_table_name(
            primary_row.table, secondary_table
        ):
            return ()

        def cached(cache: ReadCache) -> MetadataLinkRowSequence:
            """
            Validate cached topology before reading source-oriented association rows.

            Example:
                A KeyError while reading declared link records remains a real read failure.


            :param cache: Selected cache view providing the declared relationship.
            :return: CacheRecord association rows with their physical link metadata.
            """
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
