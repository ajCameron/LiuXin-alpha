"""
Execute Catalog-owned metadata search plans with explicit page provenance.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, cast

from LiuXin_alpha.caches.api import CacheConsistency, CacheQuery, CacheQueryResult
from LiuXin_alpha.caches.metadata_reads import UNAVAILABLE, MetadataReadSource
from LiuXin_alpha.caches.projection import ProjectionQueryEngine
from LiuXin_alpha.catalog.api.metadata_access import MetadataReference
from LiuXin_alpha.catalog.api.metadata_search import (
    MetadataSearchField,
    MetadataSearchPage,
    MetadataSearchRequest,
    MetadataSortField,
)
from LiuXin_alpha.catalog.repositories.base import BaseRepository
from LiuXin_alpha.catalog.search import metadata_requests

if TYPE_CHECKING:
    from LiuXin_alpha.caches.metadata import MetadataAPI
    from LiuXin_alpha.catalog.api.common import WemiLevel
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class MetadataSearch:
    """
    Bind search to the metadata owner's attachment, lock and fallback policy.

    Example:
        ``metadata.search.query(request)`` returns references, totals and provenance.
    """

    def __init__(self, owner: MetadataAPI) -> None:
        """
        Borrow the enclosing metadata service without loading another cache.

        Example:
            A retained search service rechecks attachment at each operation.


        :param owner: Metadata service providing Catalog semantics and source policy.
        :return: None; retains the owner without reading rows.
        """
        self._owner = owner

    def fields(self, *, level: WemiLevel = "work") -> tuple[MetadataSearchField, ...]:
        """
        Discover supported semantic filters without executing a search.

        Example:
            Field discovery does not execute a catalogue query.


        :param level: WEMI result family whose semantic filters are requested.
        :return: Tuple of supported semantic filter descriptors.
        """
        with self._owner._lock():
            self._owner._assert_attached()
            return metadata_requests.fields(level)

    def sort_fields(
        self, *, level: WemiLevel = "work"
    ) -> tuple[MetadataSortField, ...]:
        """
        Discover scalar ordering keys for this result family.

        Example:
            Relationship filter paths need not be valid scalar sort keys.


        :param level: WEMI result family whose scalar ordering keys are requested.
        :return: Tuple of sortable field descriptors.
        """
        with self._owner._lock():
            self._owner._assert_attached()
            return metadata_requests.sort_fields(level)

    def validate(self, request: MetadataSearchRequest) -> None:
        """
        Validate semantic options and syntax without cache or database row reads.

        Example:
            A numeric ID filter rejects a boolean operand before execution.


        :param request: Typed search request to validate without reading schema or rows.
        :return: None for a valid request; structured validation errors identify the
            invalid parameter.
        """
        with self._owner._lock():
            self._owner._assert_attached()
            metadata_requests.validate(self._owner._catalog.search, request)

    def _database_page(self, plan: CacheQuery) -> CacheQueryResult:
        """
        Filter through existing SQL macros, then reuse native sorting and paging.

        SQL-capable backends materialize all matching root rows to obtain one
        count and ordering population. Other backends use one ephemeral database
        capture. Neither route retains results or borrows a configured cache's
        generation. The metadata owner supplies the surrounding read transaction.

        Example:
            SQL filters matching roots before native sorting and paging.


        :param plan: Compiled physical query plan under the owner's read transaction.
        :return: Complete physical query result with counts and ordering from one
            population.
        """
        database = self._owner._database
        if database.macros.supports_normalized_search():
            assert plan.where is not None
            rows = database.macros.find_rows_matching(
                plan.table, plan.where, limit=None
            )
            identity = f"{plan.table[:-1]}_id"
            records = {self._identity(row[identity]): row for row in rows}
            columns = MetadataReadSource(
                database, None, allow_database_fallback=False
            ).get_column_headings(plan.table)
            engine = ProjectionQueryEngine(
                plan.table, records, columns, result_cache_size=0
            )
            return engine.query(replace(plan, where=None), generation=0)
        from LiuXin_alpha.caches.cache_plugins.database_backed.storage_cache import (
            DatabaseBackedStorageCache,
        )
        from LiuXin_alpha.caches.query import CacheQueryEngine

        # The temporary capture belongs to this execution, not to the public cache.
        storage = DatabaseBackedStorageCache(cast("DatabaseAPI", database))
        view = storage.query_view()
        try:
            return CacheQueryEngine(view, result_cache_size=0).query(plan, generation=0)
        finally:
            view.close()

    @staticmethod
    def _identity(value: object) -> int:
        """
        Refuse malformed identities instead of coercing broken search results.

        Example:
            A malformed physical ID never becomes a coerced reference.


        :param value: Candidate identity from a physical search result.
        :return: Integer identity; booleans and non-integers raise TypeError.
        """
        if type(value) is not int:
            raise TypeError(f"Invalid metadata search identity: {value!r}")
        return value

    def query(self, request: MetadataSearchRequest) -> MetadataSearchPage:
        """
        Return complete count and page from the same execution population.

        Example:
            A page of 20 references can report total_count=100 without changing its limit.


        :param request: Typed filters, query text, ordering and pagination.
        :return: Native reference page with exact totals and execution provenance.
        """
        with self._owner._reading():
            self._owner._assert_attached()
            plan = metadata_requests.compile_request(
                self._owner._catalog.search, request
            )
            cache = self._owner._cache
            if cache is not None:
                # A pending transaction or failed recovery must never trigger fallback.
                cache._refresh_dirty()
                try:
                    result = MetadataReadSource.complete(cache.query(plan))
                except UNAVAILABLE:
                    if not self._owner._fallback:
                        raise
                else:
                    return self._page(request, result, cached=True)
            return self._page(request, self._database_page(plan), cached=False)

    def _page(
        self, request: MetadataSearchRequest, result: CacheQueryResult, *, cached: bool
    ) -> MetadataSearchPage:
        """
        Discard physical rows while preserving execution counts and provenance.

        Example:
            Database fallback reports source=database and no cache generation.


        :param request: Original semantic request describing level and paging.
        :param result: Complete physical execution result.
        :param cached: Whether the configured cache produced the result.
        :return: Metadata page with physical row payloads removed.
        """
        cache = self._owner._cache
        return MetadataSearchPage(
            level=request.level,
            references=tuple(
                MetadataReference(request.level, self._identity(row.row_id))
                for row in result.records
            ),
            total_count=result.total_count,
            limit=result.limit,
            offset=result.offset,
            source="cache" if cached else "database",
            consistency="snapshot"
            if cached
            and cache is not None
            and cache.capabilities.consistency == CacheConsistency.SNAPSHOT
            else "live",
            generation=result.generation if cached else None,
        )

    def find(
        self,
        query: str,
        *,
        level: WemiLevel = "work",
        limit: int | None = 100,
        offset: int = 0,
    ) -> tuple[MetadataReference, ...]:
        """
        Retain the ID-ordered reference convenience without requiring a total.

        Example:
            ``metadata.search.find("work.title:Dune")`` is the reference-only convenience.


        :param query: Native Catalog query text.
        :param level: WEMI result family.
        :param limit: Maximum references, or None for no cap.
        :param offset: Nonnegative number of identity-ordered results to skip.
        :return: Tuple of matching metadata references in identity order.
        """
        BaseRepository._validate_page(0 if limit is None else limit, offset)
        with self._owner._reading():
            source = self._owner._reads()
            search = self._owner._catalog.search
            plan = replace(
                search.compile(query, level=level), limit=limit, offset=offset
            )
            rows = source.rows(
                plan,
                lambda: search.find(query, level=level, limit=limit, offset=offset),
            )
            return tuple(
                MetadataReference(level, self._identity(row[f"{level}_id"]))
                for row in rows
            )


__all__ = ["MetadataSearch"]
