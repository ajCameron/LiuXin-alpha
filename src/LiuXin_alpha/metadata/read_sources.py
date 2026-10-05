"""
Adapt database and composed-cache reads for metadata hydration with explicit fallback and completeness rules.

Adapters retain their database/cache resources without closing them. Database reads
mostly pass through; cache records are wrapped as read-only Row objects associated
with the adapter.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/test_metadata_top_level_facade.py
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

from LiuXin_alpha.caches.api import (
    CacheAPI,
    CacheFilterOperator,
    CachePredicate,
    CacheQuery,
    CacheQueryResult,
    CacheSort,
    UnknownCacheFieldError,
    UnknownCacheTableError,
    UnsupportedCacheQueryError,
)
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.api.from_database_api.metadata_read_source_api import (
    MetadataLinkRowSequence,
    MetadataReadSourceAPI,
    MetadataRowSequence,
    MetadataSearchTerm,
    MetadataTableColumns,
)


class DatabaseMetadataReadSource:
    """
    Expose the metadata read surface of a retained live database without managing its lifetime.

    Example:
        >>> from types import SimpleNamespace
        >>> source = DatabaseMetadataReadSource(SimpleNamespace(driver_wrapper=None))
        >>> source.refresh()
        False
    """

    def __init__(self, database: Any) -> None:
        """
        Reject a missing database, retain it, and expose its driver_wrapper.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param database: Database-like source retained for metadata reads; this facade does
            not close it.
        :return: None; ValueError for None, or the underlying attribute error when
            driver_wrapper is unavailable.
        """
        if database is None:
            raise ValueError("DatabaseMetadataReadSource requires a database.")
        self.database = database
        self.driver_wrapper = database.driver_wrapper

    def get_tables(self, force_refresh: bool = False) -> Sequence[str]:
        """
        Forward the force-refresh option to the database table listing.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param force_refresh: Whether to refresh table metadata; ignored by the cache
            adapter.
        :return: Underlying sequence without runtime conversion; the cast only supplies
            typing information.
        """
        return cast(
            Sequence[str],
            self.database.get_tables(force_refresh=force_refresh),
        )

    def get_tables_and_columns(self) -> MetadataTableColumns:
        """
        Return the database’s table-to-columns mapping without copying it.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :return: Underlying metadata mapping.
        """
        return self.database.get_tables_and_columns()

    def get_column_headings(self, table: str) -> set[str]:
        """
        Delegate heading lookup without converting the returned collection.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param table: Table name forwarded to the backend, stringified for cache lookups.
        :return: Database-returned headings, statically cast to a set.
        """
        return cast(set[str], self.database.get_column_headings(table))

    def get_row_from_id(self, table: str, row_id: int) -> Row | None:
        """
        Delegate row lookup with the supplied table and ID.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param table: Table name forwarded to the backend, stringified for cache lookups.
        :param row_id: Requested row ID; cache lookups convert it with int.
        :return: Database result, conventionally a Row or None; no copy or sentinel
            conversion occurs.
        """
        return self.database.get_row_from_id(table, row_id)

    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = False,
    ) -> MetadataRowSequence:
        """
        Delegate whole-table reads and preserve the caller’s iterator preference.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param table: Table name forwarded to the backend, stringified for cache lookups.
        :param iterator_return: Whether to request an iterator from the database; ignored by
            the cache adapter.
        :return: Database-returned row collection or iterator.
        """
        return self.database.get_all_rows(
            table,
            iterator_return=iterator_return,
        )

    def get_record_count(self, table: str) -> int:
        """
        Read the database count and convert it to int.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param table: Table name forwarded to the backend, stringified for cache lookups.
        :return: Integer row count; backend or conversion errors propagate.
        """
        return int(self.database.get_record_count(table))

    def search(
        self,
        table: str,
        column: str,
        search_term: MetadataSearchTerm,
    ) -> MetadataRowSequence:
        """
        Forward the table, column, and search value directly to the database.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param table: Table name forwarded to the backend, stringified for cache lookups.
        :param column: Search column, stringified for structured cache queries.
        :param search_term: Equality-search value passed through unchanged.
        :return: Database-returned matching rows.
        """
        return self.database.search(table, column, search_term)

    def get_interlink_rows(
        self,
        primary_row: Any,
        secondary_table: str,
    ) -> MetadataLinkRowSequence:
        """
        Delegate retrieval of link rows for a primary row and secondary table.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param primary_row: Row-like object exposing table and row_id for the primary link
            endpoint.
        :param secondary_table: Target table whose links or related rows are requested.
        :return: Database-returned link-row sequence.
        """
        return self.database.get_interlink_rows(
            primary_row=primary_row,
            secondary_table=secondary_table,
        )

    def get_interlinked_rows(
        self,
        target_row: Any,
        secondary_table: str,
        type_filter: str | None = None,
    ) -> MetadataRowSequence:
        """
        Delegate related-row traversal and the optional relation-type filter.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param target_row: Row-like object exposing table and row_id for the source
            endpoint.
        :param secondary_table: Target table whose links or related rows are requested.
        :param type_filter: Optional relation-type filter forwarded unchanged.
        :return: Database-returned related rows.
        """
        return self.database.get_interlinked_rows(
            target_row=target_row,
            secondary_table=secondary_table,
            type_filter=type_filter,
        )

    def refresh(self) -> bool:
        """
        Report that this pass-through adapter performed no refresh.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :return: False; the database is untouched.
        """
        return False

    def __getattr__(self, name: str) -> Any:
        """
        Forward otherwise unresolved attribute access to the retained database.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param name: Missing adapter attribute name to resolve on the database.
        :return: Underlying attribute; AttributeError and descriptor errors propagate.
        """
        return getattr(self.database, name)


class CacheMetadataReadSource:
    """
    Expose metadata reads over CacheAPI, with an attached database for schema information and optional read fallback.

    Completed misses remain misses. Incomplete query results can fall back, while direct
    structured query_cache calls always remain strict.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_metadata_top_level_facade.py
    """

    def __init__(
        self,
        cache: CacheAPI | None,
        database: Any = None,
        *,
        allow_database_fallback: bool = True,
    ) -> None:
        """
        Require CacheAPI and resolve an attached database, rejecting conflicting explicit and attached database objects.

        A missing cache raises ValueError, an uncomposed storage object raises TypeError,
        and no resolved database raises ValueError even when read fallback is disabled.
        Retain the resources and expose the database driver_wrapper without loading or
        refreshing the cache.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param cache: Non-None composed CacheAPI facade; raw storage plugins are rejected.
        :param database: Optional explicit database; must be identical to any cache-attached
            database.
        :param allow_database_fallback: Policy flag retained as supplied for eligible
            missing/incomplete cache reads.
        :return: None.
        """
        if cache is None:
            raise ValueError("CacheMetadataReadSource requires a cache facade.")
        if not isinstance(cache, CacheAPI):
            raise TypeError(
                "CacheMetadataReadSource requires CacheAPI; compose storage "
                "plugins with LiuXin_alpha.caches.Cache first"
            )
        self.cache: CacheAPI = cache
        attached_database = getattr(cache, "database", None)
        if (
            database is not None
            and attached_database is not None
            and database is not attached_database
        ):
            raise ValueError(
                "CacheMetadataReadSource cache and fallback database must match"
            )
        resolved_database: Any = (
            database
            if database is not None
            else attached_database
        )
        if resolved_database is None:
            raise ValueError("CacheMetadataReadSource requires an attached database for schema metadata.")
        self.database: Any = resolved_database
        self.driver_wrapper = self.database.driver_wrapper
        self.allow_database_fallback = allow_database_fallback

    def refresh(self) -> bool:
        """
        Reload the cache unconditionally through its reload method.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :return: True after successful reload; failures propagate.
        """
        self.cache.reload()
        return True

    def query_cache(self, query: CacheQuery) -> CacheQueryResult:
        """
        Execute a structured query directly on the cache without database fallback.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param query: Structured cache query forwarded unchanged.
        :return: CacheQueryResult returned unchanged, including its completeness status.
        """

        return self.cache.query(query)

    def get_tables(self, force_refresh: bool = False) -> Sequence[str]:
        """
        Collect cache table names and optionally union database names before sorting their string forms.

        Ignore force_refresh and request database names without refresh. Suppress ordinary
        database-listing errors; cache errors propagate.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param force_refresh: Whether to refresh table metadata; ignored by the cache
            adapter.
        :return: Tuple of sorted string names; distinct original values may stringify
            identically.
        """
        del force_refresh
        names = set(self.cache.table_columns())
        if self.allow_database_fallback:
            try:
                names.update(self.database.get_tables(force_refresh=False))
            except Exception:
                pass
        return tuple(sorted(str(name) for name in names))

    def get_tables_and_columns(self) -> MetadataTableColumns:
        """
        Shallow-copy cache schema entries and fill missing table names from the database when fallback is enabled.

        Cache entries win. Database-only column sequences become tuples; cache sequences are
        retained. Suppress ordinary database augmentation errors, retaining any entries
        already added.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :return: New table-to-column-sequence dictionary.
        """
        out: dict[str, Sequence[str]] = dict(self.cache.table_columns())
        if self.allow_database_fallback:
            try:
                for table_name, columns in self.database.get_tables_and_columns().items():
                    out.setdefault(str(table_name), tuple(columns))
            except Exception:
                pass
        return out

    def get_column_headings(self, table: str) -> set[str]:
        """
        Return cached headings as a set, or consult the database for an absent table when fallback is enabled.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param table: Table name forwarded to the backend, stringified for cache lookups.
        :return: Cached set, underlying database headings without conversion, or an empty
            set when unavailable and fallback is disabled.
        """
        columns = self.cache.table_columns().get(str(table))
        if columns is not None:
            return set(columns)
        if self.allow_database_fallback:
            return cast(set[str], self.database.get_column_headings(table))
        return set()

    def get_row_from_id(self, table: str, row_id: int) -> Row | None:
        """
        Read a cache row and wrap a non-None hit as read-only metadata Row.

        Unknown field/table and unsupported-query errors permit database fallback. An
        incomplete miss also permits fallback; a complete miss remains None. Conversion and
        unrelated backend errors propagate.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param table: Table name forwarded to the backend, stringified for cache lookups.
        :param row_id: Requested row ID; cache lookups convert it with int.
        :return: Read-only cache Row, database result on fallback, or None.
        """
        try:
            lookup = self.cache.get(str(table), int(row_id))
        except (
            UnknownCacheFieldError,
            UnknownCacheTableError,
            UnsupportedCacheQueryError,
        ):
            if self.allow_database_fallback:
                return self.database.get_row_from_id(table, row_id)
            return None
        if lookup.is_hit and lookup.value is not None:
            return self._row_from_mapping(lookup.value)
        if not lookup.complete and self.allow_database_fallback:
            return self.database.get_row_from_id(table, row_id)
        return None

    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = False,
    ) -> MetadataRowSequence:
        """
        Query all cached rows while ignoring iterator_return.

        Unknown-table or unsupported-query errors permit fallback, as do incomplete results.
        Without fallback, return available cached records even when incomplete; handled
        query errors yield an empty tuple.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param table: Table name forwarded to the backend, stringified for cache lookups.
        :param iterator_return: Whether to request an iterator from the database; ignored by
            the cache adapter.
        :return: Tuple of read-only cache Rows, or database rows requested with
            iterator_return=False.
        """
        del iterator_return
        try:
            result = self.cache.query(CacheQuery(table=str(table)))
        except (UnknownCacheTableError, UnsupportedCacheQueryError):
            if self.allow_database_fallback:
                return self.database.get_all_rows(table, iterator_return=False)
            return ()
        if not result.complete and self.allow_database_fallback:
            return self.database.get_all_rows(table, iterator_return=False)
        return tuple(self._row_from_mapping(record) for record in result.records)

    def get_record_count(self, table: str) -> int:
        """
        Use a zero-limit cache query to obtain total_count, with optional fallback for unsupported or incomplete results.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param table: Table name forwarded to the backend, stringified for cache lookups.
        :return: Integer cache total or database count; handled query errors return zero
            when fallback is disabled.
        """
        try:
            result = self.cache.query(
                CacheQuery(table=str(table), limit=0)
            )
        except (UnknownCacheTableError, UnsupportedCacheQueryError):
            if self.allow_database_fallback:
                return int(self.database.get_record_count(table))
            return 0
        if not result.complete and self.allow_database_fallback:
            return int(self.database.get_record_count(table))
        return int(result.total_count)

    def search(
        self,
        table: str,
        column: str,
        search_term: MetadataSearchTerm,
    ) -> MetadataRowSequence:
        """
        Build an equality cache query sorted by the database-reported ID column.

        Unknown field/table or unsupported-query errors and incomplete results permit
        database fallback. Without fallback, preserve available partial records; handled
        errors return an empty tuple.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param table: Table name forwarded to the backend, stringified for cache lookups.
        :param column: Search column, stringified for structured cache queries.
        :param search_term: Equality-search value passed through unchanged.
        :return: Tuple of read-only cached matches or the database search result.
        """
        try:
            result = self.cache.query(
                CacheQuery(
                    table=str(table),
                    predicates=(
                        CachePredicate(
                            str(column),
                            CacheFilterOperator.EQ,
                            search_term,
                        ),
                    ),
                    sort=(CacheSort(self.database.driver_wrapper.get_id_column(table)),),
                )
            )
        except (
            UnknownCacheFieldError,
            UnknownCacheTableError,
            UnsupportedCacheQueryError,
        ):
            if self.allow_database_fallback:
                return self.database.search(table, column, search_term)
            return ()
        if not result.complete and self.allow_database_fallback:
            return self.database.search(table, column, search_term)
        return tuple(self._row_from_mapping(record) for record in result.records)

    def get_interlink_rows(
        self,
        primary_row: Any,
        secondary_table: str,
    ) -> MetadataLinkRowSequence:
        """
        Fetch cached link records for a primary row, returning immediately when its ID is None.

        Only KeyError marks the link lookup unavailable and permits fallback. An empty
        successful lookup remains empty; nonempty results are wrapped as read-only Rows.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param primary_row: Row-like object exposing table and row_id for the primary link
            endpoint.
        :param secondary_table: Target table whose links or related rows are requested.
        :return: Cached Row tuple, database link rows on eligible fallback, or an empty
            tuple.
        """
        primary_table = str(primary_row.table)
        primary_id = primary_row.row_id
        if primary_id is None:
            return ()

        try:
            records = self.cache.link_records(
                primary_table,
                int(primary_id),
                str(secondary_table),
            )
        except KeyError:
            records = ()
            unavailable = True
        else:
            unavailable = False

        if records:
            return tuple(self._row_from_mapping(record) for record in records)
        if unavailable and self.allow_database_fallback:
            return self.database.get_interlink_rows(
                primary_row=primary_row,
                secondary_table=secondary_table,
            )
        return ()

    def get_interlinked_rows(
        self,
        target_row: Any,
        secondary_table: str,
        type_filter: str | None = None,
    ) -> MetadataRowSequence:
        """
        Traverse cached relations for a target ID and optional type, or return empty when the ID is None.

        KeyError and incomplete results permit database fallback. With fallback disabled,
        incomplete results retain their available records and KeyError returns empty.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param target_row: Row-like object exposing table and row_id for the source
            endpoint.
        :param secondary_table: Target table whose links or related rows are requested.
        :param type_filter: Optional relation-type filter forwarded unchanged.
        :return: Tuple of read-only cached related rows, a database result, or an empty
            tuple.
        """
        target_id = target_row.row_id
        if target_id is None:
            return ()
        try:
            result = self.cache.related(
                str(target_row.table),
                (int(target_id),),
                str(secondary_table),
                type_filter=type_filter,
            )
        except KeyError:
            if self.allow_database_fallback:
                return self.database.get_interlinked_rows(
                    target_row=target_row,
                    secondary_table=secondary_table,
                    type_filter=type_filter,
                )
            return ()
        if not result.complete and self.allow_database_fallback:
            return self.database.get_interlinked_rows(
                target_row=target_row,
                secondary_table=secondary_table,
                type_filter=type_filter,
            )
        return tuple(self._row_from_mapping(record) for record in result.records)

    def _row_from_mapping(self, row: Any) -> Row:
        """
        Copy a mapping, or a Row’s row_dict, into a new read-only Row associated with this adapter.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


        :param row: Mapping-like cache record or existing Row to copy shallowly.
        :return: New Row; invalid mapping or schema inputs can raise during construction.
        """
        mapping = row.row_dict if isinstance(row, Row) else row
        return Row(
            database=cast(Any, self),
            row_dict=dict(mapping),
            read_only=True,
        )


def metadata_read_source_from(source: Any) -> MetadataReadSourceAPI:
    """
    Keep an existing adapter, wrap CacheAPI with default fallback enabled, or treat the input as a database.

    The factory recognizes these concrete adapter classes; it does not preserve
    arbitrary structural read-source objects by protocol checking.

    Example:
        >>> from types import SimpleNamespace
        >>> original = DatabaseMetadataReadSource(SimpleNamespace(driver_wrapper=None))
        >>> metadata_read_source_from(original) is original
        True


    :param source: Existing adapter, composed CacheAPI facade, or database-like object.
    :return: Metadata read adapter; construction validation and attribute errors
        propagate.
    """
    if isinstance(source, (DatabaseMetadataReadSource, CacheMetadataReadSource)):
        return cast(MetadataReadSourceAPI, source)
    if isinstance(source, CacheAPI):
        return cast(
            MetadataReadSourceAPI,
            CacheMetadataReadSource(source),
        )
    return cast(
        MetadataReadSourceAPI,
        DatabaseMetadataReadSource(source),
    )


__all__ = [
    "CacheMetadataReadSource",
    "DatabaseMetadataReadSource",
    "metadata_read_source_from",
]
