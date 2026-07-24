"""Database/cache read-source adapters for metadata hydration."""

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
    """Pass-through adapter for a live database object."""

    def __init__(self, database: Any) -> None:
        if database is None:
            raise ValueError("DatabaseMetadataReadSource requires a database.")
        self.database = database
        self.driver_wrapper = database.driver_wrapper

    def get_tables(self, force_refresh: bool = False) -> Sequence[str]:
        return cast(
            Sequence[str],
            self.database.get_tables(force_refresh=force_refresh),
        )

    def get_tables_and_columns(self) -> MetadataTableColumns:
        return self.database.get_tables_and_columns()

    def get_column_headings(self, table: str) -> set[str]:
        return cast(set[str], self.database.get_column_headings(table))

    def get_row_from_id(self, table: str, row_id: int) -> Row | None:
        return self.database.get_row_from_id(table, row_id)

    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = False,
    ) -> MetadataRowSequence:
        return self.database.get_all_rows(
            table,
            iterator_return=iterator_return,
        )

    def get_record_count(self, table: str) -> int:
        return int(self.database.get_record_count(table))

    def search(
        self,
        table: str,
        column: str,
        search_term: MetadataSearchTerm,
    ) -> MetadataRowSequence:
        return self.database.search(table, column, search_term)

    def get_interlink_rows(
        self,
        primary_row: Any,
        secondary_table: str,
    ) -> MetadataLinkRowSequence:
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
        return self.database.get_interlinked_rows(
            target_row=target_row,
            secondary_table=secondary_table,
            type_filter=type_filter,
        )

    def refresh(self) -> bool:
        return False

    def __getattr__(self, name: str) -> Any:
        return getattr(self.database, name)


class CacheMetadataReadSource:
    """
    Metadata read-source adapter over the modern composed cache.

    The adapter intentionally exposes the same small read surface the hydrators
    use from a database. The core cache never performs implicit database
    fallback; this adapter owns the explicit fallback policy.
    """

    def __init__(
        self,
        cache: CacheAPI | None,
        database: Any = None,
        *,
        allow_database_fallback: bool = True,
    ) -> None:
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
        self.cache.reload()
        return True

    def query_cache(self, query: CacheQuery) -> CacheQueryResult:
        """Execute a strict structured query for higher-level read models."""

        return self.cache.query(query)

    def get_tables(self, force_refresh: bool = False) -> Sequence[str]:
        del force_refresh
        names = set(self.cache.table_columns())
        if self.allow_database_fallback:
            try:
                names.update(self.database.get_tables(force_refresh=False))
            except Exception:
                pass
        return tuple(sorted(str(name) for name in names))

    def get_tables_and_columns(self) -> MetadataTableColumns:
        out: dict[str, Sequence[str]] = dict(self.cache.table_columns())
        if self.allow_database_fallback:
            try:
                for table_name, columns in self.database.get_tables_and_columns().items():
                    out.setdefault(str(table_name), tuple(columns))
            except Exception:
                pass
        return out

    def get_column_headings(self, table: str) -> set[str]:
        columns = self.cache.table_columns().get(str(table))
        if columns is not None:
            return set(columns)
        if self.allow_database_fallback:
            return cast(set[str], self.database.get_column_headings(table))
        return set()

    def get_row_from_id(self, table: str, row_id: int) -> Row | None:
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
        mapping = row.row_dict if isinstance(row, Row) else row
        return Row(
            database=cast(Any, self),
            row_dict=dict(mapping),
            read_only=True,
        )


def metadata_read_source_from(source: Any) -> MetadataReadSourceAPI:
    """Return a metadata read source for a database, cache, or existing source."""
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
