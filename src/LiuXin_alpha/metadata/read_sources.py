"""Database/cache read-source adapters for metadata hydration."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

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
        return self.database.get_tables(force_refresh=force_refresh)

    def get_tables_and_columns(self) -> MetadataTableColumns:
        return self.database.get_tables_and_columns()

    def get_column_headings(self, table: str) -> set[str]:
        return self.database.get_column_headings(table)

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
        primary_row: Row,
        secondary_table: str,
    ) -> MetadataLinkRowSequence:
        return self.database.get_interlink_rows(
            primary_row=primary_row,
            secondary_table=secondary_table,
        )

    def get_interlinked_rows(
        self,
        target_row: Row,
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
    Metadata read-source adapter over a loaded storage cache.

    The adapter intentionally exposes the same small read surface the hydrators
    use from a database. Writes are not routed through this object.
    """

    def __init__(
        self,
        cache: Any,
        database: Any = None,
        *,
        allow_database_fallback: bool = True,
    ) -> None:
        if cache is None:
            raise ValueError("CacheMetadataReadSource requires a storage cache.")
        self.cache = cache
        self.database = database if database is not None else getattr(cache, "db", None)
        if self.database is None:
            raise ValueError("CacheMetadataReadSource requires an attached database for schema metadata.")
        self.driver_wrapper = self.database.driver_wrapper
        self.allow_database_fallback = allow_database_fallback
        assert_ready = getattr(cache, "assert_ready", None)
        if callable(assert_ready):
            assert_ready()

    def refresh(self) -> bool:
        loader = getattr(self.cache, "reload", None)
        if not callable(loader):
            loader = getattr(self.cache, "read", None)
        if not callable(loader):
            return False
        loader()
        assert_ready = getattr(self.cache, "assert_ready", None)
        if callable(assert_ready):
            assert_ready()
        return True

    def get_tables(self, force_refresh: bool = False) -> Sequence[str]:
        del force_refresh
        names = set(getattr(self.cache, "main_tables", {}).keys())
        if self.allow_database_fallback:
            try:
                names.update(self.database.get_tables(force_refresh=False))
            except Exception:
                pass
        return tuple(sorted(str(name) for name in names))

    def get_tables_and_columns(self) -> MetadataTableColumns:
        out: dict[str, Sequence[str]] = {}
        for table_name, table_cache in getattr(self.cache, "main_tables", {}).items():
            headings = getattr(table_cache, "column_headings", None)
            out[str(table_name)] = tuple(headings if headings is not None else ())
        if self.allow_database_fallback:
            try:
                for table_name, columns in self.database.get_tables_and_columns().items():
                    out.setdefault(str(table_name), tuple(columns))
            except Exception:
                pass
        return out

    def get_column_headings(self, table: str) -> set[str]:
        table_cache = self._get_main_table(table)
        if table_cache is not None:
            headings = getattr(table_cache, "column_headings", None)
            if headings is not None:
                return set(headings)
        if self.allow_database_fallback:
            return self.database.get_column_headings(table)
        return set()

    def get_row_from_id(self, table: str, row_id: int) -> Row | None:
        table_cache = self._get_main_table(table)
        if table_cache is not None:
            try:
                snapshot = table_cache.get_row_snapshot(int(row_id))
            except Exception:
                snapshot = None
            if snapshot is not None:
                return self._row_from_mapping(snapshot)
            try:
                row = table_cache.get_row(int(row_id))
            except Exception:
                row = None
            if row is not None:
                return self._row_from_mapping(row.row_dict if isinstance(row, Row) else row)
        if self.allow_database_fallback:
            return self.database.get_row_from_id(table, row_id)
        return None

    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = False,
    ) -> MetadataRowSequence:
        del iterator_return
        table_cache = self._get_main_table(table)
        if table_cache is not None:
            row_ids = self._cached_row_ids(table_cache)
            if row_ids is not None:
                return tuple(
                    row
                    for row_id in row_ids
                    if (row := self.get_row_from_id(table, row_id)) is not None
                )
        if self.allow_database_fallback:
            return self.database.get_all_rows(table, iterator_return=False)
        return ()

    def get_record_count(self, table: str) -> int:
        table_cache = self._get_main_table(table)
        if table_cache is not None:
            row_ids = self._cached_row_ids(table_cache)
            if row_ids is not None:
                return len(row_ids)
        if self.allow_database_fallback:
            return int(self.database.get_record_count(table))
        return 0

    def search(
        self,
        table: str,
        column: str,
        search_term: MetadataSearchTerm,
    ) -> MetadataRowSequence:
        table_cache = self._get_main_table(table)
        if table_cache is not None:
            ids: set[int] | None = None
            getter = getattr(table_cache, "get_ids_for_value", None)
            if callable(getter):
                try:
                    ids = {int(row_id) for row_id in getter(column, search_term)}
                except Exception:
                    ids = None
            if ids is not None:
                return [
                    row
                    for row_id in sorted(ids)
                    if (row := self.get_row_from_id(table, row_id)) is not None
                ]
        if self.allow_database_fallback:
            return self.database.search(table, column, search_term)
        return ()

    def get_interlink_rows(
        self,
        primary_row: Row,
        secondary_table: str,
    ) -> MetadataLinkRowSequence:
        primary_table = str(primary_row.table)
        primary_id = primary_row.row_id
        if primary_id is None:
            return ()

        source_getter = getattr(self.cache, "get_link_rows_for_source", None)
        if callable(source_getter):
            try:
                return tuple(
                    source_getter(
                        primary_table,
                        int(primary_id),
                        str(secondary_table),
                        require_ordering=True,
                    )
                )
            except KeyError:
                pass
            except TypeError:
                try:
                    return tuple(source_getter(primary_table, int(primary_id), str(secondary_table)))
                except KeyError:
                    pass
                except TypeError:
                    pass

        link_table = self._get_link_table(primary_table, secondary_table)
        if link_table is not None:
            getter = getattr(link_table, "get_link_rows_for_src", None)
            if callable(getter):
                try:
                    return tuple(getter(int(primary_id), require_ordering=True))
                except TypeError:
                    return tuple(getter(int(primary_id)))
                except Exception:
                    pass

        reverse_link_table = self._get_link_table(secondary_table, primary_table)
        if reverse_link_table is not None:
            getter = getattr(reverse_link_table, "get_link_rows_for_dst", None)
            if callable(getter):
                try:
                    return tuple(getter(int(primary_id), require_ordering=True))
                except TypeError:
                    return tuple(getter(int(primary_id)))
                except Exception:
                    pass

        if self.allow_database_fallback:
            return self.database.get_interlink_rows(
                primary_row=primary_row,
                secondary_table=secondary_table,
            )
        return ()

    def get_interlinked_rows(
        self,
        target_row: Row,
        secondary_table: str,
        type_filter: str | None = None,
    ) -> MetadataRowSequence:
        try:
            link_rows = list(
                self.get_interlink_rows(
                    primary_row=target_row,
                    secondary_table=secondary_table,
                )
            )
        except Exception:
            if self.allow_database_fallback:
                return self.database.get_interlinked_rows(
                    target_row=target_row,
                    secondary_table=secondary_table,
                    type_filter=type_filter,
                )
            return ()

        if not link_rows:
            return ()

        try:
            secondary_id_column = self.driver_wrapper.get_id_column(secondary_table)
            secondary_link_column = self.driver_wrapper.get_link_column(
                target_row.table,
                secondary_table,
                secondary_id_column,
            )
        except Exception:
            return ()

        type_column = None
        if type_filter is not None:
            try:
                type_column = self.driver_wrapper.get_link_column(
                    target_row.table,
                    secondary_table,
                    "type",
                )
            except Exception:
                type_column = None

        rows: list[Row] = []
        for link_row in link_rows:
            if type_column is not None and _mapping_value(link_row, type_column) != type_filter:
                continue
            linked_row_id = _mapping_value(link_row, secondary_link_column)
            if linked_row_id in (None, ""):
                continue
            try:
                linked_row = self.get_row_from_id(secondary_table, int(linked_row_id))
            except Exception:
                linked_row = None
            if linked_row is not None:
                rows.append(linked_row)
        return tuple(rows)

    def _get_main_table(self, table: str) -> Any | None:
        getter = getattr(self.cache, "get_main_table", None)
        if not callable(getter):
            return getattr(self.cache, "main_tables", {}).get(str(table))
        try:
            return getter(str(table))
        except Exception:
            return None

    def _get_link_table(self, primary_table: str, secondary_table: str) -> Any | None:
        getter = getattr(self.cache, "get_link_table", None)
        if not callable(getter):
            return getattr(self.cache, "link_tables", {}).get((str(primary_table), str(secondary_table)))
        try:
            return getter(str(primary_table), str(secondary_table))
        except Exception:
            return None

    @staticmethod
    def _cached_row_ids(table_cache: Any) -> tuple[int, ...] | None:
        row_ids = getattr(table_cache, "row_ids", None)
        if row_ids is not None:
            try:
                return tuple(int(row_id) for row_id in row_ids)
            except Exception:
                return None
        rows = getattr(table_cache, "_rows", None)
        if isinstance(rows, Mapping):
            try:
                return tuple(sorted(int(row_id) for row_id in rows.keys()))
            except Exception:
                return None
        rows_by_id = getattr(table_cache, "_rows_by_id", None)
        if isinstance(rows_by_id, Mapping):
            try:
                return tuple(sorted(int(row_id) for row_id in rows_by_id.keys()))
            except Exception:
                return None
        return None

    def _row_from_mapping(self, row: Any) -> Row:
        mapping = row.row_dict if isinstance(row, Row) else row
        return Row(database=self, row_dict=dict(mapping), read_only=True)


def _mapping_value(row: Any, column: str) -> Any:
    if isinstance(row, Row):
        return row[column]
    if isinstance(row, Mapping):
        return row.get(column)
    try:
        return row[column]
    except Exception:
        return getattr(row, column, None)


def metadata_read_source_from(source: Any) -> MetadataReadSourceAPI:
    """Return a metadata read source for a database, cache, or existing source."""
    if isinstance(source, (DatabaseMetadataReadSource, CacheMetadataReadSource)):
        return source
    if hasattr(source, "get_main_table") and hasattr(source, "get_link_table"):
        return CacheMetadataReadSource(source)
    return DatabaseMetadataReadSource(source)


__all__ = [
    "CacheMetadataReadSource",
    "DatabaseMetadataReadSource",
    "metadata_read_source_from",
]
