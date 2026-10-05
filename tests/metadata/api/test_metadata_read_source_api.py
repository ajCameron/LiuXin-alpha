"""
Verify database and cache read-source contracts, completeness and fallback behavior.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test metadata read source api through its owning regression module::

        python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

from LiuXin_alpha.caches import (
    CacheAPI,
    CacheCapabilities,
    CacheConsistency,
    CacheFilterOperator,
    CacheLookup,
    CacheLookupStatus,
    CacheQuery,
    CacheQueryResult,
    CacheRecord,
    CacheState,
    UnknownCacheFieldError,
    UnknownCacheTableError,
    UnsupportedCacheQueryError,
)

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata import (
    CacheMetadataReadSource,
    DatabaseMetadataReadSource,
    metadata_read_source_from,
)
from LiuXin_alpha.metadata import __all__ as metadata_facade_all
from LiuXin_alpha.metadata.api import (
    MetadataDriverWrapperAPI,
    MetadataReadSourceAPI,
)
from LiuXin_alpha.metadata.api import __all__ as metadata_api_all


class _DriverWrapper:
    """
    Provide the schema and link-name behavior needed by read-source contract tests.

    Example:
        Exercise DriverWrapper through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py
    """
    _tables = {
        "works": ("work_id", "work_title"),
        "tags": ("tag_id", "tag"),
    }

    def get_allowed_tables_snapshot(self) -> list[str]:
        """
        Return the immutable table set advertised by the test driver.

        Example:
            Exercise DriverWrapper.get allowed tables snapshot through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return list(self._tables)

    def identify_table_from_row_dict(self, row_dict: dict[str, Any]) -> str:
        """
        Infer a test table name from the row's identifying columns.

        Example:
            Exercise DriverWrapper.identify table from row dict through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param row_dict: Value supplied for row dict in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        if "work_id" in row_dict or "work_title" in row_dict:
            return "works"
        if "tag_id" in row_dict or "tag" in row_dict:
            return "tags"
        raise ValueError("unknown row")

    def get_id_column(self, table: str) -> str:
        """
        Return the configured identity column for a table.

        Example:
            Exercise DriverWrapper.get id column through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table: Table name addressed by the test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return {"works": "work_id", "tags": "tag_id"}[str(table)]

    def check_for_intralink_table(self, table: str) -> bool:
        """
        Return whether the named test table represents a self-link.

        Example:
            Exercise DriverWrapper.check for intralink table through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table: Table name addressed by the test operation.
        :return: True when the tested condition is satisfied; otherwise False.
        """
        return False

    def get_interlinked_tables(self, table: str) -> list[str]:
        """
        Return the table pair connected by a test link table.

        Example:
            Exercise DriverWrapper.get interlinked tables through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table: Table name addressed by the test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ["tags"] if str(table) == "works" else []

    def get_link_table_name(self, table1: str, table2: str) -> str:
        """
        Return the deterministic link-table name for two entity tables.

        Example:
            Exercise DriverWrapper.get link table name through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table1: Value supplied for table1 in the focused test operation.
        :param table2: Value supplied for table2 in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        left, right = sorted((str(table1).rstrip("s"), str(table2).rstrip("s")))
        return f"{left}_{right}_links"

    def get_column_base(self, table_name: str) -> str:
        """
        Return the entity base represented by a link-column name.

        Example:
            Exercise DriverWrapper.get column base through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table_name: Table name addressed by the test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return str(table_name).removesuffix("s")

    def get_link_column(
        self,
        table1: str,
        table2: str,
        secondary_id_column: str,
    ) -> str:
        """
        Return the link-column name associated with an entity table.

        Example:
            Exercise DriverWrapper.get link column through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table1: Value supplied for table1 in the focused test operation.
        :param table2: Value supplied for table2 in the focused test operation.
        :param secondary_id_column: Value supplied for secondary id column in the focused
            test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        link_table = self.get_link_table_name(table1, table2)
        return f"{self.get_column_base(link_table)}_{secondary_id_column}"


class _Database:
    """
    Provide an in-memory database double with explicit row and relation queries.

    Example:
        Exercise Database through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py
    """
    def __init__(self) -> None:
        """
        Initialize the Database test double.

        Example:
            Exercise Database.init through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :return: None; the function records state or raises through its assertions.
        """
        self.driver_wrapper = _DriverWrapper()
        self.rows_by_table: dict[str, list[Row]] = {
            "works": [],
            "tags": [],
        }
        self.links_by_source: dict[tuple[str, int, str], list[dict[str, int]]] = {}

    def add_row(self, table: str, payload: dict[str, Any]) -> Row:
        """
        Insert a copied row into the in-memory table and return its identity.

        Example:
            Exercise Database.add row through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table: Table name addressed by the test operation.
        :param payload: Value supplied for payload in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        row = Row(self, row_dict=payload, read_only=True)
        self.rows_by_table[str(table)].append(row)
        return row

    def add_link(self, work_id: int, tag_id: int) -> None:
        """
        Insert a deterministic relation row between two stored identities.

        Example:
            Exercise Database.add link through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param work_id: Value supplied for work id in the focused test operation.
        :param tag_id: Value supplied for tag id in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        primary_column = self.driver_wrapper.get_link_column(
            "works",
            "tags",
            "work_id",
        )
        secondary_column = self.driver_wrapper.get_link_column(
            "works",
            "tags",
            "tag_id",
        )
        self.links_by_source.setdefault(("works", int(work_id), "tags"), []).append(
            {
                primary_column: int(work_id),
                secondary_column: int(tag_id),
            }
        )

    def get_tables(self, force_refresh: bool = False) -> list[str]:
        """
        Return the table names exposed by the in-memory schema.

        Example:
            Exercise Database.get tables through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param force_refresh: Value supplied for force refresh in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        del force_refresh
        return list(self.rows_by_table)

    def get_tables_and_columns(self) -> dict[str, tuple[str, ...]]:
        """
        Return a copied schema mapping for discovery tests.

        Example:
            Exercise Database.get tables and columns through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return dict(self.driver_wrapper._tables)

    def get_column_headings(self, table: str) -> set[str]:
        """
        Return the known column names for a test table.

        Example:
            Exercise Database.get column headings through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table: Table name addressed by the test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return set(self.driver_wrapper._tables[str(table)])

    def get_row_from_id(self, table: str, row_id: int) -> Row | None:
        """
        Return a copied row for the requested identity, or the test double's miss value.

        Example:
            Exercise Database.get row from id through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table: Table name addressed by the test operation.
        :param row_id: Identity of the row to retrieve or mutate.
        :return: The deterministic value, row, identity or collection described above.
        """
        id_column = self.driver_wrapper.get_id_column(table)
        for row in self.rows_by_table[str(table)]:
            if row.row_dict.get(id_column) == int(row_id):
                return row
        return None

    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = False,
    ) -> list[Row]:
        """
        Return copied rows from the requested in-memory table.

        Example:
            Exercise Database.get all rows through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table: Table name addressed by the test operation.
        :param iterator_return: Value supplied for iterator return in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        del iterator_return
        return list(self.rows_by_table[str(table)])

    def get_record_count(self, table: str) -> int:
        """
        Return the number of rows stored in a test table.

        Example:
            Exercise Database.get record count through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table: Table name addressed by the test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return len(self.rows_by_table[str(table)])

    def search(self, table: str, column: str, search_term: Any) -> list[Row]:
        """
        Return rows whose selected column satisfies the test query.

        Example:
            Exercise Database.search through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table: Table name addressed by the test operation.
        :param column: Column name inspected, searched or updated.
        :param search_term: Value supplied for search term in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return [
            row
            for row in self.rows_by_table[str(table)]
            if row.row_dict.get(str(column)) == search_term
        ]

    def get_interlink_rows(
        self,
        primary_row: Row,
        secondary_table: str,
    ) -> list[dict[str, int]]:
        """
        Return relation rows matching the supplied source and destination filters.

        Example:
            Exercise Database.get interlink rows through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param primary_row: Value supplied for primary row in the focused test operation.
        :param secondary_table: Value supplied for secondary table in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return list(
            self.links_by_source.get(
                (str(primary_row.table), int(primary_row.row_id), str(secondary_table)),
                [],
            )
        )

    def get_interlinked_rows(
        self,
        target_row: Row,
        secondary_table: str,
        type_filter: str | None = None,
    ) -> list[Row]:
        """
        Resolve and return rows related through the requested link table.

        Example:
            Exercise Database.get interlinked rows through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param target_row: Value supplied for target row in the focused test operation.
        :param secondary_table: Value supplied for secondary table in the focused test
            operation.
        :param type_filter: Value supplied for type filter in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        del type_filter
        secondary_column = self.driver_wrapper.get_link_column(
            target_row.table,
            secondary_table,
            self.driver_wrapper.get_id_column(secondary_table),
        )
        linked_rows: list[Row] = []
        for link_row in self.get_interlink_rows(target_row, secondary_table):
            linked_row = self.get_row_from_id(secondary_table, link_row[secondary_column])
            if linked_row is not None:
                linked_rows.append(linked_row)
        return linked_rows


class _CacheMainTable:
    """
    Model one cached main table for exact-value and row-snapshot reads.

    Example:
        Exercise CacheMainTable through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py
    """
    def __init__(self, database: _Database, table: str) -> None:
        """
        Initialize the CacheMainTable test double.

        Example:
            Exercise CacheMainTable.init through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param database: Database double or adapter under test.
        :param table: Table name addressed by the test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self.column_headings = tuple(database.driver_wrapper._tables[str(table)])
        self._rows = {
            int(row.row_id): dict(row.row_dict)
            for row in database.rows_by_table[str(table)]
        }
        self.row_ids = tuple(sorted(self._rows))

    def get_row_snapshot(self, row_id: int) -> dict[str, Any]:
        """
        Return the cached snapshot for one row identity.

        Example:
            Exercise CacheMainTable.get row snapshot through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param row_id: Identity of the row to retrieve or mutate.
        :return: The deterministic value, row, identity or collection described above.
        """
        return dict(self._rows[int(row_id)])

    def get_ids_for_value(self, column: str, value: Any) -> list[int]:
        """
        Return cached identities matching an exact field value.

        Example:
            Exercise CacheMainTable.get ids for value through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param column: Column name inspected, searched or updated.
        :param value: Value stored, compared or projected by the operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return [
            row_id
            for row_id, row in self._rows.items()
            if row.get(str(column)) == value
        ]


class _CacheLinkTable:
    """
    Model cached link rows keyed by their source identity.

    Example:
        Exercise CacheLinkTable through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py
    """
    def __init__(self, links_by_source_id: dict[int, list[dict[str, int]]]) -> None:
        """
        Initialize the CacheLinkTable test double.

        Example:
            Exercise CacheLinkTable.init through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param links_by_source_id: Value supplied for links by source id in the focused test
            operation.
        :return: None; the function records state or raises through its assertions.
        """
        self.links_by_source_id = {
            source_id: [dict(link) for link in links]
            for source_id, links in links_by_source_id.items()
        }

    def get_link_rows_for_src(
        self,
        source_id: int,
        require_ordering: bool = False,
    ) -> list[dict[str, int]]:
        """
        Return cached relation rows for one source identity.

        Example:
            Exercise CacheLinkTable.get link rows for src through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param source_id: Value supplied for source id in the focused test operation.
        :param require_ordering: Value supplied for require ordering in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        del require_ordering
        return [
            dict(link)
            for link in self.links_by_source_id.get(int(source_id), [])
        ]


class _Cache(CacheAPI):
    """
    Provide a configurable cache double that records fallback-sensitive operations.

    Example:
        Exercise Cache through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py
    """
    def __init__(self, database: _Database) -> None:
        """
        Initialize the Cache test double.

        Example:
            Exercise Cache.init through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param database: Database double or adapter under test.
        :return: None; the function records state or raises through its assertions.
        """
        self.database = database
        self.db = database
        self.storage = self
        self.reloaded = False
        self.main_tables = {
            table: _CacheMainTable(database, table)
            for table in database.rows_by_table
        }
        self.link_tables = {
            ("works", "tags"): _CacheLinkTable(
                {
                    source_id: links
                    for (
                        primary_table,
                        source_id,
                        secondary_table,
                    ), links in database.links_by_source.items()
                    if (primary_table, secondary_table) == ("works", "tags")
                }
            )
        }

    def assert_ready(self) -> None:
        """
        Assert that the cache double is available for reads.

        Example:
            Exercise Cache.assert ready through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :return: None; the function records state or raises through its assertions.
        """
        return None

    def reload(self) -> None:
        """
        Record a cache reload and advance the test generation.

        Example:
            Exercise Cache.reload through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :return: None; the function records state or raises through its assertions.
        """
        self.reloaded = True

    def clear(self) -> None:
        """
        Clear cached test data while preserving the configured cache contract.

        Example:
            Exercise Cache.clear through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :return: None; the function records state or raises through its assertions.
        """
        return None

    def close(self) -> None:
        """
        Mark the cache double closed for lifecycle assertions.

        Example:
            Exercise Cache.close through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :return: None; the function records state or raises through its assertions.
        """
        return None

    def table_columns(self) -> Mapping[str, tuple[str, ...]]:
        """
        Return cached column names for a table.

        Example:
            Exercise Cache.table columns through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return {
            str(table_name): tuple(
                str(column) for column in table.column_headings
            )
            for table_name, table in self.main_tables.items()
        }

    @property
    def state(self) -> CacheState:
        """
        Return the cache lifecycle state exposed to the adapter.

        Example:
            Exercise Cache.state through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return CacheState.READY

    @property
    def generation(self) -> int:
        """
        Return the cache generation used to detect refreshes.

        Example:
            Exercise Cache.generation through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return 1

    @property
    def capabilities(self) -> CacheCapabilities:
        """
        Return the read capabilities advertised by the cache double.

        Example:
            Exercise Cache.capabilities through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return CacheCapabilities(
            consistency=CacheConsistency.SNAPSHOT,
            live_child_objects=False,
            vectorized_helpers=False,
        )

    def get(self, table: str, row_id: int) -> CacheLookup[CacheRecord]:
        """
        Perform the get test-helper operation with deterministic inputs.

        Example:
            Exercise Cache.get through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table: Table name addressed by the test operation.
        :param row_id: Identity of the row to retrieve or mutate.
        :return: The deterministic value, row, identity or collection described above.
        """
        table_cache = self.main_tables.get(str(table))
        if table_cache is None or int(row_id) not in table_cache.row_ids:
            return CacheLookup(
                CacheLookupStatus.MISS,
                None,
                True,
                self.generation,
            )
        return CacheLookup(
            CacheLookupStatus.HIT,
            CacheRecord(
                str(table),
                int(row_id),
                table_cache.get_row_snapshot(int(row_id)),
            ),
            True,
            self.generation,
        )

    def query(self, query: CacheQuery) -> CacheQueryResult:
        """
        Return a structured cached query result with explicit completeness.

        Example:
            Exercise Cache.query through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param query: Query value or structured selector exercised by the test.
        :return: The deterministic value, row, identity or collection described above.
        """
        table_cache = self.main_tables[str(query.table)]
        records = []
        for row_id in table_cache.row_ids:
            snapshot = table_cache.get_row_snapshot(row_id)
            if any(
                snapshot.get(predicate.field) != predicate.value
                for predicate in query.predicates
            ):
                continue
            records.append(CacheRecord(query.table, row_id, snapshot))
        total = len(records)
        end = None if query.limit is None else query.offset + query.limit
        return CacheQueryResult(
            tuple(records[query.offset:end]),
            total,
            query.offset,
            query.limit,
            True,
            self.generation,
        )

    def related(
        self,
        source_table: str,
        source_ids: Iterable[int],
        target_table: str,
        *,
        type_filter: str | None = None,
    ) -> CacheQueryResult:
        """
        Return cached related identities with explicit completeness.

        Example:
            Exercise Cache.related through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param source_table: Value supplied for source table in the focused test operation.
        :param source_ids: Value supplied for source ids in the focused test operation.
        :param target_table: Value supplied for target table in the focused test operation.
        :param type_filter: Value supplied for type filter in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        del type_filter
        secondary_column = self.database.driver_wrapper.get_link_column(
            source_table,
            target_table,
            self.database.driver_wrapper.get_id_column(target_table),
        )
        ids = []
        for source_id in source_ids:
            for link in self.link_tables[
                (str(source_table), str(target_table))
            ].get_link_rows_for_src(int(source_id)):
                ids.append(int(link[secondary_column]))
        records = tuple(
            self.get(target_table, row_id).value
            for row_id in ids
            if self.get(target_table, row_id).value is not None
        )
        return CacheQueryResult(records, len(records), 0, None, True, self.generation)

    def link_records(
        self,
        source_table: str,
        source_id: int,
        target_table: str,
        *,
        type_filter: str | None = None,
    ) -> tuple[CacheRecord, ...]:
        """
        Return cached link records with explicit availability semantics.

        Example:
            Exercise Cache.link records through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param source_table: Value supplied for source table in the focused test operation.
        :param source_id: Value supplied for source id in the focused test operation.
        :param target_table: Value supplied for target table in the focused test operation.
        :param type_filter: Value supplied for type filter in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        del type_filter
        return tuple(
            CacheRecord("links", -(index + 1), row)
            for index, row in enumerate(
                self.link_tables[
                    (str(source_table), str(target_table))
                ].get_link_rows_for_src(int(source_id))
            )
        )

    def load(self) -> None:
        """
        Load deterministic cache state for adapter tests.

        Example:
            Exercise Cache.load through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :return: None; the function records state or raises through its assertions.
        """
        return None

    def invalidate(self, **_kwargs: Any) -> None:
        """
        Record invalidated tables or identities for mutation tests.

        Example:
            Exercise Cache.invalidate through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param _kwargs: Value supplied for kwargs in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        return None

    def create_writer(self, *_args: Any, **_kwargs: Any) -> Any:
        """
        Return the configured writer double for cache-backed mutations.

        Example:
            Exercise Cache.create writer through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param _args: Value supplied for args in the focused test operation.
        :param _kwargs: Value supplied for kwargs in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise NotImplementedError

    def write(self, *_args: Any, **_kwargs: Any) -> Mapping[Any, Any]:
        """
        Record a batch mutation and return its configured result.

        Example:
            Exercise Cache.write through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param _args: Value supplied for args in the focused test operation.
        :param _kwargs: Value supplied for kwargs in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise NotImplementedError

    def write_one(self, *_args: Any, **_kwargs: Any) -> Mapping[Any, Any]:
        """
        Record one mutation and return its configured result.

        Example:
            Exercise Cache.write one through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param _args: Value supplied for args in the focused test operation.
        :param _kwargs: Value supplied for kwargs in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise NotImplementedError

    def get_main_table(self, table: str) -> _CacheMainTable:
        """
        Return the cached main-table double for a table.

        Example:
            Exercise Cache.get main table through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param table: Table name addressed by the test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return self.main_tables[str(table)]

    def get_link_table(self, primary_table: str, secondary_table: str) -> _CacheLinkTable:
        """
        Return the cached link-table double for a relation table.

        Example:
            Exercise Cache.get link table through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


        :param primary_table: Value supplied for primary table in the focused test
            operation.
        :param secondary_table: Value supplied for secondary table in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return self.link_tables[(str(primary_table), str(secondary_table))]


def _database_with_cached_snapshot() -> tuple[_Database, CacheAPI]:
    """
    Create aligned database and cache doubles for read-source fallback tests.

    Example:
        Exercise database with cached snapshot through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


    :return: The deterministic value, row, identity or collection described above.
    """
    database = _Database()
    database.add_row("works", {"work_id": 1, "work_title": "Cached Book"})
    database.add_row("tags", {"tag_id": 3, "tag": "Cached Tag"})
    database.add_link(work_id=1, tag_id=3)
    cache = _Cache(database)
    database.add_row("works", {"work_id": 2, "work_title": "Live Book"})
    return database, cache


def test_metadata_read_source_contract_is_exported_from_api_root() -> None:
    """
    Verify metadata read source contract remains exported from api root.

    Example:
        Exercise test metadata read source contract is exported from api root through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


    :return: None; the function records state or raises through its assertions.
    """
    assert "MetadataReadSourceAPI" in metadata_api_all
    assert "MetadataDriverWrapperAPI" in metadata_api_all
    assert "MetadataRowSequence" in metadata_api_all
    assert "MetadataLinkRowSequence" in metadata_api_all
    assert "MetadataSearchTerm" in metadata_api_all


def test_read_source_adapters_are_exported_from_workflow_facade() -> None:
    """
    Verify read source adapters remain exported from workflow facade.

    Example:
        Exercise test read source adapters are exported from workflow facade through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


    :return: None; the function records state or raises through its assertions.
    """
    assert "DatabaseMetadataReadSource" in metadata_facade_all
    assert "CacheMetadataReadSource" in metadata_facade_all
    assert "metadata_read_source_from" in metadata_facade_all


def test_database_read_source_satisfies_public_contract() -> None:
    """
    Verify database read source satisfies public contract.

    Example:
        Exercise test database read source satisfies public contract through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


    :return: None; the function records state or raises through its assertions.
    """
    database, cache = _database_with_cached_snapshot()
    source = DatabaseMetadataReadSource(database)

    assert isinstance(source, MetadataReadSourceAPI)
    assert isinstance(source.driver_wrapper, MetadataDriverWrapperAPI)
    assert metadata_read_source_from(source) is source
    assert isinstance(metadata_read_source_from(database), DatabaseMetadataReadSource)
    assert isinstance(metadata_read_source_from(cache), CacheMetadataReadSource)

    work_row = source.get_row_from_id("works", 1)
    assert work_row is not None

    assert source.get_record_count("works") == 2
    assert [row["work_title"] for row in source.get_all_rows("works")] == [
        "Cached Book",
        "Live Book",
    ]
    assert [row["tag"] for row in source.get_interlinked_rows(work_row, "tags")] == [
        "Cached Tag"
    ]
    assert source.refresh() is False


def test_cache_read_source_contract_serves_cache_snapshot_without_fallback() -> None:
    """
    Verify cache read source contract serves cache snapshot without fallback.

    Example:
        Exercise test cache read source contract serves cache snapshot without fallback through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


    :return: None; the function records state or raises through its assertions.
    """
    database, cache = _database_with_cached_snapshot()
    source = CacheMetadataReadSource(
        cache,
        database=database,
        allow_database_fallback=False,
    )

    assert isinstance(source, MetadataReadSourceAPI)
    assert isinstance(source.driver_wrapper, MetadataDriverWrapperAPI)

    work_row = source.get_row_from_id("works", 1)
    assert work_row is not None

    assert source.get_record_count("works") == 1
    assert source.get_row_from_id("works", 2) is None
    assert [row["work_title"] for row in source.get_all_rows("works")] == [
        "Cached Book"
    ]
    assert [
        row["work_title"]
        for row in source.search("works", "work_title", "Cached Book")
    ] == [
        "Cached Book"
    ]
    assert [row["tag"] for row in source.get_interlinked_rows(work_row, "tags")] == [
        "Cached Tag"
    ]

    assert source.refresh() is True
    assert cache.reloaded is True


def test_cache_read_source_rejects_a_different_fallback_database() -> None:
    """
    Verify cache read source rejects a different fallback database.

    Example:
        Exercise test cache read source rejects a different fallback database through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


    :return: None; the function records state or raises through its assertions.
    """
    _database, cache = _database_with_cached_snapshot()

    with pytest.raises(ValueError, match="cache and fallback database must match"):
        CacheMetadataReadSource(cache, database=_Database())


def test_database_read_source_forwards_the_entire_hydrator_surface() -> None:
    """
    Verify database read source forwards the entire hydrator surface.

    Example:
        Exercise test database read source forwards the entire hydrator surface through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


    :return: None; the function records state or raises through its assertions.
    """
    database, _cache = _database_with_cached_snapshot()
    source = DatabaseMetadataReadSource(database)
    work_row = source.get_row_from_id("works", 1)

    assert work_row is not None
    assert source.get_tables(force_refresh=True) == ["works", "tags"]
    assert source.get_tables_and_columns() == database.driver_wrapper._tables
    assert source.get_column_headings("works") == {"work_id", "work_title"}
    assert [row["work_title"] for row in source.search(
        "works",
        "work_title",
        "Live Book",
    )] == ["Live Book"]
    assert source.get_interlink_rows(work_row, "tags") == [
        {
            "tag_work_link_work_id": 1,
            "tag_work_link_tag_id": 3,
        }
    ]
    assert [row["tag"] for row in source.get_interlinked_rows(
        work_row,
        "tags",
        type_filter="ignored-by-this-schema",
    )] == ["Cached Tag"]

    database.sentinel_attribute = object()
    assert source.sentinel_attribute is database.sentinel_attribute

    with pytest.raises(ValueError, match="requires a database"):
        DatabaseMetadataReadSource(None)


def test_cache_read_source_constructor_and_schema_discovery_are_strict() -> None:
    """
    Verify cache read source constructor and schema discovery remain strict.

    Example:
        Exercise test cache read source constructor and schema discovery are strict through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


    :return: None; the function records state or raises through its assertions.
    """
    database, cache = _database_with_cached_snapshot()

    with pytest.raises(ValueError, match="requires a cache facade"):
        CacheMetadataReadSource(None)
    with pytest.raises(TypeError, match="requires CacheAPI"):
        CacheMetadataReadSource(object())  # type: ignore[arg-type]

    detached_cache = _Cache(database)
    detached_cache.database = None
    with pytest.raises(ValueError, match="requires an attached database"):
        CacheMetadataReadSource(detached_cache)

    cache.table_columns = Mock(
        return_value={
            "works": ("work_id", "work_title"),
            "cache_only": ("cache_id",),
        }
    )
    database.get_tables = Mock(return_value=["works", "tags", "database_only"])
    database.get_tables_and_columns = Mock(
        return_value={
            "works": ("database_work_id",),
            "tags": ("tag_id", "tag"),
            "database_only": ("database_only_id",),
        }
    )
    source = CacheMetadataReadSource(cache)

    assert source.database is database
    assert source.get_tables(force_refresh=True) == (
        "cache_only",
        "database_only",
        "tags",
        "works",
    )
    assert source.get_tables_and_columns() == {
        "works": ("work_id", "work_title"),
        "cache_only": ("cache_id",),
        "tags": ("tag_id", "tag"),
        "database_only": ("database_only_id",),
    }
    assert source.get_column_headings("works") == {"work_id", "work_title"}
    assert source.get_column_headings("tags") == {"tag_id", "tag"}
    database.get_tables.assert_called_once_with(force_refresh=False)

    no_fallback = CacheMetadataReadSource(
        cache,
        allow_database_fallback=False,
    )
    assert no_fallback.get_tables() == ("cache_only", "works")
    assert no_fallback.get_tables_and_columns() == {
        "works": ("work_id", "work_title"),
        "cache_only": ("cache_id",),
    }
    assert no_fallback.get_column_headings("tags") == set()

    database.get_tables = Mock(side_effect=RuntimeError("schema offline"))
    database.get_tables_and_columns = Mock(
        side_effect=RuntimeError("schema offline")
    )
    assert source.get_tables() == ("cache_only", "works")
    assert source.get_tables_and_columns() == {
        "works": ("work_id", "work_title"),
        "cache_only": ("cache_id",),
    }


def test_cache_exact_lookup_distinguishes_hits_complete_misses_and_gaps() -> None:
    """
    Verify cache exact lookup distinguishes hits complete misses and gaps.

    Example:
        Exercise test cache exact lookup distinguishes hits complete misses and gaps through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


    :return: None; the function records state or raises through its assertions.
    """
    database, cache = _database_with_cached_snapshot()
    database_lookup = Mock(wraps=database.get_row_from_id)
    database.get_row_from_id = database_lookup
    source = CacheMetadataReadSource(cache)

    cached_row = source.get_row_from_id("works", 1)

    assert cached_row is not None
    assert cached_row["work_title"] == "Cached Book"
    assert cached_row.db is source
    assert cached_row.read_only is True
    database_lookup.assert_not_called()

    cache.get = Mock(
        return_value=CacheLookup(
            CacheLookupStatus.MISS,
            None,
            True,
            cache.generation,
        )
    )
    assert source.get_row_from_id("works", 2) is None
    database_lookup.assert_not_called()

    cache.get = Mock(
        return_value=CacheLookup(
            CacheLookupStatus.MISS,
            None,
            False,
            cache.generation,
        )
    )
    live_row = source.get_row_from_id("works", 2)
    assert live_row is not None
    assert live_row["work_title"] == "Live Book"
    database_lookup.assert_called_once_with("works", 2)

    database_lookup.reset_mock()
    no_fallback = CacheMetadataReadSource(
        cache,
        allow_database_fallback=False,
    )
    assert no_fallback.get_row_from_id("works", 2) is None
    database_lookup.assert_not_called()


@pytest.mark.parametrize(
    "error",
    [
        UnknownCacheFieldError("uncached field"),
        UnknownCacheTableError("uncached table"),
        UnsupportedCacheQueryError("unsupported lookup"),
    ],
)
@pytest.mark.parametrize("allow_fallback", [False, True])
def test_cache_exact_lookup_falls_back_only_for_declared_cache_gaps(
    error: Exception,
    allow_fallback: bool,
) -> None:
    """
    Verify cache exact lookup falls back only for declared cache gaps.

    Example:
        Exercise test cache exact lookup falls back only for declared cache gaps through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


    :param error: Value supplied for error in the focused test operation.
    :param allow_fallback: Value supplied for allow fallback in the focused test
        operation.
    :return: None; the function records state or raises through its assertions.
    """
    database, cache = _database_with_cached_snapshot()
    database_lookup = Mock(wraps=database.get_row_from_id)
    database.get_row_from_id = database_lookup
    cache.get = Mock(side_effect=error)
    source = CacheMetadataReadSource(
        cache,
        allow_database_fallback=allow_fallback,
    )

    result = source.get_row_from_id("works", 2)

    if allow_fallback:
        assert result is not None
        assert result["work_title"] == "Live Book"
        database_lookup.assert_called_once_with("works", 2)
    else:
        assert result is None
        database_lookup.assert_not_called()


def test_cache_exact_lookup_does_not_hide_unexpected_backend_failures() -> None:
    """
    Verify cache exact lookup does not hide unexpected backend failures.

    Example:
        Exercise test cache exact lookup does not hide unexpected backend failures through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


    :return: None; the function records state or raises through its assertions.
    """
    database, cache = _database_with_cached_snapshot()
    cache.get = Mock(side_effect=RuntimeError("corrupt cache page"))
    source = CacheMetadataReadSource(cache)

    with pytest.raises(RuntimeError, match="corrupt cache page"):
        source.get_row_from_id("works", 1)


def test_complete_empty_cache_queries_are_authoritative_and_structured() -> None:
    """
    Verify complete empty cache queries remain authoritative and structured.

    Example:
        Exercise test complete empty cache queries are authoritative and structured through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


    :return: None; the function records state or raises through its assertions.
    """
    database, cache = _database_with_cached_snapshot()
    database.get_all_rows = Mock(wraps=database.get_all_rows)
    database.get_record_count = Mock(wraps=database.get_record_count)
    database.search = Mock(wraps=database.search)
    complete_empty = CacheQueryResult(
        (),
        0,
        0,
        None,
        True,
        cache.generation,
    )
    cache.query = Mock(return_value=complete_empty)
    source = CacheMetadataReadSource(cache)

    assert source.get_all_rows("works", iterator_return=True) == ()
    assert source.get_record_count("works") == 0
    assert source.search("works", "work_title", "Live Book") == ()

    database.get_all_rows.assert_not_called()
    database.get_record_count.assert_not_called()
    database.search.assert_not_called()

    all_query, count_query, search_query = [
        call.args[0] for call in cache.query.call_args_list
    ]
    assert all_query == CacheQuery(table="works")
    assert count_query.table == "works"
    assert count_query.limit == 0
    assert search_query.predicates[0].field == "work_title"
    assert search_query.predicates[0].operator is CacheFilterOperator.EQ
    assert search_query.predicates[0].value == "Live Book"
    assert search_query.sort[0].field == "work_id"

    explicit_query = CacheQuery(table="tags", limit=0)
    assert source.query_cache(explicit_query) is complete_empty
    assert cache.query.call_args.args == (explicit_query,)


def test_incomplete_queries_use_one_explicit_fallback_policy() -> None:
    """
    Verify incomplete queries use one explicit fallback policy.

    Example:
        Exercise test incomplete queries use one explicit fallback policy through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


    :return: None; the function records state or raises through its assertions.
    """
    database, cache = _database_with_cached_snapshot()
    incomplete = CacheQueryResult(
        (
            CacheRecord(
                "works",
                1,
                {"work_id": 1, "work_title": "Stale Cached Book"},
            ),
        ),
        99,
        0,
        None,
        False,
        cache.generation,
    )
    cache.query = Mock(return_value=incomplete)
    source = CacheMetadataReadSource(cache)

    assert [row["work_title"] for row in source.get_all_rows("works")] == [
        "Cached Book",
        "Live Book",
    ]
    assert source.get_record_count("works") == 2
    assert [row["work_title"] for row in source.search(
        "works",
        "work_title",
        "Live Book",
    )] == ["Live Book"]

    no_fallback = CacheMetadataReadSource(
        cache,
        allow_database_fallback=False,
    )
    assert [row["work_title"] for row in no_fallback.get_all_rows("works")] == [
        "Stale Cached Book"
    ]
    assert no_fallback.get_record_count("works") == 99
    assert [row["work_title"] for row in no_fallback.search(
        "works",
        "work_title",
        "does-not-matter-to-the-fake",
    )] == ["Stale Cached Book"]


@pytest.mark.parametrize(
    ("operation", "error"),
    [
        ("all", UnknownCacheTableError("uncached table")),
        ("all", UnsupportedCacheQueryError("unsupported scan")),
        ("count", UnknownCacheTableError("uncached table")),
        ("count", UnsupportedCacheQueryError("unsupported count")),
        ("search", UnknownCacheFieldError("uncached field")),
        ("search", UnknownCacheTableError("uncached table")),
        ("search", UnsupportedCacheQueryError("unsupported predicate")),
    ],
)
@pytest.mark.parametrize("allow_fallback", [False, True])
def test_query_operations_fall_back_only_for_their_documented_cache_gaps(
    operation: str,
    error: Exception,
    allow_fallback: bool,
) -> None:
    """
    Verify query operations fall back only for their documented cache gaps.

    Example:
        Exercise test query operations fall back only for their documented cache gaps through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


    :param operation: Callable invoked to exercise the requested behavior.
    :param error: Value supplied for error in the focused test operation.
    :param allow_fallback: Value supplied for allow fallback in the focused test
        operation.
    :return: None; the function records state or raises through its assertions.
    """
    database, cache = _database_with_cached_snapshot()
    cache.query = Mock(side_effect=error)
    source = CacheMetadataReadSource(
        cache,
        allow_database_fallback=allow_fallback,
    )

    if operation == "all":
        result: Any = source.get_all_rows("works")
        expected_without_fallback: Any = ()
    elif operation == "count":
        result = source.get_record_count("works")
        expected_without_fallback = 0
    else:
        result = source.search("works", "work_title", "Live Book")
        expected_without_fallback = ()

    if not allow_fallback:
        assert result == expected_without_fallback
    elif operation == "all":
        assert [row["work_title"] for row in result] == [
            "Cached Book",
            "Live Book",
        ]
    elif operation == "count":
        assert result == 2
    else:
        assert [row["work_title"] for row in result] == ["Live Book"]


@pytest.mark.parametrize("operation", ["all", "count", "search"])
def test_query_operations_propagate_unexpected_cache_failures(
    operation: str,
) -> None:
    """
    Verify query operations propagate unexpected cache failures.

    Example:
        Exercise test query operations propagate unexpected cache failures through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


    :param operation: Callable invoked to exercise the requested behavior.
    :return: None; the function records state or raises through its assertions.
    """
    database, cache = _database_with_cached_snapshot()
    cache.query = Mock(side_effect=RuntimeError("cache invariant broken"))
    source = CacheMetadataReadSource(cache)

    with pytest.raises(RuntimeError, match="cache invariant broken"):
        if operation == "all":
            source.get_all_rows("works")
        elif operation == "count":
            source.get_record_count("works")
        else:
            source.search("works", "work_title", "Cached Book")


def test_link_record_reads_distinguish_empty_unavailable_and_broken() -> None:
    """
    Verify link record reads distinguish empty unavailable and broken.

    Example:
        Exercise test link record reads distinguish empty unavailable and broken through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


    :return: None; the function records state or raises through its assertions.
    """
    database, cache = _database_with_cached_snapshot()
    source = CacheMetadataReadSource(cache)
    primary = SimpleNamespace(table="works", row_id=1)
    idless = SimpleNamespace(table="works", row_id=None)
    database_links = Mock(wraps=database.get_interlink_rows)
    database.get_interlink_rows = database_links

    cache.link_records = Mock()
    assert source.get_interlink_rows(idless, "tags") == ()
    cache.link_records.assert_not_called()

    cache.link_records.return_value = (
        CacheRecord(
            "tag_work_links",
            -1,
            {"work_id": 1, "work_title": "Materialized link record"},
        ),
    )
    rows = source.get_interlink_rows(primary, "tags")
    assert [row["work_title"] for row in rows] == ["Materialized link record"]
    assert rows[0].db is source
    database_links.assert_not_called()

    cache.link_records.return_value = ()
    assert source.get_interlink_rows(primary, "tags") == ()
    database_links.assert_not_called()

    cache.link_records.side_effect = KeyError("link table is not cached")
    assert source.get_interlink_rows(primary, "tags") == [
        {
            "tag_work_link_work_id": 1,
            "tag_work_link_tag_id": 3,
        }
    ]
    database_links.assert_called_once_with(
        primary_row=primary,
        secondary_table="tags",
    )

    database_links.reset_mock()
    no_fallback = CacheMetadataReadSource(
        cache,
        allow_database_fallback=False,
    )
    assert no_fallback.get_interlink_rows(primary, "tags") == ()
    database_links.assert_not_called()

    cache.link_records.side_effect = RuntimeError("bad link index")
    with pytest.raises(RuntimeError, match="bad link index"):
        source.get_interlink_rows(primary, "tags")


def test_related_reads_preserve_type_filters_and_completeness() -> None:
    """
    Verify related reads preserve type filters and completeness.

    Example:
        Exercise test related reads preserve type filters and completeness through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_read_source_api.py


    :return: None; the function records state or raises through its assertions.
    """
    database, cache = _database_with_cached_snapshot()
    source = CacheMetadataReadSource(cache)
    target = SimpleNamespace(table="works", row_id=1)
    idless = SimpleNamespace(table="works", row_id=None)
    database_related = Mock(wraps=database.get_interlinked_rows)
    database.get_interlinked_rows = database_related
    complete = CacheQueryResult(
        (
            CacheRecord(
                "tags",
                3,
                {"tag_id": 3, "tag": "Cache-only relation"},
            ),
        ),
        1,
        0,
        None,
        True,
        cache.generation,
    )
    cache.related = Mock(return_value=complete)

    assert source.get_interlinked_rows(idless, "tags") == ()
    cache.related.assert_not_called()

    rows = source.get_interlinked_rows(
        target,
        "tags",
        type_filter="primary",
    )
    assert [row["tag"] for row in rows] == ["Cache-only relation"]
    cache.related.assert_called_once_with(
        "works",
        (1,),
        "tags",
        type_filter="primary",
    )
    database_related.assert_not_called()

    cache.related.return_value = CacheQueryResult(
        complete.records,
        1,
        0,
        None,
        False,
        cache.generation,
    )
    assert [row["tag"] for row in source.get_interlinked_rows(
        target,
        "tags",
        type_filter="primary",
    )] == ["Cached Tag"]
    database_related.assert_called_once_with(
        target_row=target,
        secondary_table="tags",
        type_filter="primary",
    )

    database_related.reset_mock()
    no_fallback = CacheMetadataReadSource(
        cache,
        allow_database_fallback=False,
    )
    assert [row["tag"] for row in no_fallback.get_interlinked_rows(
        target,
        "tags",
        type_filter="primary",
    )] == ["Cache-only relation"]
    database_related.assert_not_called()

    cache.related.side_effect = KeyError("relation is not cached")
    assert [row["tag"] for row in source.get_interlinked_rows(
        target,
        "tags",
        type_filter="secondary",
    )] == ["Cached Tag"]

    database_related.reset_mock()
    assert no_fallback.get_interlinked_rows(
        target,
        "tags",
        type_filter="secondary",
    ) == ()
    database_related.assert_not_called()

    cache.related.side_effect = RuntimeError("corrupt relation index")
    with pytest.raises(RuntimeError, match="corrupt relation index"):
        source.get_interlinked_rows(target, "tags")
