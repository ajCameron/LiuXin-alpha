from __future__ import annotations

from typing import Any

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
    _tables = {
        "works": ("work_id", "work_title"),
        "tags": ("tag_id", "tag"),
    }

    def get_allowed_tables_snapshot(self) -> list[str]:
        return list(self._tables)

    def identify_table_from_row_dict(self, row_dict: dict[str, Any]) -> str:
        if "work_id" in row_dict or "work_title" in row_dict:
            return "works"
        if "tag_id" in row_dict or "tag" in row_dict:
            return "tags"
        raise ValueError("unknown row")

    def get_id_column(self, table: str) -> str:
        return {"works": "work_id", "tags": "tag_id"}[str(table)]

    def check_for_intralink_table(self, table: str) -> bool:
        return False

    def get_interlinked_tables(self, table: str) -> list[str]:
        return ["tags"] if str(table) == "works" else []

    def get_link_table_name(self, table1: str, table2: str) -> str:
        left, right = sorted((str(table1).rstrip("s"), str(table2).rstrip("s")))
        return f"{left}_{right}_links"

    def get_column_base(self, table_name: str) -> str:
        return str(table_name).removesuffix("s")

    def get_link_column(
        self,
        table1: str,
        table2: str,
        secondary_id_column: str,
    ) -> str:
        link_table = self.get_link_table_name(table1, table2)
        return f"{self.get_column_base(link_table)}_{secondary_id_column}"


class _Database:
    def __init__(self) -> None:
        self.driver_wrapper = _DriverWrapper()
        self.rows_by_table: dict[str, list[Row]] = {
            "works": [],
            "tags": [],
        }
        self.links_by_source: dict[tuple[str, int, str], list[dict[str, int]]] = {}

    def add_row(self, table: str, payload: dict[str, Any]) -> Row:
        row = Row(self, row_dict=payload, read_only=True)
        self.rows_by_table[str(table)].append(row)
        return row

    def add_link(self, work_id: int, tag_id: int) -> None:
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
        del force_refresh
        return list(self.rows_by_table)

    def get_tables_and_columns(self) -> dict[str, tuple[str, ...]]:
        return dict(self.driver_wrapper._tables)

    def get_column_headings(self, table: str) -> set[str]:
        return set(self.driver_wrapper._tables[str(table)])

    def get_row_from_id(self, table: str, row_id: int) -> Row | None:
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
        del iterator_return
        return list(self.rows_by_table[str(table)])

    def get_record_count(self, table: str) -> int:
        return len(self.rows_by_table[str(table)])

    def search(self, table: str, column: str, search_term: Any) -> list[Row]:
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
    def __init__(self, database: _Database, table: str) -> None:
        self.column_headings = tuple(database.driver_wrapper._tables[str(table)])
        self._rows = {
            int(row.row_id): dict(row.row_dict)
            for row in database.rows_by_table[str(table)]
        }
        self.row_ids = tuple(sorted(self._rows))

    def get_row_snapshot(self, row_id: int) -> dict[str, Any]:
        return dict(self._rows[int(row_id)])

    def get_ids_for_value(self, column: str, value: Any) -> list[int]:
        return [
            row_id
            for row_id, row in self._rows.items()
            if row.get(str(column)) == value
        ]


class _CacheLinkTable:
    def __init__(self, links_by_source_id: dict[int, list[dict[str, int]]]) -> None:
        self.links_by_source_id = {
            source_id: [dict(link) for link in links]
            for source_id, links in links_by_source_id.items()
        }

    def get_link_rows_for_src(
        self,
        source_id: int,
        require_ordering: bool = False,
    ) -> list[dict[str, int]]:
        del require_ordering
        return [
            dict(link)
            for link in self.links_by_source_id.get(int(source_id), [])
        ]


class _Cache:
    def __init__(self, database: _Database) -> None:
        self.db = database
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
        return None

    def reload(self) -> None:
        self.reloaded = True

    def get_main_table(self, table: str) -> _CacheMainTable:
        return self.main_tables[str(table)]

    def get_link_table(self, primary_table: str, secondary_table: str) -> _CacheLinkTable:
        return self.link_tables[(str(primary_table), str(secondary_table))]


def _database_with_cached_snapshot() -> tuple[_Database, _Cache]:
    database = _Database()
    database.add_row("works", {"work_id": 1, "work_title": "Cached Book"})
    database.add_row("tags", {"tag_id": 3, "tag": "Cached Tag"})
    database.add_link(work_id=1, tag_id=3)
    cache = _Cache(database)
    database.add_row("works", {"work_id": 2, "work_title": "Live Book"})
    return database, cache


def test_metadata_read_source_contract_is_exported_from_api_root() -> None:
    assert "MetadataReadSourceAPI" in metadata_api_all
    assert "MetadataDriverWrapperAPI" in metadata_api_all
    assert "MetadataRowSequence" in metadata_api_all
    assert "MetadataLinkRowSequence" in metadata_api_all
    assert "MetadataSearchTerm" in metadata_api_all


def test_read_source_adapters_are_exported_from_workflow_facade() -> None:
    assert "DatabaseMetadataReadSource" in metadata_facade_all
    assert "CacheMetadataReadSource" in metadata_facade_all
    assert "metadata_read_source_from" in metadata_facade_all


def test_database_read_source_satisfies_public_contract() -> None:
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
