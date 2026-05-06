from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import DatabaseIntegrityError
from LiuXin_alpha.metadata.api import WorkRelationLink
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData,
)
from LiuXin_alpha.metadata.containers import (
    ExpressionMetadata,
    ItemMetadata,
    ItemMetadataHydrator,
    LazyLiuXinWEMIMetadata,
    LazyLiuXinWEMIMetadataHydrator,
    LiuXinWEMIMetadata,
    LiuXinWEMIMetadataHydrator,
    ManifestationMetadata,
    WorkMetadata,
)
from LiuXin_alpha.metadata.read_sources import CacheMetadataReadSource


SINGULARS = {
    "items": "item",
    "manifestations": "manifestation",
    "expressions": "expression",
    "works": "work",
    "agents": "agent",
    "files": "file",
    "images": "image",
    "stores": "store",
    "folders": "folder",
    "genres": "genre",
    "labels": "label",
    "series": "series",
    "tags": "tag",
    "languages": "language",
    "ratings": "rating",
    "item_identifiers": "item_identifier",
    "entity_identifiers": "entity_identifier",
    "annotations": "annotation",
    "digital_assets": "digital_asset",
    "composite_digital_assets": "composite_digital_asset",
    "asset_replicas": "asset_replica",
}


class FakeDriverWrapper:
    def __init__(
        self,
        tables_and_columns: Mapping[str, list[str]],
        database: "FakeDatabase",
    ) -> None:
        self.tables_and_columns = dict(tables_and_columns)
        self.database = database

    def get_allowed_tables_snapshot(self) -> list[str]:
        return list(self.tables_and_columns)

    def identify_table_from_row_dict(self, row_dict: Mapping[str, Any]) -> str:
        keys = set(row_dict)
        for table, singular in SINGULARS.items():
            id_column = f"{singular}_id"
            if id_column in keys:
                return table
            prefix = singular + "_"
            if any(str(key).startswith(prefix) for key in keys):
                return table
        raise ValueError(f"Could not identify table from keys: {sorted(keys)}")

    def get_id_column(self, table: str) -> str:
        return f"{SINGULARS[str(table)]}_id"

    def check_for_intralink_table(self, table: str) -> bool:
        return False

    def get_interlinked_tables(self, table: str) -> list[str]:
        return []

    @staticmethod
    def _singular(table: str) -> str:
        return SINGULARS.get(str(table), str(table).rstrip("s"))

    def get_link_table_name(self, table1: str, table2: str) -> str:
        left = self._singular(table1)
        right = self._singular(table2)
        names = sorted((left, right))
        if left == right:
            return f"{left}_{left}_intralinks"
        return f"{names[0]}_{names[1]}_links"

    @staticmethod
    def get_column_base(table_name: str) -> str:
        text = str(table_name)
        if text.endswith("_links"):
            return text[:-1]
        if text.endswith("_intralinks"):
            return text[:-1]
        return text.rstrip("s")

    def get_link_column(self, table1: str, table2: str, secondary_id_column: str) -> str:
        link_table = self.get_link_table_name(table1, table2)
        return f"{self.get_column_base(link_table)}_{secondary_id_column}"

    def add_row(self, row_dict: Mapping[str, Any]) -> int:
        table = self.identify_table_from_row_dict(row_dict)
        id_column = self.get_id_column(table)
        existing_ids = [
            int(row.row_dict[id_column])
            for row in self.database.rows_by_table.get(table, [])
            if row.row_dict.get(id_column) not in (None, "")
        ]
        row_id = max(existing_ids, default=0) + 1
        payload = dict(row_dict)
        payload[id_column] = row_id
        self.database.add_row(table, payload)
        return row_id

    def get_row_from_id(self, table: str, row_id: int) -> dict[str, Any]:
        row = self.database.get_row_from_id(table, row_id)
        if row is None:
            raise KeyError((table, row_id))
        return dict(row.row_dict)


@dataclass
class FakeDatabase:
    tables_and_columns: dict[str, list[str]]
    driver_wrapper: FakeDriverWrapper = field(init=False)
    rows_by_table: dict[str, list[Row]] = field(default_factory=dict)
    interlinks: dict[tuple[str, int, str], list[dict[str, Any]]] = field(default_factory=dict)
    interlink_queries: list[tuple[str, int, str]] = field(default_factory=list)
    search_queries: list[tuple[str, str, Any]] = field(default_factory=list)
    dirtied: list[tuple[str, int, str]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.driver_wrapper = FakeDriverWrapper(self.tables_and_columns, self)

    def get_tables(self, force_refresh: bool = False) -> list[str]:
        return list(self.tables_and_columns)

    def get_tables_and_columns(self) -> dict[str, list[str]]:
        return dict(self.tables_and_columns)

    def get_column_headings(self, table: str) -> set[str]:
        return set(self.tables_and_columns.get(str(table), []))

    def add_row(self, table: str, row_dict: dict[str, Any]) -> Row:
        row = Row(self, row_dict=row_dict, read_only=True)
        self.rows_by_table.setdefault(str(table), []).append(row)
        return row

    def get_row_from_id(self, table: str, row_id: int) -> Row | None:
        target_table = str(table)
        target_row_id = int(row_id)
        id_column = self.driver_wrapper.get_id_column(target_table)
        for row in self.rows_by_table.get(target_table, []):
            if int(row.row_dict.get(id_column)) == target_row_id:
                return row
        return None

    def search(self, table: str, column: str, search_term: Any) -> list[Row]:
        self.search_queries.append((str(table), str(column), search_term))
        out: list[Row] = []
        for row in self.rows_by_table.get(str(table), []):
            if row.row_dict.get(str(column)) == search_term:
                out.append(row)
        return out

    def get_interlink_rows(self, primary_row: Row, secondary_table: str) -> list[dict[str, Any]]:
        key = (str(primary_row.table), int(primary_row.row_id), str(secondary_table))
        self.interlink_queries.append(key)
        return list(self.interlinks.get(key, []))

    def interlink_rows(
        self,
        primary_row: Row,
        secondary_row: Row,
        priority: Any = "highest",
        type: str | None = None,
        **col_value_pairs: Any,
    ) -> dict[str, Any]:
        primary_table = str(primary_row.table)
        secondary_table = str(secondary_row.table)
        primary_id_column = self.driver_wrapper.get_id_column(primary_table)
        secondary_id_column = self.driver_wrapper.get_id_column(secondary_table)
        link_table = self.driver_wrapper.get_link_table_name(primary_table, secondary_table)
        base = self.driver_wrapper.get_column_base(link_table)
        primary_link_column = self.driver_wrapper.get_link_column(
            primary_table,
            secondary_table,
            primary_id_column,
        )
        secondary_link_column = self.driver_wrapper.get_link_column(
            primary_table,
            secondary_table,
            secondary_id_column,
        )
        key = (primary_table, int(primary_row.row_id), secondary_table)
        links = self.interlinks.setdefault(key, [])
        for link in links:
            if int(link.get(secondary_link_column)) == int(secondary_row.row_id):
                raise DatabaseIntegrityError("Duplicate fake interlink")

        link: dict[str, Any] = {
            f"{base}_id": len(links) + 1,
            primary_link_column: int(primary_row.row_id),
            secondary_link_column: int(secondary_row.row_id),
        }
        if priority != "not_set":
            link[f"{base}_priority"] = len(links) + 1
        if type is not None:
            link[f"{base}_type"] = type
        for column, value in col_value_pairs.items():
            link[self.driver_wrapper.get_link_column(primary_table, secondary_table, column)] = value
        links.append(link)
        return link

    def unlink_interlink(self, primary_row: Row, secondary_row: Row) -> None:
        primary_table = str(primary_row.table)
        secondary_table = str(secondary_row.table)
        secondary_id_column = self.driver_wrapper.get_id_column(secondary_table)
        secondary_link_column = self.driver_wrapper.get_link_column(
            primary_table,
            secondary_table,
            secondary_id_column,
        )
        key = (primary_table, int(primary_row.row_id), secondary_table)
        kept = [
            link
            for link in self.interlinks.get(key, [])
            if int(link.get(secondary_link_column)) != int(secondary_row.row_id)
        ]
        self.interlinks[key] = kept

    def dirty_record(self, table: str, row_id: int, reason: str = "") -> None:
        self.dirtied.append((str(table), int(row_id), str(reason)))


class FakeCacheMainTable:
    def __init__(self, database: FakeDatabase, table: str) -> None:
        self.database = database
        self.table = table
        self.column_headings = tuple(database.tables_and_columns[str(table)])
        self.id_column = database.driver_wrapper.get_id_column(str(table))
        self._rows = {
            int(row.row_dict[self.id_column]): dict(row.row_dict)
            for row in database.rows_by_table.get(str(table), [])
            if row.row_dict.get(self.id_column) not in (None, "")
        }

    def get_row_snapshot(self, table_id: int) -> dict[str, Any]:
        return dict(self._rows[int(table_id)])

    def get_ids_for_value(self, column: str, value: Any) -> set[int]:
        return {
            row_id
            for row_id, row in self._rows.items()
            if row.get(str(column)) == value
        }


class FakeCacheLinkTable:
    def __init__(
        self,
        database: FakeDatabase,
        primary_table: str,
        secondary_table: str,
        links_by_source_id: Mapping[int, list[dict[str, Any]]],
    ) -> None:
        self.database = database
        self.primary_table = primary_table
        self.secondary_table = secondary_table
        self._links_by_source_id = {
            int(source_id): [dict(link) for link in links]
            for source_id, links in links_by_source_id.items()
        }

    def get_link_rows_for_src(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: str | None = None,
    ) -> list[dict[str, Any]]:
        links = [dict(link) for link in self._links_by_source_id.get(int(src_id), [])]
        if type_filter is not None:
            links = [
                link
                for link in links
                if any(str(key).endswith("_type") and value == type_filter for key, value in link.items())
            ]
        if require_ordering:
            links.sort(
                key=lambda link: next(
                    (
                        value
                        for key, value in link.items()
                        if str(key).endswith("_priority")
                    ),
                    0,
                )
            )
        return links


class FakeStorageCache:
    def __init__(self, database: FakeDatabase) -> None:
        self.db = database
        self.main_tables = {
            table: FakeCacheMainTable(database, table)
            for table in database.tables_and_columns
        }
        self.link_tables: dict[tuple[str, str], FakeCacheLinkTable] = {}
        grouped_links: dict[tuple[str, str], dict[int, list[dict[str, Any]]]] = {}
        for (primary_table, source_id, secondary_table), links in database.interlinks.items():
            key = (primary_table, secondary_table)
            grouped_links.setdefault(key, {})[int(source_id)] = [
                self._normalize_link(database, primary_table, source_id, secondary_table, link, index)
                for index, link in enumerate(links, start=1)
            ]
        for (primary_table, secondary_table), links_by_source_id in grouped_links.items():
            self.link_tables[(primary_table, secondary_table)] = FakeCacheLinkTable(
                database,
                primary_table,
                secondary_table,
                links_by_source_id,
            )

    @property
    def is_initialized(self) -> bool:
        return True

    def assert_ready(self) -> None:
        return None

    def get_main_table(self, table: str) -> FakeCacheMainTable:
        return self.main_tables[str(table)]

    def get_link_table(self, primary_table: str, secondary_table: str) -> FakeCacheLinkTable:
        return self.link_tables[(str(primary_table), str(secondary_table))]

    @staticmethod
    def _normalize_link(
        database: FakeDatabase,
        primary_table: str,
        source_id: int,
        secondary_table: str,
        link: Mapping[str, Any],
        index: int,
    ) -> dict[str, Any]:
        link_table = database.driver_wrapper.get_link_table_name(primary_table, secondary_table)
        base = database.driver_wrapper.get_column_base(link_table)
        primary_id_column = database.driver_wrapper.get_id_column(primary_table)
        secondary_id_column = database.driver_wrapper.get_id_column(secondary_table)
        primary_link_column = database.driver_wrapper.get_link_column(
            primary_table,
            secondary_table,
            primary_id_column,
        )
        secondary_link_column = database.driver_wrapper.get_link_column(
            primary_table,
            secondary_table,
            secondary_id_column,
        )
        payload = dict(link)
        payload.setdefault(f"{base}_id", index)
        payload.setdefault(primary_link_column, int(source_id))
        if secondary_link_column not in payload:
            for key, value in list(payload.items()):
                if str(key).endswith("_" + secondary_id_column):
                    payload[secondary_link_column] = value
                    break
        return payload


def _build_fake_database() -> FakeDatabase:
    tables_and_columns = {
        "items": [
            "item_id",
            "item_manifestation_id",
            "item_type",
            "item_source_name",
            "item_source",
        ],
        "manifestations": [
            "manifestation_id",
            "manifestation_format_detail",
            "manifestation_subtitle",
        ],
        "expressions": [
            "expression_id",
            "expression_title_override",
        ],
        "works": [
            "work_id",
            "work_title",
            "work_canonical_title",
            "work_sort_title",
        ],
        "agents": [
            "agent_id",
            "agent_canonical_name",
            "agent_sort_name",
        ],
        "files": [
            "file_id",
            "file_item_id",
            "file_store_id",
            "file_extension",
            "file_role",
            "file_storage_key",
        ],
        "images": [
            "image_id",
            "image_item_id",
            "image_role",
            "image_storage_key",
        ],
        "digital_assets": [
            "digital_asset_id",
            "digital_asset_name",
            "digital_asset_media_category",
        ],
        "asset_replicas": [
            "asset_replica_id",
            "asset_replica_digital_asset_id",
            "asset_replica_storage_key",
        ],
        "stores": [
            "store_id",
            "store_name",
            "store_root_uri",
        ],
        "labels": [
            "label_id",
            "label_text",
            "label_text_norm",
        ],
        "genres": [
            "genre_id",
            "genre",
            "genre_full",
        ],
        "series": [
            "series_id",
            "series",
            "series_full",
        ],
        "tags": [
            "tag_id",
            "tag",
            "tag_phash",
        ],
        "languages": [
            "language_id",
            "language_code",
            "language_name",
        ],
        "ratings": [
            "rating_id",
            "rating",
            "rating_for_calibre_tag_viewer",
            "rating_source",
        ],
        "annotations": [
            "annotation_id",
            "annotation_item_id",
            "annotation_kind",
            "annotation_note_text",
        ],
        "item_identifiers": [
            "item_identifier_id",
            "item_identifier_item_id",
            "item_identifier_scheme",
            "item_identifier_value",
            "item_identifier_source",
        ],
        "entity_identifiers": [
            "entity_identifier_id",
            "entity_identifier_entity_type",
            "entity_identifier_entity_id",
            "entity_identifier_scheme",
            "entity_identifier_value",
            "entity_identifier_is_primary",
            "entity_identifier_provenance",
        ],
    }
    db = FakeDatabase(tables_and_columns=tables_and_columns)

    db.add_row(
        "items",
        {
            "item_id": 1,
            "item_manifestation_id": 10,
            "item_type": "digital",
            "item_source": "fixture",
            "item_source_name": "Permutation City.epub",
        },
    )
    db.add_row(
        "manifestations",
        {
            "manifestation_id": 10,
            "manifestation_format_detail": "epub",
            "manifestation_subtitle": "A Novel",
        },
    )
    db.add_row(
        "expressions",
        {
            "expression_id": 20,
            "expression_title_override": None,
        },
    )
    db.add_row(
        "works",
        {
            "work_id": 30,
            "work_title": "Permutation City",
            "work_canonical_title": "Permutation City",
            "work_sort_title": "Permutation City",
        },
    )
    db.add_row(
        "agents",
        {
            "agent_id": 40,
            "agent_canonical_name": "Greg Egan",
            "agent_sort_name": "Egan, Greg",
        },
    )
    db.add_row(
        "files",
        {
            "file_id": 50,
            "file_item_id": 1,
            "file_store_id": 60,
            "file_extension": "epub",
            "file_role": "primary",
            "file_storage_key": "Greg Egan/Permutation City (30)/Permutation City - Greg Egan.epub",
        },
    )
    db.add_row(
        "images",
        {
            "image_id": 51,
            "image_item_id": 1,
            "image_role": "cover",
            "image_storage_key": "covers/permutation-city.jpg",
        },
    )
    db.add_row(
        "digital_assets",
        {
            "digital_asset_id": 52,
            "digital_asset_name": "Permutation City",
            "digital_asset_media_category": "ebook",
        },
    )
    db.add_row(
        "asset_replicas",
        {
            "asset_replica_id": 53,
            "asset_replica_digital_asset_id": 52,
            "asset_replica_storage_key": "replicas/permutation-city.epub",
        },
    )
    db.add_row(
        "stores",
        {
            "store_id": 60,
            "store_name": "Main Store",
            "store_root_uri": "file:///library/main",
        },
    )
    db.add_row(
        "labels",
        {
            "label_id": 90,
            "label_text": "Science Fiction",
            "label_text_norm": "sciencefiction",
        },
    )
    db.add_row(
        "genres",
        {
            "genre_id": 95,
            "genre": "Cyberpunk",
            "genre_full": "Science Fiction: Cyberpunk",
        },
    )
    db.add_row(
        "series",
        {
            "series_id": 96,
            "series": "Permutation Cycle",
            "series_full": "Permutation Cycle",
        },
    )
    db.add_row(
        "tags",
        {
            "tag_id": 91,
            "tag": "Space Opera",
            "tag_phash": "spaceopera",
        },
    )
    db.add_row(
        "languages",
        {
            "language_id": 92,
            "language_code": "eng",
            "language_name": "English",
        },
    )
    db.add_row(
        "ratings",
        {
            "rating_id": 93,
            "rating": 8,
            "rating_for_calibre_tag_viewer": 4,
            "rating_source": "fixture",
        },
    )
    db.add_row(
        "annotations",
        {
            "annotation_id": 94,
            "annotation_item_id": 1,
            "annotation_kind": "highlight",
            "annotation_note_text": "A lazy annotation.",
        },
    )
    db.add_row(
        "item_identifiers",
        {
            "item_identifier_id": 70,
            "item_identifier_item_id": 1,
            "item_identifier_scheme": "isbn",
            "item_identifier_value": "9780000000001",
            "item_identifier_source": "fixture",
        },
    )
    db.add_row(
        "entity_identifiers",
        {
            "entity_identifier_id": 80,
            "entity_identifier_entity_type": "work",
            "entity_identifier_entity_id": 30,
            "entity_identifier_scheme": "openlibrary",
            "entity_identifier_value": "OL123W",
            "entity_identifier_is_primary": 1,
            "entity_identifier_provenance": "fixture",
        },
    )

    db.interlinks[("manifestations", 10, "expressions")] = [
        {
            "expression_manifestation_link_expression_id": 20,
            "expression_manifestation_link_priority": 1,
            "expression_manifestation_link_primary": 1,
            "expression_manifestation_link_type": "content_expression",
        }
    ]
    db.interlinks[("expressions", 20, "works")] = [
        {
            "expression_work_link_work_id": 30,
            "expression_work_link_priority": 1,
            "expression_work_link_primary": 1,
            "expression_work_link_type": "realises",
        }
    ]
    db.interlinks[("works", 30, "agents")] = [
        {
            "agent_work_link_agent_id": 40,
            "agent_work_link_priority": 1,
            "agent_work_link_primary": 1,
            "agent_work_link_type": "author",
        }
    ]
    db.interlinks[("works", 30, "labels")] = [
        {
            "label_work_link_label_id": 90,
            "label_work_link_priority": 1,
            "label_work_link_source": "fixture",
        }
    ]
    db.interlinks[("works", 30, "genres")] = [
        {
            "genre_work_link_genre_id": 95,
            "genre_work_link_priority": 1,
            "genre_work_link_source": "fixture",
        }
    ]
    db.interlinks[("works", 30, "series")] = [
        {
            "series_work_link_series_id": 96,
            "series_work_link_priority": 1,
            "series_work_link_source": "fixture",
        }
    ]
    db.interlinks[("works", 30, "tags")] = [
        {
            "tag_work_link_tag_id": 91,
            "tag_work_link_priority": 1,
            "tag_work_link_source": "fixture",
        }
    ]
    db.interlinks[("works", 30, "languages")] = [
        {
            "language_work_link_language_id": 92,
            "language_work_link_priority": 1,
            "language_work_link_source": "fixture",
        }
    ]
    db.interlinks[("works", 30, "ratings")] = [
        {
            "rating_work_link_rating_id": 93,
            "rating_work_link_priority": 1,
            "rating_work_link_source": "fixture",
        }
    ]
    db.interlinks[("items", 1, "digital_assets")] = [
        {
            "digital_asset_item_link_digital_asset_id": 52,
            "digital_asset_item_link_priority": 1,
            "digital_asset_item_link_source": "fixture",
        }
    ]
    return db


def test_item_metadata_hydrator_from_item_id_and_source_row() -> None:
    db = _build_fake_database()
    hydrator = ItemMetadataHydrator(db)

    container = hydrator.from_item_id(1)
    assert isinstance(container, ItemMetadata)
    assert container.item is not None
    assert container.item.item_id == 1
    assert container.item.item_manifestation_id == 10
    assert not hasattr(container, "storage_hints")

    work_links = container.get_relation_links("works")
    assert len(work_links) == 1
    assert work_links[0].type == "realises"

    identifier_links = container.get_relation_links("identifiers")
    assert len(identifier_links) == 2

    via_mapping = ItemMetadata.from_database(
        db,
        source_row={
            "item_id": 1,
            "manifestation_id": 10,
            "expression_id": 20,
            "work_id": 30,
        },
    )
    assert via_mapping.item is not None
    assert via_mapping.item.item_id == 1


def test_liuxin_wemi_metadata_hydrator_builds_complete_item_slice() -> None:
    db = _build_fake_database()
    hydrator = LiuXinWEMIMetadataHydrator(db)

    metadata = hydrator.get_liuxin_wemi_metadata(item_id=1)

    assert isinstance(metadata, LiuXinWEMIMetadata)
    assert metadata.item is not None
    assert metadata.item.item_id == 1
    assert metadata.manifestation is not None
    assert metadata.manifestation.manifestation_id == 10
    assert metadata.expression is not None
    assert metadata.expression.expression_id == 20
    assert metadata.work is not None
    assert metadata.work.work_id == 30
    assert metadata.title == "Permutation City"
    assert metadata.database_ids["item_id"] == 1
    assert metadata.database_ids["manifestation_id"] == 10
    assert metadata.database_ids["expression_id"] == 20
    assert metadata.database_ids["work_id"] == 30
    assert metadata.get_wemi_relation_links("item", "files")
    label = metadata.get_wemi_related("work", "labels")[0]
    assert label.row_dict["label_text"] == "Science Fiction"
    assert list(metadata.labels.keys()) == ["Science Fiction"]
    assert metadata.labels["Science Fiction"] == 90
    assert list(metadata.tags.keys()) == ["Space Opera"]
    assert metadata.tags["Space Opera"] == 91


def test_liuxin_wemi_metadata_hydrator_can_read_from_loaded_cache_source() -> None:
    db = _build_fake_database()
    cache_source = CacheMetadataReadSource(
        FakeStorageCache(db),
        allow_database_fallback=False,
    )

    metadata = LiuXinWEMIMetadataHydrator(cache_source).get_liuxin_wemi_metadata(item_id=1)

    assert metadata.item is not None
    assert metadata.item.item_id == 1
    assert metadata.manifestation is not None
    assert metadata.manifestation.manifestation_id == 10
    assert metadata.expression is not None
    assert metadata.expression.expression_id == 20
    assert metadata.work is not None
    assert metadata.work.work_id == 30
    assert metadata.title == "Permutation City"
    assert list(metadata.tags.keys()) == ["Space Opera"]
    assert metadata.tags["Space Opera"] == 91
    assert list(metadata.labels.keys()) == ["Science Fiction"]
    assert metadata.get_wemi_relation_links("item", "files")
    assert metadata.get_wemi_related("work", "agents")[0].row_dict["agent_canonical_name"] == "Greg Egan"


def test_liuxin_wemi_metadata_from_database_uses_central_hydrator() -> None:
    db = _build_fake_database()

    metadata = LiuXinWEMIMetadata.from_database(db, item_id=1)

    assert metadata.item is not None
    assert metadata.item.item_id == 1
    assert metadata.database_ids["work_id"] == 30
    assert metadata.title == "Permutation City"


def test_lazy_liuxin_wemi_metadata_defers_relation_backed_fields() -> None:
    db = _build_fake_database()
    hydrator = LazyLiuXinWEMIMetadataHydrator(db)

    metadata = hydrator.get_lazy_liuxin_wemi_metadata(item_id=1)

    assert isinstance(metadata, LazyLiuXinWEMIMetadata)
    assert metadata.item is not None
    assert metadata.work is not None
    assert metadata.title == "Permutation City"
    assert metadata.is_lazy_field_loaded("tags") is False
    assert metadata.is_lazy_field_loaded("labels") is False
    assert ("works", 30, "tags") not in db.interlink_queries
    assert ("works", 30, "labels") not in db.interlink_queries
    assert ("items", "file_item_id", 1) not in db.search_queries
    assert ("images", "image_item_id", 1) not in db.search_queries
    assert ("annotations", "annotation_item_id", 1) not in db.search_queries
    assert ("items", 1, "digital_assets") not in db.interlink_queries
    assert "<lazy tags>" in str(metadata)

    assert list(metadata.tags.keys()) == ["Space Opera"]
    assert metadata.tags["Space Opera"] == 91
    assert metadata.is_lazy_field_loaded("tags") is True
    assert "tags" not in metadata.lazy_fields()
    assert ("works", 30, "tags") in db.interlink_queries
    assert ("works", 30, "labels") not in db.interlink_queries

    assert list(metadata.labels.keys()) == ["Science Fiction"]
    assert metadata.labels["Science Fiction"] == 90
    assert metadata.is_lazy_field_loaded("labels") is True
    assert ("works", 30, "labels") in db.interlink_queries

    assert metadata.ratings["calibre"] == 4
    assert metadata.is_lazy_field_loaded("ratings") is True
    assert ("works", 30, "ratings") in db.interlink_queries

    assert metadata.languages_available["eng"] == 92
    assert ("works", 30, "languages") in db.interlink_queries

    item_files = metadata.get_wemi_relation_links("item", "files")
    assert item_files[0].target.row_dict["file_id"] == 50
    assert ("files", "file_item_id", 1) in db.search_queries

    images = metadata.get_wemi_relation_links("item", "images")
    assert images[0].target.row_dict["image_id"] == 51
    assert ("images", "image_item_id", 1) in db.search_queries

    annotations = metadata.get_wemi_relation_links("item", "annotations")
    assert annotations[0].target.row_dict["annotation_id"] == 94
    assert ("annotations", "annotation_item_id", 1) in db.search_queries

    asset_replicas = metadata.get_wemi_relation_links("item", "asset_replicas")
    assert asset_replicas[0].target.row_dict["asset_replica_id"] == 53
    assert ("items", 1, "digital_assets") in db.interlink_queries
    assert ("asset_replicas", "asset_replica_digital_asset_id", 52) in db.search_queries


def test_lazy_liuxin_wemi_metadata_can_force_hydrate_fields() -> None:
    db = _build_fake_database()

    metadata = LazyLiuXinWEMIMetadata.from_database(db, item_id=1)
    metadata.force_hydrate(fields=("tags", "labels"))

    assert set(metadata.lazy_fields()) == {
        "genre",
        "subject",
        "series",
        "notes",
        "comments",
        "synopses",
        "ratings",
        "files",
        "languages_available",
    }
    assert list(metadata.direct_get("tags").keys()) == ["Space Opera"]
    assert list(metadata.direct_get("labels").keys()) == ["Science Fiction"]


def test_liuxin_wemi_metadata_write_to_database_adds_missing_relation_terms() -> None:
    db = _build_fake_database()
    metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)

    metadata.tags = "Simulation"
    metadata.labels = "Needs Review"

    report = metadata.write_to_database(db, fields=("tags", "labels"))

    assert report.changed is True
    assert {row["text"] for row in report.rows_added} == {"Simulation", "Needs Review"}
    assert [row.row_dict["tag"] for row in db.search("tags", "tag", "Simulation")] == [
        "Simulation",
    ]
    assert [
        row.row_dict["label_text"]
        for row in db.search("labels", "label_text", "Needs Review")
    ] == ["Needs Review"]
    assert ("works", 30, "tags") in db.interlink_queries
    assert ("works", 30, "labels") in db.interlink_queries
    assert ("works", 30, "metadata_write_back") in db.dirtied

    rehydrated = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    assert list(rehydrated.tags.keys()) == ["Space Opera", "Simulation"]
    assert list(rehydrated.labels.keys()) == ["Science Fiction", "Needs Review"]

    no_change_report = rehydrated.write_to_database(db, fields=("tags", "labels"))
    assert no_change_report.changed is False
    assert no_change_report.rows_added == []
    assert no_change_report.links_added == []


def test_liuxin_wemi_metadata_write_to_database_can_replace_relation_terms() -> None:
    db = _build_fake_database()
    metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)

    metadata.nullify("tags")
    metadata.tags = "Simulation"

    report = metadata.write_to_database(db, fields=("tags",), replace=True)

    assert report.changed is True
    assert [row["text"] for row in report.rows_added] == ["Simulation"]
    assert len(report.links_removed) == 1

    rehydrated = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    assert list(rehydrated.tags.keys()) == ["Simulation"]


def test_liuxin_wemi_metadata_wemi_relation_edits_round_trip_to_database() -> None:
    db = _build_fake_database()
    metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    metadata.add_wemi_relation_link(
        "work",
        "tags",
        WorkRelationLink(
            target="WEMI Relation Round Trip",
            extra={"source_entity_type": "work"},
        ),
    )

    report = metadata.write_to_database(db, fields=("tags",))

    assert report.changed is True
    assert [row["text"] for row in report.rows_added] == ["WEMI Relation Round Trip"]

    rehydrated = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    assert list(rehydrated.tags.keys()) == [
        "Space Opera",
        "WEMI Relation Round Trip",
    ]


def test_liuxin_wemi_metadata_sidecar_without_legacy_round_trips_wemi_relations() -> None:
    db = _build_fake_database()
    metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    sidecar = metadata.to_mapping(include_legacy=False)
    round_tripped = LiuXinWEMIMetadata.from_mapping(sidecar)
    round_tripped.add_wemi_relation_link(
        "work",
        "tags",
        WorkRelationLink(
            target="Sidecar WEMI Round Trip",
            extra={"source_entity_type": "work"},
        ),
    )

    report = round_tripped.write_to_database(db, fields=("tags",))

    assert report.changed is True
    assert [row["text"] for row in report.rows_added] == ["Sidecar WEMI Round Trip"]

    rehydrated = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    assert list(rehydrated.tags.keys()) == [
        "Space Opera",
        "Sidecar WEMI Round Trip",
    ]


def test_wemi_metadata_bundles_write_supported_relation_terms() -> None:
    db = _build_fake_database()
    metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)

    cases = [
        (
            WorkMetadata(work=metadata.work),
            "tags",
            "Work Bundle Tag",
            "works",
            30,
            "tags",
        ),
        (
            ExpressionMetadata(expression=metadata.expression),
            "tags",
            "Expression Bundle Tag",
            "expressions",
            20,
            "tags",
        ),
        (
            ManifestationMetadata(manifestation=metadata.manifestation),
            "labels",
            "Manifestation Bundle Label",
            "manifestations",
            10,
            "labels",
        ),
        (
            ItemMetadata(item=metadata.item),
            "tags",
            "Item Bundle Tag",
            "items",
            1,
            "tags",
        ),
    ]

    for bundle, field, value, source_table, source_id, target_table in cases:
        bundle.add_related(field, value)

        report = bundle.write_to_database(db, fields=(field,))

        assert report.changed is True
        assert report.target_table == source_table
        assert report.target_id == source_id
        assert [row["text"] for row in report.rows_added] == [value]
        assert (source_table, source_id, target_table) in db.interlink_queries


def test_wemi_bundle_write_skips_context_relations_from_other_levels() -> None:
    db = _build_fake_database()
    metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    expression_metadata = metadata.expression_metadata
    assert [row.row_dict["tag"] for row in expression_metadata.tags] == ["Space Opera"]

    expression_metadata.add_related("tags", "Expression Owned Tag")

    report = expression_metadata.write_to_database(db, fields=("tags",))

    assert report.changed is True
    assert report.target_table == "expressions"
    assert report.target_id == 20
    assert [row["text"] for row in report.rows_added] == ["Expression Owned Tag"]
    assert len(report.links_added) == 1


def test_calibre_like_metadata_write_to_database_resolves_target_from_item_id() -> None:
    db = _build_fake_database()
    metadata = CalibreLikeLiuXinBookMetaData(
        title="Permutation City",
        authors=["Greg Egan"],
    )
    metadata.tags = "Calibre Round Trip"

    report = metadata.write_to_database(db, fields=("tags",), item_id=1)

    assert report.changed is True
    assert report.target_table == "works"
    assert report.target_id == 30
    assert [row["text"] for row in report.rows_added] == ["Calibre Round Trip"]

    rehydrated = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    assert list(rehydrated.tags.keys()) == ["Space Opera", "Calibre Round Trip"]


def test_calibre_metadata_view_round_trips_tags_to_database() -> None:
    db = _build_fake_database()
    metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    calibre_metadata = metadata.as_calibre_metadata()
    assert calibre_metadata.db_id == 1

    calibre_metadata.tags = list(calibre_metadata.tags) + ["Calibre View Round Trip"]

    report = calibre_metadata.write_to_database(db, fields=("tags",))

    assert report.changed is True
    assert report.target_table == "works"
    assert report.target_id == 30
    assert [row["text"] for row in report.rows_added] == ["Calibre View Round Trip"]

    rehydrated = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    assert list(rehydrated.tags.keys()) == [
        "Space Opera",
        "Calibre View Round Trip",
    ]


def test_liuxin_wemi_metadata_hydrator_dispatches_typed_shapes() -> None:
    db = _build_fake_database()
    hydrator = LiuXinWEMIMetadataHydrator(db)

    work_metadata = hydrator.hydrate_metadata("work", work_id=30)
    item_metadata = hydrator.hydrate_metadata("item", item_id=1)
    liuxin_metadata = hydrator.hydrate_metadata("liuxin", item_id=1)
    calibre_metadata = hydrator.hydrate_metadata("calibre", item_id=1)

    assert getattr(work_metadata, "work").work_id == 30
    assert getattr(item_metadata, "item").item_id == 1
    assert liuxin_metadata.title == "Permutation City"
    assert list(liuxin_metadata.labels.keys()) == ["Science Fiction"]
    assert list(liuxin_metadata.tags.keys()) == ["Space Opera"]
    assert calibre_metadata.title == "Permutation City"
    assert calibre_metadata.tags == ["Space Opera"]
