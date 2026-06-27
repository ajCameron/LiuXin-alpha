from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.containers import ItemMetadataContainer, ItemMetadataHydrator


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
    "item_identifiers": "item_identifier",
    "entity_identifiers": "entity_identifier",
    "annotations": "annotation",
    "digital_assets": "digital_asset",
    "composite_digital_assets": "composite_digital_asset",
    "asset_replicas": "asset_replica",
}


class FakeDriverWrapper:
    def __init__(self, tables_and_columns: Mapping[str, list[str]]) -> None:
        self.tables_and_columns = dict(tables_and_columns)

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


@dataclass
class FakeDatabase:
    tables_and_columns: dict[str, list[str]]
    driver_wrapper: FakeDriverWrapper = field(init=False)
    rows_by_table: dict[str, list[Row]] = field(default_factory=dict)
    interlinks: dict[tuple[str, int, str], list[dict[str, Any]]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.driver_wrapper = FakeDriverWrapper(self.tables_and_columns)

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
        out: list[Row] = []
        for row in self.rows_by_table.get(str(table), []):
            if row.row_dict.get(str(column)) == search_term:
                out.append(row)
        return out

    def get_interlink_rows(self, primary_row: Row, secondary_table: str) -> list[dict[str, Any]]:
        key = (str(primary_row.table), int(primary_row.row_id), str(secondary_table))
        return list(self.interlinks.get(key, []))


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
        "stores": [
            "store_id",
            "store_name",
            "store_root_uri",
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
        "stores",
        {
            "store_id": 60,
            "store_name": "Main Store",
            "store_root_uri": "file:///library/main",
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
    return db


def test_item_metadata_hydrator_from_item_id_and_source_row() -> None:
    db = _build_fake_database()
    hydrator = ItemMetadataHydrator(db)

    container = hydrator.from_item_id(1)
    assert isinstance(container, ItemMetadataContainer)

    hints = container.storage_hints()
    assert hints.item_id == 1
    assert hints.manifestation_id == 10
    assert hints.expression_id == 20
    assert hints.work_id == 30
    assert hints.title == "Permutation City"
    assert hints.subtitle == "A Novel"
    assert hints.primary_agents == ("Greg Egan",)
    assert hints.file_formats == ("EPUB",)
    assert hints.preferred_filename_stem == "Permutation City - Greg Egan"
    assert hints.preferred_storage_key == "Greg Egan/Permutation City (30)/Permutation City - Greg Egan.epub"

    work_links = container.get_relation_links("works")
    assert len(work_links) == 1
    assert work_links[0].type == "realises"

    identifier_links = container.get_relation_links("identifiers")
    assert len(identifier_links) == 2

    via_mapping = ItemMetadataContainer.from_database(
        db,
        source_row={
            "item_id": 1,
            "manifestation_id": 10,
            "expression_id": 20,
            "work_id": 30,
        },
    )
    assert via_mapping.storage_hints().work_id == 30
