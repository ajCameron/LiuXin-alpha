from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.containers import WorkMetadata, WorkMetadataHydrator


SINGULARS = {
    "items": "item",
    "manifestations": "manifestation",
    "expressions": "expression",
    "works": "work",
    "agents": "agent",
    "files": "file",
    "entity_identifiers": "entity_identifier",
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
        "works": [
            "work_id",
            "work_title",
            "work_canonical_title",
            "work_sort_title",
            "work_type",
            "work_medium",
        ],
        "expressions": [
            "expression_id",
            "expression_title_override",
        ],
        "manifestations": [
            "manifestation_id",
            "manifestation_format_detail",
            "manifestation_carrier_type",
        ],
        "items": [
            "item_id",
            "item_manifestation_id",
            "item_type",
        ],
        "agents": [
            "agent_id",
            "agent_canonical_name",
            "agent_sort_name",
        ],
        "files": [
            "file_id",
            "file_item_id",
            "file_extension",
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
        "works",
        {
            "work_id": 30,
            "work_title": "Permutation City",
            "work_canonical_title": "Permutation City",
            "work_sort_title": "Permutation City",
            "work_type": "novel",
            "work_medium": "text",
        },
    )
    db.add_row(
        "expressions",
        {
            "expression_id": 20,
            "expression_title_override": "Permutation City",
        },
    )
    db.add_row(
        "manifestations",
        {
            "manifestation_id": 10,
            "manifestation_format_detail": "EPUB",
            "manifestation_carrier_type": "ebook",
        },
    )
    db.add_row(
        "items",
        {
            "item_id": 1,
            "item_manifestation_id": 10,
            "item_type": "digital",
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
            "file_extension": "epub",
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

    db.interlinks[("works", 30, "expressions")] = [
        {
            "expression_work_link_expression_id": 20,
            "expression_work_link_priority": 1,
            "expression_work_link_type": "realised_by",
            "expression_work_link_primary": 1,
        }
    ]
    db.interlinks[("expressions", 20, "manifestations")] = [
        {
            "expression_manifestation_link_manifestation_id": 10,
            "expression_manifestation_link_priority": 1,
            "expression_manifestation_link_type": "embodied_as",
            "expression_manifestation_link_primary": 1,
        }
    ]
    db.interlinks[("works", 30, "agents")] = [
        {
            "agent_work_link_agent_id": 40,
            "agent_work_link_priority": 1,
            "agent_work_link_type": "author",
            "agent_work_link_primary": 1,
        }
    ]
    return db


def test_work_metadata_hydrator_from_work_id_and_source_row() -> None:
    db = _build_fake_database()
    hydrator = WorkMetadataHydrator(db)

    container = hydrator.from_work_id(30)
    assert isinstance(container, WorkMetadata)

    hints = container.storage_hints()
    assert hints.work_id == 30
    assert hints.title == "Permutation City"
    assert hints.primary_agents == ("Greg Egan",)
    assert hints.manifestation_types == ("ebook",)
    assert hints.file_formats == ("EPUB",)
    assert hints.preferred_filename_stem == "Permutation City - Greg Egan"

    expression_links = container.get_relation_links("expressions")
    assert len(expression_links) == 1
    assert expression_links[0].type == "realised_by"

    manifestation_links = container.get_relation_links("manifestations")
    assert len(manifestation_links) == 1
    assert manifestation_links[0].type == "embodied_as"

    item_links = container.get_relation_links("items")
    assert len(item_links) == 1
    assert item_links[0].type == "manifestation_item"

    identifier_links = container.get_relation_links("identifiers")
    assert len(identifier_links) == 1

    via_mapping = WorkMetadata.from_database(
        db,
        source_row={
            "work_id": 30,
            "expression_id": 20,
            "manifestation_id": 10,
            "item_id": 1,
        },
    )
    assert via_mapping.storage_hints().work_id == 30
