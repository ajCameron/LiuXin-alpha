"""
Verify expression hydration from rows, relations, identifiers and assets.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test expression metadata hydrator through its owning regression module::

        python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.containers import (
    ExpressionMetadata,
    ExpressionMetadataHydrator,
)


SINGULARS = {
    "expressions": "expression",
    "works": "work",
    "manifestations": "manifestation",
    "items": "item",
    "agents": "agent",
    "entity_identifiers": "entity_identifier",
    "item_identifiers": "item_identifier",
}


class FakeDriverWrapper:
    """
    Model table identity, link naming and row mutation for item hydrator tests.

    Example:
        Exercise FakeDriverWrapper through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py
    """
    def __init__(self, tables_and_columns: Mapping[str, list[str]]) -> None:
        """
        Initialize the FakeDriverWrapper test double.

        Example:
            Exercise FakeDriverWrapper.init through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :param tables_and_columns: Value supplied for tables and columns in the focused test
            operation.
        :return: None; the function records state or raises through its assertions.
        """
        self.tables_and_columns = dict(tables_and_columns)

    def get_allowed_tables_snapshot(self) -> list[str]:
        """
        Return the immutable table set advertised by the test driver.

        Example:
            Exercise FakeDriverWrapper.get allowed tables snapshot through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return list(self.tables_and_columns)

    def identify_table_from_row_dict(self, row_dict: Mapping[str, Any]) -> str:
        """
        Infer a test table name from the row's identifying columns.

        Example:
            Exercise FakeDriverWrapper.identify table from row dict through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :param row_dict: Value supplied for row dict in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
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
        """
        Return the configured identity column for a table.

        Example:
            Exercise FakeDriverWrapper.get id column through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return f"{SINGULARS[str(table)]}_id"

    def check_for_intralink_table(self, table: str) -> bool:
        """
        Return whether the named test table represents a self-link.

        Example:
            Exercise FakeDriverWrapper.check for intralink table through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :return: True when the tested condition is satisfied; otherwise False.
        """
        return False

    def get_interlinked_tables(self, table: str) -> list[str]:
        """
        Return the table pair connected by a test link table.

        Example:
            Exercise FakeDriverWrapper.get interlinked tables through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return []

    @staticmethod
    def _singular(table: str) -> str:
        """
        Return the deterministic singular form used in test link names.

        Example:
            Exercise FakeDriverWrapper.singular through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return SINGULARS.get(str(table), str(table).rstrip("s"))

    def get_link_table_name(self, table1: str, table2: str) -> str:
        """
        Return the deterministic link-table name for two entity tables.

        Example:
            Exercise FakeDriverWrapper.get link table name through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :param table1: Value supplied for table1 in the focused test operation.
        :param table2: Value supplied for table2 in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        left = self._singular(table1)
        right = self._singular(table2)
        names = sorted((left, right))
        if left == right:
            return f"{left}_{left}_intralinks"
        return f"{names[0]}_{names[1]}_links"

    @staticmethod
    def get_column_base(table_name: str) -> str:
        """
        Return the entity base represented by a link-column name.

        Example:
            Exercise FakeDriverWrapper.get column base through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :param table_name: Table name addressed by the test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        text = str(table_name)
        if text.endswith("_links"):
            return text[:-1]
        if text.endswith("_intralinks"):
            return text[:-1]
        return text.rstrip("s")

    def get_link_column(self, table1: str, table2: str, secondary_id_column: str) -> str:
        """
        Return the link-column name associated with an entity table.

        Example:
            Exercise FakeDriverWrapper.get link column through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :param table1: Value supplied for table1 in the focused test operation.
        :param table2: Value supplied for table2 in the focused test operation.
        :param secondary_id_column: Value supplied for secondary id column in the focused
            test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        link_table = self.get_link_table_name(table1, table2)
        return f"{self.get_column_base(link_table)}_{secondary_id_column}"


@dataclass
class FakeDatabase:
    """
    Provide deterministic in-memory rows, searches and interlinks for item hydration.

    Example:
        Exercise FakeDatabase through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py
    """
    tables_and_columns: dict[str, list[str]]
    driver_wrapper: FakeDriverWrapper = field(init=False)
    uuid: str = "fake-db"
    rows_by_table: dict[str, list[Row]] = field(default_factory=dict)
    interlinks: dict[tuple[str, int, str], list[dict[str, Any]]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """
        Normalize in-memory test state after dataclass initialization.

        Example:
            Exercise FakeDatabase.post init through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :return: None; the function records state or raises through its assertions.
        """
        self.driver_wrapper = FakeDriverWrapper(self.tables_and_columns)

    def get_tables(self, force_refresh: bool = False) -> list[str]:
        """
        Return the table names exposed by the in-memory schema.

        Example:
            Exercise FakeDatabase.get tables through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :param force_refresh: Value supplied for force refresh in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return list(self.tables_and_columns)

    def get_tables_and_columns(self) -> dict[str, list[str]]:
        """
        Return a copied schema mapping for discovery tests.

        Example:
            Exercise FakeDatabase.get tables and columns through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return dict(self.tables_and_columns)

    def get_column_headings(self, table: str) -> set[str]:
        """
        Return the known column names for a test table.

        Example:
            Exercise FakeDatabase.get column headings through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return set(self.tables_and_columns.get(str(table), []))

    def add_row(self, table: str, row_dict: dict[str, Any]) -> Row:
        """
        Insert a copied row into the in-memory table and return its identity.

        Example:
            Exercise FakeDatabase.add row through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :param row_dict: Value supplied for row dict in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        row = Row(self, row_dict=row_dict, read_only=True)
        self.rows_by_table.setdefault(str(table), []).append(row)
        return row

    def get_row_from_id(self, table: str, row_id: int) -> Row | None:
        """
        Return a copied row for the requested identity, or the test double's miss value.

        Example:
            Exercise FakeDatabase.get row from id through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :param row_id: Identity of the row to retrieve or mutate.
        :return: The deterministic value, row, identity or collection described above.
        """
        target_table = str(table)
        target_row_id = int(row_id)
        id_column = self.driver_wrapper.get_id_column(target_table)
        for row in self.rows_by_table.get(target_table, []):
            if int(row.row_dict.get(id_column)) == target_row_id:
                return row
        return None

    def search(self, table: str, column: str, search_term: Any) -> list[Row]:
        """
        Return rows whose selected column satisfies the test query.

        Example:
            Exercise FakeDatabase.search through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :param column: Column name inspected, searched or updated.
        :param search_term: Value supplied for search term in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        out: list[Row] = []
        for row in self.rows_by_table.get(str(table), []):
            if row.row_dict.get(str(column)) == search_term:
                out.append(row)
        return out

    def get_interlink_rows(self, primary_row: Row, secondary_table: str) -> list[dict[str, Any]]:
        """
        Return relation rows matching the supplied source and destination filters.

        Example:
            Exercise FakeDatabase.get interlink rows through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :param primary_row: Value supplied for primary row in the focused test operation.
        :param secondary_table: Value supplied for secondary table in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        key = (str(primary_row.table), int(primary_row.row_id), str(secondary_table))
        return list(self.interlinks.get(key, []))


def _build_fake_database() -> FakeDatabase:
    """
    Perform the build fake database test-helper operation with deterministic inputs.

    Example:
        Exercise build fake database through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


    :return: The deterministic value, row, identity or collection described above.
    """
    tables_and_columns = {
        "expressions": [
            "expression_id",
            "expression_work_id",
            "expression_title_override",
            "expression_label",
            "expression_type",
        ],
        "works": [
            "work_id",
            "work_title",
            "work_canonical_title",
        ],
        "manifestations": [
            "manifestation_id",
            "manifestation_expression_id",
            "manifestation_format_detail",
        ],
        "items": [
            "item_id",
            "item_manifestation_id",
            "item_type",
        ],
        "agents": [
            "agent_id",
            "agent_canonical_name",
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
        "expressions",
        {
            "expression_id": 20,
            "expression_work_id": 30,
            "expression_title_override": "Permutation City",
            "expression_label": "English text",
            "expression_type": "text",
        },
    )
    db.add_row(
        "works",
        {
            "work_id": 30,
            "work_title": "Permutation City",
            "work_canonical_title": "Permutation City",
        },
    )
    db.add_row(
        "works",
        {
            "work_id": 31,
            "work_title": "Permutation City Shared Universe",
            "work_canonical_title": "Permutation City Shared Universe",
        },
    )
    db.add_row(
        "manifestations",
        {
            "manifestation_id": 10,
            "manifestation_expression_id": 20,
            "manifestation_format_detail": "EPUB",
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
        },
    )
    db.add_row(
        "entity_identifiers",
        {
            "entity_identifier_id": 80,
            "entity_identifier_entity_type": "expression",
            "entity_identifier_entity_id": 20,
            "entity_identifier_scheme": "uuid",
            "entity_identifier_value": "expr-uuid-1",
            "entity_identifier_is_primary": 1,
            "entity_identifier_provenance": "fixture",
        },
    )

    db.interlinks[("expressions", 20, "works")] = [
        {
            "expression_work_link_work_id": 30,
            "expression_work_link_priority": 1,
            "expression_work_link_type": "realisation_of",
            "expression_work_link_primary": 1,
        },
        {
            "expression_work_link_work_id": 31,
            "expression_work_link_priority": 2,
            "expression_work_link_type": "adapted_from",
            "expression_work_link_primary": 0,
        },
    ]
    db.interlinks[("expressions", 20, "manifestations")] = [
        {
            "expression_manifestation_link_manifestation_id": 10,
            "expression_manifestation_link_priority": 1,
            "expression_manifestation_link_type": "embodied_as",
            "expression_manifestation_link_primary": 1,
        }
    ]
    db.interlinks[("expressions", 20, "agents")] = [
        {
            "agent_expression_link_agent_id": 40,
            "agent_expression_link_priority": 1,
            "agent_expression_link_type": "author",
            "agent_expression_link_primary": 1,
        }
    ]
    return db


def test_expression_metadata_hydrator_from_expression_id_and_source_row() -> None:
    """
    Verify expression metadata hydrator from expression id and source row.

    Example:
        Exercise test expression metadata hydrator from expression id and source row through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()
    hydrator = ExpressionMetadataHydrator(db)

    container = hydrator.from_expression_id(20)
    assert container.expression is not None
    assert container.expression.expression_id == 20
    work_links = container.get_relation_links("works")
    assert [row.row_id for row in container.get_related("works") if isinstance(row, Row)] == [30, 31]
    assert [link.type for link in work_links] == ["realisation_of", "adapted_from"]
    assert [row.row_id for row in container.get_related("manifestations") if isinstance(row, Row)] == [10]
    assert [row.row_id for row in container.get_related("items") if isinstance(row, Row)] == [1]
    assert [row.row_id for row in container.get_related("agents") if isinstance(row, Row)] == [40]
    assert len(container.get_related("identifiers")) == 1
    assert not hasattr(container, "storage_hints")

    source_row = db.get_row_from_id("expressions", 20)
    assert source_row is not None
    from_source = hydrator.from_source_row(source_row)
    assert from_source.expression is not None
    assert from_source.expression.expression_id == 20


def test_expression_metadata_container_from_database() -> None:
    """
    Verify expression metadata container from database.

    Example:
        Exercise test expression metadata container from database through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()
    container = ExpressionMetadata.from_database(db, expression_id=20)
    assert container.expression is not None
    assert container.expression.expression_id == 20
