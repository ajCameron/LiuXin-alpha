"""
Verify item hydration and its database/cache test doubles across relation paths.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test item metadata hydrator through its owning regression module::

        python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any

import pytest

from LiuXin_alpha.caches import (
    CacheAPI,
    CacheCapabilities,
    CacheConsistency,
    CacheLookup,
    CacheLookupStatus,
    CacheQuery,
    CacheQueryResult,
    CacheRecord,
    CacheState,
)
from LiuXin_alpha.databases.column_metadata import (
    ColumnMetadata,
    default_column_metadata,
)
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import DatabaseIntegrityError
from LiuXin_alpha.metadata.api import UnloadedMetadataProjectionError, WorkRelationLink
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
    "notes": "note",
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


def _metadata_values(raw: Any) -> list[Any]:
    """
    Return a normalized snapshot of metadata values used in projection assertions.

    Example:
        Exercise metadata values through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :param raw: Value supplied for raw in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    if raw is None:
        return []
    if isinstance(raw, Mapping):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    try:
        return list(raw)
    except TypeError:
        return [raw]


def _identifier_values(metadata: Any, scheme: str) -> list[Any]:
    """
    Return normalized identifier values used to compare hydrated projections.

    Example:
        Exercise identifier values through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :param metadata: Metadata container or mapping supplied to the assertion helper.
    :param scheme: Value supplied for scheme in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    return _metadata_values(metadata.get_identifiers().get(scheme))


def _projection_snapshot(metadata: Any) -> dict[str, Any]:
    """
    Return the stable projection fields compared across hydration paths.

    Example:
        Exercise projection snapshot through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :param metadata: Metadata container or mapping supplied to the assertion helper.
    :return: The deterministic value, row, identity or collection described above.
    """
    values = metadata.values
    return {
        "tags": values.tags,
        "labels": values.labels,
        "genres": values.genres,
        "subjects": values.subjects,
        "series": values.series,
        "languages": values.languages,
        "ratings": values.ratings,
        "agent_names": values.agent_names,
        "identifiers": {
            scheme: tuple(raw_values)
            for scheme, raw_values in values.identifiers.items()
        },
        "titles": values.titles,
        "primary_title": values.primary_title,
    }


class FakeDriverWrapper:
    """
    Model table identity, link naming and row mutation for item hydrator tests.

    Example:
        Exercise FakeDriverWrapper through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py
    """
    def __init__(
        self,
        tables_and_columns: Mapping[str, list[str]],
        database: "FakeDatabase",
    ) -> None:
        """
        Initialize the FakeDriverWrapper test double.

        Example:
            Exercise FakeDriverWrapper.init through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param tables_and_columns: Value supplied for tables and columns in the focused test
            operation.
        :param database: Database double or adapter under test.
        :return: None; the function records state or raises through its assertions.
        """
        self.tables_and_columns = dict(tables_and_columns)
        self.database = database

    def get_allowed_tables_snapshot(self) -> list[str]:
        """
        Return the immutable table set advertised by the test driver.

        Example:
            Exercise FakeDriverWrapper.get allowed tables snapshot through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return list(self.tables_and_columns)

    def identify_table_from_row_dict(self, row_dict: Mapping[str, Any]) -> str:
        """
        Infer a test table name from the row's identifying columns.

        Example:
            Exercise FakeDriverWrapper.identify table from row dict through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param row_dict: Value supplied for row dict in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        keys = set(row_dict)
        for table, singular in SINGULARS.items():
            id_column = f"{singular}_id"
            if id_column in keys:
                return table
            if singular in keys:
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

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return f"{SINGULARS[str(table)]}_id"

    def check_for_intralink_table(self, table: str) -> bool:
        """
        Return whether the named test table represents a self-link.

        Example:
            Exercise FakeDriverWrapper.check for intralink table through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :return: True when the tested condition is satisfied; otherwise False.
        """
        return False

    def get_interlinked_tables(self, table: str) -> list[str]:
        """
        Return the table pair connected by a test link table.

        Example:
            Exercise FakeDriverWrapper.get interlinked tables through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


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

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return SINGULARS.get(str(table), str(table).rstrip("s"))

    def get_link_table_name(self, table1: str, table2: str) -> str:
        """
        Return the deterministic link-table name for two entity tables.

        Example:
            Exercise FakeDriverWrapper.get link table name through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


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

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


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

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table1: Value supplied for table1 in the focused test operation.
        :param table2: Value supplied for table2 in the focused test operation.
        :param secondary_id_column: Value supplied for secondary id column in the focused
            test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        link_table = self.get_link_table_name(table1, table2)
        return f"{self.get_column_base(link_table)}_{secondary_id_column}"

    def add_row(self, row_dict: Mapping[str, Any]) -> int:
        """
        Insert a copied row into the in-memory table and return its identity.

        Example:
            Exercise FakeDriverWrapper.add row through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param row_dict: Value supplied for row dict in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
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
        """
        Return a copied row for the requested identity, or the test double's miss value.

        Example:
            Exercise FakeDriverWrapper.get row from id through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :param row_id: Identity of the row to retrieve or mutate.
        :return: The deterministic value, row, identity or collection described above.
        """
        row = self.database.get_row_from_id(table, row_id)
        if row is None:
            raise KeyError((table, row_id))
        return dict(row.row_dict)

    def update_column(self, table: str, row_id: int, column: str, new_value: Any) -> bool:
        """
        Update one stored row column for mutation-path assertions.

        Example:
            Exercise FakeDriverWrapper.update column through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :param row_id: Identity of the row to retrieve or mutate.
        :param column: Column name inspected, searched or updated.
        :param new_value: Value supplied for new value in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        row = self.database.get_row_from_id(table, row_id)
        if row is None:
            raise KeyError((table, row_id))
        row.row_dict[str(column)] = new_value
        return True


@dataclass
class FakeDatabase:
    """
    Provide deterministic in-memory rows, searches and interlinks for item hydration.

    Example:
        Exercise FakeDatabase through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py
    """
    tables_and_columns: dict[str, list[str]]
    driver_wrapper: FakeDriverWrapper = field(init=False)
    rows_by_table: dict[str, list[Row]] = field(default_factory=dict)
    interlinks: dict[tuple[str, int, str], list[dict[str, Any]]] = field(default_factory=dict)
    interlink_queries: list[tuple[str, int, str]] = field(default_factory=list)
    search_queries: list[tuple[str, str, Any]] = field(default_factory=list)
    dirtied: list[tuple[str, int, str]] = field(default_factory=list)
    column_case_sensitivity: dict[tuple[str, str], bool] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """
        Normalize in-memory test state after dataclass initialization.

        Example:
            Exercise FakeDatabase.post init through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :return: None; the function records state or raises through its assertions.
        """
        self.driver_wrapper = FakeDriverWrapper(self.tables_and_columns, self)

    def get_tables(self, force_refresh: bool = False) -> list[str]:
        """
        Return the table names exposed by the in-memory schema.

        Example:
            Exercise FakeDatabase.get tables through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


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

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return dict(self.tables_and_columns)

    def get_column_headings(self, table: str) -> set[str]:
        """
        Return the known column names for a test table.

        Example:
            Exercise FakeDatabase.get column headings through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return set(self.tables_and_columns.get(str(table), []))

    def add_row(self, table: str, row_dict: dict[str, Any]) -> Row:
        """
        Insert a copied row into the in-memory table and return its identity.

        Example:
            Exercise FakeDatabase.add row through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


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

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


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

    def delete(self, row: Row) -> None:
        """
        Delete matching in-memory rows and return the affected count.

        Example:
            Exercise FakeDatabase.delete through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param row: Row mapping supplied to the in-memory test double.
        :return: The deterministic value, row, identity or collection described above.
        """
        target_table = str(row.table)
        if row.row_id is None:
            raise KeyError((target_table, None))
        target_row_id = int(row.row_id)
        id_column = self.driver_wrapper.get_id_column(target_table)
        self.rows_by_table[target_table] = [
            existing
            for existing in self.rows_by_table.get(target_table, [])
            if int(existing.row_dict.get(id_column)) != target_row_id
        ]

    def search(self, table: str, column: str, search_term: Any) -> list[Row]:
        """
        Return rows whose selected column satisfies the test query.

        Example:
            Exercise FakeDatabase.search through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :param column: Column name inspected, searched or updated.
        :param search_term: Value supplied for search term in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.search_queries.append((str(table), str(column), search_term))
        out: list[Row] = []
        for row in self.rows_by_table.get(str(table), []):
            if row.row_dict.get(str(column)) == search_term:
                out.append(row)
        return out

    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = True,
    ) -> Iterable[Row]:
        """
        Return copied rows from the requested in-memory table.

        Example:
            Exercise FakeDatabase.get all rows through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :param iterator_return: Value supplied for iterator return in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        rows = list(self.rows_by_table.get(str(table), []))
        return iter(rows) if iterator_return else rows

    def get_case_sensitivity(self, table: str, column: str) -> bool:
        """
        Return the configured database case-sensitivity policy.

        Example:
            Exercise FakeDatabase.get case sensitivity through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :param column: Column name inspected, searched or updated.
        :return: The deterministic value, row, identity or collection described above.
        """
        return self.column_case_sensitivity.get((str(table), str(column)), False)

    def get_column_metadata(self, table: str, column: str) -> ColumnMetadata:
        """
        Return synthetic metadata for a test table column.

        Example:
            Exercise FakeDatabase.get column metadata through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :param column: Column name inspected, searched or updated.
        :return: The deterministic value, row, identity or collection described above.
        """
        metadata = default_column_metadata(table, column)
        return replace(
            metadata,
            case_sensitive=self.get_case_sensitivity(table, column),
        )

    def is_column_case_sensitive(self, table: str, column: str) -> bool:
        """
        Return whether a test column uses case-sensitive matching.

        Example:
            Exercise FakeDatabase.is column case sensitive through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :param column: Column name inspected, searched or updated.
        :return: True when the tested condition is satisfied; otherwise False.
        """
        return self.get_case_sensitivity(table, column)

    def get_interlink_rows(self, primary_row: Row, secondary_table: str) -> list[dict[str, Any]]:
        """
        Return relation rows matching the supplied source and destination filters.

        Example:
            Exercise FakeDatabase.get interlink rows through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param primary_row: Value supplied for primary row in the focused test operation.
        :param secondary_table: Value supplied for secondary table in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
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
        """
        Insert an in-memory relation and return the created link identity.

        Example:
            Exercise FakeDatabase.interlink rows through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param primary_row: Value supplied for primary row in the focused test operation.
        :param secondary_row: Value supplied for secondary row in the focused test
            operation.
        :param priority: Value supplied for priority in the focused test operation.
        :param type: Value supplied for type in the focused test operation.
        :param col_value_pairs: Value supplied for col value pairs in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
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
        """
        Remove matching in-memory relations and return the affected count.

        Example:
            Exercise FakeDatabase.unlink interlink through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param primary_row: Value supplied for primary row in the focused test operation.
        :param secondary_row: Value supplied for secondary row in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
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
        """
        Record that a row requires cache refresh after mutation.

        Example:
            Exercise FakeDatabase.dirty record through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :param row_id: Identity of the row to retrieve or mutate.
        :param reason: Value supplied for reason in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self.dirtied.append((str(table), int(row_id), str(reason)))


class FakeCacheMainTable:
    """
    Expose cached row snapshots and exact-value identifiers for cache-backed hydration.

    Example:
        Exercise FakeCacheMainTable through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py
    """
    def __init__(self, database: FakeDatabase, table: str) -> None:
        """
        Initialize the FakeCacheMainTable test double.

        Example:
            Exercise FakeCacheMainTable.init through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param database: Database double or adapter under test.
        :param table: Table name addressed by the test operation.
        :return: None; the function records state or raises through its assertions.
        """
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
        """
        Return the cached snapshot for one row identity.

        Example:
            Exercise FakeCacheMainTable.get row snapshot through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table_id: Value supplied for table id in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return dict(self._rows[int(table_id)])

    def get_ids_for_value(self, column: str, value: Any) -> set[int]:
        """
        Return cached identities matching an exact field value.

        Example:
            Exercise FakeCacheMainTable.get ids for value through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param column: Column name inspected, searched or updated.
        :param value: Value stored, compared or projected by the operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return {
            row_id
            for row_id, row in self._rows.items()
            if row.get(str(column)) == value
        }


class FakeCacheLinkTable:
    """
    Provide the FakeCacheLinkTable test fixture or double with explicit deterministic behavior.

    Example:
        Exercise FakeCacheLinkTable through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py
    """
    def __init__(
        self,
        database: FakeDatabase,
        primary_table: str,
        secondary_table: str,
        links_by_source_id: Mapping[int, list[dict[str, Any]]],
    ) -> None:
        """
        Initialize the FakeCacheLinkTable test double.

        Example:
            Exercise FakeCacheLinkTable.init through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param database: Database double or adapter under test.
        :param primary_table: Value supplied for primary table in the focused test
            operation.
        :param secondary_table: Value supplied for secondary table in the focused test
            operation.
        :param links_by_source_id: Value supplied for links by source id in the focused test
            operation.
        :return: None; the function records state or raises through its assertions.
        """
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
        """
        Return cached relation rows for one source identity.

        Example:
            Exercise FakeCacheLinkTable.get link rows for src through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param src_id: Source-row identity used to filter or create relations.
        :param require_ordering: Value supplied for require ordering in the focused test
            operation.
        :param type_filter: Value supplied for type filter in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
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
    """
    Provide the FakeStorageCache test fixture or double with explicit deterministic behavior.

    Example:
        Exercise FakeStorageCache through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py
    """
    def __init__(self, database: FakeDatabase) -> None:
        """
        Initialize the FakeStorageCache test double.

        Example:
            Exercise FakeStorageCache.init through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param database: Database double or adapter under test.
        :return: None; the function records state or raises through its assertions.
        """
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
        """
        Perform the is initialized test-helper operation with deterministic inputs.

        Example:
            Exercise FakeStorageCache.is initialized through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :return: True when the tested condition is satisfied; otherwise False.
        """
        return True

    def assert_ready(self) -> None:
        """
        Assert that the cache double is available for reads.

        Example:
            Exercise FakeStorageCache.assert ready through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :return: None; the function records state or raises through its assertions.
        """
        return None

    def get_main_table(self, table: str) -> FakeCacheMainTable:
        """
        Return the cached main-table double for a table.

        Example:
            Exercise FakeStorageCache.get main table through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return self.main_tables[str(table)]

    def get_link_table(self, primary_table: str, secondary_table: str) -> FakeCacheLinkTable:
        """
        Return the cached link-table double for a relation table.

        Example:
            Exercise FakeStorageCache.get link table through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param primary_table: Value supplied for primary table in the focused test
            operation.
        :param secondary_table: Value supplied for secondary table in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
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
        """
        Normalize link for stable comparison.

        Example:
            Exercise FakeStorageCache.normalize link through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param database: Database double or adapter under test.
        :param primary_table: Value supplied for primary table in the focused test
            operation.
        :param source_id: Value supplied for source id in the focused test operation.
        :param secondary_table: Value supplied for secondary table in the focused test
            operation.
        :param link: Value supplied for link in the focused test operation.
        :param index: Value supplied for index in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
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


class FakeCacheFacade(CacheAPI):
    """
    Modern facade-shaped wrapper used by the cache hydrator unit test.

    Example:
        Exercise FakeCacheFacade through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py
    """

    def __init__(self, storage: FakeStorageCache) -> None:
        """
        Initialize the FakeCacheFacade test double.

        Example:
            Exercise FakeCacheFacade.init through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param storage: Value supplied for storage in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self.storage = storage
        self.database = storage.db

    @property
    def state(self) -> CacheState:
        """
        Return the cache lifecycle state exposed to the adapter.

        Example:
            Exercise FakeCacheFacade.state through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return CacheState.READY

    @property
    def generation(self) -> int:
        """
        Return the cache generation used to detect refreshes.

        Example:
            Exercise FakeCacheFacade.generation through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return 1

    @property
    def capabilities(self) -> CacheCapabilities:
        """
        Return the read capabilities advertised by the cache double.

        Example:
            Exercise FakeCacheFacade.capabilities through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return CacheCapabilities(
            consistency=CacheConsistency.SNAPSHOT,
            live_child_objects=False,
            vectorized_helpers=False,
        )

    def load(self) -> None:
        """
        Load deterministic cache state for adapter tests.

        Example:
            Exercise FakeCacheFacade.load through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :return: None; the function records state or raises through its assertions.
        """
        return None

    def reload(self) -> None:
        """
        Record a cache reload and advance the test generation.

        Example:
            Exercise FakeCacheFacade.reload through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :return: None; the function records state or raises through its assertions.
        """
        return None

    def clear(self) -> None:
        """
        Clear cached test data while preserving the configured cache contract.

        Example:
            Exercise FakeCacheFacade.clear through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :return: None; the function records state or raises through its assertions.
        """
        return None

    def close(self) -> None:
        """
        Mark the cache double closed for lifecycle assertions.

        Example:
            Exercise FakeCacheFacade.close through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :return: None; the function records state or raises through its assertions.
        """
        return None

    def table_columns(self) -> Mapping[str, tuple[str, ...]]:
        """
        Return cached column names for a table.

        Example:
            Exercise FakeCacheFacade.table columns through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return {
            str(table_name): tuple(
                str(column) for column in table.column_headings
            )
            for table_name, table in self.storage.main_tables.items()
        }

    def get(self, table: str, row_id: int) -> CacheLookup[CacheRecord]:
        """
        Perform the get test-helper operation with deterministic inputs.

        Example:
            Exercise FakeCacheFacade.get through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table: Table name addressed by the test operation.
        :param row_id: Identity of the row to retrieve or mutate.
        :return: The deterministic value, row, identity or collection described above.
        """
        table_cache = self.storage.main_tables.get(str(table))
        snapshot = (
            table_cache._rows.get(int(row_id))
            if table_cache is not None
            else None
        )
        if snapshot is None:
            return CacheLookup(
                CacheLookupStatus.MISS,
                None,
                True,
                self.generation,
            )
        return CacheLookup(
            CacheLookupStatus.HIT,
            CacheRecord(str(table), int(row_id), snapshot),
            True,
            self.generation,
        )

    def query(self, query: CacheQuery) -> CacheQueryResult:
        """
        Return a structured cached query result with explicit completeness.

        Example:
            Exercise FakeCacheFacade.query through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param query: Query value or structured selector exercised by the test.
        :return: The deterministic value, row, identity or collection described above.
        """
        table_cache = self.storage.main_tables[str(query.table)]
        records = []
        for row_id, snapshot in table_cache._rows.items():
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
            Exercise FakeCacheFacade.link records through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param source_table: Value supplied for source table in the focused test operation.
        :param source_id: Value supplied for source id in the focused test operation.
        :param target_table: Value supplied for target table in the focused test operation.
        :param type_filter: Value supplied for type filter in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        rows = self.storage.get_link_table(
            source_table,
            target_table,
        ).get_link_rows_for_src(
            int(source_id),
            require_ordering=True,
            type_filter=type_filter,
        )
        return tuple(
            CacheRecord("link", -(index + 1), row)
            for index, row in enumerate(rows)
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
            Exercise FakeCacheFacade.related through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param source_table: Value supplied for source table in the focused test operation.
        :param source_ids: Value supplied for source ids in the focused test operation.
        :param target_table: Value supplied for target table in the focused test operation.
        :param type_filter: Value supplied for type filter in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        secondary_id_column = self.database.driver_wrapper.get_id_column(
            target_table
        )
        secondary_link_column = self.database.driver_wrapper.get_link_column(
            source_table,
            target_table,
            secondary_id_column,
        )
        records: list[CacheRecord] = []
        for source_id in source_ids:
            for link in self.link_records(
                source_table,
                int(source_id),
                target_table,
                type_filter=type_filter,
            ):
                target_id = int(link[secondary_link_column])
                lookup = self.get(target_table, target_id)
                if lookup.value is not None:
                    records.append(lookup.value)
        return CacheQueryResult(
            tuple(records),
            len(records),
            0,
            None,
            True,
            self.generation,
        )

    def invalidate(self, **_kwargs: Any) -> None:
        """
        Record invalidated tables or identities for mutation tests.

        Example:
            Exercise FakeCacheFacade.invalidate through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param _kwargs: Value supplied for kwargs in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        return None

    def create_writer(self, *_args: Any, **_kwargs: Any) -> Any:
        """
        Return the configured writer double for cache-backed mutations.

        Example:
            Exercise FakeCacheFacade.create writer through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param _args: Value supplied for args in the focused test operation.
        :param _kwargs: Value supplied for kwargs in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise NotImplementedError

    def write(self, *_args: Any, **_kwargs: Any) -> Mapping[Any, Any]:
        """
        Record a batch mutation and return its configured result.

        Example:
            Exercise FakeCacheFacade.write through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param _args: Value supplied for args in the focused test operation.
        :param _kwargs: Value supplied for kwargs in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise NotImplementedError

    def write_one(self, *_args: Any, **_kwargs: Any) -> Mapping[Any, Any]:
        """
        Record one mutation and return its configured result.

        Example:
            Exercise FakeCacheFacade.write one through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param _args: Value supplied for args in the focused test operation.
        :param _kwargs: Value supplied for kwargs in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise NotImplementedError


def _build_fake_database() -> FakeDatabase:
    """
    Perform the build fake database test-helper operation with deterministic inputs.

    Example:
        Exercise build fake database through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: The deterministic value, row, identity or collection described above.
    """
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
        "manifestations",
        {
            "manifestation_id": 11,
            "manifestation_format_detail": "audiobook",
            "manifestation_subtitle": "Audio Edition",
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
    db.interlinks[("items", 1, "manifestations")] = [
        {
            "item_manifestation_link_manifestation_id": 10,
            "item_manifestation_link_priority": 1,
            "item_manifestation_link_primary": 1,
            "item_manifestation_link_type": "stored_as",
        },
        {
            "item_manifestation_link_manifestation_id": 11,
            "item_manifestation_link_priority": 2,
            "item_manifestation_link_primary": 0,
            "item_manifestation_link_type": "also_available_as",
        },
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


def _add_alternate_primary_spine(db: FakeDatabase) -> None:
    """
    Perform the add alternate primary spine test-helper operation with deterministic inputs.

    Example:
        Exercise add alternate primary spine through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :param db: Value supplied for db in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    db.add_row(
        "expressions",
        {
            "expression_id": 21,
            "expression_title_override": "Alternate expression",
        },
    )
    db.add_row(
        "works",
        {
            "work_id": 31,
            "work_title": "Alternate Work",
            "work_canonical_title": "Alternate Work",
            "work_sort_title": "Alternate Work",
        },
    )
    db.interlinks[("items", 1, "manifestations")] = [
        {
            "item_manifestation_link_id": "im-10",
            "item_manifestation_link_manifestation_id": 10,
            "item_manifestation_link_priority": 2,
            "item_manifestation_link_primary": 0,
            "item_manifestation_link_type": "stored_as",
        },
        {
            "item_manifestation_link_id": "im-11",
            "item_manifestation_link_manifestation_id": 11,
            "item_manifestation_link_priority": 1,
            "item_manifestation_link_primary": 1,
            "item_manifestation_link_type": "preferred_variant",
        },
    ]
    db.interlinks[("manifestations", 11, "expressions")] = [
        {
            "expression_manifestation_link_id": "em-21",
            "expression_manifestation_link_expression_id": 21,
            "expression_manifestation_link_priority": 1,
            "expression_manifestation_link_primary": 1,
            "expression_manifestation_link_type": "preferred_expression",
        }
    ]
    db.interlinks[("expressions", 21, "works")] = [
        {
            "expression_work_link_id": "ew-31",
            "expression_work_link_work_id": 31,
            "expression_work_link_priority": 1,
            "expression_work_link_primary": 1,
            "expression_work_link_type": "preferred_work",
        }
    ]


def _add_no_primary_priority_spine(db: FakeDatabase) -> None:
    """
    Perform the add no primary priority spine test-helper operation with deterministic inputs.

    Example:
        Exercise add no primary priority spine through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :param db: Value supplied for db in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    db.add_row(
        "expressions",
        {
            "expression_id": 21,
            "expression_title_override": "Fallback expression",
        },
    )
    db.add_row(
        "expressions",
        {
            "expression_id": 22,
            "expression_title_override": "Preferred by priority",
        },
    )
    db.add_row(
        "works",
        {
            "work_id": 31,
            "work_title": "Fallback Work",
            "work_canonical_title": "Fallback Work",
            "work_sort_title": "Fallback Work",
        },
    )
    db.add_row(
        "works",
        {
            "work_id": 32,
            "work_title": "Priority Work",
            "work_canonical_title": "Priority Work",
            "work_sort_title": "Priority Work",
        },
    )
    db.interlinks[("items", 1, "manifestations")] = [
        {
            "item_manifestation_link_id": "im-fallback-10",
            "item_manifestation_link_manifestation_id": 10,
            "item_manifestation_link_priority": 4,
            "item_manifestation_link_primary": 0,
            "item_manifestation_link_type": "stored_as",
        },
        {
            "item_manifestation_link_id": "im-fallback-11",
            "item_manifestation_link_manifestation_id": 11,
            "item_manifestation_link_priority": 1,
            "item_manifestation_link_primary": 0,
            "item_manifestation_link_type": "preferred_by_priority",
        },
    ]
    db.interlinks[("manifestations", 11, "expressions")] = [
        {
            "expression_manifestation_link_id": "em-fallback-21",
            "expression_manifestation_link_expression_id": 21,
            "expression_manifestation_link_priority": 3,
            "expression_manifestation_link_primary": 0,
            "expression_manifestation_link_type": "fallback_expression",
        },
        {
            "expression_manifestation_link_id": "em-fallback-22",
            "expression_manifestation_link_expression_id": 22,
            "expression_manifestation_link_priority": 1,
            "expression_manifestation_link_primary": 0,
            "expression_manifestation_link_type": "preferred_by_priority",
        },
    ]
    db.interlinks[("expressions", 22, "works")] = [
        {
            "expression_work_link_id": "ew-fallback-31",
            "expression_work_link_work_id": 31,
            "expression_work_link_priority": 2,
            "expression_work_link_primary": 0,
            "expression_work_link_type": "fallback_work",
        },
        {
            "expression_work_link_id": "ew-fallback-32",
            "expression_work_link_work_id": 32,
            "expression_work_link_priority": 1,
            "expression_work_link_primary": 0,
            "expression_work_link_type": "preferred_by_priority",
        },
    ]


def test_item_metadata_hydrator_from_item_id_and_source_row() -> None:
    """
    Verify item metadata hydrator from item id and source row.

    Example:
        Exercise test item metadata hydrator from item id and source row through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()
    hydrator = ItemMetadataHydrator(db)

    container = hydrator.from_item_id(1)
    assert isinstance(container, ItemMetadata)
    assert container.item is not None
    assert container.item.item_id == 1
    assert container.item.item_manifestation_id == 10
    assert not hasattr(container, "storage_hints")

    manifestation_links = container.get_relation_links("manifestations")
    assert [
        link.target.row_id
        for link in manifestation_links
        if isinstance(link.target, Row)
    ] == [10, 11]
    assert [link.type for link in manifestation_links] == [
        "stored_as",
        "also_available_as",
    ]

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


def test_wemi_hydrator_selected_spine_uses_primary_links_without_collapsing_graph() -> None:
    """
    Verify wemi hydrator selected spine uses primary links without collapsing graph.

    Example:
        Exercise test wemi hydrator selected spine uses primary links without collapsing graph through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()
    _add_alternate_primary_spine(db)

    metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)

    assert metadata.item is not None
    assert metadata.item.item_manifestation_id == 10
    assert metadata.manifestation is not None
    assert metadata.manifestation.manifestation_id == 11
    assert metadata.expression is not None
    assert metadata.expression.expression_id == 21
    assert metadata.work is not None
    assert metadata.work.work_id == 31

    assert [
        row.row_id
        for row in metadata.get_wemi_related("item", "manifestations")
        if isinstance(row, Row)
    ] == [10, 11]
    assert metadata.get_primary_wemi_related("item", "manifestations").row_id == 11
    assert metadata.get_wemi_relation_link_ids("item", "manifestations") == (
        "im-10",
        "im-11",
    )

    sidecar = metadata.to_sidecar_mapping()
    round_tripped = LiuXinWEMIMetadata.from_mapping(sidecar)

    assert round_tripped.get_wemi_relation_link_ids("item", "manifestations") == (
        "im-10",
        "im-11",
    )
    assert round_tripped.get_primary_wemi_related("item", "manifestations")[
        "manifestation_id"
    ] == 11


def test_lazy_wemi_hydrator_selected_spine_matches_primary_graph_links() -> None:
    """
    Verify lazy wemi hydrator selected spine matches primary graph links.

    Example:
        Exercise test lazy wemi hydrator selected spine matches primary graph links through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()
    _add_alternate_primary_spine(db)

    metadata = LazyLiuXinWEMIMetadataHydrator(db).get_lazy_liuxin_wemi_metadata(item_id=1)

    assert metadata.manifestation is not None
    assert metadata.manifestation.manifestation_id == 11
    assert metadata.expression is not None
    assert metadata.expression.expression_id == 21
    assert metadata.work is not None
    assert metadata.work.work_id == 31

    assert [
        row.row_id
        for row in metadata.get_wemi_related("item", "manifestations")
        if isinstance(row, Row)
    ] == [11, 10]
    assert metadata.get_primary_wemi_related("item", "manifestations").row_id == 11


def test_wemi_hydrator_no_primary_spine_uses_priority_fallback() -> None:
    """
    Verify wemi hydrator no primary spine uses priority fallback.

    Example:
        Exercise test wemi hydrator no primary spine uses priority fallback through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()
    _add_no_primary_priority_spine(db)

    metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)

    assert metadata.manifestation is not None
    assert metadata.manifestation.manifestation_id == 11
    assert metadata.expression is not None
    assert metadata.expression.expression_id == 22
    assert metadata.work is not None
    assert metadata.work.work_id == 32

    assert metadata.get_primary_wemi_related("item", "manifestations").row_id == 11
    assert (
        metadata.get_primary_wemi_related("manifestation", "expressions").row_id
        == 22
    )
    assert metadata.get_primary_wemi_related("expression", "works").row_id == 32


def test_lazy_wemi_hydrator_no_primary_spine_uses_priority_fallback() -> None:
    """
    Verify lazy wemi hydrator no primary spine uses priority fallback.

    Example:
        Exercise test lazy wemi hydrator no primary spine uses priority fallback through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()
    _add_no_primary_priority_spine(db)

    metadata = LazyLiuXinWEMIMetadataHydrator(db).get_lazy_liuxin_wemi_metadata(item_id=1)

    assert metadata.manifestation is not None
    assert metadata.manifestation.manifestation_id == 11
    assert metadata.expression is not None
    assert metadata.expression.expression_id == 22
    assert metadata.work is not None
    assert metadata.work.work_id == 32

    assert [
        row.row_id
        for row in metadata.get_wemi_related("manifestation", "expressions")
        if isinstance(row, Row)
    ] == [22, 21]
    assert [
        row.row_id
        for row in metadata.get_wemi_related("expression", "works")
        if isinstance(row, Row)
    ] == [32, 31]


def test_liuxin_wemi_metadata_hydrator_builds_complete_item_slice() -> None:
    """
    Verify liuxin wemi metadata hydrator builds complete item slice.

    Example:
        Exercise test liuxin wemi metadata hydrator builds complete item slice through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify liuxin wemi metadata hydrator can read from loaded cache source.

    Example:
        Exercise test liuxin wemi metadata hydrator can read from loaded cache source through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()
    cache_source = CacheMetadataReadSource(
        FakeCacheFacade(FakeStorageCache(db)),
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
    """
    Verify liuxin wemi metadata from database uses central hydrator.

    Example:
        Exercise test liuxin wemi metadata from database uses central hydrator through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()

    metadata = LiuXinWEMIMetadata.from_database(db, item_id=1)

    assert metadata.item is not None
    assert metadata.item.item_id == 1
    assert metadata.database_ids["work_id"] == 30
    assert metadata.title == "Permutation City"


def test_lazy_liuxin_wemi_metadata_defers_relation_backed_fields() -> None:
    """
    Verify lazy liuxin wemi metadata defers relation backed fields.

    Example:
        Exercise test lazy liuxin wemi metadata defers relation backed fields through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify lazy liuxin wemi metadata can force hydrate fields.

    Example:
        Exercise test lazy liuxin wemi metadata can force hydrate fields through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
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
        "identifiers",
        "languages_available",
    }
    assert list(metadata.direct_get("tags").keys()) == ["Space Opera"]
    assert list(metadata.direct_get("labels").keys()) == ["Science Fiction"]


def test_lazy_wemi_projection_errors_do_not_materialize_hydrated_dependencies() -> None:
    """
    Verify lazy wemi projection errors do not materialize hydrated dependencies.

    Example:
        Exercise test lazy wemi projection errors do not materialize hydrated dependencies through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()
    metadata = LazyLiuXinWEMIMetadataHydrator(db).get_lazy_liuxin_wemi_metadata(item_id=1)
    interlink_queries = list(db.interlink_queries)

    with pytest.raises(UnloadedMetadataProjectionError) as error_info:
        metadata.values.tags

    assert error_info.value.relation_key == "tags"
    assert set(error_info.value.unloaded_dependencies) >= {
        "legacy:tags",
        "work:tags",
    }
    assert metadata.is_lazy_field_loaded("tags") is False
    assert "tags" in metadata.lazy_fields()
    assert db.interlink_queries == interlink_queries


def test_lazy_wemi_projection_loads_one_field_and_keeps_other_fields_guarded() -> None:
    """
    Verify lazy wemi projection loads one field and keeps other fields guarded.

    Example:
        Exercise test lazy wemi projection loads one field and keeps other fields guarded through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    eager_db = _build_fake_database()
    eager = LiuXinWEMIMetadataHydrator(eager_db).get_liuxin_wemi_metadata(item_id=1)
    lazy_db = _build_fake_database()
    lazy = LazyLiuXinWEMIMetadataHydrator(lazy_db).get_lazy_liuxin_wemi_metadata(
        item_id=1,
    )

    assert lazy.load("tags") is lazy
    assert lazy.values.tags == eager.values.tags
    assert lazy.text.tags == eager.text.tags

    with pytest.raises(UnloadedMetadataProjectionError) as error_info:
        lazy.values.labels

    assert error_info.value.relation_key == "labels"
    assert "labels" in lazy.lazy_fields()


def test_lazy_wemi_projection_force_hydrate_selected_fields_matches_eager() -> None:
    """
    Verify lazy wemi projection force hydrate selected fields matches eager.

    Example:
        Exercise test lazy wemi projection force hydrate selected fields matches eager through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    eager_db = _build_fake_database()
    eager = LiuXinWEMIMetadataHydrator(eager_db).get_liuxin_wemi_metadata(item_id=1)
    lazy_db = _build_fake_database()
    lazy = LazyLiuXinWEMIMetadataHydrator(lazy_db).get_lazy_liuxin_wemi_metadata(
        item_id=1,
    )

    assert lazy.force_hydrate(fields=("tags", "labels", "identifiers")) is lazy
    assert lazy.values.tags == eager.values.tags
    assert lazy.values.labels == eager.values.labels
    assert dict(lazy.values.identifiers) == dict(eager.values.identifiers)

    with pytest.raises(UnloadedMetadataProjectionError) as error_info:
        lazy.values.genres

    assert error_info.value.relation_key == "genres"


def test_lazy_wemi_projection_full_load_matches_eager_and_repeated_access_is_stable() -> None:
    """
    Verify lazy wemi projection full load matches eager and repeated access remains stable.

    Example:
        Exercise test lazy wemi projection full load matches eager and repeated access is stable through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    eager_db = _build_fake_database()
    eager = LiuXinWEMIMetadataHydrator(eager_db).get_liuxin_wemi_metadata(item_id=1)
    lazy_db = _build_fake_database()
    lazy = LazyLiuXinWEMIMetadataHydrator(lazy_db).get_lazy_liuxin_wemi_metadata(
        item_id=1,
    )

    assert lazy.load() is lazy
    assert _projection_snapshot(lazy) == _projection_snapshot(eager)

    interlink_queries = list(lazy_db.interlink_queries)
    search_queries = list(lazy_db.search_queries)

    assert _projection_snapshot(lazy) == _projection_snapshot(eager)
    assert lazy.text.tags == eager.text.tags
    assert lazy.text.agent_names == eager.text.agent_names
    assert lazy_db.interlink_queries == interlink_queries
    assert lazy_db.search_queries == search_queries


def test_liuxin_wemi_metadata_write_to_database_adds_missing_relation_terms() -> None:
    """
    Verify liuxin wemi metadata write to database adds missing relation terms.

    Example:
        Exercise test liuxin wemi metadata write to database adds missing relation terms through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
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


def test_liuxin_wemi_metadata_writer_obeys_column_case_sensitivity() -> None:
    """
    Verify liuxin wemi metadata writer obeys column case sensitivity.

    Example:
        Exercise test liuxin wemi metadata writer obeys column case sensitivity through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()
    db.tables_and_columns["notes"] = ["note_id", "note"]
    db.driver_wrapper.tables_and_columns["notes"] = ["note_id", "note"]
    db.add_row("notes", {"note_id": 97, "note": "Mind US"})
    db.interlinks[("works", 30, "notes")] = [
        {
            "note_work_link_note_id": 97,
            "note_work_link_priority": 1,
            "note_work_link_source": "fixture",
        }
    ]
    db.column_case_sensitivity[("tags", "tag")] = False
    db.column_case_sensitivity[("labels", "label_text")] = False
    db.column_case_sensitivity[("notes", "note")] = True
    metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)

    metadata.tags = "space opera"
    metadata.labels = "science fiction"
    metadata.notes = "Mind us"

    report = metadata.write_to_database(
        db,
        fields=("tags", "labels", "notes"),
        mark_dirty=False,
    )

    assert [row["text"] for row in report.rows_added] == ["Mind us"]
    assert [row.row_dict["tag"] for row in db.rows_by_table["tags"]] == [
        "Space Opera",
    ]
    assert [row.row_dict["label_text"] for row in db.rows_by_table["labels"]] == [
        "Science Fiction",
    ]
    assert [row.row_dict["note"] for row in db.rows_by_table["notes"]] == [
        "Mind US",
        "Mind us",
    ]

    rehydrated = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    assert [
        target.row_dict["tag"]
        for target in rehydrated.get_wemi_related("work", "tags")
    ] == ["Space Opera"]
    assert list(rehydrated.labels.keys()) == ["Science Fiction"]
    assert [
        target.row_dict["note"]
        for target in rehydrated.get_wemi_related("work", "notes")
    ] == ["Mind US", "Mind us"]


def test_liuxin_wemi_metadata_write_to_database_can_replace_relation_terms() -> None:
    """
    Verify liuxin wemi metadata write to database can replace relation terms.

    Example:
        Exercise test liuxin wemi metadata write to database can replace relation terms through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
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


def test_liuxin_wemi_metadata_write_to_database_can_replace_identifier_rows() -> None:
    """
    Verify liuxin wemi metadata write to database can replace identifier rows.

    Example:
        Exercise test liuxin wemi metadata write to database can replace identifier rows through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()
    metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    assert [
        row.row_dict["entity_identifier_value"]
        for row in db.rows_by_table["entity_identifiers"]
    ] == ["OL123W"]

    metadata.set_identifiers({"doi": {"10.5555/replacement"}}, update=False)

    report = metadata.write_to_database(db, fields=("identifiers",), replace=True)

    assert report.changed is True
    assert report.skipped == []
    assert report.errors == []
    assert [row["value"] for row in report.rows_removed] == ["OL123W"]
    assert {row["value"] for row in report.rows_added} == {
        "9780000000001",
        "10.5555/replacement",
    }

    identifier_rows = db.rows_by_table["entity_identifiers"]
    assert {
        row.row_dict["entity_identifier_value"]: row.row_dict["entity_identifier_is_primary"]
        for row in identifier_rows
    } == {
        "9780000000001": 1,
        "10.5555/replacement": 1,
    }

    rehydrated = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    assert _identifier_values(rehydrated, "openlibrary") == []
    assert _identifier_values(rehydrated, "doi") == ["10.5555/replacement"]
    assert _identifier_values(rehydrated, "isbn") == ["9780000000001"]


def test_liuxin_wemi_metadata_write_marks_existing_identifier_primary() -> None:
    """
    Verify liuxin wemi metadata write marks existing identifier primary.

    Example:
        Exercise test liuxin wemi metadata write marks existing identifier primary through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()
    for row in db.rows_by_table["entity_identifiers"]:
        row.row_dict["entity_identifier_is_primary"] = 0
    metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)

    report = metadata.write_to_database(db, fields=("identifiers",))

    assert report.changed is True
    assert {
        (
            row["scheme"],
            row["value"],
            row["column"],
            row["new_value"],
        )
        for row in report.rows_updated
    } == {("openlibrary", "OL123W", "entity_identifier_is_primary", 1)}
    assert [
        row.row_dict["entity_identifier_is_primary"]
        for row in db.rows_by_table["entity_identifiers"]
        if row.row_dict["entity_identifier_value"] == "OL123W"
    ] == [1]


def test_liuxin_wemi_metadata_write_adds_existing_term_link_with_relation_metadata() -> None:
    """
    Verify liuxin wemi metadata write adds existing term link with relation metadata.

    Example:
        Exercise test liuxin wemi metadata write adds existing term link with relation metadata through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()
    db.add_row(
        "tags",
        {
            "tag_id": 99,
            "tag": "Metadata Writer Existing",
            "tag_phash": "metadatawriterexisting",
        },
    )
    metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    metadata.add_wemi_relation_link(
        "work",
        "tags",
        WorkRelationLink(
            target={
                "tag_id": 99,
                "tag": "Metadata Writer Existing",
            },
            priority=7,
            primary=True,
            type="curated",
            origin="unit-origin",
            source="unit-source",
            policy="unit-policy",
            data="unit-data",
            index=3,
            extra={"source_entity_type": "work"},
        ),
    )

    report = metadata.write_to_database(db, fields=("tags",), mark_dirty=False)

    assert report.changed is True
    assert report.rows_added == []
    assert len(report.links_added) == 1
    assert db.dirtied == []
    link = db.interlinks[("works", 30, "tags")][-1]
    assert link["tag_work_link_tag_id"] == 99
    assert link["tag_work_link_type"] == "curated"
    assert link["tag_work_link_primary"] == 1
    assert link["tag_work_link_origin"] == "unit-origin"
    assert link["tag_work_link_source"] == "unit-source"
    assert link["tag_work_link_policy"] == "unit-policy"
    assert link["tag_work_link_data"] == "unit-data"
    assert link["tag_work_link_index"] == 3


def test_calibre_like_metadata_write_to_database_accepts_explicit_target_row_mapping() -> None:
    """
    Verify calibre like metadata write to database accepts explicit target row mapping.

    Example:
        Exercise test calibre like metadata write to database accepts explicit target row mapping through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()
    metadata = CalibreLikeLiuXinBookMetaData(
        title="Permutation City",
        authors=["Greg Egan"],
    )
    metadata.tags = "Expression Target Row Tag"

    report = metadata.write_to_database(
        db,
        fields=("tags",),
        target_level="expression",
        target_row={"expression_id": 20},
    )

    assert report.changed is True
    assert report.target_level == "expression"
    assert report.target_table == "expressions"
    assert report.target_id == 20
    assert [row["text"] for row in report.rows_added] == [
        "Expression Target Row Tag",
    ]
    assert db.interlinks[("expressions", 20, "tags")][-1][
        "expression_tag_link_tag_id"
    ] == report.links_added[0]["target"]["row_id"]


def test_metadata_writer_skips_missing_relation_table_and_identifier_columns() -> None:
    """
    Verify metadata writer skips missing relation table and identifier columns.

    Example:
        Exercise test metadata writer skips missing relation table and identifier columns through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()
    del db.tables_and_columns["tags"]
    metadata = CalibreLikeLiuXinBookMetaData(
        title="Permutation City",
        authors=["Greg Egan"],
    )
    metadata.tags = "Missing Table Tag"

    report = metadata.write_to_database(db, fields=("tags",), item_id=1)

    assert report.changed is False
    assert report.fields_checked == ["tags"]
    assert report.skipped == ["tags: table 'tags' is not present."]
    assert report.rows_added == []
    assert report.links_added == []

    db = _build_fake_database()
    db.tables_and_columns["entity_identifiers"].remove("entity_identifier_value")
    metadata = CalibreLikeLiuXinBookMetaData(
        title="Permutation City",
        authors=["Greg Egan"],
    )
    metadata.set_identifier("doi", "10.5555/missing-column")

    report = metadata.write_to_database(db, fields=("identifiers",), item_id=1)

    assert report.changed is False
    assert report.fields_checked == ["identifiers"]
    assert report.rows_added == []
    assert report.skipped == [
        "identifiers: table 'entity_identifiers' is missing columns "
        "entity_identifier_value."
    ]


def test_metadata_writer_skips_unsupported_relation_pairs() -> None:
    """
    Verify metadata writer skips unsupported relation pairs.

    Example:
        Exercise test metadata writer skips unsupported relation pairs through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()
    original_link_table_name = db.driver_wrapper.get_link_table_name

    def unsupported_work_tag_link(table1: str, table2: str) -> str:
        """
        Perform the unsupported work tag link test-helper operation with deterministic inputs.

        Example:
            Exercise test metadata writer skips unsupported relation pairs.unsupported work tag link through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table1: Value supplied for table1 in the focused test operation.
        :param table2: Value supplied for table2 in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        if (str(table1), str(table2)) == ("works", "tags"):
            return ""
        return original_link_table_name(table1, table2)

    setattr(db.driver_wrapper, "get_link_table_name", unsupported_work_tag_link)
    metadata = CalibreLikeLiuXinBookMetaData(
        title="Permutation City",
        authors=["Greg Egan"],
    )
    metadata.tags = "Unsupported Link Tag"

    report = metadata.write_to_database(
        db,
        fields=("tags",),
        target_level="work",
        target_row={"work_id": 30},
    )

    assert report.changed is False
    assert report.skipped == ["tags: 'works' cannot link to 'tags'."]
    assert report.rows_added == []
    assert report.links_added == []


def test_metadata_writer_reports_failed_link_and_unlink_operations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Verify metadata writer reports failed link and unlink operations.

    Example:
        Exercise test metadata writer reports failed link and unlink operations through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()
    metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    metadata.tags = "Link Failure Tag"

    def fail_interlink_rows(*_args: Any, **_kwargs: Any) -> None:
        """
        Perform the fail interlink rows test-helper operation with deterministic inputs.

        Example:
            Exercise test metadata writer reports failed link and unlink operations.fail interlink rows through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param _args: Value supplied for args in the focused test operation.
        :param _kwargs: Value supplied for kwargs in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise RuntimeError("link failed")

    monkeypatch.setattr(db, "interlink_rows", fail_interlink_rows)

    report = metadata.write_to_database(db, fields=("tags",))

    assert report.changed is True
    assert [row["text"] for row in report.rows_added] == ["Link Failure Tag"]
    assert report.links_added == []
    assert report.errors == [
        "tags: could not link works:30 to tags:92.",
    ]
    rehydrated = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    assert "Link Failure Tag" not in rehydrated.tags

    db = _build_fake_database()
    metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    metadata.nullify("tags")

    def fail_unlink_interlink(*_args: Any, **_kwargs: Any) -> None:
        """
        Perform the fail unlink interlink test-helper operation with deterministic inputs.

        Example:
            Exercise test metadata writer reports failed link and unlink operations.fail unlink interlink through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param _args: Value supplied for args in the focused test operation.
        :param _kwargs: Value supplied for kwargs in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise RuntimeError("unlink failed")

    monkeypatch.setattr(db, "unlink_interlink", fail_unlink_interlink)

    report = metadata.write_to_database(db, fields=("tags",), replace=True)

    assert report.changed is False
    assert report.links_removed == []
    assert report.errors == [
        "tags: could not remove link from works:30 to tags:91.",
    ]
    rehydrated = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    assert list(rehydrated.tags.keys()) == ["Space Opera"]


def test_metadata_writer_reports_failed_identifier_delete_and_primary_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Verify metadata writer reports failed identifier delete and primary update.

    Example:
        Exercise test metadata writer reports failed identifier delete and primary update through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()
    metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    metadata.set_identifiers({"doi": {"10.5555/delete-failure"}}, update=False)

    def fail_delete(_row: Row) -> None:
        """
        Perform the fail delete test-helper operation with deterministic inputs.

        Example:
            Exercise test metadata writer reports failed identifier delete and primary update.fail delete through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param _row: Value supplied for row in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise RuntimeError("delete failed")

    monkeypatch.setattr(db, "delete", fail_delete)

    report = metadata.write_to_database(db, fields=("identifiers",), replace=True)

    assert report.changed is True
    assert report.rows_removed == []
    assert any("could not remove row entity_identifiers:80" in error for error in report.errors)
    assert [
        row.row_dict["entity_identifier_value"]
        for row in db.rows_by_table["entity_identifiers"]
        if row.row_dict["entity_identifier_value"] == "OL123W"
    ] == ["OL123W"]

    db = _build_fake_database()
    for row in db.rows_by_table["entity_identifiers"]:
        row.row_dict["entity_identifier_is_primary"] = 0
    metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)

    def fail_update_column(
        _table: str,
        _row_id: int,
        _column: str,
        _new_value: Any,
    ) -> None:
        """
        Perform the fail update column test-helper operation with deterministic inputs.

        Example:
            Exercise test metadata writer reports failed identifier delete and primary update.fail update column through its owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param _table: Value supplied for table in the focused test operation.
        :param _row_id: Value supplied for row id in the focused test operation.
        :param _column: Value supplied for column in the focused test operation.
        :param _new_value: Value supplied for new value in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise RuntimeError("update failed")

    monkeypatch.setattr(db.driver_wrapper, "update_column", fail_update_column)

    report = metadata.write_to_database(db, fields=("identifiers",))

    assert report.rows_updated == []
    assert any(
        "could not mark row entity_identifiers:80 as primary" in error
        for error in report.errors
    )
    assert [
        row.row_dict["entity_identifier_is_primary"]
        for row in db.rows_by_table["entity_identifiers"]
        if row.row_dict["entity_identifier_value"] == "OL123W"
    ] == [0]


def test_metadata_writer_accepts_valid_unicode_and_rejects_unsafe_text() -> None:
    """
    Verify metadata writer accepts valid unicode and rejects unsafe text.

    Example:
        Exercise test metadata writer accepts valid unicode and rejects unsafe text through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()
    metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    valid_tag = "unicode-\u4e66\u7c4d-\u30bf\u30b0-\U0001f4da"
    metadata.tags = [
        valid_tag,
        "bad\x00tag",
        "bad" + chr(0xD800) + "tag",
    ]

    report = metadata.write_to_database(db, fields=("tags",))

    assert [row["text"] for row in report.rows_added] == [valid_tag]
    assert len(report.errors) == 2
    assert all("skipped unsafe text value" in error for error in report.errors)
    assert [
        row.row_dict["tag"]
        for row in db.rows_by_table["tags"]
        if row.row_dict["tag"] == valid_tag
    ] == [valid_tag]
    assert all("\x00" not in row.row_dict["tag"] for row in db.rows_by_table["tags"])

    db = _build_fake_database()
    metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    valid_identifier = "10.5555/unicode-\u4e66\u7c4d"
    metadata.set_identifiers(
        {
            "doi": [
                valid_identifier,
                "10.5555/bad\x00identifier",
            ],
        },
        update=False,
    )

    report = metadata.write_to_database(db, fields=("identifiers",), replace=True)

    assert valid_identifier in {row["value"] for row in report.rows_added}
    assert all(
        row["value"] != "10.5555/bad\x00identifier"
        for row in report.rows_added
    )
    assert any("skipped unsafe value" in error for error in report.errors)
    assert all(
        "\x00" not in row.row_dict["entity_identifier_value"]
        for row in db.rows_by_table["entity_identifiers"]
    )


def test_liuxin_wemi_metadata_wemi_relation_edits_round_trip_to_database() -> None:
    """
    Verify liuxin wemi metadata wemi relation edits round trip to database.

    Example:
        Exercise test liuxin wemi metadata wemi relation edits round trip to database through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify liuxin wemi metadata sidecar without legacy round trips wemi relations.

    Example:
        Exercise test liuxin wemi metadata sidecar without legacy round trips wemi relations through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify wemi metadata bundles write supported relation terms.

    Example:
        Exercise test wemi metadata bundles write supported relation terms through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify wemi bundle write skips context relations from other levels.

    Example:
        Exercise test wemi bundle write skips context relations from other levels through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify calibre like metadata write to database resolves target from item id.

    Example:
        Exercise test calibre like metadata write to database resolves target from item id through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify calibre metadata view round trips tags to database.

    Example:
        Exercise test calibre metadata view round trips tags to database through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify liuxin wemi metadata hydrator dispatches typed shapes.

    Example:
        Exercise test liuxin wemi metadata hydrator dispatches typed shapes through its owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


    :return: None; the function records state or raises through its assertions.
    """
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
