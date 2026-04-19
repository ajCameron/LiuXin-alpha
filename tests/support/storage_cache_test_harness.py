"""Shared fake schema/database helpers for storage-cache tests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from LiuXin_alpha.caches.cache_plugins.registry import create_storage_cache
from LiuXin_alpha.databases.schema_specs import (
    RelationKind,
    StorageColumnSpec,
    StorageSchemaSpec,
    StorageTableSpec,
)


CACHE_PLUGIN_KWARGS: dict[str, dict[str, Any]] = {
    "schema_backed": {},
    "database_backed": {},
    "numpy_vectorized": {"require_numpy": False},
}


@dataclass
class FakeResultRow:
    row_dict: dict[str, Any]


class FakeDriverWrapper:
    def __init__(
        self,
        schema: StorageSchemaSpec,
        rows_by_table: dict[str, list[dict[str, Any]]],
    ) -> None:
        self._schema = schema
        self._rows_by_table = rows_by_table

    def get_schema_spec(self, force_refresh: bool = False) -> StorageSchemaSpec:
        del force_refresh
        return self._schema

    def get_table_spec(self, table: str) -> StorageTableSpec:
        return self._schema.tables[table]

    def get_id_column(self, table: str) -> str:
        id_column = self._schema.tables[table].id_column
        if id_column is None:
            raise KeyError(table)
        return id_column

    def get_allowed_tables_snapshot(self) -> set[str]:
        return set(self._schema.tables)

    def identify_table_from_row_dict(self, row_dict: dict[str, Any]) -> str:
        keys = set(row_dict)
        matches = [
            table_name
            for table_name, spec in self._schema.tables.items()
            if keys.issubset({column.name for column in spec.columns})
        ]
        if not matches:
            raise KeyError(sorted(keys))
        matches.sort(key=lambda table_name: len(self._schema.tables[table_name].columns))
        return matches[0]

    def check_for_intralink_table(self, table: str) -> bool:
        return any(
            link.link_table == table and link.primary_table == link.secondary_table
            for link in self._schema.intralinks
        )

    def get_interlinked_tables(self, table: str) -> list[str]:
        spec = self._schema.tables.get(table)
        if spec is None:
            return []
        return list(spec.linked_tables)

    def add_row(self, row_dict: dict[str, Any]) -> int:
        table = self.identify_table_from_row_dict(row_dict)
        id_column = self.get_id_column(table)
        next_id = max((int(row[id_column]) for row in self._rows_by_table[table]), default=0) + 1
        payload = dict(row_dict)
        payload.setdefault(id_column, next_id)
        self._rows_by_table[table].append(payload)
        return int(payload[id_column])

    def get_blank_row(self, table: str) -> dict[str, Any]:
        spec = self.get_table_spec(table)
        id_column = self.get_id_column(table)
        next_id = max((int(row[id_column]) for row in self._rows_by_table[table]), default=0) + 1
        payload = {column.name: None for column in spec.columns}
        payload[id_column] = next_id
        self._rows_by_table[table].append(dict(payload))
        return payload

    def update_row(self, row_dict: dict[str, Any]) -> None:
        table = self.identify_table_from_row_dict(row_dict)
        id_column = self.get_id_column(table)
        row_id = int(row_dict[id_column])
        for index, row in enumerate(self._rows_by_table[table]):
            if int(row[id_column]) == row_id:
                merged = dict(row)
                merged.update(row_dict)
                self._rows_by_table[table][index] = merged
                return
        raise KeyError((table, row_id))

    def update_column(self, table: str, row_id: int, column: str, value: Any) -> None:
        id_column = self.get_id_column(table)
        for row in self._rows_by_table[table]:
            if int(row[id_column]) == int(row_id):
                row[column] = value
                return
        raise KeyError((table, row_id))

    def delete_by_id(self, table: str, ids: set[int]) -> None:
        id_column = self.get_id_column(table)
        deleted = {int(row_id) for row_id in ids}
        self._rows_by_table[table] = [
            row for row in self._rows_by_table[table] if int(row[id_column]) not in deleted
        ]


class FakeDB:
    def __init__(
        self,
        schema: StorageSchemaSpec,
        rows_by_table: dict[str, list[dict[str, Any]]],
    ) -> None:
        self.driver_wrapper = FakeDriverWrapper(schema, rows_by_table)
        self._rows_by_table = rows_by_table
        self.conn = None

    def get_all_rows(self, table: str, iterator_return: bool = False):
        del iterator_return
        return [FakeResultRow(dict(row)) for row in self._rows_by_table[table]]

    def get_row_from_id(self, table: str, row_id: int):
        id_column = self.driver_wrapper.get_id_column(table)
        for row in self._rows_by_table[table]:
            if int(row[id_column]) == int(row_id):
                return FakeResultRow(dict(row))
        return None

    def get_column_headings(self, table: str) -> list[str]:
        return [column.name for column in self.driver_wrapper.get_table_spec(table).columns]


def make_column(name: str, ordinal: int, *, declared_type: str = "TEXT") -> StorageColumnSpec:
    return StorageColumnSpec(
        name=name,
        ordinal=ordinal,
        declared_type=declared_type,
        affinity=declared_type,
        is_primary_key=name == "id",
    )


def make_table(
    name: str,
    column_names: tuple[str, ...],
    *,
    is_main_table: bool = False,
    is_link_table: bool = False,
    linked_tables: tuple[str, ...] = (),
) -> StorageTableSpec:
    return StorageTableSpec(
        name=name,
        relation_kind=RelationKind.TABLE,
        columns=tuple(
            make_column(
                column_name,
                index,
                declared_type="INTEGER"
                if column_name.endswith("_id") or column_name == "id"
                else "TEXT",
            )
            for index, column_name in enumerate(column_names)
        ),
        id_column="id",
        is_main_table=is_main_table,
        is_link_table=is_link_table,
        linked_tables=linked_tables,
    )


def make_fake_db(
    schema: StorageSchemaSpec,
    rows_by_table: dict[str, list[dict[str, Any]]],
) -> FakeDB:
    return FakeDB(schema=schema, rows_by_table=rows_by_table)


def create_loaded_test_cache(db: Any, cache_type: str, **kwargs: Any):
    resolved_kwargs = dict(CACHE_PLUGIN_KWARGS.get(str(cache_type), {}))
    resolved_kwargs.update(kwargs)
    cache = create_storage_cache(db, cache_type, **resolved_kwargs)
    cache.read()
    return cache


__all__ = [
    "CACHE_PLUGIN_KWARGS",
    "FakeDB",
    "FakeDriverWrapper",
    "FakeResultRow",
    "create_loaded_test_cache",
    "make_column",
    "make_fake_db",
    "make_table",
]
