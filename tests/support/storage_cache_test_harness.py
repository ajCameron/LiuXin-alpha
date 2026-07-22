"""Shared fake schema/database helpers for storage-cache tests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from LiuXin_alpha.caches.cache_plugins.registry import create_storage_cache
from LiuXin_alpha.databases.macro_types import (
    LINK_TYPE_UNSET,
    LinkRow,
    LinkValue,
)
from LiuXin_alpha.databases.schema_specs import (
    RelationKind,
    StorageColumnSpec,
    StorageLinkSpec,
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

    def get_link_spec(
        self,
        source_table: str,
        destination_table: str,
        *,
        force_refresh: bool = False,
    ) -> StorageLinkSpec | None:
        del force_refresh
        return next(
            (
                link_spec
                for link_spec in self._schema.interlinks + self._schema.intralinks
                if link_spec.primary_table == source_table
                and link_spec.secondary_table == destination_table
            ),
            None,
        )

    def get_allowed_link_types(
        self,
        link_spec: StorageLinkSpec,
        *,
        force_refresh: bool = False,
    ) -> tuple[str, ...] | None:
        del force_refresh
        table = link_spec.allowed_types_table
        if table is None:
            return None
        rows = self._rows_by_table[table]
        headings = tuple(
            column.name for column in self._schema.tables[table].columns
        )
        type_column = "type" if "type" in headings else next(
            heading for heading in headings if heading.endswith("_type")
        )
        return tuple(
            sorted(
                str(row[type_column])
                for row in rows
                if row.get(type_column) is not None
            )
        )

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


class FakeMacros:
    """Small in-memory macro layer for cache/catalog integration tests."""

    def __init__(self, db: "FakeDB") -> None:
        self.db = db

    def ensure_table_value(
        self,
        table: str,
        column: str,
        value: Any,
        *,
        id_column: str | None = None,
    ) -> int:
        id_column = id_column or self.db.driver_wrapper.get_id_column(table)
        for row in self.db._rows_by_table[table]:
            if row.get(column) == value:
                return int(row[id_column])

        next_id = max(
            (
                int(row[id_column])
                for row in self.db._rows_by_table[table]
            ),
            default=0,
        ) + 1
        row = {
            heading: None
            for heading in self.db.get_column_headings(table)
        }
        row[id_column] = next_id
        row[column] = value
        self.db._rows_by_table[table].append(row)
        return next_id

    def find_table_value(
        self,
        table: str,
        column: str,
        value: Any,
        *,
        id_column: str | None = None,
    ) -> int | None:
        id_column = id_column or self.db.driver_wrapper.get_id_column(table)
        return next(
            (
                int(row[id_column])
                for row in self.db._rows_by_table[table]
                if row.get(column) == value
            ),
            None,
        )

    @staticmethod
    def _type_matches(
        link_spec: StorageLinkSpec,
        row: dict[str, Any],
        link_type: Any,
    ) -> bool:
        if link_type is LINK_TYPE_UNSET:
            return True
        if link_spec.type_link_col is None:
            return False
        return row.get(link_spec.type_link_col) == link_type

    def _link_row(
        self,
        link_spec: StorageLinkSpec,
        row: dict[str, Any],
    ) -> LinkRow:
        return LinkRow(
            primary_id=row[link_spec.primary_link_col],
            secondary_id=row[link_spec.secondary_link_col],
            priority=(
                row.get(link_spec.priority_link_col)
                if link_spec.priority_link_col is not None
                else None
            ),
            link_type=(
                row.get(link_spec.type_link_col)
                if link_spec.type_link_col is not None
                else None
            ),
        )

    def get_link_rows_bulk(
        self,
        link_spec: StorageLinkSpec,
        primary_ids: Any,
        *,
        link_type: Any = LINK_TYPE_UNSET,
    ) -> dict[int, tuple[LinkRow, ...]]:
        requested = tuple(int(primary_id) for primary_id in primary_ids)
        grouped: dict[int, list[LinkRow]] = {
            primary_id: [] for primary_id in requested
        }
        for row in self.db._rows_by_table[link_spec.link_table]:
            primary_id = int(row[link_spec.primary_link_col])
            if (
                primary_id in grouped
                and self._type_matches(link_spec, row, link_type)
            ):
                grouped[primary_id].append(self._link_row(link_spec, row))
        return {
            primary_id: tuple(rows)
            for primary_id, rows in grouped.items()
        }

    def replace_links_bulk(
        self,
        link_spec: StorageLinkSpec,
        replacements: Any,
        *,
        link_type: Any = LINK_TYPE_UNSET,
    ) -> dict[int, tuple[LinkRow, ...]]:
        link_rows = self.db._rows_by_table[link_spec.link_table]
        result: dict[int, tuple[LinkRow, ...]] = {}
        for supplied_primary_id, supplied_links in replacements.items():
            primary_id = int(supplied_primary_id)
            link_rows[:] = [
                row
                for row in link_rows
                if not (
                    int(row[link_spec.primary_link_col]) == primary_id
                    and self._type_matches(link_spec, row, link_type)
                )
            ]

            written: list[LinkRow] = []
            for supplied_link in supplied_links:
                if not isinstance(supplied_link, LinkValue):
                    raise TypeError("fake link writes require LinkValue instances")
                row = {
                    heading: None
                    for heading in self.db.get_column_headings(
                        link_spec.link_table
                    )
                }
                id_column = self.db.driver_wrapper.get_id_column(
                    link_spec.link_table
                )
                row[id_column] = max(
                    (int(item[id_column]) for item in link_rows),
                    default=0,
                ) + 1
                row[link_spec.primary_link_col] = primary_id
                row[link_spec.secondary_link_col] = supplied_link.secondary_id
                if link_spec.priority_link_col is not None:
                    row[link_spec.priority_link_col] = supplied_link.priority
                if link_spec.type_link_col is not None:
                    row[link_spec.type_link_col] = supplied_link.link_type
                row.update(supplied_link.extra)
                link_rows.append(row)
                written.append(self._link_row(link_spec, row))
            result[primary_id] = tuple(written)
        return result

    def replace_owned_one_to_one_values_bulk(
        self,
        link_spec: StorageLinkSpec,
        value_column: str,
        replacements: Any,
    ) -> dict[int, tuple[LinkRow, ...]]:
        link_rows = self.db._rows_by_table[link_spec.link_table]
        result: dict[int, tuple[LinkRow, ...]] = {}
        for supplied_primary_id, value in replacements.items():
            primary_id = int(supplied_primary_id)
            current = next(
                (
                    row
                    for row in link_rows
                    if int(row[link_spec.primary_link_col]) == primary_id
                ),
                None,
            )
            if value is None:
                link_rows[:] = [
                    row
                    for row in link_rows
                    if int(row[link_spec.primary_link_col]) != primary_id
                ]
                result[primary_id] = ()
                continue

            if current is None:
                destination_rows = self.db._rows_by_table[
                    link_spec.secondary_table
                ]
                destination_id = max(
                    (
                        int(row[link_spec.secondary_id_col])
                        for row in destination_rows
                    ),
                    default=0,
                ) + 1
                destination_row = {
                    heading: None
                    for heading in self.db.get_column_headings(
                        link_spec.secondary_table
                    )
                }
                destination_row[link_spec.secondary_id_col] = destination_id
                destination_row[value_column] = value
                destination_rows.append(destination_row)
                result.update(
                    self.replace_links_bulk(
                        link_spec,
                        {primary_id: (LinkValue(destination_id),)},
                    )
                )
                continue

            destination_id = int(current[link_spec.secondary_link_col])
            self.db.driver_wrapper.update_column(
                link_spec.secondary_table,
                destination_id,
                value_column,
                value,
            )
            result[primary_id] = (self._link_row(link_spec, current),)
        return result


class FakeDB:
    def __init__(
        self,
        schema: StorageSchemaSpec,
        rows_by_table: dict[str, list[dict[str, Any]]],
    ) -> None:
        self.driver_wrapper = FakeDriverWrapper(schema, rows_by_table)
        self._rows_by_table = rows_by_table
        self.macros = FakeMacros(self)
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

    def update_columns(
        self,
        values_map: dict[int, Any],
        field: str,
        table: str,
    ) -> None:
        for row_id, value in values_map.items():
            self.driver_wrapper.update_column(table, int(row_id), field, value)


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
