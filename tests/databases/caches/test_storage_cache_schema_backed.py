from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from LiuXin_alpha.caches import SchemaBackedStorageCache, StorageCache
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.one_one_field import (
    OneOneInOneTableFieldUpdate,
)
from LiuXin_alpha.databases.schema_specs import (
    LinkCardinality,
    RelationKind,
    StorageColumnSpec,
    StorageLinkSpec,
    StorageSchemaSpec,
    StorageTableSpec,
)


@dataclass
class _FakeResultRow:
    row_dict: dict[str, Any]


class _FakeDriverWrapper:
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


class _FakeDB:
    def __init__(
        self,
        schema: StorageSchemaSpec,
        rows_by_table: dict[str, list[dict[str, Any]]],
    ) -> None:
        self.driver_wrapper = _FakeDriverWrapper(schema, rows_by_table)
        self._rows_by_table = rows_by_table
        self.conn = None

    def get_all_rows(self, table: str, iterator_return: bool = False):
        del iterator_return
        return [_FakeResultRow(dict(row)) for row in self._rows_by_table[table]]

    def get_row_from_id(self, table: str, row_id: int):
        id_column = self.driver_wrapper.get_id_column(table)
        for row in self._rows_by_table[table]:
            if int(row[id_column]) == int(row_id):
                return _FakeResultRow(dict(row))
        return None

    def get_column_headings(self, table: str) -> list[str]:
        return [column.name for column in self.driver_wrapper.get_table_spec(table).columns]


def _column(name: str, ordinal: int, *, declared_type: str = "TEXT") -> StorageColumnSpec:
    return StorageColumnSpec(
        name=name,
        ordinal=ordinal,
        declared_type=declared_type,
        affinity=declared_type,
        is_primary_key=name == "id",
    )


def _table(
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
        columns=tuple(_column(column_name, index, declared_type="INTEGER" if column_name.endswith("_id") or column_name == "id" else "TEXT") for index, column_name in enumerate(column_names)),
        id_column="id",
        is_main_table=is_main_table,
        is_link_table=is_link_table,
        linked_tables=linked_tables,
    )


@pytest.fixture()
def schema_backed_cache() -> SchemaBackedStorageCache:
    books = _table(
        "books",
        ("id", "title", "shared_code"),
        is_main_table=True,
        linked_tables=("covers",),
    )
    covers = _table(
        "covers",
        ("id", "path", "shared_code"),
        is_main_table=True,
        linked_tables=("books",),
    )
    book_covers = _table(
        "book_covers",
        ("id", "book_id", "cover_id"),
        is_link_table=True,
        linked_tables=("books", "covers"),
    )

    schema = StorageSchemaSpec(
        tables={
            "books": books,
            "covers": covers,
            "book_covers": book_covers,
        },
        interlinks=(
            StorageLinkSpec(
                primary_table="books",
                secondary_table="covers",
                link_table="book_covers",
                cardinality=LinkCardinality.ONE_TO_ONE,
                primary_link_col="book_id",
                secondary_link_col="cover_id",
            ),
        ),
        intralinks=(),
    )

    db = _FakeDB(
        schema=schema,
        rows_by_table={
            "books": [
                {"id": 1, "title": "Book One", "shared_code": "A-1"},
                {"id": 2, "title": "Book Two", "shared_code": "A-2"},
            ],
            "covers": [
                {"id": 10, "path": "/covers/one.jpg", "shared_code": "C-1"},
                {"id": 11, "path": "/covers/two.jpg", "shared_code": "C-2"},
            ],
            "book_covers": [
                {"id": 100, "book_id": 1, "cover_id": 10},
                {"id": 101, "book_id": 2, "cover_id": 11},
            ],
        },
    )

    cache = SchemaBackedStorageCache(db)
    cache.read()
    return cache


def test_package_root_exports_schema_backed_storage_cache() -> None:
    assert StorageCache is SchemaBackedStorageCache


def test_storage_cache_uses_canonical_field_keys_and_unique_aliases(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = schema_backed_cache

    assert cache.has_field("title")
    assert cache.get_field("title") is cache.get_field("books.title")

    assert cache.has_field("books.shared_code")
    assert cache.has_field("covers.shared_code")
    assert "shared_code" not in cache.fields
    assert cache.has_field("shared_code") is False

    with pytest.raises(KeyError):
        cache.get_field("shared_code")

    assert {field.field_key for field in cache.get_fields_for_table("books")} == {
        "books.id",
        "books.shared_code",
        "books.title",
    }
    assert {field.field_key for field in cache.iter_fields()} == {
        "books.id",
        "books.shared_code",
        "books.title",
        "covers.id",
        "covers.path",
        "covers.shared_code",
    }


def test_one_to_one_link_table_maps_are_exposed(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    link_table = schema_backed_cache.get_one_one_link_table("books", "covers")

    assert link_table.get_primary_id_secondary_value_id_map() == {1: 10, 2: 11}
    assert link_table.get_secondary_id_primary_id_map() == {10: 1, 11: 2}
    assert link_table.get_primary_id_secondary_value_map() == {
        1: "/covers/one.jpg",
        2: "/covers/two.jpg",
    }


def test_same_table_field_deleted_ids_nullify_column_without_deleting_rows(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = schema_backed_cache
    field = cache.get_field("title")

    field.update(
        OneOneInOneTableFieldUpdate(
            added_maps={},
            updated_maps={},
            deleted_ids={1},
            dirtied=set(),
        )
    )

    assert cache.get_main_table("books").has_id(1) is True
    assert cache.get_main_table("books").get_row_snapshot(1)["title"] is None
    assert field.get_value_from_id(1) is None
    assert cache.db.get_row_from_id("books", 1).row_dict["title"] is None


def test_same_table_field_can_refresh_and_remove_ids_after_external_changes(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = schema_backed_cache
    field = cache.get_field("title")

    cache.db.driver_wrapper.update_column("books", 2, "title", "Retitled")
    field.refresh_ids({2})
    assert field.get_value_from_id(2) == "Retitled"

    cache.db.driver_wrapper.delete_by_id("books", {2})
    field.remove_ids({2})
    assert 2 not in field.ids
    assert field.get_value_from_id(2) is None


def test_same_table_field_refuses_to_clear_primary_key_values(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = schema_backed_cache
    field = cache.get_field("books.id")

    with pytest.raises(ValueError):
        field.update(
            OneOneInOneTableFieldUpdate(
                added_maps={},
                updated_maps={},
                deleted_ids={1},
                dirtied=set(),
            )
        )
