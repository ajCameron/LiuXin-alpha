from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from LiuXin_alpha.caches import (
    DatabaseBackedStorageCache,
    NumpyVectorizedStorageCache,
    SchemaBackedStorageCache,
    StorageCache,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_view_api import CacheViewSpec
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_view import SchemaBackedCacheView
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.many_many_field import (
    LinkDstUpdate as ManyManyLinkDstUpdate,
    ManyManyInTwoTableFieldUpdate,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.many_one_field import (
    ManyOneInTwoTableFieldUpdate,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.one_many_field import (
    LinkDstUpdate as OneManyLinkDstUpdate,
    OneManyInTwoTableFieldUpdate,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.one_one_field import (
    OneOneInTwoTableFieldUpdate,
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
def _schema_backed_cache_db() -> _FakeDB:
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
    return db


@pytest.fixture()
def schema_backed_cache(_schema_backed_cache_db: _FakeDB) -> SchemaBackedStorageCache:
    cache = SchemaBackedStorageCache(_schema_backed_cache_db)
    cache.read()
    return cache


@pytest.fixture()
def numpy_vectorized_cache(_schema_backed_cache_db: _FakeDB) -> NumpyVectorizedStorageCache:
    cache = NumpyVectorizedStorageCache(_schema_backed_cache_db, require_numpy=False)
    cache.read()
    return cache


@pytest.fixture()
def database_backed_cache(_schema_backed_cache_db: _FakeDB) -> DatabaseBackedStorageCache:
    cache = DatabaseBackedStorageCache(_schema_backed_cache_db)
    cache.read()
    return cache


@pytest.fixture()
def many_one_schema_backed_cache() -> SchemaBackedStorageCache:
    books = _table(
        "books",
        ("id", "title"),
        is_main_table=True,
        linked_tables=("publishers",),
    )
    publishers = _table(
        "publishers",
        ("id", "publisher_name"),
        is_main_table=True,
        linked_tables=("books",),
    )
    book_publishers = _table(
        "book_publishers",
        ("id", "book_id", "publisher_id"),
        is_link_table=True,
        linked_tables=("books", "publishers"),
    )

    schema = StorageSchemaSpec(
        tables={
            "books": books,
            "publishers": publishers,
            "book_publishers": book_publishers,
        },
        interlinks=(
            StorageLinkSpec(
                primary_table="books",
                secondary_table="publishers",
                link_table="book_publishers",
                cardinality=LinkCardinality.MANY_TO_ONE,
                primary_link_col="book_id",
                secondary_link_col="publisher_id",
            ),
        ),
        intralinks=(),
    )

    db = _FakeDB(
        schema=schema,
        rows_by_table={
            "books": [
                {"id": 1, "title": "Book One"},
                {"id": 2, "title": "Book Two"},
                {"id": 3, "title": "Book Three"},
            ],
            "publishers": [
                {"id": 20, "publisher_name": "Tor Books"},
            ],
            "book_publishers": [
                {"id": 200, "book_id": 1, "publisher_id": 20},
                {"id": 201, "book_id": 2, "publisher_id": 20},
            ],
        },
    )

    cache = SchemaBackedStorageCache(db)
    cache.read()
    return cache


@pytest.fixture()
def one_many_schema_backed_cache() -> SchemaBackedStorageCache:
    books = _table(
        "books",
        ("id", "title"),
        is_main_table=True,
        linked_tables=("notes",),
    )
    notes = _table(
        "notes",
        ("id", "note_text"),
        is_main_table=True,
        linked_tables=("books",),
    )
    book_notes = _table(
        "book_notes",
        ("id", "book_id", "note_id", "note_priority", "note_type"),
        is_link_table=True,
        linked_tables=("books", "notes"),
    )

    schema = StorageSchemaSpec(
        tables={
            "books": books,
            "notes": notes,
            "book_notes": book_notes,
        },
        interlinks=(
            StorageLinkSpec(
                primary_table="books",
                secondary_table="notes",
                link_table="book_notes",
                cardinality=LinkCardinality.ONE_TO_MANY,
                primary_link_col="book_id",
                secondary_link_col="note_id",
                priority_link_col="note_priority",
                type_link_col="note_type",
                ordered=True,
                typed=True,
            ),
        ),
        intralinks=(),
    )

    db = _FakeDB(
        schema=schema,
        rows_by_table={
            "books": [
                {"id": 1, "title": "Book One"},
                {"id": 2, "title": "Book Two"},
            ],
            "notes": [
                {"id": 30, "note_text": "Existing note"},
                {"id": 31, "note_text": "Other owner's note"},
            ],
            "book_notes": [
                {"id": 300, "book_id": 1, "note_id": 30, "note_priority": 2, "note_type": "main"},
                {"id": 301, "book_id": 2, "note_id": 31, "note_priority": 1, "note_type": "main"},
            ],
        },
    )

    cache = SchemaBackedStorageCache(db)
    cache.read()
    return cache


@pytest.fixture()
def many_many_schema_backed_cache() -> SchemaBackedStorageCache:
    books = _table(
        "books",
        ("id", "title"),
        is_main_table=True,
        linked_tables=("tags",),
    )
    tags = _table(
        "tags",
        ("id", "tag_name"),
        is_main_table=True,
        linked_tables=("books",),
    )
    book_tags = _table(
        "book_tags",
        ("id", "book_id", "tag_id", "tag_priority"),
        is_link_table=True,
        linked_tables=("books", "tags"),
    )

    schema = StorageSchemaSpec(
        tables={
            "books": books,
            "tags": tags,
            "book_tags": book_tags,
        },
        interlinks=(
            StorageLinkSpec(
                primary_table="books",
                secondary_table="tags",
                link_table="book_tags",
                cardinality=LinkCardinality.MANY_TO_MANY,
                primary_link_col="book_id",
                secondary_link_col="tag_id",
                priority_link_col="tag_priority",
                ordered=True,
            ),
        ),
        intralinks=(),
    )

    db = _FakeDB(
        schema=schema,
        rows_by_table={
            "books": [
                {"id": 1, "title": "Book One"},
                {"id": 2, "title": "Book Two"},
            ],
            "tags": [
                {"id": 40, "tag_name": "Science Fiction"},
                {"id": 41, "tag_name": "Classic"},
            ],
            "book_tags": [
                {"id": 400, "book_id": 1, "tag_id": 40, "tag_priority": 2},
                {"id": 401, "book_id": 2, "tag_id": 40, "tag_priority": 2},
                {"id": 402, "book_id": 2, "tag_id": 41, "tag_priority": 1},
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
        "books.covers.path",
        "books.covers.shared_code",
        "books.id",
        "books.shared_code",
        "books.title",
    }
    assert {field.field_key for field in cache.iter_fields()} == {
        "books.covers.path",
        "books.covers.shared_code",
        "books.id",
        "books.shared_code",
        "books.title",
        "covers.id",
        "covers.path",
        "covers.shared_code",
        "covers.books.shared_code",
        "covers.books.title",
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


def test_one_to_one_relation_fields_are_discovered_and_readable(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = schema_backed_cache
    field = cache.get_field("books.covers.path")

    assert cache.has_field("books.covers.path")
    assert cache.has_field("covers.path") is True
    assert cache.has_field("path") is True

    assert field.get_value_from_src_id(1) == "/covers/one.jpg"
    assert field.get_value_from_src_id(2) == "/covers/two.jpg"
    assert field.get_dst_id_from_src_id(1) == 10
    assert field.get_src_id_from_dst_id(11) == 2
    assert field.dst_ids_values_map == {
        10: "/covers/one.jpg",
        11: "/covers/two.jpg",
    }


def test_one_to_one_relation_field_deleted_ids_unlink_without_deleting_dst_rows(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = schema_backed_cache
    field = cache.get_field("books.covers.path")

    field.update(
        OneOneInTwoTableFieldUpdate(
            src_table="books",
            dst_table="covers",
            dst_table_target_column="path",
            added_maps={},
            updated_maps={},
            deleted_ids={1},
            dirtied=set(),
        )
    )

    assert cache.get_one_one_link_table("books", "covers").get_dst_id(1) is None
    assert cache.db.get_row_from_id("covers", 10).row_dict["path"] == "/covers/one.jpg"


def test_one_to_one_relation_field_updates_existing_linked_values(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = schema_backed_cache
    field = cache.get_field("books.covers.path")

    field.update(
        OneOneInTwoTableFieldUpdate(
            src_table="books",
            dst_table="covers",
            dst_table_target_column="path",
            added_maps={},
            updated_maps={1: "/covers/one-updated.jpg"},
            deleted_ids=set(),
            dirtied=set(),
        )
    )

    assert field.get_value_from_src_id(1) == "/covers/one-updated.jpg"
    assert cache.get_main_table("covers").get_row_snapshot(10)["path"] == "/covers/one-updated.jpg"
    assert cache.db.get_row_from_id("covers", 10).row_dict["path"] == "/covers/one-updated.jpg"


def test_one_to_one_relation_field_can_recreate_missing_link_from_existing_value(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = schema_backed_cache
    field = cache.get_field("books.covers.path")

    field.update(
        OneOneInTwoTableFieldUpdate(
            src_table="books",
            dst_table="covers",
            dst_table_target_column="path",
            added_maps={},
            updated_maps={},
            deleted_ids={1},
            dirtied=set(),
        )
    )

    field.update(
        OneOneInTwoTableFieldUpdate(
            src_table="books",
            dst_table="covers",
            dst_table_target_column="path",
            added_maps={1: "/covers/one.jpg"},
            updated_maps={},
            deleted_ids=set(),
            dirtied=set(),
            create_missing_links=True,
        )
    )

    assert field.get_dst_id_from_src_id(1) == 10
    assert cache.get_one_one_link_table("books", "covers").get_dst_id(1) == 10


def test_one_to_one_relation_field_can_create_missing_related_row_and_link(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = schema_backed_cache
    field = cache.get_field("books.covers.path")

    field.update(
        OneOneInTwoTableFieldUpdate(
            src_table="books",
            dst_table="covers",
            dst_table_target_column="path",
            added_maps={},
            updated_maps={},
            deleted_ids={1},
            dirtied=set(),
        )
    )

    field.update(
        OneOneInTwoTableFieldUpdate(
            src_table="books",
            dst_table="covers",
            dst_table_target_column="path",
            added_maps={1: "/covers/one-fresh.jpg"},
            updated_maps={},
            deleted_ids=set(),
            dirtied=set(),
            create_missing_links=True,
            create_missing_related_rows=True,
        )
    )

    assert field.get_value_from_src_id(1) == "/covers/one-fresh.jpg"
    new_dst_id = field.get_dst_id_from_src_id(1)
    assert new_dst_id is not None
    assert new_dst_id != 10
    assert cache.db.get_row_from_id("covers", new_dst_id).row_dict["path"] == "/covers/one-fresh.jpg"


def test_one_to_one_relation_field_refuses_to_reassign_existing_linked_dst_row(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = schema_backed_cache
    field = cache.get_field("books.covers.path")

    field.update(
        OneOneInTwoTableFieldUpdate(
            src_table="books",
            dst_table="covers",
            dst_table_target_column="path",
            added_maps={},
            updated_maps={},
            deleted_ids={1},
            dirtied=set(),
        )
    )

    with pytest.raises(ValueError):
        field.update(
            OneOneInTwoTableFieldUpdate(
                src_table="books",
                dst_table="covers",
                dst_table_target_column="path",
                added_maps={1: "/covers/two.jpg"},
                updated_maps={},
                deleted_ids=set(),
                dirtied=set(),
                create_missing_links=True,
            )
        )


def test_relation_field_rejects_creating_related_rows_without_creating_links(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    field = schema_backed_cache.get_field("books.covers.path")

    with pytest.raises(ValueError):
        field.update(
            OneOneInTwoTableFieldUpdate(
                src_table="books",
                dst_table="covers",
                dst_table_target_column="path",
                added_maps={1: "/covers/one-fresh.jpg"},
                updated_maps={},
                deleted_ids=set(),
                dirtied=set(),
                create_missing_related_rows=True,
            )
        )


def test_many_one_relation_field_can_create_missing_link_from_existing_value(
    many_one_schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = many_one_schema_backed_cache
    field = cache.get_field("books.publishers.publisher_name")

    field.update(
        ManyOneInTwoTableFieldUpdate(
            src_table="books",
            dst_table="publishers",
            dst_table_target_column="publisher_name",
            added_maps={3: "Tor Books"},
            updated_maps={},
            deleted_ids=set(),
            dirtied=set(),
            create_missing_links=True,
        )
    )

    assert field.get_dst_id_from_src_id(3) == 20
    assert field.get_value_from_src_id(3) == "Tor Books"


def test_many_one_relation_field_can_create_missing_related_row_and_link(
    many_one_schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = many_one_schema_backed_cache
    field = cache.get_field("books.publishers.publisher_name")

    field.update(
        ManyOneInTwoTableFieldUpdate(
            src_table="books",
            dst_table="publishers",
            dst_table_target_column="publisher_name",
            added_maps={3: "Orbit Books"},
            updated_maps={},
            deleted_ids=set(),
            dirtied=set(),
            create_missing_links=True,
            create_missing_related_rows=True,
        )
    )

    new_dst_id = field.get_dst_id_from_src_id(3)
    assert new_dst_id is not None
    assert new_dst_id != 20
    assert field.get_value_from_src_id(3) == "Orbit Books"
    assert cache.db.get_row_from_id("publishers", new_dst_id).row_dict["publisher_name"] == "Orbit Books"


def test_one_many_relation_field_explicit_link_replacements_can_create_and_order_rows(
    one_many_schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = one_many_schema_backed_cache
    field = cache.get_field("books.notes.note_text")

    field.update(
        OneManyInTwoTableFieldUpdate(
            src_table="books",
            dst_table="notes",
            dst_table_target_column="note_text",
            added_maps={},
            updated_maps={},
            deleted_ids=set(),
            dirtied=set(),
            link_replacements={
                1: [
                    OneManyLinkDstUpdate(
                        dst_table="notes",
                        dst_table_target_column="note_text",
                        dst_col_val="Existing note",
                        type="intro",
                    ),
                    OneManyLinkDstUpdate(
                        dst_table="notes",
                        dst_table_target_column="note_text",
                        dst_col_val="Fresh note",
                        type="sidebar",
                    ),
                ]
            },
        )
    )

    assert list(field.get_values_from_src_id(1, require_ordering=True)) == [
        "Existing note",
        "Fresh note",
    ]
    dst_ids = list(field.get_dst_ids_from_src_id(1, require_ordering=True))
    assert dst_ids[0] == 30
    assert field.get_link_properties(1, dst_ids[0]).type == "intro"
    assert field.get_link_properties(1, dst_ids[1]).type == "sidebar"
    assert cache.db.get_row_from_id("notes", dst_ids[1]).row_dict["note_text"] == "Fresh note"


def test_one_many_relation_field_explicit_replacement_wont_steal_other_src_dst_rows(
    one_many_schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = one_many_schema_backed_cache
    field = cache.get_field("books.notes.note_text")

    field.update(
        OneManyInTwoTableFieldUpdate(
            src_table="books",
            dst_table="notes",
            dst_table_target_column="note_text",
            added_maps={},
            updated_maps={},
            deleted_ids=set(),
            dirtied=set(),
            link_replacements={
                1: [
                    OneManyLinkDstUpdate(
                        dst_table="notes",
                        dst_table_target_column="note_text",
                        dst_col_val="Other owner's note",
                    ),
                ]
            },
        )
    )

    dst_id = field.get_dst_ids_from_src_id(1)[0]
    assert dst_id != 31
    assert cache.get_one_many_link_table("books", "notes").get_src_id(31) == 2
    assert cache.db.get_row_from_id("notes", dst_id).row_dict["note_text"] == "Other owner's note"


def test_many_many_relation_field_explicit_link_replacements_can_reuse_shared_dst_rows(
    many_many_schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = many_many_schema_backed_cache
    field = cache.get_field("books.tags.tag_name")

    field.update(
        ManyManyInTwoTableFieldUpdate(
            src_table="books",
            dst_table="tags",
            dst_table_target_column="tag_name",
            added_maps={},
            updated_maps={},
            deleted_ids=set(),
            dirtied=set(),
            link_replacements={
                1: [
                    ManyManyLinkDstUpdate(
                        dst_table="tags",
                        dst_table_target_column="tag_name",
                        dst_col_val="Science Fiction",
                    ),
                    ManyManyLinkDstUpdate(
                        dst_table="tags",
                        dst_table_target_column="tag_name",
                        dst_col_val="Space Opera",
                    ),
                ]
            },
        )
    )

    dst_ids = list(field.get_dst_ids_from_src_id(1, require_ordering=True))
    assert dst_ids[0] == 40
    assert field.get_values_from_src_id(1, require_ordering=True) == (
        "Science Fiction",
        "Space Opera",
    )
    assert cache.get_many_many_link_table("books", "tags").has_link(2, 40) is True
    assert cache.db.get_row_from_id("tags", dst_ids[1]).row_dict["tag_name"] == "Space Opera"


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


def test_schema_backed_cache_exposes_cached_value_helpers(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = schema_backed_cache

    assert cache.cache_type == "schema_backed"
    assert cache.get_cached_value(1, "title") == "Book One"
    assert cache.get_cached_value(999, "title", default_value="missing") == "missing"
    assert cache.get_cached_row_values(1, ("title", "books.shared_code")) == (
        "Book One",
        "A-1",
    )


def test_numpy_vectorized_cache_supports_cached_value_helpers(
    numpy_vectorized_cache: NumpyVectorizedStorageCache,
) -> None:
    cache = numpy_vectorized_cache

    assert cache.cache_type == "numpy_vectorized"
    assert cache.get_cached_value(1, "title") == "Book One"
    assert cache.get_cached_value(999, "title", default_value="missing") == "missing"
    assert cache.get_cached_row_values(1, ("title", "books.shared_code")) == (
        "Book One",
        "A-1",
    )


def test_cache_view_reads_through_cache_value_helpers(
    numpy_vectorized_cache: NumpyVectorizedStorageCache,
) -> None:
    view = SchemaBackedCacheView(
        numpy_vectorized_cache,
        CacheViewSpec(name="books", base_table="books"),
    )

    assert view.value_for(1, "title") == "Book One"
    assert view.row_values_for_id(1) == (1, "Book One", "A-1")


def test_database_backed_cache_reflects_external_db_changes_without_manual_invalidation(
    database_backed_cache: DatabaseBackedStorageCache,
) -> None:
    cache = database_backed_cache

    assert cache.cache_type == "database_backed"
    assert cache.get_cached_value(1, "title") == "Book One"

    cache.db.driver_wrapper.update_column("books", 1, "title", "Retitled")

    assert cache.get_cached_value(1, "title") == "Retitled"
    assert cache.get_main_table("books").get_row_snapshot(1)["title"] == "Retitled"


def test_database_backed_held_table_proxy_stays_live(
    database_backed_cache: DatabaseBackedStorageCache,
) -> None:
    cache = database_backed_cache
    books_table = cache.get_main_table("books")

    assert books_table.has_id(3) is False

    cache.db.driver_wrapper.add_row({"title": "Book Three", "shared_code": "A-3"})

    assert books_table.has_id(3) is True
    assert books_table.get_row_snapshot(3)["title"] == "Book Three"


def test_database_backed_held_field_proxy_stays_live(
    database_backed_cache: DatabaseBackedStorageCache,
) -> None:
    cache = database_backed_cache
    cover_path_field = cache.get_field("books.covers.path")

    assert cover_path_field.get_value_from_src_id(1) == "/covers/one.jpg"

    cache.db.driver_wrapper.update_column("covers", 10, "path", "/covers/live-one.jpg")

    assert cover_path_field.get_value_from_src_id(1) == "/covers/live-one.jpg"


def test_database_backed_view_uses_live_field_proxies(
    database_backed_cache: DatabaseBackedStorageCache,
) -> None:
    cache = database_backed_cache
    view = SchemaBackedCacheView(
        cache,
        CacheViewSpec(name="books", base_table="books"),
    )

    assert view.value_for(1, "title") == "Book One"

    cache.db.driver_wrapper.update_column("books", 1, "title", "View Retitled")

    assert view.value_for(1, "title") == "View Retitled"
    assert view.row_values_for_id(1) == (1, "View Retitled", "A-1")


def test_database_backed_cache_reflects_new_rows_without_explicit_reload(
    database_backed_cache: DatabaseBackedStorageCache,
) -> None:
    cache = database_backed_cache

    assert cache.get_main_table("books").has_id(3) is False
    cache.db.driver_wrapper.add_row({"title": "Book Three", "shared_code": "A-3"})

    books_table = cache.get_main_table("books")
    assert books_table.has_id(3) is True
    assert books_table.get_row_snapshot(3)["title"] == "Book Three"
