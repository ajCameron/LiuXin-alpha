from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from typing import Any

import pytest

from LiuXin_alpha.caches.cache_plugins.registry import create_storage_cache
from LiuXin_alpha.databases.schema_specs import (
    LinkCardinality,
    RelationKind,
    StorageColumnSpec,
    StorageLinkSpec,
    StorageSchemaSpec,
    StorageTableSpec,
)


_BOOK_TITLE_NFD = 'Cafe\u0301 | 雪 | 👩‍💻 | line\nbreak | "quote"'
_BOOK_TITLE_NFC = 'Caf\u00e9 | 雪 | 👩‍💻 | line\nbreak | "quote"'
_COVER_PATH_1 = "/cøvers/📚-雪.jpg"
_COVER_PATH_2 = "/обложки/كتاب-🧪.png"
_TAG_1 = "naïve café"
_TAG_2 = "タグ🧪"
_TAG_3 = "مرحبا-世界"
_UPDATED_TITLE = "e\u0301xtra | 🌈 | \u2066RTL\u2069 | rewritten"
_NEW_BOOK_TITLE = "नई-पुस्तक 📖"
_UPDATED_TAG = "חדש-タグ-🧬"

_CACHE_PLUGIN_KWARGS: dict[str, dict[str, Any]] = {
    "schema_backed": {},
    "database_backed": {},
    "numpy_vectorized": {"require_numpy": False},
}


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

    def update_column(self, table: str, row_id: int, column: str, value: Any) -> None:
        id_column = self.get_id_column(table)
        for row in self._rows_by_table[table]:
            if int(row[id_column]) == int(row_id):
                row[column] = value
                return
        raise KeyError((table, row_id))


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
        columns=tuple(
            _column(
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


@pytest.fixture(params=tuple(_CACHE_PLUGIN_KWARGS), ids=tuple(_CACHE_PLUGIN_KWARGS))
def cache_plugin_name(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.fixture()
def unicode_contract_db() -> _FakeDB:
    books = _table(
        "books",
        ("id", "title", "shared_code"),
        is_main_table=True,
        linked_tables=("covers", "tags"),
    )
    covers = _table(
        "covers",
        ("id", "path", "shared_code"),
        is_main_table=True,
        linked_tables=("books",),
    )
    tags = _table(
        "tags",
        ("id", "tag_name"),
        is_main_table=True,
        linked_tables=("books",),
    )
    book_covers = _table(
        "book_covers",
        ("id", "book_id", "cover_id"),
        is_link_table=True,
        linked_tables=("books", "covers"),
    )
    book_tags = _table(
        "book_tags",
        ("id", "book_id", "tag_id"),
        is_link_table=True,
        linked_tables=("books", "tags"),
    )

    schema = StorageSchemaSpec(
        tables={
            "books": books,
            "covers": covers,
            "tags": tags,
            "book_covers": book_covers,
            "book_tags": book_tags,
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
            StorageLinkSpec(
                primary_table="books",
                secondary_table="tags",
                link_table="book_tags",
                cardinality=LinkCardinality.MANY_TO_MANY,
                primary_link_col="book_id",
                secondary_link_col="tag_id",
            ),
        ),
        intralinks=(),
    )

    return _FakeDB(
        schema=schema,
        rows_by_table={
            "books": [
                {"id": 1, "title": _BOOK_TITLE_NFD, "shared_code": "A-α"},
                {"id": 2, "title": _BOOK_TITLE_NFC, "shared_code": "A-β"},
            ],
            "covers": [
                {"id": 10, "path": _COVER_PATH_1, "shared_code": "C-一"},
                {"id": 11, "path": _COVER_PATH_2, "shared_code": "C-二"},
            ],
            "tags": [
                {"id": 40, "tag_name": _TAG_1},
                {"id": 41, "tag_name": _TAG_2},
                {"id": 42, "tag_name": _TAG_3},
            ],
            "book_covers": [
                {"id": 100, "book_id": 1, "cover_id": 10},
                {"id": 101, "book_id": 2, "cover_id": 11},
            ],
            "book_tags": [
                {"id": 200, "book_id": 1, "tag_id": 40},
                {"id": 201, "book_id": 1, "tag_id": 41},
                {"id": 202, "book_id": 2, "tag_id": 42},
            ],
        },
    )


@pytest.fixture()
def contract_cache(cache_plugin_name: str, unicode_contract_db: _FakeDB):
    cache = create_storage_cache(
        unicode_contract_db,
        cache_plugin_name,
        **_CACHE_PLUGIN_KWARGS[cache_plugin_name],
    )
    cache.read()
    return cache


def test_cache_plugin_unicode_contract_reads_scalar_and_relation_values(contract_cache) -> None:
    cache = contract_cache

    assert cache.get_cached_value(1, "title") == _BOOK_TITLE_NFD
    assert cache.get_main_table("books").get_row_snapshot(2)["title"] == _BOOK_TITLE_NFC

    title_field = cache.get_field("title")
    assert title_field.get_value_from_id(1) == _BOOK_TITLE_NFD
    assert title_field.get_value_from_id(2) == _BOOK_TITLE_NFC

    cover_field = cache.get_field("books.covers.path")
    assert cover_field.get_value_from_src_id(1) == _COVER_PATH_1
    assert cover_field.get_value_from_src_id(2) == _COVER_PATH_2

    tags_field = cache.get_field("books.tags.tag_name")
    assert tuple(tags_field.get_values_from_src_id(1, require_ordering=True)) == (
        _TAG_1,
        _TAG_2,
    )
    assert tuple(tags_field.get_values_from_src_id(2, require_ordering=True)) == (_TAG_3,)


def test_cache_plugin_preserves_distinct_unicode_normalization_forms(contract_cache) -> None:
    cache = contract_cache
    title_field = cache.get_field("title")

    assert _BOOK_TITLE_NFD != _BOOK_TITLE_NFC
    assert unicodedata.normalize("NFC", _BOOK_TITLE_NFD) == unicodedata.normalize(
        "NFC",
        _BOOK_TITLE_NFC,
    )

    assert title_field.get_ids_from_value(_BOOK_TITLE_NFD) == [1]
    assert title_field.get_ids_from_value(_BOOK_TITLE_NFC) == [2]


def test_cache_plugin_field_resolution_contract(contract_cache) -> None:
    cache = contract_cache

    assert cache.get_field("title") is cache.get_field("books.title")
    assert cache.get_field("path").field_key == "covers.path"
    assert cache.get_field("books.tags.tag_name").field_key == "books.tags.tag_name"

    assert cache.has_field("shared_code") is False
    with pytest.raises(KeyError):
        cache.get_field("shared_code")

    assert {field.field_key for field in cache.get_fields_for_table("books")} == {
        "books.covers.path",
        "books.covers.shared_code",
        "books.id",
        "books.shared_code",
        "books.tags.tag_name",
        "books.title",
    }


def test_cache_plugin_row_helpers_and_defaults(contract_cache) -> None:
    cache = contract_cache

    assert cache.get_cached_row_values(1, ("title", "books.shared_code")) == (
        _BOOK_TITLE_NFD,
        "A-α",
    )
    assert cache.get_cached_value(999, "title", default_value="missing") == "missing"
    assert cache.get_cached_row_values(
        999,
        ("title", "books.shared_code"),
        default_value="missing",
    ) == ("missing", "missing")


def test_cache_plugin_reload_observes_external_unicode_changes(
    contract_cache,
    unicode_contract_db: _FakeDB,
) -> None:
    cache = contract_cache

    unicode_contract_db.driver_wrapper.update_column("books", 1, "title", _UPDATED_TITLE)
    unicode_contract_db.driver_wrapper.update_column("tags", 42, "tag_name", _UPDATED_TAG)
    unicode_contract_db.driver_wrapper.add_row(
        {"title": _NEW_BOOK_TITLE, "shared_code": "A-γ"}
    )

    cache.reload()

    assert cache.get_cached_value(1, "title") == _UPDATED_TITLE
    assert tuple(
        cache.get_field("books.tags.tag_name").get_values_from_src_id(2, require_ordering=True)
    ) == (_UPDATED_TAG,)
    assert cache.get_main_table("books").get_row_snapshot(3)["title"] == _NEW_BOOK_TITLE


def test_cache_plugin_lifecycle_contract(
    contract_cache,
    unicode_contract_db: _FakeDB,
) -> None:
    cache = contract_cache

    assert cache.is_loaded is True
    assert cache.is_initialized is True

    detached_db = cache.detach_db()
    assert detached_db is unicode_contract_db
    assert cache.db is None

    cache.read(detached_db)
    assert cache.db is unicode_contract_db
    assert cache.is_loaded is True
    assert cache.is_initialized is True

    cache.clear()
    assert cache.is_loaded is False
    assert cache.is_initialized is False
    assert cache.main_tables == {}
    assert cache.link_tables == {}
    assert cache.fields == {}

    cache.read(unicode_contract_db)
    assert cache.is_loaded is True
    assert cache.is_initialized is True

    cache.close()
    assert cache.db is None
    assert cache.is_loaded is False
    assert cache.is_initialized is False
    assert cache.main_tables == {}
    assert cache.link_tables == {}
    assert cache.fields == {}

    with pytest.raises(RuntimeError):
        cache.read()
