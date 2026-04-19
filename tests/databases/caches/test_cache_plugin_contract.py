from __future__ import annotations

import unicodedata

import pytest

from LiuXin_alpha.caches.api.storage_cache_api.storage_view_api import CacheViewSpec
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_view import SchemaBackedCacheView
from LiuXin_alpha.databases.schema_specs import (
    LinkCardinality,
    StorageLinkSpec,
    StorageSchemaSpec,
)
from tests.support.storage_cache_test_harness import (
    CACHE_PLUGIN_KWARGS,
    FakeDB,
    create_loaded_test_cache,
    make_fake_db,
    make_table,
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
_LIVE_COVER_PATH = "/covers/live-one.jpg"

@pytest.fixture(params=tuple(CACHE_PLUGIN_KWARGS), ids=tuple(CACHE_PLUGIN_KWARGS))
def cache_plugin_name(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.fixture()
def unicode_contract_db() -> FakeDB:
    books = make_table(
        "books",
        ("id", "title", "shared_code"),
        is_main_table=True,
        linked_tables=("covers", "tags"),
    )
    covers = make_table(
        "covers",
        ("id", "path", "shared_code"),
        is_main_table=True,
        linked_tables=("books",),
    )
    tags = make_table(
        "tags",
        ("id", "tag_name"),
        is_main_table=True,
        linked_tables=("books",),
    )
    book_covers = make_table(
        "book_covers",
        ("id", "book_id", "cover_id"),
        is_link_table=True,
        linked_tables=("books", "covers"),
    )
    book_tags = make_table(
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

    return make_fake_db(
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
def contract_cache(cache_plugin_name: str, unicode_contract_db: FakeDB):
    return create_loaded_test_cache(unicode_contract_db, cache_plugin_name)


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


def test_cache_plugin_view_reads_through_cache_value_helpers(contract_cache) -> None:
    view = SchemaBackedCacheView(
        contract_cache,
        CacheViewSpec(name="books", base_table="books"),
    )

    assert view.value_for(1, "title") == _BOOK_TITLE_NFD
    assert view.row_values_for_id(1) == (1, _BOOK_TITLE_NFD, "A-α")


def test_cache_plugin_fresh_reads_follow_declared_live_read_capability(
    contract_cache,
    unicode_contract_db: FakeDB,
) -> None:
    cache = contract_cache

    assert cache.get_cached_value(1, "title") == _BOOK_TITLE_NFD
    assert cache.get_main_table("books").has_id(3) is False

    unicode_contract_db.driver_wrapper.update_column("books", 1, "title", _UPDATED_TITLE)
    unicode_contract_db.driver_wrapper.add_row(
        {"title": _NEW_BOOK_TITLE, "shared_code": "A-γ"}
    )

    if cache.capabilities.live_reads:
        assert cache.get_cached_value(1, "title") == _UPDATED_TITLE
        assert cache.get_main_table("books").has_id(3) is True
        assert cache.get_main_table("books").get_row_snapshot(3)["title"] == _NEW_BOOK_TITLE
    else:
        assert cache.get_cached_value(1, "title") == _BOOK_TITLE_NFD
        assert cache.get_main_table("books").has_id(3) is False

        cache.reload()

        assert cache.get_cached_value(1, "title") == _UPDATED_TITLE
        assert cache.get_main_table("books").has_id(3) is True
        assert cache.get_main_table("books").get_row_snapshot(3)["title"] == _NEW_BOOK_TITLE


def test_cache_plugin_held_objects_follow_declared_live_child_capability(
    contract_cache,
    unicode_contract_db: FakeDB,
) -> None:
    cache = contract_cache
    books_table = cache.get_main_table("books")
    cover_path_field = cache.get_field("books.covers.path")
    view = SchemaBackedCacheView(
        cache,
        CacheViewSpec(name="books", base_table="books"),
    )

    assert books_table.has_id(3) is False
    assert cover_path_field.get_value_from_src_id(1) == _COVER_PATH_1
    assert view.value_for(1, "title") == _BOOK_TITLE_NFD

    unicode_contract_db.driver_wrapper.update_column("books", 1, "title", _UPDATED_TITLE)
    unicode_contract_db.driver_wrapper.update_column("covers", 10, "path", _LIVE_COVER_PATH)
    unicode_contract_db.driver_wrapper.add_row(
        {"title": _NEW_BOOK_TITLE, "shared_code": "A-γ"}
    )

    if cache.capabilities.live_child_objects:
        assert books_table.has_id(3) is True
        assert books_table.get_row_snapshot(3)["title"] == _NEW_BOOK_TITLE
        assert cover_path_field.get_value_from_src_id(1) == _LIVE_COVER_PATH
        assert view.value_for(1, "title") == _UPDATED_TITLE
        assert view.row_values_for_id(1) == (1, _UPDATED_TITLE, "A-α")
    else:
        assert books_table.has_id(3) is False
        assert cover_path_field.get_value_from_src_id(1) == _COVER_PATH_1
        assert view.value_for(1, "title") == _BOOK_TITLE_NFD
        assert view.row_values_for_id(1) == (1, _BOOK_TITLE_NFD, "A-α")


def test_cache_plugin_vectorized_helper_surface_follows_declared_capabilities(
    contract_cache,
) -> None:
    cache = contract_cache

    if cache.capabilities.vectorized_helpers:
        assert tuple(int(row_id) for row_id in cache.get_numpy_row_id_array("books")) == (1, 2)
        assert tuple(int(row_id) for row_id in cache.get_numpy_field_owner_ids("title")) == (1, 2)
        assert tuple(str(value) for value in cache.get_numpy_field_array("title")) == (
            _BOOK_TITLE_NFD,
            _BOOK_TITLE_NFC,
        )
    else:
        assert cache.capabilities.vectorized_helpers is False


def test_cache_plugin_reload_observes_external_unicode_changes(
    contract_cache,
    unicode_contract_db: FakeDB,
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
    unicode_contract_db: FakeDB,
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
