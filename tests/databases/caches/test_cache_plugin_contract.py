"""
Check cache plugin read, write, Unicode, refresh, capability, and lifecycle contracts against in-memory doubles.

Pytest repeats the contracts for the registered test plugin configurations. Each
fixture builds fresh fake rows, allowing external mutations without a persistent
database.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py
"""
from __future__ import annotations

import unicodedata

import pytest

from LiuXin_alpha.catalog.write import (
    CatalogColumnWriter,
    CatalogOwnedRowOneToOneWriter,
    CatalogTableValueLinkWriter,
    LinkUpdate,
)
from LiuXin_alpha.caches import Cache
from LiuXin_alpha.databases.macro_types import LinkValue
from LiuXin_alpha.databases.schema_specs import (
    LinkCardinality,
    RelationKind,
    StorageColumnSpec,
    StorageLinkSpec,
    StorageSchemaSpec,
    StorageTableSpec,
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
    """
    Return the current cache plugin parameter as a string.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py


    :param request: Pytest fixture request carrying the current plugin parameter.
    :return: Plugin name for this parametrized fixture invocation.
    """
    return str(request.param)


@pytest.fixture()
def unicode_contract_db() -> FakeDB:
    """
    Build fresh books, covers, tags, and link rows containing distinct Unicode normalization forms.

    The schema provides owned one-to-one covers and many-to-many tags. Repeated
    shared_code column names exercise ambiguous field resolution.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py


    :return: New FakeDB with two books, two covers, three tags, and their links.
    """
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
                destination_owned=True,
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
    """
    Create and load the selected storage plugin against the Unicode fixture database.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py


    :param cache_plugin_name: Plugin name selected from CACHE_PLUGIN_KWARGS.
    :param unicode_contract_db: Fresh FakeDB with Unicode books, covers, tags, and
        relation rows; tests may mutate it.
    :return: Loaded storage cache created by the shared test harness.
    """
    return create_loaded_test_cache(unicode_contract_db, cache_plugin_name)


def test_cache_plugin_unicode_contract_reads_scalar_and_relation_values(contract_cache) -> None:
    """
    Check scalar titles, cover paths, and ordered tag values preserve the exact stored Unicode strings.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_plugin_unicode_contract_reads_scalar_and_relation_values


    :param contract_cache: Loaded storage cache for the current plugin, backed by
        unicode_contract_db.
    :return: None; failed expectations raise AssertionError.
    """
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


def test_cache_api_creates_writers_and_reconciles_scalar_writes(
    contract_cache,
) -> None:
    """
    Check planned, direct, writer-bound, and bulk scalar writes are visible through the cache API.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_api_creates_writers_and_reconciles_scalar_writes


    :param contract_cache: Loaded storage cache for the current plugin, backed by
        unicode_contract_db.
    :return: None; failed expectations raise AssertionError.
    """
    cache = Cache.from_storage(contract_cache)
    writer = cache.create_writer("books", "title")

    assert isinstance(writer, CatalogColumnWriter)
    planned = writer.build_one_update(1, "planned")
    assert planned.values == {1: "planned"}
    assert writer.apply_update(planned) == {1: "planned"}
    assert cache.get("books", 1).value["title"] == "planned"
    assert cache.write_one("books", "title", 1, _UPDATED_TITLE) == {
        1: _UPDATED_TITLE
    }
    assert cache.get("books", 1).value["title"] == _UPDATED_TITLE

    writer.write_one(2, "writer-bound update")
    assert cache.get("books", 2).value["title"] == "writer-bound update"

    cache.write("books", "title", {1: "bulk update"})
    assert cache.get("books", 1).value["title"] == "bulk update"


def test_cache_bound_writer_rejects_use_after_cache_detach(
    contract_cache,
    unicode_contract_db: FakeDB,
) -> None:
    """
    Detach the backing database and check a retained writer raises without changing the stored title.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_bound_writer_rejects_use_after_cache_detach


    :param contract_cache: Loaded storage cache for the current plugin, backed by
        unicode_contract_db.
    :param unicode_contract_db: Fresh FakeDB with Unicode books, covers, tags, and
        relation rows; tests may mutate it.
    :return: None; failed expectations raise AssertionError.
    """
    cache = Cache.from_storage(contract_cache)
    writer = cache.create_writer("books", "title")
    before = unicode_contract_db._rows_by_table["books"][0]["title"]

    assert contract_cache.detach_db() is unicode_contract_db
    with pytest.raises(RuntimeError, match="closed, detached"):
        writer.write_one(1, "must not be written")

    assert unicode_contract_db._rows_by_table["books"][0]["title"] == before


def test_cache_api_reconciles_owned_one_to_one_writes(contract_cache) -> None:
    """
    Check cover writes reuse the owned row and unlinking removes the cached relation value.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_api_reconciles_owned_one_to_one_writes


    :param contract_cache: Loaded storage cache for the current plugin, backed by
        unicode_contract_db.
    :return: None; failed expectations raise AssertionError.
    """
    cache = Cache.from_storage(contract_cache)
    writer = cache.create_writer("books", "path")

    assert isinstance(writer, CatalogOwnedRowOneToOneWriter)
    result = cache.write_one(
        "books",
        "path",
        1,
        "/covers/cache-api-updated.jpg",
    )

    assert result[1][0].secondary_id == 10
    assert cache.storage.get_field("books.covers.path").get_value_from_src_id(1) == (
        "/covers/cache-api-updated.jpg"
    )

    assert writer.write_one(1, None) == {1: ()}
    assert cache.storage.get_field("books.covers.path").get_value_from_src_id(1) is None


def test_cache_api_reconciles_shared_link_bulk_and_single_writes(
    contract_cache,
    unicode_contract_db: FakeDB,
) -> None:
    """
    Check shared-tag replacement and addition update cached relations, and deleting a missing tag creates no tag row.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_api_reconciles_shared_link_bulk_and_single_writes


    :param contract_cache: Loaded storage cache for the current plugin, backed by
        unicode_contract_db.
    :param unicode_contract_db: Fresh FakeDB with Unicode books, covers, tags, and
        relation rows; tests may mutate it.
    :return: None; failed expectations raise AssertionError.
    """
    cache = Cache.from_storage(contract_cache)
    writer = cache.create_writer("books", "tag_name")

    assert isinstance(writer, CatalogTableValueLinkWriter)
    planned = writer.build_one_update(1, LinkValue(40))
    assert isinstance(planned, LinkUpdate)
    assert planned.replacements == {1: (LinkValue(40),)}

    before = len(unicode_contract_db._rows_by_table["tags"])
    with pytest.raises(ValueError, match="requires a typed link spec"):
        cache.write_one(
            "books",
            "tag_name",
            1,
            "invalid typed value",
            link_type="author",
        )
    assert len(unicode_contract_db._rows_by_table["tags"]) == before

    cache.write_one("books", "tag_name", 1, "new cache API tag")
    field = cache.storage.get_field("books.tags.tag_name")
    assert tuple(field.get_values_from_src_id(1)) == ("new cache API tag",)

    cache.write(
        "books",
        "tag_name",
        additions={1: _TAG_1},
    )
    assert set(cache.storage.get_field("books.tags.tag_name").get_values_from_src_id(1)) == {
        "new cache API tag",
        _TAG_1,
    }

    before = len(unicode_contract_db._rows_by_table["tags"])
    cache.write(
        "books",
        "tag_name",
        deletions={1: "missing cache API tag"},
    )
    assert len(unicode_contract_db._rows_by_table["tags"]) == before


@pytest.fixture()
def typed_writer_cache(cache_plugin_name: str):
    """
    Build a loaded cache and fake database with role-typed tag links and a live allowed-types table.

    Start with one book, no tags or links, and only author permitted as a type. Tests
    can append allowed types directly to the fake rows.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py


    :param cache_plugin_name: Plugin name selected from CACHE_PLUGIN_KWARGS.
    :return: Tuple of (loaded storage cache, FakeDB).
    """
    books = make_table(
        "typed_books",
        ("id", "title"),
        is_main_table=True,
        linked_tables=("typed_tags",),
    )
    tags = make_table(
        "typed_tags",
        ("id", "tag_name"),
        is_main_table=True,
        linked_tables=("typed_books",),
    )
    links = make_table(
        "typed_book_tag_links",
        ("id", "book_id", "tag_id", "link_type"),
        is_link_table=True,
        linked_tables=("typed_books", "typed_tags"),
    )
    allowed_types = StorageTableSpec(
        name="typed_book_tag_links__types",
        relation_kind=RelationKind.TABLE,
        columns=(
            StorageColumnSpec(
                name="type",
                ordinal=0,
                declared_type="TEXT",
                affinity="TEXT",
                is_primary_key=True,
            ),
        ),
        id_column="type",
    )
    link_spec = StorageLinkSpec(
        primary_table="typed_books",
        secondary_table="typed_tags",
        link_table="typed_book_tag_links",
        cardinality=LinkCardinality.MANY_TO_MANY,
        primary_link_col="book_id",
        secondary_link_col="tag_id",
        type_link_col="link_type",
        typed=True,
        type_part_of_identity=True,
        allowed_types_table="typed_book_tag_links__types",
    )
    database = make_fake_db(
        schema=StorageSchemaSpec(
            tables={
                "typed_books": books,
                "typed_tags": tags,
                "typed_book_tag_links": links,
                "typed_book_tag_links__types": allowed_types,
            },
            interlinks=(link_spec,),
            intralinks=(),
        ),
        rows_by_table={
            "typed_books": [{"id": 1, "title": "Typed Book"}],
            "typed_tags": [],
            "typed_book_tag_links": [],
            "typed_book_tag_links__types": [{"type": "author"}],
        },
    )
    return create_loaded_test_cache(database, cache_plugin_name), database


def test_cache_api_preserves_live_link_type_guards(typed_writer_cache) -> None:
    """
    Check a disallowed role writes no rows, then a newly allowed role supports writer and typed bulk updates.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_api_preserves_live_link_type_guards


    :param typed_writer_cache: Pair of loaded storage cache and its FakeDB, including a
        live allowed-types table.
    :return: None; failed expectations raise AssertionError.
    """
    storage, database = typed_writer_cache
    cache = Cache.from_storage(storage)
    writer = cache.create_writer("typed_books", "tag_name")

    with pytest.raises(ValueError, match="does not exist in allowed-types"):
        cache.write_one(
            "typed_books",
            "tag_name",
            1,
            "Ada",
            link_type="reviewer",
        )
    assert database._rows_by_table["typed_tags"] == []
    assert database._rows_by_table["typed_book_tag_links"] == []

    database._rows_by_table["typed_book_tag_links__types"].append(
        {"type": "reviewer"}
    )
    result = writer.write_one(1, "Ada", link_type="reviewer")

    assert result[1][0].link_type == "reviewer"
    assert tuple(
        cache.storage.get_field(
            "typed_books.typed_tags.tag_name"
        ).get_values_from_src_id(1)
    ) == ("Ada",)

    typed_result = cache.write(
        "typed_books",
        "tag_name",
        {1: {"reviewer": "Grace"}},
    )
    assert typed_result[1][0].link_type == "reviewer"
    assert tuple(
        cache.storage.get_field(
            "typed_books.typed_tags.tag_name"
        ).get_values_from_src_id(1)
    ) == ("Grace",)


def test_cache_plugin_preserves_distinct_unicode_normalization_forms(contract_cache) -> None:
    """
    Check canonically equivalent NFC and NFD titles retain separate reverse-lookup IDs.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_plugin_preserves_distinct_unicode_normalization_forms


    :param contract_cache: Loaded storage cache for the current plugin, backed by
        unicode_contract_db.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Check qualified and unambiguous field resolution, rejection of shared_code ambiguity, and exact field enumeration sets.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_plugin_field_resolution_contract


    :param contract_cache: Loaded storage cache for the current plugin, backed by
        unicode_contract_db.
    :return: None; failed expectations raise AssertionError.
    """
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
    assert {field.field_key for field in cache.iter_fields()} == {
        "books.covers.path",
        "books.covers.shared_code",
        "books.id",
        "books.shared_code",
        "books.tags.tag_name",
        "books.title",
        "covers.books.shared_code",
        "covers.books.title",
        "covers.id",
        "covers.path",
        "covers.shared_code",
        "tags.books.shared_code",
        "tags.books.title",
        "tags.id",
        "tags.tag_name",
    }


def test_cache_plugin_one_to_one_link_table_maps_are_exposed(contract_cache) -> None:
    """
    Check one-to-one forward IDs, reverse IDs, and cover-value maps.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_plugin_one_to_one_link_table_maps_are_exposed


    :param contract_cache: Loaded storage cache for the current plugin, backed by
        unicode_contract_db.
    :return: None; failed expectations raise AssertionError.
    """
    link_table = contract_cache.get_one_one_link_table("books", "covers")

    assert link_table.get_primary_id_secondary_value_id_map() == {1: 10, 2: 11}
    assert link_table.get_secondary_id_primary_id_map() == {10: 1, 11: 2}
    assert link_table.get_primary_id_secondary_value_map() == {
        1: _COVER_PATH_1,
        2: _COVER_PATH_2,
    }


def test_cache_plugin_link_table_reverse_and_pair_lookups_are_readable(contract_cache) -> None:
    """
    Check reverse one-to-one ID lookup and the presence of a many-to-many pair.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_plugin_link_table_reverse_and_pair_lookups_are_readable


    :param contract_cache: Loaded storage cache for the current plugin, backed by
        unicode_contract_db.
    :return: None; failed expectations raise AssertionError.
    """
    one_one = contract_cache.get_one_one_link_table("books", "covers")
    many_many = contract_cache.get_many_many_link_table("books", "tags")

    assert one_one.get_src_id(11) == 2
    assert many_many.has_link(1, 40) is True


def test_cache_plugin_source_oriented_link_rows_hide_storage_orientation(contract_cache) -> None:
    """
    Check ordered forward and reverse source lookups expose the expected link and endpoint IDs.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_plugin_source_oriented_link_rows_hide_storage_orientation


    :param contract_cache: Loaded storage cache for the current plugin, backed by
        unicode_contract_db.
    :return: None; failed expectations raise AssertionError.
    """
    forward_rows = tuple(
        contract_cache.get_link_rows_for_source(
            "books",
            1,
            "tags",
            require_ordering=True,
        )
    )
    reverse_rows = tuple(
        contract_cache.get_link_rows_for_source(
            "tags",
            40,
            "books",
            require_ordering=True,
        )
    )

    assert [row.row_dict["id"] for row in forward_rows] == [200, 201]
    assert [row.row_dict["tag_id"] for row in forward_rows] == [40, 41]
    assert [row.row_dict["id"] for row in reverse_rows] == [200]
    assert [row.row_dict["book_id"] for row in reverse_rows] == [1]


def test_cache_plugin_one_to_one_relation_fields_are_discovered_and_readable(
    contract_cache,
) -> None:
    """
    Check cover field aliases, values, endpoint lookups, and the destination-value map.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_plugin_one_to_one_relation_fields_are_discovered_and_readable


    :param contract_cache: Loaded storage cache for the current plugin, backed by
        unicode_contract_db.
    :return: None; failed expectations raise AssertionError.
    """
    cache = contract_cache
    field = cache.get_field("books.covers.path")

    assert cache.has_field("books.covers.path")
    assert cache.has_field("covers.path") is True
    assert cache.has_field("path") is True

    assert field.get_value_from_src_id(1) == _COVER_PATH_1
    assert field.get_value_from_src_id(2) == _COVER_PATH_2
    assert field.get_dst_id_from_src_id(1) == 10
    assert field.get_src_id_from_dst_id(11) == 2
    assert field.dst_ids_values_map == {
        10: _COVER_PATH_1,
        11: _COVER_PATH_2,
    }


def test_cache_plugin_relation_value_reverse_lookups_are_readable(contract_cache) -> None:
    """
    Check destination and source IDs from relation values and the complete tag value set.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_plugin_relation_value_reverse_lookups_are_readable


    :param contract_cache: Loaded storage cache for the current plugin, backed by
        unicode_contract_db.
    :return: None; failed expectations raise AssertionError.
    """
    cache = contract_cache
    cover_field = cache.get_field("books.covers.path")
    tags_field = cache.get_field("books.tags.tag_name")

    assert cover_field.get_value_from_dst_id(10) == _COVER_PATH_1
    assert tags_field.get_dst_ids_from_value(_TAG_2) == [41]
    assert tags_field.get_src_ids_from_value(_TAG_1) == [1]
    assert tags_field.values_set == {_TAG_1, _TAG_2, _TAG_3}


def test_cache_plugin_row_helpers_and_defaults(contract_cache) -> None:
    """
    Check scalar row tuples and supplied defaults for a missing row ID.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_plugin_row_helpers_and_defaults


    :param contract_cache: Loaded storage cache for the current plugin, backed by
        unicode_contract_db.
    :return: None; failed expectations raise AssertionError.
    """
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


def test_cache_plugin_fresh_reads_follow_declared_live_read_capability(
    contract_cache,
    unicode_contract_db: FakeDB,
) -> None:
    """
    Mutate fake rows externally and check fresh lookups follow live_reads, reloading snapshot plugins before expecting changes.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_plugin_fresh_reads_follow_declared_live_read_capability


    :param contract_cache: Loaded storage cache for the current plugin, backed by
        unicode_contract_db.
    :param unicode_contract_db: Fresh FakeDB with Unicode books, covers, tags, and
        relation rows; tests may mutate it.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Mutate rows externally and check retained table and field objects follow live_child_objects without a reload.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_plugin_held_objects_follow_declared_live_child_capability


    :param contract_cache: Loaded storage cache for the current plugin, backed by
        unicode_contract_db.
    :param unicode_contract_db: Fresh FakeDB with Unicode books, covers, tags, and
        relation rows; tests may mutate it.
    :return: None; failed expectations raise AssertionError.
    """
    cache = contract_cache
    books_table = cache.get_main_table("books")
    cover_path_field = cache.get_field("books.covers.path")

    assert books_table.has_id(3) is False
    assert cover_path_field.get_value_from_src_id(1) == _COVER_PATH_1

    unicode_contract_db.driver_wrapper.update_column("books", 1, "title", _UPDATED_TITLE)
    unicode_contract_db.driver_wrapper.update_column("covers", 10, "path", _LIVE_COVER_PATH)
    unicode_contract_db.driver_wrapper.add_row(
        {"title": _NEW_BOOK_TITLE, "shared_code": "A-γ"}
    )

    if cache.capabilities.live_child_objects:
        assert books_table.has_id(3) is True
        assert books_table.get_row_snapshot(3)["title"] == _NEW_BOOK_TITLE
        assert cover_path_field.get_value_from_src_id(1) == _LIVE_COVER_PATH
    else:
        assert books_table.has_id(3) is False
        assert cover_path_field.get_value_from_src_id(1) == _COVER_PATH_1


def test_cache_plugin_vectorized_helper_surface_follows_declared_capabilities(
    contract_cache,
) -> None:
    """
    Check NumPy row IDs, field-owner IDs, and title arrays when vectorized helpers are advertised.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_plugin_vectorized_helper_surface_follows_declared_capabilities


    :param contract_cache: Loaded storage cache for the current plugin, backed by
        unicode_contract_db.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Check a full reload observes changed Unicode title/tag values and a newly inserted book.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_plugin_reload_observes_external_unicode_changes


    :param contract_cache: Loaded storage cache for the current plugin, backed by
        unicode_contract_db.
    :param unicode_contract_db: Fresh FakeDB with Unicode books, covers, tags, and
        relation rows; tests may mutate it.
    :return: None; failed expectations raise AssertionError.
    """
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


def test_cache_plugin_reload_main_table_refreshes_relation_projection(
    contract_cache,
    unicode_contract_db: FakeDB,
) -> None:
    """
    Delete a cover and reload its table, checking the missing projection and the unaffected second cover.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_plugin_reload_main_table_refreshes_relation_projection


    :param contract_cache: Loaded storage cache for the current plugin, backed by
        unicode_contract_db.
    :param unicode_contract_db: Fresh FakeDB with Unicode books, covers, tags, and
        relation rows; tests may mutate it.
    :return: None; failed expectations raise AssertionError.
    """
    cache = contract_cache

    assert cache.get_field("books.covers.path").get_value_from_src_id(1) == _COVER_PATH_1

    unicode_contract_db.driver_wrapper.delete_by_id("covers", {10})
    cache.reload_main_table("covers")

    field = cache.get_field("books.covers.path")
    assert field.get_value_from_src_id(1) is None
    assert field.get_value_from_src_id(2) == _COVER_PATH_2


def test_cache_plugin_invalidations_reload_relation_dependencies(
    contract_cache,
    unicode_contract_db: FakeDB,
) -> None:
    """
    Delete fake cover and link rows, invalidate their tables, and check dependent relation values refresh.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_plugin_invalidations_reload_relation_dependencies


    :param contract_cache: Loaded storage cache for the current plugin, backed by
        unicode_contract_db.
    :param unicode_contract_db: Fresh FakeDB with Unicode books, covers, tags, and
        relation rows; tests may mutate it.
    :return: None; failed expectations raise AssertionError.
    """
    cache = contract_cache

    unicode_contract_db.driver_wrapper.delete_by_id("covers", {10})
    cache.invalidate_table("covers")

    assert cache.get_field("books.covers.path").get_value_from_src_id(1) is None

    unicode_contract_db.driver_wrapper.delete_by_id("book_tags", {200})
    cache.invalidate_link_table("books", "tags")

    assert tuple(
        cache.get_field("books.tags.tag_name").get_values_from_src_id(
            1,
            require_ordering=True,
        )
    ) == (_TAG_2,)


def test_cache_plugin_lifecycle_contract(
    contract_cache,
    unicode_contract_db: FakeDB,
) -> None:
    """
    Check detach, reattach, clear, reload, and close update catalog references, loaded flags, and cached collections.

    After close, a read without a database raises RuntimeError.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_cache_plugin_contract.py::test_cache_plugin_lifecycle_contract


    :param contract_cache: Loaded storage cache for the current plugin, backed by
        unicode_contract_db.
    :param unicode_contract_db: Fresh FakeDB with Unicode books, covers, tags, and
        relation rows; tests may mutate it.
    :return: None; failed expectations raise AssertionError.
    """
    cache = contract_cache

    assert cache.is_loaded is True
    assert cache.is_initialized is True

    detached_db = cache.detach_db()
    assert detached_db is unicode_contract_db
    assert cache.catalog is None

    cache.read(detached_db)
    assert cache.catalog is unicode_contract_db
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
    assert cache.catalog is None
    assert cache.is_loaded is False
    assert cache.is_initialized is False
    assert cache.main_tables == {}
    assert cache.link_tables == {}
    assert cache.fields == {}

    with pytest.raises(RuntimeError):
        cache.read()
