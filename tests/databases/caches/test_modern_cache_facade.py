from __future__ import annotations

import unicodedata

from types import SimpleNamespace

import pytest

from LiuXin_alpha.caches import (
    Cache,
    CacheClosedError,
    CacheDirtyError,
    CacheFilterOperator,
    CacheLookupStatus,
    CachePredicate,
    CacheQuery,
    CacheReconciliationError,
    CacheRelation,
    CacheSort,
    CacheState,
)
from LiuXin_alpha.databases.schema_specs import (
    LinkCardinality,
    StorageLinkSpec,
    StorageSchemaSpec,
)
from LiuXin_alpha.metadata.read_sources import CacheMetadataReadSource
from tests.support.storage_cache_test_harness import (
    CACHE_PLUGIN_KWARGS,
    make_fake_db,
    make_table,
)


@pytest.fixture(params=("schema_backed", "database_backed", "numpy_vectorized"))
def modern_cache(request):
    books = make_table(
        "books",
        ("id", "title", "rating"),
        is_main_table=True,
        linked_tables=("tags",),
    )
    tags = make_table(
        "tags",
        ("id", "name"),
        is_main_table=True,
        linked_tables=("books",),
    )
    links = make_table(
        "book_tags",
        ("id", "book_id", "tag_id", "priority"),
        is_link_table=True,
        linked_tables=("books", "tags"),
    )
    link_spec = StorageLinkSpec(
        primary_table="books",
        secondary_table="tags",
        link_table="book_tags",
        cardinality=LinkCardinality.MANY_TO_MANY,
        primary_link_col="book_id",
        secondary_link_col="tag_id",
        priority_link_col="priority",
    )
    title_nfd = unicodedata.normalize("NFD", "Café Society")
    database = make_fake_db(
        StorageSchemaSpec(
            tables={
                "books": books,
                "tags": tags,
                "book_tags": links,
            },
            interlinks=(link_spec,),
            intralinks=(),
        ),
        {
            "books": [
                {"id": 1, "title": title_nfd, "rating": 4},
                {"id": 2, "title": "Alpha", "rating": None},
                {"id": 3, "title": "beta", "rating": 5},
            ],
            "tags": [
                {"id": 10, "name": "Fiction"},
                {"id": 11, "name": "History"},
            ],
            "book_tags": [
                {"id": 100, "book_id": 1, "tag_id": 10, "priority": 2},
                {"id": 101, "book_id": 1, "tag_id": 11, "priority": 1},
                {"id": 102, "book_id": 3, "tag_id": 10, "priority": 1},
            ],
        },
    )
    cache = Cache(
        database,
        storage_type=str(request.param),
        storage_kwargs=CACHE_PLUGIN_KWARGS[str(request.param)],
    )
    cache.load()
    return cache, database


def test_modern_cache_exact_lookup_and_known_miss(modern_cache) -> None:
    cache, _database = modern_cache

    schema = cache.table_columns()
    hit = cache.get("books", 1)
    miss = cache.get("books", 999)

    assert schema["books"] == ("id", "title", "rating")
    with pytest.raises(TypeError):
        schema["books"] = ()
    assert hit.status == CacheLookupStatus.HIT
    assert hit.complete is True
    assert hit.value is not None
    assert hit.value["rating"] == 4
    assert miss.status == CacheLookupStatus.MISS
    assert miss.value is None
    assert miss.complete is True


def test_modern_cache_structured_query_sort_page_and_unicode_text(modern_cache) -> None:
    cache, _database = modern_cache

    exact_nfd = cache.query(
        CacheQuery(
            table="books",
            predicates=(
                CachePredicate(
                    "title",
                    CacheFilterOperator.EQ,
                    unicodedata.normalize("NFD", "Café Society"),
                ),
            ),
        )
    )
    exact_nfc = cache.query(
        CacheQuery(
            table="books",
            predicates=(
                CachePredicate(
                    "title",
                    CacheFilterOperator.EQ,
                    unicodedata.normalize("NFC", "Café Society"),
                ),
            ),
        )
    )
    text = cache.query(
        CacheQuery(
            table="books",
            text="CAFÉ",
            text_fields=("title",),
        )
    )
    page = cache.query(
        CacheQuery(
            table="books",
            sort=(CacheSort("rating", ascending=False),),
            offset=1,
            limit=2,
        )
    )

    assert exact_nfd.ids == (1,)
    assert exact_nfc.ids == ()
    assert text.ids == (1,)
    assert page.total_count == 3
    assert page.ids == (1, 2)

    with pytest.raises(TypeError):
        text.records[0].values["title"] = "cannot mutate"


def test_modern_cache_relation_constraint_and_ordered_traversal(modern_cache) -> None:
    cache, _database = modern_cache

    fiction_books = cache.query(
        CacheQuery(
            table="books",
            relation=CacheRelation("tags", (10,)),
        )
    )
    tags = cache.related("books", (1,), "tags")
    link_records = cache.link_records("books", 1, "tags")

    assert fiction_books.ids == (1, 3)
    assert tags.ids == (10, 11)
    assert tuple(record["tag_id"] for record in link_records) == (10, 11)


def test_modern_cache_write_reconciles_and_advances_generation(modern_cache) -> None:
    cache, _database = modern_cache
    before = cache.generation

    assert cache.write_one("books", "title", 1, "Updated") == {1: "Updated"}

    assert cache.get("books", 1).value["title"] == "Updated"
    assert cache.generation > before
    assert cache.state == CacheState.READY


def test_modern_cache_external_write_requires_explicit_invalidation(modern_cache) -> None:
    cache, database = modern_cache
    if cache.capabilities.consistency.value == "live":
        pytest.skip("live backend observes the external change directly")

    database.driver_wrapper.update_column("books", 1, "title", "External")
    assert cache.get("books", 1).value["title"] != "External"

    cache.invalidate(tables=("books",))

    assert cache.state == CacheState.DIRTY
    assert cache.get("books", 1).value["title"] == "External"
    assert cache.state == CacheState.READY


def test_cache_bound_writer_preserves_preexisting_dirty_dependencies(
    modern_cache,
) -> None:
    cache, database = modern_cache
    if cache.capabilities.consistency.value == "live":
        pytest.skip("live backend has no pending snapshot dependencies")
    writer = cache.create_writer("books", "title")

    database.driver_wrapper.update_column("tags", 10, "name", "External tag")
    cache.invalidate(tables=("tags",))
    writer.write_one(1, "Writer update")

    assert cache.get("books", 1).value["title"] == "Writer update"
    assert cache.get("tags", 10).value["name"] == "External tag"
    assert cache.state == CacheState.READY


def test_modern_cache_reconciliation_failure_preserves_receipt_and_recovers(
    modern_cache,
    monkeypatch,
) -> None:
    cache, _database = modern_cache
    if cache.capabilities.consistency.value == "live":
        pytest.skip("live backend has no snapshot reconciliation step")

    original_reload = cache.storage.reload_main_table

    def fail_reload(*_args, **_kwargs):
        raise RuntimeError("injected reconciliation failure")

    monkeypatch.setattr(cache.storage, "reload_main_table", fail_reload)
    with pytest.raises(CacheReconciliationError) as caught:
        cache.write_one("books", "title", 1, "Committed")

    assert caught.value.receipt == {1: "Committed"}
    assert "books" in caught.value.dependencies
    assert cache.state == CacheState.DIRTY

    monkeypatch.setattr(cache.storage, "reload_main_table", original_reload)
    assert cache.get("books", 1).value["title"] == "Committed"
    assert cache.state == CacheState.READY


def test_snapshot_cache_defers_dirty_reads_until_outer_transaction_closes(
    modern_cache,
) -> None:
    cache, database = modern_cache
    if cache.capabilities.consistency.value == "live":
        pytest.skip("live backend does not require deferred snapshot refresh")

    database.macros._macro_transaction_state = SimpleNamespace(depth=1)
    cache.write_one("books", "title", 1, "Transaction")

    assert cache.state == CacheState.DIRTY
    with pytest.raises(CacheDirtyError):
        cache.get("books", 1)

    database.macros._macro_transaction_state.depth = 0
    assert cache.get("books", 1).value["title"] == "Transaction"
    assert cache.state == CacheState.READY


def test_closed_cache_rejects_reads(modern_cache) -> None:
    cache, _database = modern_cache
    cache.close()

    assert cache.state == CacheState.CLOSED
    with pytest.raises(CacheClosedError):
        cache.get("books", 1)


def test_complete_cache_miss_does_not_use_adapter_database_fallback(
    modern_cache,
    monkeypatch,
) -> None:
    cache, database = modern_cache
    source = CacheMetadataReadSource(
        cache,
        database=database,
        allow_database_fallback=True,
    )

    def forbidden(*_args, **_kwargs):
        raise AssertionError("known cache miss must not hit the database")

    monkeypatch.setattr(database, "get_row_from_id", forbidden)
    assert source.get_row_from_id("books", 999) is None
