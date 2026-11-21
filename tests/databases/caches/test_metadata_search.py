"""Exercise native search pages, semantic validation and execution provenance."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, replace

import pytest

from LiuXin_alpha.caches.api import (
    CacheClosedError,
    CacheDirtyError,
    UnsupportedCacheQueryError,
)
from LiuXin_alpha.caches.cache import create_cache
from LiuXin_alpha.caches.metadata import MetadataAPI
from LiuXin_alpha.catalog.api.metadata_search import (
    MetadataSearchFilter as Filter,
)
from LiuXin_alpha.catalog.api.metadata_search import (
    MetadataSearchRequest as Request,
)
from LiuXin_alpha.catalog.api.metadata_search import (
    MetadataSearchSort as Sort,
)
from LiuXin_alpha.catalog.api.metadata_search import (
    MetadataSearchValidationError,
)
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.catalog.search.database_search import CatalogSearch
from LiuXin_alpha.catalog.search.metadata_requests import fields, sort_fields, validate
from tests.databases.caches.test_metadata_api import (
    MODES,
)
from tests.databases.caches.test_metadata_api import (
    metadata_db as metadata_db,
)
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)


def references(page):
    return tuple(ref.entity_id for ref in page.references)


def seeded(db, mode):
    catalog = Catalog(db)
    ids = tuple(
        catalog.works.create({"title": title, "original_year": year})
        for title, year in (
            ("Zulu", 1999),
            ("alpha", 2001),
            ("ALPHA", 2001),
            ("Missing year", None),
        )
    )
    cache = None if mode is None else create_cache(db, mode)
    return MetadataAPI(catalog, cache=cache), cache, ids


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
def test_search_pages_have_exact_totals_stable_sorts_and_provenance(metadata_db, mode):
    api, cache, ids = seeded(metadata_db, mode)
    request = Request(sort=(Sort("year", False), Sort("title")), limit=1, offset=1)
    page = api.search.query(request)
    assert page.level == "work"
    assert references(page) == (ids[1],)
    assert page.total_count == 4 and page.has_more
    assert (page.offset, page.limit) == (1, 1)
    assert page.source == ("database" if cache is None else "cache")
    assert page.consistency == (
        "live" if mode in (None, "database_backed") else "snapshot"
    )
    assert page.generation == (None if cache is None else cache.generation)
    whole = api.search.query(replace(request, offset=0, limit=None))
    # Native text ordering uses normalized text, then original text.
    assert references(whole) == (ids[2], ids[1], ids[0], ids[3])
    tied = api.search.query(Request(sort=(Sort("year", False),)))
    assert references(tied) == (ids[1], ids[2], ids[0], ids[3])
    assert not whole.has_more
    zero = api.search.query(replace(request, limit=0, offset=0))
    assert zero.references == () and zero.total_count == 4 and zero.has_more
    beyond = api.search.query(replace(request, offset=100))
    assert beyond.references == () and beyond.total_count == 4 and not beyond.has_more
    filtered = Request(
        "title:alpha", filters=(Filter("year", "gte", 2000),), sort=(Sort("id", False),)
    )
    page = api.search.query(filtered)
    assert references(page) == (ids[2], ids[1]) and page.total_count == 2
    assert tuple(ref.entity_id for ref in api.search.find("title:alpha")) == (
        ids[1],
        ids[2],
    )
    # Warm pages must be invalidated by semantic writes through the same owner.
    api.works.update(ids[0], {"title": "alpha", "original_year": 2005})
    assert api.search.query(filtered).total_count == 3
    api.works.delete(ids[1])
    assert references(api.search.query(filtered)) == (ids[2], ids[0])
    if cache is not None:
        cache.close()


@pytest.mark.parametrize("mode", MODES)
def test_search_filters_traverse_wemi_without_borrowing_siblings(metadata_db, mode):
    catalog = Catalog(metadata_db)
    writer = catalog.mutations.writer
    first = writer.create_wemi_stack(
        work={"title": "Original", "original_year": 2001},
        expression={"label": "First"},
        manifestation={"edition_statement": "Shared"},
        items=({"inventory_code": "A"},),
    )
    second = writer.create_wemi_stack(
        work={"title": "Sibling", "original_year": 1998},
        expression={"label": "Second"},
        manifestation={"edition_statement": "Separate"},
        items=({"inventory_code": "B"},),
    )
    writer.link_wemi(
        parent_level="expression",
        parent_id=second.expression_id,
        child_level="manifestation",
        child_id=first.manifestation_id,
    )
    literal = '=Quoted "title": (and OR)'
    catalog.works.update(first.work_id, {"title": literal})
    cache = None if mode is None else create_cache(metadata_db, mode)
    api = MetadataAPI(catalog, cache=cache)
    # Values are compiled as literals, never interpolated into text query syntax.
    for operator in ("contains", "eq"):
        request = Request(filters=(Filter("works.titles", operator, literal),))
        assert references(api.search.query(request)) == (first.work_id,)
    for level, expected in (
        ("work", first.work_id),
        ("expression", first.expression_id),
        ("manifestation", first.manifestation_id),
        ("item", first.item_ids[0]),
    ):
        page = api.search.query(
            Request(level=level, filters=(Filter("work.year", "gte", 2000),))
        )
        assert references(page) == (expected,)
        assert all(ref.level == level for ref in page.references)
    assert references(
        api.search.query(Request(filters=(Filter("work.id", "in", (second.work_id,)),)))
    ) == (second.work_id,)
    assert api.search.query(Request(filters=(Filter("id", "in", ()),))).total_count == 0
    if cache is not None:
        cache.close()


@pytest.mark.parametrize(
    "operator,value,indices",
    [
        ("eq", None, (3,)),
        ("ne", None, (0, 1, 2)),
        ("lt", 2000, (0,)),
        ("lte", 1999, (0,)),
        ("gt", 1999, (1, 2)),
        ("is_null", True, (3,)),
        ("is_null", False, (0, 1, 2)),
        ("in", (1999, 2001), (0, 1, 2)),
    ],
)
def test_numeric_filter_operands_agree_between_sql_and_cache(
    metadata_db, operator, value, indices
):
    api, cache, ids = seeded(metadata_db, "schema_backed")
    direct = MetadataAPI(Catalog(metadata_db))
    request = Request(filters=(Filter("year", operator, value),))
    expected = tuple(ids[index] for index in indices)
    assert (
        references(api.search.query(request))
        == references(direct.search.query(request))
        == expected
    )
    cache.close()


@pytest.mark.parametrize(
    "search_request,parameter",
    [
        (Request(level="book"), "level"),
        (Request(query=42), "query"),
        (Request("title:"), "query"),
        (Request("("), "query"),
        (Request(limit=True), "limit"),
        (Request(limit=-1), "limit"),
        (Request(offset=None), "offset"),
        (Request(offset=False), "offset"),
        (Request(filters=(Filter("work_title", "eq", "x"),)), "filters[0].field"),
        (Request(filters=(Filter("title", "gt", "x"),)), "filters[0].operator"),
        (Request(filters=(Filter("title", "eq", "  "),)), "filters[0].value"),
        (Request(filters=(Filter("year", "gte", True),)), "filters[0].value"),
        (Request(filters=(Filter("year", "in", (True,)),)), "filters[0].value"),
        (Request(filters=(Filter("year", "in", [2000]),)), "filters[0].value"),
        (Request(filters=(Filter("year", "is_null", 0),)), "filters[0].value"),
        (Request(filters=(Filter("year", "lt", None),)), "filters[0].value"),
        (
            Request(level="item", filters=(Filter("year", "eq", 2000),)),
            "filters[0].field",
        ),
        (Request(sort=(Sort("author"),)), "sort[0].field"),
        (Request(sort=(Sort("title", "desc"),)), "sort[0].ascending"),
        (Request(sort=(Sort("title"), Sort("title"))), "sort[1].field"),
    ],
)
def test_search_validation_is_structured_and_requires_no_database(
    search_request, parameter
):
    with pytest.raises(MetadataSearchValidationError) as caught:
        validate(CatalogSearch(None, None, None), search_request)
    assert caught.value.parameter == parameter
    assert caught.value.message


def test_search_discovery_and_request_values_are_passive_and_immutable(
    metadata_db, monkeypatch
):
    api = MetadataAPI(Catalog(metadata_db))

    def unexpected(*args, **kwargs):
        raise AssertionError("Unexpected data/schema read")

    monkeypatch.setattr(metadata_db, "get_tables", unexpected)
    monkeypatch.setattr(metadata_db.driver_wrapper, "get_column_headings", unexpected)
    monkeypatch.setattr(metadata_db.macros, "find_rows_matching", unexpected)
    for level in ("work", "expression", "manifestation", "item"):
        descriptors = api.search.fields(level=level)
        assert descriptors == fields(level)
        assert len({field.name for field in descriptors}) == len(descriptors)
        assert api.search.sort_fields(level=level) == sort_fields(level)
        for descriptor in descriptors:
            term = Filter(
                descriptor.name,
                descriptor.operators[0],
                "example" if descriptor.value_type == "text" else 1,
            )
            api.search.validate(Request(level=level, filters=(term,)))
        for descriptor in api.search.sort_fields(level=level):
            api.search.validate(Request(level=level, sort=(Sort(descriptor.name),)))
    parts = [Filter("author", "contains", "Ursula")]
    request = Request(filters=parts)
    parts.clear()
    assert len(request.filters) == 1
    with pytest.raises(FrozenInstanceError):
        request.offset = 1


def test_search_fallback_uses_one_complete_database_page(metadata_db, monkeypatch):
    api, cache, ids = seeded(metadata_db, "schema_backed")
    fallback = MetadataAPI(
        Catalog(metadata_db), cache=cache, allow_database_fallback=True
    )
    with monkeypatch.context() as forbidden:

        def unexpected(*args, **kwargs):
            raise AssertionError("A complete miss must not query the database")

        forbidden.setattr(metadata_db.macros, "find_rows_matching", unexpected)
        miss = fallback.search.query(Request("title:absent"))
        assert miss.references == () and miss.total_count == 0
        assert miss.source == "cache" and miss.generation == cache.generation
    original = cache.query

    def partial(plan):
        return replace(original(plan), complete=False, total_count=900)

    monkeypatch.setattr(cache, "query", partial)
    request = Request(sort=(Sort("title"),), offset=1, limit=1)
    with pytest.raises(UnsupportedCacheQueryError):
        api.search.query(request)
    page = fallback.search.query(request)
    assert references(page) == (ids[1],) and page.total_count == 4
    assert (
        page.source == "database"
        and page.generation is None
        and page.consistency == "live"
    )

    def broken(plan):
        raise OSError("broken query")

    monkeypatch.setattr(cache, "query", broken)
    with pytest.raises(OSError, match="broken query"):
        fallback.search.query(request)
    cache.close()
    with pytest.raises(CacheClosedError):
        fallback.search.validate(request)


def test_search_driver_without_normalized_sql_uses_ephemeral_capture(
    metadata_db, monkeypatch
):
    api, _, ids = seeded(metadata_db, None)
    monkeypatch.setattr(metadata_db.macros, "supports_normalized_search", lambda: False)
    page = api.search.query(
        Request(filters=(Filter("year", "gte", 2000),), sort=(Sort("id", False),))
    )
    assert references(page) == (ids[2], ids[1])
    assert page.source == "database" and page.generation is None


def test_search_pending_transactions_block_reads_and_explicit_fallback(metadata_db):
    api, cache, ids = seeded(metadata_db, "schema_backed")
    fallback = MetadataAPI(
        Catalog(metadata_db), cache=cache, allow_database_fallback=True
    )
    with pytest.raises(RuntimeError, match="rollback"):
        with metadata_db.macros.transaction():
            api.works.delete(ids[0])
            with pytest.raises(CacheDirtyError):
                fallback.search.query(Request())
            executor = ThreadPoolExecutor(max_workers=1)
            try:
                with pytest.raises(CacheDirtyError):
                    executor.submit(fallback.search.query, Request()).result(timeout=15)
            finally:
                executor.shutdown(wait=False)
            api.search.validate(Request())
            raise RuntimeError("rollback")
    assert api.search.query(Request()).total_count == 4
    cache.close()
