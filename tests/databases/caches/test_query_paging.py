"""Ordering/page oracles and bounded selection work across native cache backends."""

import random
import unicodedata
from dataclasses import replace

import pytest

from LiuXin_alpha.caches.api import CacheQuery, CacheSort, UnknownCacheFieldError
from LiuXin_alpha.caches.projection import ProjectionQueryEngine
from tests.databases.caches.test_modern_cache_facade import modern_cache as modern_cache


def reference_order(rows, specs):
    """Independent stable ordering over source values; no production sort helpers."""

    def key(value):
        if isinstance(value, bool):
            return (0, int(value))
        if isinstance(value, (int, float)):
            return (1, value)
        if isinstance(value, str):
            return (2, (unicodedata.normalize("NFKC", value).casefold(), value))
        if isinstance(value, (tuple, list, set, frozenset)):
            return (3, tuple(key(item) for item in value))
        return (4, repr(value))

    ordered = sorted(rows)
    for spec in reversed(specs):
        column = spec.field.removeprefix("rows.")
        present = [i for i in ordered if rows[i][column] is not None]
        missing = [i for i in ordered if rows[i][column] is None]
        present.sort(key=lambda i: key(rows[i][column]), reverse=not spec.ascending)
        ordered = present + missing
    return tuple(ordered)


@pytest.mark.parametrize("seed", range(4))
@pytest.mark.parametrize("cache_size", (0, 3))
def test_pages_match_independent_full_order(seed, cache_size):
    rng = random.Random(seed)
    identities = list(range(1, 258))
    rng.shuffle(identities)
    values = (
        None,
        False,
        True,
        -2,
        0,
        0.5,
        2,
        "",
        "Café",
        "Cafe\u0301",
        "Ｓtraße",
        "STRASSE",
        (),
        (0,),
        (1, 2),
    )
    rows = {
        i: {
            "id": 1000 - i,
            "value": rng.choice(values),
            "other": rng.choice((None, 0, 1, 2)),
        }
        for i in identities
    }
    engine = ProjectionQueryEngine(
        "rows", rows, ("id", "value", "other"), result_cache_size=cache_size
    )
    # Stored ID values intentionally differ from the row identity. Explicit ID
    # sorting must read the field; default ordering must use the row identity.
    for case in range(60):
        specs = tuple(
            CacheSort(field, rng.choice((True, False)))
            for field in rng.sample(("id", "value", "other"), rng.randrange(4))
        )
        ordered = reference_order(rows, specs)
        offset = rng.choice((0, 5, 250, 256, 257, 300, 128))
        limit = rng.choice((None, 0, 1, 7, 25, 150, 500))
        query = CacheQuery(
            "rows", sort=specs, offset=offset, limit=limit, projection=("value",)
        )
        expected = ordered[offset : None if limit is None else offset + limit]
        for _ in range(2):
            result = engine.query(query, generation=4)
            assert result.ids == expected, (seed, case, query)
            assert result.total_count == len(rows)
            assert (
                result.offset,
                result.limit,
                result.generation,
                result.complete,
            ) == (offset, limit, 4, True)
            assert all(
                record["value"] == rows[record.row_id]["value"]
                for record in result.records
            )
        assert len(engine._results) <= cache_size


@pytest.mark.parametrize("offset,limit", ((0, 0), (1000, 20), (2000, None)))
def test_empty_pages_do_no_ordering_or_value_reads(monkeypatch, offset, limit):
    rows = {i: {"id": i, "value": i % 10} for i in range(1000)}
    engine = ProjectionQueryEngine("rows", rows, ("id", "value"))
    engine.query(CacheQuery("rows", limit=0), generation=1)

    def fail(*args, **kwargs):
        raise AssertionError("An empty page does not need values or ordering")

    monkeypatch.setattr(engine, "_sort_ids", fail)
    monkeypatch.setattr(engine, "get_cached_value", fail)
    query = CacheQuery("rows", sort=(CacheSort("value"),), offset=offset, limit=limit)
    result = engine.query(query, generation=1)
    assert result.ids == () and result.total_count == 1000
    with pytest.raises(UnknownCacheFieldError):
        engine.query(replace(query, sort=(CacheSort("missing"),)), generation=1)
    with pytest.raises(UnknownCacheFieldError):
        engine.query(replace(query, projection=("missing",)), generation=1)


@pytest.mark.parametrize("offset,limit", ((5, 7), (500, 7), (500, None)))
@pytest.mark.parametrize("descending", (False, True))
def test_small_pages_do_not_full_sort_and_resolve_fields_once(
    monkeypatch, offset, limit, descending
):
    rows = {i: {"id": i, "value": None if i % 13 == 0 else i % 7} for i in range(512)}
    engine = ProjectionQueryEngine("rows", rows, ("id", "value"))
    specs = (CacheSort("value", not descending), CacheSort("id", descending))
    expected = reference_order(rows, specs)[
        offset : None if limit is None else offset + limit
    ]
    original = engine._canonical_field
    resolutions = []
    reads = []
    getter = engine.get_cached_value

    def resolve(*args):
        resolutions.append(args)
        return original(*args)

    def read(*args):
        reads.append(args)
        return getter(*args)

    def fail(*args, **kwargs):
        raise AssertionError("A small page must not sort every matching ID")

    monkeypatch.setattr(engine, "_sort_ids", fail)
    monkeypatch.setattr(engine, "_canonical_field", resolve)
    monkeypatch.setattr(engine, "get_cached_value", read)
    result = engine.query(
        CacheQuery("rows", sort=specs, offset=offset, limit=limit), generation=1
    )
    assert result.ids == expected
    assert result.total_count == len(rows)
    assert len(resolutions) <= 2 * len(specs)
    assert len(reads) == len(rows) * len(specs)


@pytest.mark.parametrize("nested", (False, True))
@pytest.mark.parametrize("descending", (False, True))
def test_nan_pages_preserve_stable_full_sort(nested, descending):
    rows = {
        i: {
            "id": i,
            "value": [float("nan") if i % 11 == 0 else i % 7]
            if nested
            else float("nan")
            if i % 11 == 0
            else i % 7,
        }
        for i in range(150)
    }
    specs = (CacheSort("value", not descending),)
    engine = ProjectionQueryEngine("rows", rows, ("id", "value"))
    ordered = reference_order(rows, specs)
    for offset in (0, 5, 140):
        result = engine.query(
            CacheQuery("rows", sort=specs, offset=offset, limit=4), generation=1
        )
        assert result.ids == ordered[offset : offset + 4]
        assert result.total_count == len(rows)


@pytest.mark.parametrize("offset,limit", ((0, 0.0), (0.0, 0), (999.0, 1), (0, 1.5)))
def test_non_integer_page_bounds_are_rejected(offset, limit):
    engine = ProjectionQueryEngine("rows", {1: {"id": 1}}, ("id",))
    with pytest.raises(TypeError):
        engine.query(CacheQuery("rows", offset=offset, limit=limit), generation=1)


def test_small_page_propagates_sort_value_read_failures(monkeypatch):
    engine = ProjectionQueryEngine(
        "rows", {i: {"id": i, "value": i} for i in range(100)}, ("id", "value")
    )
    original = engine.get_cached_value

    def read(i, field):
        if i == 99:
            raise RuntimeError("unavailable sort value")
        return original(i, field)

    monkeypatch.setattr(engine, "get_cached_value", read)
    with pytest.raises(RuntimeError, match="unavailable sort value"):
        engine.query(
            CacheQuery("rows", sort=(CacheSort("value"),), limit=1), generation=1
        )


def test_sorted_pages_follow_mutations_and_deletions(modern_cache):
    cache, db = modern_cache
    source = [{"id": i, "title": "Book", "rating": i % 9} for i in range(1, 301)]
    db.driver_wrapper._rows_by_table["books"] = source
    cache.invalidate(tables=("books",))
    query = CacheQuery("books", sort=(CacheSort("rating", False),), offset=2, limit=7)
    for step in range(9):
        expected = sorted(source, key=lambda row: (-row["rating"], row["id"]))
        for _ in range(2):
            result = cache.query(query)
            assert result.ids == tuple(row["id"] for row in expected[2:9])
            assert result.total_count == len(source)
        changed = source[step]
        changed["rating"] = 20 + step
        cache.invalidate(ids={"books": (changed["id"],)})
        removed = source.pop()
        cache.invalidate(ids={"books": (removed["id"],)})
    expected = sorted(source, key=lambda row: (-row["rating"], row["id"]))
    assert cache.query(query).ids == tuple(row["id"] for row in expected[2:9])
    assert cache.query(query).total_count == len(source)
