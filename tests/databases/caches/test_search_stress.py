"""Deterministic differential and failure tests protecting search optimizations.

The reference predicates below use plain Python over source rows, without the
production evaluator, normalization helpers, or cache indexes.
"""

import operator
import random
import unicodedata
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from dataclasses import replace
from threading import Event

import pytest

from LiuXin_alpha.caches.api import CacheDirtyError, CacheQuery, CacheSort, CacheState
from LiuXin_alpha.caches.projection import ProjectionQueryEngine
from LiuXin_alpha.databases.db_types.query_types import (
    BooleanMatch,
    ComparisonMatch,
    CountMatch,
    ValueMatch,
)
from tests.databases.caches.test_modern_cache_facade import modern_cache as modern_cache


def _tree(rng, depth=0):
    """Generate an expression together with an independent row-wise oracle."""
    if depth < 4 and rng.random() < 0.55:
        kind = rng.choice(("and", "or", "not"))
        children = [
            _tree(rng, depth + 1)
            for _ in range(1 if kind == "not" else rng.randrange(4))
        ]
        node = BooleanMatch(kind, tuple(child[0] for child in children))
        if kind == "and":
            return node, lambda row: all(test(row) for _, test in children)
        if kind == "or":
            return node, lambda row: any(test(row) for _, test in children)
        return node, lambda row: not children[0][1](row)
    field = rng.choice(("number", "title", "items"))
    if field == "items":
        amount = rng.randrange(5)
        op = rng.choice(("eq", "ne", "lt", "lte", "gt", "gte"))
        fn = {
            "eq": operator.eq,
            "ne": operator.ne,
            "lt": operator.lt,
            "lte": operator.le,
            "gt": operator.gt,
            "gte": operator.ge,
        }[op]
        return CountMatch(field, op, amount), lambda row: fn(
            len(row[field] or ()), amount
        )
    if field == "title":
        term = rng.choice(("a", "Book", "%_", "é", "Straße", "", "missing"))
        op = rng.choice(("eq", "ne", "contains", "prefix"))

        def oracle(row):
            if op == "eq":
                return row[field] == term
            if op == "ne":
                return row[field] != term
            if op == "contains":
                return row[field] is not None and term in row[field]
            return row[field] is not None and row[field].startswith(term)

        return ComparisonMatch(field, op, term), oracle
    amount = rng.randrange(-5, 6)
    op = rng.choice(("eq", "ne", "lt", "lte", "gt", "gte", "is_null", "in"))
    if op == "is_null":
        operand = rng.choice((True, False))
        return ComparisonMatch(
            field, op, operand
        ), lambda row: (row[field] is None) == operand
    if op == "in":
        choices = (None, amount, amount + 1)
        return ComparisonMatch(field, op, choices), lambda row: row[field] in choices
    fn = {
        "eq": operator.eq,
        "ne": operator.ne,
        "lt": operator.lt,
        "lte": operator.le,
        "gt": operator.gt,
        "gte": operator.ge,
    }[op]
    return (
        ComparisonMatch(field, op, amount),
        lambda row: (
            fn(row[field], amount)
            if op in {"eq", "ne"} or row[field] is not None
            else False
        ),
    )


@pytest.mark.parametrize("seed", range(8))
def test_random_predicate_trees_match_independent_oracle(seed):
    rng = random.Random(seed)
    rows = {
        i: {
            "id": i,
            "number": rng.choice((None, *range(-5, 6))),
            "title": rng.choice((None, "Book Alpha", "Café", "Straße", "%_", "")),
            "items": rng.choice((None, (), (1,), (1, 2), (1, 2, 3))),
        }
        for i in range(1, 97)
    }
    engine = ProjectionQueryEngine(
        "rows",
        rows,
        ("id", "number", "title", "items"),
        plan_cache_size=7,
        result_cache_size=5,
    )
    for case in range(120):
        node, oracle = _tree(rng)
        expected = tuple(i for i, row in rows.items() if oracle(row))
        query = CacheQuery("rows", where=node)
        assert engine.query(query, generation=1).ids == expected, (seed, case, node)
        # A membership hit must still respect each request's ordering and paging.
        paged = engine.query(
            replace(
                query,
                sort=(CacheSort("id", False),),
                offset=2,
                limit=3,
                projection=("title",),
            ),
            generation=1,
        )
        assert paged.ids == tuple(reversed(expected))[2:5], (seed, case)
        assert paged.total_count == len(expected)
        assert tuple(engine._results) == tuple(engine._result_queries)
        assert len(engine._plans) <= 7 and len(engine._results) <= 5


def test_unicode_trigram_index_matches_literal_scan():
    values = (
        "Café",
        "Cafe\u0301",
        "Ｓtraße",
        "STRASSE",
        "%_ O'Reilly",
        "alpha beta",
        "beta alpha",
        "ababa",
        "🙂猫",
        "",
        None,
    )
    rows = {
        i: {"id": i, "title": value, "other": "different field"}
        for i, value in enumerate(values)
    }
    engine = ProjectionQueryEngine("rows", rows, ("id", "title", "other"))
    for term in (
        "café",
        "strasse",
        "%_",
        "alpha beta",
        "aba",
        "🙂",
        "猫",
        "ab",
        "different café",
        "missing",
        "",
    ):
        tokens = unicodedata.normalize("NFKC", term).casefold().split()
        expected = tuple(
            i
            for i, row in rows.items()
            if all(
                any(
                    token in unicodedata.normalize("NFKC", row[field] or "").casefold()
                    for field in ("title", "other")
                )
                for token in tokens
            )
        )
        query = CacheQuery("rows", text=term, text_fields=("title", "other"))
        assert engine.query(query, generation=1).ids == expected
        assert engine.query(query, generation=1).ids == expected


@pytest.mark.parametrize("seed", range(3))
def test_mutation_sequences_equal_fresh_source_scan(modern_cache, seed):
    cache, db = modern_cache
    rng = random.Random(seed)
    source = db.driver_wrapper._rows_by_table["books"]
    predicates = [
        (
            ComparisonMatch("rating", "gte", 4),
            lambda row: row["rating"] is not None and row["rating"] >= 4,
        ),
        (ValueMatch("title", "Alpha"), lambda row: row["title"] == "Alpha"),
        (ComparisonMatch("rating", "is_null"), lambda row: row["rating"] is None),
    ]
    for step in range(45):
        for node, oracle in predicates:
            expected = tuple(sorted(row["id"] for row in source if oracle(row)))
            query = CacheQuery("books", where=node)
            assert cache.query(query).ids == expected, (seed, step, node)
            assert cache.query(query).ids == expected
        row_id = rng.randrange(1, 16)
        source[:] = [row for row in source if row["id"] != row_id]
        if rng.random() > 0.25:
            source.append(
                {
                    "id": row_id,
                    "title": rng.choice(("Alpha", "Changed", "Café")),
                    "rating": rng.choice((None, 0, 3, 4, 5)),
                }
            )
        if step % 7 == 0:
            cache.invalidate(tables=("books",))
        else:
            cache.invalidate(ids={"books": (row_id,)})
    assert cache.query(CacheQuery("books")).ids == tuple(
        sorted(row["id"] for row in source)
    )


@pytest.mark.parametrize("operation", ("clear", "close"))
def test_failed_destructive_lifecycle_never_leaves_ready_cache(
    modern_cache, monkeypatch, operation
):
    cache, _ = modern_cache
    cache.query(CacheQuery("books", where=ValueMatch("title", "Alpha")))
    before = cache.generation
    original = getattr(cache.storage, operation)

    def partial_failure():
        original()
        raise RuntimeError("injected after backend mutation")

    with monkeypatch.context() as patch:
        patch.setattr(cache.storage, operation, partial_failure)
        with pytest.raises(RuntimeError, match="injected"):
            getattr(cache, operation)()
        assert cache.state == CacheState.DIRTY
        assert cache.generation > before
        assert not cache._query_engine._results
        assert cache._pending_query_results is None
        patch.setattr(
            cache.storage,
            "reload",
            lambda: (_ for _ in ()).throw(RuntimeError("offline")),
        )
        with pytest.raises(CacheDirtyError):
            cache.query(CacheQuery("books"))
    cache.close()


def test_failed_membership_repair_falls_back_to_fresh_query(modern_cache, monkeypatch):
    cache, db = modern_cache
    query = CacheQuery("books", where=ValueMatch("title", "Alpha"))
    assert cache.query(query).ids == (2,)
    db.driver_wrapper._rows_by_table["books"][1]["title"] = "Updated"
    cache.invalidate(ids={"books": (2,)})
    monkeypatch.setattr(
        cache._query_engine,
        "repair_results",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("repair failed")),
    )
    assert cache.query(query).ids == ()
    assert cache.state == CacheState.READY


def test_failed_initial_load_can_recover_uninitialized_storage(
    modern_cache, monkeypatch
):
    cache, _ = modern_cache
    cache.clear()

    def fail():
        cache.storage.clear()
        raise RuntimeError("read failed after clearing storage")

    with monkeypatch.context() as patch:
        patch.setattr(cache.storage, "read", fail)
        with pytest.raises(RuntimeError, match="read failed"):
            cache.load()
        assert cache.state == CacheState.DIRTY
    assert cache.query(CacheQuery("books")).ids == (1, 2, 3)
    assert cache.state == CacheState.READY


@pytest.mark.parametrize(
    "modern_cache", ("schema_backed", "numpy_vectorized"), indirect=True
)
@pytest.mark.parametrize(
    "method,dependency",
    (
        ("invalidate_table", "table"),
        ("reload_main_table", "table"),
        ("invalidate_ids", "ids"),
        ("reload_ids", "ids"),
        ("invalidate_field", "field"),
        ("reload_field", "field"),
        ("invalidate_link_table", "link"),
        ("reload_link_table", "link"),
    ),
)
def test_partial_dependency_failures_require_complete_recovery(
    modern_cache, monkeypatch, method, dependency
):
    cache, db = modern_cache
    query = CacheQuery("books", where=ValueMatch("title", "Alpha"))
    assert cache.query(query).ids == (2,)
    db.driver_wrapper._rows_by_table["books"][1]["title"] = "Updated"
    kwargs = {
        "table": {"tables": ("books",)},
        "ids": {"ids": {"books": (2,)}},
        "field": {"fields": ("books.title",)},
        "link": {"links": (("books", "tags"),)},
    }[dependency]
    original = getattr(cache.storage, method)

    def fail(*args, **kw):
        original(*args, **kw)
        raise RuntimeError("after partial dependency mutation")

    with monkeypatch.context() as patch:
        patch.setattr(cache.storage, method, fail)
        if method.startswith("invalidate"):
            with pytest.raises(RuntimeError, match="partial"):
                cache.invalidate(**kwargs)
        else:
            cache.invalidate(**kwargs)
            with pytest.raises(CacheDirtyError):
                cache.query(query)
        assert cache.state == CacheState.DIRTY
        assert cache._full_reload_required
        assert not cache._query_engine._results
    assert cache.query(query).ids == ()
    assert cache.state == CacheState.READY


@pytest.mark.parametrize(
    "modern_cache", ("schema_backed", "numpy_vectorized"), indirect=True
)
def test_readers_cannot_observe_partial_refresh(modern_cache, monkeypatch):
    cache, db = modern_cache
    query = CacheQuery("books", where=ValueMatch("title", "Alpha"))
    assert cache.query(query).ids == (2,)
    db.driver_wrapper._rows_by_table["books"][1]["title"] = "Updated"
    cache.invalidate(ids={"books": (2,)})
    entered, release, reader_started = Event(), Event(), Event()
    original = cache.storage.reload_ids

    def paused_refresh(*args, **kwargs):
        entered.set()
        assert release.wait(10), "test reader did not release refresh"
        return original(*args, **kwargs)

    def second_reader():
        reader_started.set()
        return cache.query(query)

    monkeypatch.setattr(cache.storage, "reload_ids", paused_refresh)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(cache.query, query)
        try:
            assert entered.wait(10)
            second = pool.submit(second_reader)
            assert reader_started.wait(10)
            with pytest.raises(TimeoutError):
                second.result(timeout=0.05)
        finally:
            release.set()
        assert first.result(timeout=10).ids == second.result(timeout=10).ids == ()


def test_mutable_query_operands_do_not_poison_membership_cache():
    engine = ProjectionQueryEngine(
        "rows", {i: {"id": i, "value": i} for i in range(8)}, ("id", "value")
    )
    operand = [1, 2]
    query = CacheQuery("rows", where=ComparisonMatch("value", "in", operand))
    assert engine.query(query, generation=1).ids == (1, 2)
    operand[:] = [4]
    assert engine.query(query, generation=1).ids == (4,)
    operand[:] = [1, 2]
    assert engine.query(query, generation=1).ids == (1, 2)
    saved = engine.row_local_results()
    operand[:] = [7]
    engine.reset()
    engine.repair_results(saved, {"rows": {1}}, generation=2)
    assert engine.query(
        CacheQuery("rows", where=ComparisonMatch("value", "in", [1, 2])), generation=2
    ).ids == (1, 2)


@pytest.mark.parametrize("changed_count,expected_repairs", ((25, 2), (26, 0)))
def test_repair_budget_counts_changed_ids_times_saved_queries(
    changed_count, expected_repairs, monkeypatch
):
    rows = {i: {"id": i, "value": i % 3} for i in range(1, 2001)}
    engine = ProjectionQueryEngine("rows", rows, ("id", "value"))
    queries = [
        CacheQuery("rows", where=ValueMatch("value", value), limit=0)
        for value in (0, 1)
    ]
    for query in queries:
        engine.query(query, generation=1)
    saved = engine.row_local_results()
    changed = set(range(1, changed_count + 1))
    for row_id in changed:
        engine._rows[row_id]["value"] = 0
    reads = []
    original = engine.get_row_snapshot

    def observe(row_id):
        reads.append(row_id)
        return original(row_id)

    monkeypatch.setattr(engine, "get_row_snapshot", observe)
    engine.reset()
    engine.repair_results(saved, {"rows": changed}, generation=2)
    assert engine.repaired_results == expected_repairs
    # Future vectorized implementations may need fewer row snapshots.
    assert len(reads) <= changed_count * expected_repairs
    assert set(reads) <= changed
    for query in queries:
        expected = sum(
            row["value"] == query.where.value for row in engine._rows.values()
        )
        assert engine.query(query, generation=2).total_count == expected


def test_warm_membership_does_not_evaluate_rows_again(monkeypatch):
    engine = ProjectionQueryEngine(
        "rows", {i: {"id": i, "value": i} for i in range(1000)}, ("id", "value")
    )
    query = CacheQuery("rows", where=ComparisonMatch("value", "gte", 500), limit=0)
    assert engine.query(query, generation=1).total_count == 500
    monkeypatch.setattr(
        engine, "get_row_snapshot", lambda _: pytest.fail("warm membership read a row")
    )
    assert engine.query(query, generation=1).total_count == 500
    assert engine.result_hits == 1
