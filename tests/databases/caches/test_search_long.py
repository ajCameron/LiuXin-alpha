"""Long, reproducible mutation, recovery and Calibre query-churn workloads."""

from __future__ import annotations

import random
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from unittest.mock import patch

import pytest

from LiuXin_alpha.caches.api import CacheQuery, CacheSort, CacheState
from LiuXin_alpha.caches.read_cache import ReadCache
from LiuXin_alpha.databases.db_types.query_types import ComparisonMatch
from LiuXin_alpha.databases.schema_specs import StorageSchemaSpec
from LiuXin_alpha.metadata.calibre_conversion import CalibreBookRef
from LiuXin_alpha.utils.calibre_compat.caches.search.adapter import (
    CalibreSearchAdapter,
    SearchDefinitions,
)
from tests.databases.caches.test_search_stress import _tree
from tests.support.cache_long_runs import BACKENDS, long_marks
from tests.support.cache_long_runs import measurements as measurements
from tests.support.cache_long_runs import workload as workload
from tests.support.storage_cache_test_harness import make_fake_db, make_table

pytestmark = long_marks()


@pytest.mark.parametrize("storage_type", BACKENDS)
def test_native_mutation_recovery_soak(storage_type, workload, measurements):
    """Check every mutation, including the final one, against independent scans."""
    rng = random.Random(workload.seed)
    source = {
        i: {"id": i, "number": i % 11 - 5, "title": f"Book {i}", "items": (1,)}
        for i in range(1, workload.records + 1)
    }
    columns = ("id", "number", "title", "items")
    db = make_fake_db(
        StorageSchemaSpec(
            tables={"rows": make_table("rows", columns, is_main_table=True)},
            interlinks=(),
            intralinks=(),
        ),
        {"rows": list(source.values())},
    )
    cache = ReadCache(
        db, storage_type=storage_type, plan_cache_size=13, result_cache_size=7
    )
    try:
        measurements.measure("load", cache.load)
        for step in range(workload.steps):
            row_id = rng.randint(1, workload.records + 10)
            source.pop(row_id, None)
            if step % 4:
                source[row_id] = {
                    "id": row_id,
                    "number": rng.choice((None, *range(-5, 6))),
                    "title": rng.choice((None, "Book Alpha", "Café", "Straße", "%_")),
                    "items": rng.choice((None, (), (1,), (1, 2, 3))),
                }
            db.driver_wrapper._rows_by_table["rows"] = list(source.values())
            generation = cache.generation
            if step % 7 == 0:
                cache.invalidate(tables=("rows",))
            else:
                cache.invalidate(ids={"rows": (row_id,)})
            assert cache.generation > generation

            # Repeat a destructive partial failure and recovery over the same owner.
            if step % 37 == 0:
                clear = cache.storage.clear

                def partial_failure(clear=clear):
                    clear()
                    raise RuntimeError("long-run injected clear failure")

                with patch.object(cache.storage, "clear", partial_failure):
                    with pytest.raises(RuntimeError, match="injected"):
                        cache.clear()
                assert cache.state == CacheState.DIRTY
                assert not cache._query_engine._results

            node, oracle = _tree(rng)
            expected = tuple(sorted(i for i, row in source.items() if oracle(row)))
            query = CacheQuery("rows", where=node)
            actual = measurements.measure("changed_query", cache.query, query)
            assert actual.ids == expected, (workload.seed, step, node)
            page = measurements.measure(
                "repeated_sorted_page",
                cache.query,
                replace(query, sort=(CacheSort("id", False),), offset=2, limit=11),
            )
            assert page.ids == tuple(reversed(expected))[2:13], (workload.seed, step)
            assert page.total_count == len(expected)
            # Keep a fixed predicate hot across mutations as well as churning trees.
            expected_nulls = tuple(
                sorted(i for i, r in source.items() if r["number"] is None)
            )
            assert (
                cache.query(
                    CacheQuery("rows", where=ComparisonMatch("number", "is_null"))
                ).ids
                == expected_nulls
            )
            engine = cache._query_engine
            assert len(engine._plans) <= 13 and len(engine._results) <= 7
            assert tuple(engine._results) == tuple(engine._result_queries)
            if step % 97 == 0:
                cache.clear()
                measurements.measure("reload_after_clear", cache.load)
                assert cache.query(CacheQuery("rows")).ids == tuple(sorted(source))
        assert cache.query(CacheQuery("rows")).ids == tuple(sorted(source))
    finally:
        cache.close()
    assert cache.state == CacheState.CLOSED
    assert not cache._query_engine._results


def test_calibre_query_definition_and_candidate_soak(workload, measurements):
    """Churn bounded caches and definitions; concurrent requests have distinct scopes."""
    rng = random.Random(workload.seed)
    rows = {
        CalibreBookRef(i): {
            "title": f"Book {i}",
            "rating": 2 * (i % 6),
            "tags": ("fiction", "space") if i % 2 else ("history",),
        }
        for i in range(1, workload.records + 1)
    }
    definitions = SearchDefinitions({})
    adapter = measurements.measure(
        "construction",
        CalibreSearchAdapter,
        rows,
        definitions=definitions,
        plan_cache_size=13,
        result_cache_size=7,
    )
    terms = (
        ("tags:fiction", lambda row: "fiction" in row["tags"]),
        ("rating:>=3", lambda row: row["rating"] >= 6),
        ('title:"~[135]$"', lambda row: row["title"][-1] in "135"),
        ("tags:#=2", lambda row: len(row["tags"]) == 2),
    )
    with ThreadPoolExecutor(max_workers=4) as pool:
        for step in range(workload.steps):
            a, b, c = (rng.choice(terms) for _ in range(3))
            query = f"({a[0]} or {b[0]}) and not ({c[0]})"
            selected = frozenset(ref for ref in rows if (ref.expression_id + step) % 3)
            expected = frozenset(
                ref
                for ref in selected
                if (a[1](rows[ref]) or b[1](rows[ref])) and not c[1](rows[ref])
            )
            for phase in ("candidate_query", "warm_query"):
                assert (
                    measurements.measure(
                        phase, adapter.search, query, references=selected
                    )
                    == expected
                ), (workload.seed, step, query)
            saved_term = terms[(step // 11) % len(terms)]
            if step % 11 == 0:
                definitions.set("saved_searches", "rotating", saved_term[0])
            assert adapter.search("search:rotating", references=selected) == frozenset(
                ref for ref in selected if saved_term[1](rows[ref])
            )
            if step % 97 == 0:
                definitions.delete("saved_searches", "rotating")
                definitions.set("saved_searches", "rotating", "title:absent")
                assert adapter.search("search:rotating", references=selected) == set()
                definitions.set("saved_searches", "rotating", saved_term[0])
            if step % 29 == 0:
                scopes = (
                    selected,
                    frozenset(rows) - selected,
                    frozenset(),
                    frozenset(rows),
                )
                futures = [
                    pool.submit(adapter.search, query, references=s) for s in scopes
                ]
                for scope, future in zip(scopes, futures, strict=True):
                    assert future.result(timeout=60) == frozenset(
                        ref
                        for ref in scope
                        if (a[1](rows[ref]) or b[1](rows[ref])) and not c[1](rows[ref])
                    ), (workload.seed, step)
            assert adapter.search(f"id:{step + 1}") == (
                {CalibreBookRef(step + 1)} if step < workload.records else set()
            )
            assert len(adapter._plans) <= 13 and len(adapter._results) <= 7
