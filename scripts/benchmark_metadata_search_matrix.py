"""Reproducible correctness-checked metadata search baseline before optimization.

Run with the project virtualenv. Timings exclude oracle assertions and do not
enforce machine-dependent thresholds. Synthetic copied metadata excludes database
capture, disk I/O, GUI work, and input/import allocations. Each scale is isolated
in a subprocess so earlier runs cannot retain allocator or cache state.
"""

from __future__ import annotations

import argparse
import json
import math
import platform
import statistics
import subprocess
import sys
import time
import tracemalloc

from LiuXin_alpha.caches.api import CacheQuery, CacheSort
from LiuXin_alpha.caches.projection import ProjectionQueryEngine
from LiuXin_alpha.databases.db_types.query_types import ComparisonMatch
from LiuXin_alpha.metadata.calibre_conversion import CalibreBookRef
from LiuXin_alpha.utils.calibre_compat.caches.search.adapter import CalibreSearchAdapter


def timed(operation, expected, repeats):
    samples = []
    for _ in range(repeats):
        start = time.perf_counter()
        actual = operation()
        samples.append(time.perf_counter() - start)
        assert actual == expected, (actual, expected)
    return {
        "median_seconds": statistics.median(samples),
        "p95_seconds": sorted(samples)[math.ceil(len(samples) * 0.95) - 1],
        "samples_seconds": samples,
    }


def constructed(factory):
    tracemalloc.start()
    start = time.perf_counter()
    result = factory()
    elapsed = time.perf_counter() - start
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return result, {
        "seconds_with_tracing": elapsed,
        "retained_traced_bytes": current,
        "peak_traced_bytes": peak,
    }


def measure(records, repeats):
    rows = {
        i: {
            "id": i,
            "title": f"Book {i}",
            "sort": f"Book {i}",
            "rating": 2 * (i % 6),
            "tags": ("fiction", "space") if i % 2 else ("history",),
        }
        for i in range(1, records + 1)
    }
    refs = {CalibreBookRef(i): row for i, row in rows.items()}
    adapter, adapter_construction = constructed(lambda: CalibreSearchAdapter(refs))
    workloads = [
        ("all", "", lambda i: True),
        ("absent", "title:missing", lambda i: False),
        ("numeric", "rating:>=3", lambda i: i % 6 >= 3),
        ("selective_id", f"id:{records}", lambda i: i == records),
        ("collection_count", "tags:#=2", lambda i: i % 2 == 1),
        ("regex", 'title:"~9$"', lambda i: i % 10 == 9),
        (
            "boolean",
            'tags:fiction and rating:>=3 and not title:"~9$"',
            lambda i: i % 2 == 1 and i % 6 >= 3 and i % 10 != 9,
        ),
    ]
    results = {}
    for name, query, accepts in workloads:
        expected = frozenset(ref for ref in refs if accepts(ref.expression_id))

        def cold(query=query):
            adapter._plans.clear()
            adapter._results.clear()
            return adapter.search(query)

        results[name] = {
            "matched": len(expected),
            "cold": timed(cold, expected, repeats),
            "warm": timed(lambda query=query: adapter.search(query), expected, repeats),
        }
    # Query churn must evict bounded caches; a hot entry must still be correct.
    for i in range(200):
        assert adapter.search(f"id:{i}") == (
            frozenset({CalibreBookRef(i)}) if i in rows else frozenset()
        )
    assert len(adapter._plans) <= 100 and len(adapter._results) <= 50
    results["churn"] = {"plans": len(adapter._plans), "results": len(adapter._results)}
    engine, native_construction = constructed(
        lambda: ProjectionQueryEngine("books", rows, next(iter(rows.values())))
    )
    query = CacheQuery("books", where=ComparisonMatch("rating", "gte", 6), limit=0)
    expected = sum(row["rating"] >= 6 for row in rows.values())

    def native_cold():
        engine.reset()
        return engine.query(query, generation=1).total_count

    native = {
        "cold": timed(native_cold, expected, repeats),
        "warm": timed(
            lambda: engine.query(query, generation=1).total_count, expected, repeats
        ),
    }
    page = CacheQuery(
        "books", where=query.where, sort=(CacheSort("id", False),), offset=5, limit=20
    )
    expected_page = tuple(i for i in reversed(rows) if rows[i]["rating"] >= 6)[5:25]
    native["sorted_page"] = timed(
        lambda: engine.query(page, generation=1).ids, expected_page, repeats
    )
    saved = engine.row_local_results()
    repair = []
    generation = 1
    for changed_count in (1, 10, 50, 51):
        changed = set(range(1, min(records, changed_count) + 1))
        for i in changed:
            engine._rows[i]["rating"] = 10
        generation += 1
        engine.reset()
        before = engine.repaired_results
        start = time.perf_counter()
        engine.repair_results(saved, {"books": changed}, generation=generation)
        elapsed = time.perf_counter() - start
        count = engine.query(query, generation=generation).total_count
        assert count == sum(row["rating"] >= 6 for row in engine._rows.values())
        repair.append(
            {
                "changed_rows": len(changed),
                "seconds": elapsed,
                "repaired_queries": engine.repaired_results - before,
            }
        )
        saved = engine.row_local_results()
    return {
        "records": records,
        "repeats": repeats,
        "adapter_construction": adapter_construction,
        "native_construction": native_construction,
        "adapter": results,
        "native": native,
        "repair": repair,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scales", type=int, nargs="+", default=[1000, 10000, 100000])
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--worker", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if (
        any(scale <= 0 for scale in args.scales)
        or args.repeats <= 0
        or args.worker is not None
        and args.worker <= 0
    ):
        parser.error("scales, repeats and worker size must be positive")
    if args.worker is not None:
        print(json.dumps(measure(args.worker, args.repeats)))
        return
    runs = []
    for scale in args.scales:
        result = subprocess.run(
            [
                sys.executable,
                __file__,
                "--worker",
                str(scale),
                "--repeats",
                str(args.repeats),
            ],
            check=True,
            text=True,
            capture_output=True,
        )
        runs.append(json.loads(result.stdout))
        print(f"Verified {scale:,} records", file=sys.stderr, flush=True)
    print(
        json.dumps(
            {
                "schema_version": 1,
                "python": platform.python_version(),
                "platform": platform.platform(),
                "runs": runs,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
