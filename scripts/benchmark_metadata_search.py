"""Measure copied metadata search and bounded native predicate repair locally.

Run with the project virtualenv. This uses synthetic metadata, makes no database
writes and is not a GUI, database-capture or whole-library memory benchmark.
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import time
import tracemalloc

from LiuXin_alpha.caches.api import CacheQuery
from LiuXin_alpha.caches.projection import ProjectionQueryEngine
from LiuXin_alpha.databases.db_types.query_types import ComparisonMatch
from LiuXin_alpha.metadata.calibre_conversion import CalibreBookRef
from LiuXin_alpha.utils.calibre_compat.caches.search.adapter import CalibreSearchAdapter


def measure(records: int, repeats: int) -> dict:
    """Return timings and peak traced allocation for one deterministic workload."""
    rows = {
        i: {
            "id": i,
            "title": f"Book {i}",
            "rating": 2 * (i % 6),
            "tags": ("fiction", "space") if i % 2 else ("history",),
        }
        for i in range(1, records + 1)
    }
    tracemalloc.start()
    start = time.perf_counter()
    adapter = CalibreSearchAdapter({CalibreBookRef(i): row for i, row in rows.items()})
    construction = time.perf_counter() - start
    query = 'tags:fiction and rating:>=3 and not title:"~9$"'
    start = time.perf_counter()
    expected = adapter.search(query)
    cold = time.perf_counter() - start
    warm = []
    for _ in range(repeats):
        start = time.perf_counter()
        assert adapter.search(query) == expected
        warm.append(time.perf_counter() - start)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    engine = ProjectionQueryEngine("books", rows, ("id", "title", "rating", "tags"))
    native = CacheQuery("books", where=ComparisonMatch("rating", "gte", 6), limit=0)
    engine.query(native, generation=1)
    saved = engine.row_local_results()
    changed = set(range(1, min(records, 10) + 1))
    # Benchmark harness mutates the private copied rows to model completed refresh.
    for i in changed:
        engine._rows[i]["rating"] = 10
    engine.reset()
    start = time.perf_counter()
    engine.repair_results(saved, {"books": changed}, generation=2)
    repair = time.perf_counter() - start
    repaired = engine.query(native, generation=2).total_count
    assert repaired == sum(row["rating"] >= 6 for row in engine._rows.values())
    return {
        "python": platform.python_version(),
        "records": records,
        "repeats": repeats,
        "construction_seconds": construction,
        "cold_seconds": cold,
        "warm_median_seconds": statistics.median(warm),
        "matched": len(expected),
        "peak_traced_bytes": peak,
        "changed_rows": len(changed),
        "membership_repair_seconds": repair,
        "repaired_query_count": engine.repaired_results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, default=10000)
    parser.add_argument("--repeats", type=int, default=20)
    args = parser.parse_args()
    if args.records <= 0 or args.repeats <= 0:
        parser.error("records and repeats must be positive")
    print(json.dumps(measure(args.records, args.repeats), indent=2))


if __name__ == "__main__":
    main()
