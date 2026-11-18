"""Measure bounded invalidation/refresh with warm native query membership.

The schema cache uses an indexed in-memory database double: timings include the
facade and backend repair, excluding database I/O, initial load and oracle checks.
Each scale runs in a new subprocess. Traced allocations are measured separately.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import statistics
import subprocess
import sys
import time
import tracemalloc
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from LiuXin_alpha.caches.api import CacheQuery  # noqa: E402
from LiuXin_alpha.caches.read_cache import ReadCache  # noqa: E402
from LiuXin_alpha.databases.db_types.query_types import ComparisonMatch  # noqa: E402
from LiuXin_alpha.databases.schema_specs import StorageSchemaSpec  # noqa: E402
from tests.support.storage_cache_test_harness import (  # noqa: E402
    FakeResultRow,
    make_fake_db,
    make_table,
)


def measure(size, repeats):
    results = {}
    for count in (1, 10, 50):
        for membership_changes in (False, True):
            rows = {i: {"id": i, "value": 1} for i in range(1, size + 1)}
            schema = StorageSchemaSpec(
                tables={
                    "rows": make_table("rows", ("id", "value"), is_main_table=True)
                },
                interlinks=(),
                intralinks=(),
            )
            db = make_fake_db(schema, {"rows": list(rows.values())})
            db.get_row_from_id = lambda table, row_id, rows=rows: FakeResultRow(
                rows[row_id]
            )
            cache = ReadCache(db, result_cache_size=50)
            cache.load()
            queries = [
                CacheQuery("rows", where=ComparisonMatch("value", "gte", -i), limit=0)
                for i in range(count)
            ]
            for query in queries:
                assert cache.query(query).total_count == size
            samples = []
            for repeat in range(repeats + 1):
                rows[1]["value"] = -100 if membership_changes and repeat % 2 == 0 else 2
                expected = size - int(rows[1]["value"] == -100)
                if repeat == repeats:
                    tracemalloc.start()
                start = time.perf_counter()
                cache.invalidate(ids={"rows": (1,)})
                result = cache.query(queries[0])
                elapsed = time.perf_counter() - start
                if repeat == repeats:
                    retained, peak = tracemalloc.get_traced_memory()
                    tracemalloc.stop()
                else:
                    samples.append(elapsed)
                assert result.total_count == expected
                assert all(
                    cache.query(query).total_count == expected for query in queries
                )
            results[
                f"{count}_queries_{'changed' if membership_changes else 'unchanged'}"
            ] = {
                "median_seconds": statistics.median(samples),
                "samples_seconds": samples,
                "peak_traced_bytes": peak,
                "retained_traced_bytes": retained,
            }
            cache.close()
    return {"records": size, "repeats": repeats, "cases": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scales", type=int, nargs="+", default=[1000, 10000, 100000])
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--worker", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.repeats < 1 or any(size < 2 for size in args.scales):
        parser.error("repeats must be positive and scales at least two")
    if args.worker is not None:
        print(json.dumps(measure(args.worker, args.repeats)))
        return
    runs = []
    for size in args.scales:
        run = subprocess.run(
            [
                sys.executable,
                __file__,
                "--worker",
                str(size),
                "--repeats",
                str(args.repeats),
            ],
            check=True,
            text=True,
            capture_output=True,
        )
        runs.append(json.loads(run.stdout))
        print(f"Verified {size:,} rows", file=sys.stderr, flush=True)
    paths = {
        "scripts/benchmark_cache_invalidation.py": Path(__file__),
        "src/LiuXin_alpha/caches/query.py": Path(
            sys.modules["LiuXin_alpha.caches.query"].__file__
        ),
        "src/LiuXin_alpha/caches/read_cache.py": Path(
            sys.modules["LiuXin_alpha.caches.read_cache"].__file__
        ),
        "src/LiuXin_alpha/caches/cache_plugins/schema_backed/storage_cache.py": ROOT
        / "src/LiuXin_alpha/caches/cache_plugins/schema_backed/storage_cache.py",
        "src/LiuXin_alpha/caches/cache_plugins/schema_backed/storage_tables/single_table.py": ROOT
        / "src/LiuXin_alpha/caches/cache_plugins/schema_backed/storage_tables/single_table.py",
    }
    print(
        json.dumps(
            {
                "python": platform.python_version(),
                "platform": platform.platform(),
                "source_sha256": {
                    name: hashlib.sha256(p.read_bytes()).hexdigest()
                    for name, p in paths.items()
                },
                "runs": runs,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
