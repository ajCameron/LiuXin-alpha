"""Measure native cache pagination with warm membership and independent oracles.

Each scale runs in a fresh process. Input construction, imports, predicate-cache
priming and oracle checks are outside timings. Allocation measurements are
separate from untraced timings; no machine-dependent thresholds are enforced.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import platform
import random
import statistics
import subprocess
import sys
import time
import tracemalloc
import unicodedata
from pathlib import Path

from LiuXin_alpha.caches.api import CacheQuery, CacheSort
from LiuXin_alpha.caches.projection import ProjectionQueryEngine


def measure(size, repeats):
    rng = random.Random(20261117)
    ids = list(range(1, size + 1))
    rng.shuffle(ids)
    rows = {
        i: {
            "id": i,
            "score": None if i % 17 == 0 else rng.randrange(1000) / 10,
            "title": (None, "Café", "Cafe\u0301", "Ｓtraße", "STRASSE", "Book")[i % 6],
        }
        for i in ids
    }
    engine = ProjectionQueryEngine("rows", rows, ("id", "score", "title"))
    engine.query(CacheQuery("rows", limit=0), generation=1)
    multi = (CacheSort("score", False), CacheSort("title"))
    cases = (
        ("count_only", multi, 0, 0),
        ("past_end", multi, size + 1, 20),
        ("default_first", (), 0, 20),
        ("id_descending", (CacheSort("id", False),), 5, 20),
        ("numeric_ascending", (CacheSort("score"),), 5, 20),
        ("numeric_descending", (CacheSort("score", False),), 5, 20),
        ("text_descending", (CacheSort("title", False),), 5, 20),
        ("multi_first", multi, 5, 20),
        ("multi_middle", multi, size // 2, 20),
        ("multi_tail", multi, size - 25, 20),
        ("multi_unlimited_tail", multi, size - 25, None),
        ("multi_full", multi, 0, None),
    )
    results = {}
    for name, specs, offset, limit in cases:
        # Independent stable reference ordering over source rows, never engine keys.
        expected = sorted(rows)
        for spec in reversed(specs):
            present = [i for i in expected if rows[i][spec.field] is not None]
            missing = [i for i in expected if rows[i][spec.field] is None]

            def key(i, field=spec.field):
                value = rows[i][field]
                return (
                    (unicodedata.normalize("NFKC", value).casefold(), value)
                    if isinstance(value, str)
                    else value
                )

            present.sort(key=key, reverse=not spec.ascending)
            expected = present + missing
        expected = tuple(expected[offset : None if limit is None else offset + limit])
        query = CacheQuery(
            "rows", sort=specs, offset=offset, limit=limit, projection=("title",)
        )

        def check(result, expected=expected, name=name):
            assert result.ids == expected, name
            assert result.total_count == size, name
            assert all(
                record["title"] == rows[record.row_id]["title"]
                for record in result.records
            )

        check(engine.query(query, generation=1))
        samples = []
        for _ in range(repeats):
            start = time.perf_counter()
            result = engine.query(query, generation=1)
            samples.append(time.perf_counter() - start)
            check(result)
        tracemalloc.start()
        result = engine.query(query, generation=1)
        retained, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        check(result)
        results[name] = {
            "page_size": len(expected),
            "total_count": size,
            "median_seconds": statistics.median(samples),
            "p95_seconds": sorted(samples)[math.ceil(repeats * 0.95) - 1],
            "samples_seconds": samples,
            "retained_traced_bytes": retained,
            "peak_traced_bytes": peak,
        }
    return {"records": size, "repeats": repeats, "cases": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scales", type=int, nargs="+", default=[1000, 10000, 100000])
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--worker", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if (
        args.repeats < 1
        or any(n < 100 for n in args.scales)
        or (args.worker is not None and args.worker < 100)
    ):
        parser.error("scales must be at least 100 and repeats must be positive")
    if args.worker is not None:
        print(json.dumps(measure(args.worker, args.repeats)))
        return
    hashes = {
        name: hashlib.sha256(path.read_bytes()).hexdigest()
        for name, path in {
            "scripts/benchmark_cache_paging.py": Path(__file__).resolve(),
            "src/LiuXin_alpha/caches/query.py": Path(
                sys.modules["LiuXin_alpha.caches.query"].__file__
            ),
            "src/LiuXin_alpha/caches/projection.py": Path(
                sys.modules[ProjectionQueryEngine.__module__].__file__
            ),
        }.items()
    }
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
                "source_sha256": hashes,
                "runs": runs,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
