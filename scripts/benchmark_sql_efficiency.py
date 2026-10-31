#!/usr/bin/env python3
"""Measure physical SQL paths on an isolated, reproducible WEMI catalogue.

Run with the repository interpreter and an output path outside the data checkout::

    .venv/bin/python scripts/benchmark_sql_efficiency.py \
        --works 50000 --iterations 3 --output /tmp/sql-efficiency.json

Each scenario runs in a fresh process. Timings exclude startup, fixture creation,
warmup, tracing and memory instrumentation. Separate invocations measure Python
allocation peaks and SQL statement counts. RSS includes interpreter/DB startup.
Reference scenarios use current scalar/materializing/fallback APIs, not a checkout
of a historical release. Writes roll back; only this script's temporary directory
is removed. No application catalogue or external file collection is opened.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import resource
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
import tracemalloc
from contextlib import redirect_stdout
from datetime import UTC, datetime
from pathlib import Path

SCENARIOS = (
    "page_offset",
    "page_keyset",
    "page_materialized",
    "graph_batch",
    "graph_scalar",
    "title_index",
    "title_fallback",
    "identifier_index",
    "identifier_fallback",
    "shared_wemi",
    "insert_batch",
    "insert_scalar",
)


def _open(path: Path, *, create: bool = False):
    from LiuXin_alpha.databases.database import Database

    return Database(
        metadata={"database_path": str(path)},
        db_type="SQLite_apsw",
        create=create,
        enable_storage_manager=False,
        enable_maintenance=False,
    )


def _build(path: Path, count: int) -> dict:
    """Populate through raw SQL for setup only; measured writes use public macros."""
    from LiuXin_alpha.databases.search_functions import register_search_functions

    started = time.perf_counter()
    with _open(path, create=True):
        pass
    with sqlite3.connect(path) as conn:
        register_search_functions(conn)
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executemany(
            "INSERT INTO works (work_id, work_title) VALUES (?, ?)",
            ((i, f"Benchmark Work {i:08d}") for i in range(1, count + 1)),
        )
        conn.executemany(
            "INSERT INTO expressions (expression_id, expression_label) VALUES (?, ?)",
            ((i, f"Expression {i}") for i in range(1, count + 1)),
        )
        conn.executemany(
            "INSERT INTO manifestations (manifestation_id, manifestation_edition_statement) VALUES (?, ?)",
            ((i, f"Novel and introduction {i}") for i in range(1, count // 2 + 1)),
        )
        conn.executemany(
            "INSERT INTO items (item_id, item_manifestation_id) VALUES (?, ?)",
            ((i, (i + 1) // 2) for i in range(1, count + 1)),
        )
        conn.executemany(
            "INSERT INTO expression_work_links (expression_work_link_work_id, expression_work_link_expression_id, expression_work_link_priority) VALUES (?, ?, ?)",
            ((i, i, 1) for i in range(1, count + 1)),
        )
        conn.executemany(
            "INSERT INTO expression_work_links (expression_work_link_work_id, expression_work_link_expression_id, expression_work_link_priority) VALUES (?, 1, 2)",
            ((i,) for i in range(100, count + 1, 100)),
        )
        conn.executemany(
            "INSERT INTO expression_manifestation_links (expression_manifestation_link_expression_id, expression_manifestation_link_manifestation_id, expression_manifestation_link_priority) VALUES (?, ?, 1)",
            ((i, (i + 1) // 2) for i in range(1, count + 1)),
        )
        conn.executemany(
            "INSERT INTO entity_identifiers (entity_identifier_entity_type, entity_identifier_entity_id, entity_identifier_scheme, entity_identifier_value) VALUES ('work', ?, 'doi', ?)",
            ((i, f"10.1000/bench-{i:08d}") for i in range(1, count + 1)),
        )
        assert not conn.execute("PRAGMA foreign_key_check").fetchall()
        conn.execute("ANALYZE")
    return {
        "works": count,
        "expressions": count,
        "manifestations": count // 2,
        "items": count,
        "identifiers": count,
        "shared_expression_links": count // 100,
        "database_bytes": path.stat().st_size,
        "setup_seconds": time.perf_counter() - started,
    }


def _scenario(db, name: str, count: int, batch: int):
    from LiuXin_alpha.catalog.catalog import Catalog

    catalog, macros = Catalog(db), db.macros
    middle = count // 2
    owners = tuple(range(1, min(count, 1000) + 1))
    spec = db.driver_wrapper.get_link_spec("works", "expressions")
    if name.endswith("_fallback"):
        db.driver.supports_normalized_search = False

    def run():
        if name.startswith("page_"):
            if name == "page_offset":
                rows = catalog.works.list(limit=100, offset=middle)
            elif name == "page_keyset":
                rows = catalog.works.list_after(middle, limit=100)
            else:
                rows = catalog.works._all_rows()[middle : middle + 100]
            return tuple(row["work_id"] for row in rows)
        if name.startswith("graph_"):
            groups = (
                macros.get_linked_rows_bulk(spec, owners)
                if name == "graph_batch"
                else {owner: macros.get_linked_rows(spec, owner) for owner in owners}
            )
            return tuple(
                (owner, row.row["expression_id"])
                for owner in owners
                for row in groups[owner]
            )
        if name.startswith("title_"):
            return tuple(
                row["work_id"]
                for row in catalog.works.find_by_title(f"benchmark work {count:08d}")
            )
        if name.startswith("identifier_"):
            row = catalog.identifiers.find(
                identifier_type="doi", value=f"10.1000/BENCH-{count:08d}"
            )
            assert row is not None
            return (row["entity_identifier_entity_id"],)
        if name == "shared_wemi":
            rows = catalog.search.find(
                f'work.title:"=benchmark work {count - 1:08d}"', level="item"
            )
            found = tuple(row["item_id"] for row in rows)
            assert found == (count - 1, count)
            return found

        class Undo(Exception):
            pass

        values = [{"tag": f"Benchmark inserted tag {i}"} for i in range(batch)]
        try:
            with macros.transaction():
                if name == "insert_batch":
                    macros.insert_rows("tags", values)
                else:
                    for row in values:
                        macros.insert_row("tags", row)
                result = macros.aggregate_rows("tags")["count"]
                assert result == batch
                raise Undo
        except Undo:
            pass
        assert macros.aggregate_rows("tags")["count"] == 0
        return (result,)

    return run


def _measure(path: Path, args) -> dict:
    with _open(path) as db:
        run = _scenario(db, args.scenario, args.works, args.batch)
        expected = run()  # Warm schema caches and the operation's working set.
        durations = []
        for _ in range(args.iterations):
            start = time.perf_counter()
            result = run()
            durations.append(time.perf_counter() - start)
            assert result == expected
        rss_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        tracemalloc.start()
        assert run() == expected
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # Count SQLite trace events, including metadata reads and any trigger
        # executions on newly opened driver connections. Connection initialization
        # itself is excluded; these counts are not just data SELECTs.
        statements = 0

        def trace(_sql):
            nonlocal statements
            statements += 1

        original = db.driver.get_connection

        def traced_connection():
            conn = original()
            conn.set_trace_callback(trace)
            return conn

        db.driver.get_connection = traced_connection
        db.driver.conn.set_trace_callback(trace)
        try:
            assert run() == expected
        finally:
            db.driver.get_connection = original
            db.driver.conn.set_trace_callback(None)
        return {
            "scenario": args.scenario,
            "seconds": durations,
            "median_seconds": statistics.median(durations),
            "process_peak_rss_kib": rss_kib,
            "operation_python_peak_bytes": peak,
            "sql_statements": statements,
            "result_count": len(expected),
            "result_sha256": hashlib.sha256(json.dumps(expected).encode()).hexdigest(),
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--works", type=int, default=50000)
    parser.add_argument("--batch", type=int, default=1000)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scenarios", nargs="+", choices=SCENARIOS, default=SCENARIOS)
    parser.add_argument("--scenario", choices=SCENARIOS, help=argparse.SUPPRESS)
    parser.add_argument("--database", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.works < 200 or args.works % 2 or args.batch < 1 or args.iterations < 1:
        parser.error(
            "works must be even and >=200; batch and iterations must be positive"
        )
    if args.output.exists():
        parser.error("output already exists; choose a new report path")
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root / "src"))
    with tempfile.TemporaryDirectory(prefix="liuxin-sql-benchmark-") as scratch:
        os.environ["LIUXIN_PREFS_DIR"] = str(Path(scratch) / "prefs")
        os.environ["LIUXIN_CONFIG_DIR"] = str(Path(scratch) / "config")
        with redirect_stdout(sys.stderr):
            if args.scenario:
                if args.database is None:
                    parser.error("worker scenario requires a database")
                report = _measure(args.database, args)
            else:
                path = Path(scratch) / "catalog.sqlite"
                dataset = _build(path, args.works)
                results = []
                for name in args.scenarios:
                    output = Path(scratch) / f"{name}.json"
                    subprocess.run(
                        [
                            sys.executable,
                            str(Path(__file__).resolve()),
                            "--works",
                            str(args.works),
                            "--batch",
                            str(args.batch),
                            "--iterations",
                            str(args.iterations),
                            "--scenario",
                            name,
                            "--database",
                            str(path),
                            "--output",
                            str(output),
                        ],
                        check=True,
                    )
                    result = json.loads(output.read_text())
                    results.append(result)
                    print(
                        f"{name}: {result['median_seconds']:.4f}s, {result['sql_statements']} statements",
                        file=sys.stderr,
                        flush=True,
                    )
                by_name = {row["scenario"]: row for row in results}
                for group in (
                    ("page_offset", "page_keyset", "page_materialized"),
                    ("graph_batch", "graph_scalar"),
                    ("title_index", "title_fallback"),
                    ("identifier_index", "identifier_fallback"),
                    ("insert_batch", "insert_scalar"),
                ):
                    if not all(name in by_name for name in group):
                        continue
                    assert (
                        len({by_name[name]["result_sha256"] for name in group}) == 1
                    ), group
                report = {
                    "created_at": datetime.now(UTC).isoformat(),
                    "python": platform.python_version(),
                    "platform": platform.platform(),
                    "backend": "SQLite_apsw driver (stdlib SQLite connections)",
                    "sqlite_version": sqlite3.sqlite_version,
                    "dataset": dataset,
                    "batch": args.batch,
                    "iterations": args.iterations,
                    "results": results,
                    "methodology": "Warm operation timings; separate SQLite trace-event counts (including metadata and triggers, excluding connection setup) and tracemalloc samples; process RSS includes startup. References are current APIs, not historical releases. Writes roll back.",
                }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x") as stream:
            json.dump(report, stream, indent=2)
            stream.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
