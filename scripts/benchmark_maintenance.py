#!/usr/bin/env python3
"""Measure catalogue import, rebuild and durable jobs delivery on disposable data.

This is a synthetic metadata workload, not a file/SquashFS ingest benchmark. All
measured mutations use Catalog; lazy delivery uses Core's real jobs path. The
report distinguishes scheduled work, verified completion and timeout. SQLite
scratch lives under /tmp; PostgreSQL uses a fresh schema in the supplied test URL.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import resource
import shutil
import statistics
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, redirect_stdout
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4


def _populate(core, count, batch, label):
    from LiuXin_alpha.catalog.api.common import MetadataCandidate

    durations = []
    started = time.perf_counter()
    for offset in range(0, count, batch):
        tick = time.perf_counter()
        with core.database.macros.transaction(serialized=True):
            for index in range(offset, min(offset + batch, count)):
                work_id = core.catalog.works.create({"title": f"{label} Work {index}"})
                core.catalog.expressions.match_or_create(
                    work_id, MetadataCandidate({"label": f"{label} Expression {index}"})
                )
        durations.append(time.perf_counter() - tick)
    elapsed = time.perf_counter() - started
    return {
        "works": count,
        "seconds": elapsed,
        "works_per_second": count / elapsed,
        "transaction_seconds": durations,
        "transaction_median_seconds": statistics.median(durations),
        "transaction_max_seconds": max(durations),
    }


def _snapshot(core, scratch, phase, started):
    state = core.query("maintenance.status")
    with closing(core.database.driver.get_connection()) as connection:
        counts = {
            table: connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            for table in (
                "works",
                "maintenance_changes",
                "maintenance_obligations",
                "maintenance_projections",
            )
        }
        schema = getattr(core.database.driver, "schema", None)
        catalogue_bytes = (
            connection.execute(
                "SELECT COALESCE(SUM(pg_total_relation_size(quote_ident(schemaname)||'.'||quote_ident(tablename))),0) "
                "FROM pg_tables WHERE schemaname=?",
                (schema,),
            ).fetchone()[0]
            if schema
            else (scratch / "catalog.sqlite").stat().st_size
        )
    now_ms = time.time_ns() // 1_000_000
    return {
        "phase": phase,
        "elapsed_seconds": time.perf_counter() - started,
        "counts": counts,
        "catalogue_bytes": int(catalogue_bytes),
        "job_ledger_bytes": sum(p.stat().st_size for p in scratch.glob("jobs.sqlite*")),
        "scratch_bytes": sum(
            p.stat().st_size for p in scratch.rglob("*") if p.is_file()
        ),
        "pending": sum(row["pending"] for row in state.get("totals", [])),
        "oldest_pending_seconds": max(
            (
                max(0, now_ms - row["oldest_ms"]) / 1000
                for row in state.get("totals", [])
            ),
            default=0,
        ),
        "process_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "tmp_free_bytes": shutil.disk_usage("/tmp").free,
        "windows_free_bytes": shutil.disk_usage("/mnt/c").free
        if Path("/mnt/c").exists()
        else None,
        "rebuilds": state.get("rebuilds", []),
    }


def _verify(core):
    """Compare every live Work's projection against independently counted links."""
    with closing(core.database.driver.get_connection()) as connection:
        expected = connection.execute(
            "SELECT w.work_id,COUNT(DISTINCT l.expression_work_link_expression_id) "
            "FROM works w LEFT JOIN expression_work_links l ON l.expression_work_link_work_id=w.work_id "
            "GROUP BY w.work_id ORDER BY w.work_id"
        ).fetchall()
        stored = {
            row[0]: (json.loads(row[1]), row[2], row[3])
            for row in connection.execute(
                "SELECT s.projection_key,s.value_json,s.revision,r.revision "
                "FROM maintenance_projections s JOIN maintenance_projections r "
                "ON r.projection_key=s.projection_key AND r.plugin_id='catalog.work-revision' "
                "WHERE s.plugin_id='catalog.work-summary'"
            ).fetchall()
        }
    for work_id, count in expected:
        value, summary_revision, source_revision = stored[str(work_id)]
        assert value == {"deleted": False, "expression_count": count}
        assert summary_revision == source_revision
    assert len(stored) == len(expected)
    return {"works_checked": len(expected), "all_counts_and_revisions_match": True}


def run(args, scratch, emit):
    from LiuXin_alpha.core.factory import create_core

    schema, cleanup = None, None
    options = {
        "database_path": scratch / "catalog.sqlite",
        "db_type": args.backend,
        "enable_storage_manager": False,
        "enable_maintenance": False,
        "durable_job_path": scratch / "jobs.sqlite",
    }
    if args.backend == "PostgreSQL":
        from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
            connect_postgres,
        )

        url = os.environ["LIUXIN_TEST_POSTGRES_URL"]
        schema = "maintenance_bench_" + uuid4().hex
        options.update(
            database_path=url, database_metadata={"postgres_url": url, "schema": schema}
        )
        cleanup = connect_postgres({"postgres_url": url}, prompt_for_password=False)
    started = time.perf_counter()
    report = {
        "date": datetime.now(UTC).isoformat(),
        "backend": args.backend,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "workload": vars(args) | {"output": str(args.output)},
        "kind": "synthetic Catalog import with real durable jobs",
        "memory_scope": "application process; PostgreSQL server excluded",
        "snapshots": [],
        "complete": False,
    }

    def sample(core, phase):
        snapshot = _snapshot(core, scratch, phase, started)
        report["snapshots"].append(snapshot)
        emit(snapshot)
        return snapshot

    def deadline():
        if time.perf_counter() - started > args.max_seconds:
            raise TimeoutError(
                "Benchmark time budget exceeded; results remain incomplete"
            )

    try:
        with create_core(**options, create=True) as core:
            report["historical_import"] = _populate(
                core, args.historical, args.batch, "Historical"
            )
            core.command("maintenance.run", {"action": "install"})
            report["captured_import"] = _populate(
                core, args.works, args.batch, "Captured"
            )
            sample(core, "imported")
            first = core.command(
                "maintenance.run", {"action": "rebuild", "max_events": args.batch}
            )
            report["rebuild_run_uuid"] = first["run_uuid"]
        # A real close/reopen must resume the committed checkpoint.
        with create_core(**options) as core:
            while True:
                deadline()
                page = core.command(
                    "maintenance.run", {"action": "rebuild", "max_events": args.batch}
                )
                assert page["run_uuid"] == report["rebuild_run_uuid"]
                if page["scheduling_complete"]:
                    break
            sample(core, "rebuilt_scheduled")

            def concurrent_import():
                with create_core(**options) as writer:
                    return _populate(writer, args.concurrent, args.batch, "Concurrent")

            drain_started = time.perf_counter()
            with ThreadPoolExecutor(max_workers=1) as pool:
                producer = pool.submit(concurrent_import)
                passes = 0
                while True:
                    deadline()
                    result = core.command(
                        "maintenance.run",
                        {
                            "action": "drain",
                            "max_events": args.delivery_page,
                            "max_batches": args.drain_passes,
                        },
                    )
                    passes += result["job_passes"]
                    state = sample(core, "draining")
                    if producer.done():
                        report["concurrent_import"] = producer.result()
                        if state["pending"] == 0:
                            break
                report["drain"] = {
                    "seconds": time.perf_counter() - drain_started,
                    "job_passes": passes,
                }
            report["verification"] = _verify(core)
            sample(core, "before_pruning")
            pruned = 0
            while True:
                deadline()
                removed = core.command(
                    "maintenance.run", {"action": "prune", "max_events": 1000}
                )["removed"]
                pruned += removed
                if removed == 0:
                    break
            report["pruned_changes"] = pruned
            sample(core, "after_pruning")
            report["complete"] = True
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        if cleanup is not None:
            with cleanup, cleanup.cursor() as cursor:
                cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            cleanup.close()
        report["total_seconds"] = time.perf_counter() - started
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backend", choices=("SQLite", "SQLite_apsw", "PostgreSQL"), default="SQLite"
    )
    parser.add_argument("--historical", type=int, default=100)
    parser.add_argument("--works", type=int, default=100)
    parser.add_argument("--concurrent", type=int, default=20)
    parser.add_argument("--batch", type=int, default=50)
    parser.add_argument("--delivery-page", type=int, default=128)
    parser.add_argument("--drain-passes", type=int, default=25)
    parser.add_argument("--max-seconds", type=int, default=600)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if any(
        value < 1
        for value in (
            args.historical,
            args.works,
            args.concurrent,
            args.batch,
            args.delivery_page,
            args.drain_passes,
            args.max_seconds,
        )
    ):
        parser.error("work counts, page sizes and time budget must be positive")
    if args.batch > 500 or args.delivery_page > 500 or args.drain_passes > 1000:
        parser.error(
            "batch/delivery pages cannot exceed 500; drain passes cannot exceed 1000"
        )
    if args.output.exists():
        parser.error("output exists; choose a new path")
    if args.backend == "PostgreSQL" and not os.environ.get("LIUXIN_TEST_POSTGRES_URL"):
        parser.error(
            "PostgreSQL requires LIUXIN_TEST_POSTGRES_URL for a disposable schema"
        )
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
    output = sys.stdout
    with tempfile.TemporaryDirectory(
        prefix="liuxin-maintenance-bench-", dir="/tmp"
    ) as scratch:
        scratch = Path(scratch)
        os.environ["LIUXIN_PREFS_DIR"] = str(scratch / "prefs")
        os.environ["LIUXIN_CONFIG_DIR"] = str(scratch / "config")
        with redirect_stdout(sys.stderr):
            run(
                args,
                scratch,
                lambda row: print(json.dumps(row), file=output, flush=True),
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
