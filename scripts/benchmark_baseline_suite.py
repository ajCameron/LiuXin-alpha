#!/usr/bin/env python3
"""Run and consolidate LiuXin's representative performance baselines."""

from __future__ import annotations

import argparse
import sys

from pathlib import Path
from typing import Optional

from _benchmark_common import DEFAULT_CACHE_DIR, DEFAULT_RESULTS_DIR, environment_payload, print_report_summary, stderr_progress, utc_now_iso, write_json_report
from benchmark_surface_paths import run_surface_path_benchmarks
from benchmark_read_paths import run_read_path_benchmarks


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the default LiuXin benchmark baseline suite.")
    parser.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR), help="Cache directory for named DB provisioning.")
    parser.add_argument("--regenerate", action="store_true", help="Force regeneration of named DB templates.")
    parser.add_argument("--keep-provisioned", action="store_true", help="Do not delete temporary provisioned DB copies.")
    parser.add_argument("--iterations", type=int, default=5, help="Measured iterations per scenario.")
    parser.add_argument("--warmups", type=int, default=1, help="Warmup iterations per scenario.")
    parser.add_argument(
        "--profile",
        choices=("interactive", "nightly"),
        default="interactive",
        help="Benchmark suite profile. `interactive` stays fast enough for local iteration; `nightly` includes the slower medium corpus.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Write combined JSON report to this path. Defaults to a profile-specific file under LiuXin_data/benchmarks/results.",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress progress logging and only emit the final summary line.")
    return parser.parse_args()


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args()
    quiet = bool(args.quiet)

    backend_targets = [
        "benchmark_db_smoke",
        "metadata_rich_db_1",
        "stores_assets_db_1",
        "images_covers_db_1",
        "pathological_relations_db_0",
        "weird_data_db_0",
    ]
    if str(args.profile) == "nightly":
        backend_targets.insert(1, "benchmark_db_medium")
    surface_targets = (
        "metadata_rich_db_1",
        "stores_assets_db_1",
        "images_covers_db_1",
        "weird_data_db_0",
    )
    output_path = str(args.output).strip()
    if not output_path:
        filename = (
            "benchmark-baseline-2026-03-19.json"
            if str(args.profile) == "interactive"
            else "benchmark-baseline-nightly-2026-03-19.json"
        )
        output_path = str(DEFAULT_RESULTS_DIR / filename)
    if not quiet:
        sys.stderr.write(
            "[benchmark] baseline profile={} iterations={} warmups={} output={}\n".format(
                args.profile,
                max(1, int(args.iterations)),
                max(0, int(args.warmups)),
                output_path,
            )
        )
        sys.stderr.write("[benchmark] backend targets={}\n".format(",".join(backend_targets)))
        sys.stderr.write("[benchmark] surface targets={}\n".format(",".join(surface_targets)))
        sys.stderr.flush()

    backend_reports = []
    for index, name in enumerate(tuple(backend_targets), start=1):
        progress = None
        if not quiet:
            prefix = "backend {}/{} {}".format(index, len(backend_targets), name)
            progress = lambda message, prefix=prefix: stderr_progress("{} {}".format(prefix, message))
            stderr_progress("starting backend target {}".format(name))
        backend_reports.append(
            run_read_path_benchmarks(
                db_name=name,
                database="",
                cache_dir=str(args.cache_dir),
                regenerate=bool(args.regenerate),
                keep_provisioned=bool(args.keep_provisioned),
                iterations=max(1, int(args.iterations)),
                warmups=max(0, int(args.warmups)),
                query="",
                scenario_names=[
                    "open_database",
                    "work_list_title",
                    "work_search_global",
                    "work_detail",
                    "file_download",
                    "image_bytes",
                ],
                progress=progress,
            )
        )
        if not quiet:
            stderr_progress("finished backend target {}".format(name))

    surface_reports = []
    for index, name in enumerate(surface_targets, start=1):
        progress = None
        if not quiet:
            prefix = "surface {}/{} {}".format(index, len(surface_targets), name)
            progress = lambda message, prefix=prefix: stderr_progress("{} {}".format(prefix, message))
            stderr_progress("starting surface target {}".format(name))
        surface_reports.append(
            run_surface_path_benchmarks(
                db_name=name,
                database="",
                cache_dir=str(args.cache_dir),
                regenerate=bool(args.regenerate),
                keep_provisioned=bool(args.keep_provisioned),
                iterations=max(1, int(args.iterations)),
                warmups=max(0, int(args.warmups)),
                query="",
                app_names=["web", "api", "opds"],
                progress=progress,
            )
        )
        if not quiet:
            stderr_progress("finished surface target {}".format(name))

    payload = {
        "script": "benchmark_baseline_suite",
        "created_utc": utc_now_iso(),
        "environment": environment_payload(),
        "inputs": {
            "profile": str(args.profile),
            "iterations": max(1, int(args.iterations)),
            "warmups": max(0, int(args.warmups)),
            "backend_targets": list(backend_targets),
            "surface_targets": list(surface_targets),
        },
        "backend_reports": backend_reports,
        "surface_reports": surface_reports,
    }
    print_report_summary(
        {
            "script": payload["script"],
            "database": {"source": "baseline-suite"},
            "results": [*backend_reports, *surface_reports],
        }
    )
    write_json_report(payload, output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
