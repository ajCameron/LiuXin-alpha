"""Run opt-in cache soaks with mandatory cases, timings and optional call profiles.

Examples::

    .venv/bin/python scripts/run_cache_long_tests.py --lane synthetic --artifacts /tmp/cache-soak
    .venv/bin/python scripts/run_cache_long_tests.py --lane sqlite --size smoke --artifacts /tmp/cache-smoke

PostgreSQL requires LIUXIN_TEST_POSTGRES_URL and owns disposable schemas only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import sys
import tempfile
from dataclasses import asdict
from pathlib import Path

import pytest

# Support direct execution from any working directory without an editable install.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.run_database_regression_ratchet import RegressionGate  # noqa: E402
from tests.support.cache_long_runs import BACKENDS, WORKLOADS  # noqa: E402

BASE = "tests/databases/caches/"
SYNTHETIC = BASE + "test_search_long.py::"
DATABASE = BASE + "test_search_long_database.py::"


def selected_cases(lane: str) -> set[str]:
    """Name every required case so deletion or a capability skip fails this lane."""
    cases = set()
    if lane in {"synthetic", "all"}:
        cases.add(SYNTHETIC + "test_calibre_query_definition_and_candidate_soak")
        cases.update(
            SYNTHETIC + f"test_native_mutation_recovery_soak[{backend}]"
            for backend in BACKENDS
        )
    drivers = {
        "synthetic": (),
        "sqlite": ("SQLite", "SQLite_apsw"),
        "postgres": ("PostgreSQL",),
        "all": ("SQLite", "SQLite_apsw", "PostgreSQL"),
    }[lane]
    cases.update(
        DATABASE + f"test_full_load_capture_and_transaction_cycles[{driver}-{backend}]"
        for driver in drivers
        for backend in BACKENDS
    )
    return cases


def source_hashes() -> dict[str, str]:
    """Identify the working sources, including uncommitted optimization changes."""
    paths = [
        ROOT / "scripts/run_cache_long_tests.py",
        ROOT / "tests/support/cache_long_runs.py",
        ROOT / BASE / "test_search_long.py",
        ROOT / BASE / "test_search_long_database.py",
    ]
    for directory in (
        "caches",
        "databases",
        "catalog",
        "utils/calibre_compat/caches/search",
    ):
        paths.extend((ROOT / "src/LiuXin_alpha" / directory).rglob("*.py"))
    return {
        str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(paths)
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lane",
        choices=("synthetic", "sqlite", "postgres", "all"),
        default="synthetic",
    )
    parser.add_argument("--size", choices=tuple(WORKLOADS), default="full")
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument(
        "--cprofile",
        action="store_true",
        help="Profile measured operations; timings include profiling overhead",
    )
    args = parser.parse_args(argv)
    if (
        args.lane in {"postgres", "all"}
        and not os.environ.get("LIUXIN_TEST_POSTGRES_URL", "").strip()
    ):
        parser.error("The selected lane requires LIUXIN_TEST_POSTGRES_URL")
    artifacts = args.artifacts.resolve()
    if artifacts == ROOT or ROOT in artifacts.parents:
        parser.error("Keep long-run artifacts outside the checkout")
    # A fresh directory prevents previous passing results masking an interrupted run.
    artifacts.mkdir(parents=True, exist_ok=False)
    required = selected_cases(args.lane)
    report = {
        "lane": args.lane,
        "size": args.size,
        "workload": asdict(WORKLOADS[args.size]),
        "profiled": args.cprofile,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "required_cases": sorted(required),
        "source_sha256": source_hashes(),
    }
    (artifacts / "run.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    os.environ.update(
        {
            "LIUXIN_RUN_CACHE_LONG_TESTS": "1",
            "LIUXIN_CACHE_LONG_SIZE": args.size,
            "LIUXIN_CACHE_LONG_ARTIFACTS": str(artifacts / "measurements"),
            "LIUXIN_CACHE_LONG_CPROFILE": "1" if args.cprofile else "0",
        }
    )
    gate = RegressionGate(required)
    with tempfile.TemporaryDirectory(prefix="liuxin-cache-long-") as scratch:
        os.chdir(ROOT)
        status = pytest.main(
            [
                *sorted(required),
                "-q",
                "-n",
                "0",
                "--tb=short",
                "-o",
                "tmp_path_retention_policy=none",
                "--basetemp",
                str(Path(scratch) / "tests"),
                "--telemetry-dir",
                str(artifacts / "telemetry"),
                "--junitxml",
                str(artifacts / "junit.xml"),
            ],
            plugins=[gate],
        )
    failures = gate.failures()
    (artifacts / "gate.json").write_text(
        json.dumps(
            {
                "required_cases": len(required),
                "fully_passed": sum(
                    phases == {"setup", "call", "teardown"}
                    for phases in gate.passed.values()
                ),
                "failures": failures,
                "pytest_exit_code": int(status),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    for failure in failures:
        print(failure)
    return int(status) if status else int(bool(failures))


if __name__ == "__main__":
    raise SystemExit(main())
