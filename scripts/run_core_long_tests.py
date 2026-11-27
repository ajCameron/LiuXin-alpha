"""Run an explicit Core load/soak lane and retain latency evidence, including failures."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--size", choices=("smoke", "warm", "large", "million", "soak"), default="warm"
    )
    parser.add_argument(
        "--backend",
        choices=("all", "SQLite", "SQLite_apsw", "PostgreSQL"),
        default="SQLite",
    )
    parser.add_argument(
        "--cache",
        choices=("all", "none", "schema_backed", "database_backed", "numpy_vectorized"),
        default="schema_backed",
    )
    parser.add_argument("--transport", choices=("all", "local", "http"), default="http")
    parser.add_argument("--artifacts", type=Path, required=True)
    args = parser.parse_args()
    if args.backend in ("all", "PostgreSQL") and not os.environ.get(
        "LIUXIN_TEST_POSTGRES_URL"
    ):
        parser.error("Selected backend requires LIUXIN_TEST_POSTGRES_URL")
    root = args.artifacts.resolve()
    root.mkdir(parents=True, exist_ok=True)
    os.environ.update(
        LIUXIN_RUN_CORE_LONG="1",
        LIUXIN_CORE_LONG_SIZE=args.size,
        LIUXIN_CORE_LONG_BACKEND=args.backend,
        LIUXIN_CORE_LONG_CACHE=args.cache,
        LIUXIN_CORE_LONG_TRANSPORT=args.transport,
        LIUXIN_CORE_LONG_ARTIFACTS=str(root),
    )
    sources = {}
    for package in (
        "core",
        "jobs",
        "caches",
        "catalog",
        "databases",
        "storage",
        "ingest",
        "file_formats",
        "surfaces",
    ):
        for path in sorted((ROOT / "src/LiuXin_alpha" / package).rglob("*.py")):
            sources[str(path.relative_to(ROOT))] = hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
    for name in (
        "src/LiuXin_alpha/configuration.py",
        "scripts/run_core_long_tests.py",
        "tests/core/test_runtime_load_long.py",
    ):
        sources[name] = hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
    (root / "sources.json").write_text(json.dumps(sources, indent=2) + "\n")
    import pytest

    class Gate:
        def __init__(self):
            self.passed = 0
            self.skipped = 0

        def pytest_runtest_logreport(self, report):
            if report.skipped:
                self.skipped += 1
            if report.when == "call" and report.passed:
                self.passed += 1

    gate = Gate()
    result = pytest.main(
        [str(ROOT / "tests/core/test_runtime_load_long.py"), "-q", "--tb=short"],
        plugins=[gate],
    )
    expected = (
        (3 if args.backend == "all" else 1)
        * (4 if args.cache == "all" else 1)
        * (2 if args.transport == "all" else 1)
    )
    return 0 if result == 0 and gate.passed == expected and not gate.skipped else 1


if __name__ == "__main__":
    raise SystemExit(main())
