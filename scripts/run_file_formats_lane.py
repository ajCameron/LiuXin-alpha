#!/usr/bin/env python3
"""
Run file_formats tests in CI-friendly lanes.

Lanes:
  - fast: all file_formats tests except intentionally heavy paths
  - heavy: unicode torture / end-to-end / full-stack style tests
  - all: everything under tests/file_formats
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


HEAVY_HINTS = (
    "unicode_torture",
    "end_to_end",
    "full_stack",
    "fuzz",
    "regex_torture",
    "unicode_robustness",
)


def discover_test_files(repo_root: Path) -> list[Path]:
    tests_root = repo_root / "tests" / "file_formats"
    return sorted(p for p in tests_root.rglob("test_*.py") if p.is_file())


def split_lanes(files: list[Path]) -> tuple[list[Path], list[Path]]:
    heavy: list[Path] = []
    fast: list[Path] = []
    for path in files:
        name = path.name.lower()
        if any(hint in name for hint in HEAVY_HINTS):
            heavy.append(path)
        else:
            fast.append(path)
    return fast, heavy


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a specific file_formats test lane")
    parser.add_argument(
        "--lane",
        choices=("fast", "heavy", "all"),
        required=True,
        help="Which test lane to run",
    )
    parser.add_argument(
        "--pytest-args",
        nargs=argparse.REMAINDER,
        help="Additional args to pass to pytest (use after --pytest-args)",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    all_files = discover_test_files(repo_root)
    fast_files, heavy_files = split_lanes(all_files)

    if args.lane == "fast":
        selected = fast_files
    elif args.lane == "heavy":
        selected = heavy_files
    else:
        selected = all_files

    print(f"Discovered {len(all_files)} file_formats test files", flush=True)
    print(f"Lane '{args.lane}' has {len(selected)} test files", flush=True)

    if not selected:
        print("No files matched lane; nothing to run.", flush=True)
        return 0

    cmd = [sys.executable, "-m", "pytest", "-q"]
    if args.pytest_args:
        cmd.extend(args.pytest_args)
    cmd.extend(str(p.relative_to(repo_root)) for p in selected)

    print("Running:", " ".join(cmd), flush=True)
    return subprocess.call(cmd, cwd=repo_root)


if __name__ == "__main__":
    raise SystemExit(main())
