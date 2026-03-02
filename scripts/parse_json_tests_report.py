
"""
List pytest test *files* that have failing tests, from a pytest-json-report JSON file.

- Primary path: json.load() then iterate report["tests"].
- Fallback: if the JSON is truncated/corrupt, scan lines inside the "tests" array and
  detect `"outcome": "failed"` paired with the most recent `"nodeid": "..."`
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def try_load_json(path: Path) -> dict[str, Any] | None:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return None


def extract_failed_from_parsed(report: dict[str, Any]) -> tuple[Counter, dict[str, list[str]]]:
    failed = Counter()
    details: dict[str, list[str]] = defaultdict(list)

    tests = report.get("tests", [])
    if not isinstance(tests, list):
        return failed, details

    for t in tests:
        if not isinstance(t, dict):
            continue
        if t.get("outcome") != "failed":
            continue

        nodeid = t.get("nodeid")
        if not isinstance(nodeid, str) or not nodeid:
            continue

        file_path = nodeid.split("::", 1)[0]
        failed[file_path] += 1
        details[file_path].append(nodeid)

    return failed, details


def extract_failed_fallback_scan(path: Path) -> tuple[Counter, dict[str, list[str]]]:
    """
    Best-effort scan for failures inside the `"tests": [` section without parsing JSON.
    """
    failed = Counter()
    details: dict[str, list[str]] = defaultdict(list)

    in_tests = False
    current_nodeid: str | None = None
    nodeid_re = re.compile(r'"nodeid"\s*:\s*"([^"]+)"')

    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if not in_tests:
                if '"tests": [' in line:
                    in_tests = True
                continue

            m = nodeid_re.search(line)
            if m:
                current_nodeid = m.group(1)

            if '"outcome": "failed"' in line and current_nodeid:
                file_path = current_nodeid.split("::", 1)[0]
                failed[file_path] += 1
                details[file_path].append(current_nodeid)

    return failed, details


def main() -> int:
    ap = argparse.ArgumentParser(
        description="List test files that have failing tests (from pytest-json-report output)."
    )
    ap.add_argument(
        "report",
        nargs="?",
        default="all-liuxin-tests-pytest-report.json",
        help="Path to pytest JSON report.",
    )
    ap.add_argument("--details", action="store_true", help="Print failing nodeids per file.")
    ap.add_argument("--min", type=int, default=1, dest="min_failures", help="Only show files with >= N failures.")
    ap.add_argument("--sort", choices=("count", "path"), default="count", help="Sort output.")
    args = ap.parse_args()

    path = Path(args.report)
    if not path.exists():
        ap.error(f"Report not found: {path}")

    report = try_load_json(path)
    if report is not None:
        failed, details = extract_failed_from_parsed(report)
        mode = "json"
    else:
        failed, details = extract_failed_fallback_scan(path)
        mode = "fallback"

    items = [(p, c) for p, c in failed.items() if c >= args.min_failures]
    if args.sort == "count":
        items.sort(key=lambda x: (-x[1], x[0]))
    else:
        items.sort(key=lambda x: (x[0], -x[1]))

    if not items:
        print("No failed tests found.")
        return 0

    total_failed = sum(c for _, c in items)
    print(f"Found {total_failed} failed tests across {len(items)} files (mode={mode}).")

    for p, c in items:
        print(f"{c:4d}  {p}")
        if args.details:
            for nodeid in details.get(p, []):
                print(f"      - {nodeid}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())