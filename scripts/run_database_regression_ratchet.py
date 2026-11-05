#!/usr/bin/env python3
"""Run named database regressions; missing, skipped and xfailed cases fail the gate."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = Path("tests/databases/regression_ratchet.json")


def required_cases(document: dict[str, Any]) -> set[str]:
    """Validate stable, explicit node IDs rather than relying on a test count."""
    if document.get("version") != 1 or not document.get("invariants"):
        raise ValueError("Expected a version 1 manifest with nonempty invariants")
    required: set[str] = set()
    for name, cases in document["invariants"].items():
        if not isinstance(name, str) or not isinstance(cases, list) or not cases:
            raise ValueError("Each invariant must name at least one regression case")
        for case in cases:
            if not isinstance(case, str) or "::test_" not in case:
                raise ValueError(f"Invalid regression node ID: {case!r}")
            path = Path(case.split("::", 1)[0])
            if (
                path.is_absolute()
                or ".." in path.parts
                or path.parts[:2] != ("tests", "databases")
            ):
                raise ValueError(
                    f"Regression path escapes the database tests: {case!r}"
                )
            if case in required:
                raise ValueError(f"Duplicate regression case: {case}")
            required.add(case)
    return required


def check_baseline(current: dict[str, Any], previous: dict[str, Any]) -> None:
    """Refuse to remove or reassign a previously required case from its invariant."""
    required_cases(current)
    required_cases(previous)
    for invariant, cases in previous["invariants"].items():
        missing = set(cases) - set(current["invariants"].get(invariant, []))
        if missing:
            raise ValueError(
                f"Regression ratchet decreased ({invariant}): {sorted(missing)}"
            )


def baseline_at(ref: str) -> dict[str, Any] | None:
    """Read a trusted commit baseline; a genuinely absent older manifest bootstraps."""
    if not re.fullmatch(r"[0-9a-f]{40}", ref):
        raise ValueError("Baseline must be a full commit SHA")
    listing = subprocess.run(
        ["git", "ls-tree", "--name-only", ref, "--", str(MANIFEST)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    if not listing.stdout.strip():
        return None
    result = subprocess.run(
        ["git", "show", f"{ref}:{MANIFEST.as_posix()}"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(result.stdout)


class RegressionGate:
    """Require every case to be collected and to pass setup, call and teardown."""

    def __init__(self, required: set[str]) -> None:
        self.required = required
        self.collected: set[str] = set()
        self.passed: dict[str, set[str]] = {}
        self.violations: list[str] = []

    @pytest.hookimpl(trylast=True)
    def pytest_collection_modifyitems(self, items: list[pytest.Item]) -> None:
        self.collected = {item.nodeid for item in items}

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        if report.nodeid not in self.required:
            return
        if report.outcome != "passed" or hasattr(report, "wasxfail"):
            self.violations.append(
                f"{report.nodeid}: {report.when} {report.outcome} (xfail={hasattr(report, 'wasxfail')})"
            )
        else:
            self.passed.setdefault(report.nodeid, set()).add(report.when)

    def failures(self) -> list[str]:
        failures = list(self.violations)
        failures.extend(
            f"Not collected: {case}" for case in sorted(self.required - self.collected)
        )
        failures.extend(
            f"Did not fully pass: {case}"
            for case in sorted(self.required)
            if self.passed.get(case) != {"setup", "call", "teardown"}
        )
        return failures

    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int) -> None:
        if self.failures():
            session.exitstatus = pytest.ExitCode.TESTS_FAILED


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--baseline-ref", help="Compare with the PR base or previous push commit"
    )
    parser.add_argument(
        "--artifacts",
        type=Path,
        required=True,
        help="External directory for telemetry and results",
    )
    args = parser.parse_args(argv)
    document = json.loads((ROOT / MANIFEST).read_text())
    required = required_cases(document)
    if args.baseline_ref:
        previous = baseline_at(args.baseline_ref)
        if previous is not None:
            check_baseline(document, previous)
    artifacts = args.artifacts.resolve()
    if artifacts == ROOT or ROOT in artifacts.parents:
        raise ValueError("Keep ratchet artifacts outside the checkout")
    artifacts.mkdir(parents=True, exist_ok=True)
    gate = RegressionGate(required)
    paths = sorted({case.split("::", 1)[0] for case in required})
    # Tests already isolate preferences; scratch stays on the Linux filesystem.
    with tempfile.TemporaryDirectory(prefix="liuxin-db-ratchet-") as scratch:
        os.chdir(ROOT)
        status = pytest.main(
            [
                *paths,
                "-q",
                "-n",
                "0",
                "-o",
                "tmp_path_retention_policy=none",
                "--basetemp",
                str(Path(scratch) / "tests"),
                "--telemetry-dir",
                str(artifacts / "telemetry"),
            ],
            plugins=[gate],
        )
    failures = gate.failures()
    (artifacts / "ratchet.json").write_text(
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
        + "\n"
    )
    for failure in failures:
        print(failure)
    return int(status) if status else int(bool(failures))


if __name__ == "__main__":
    raise SystemExit(main())
