#!/usr/bin/env python3
"""Run the whole database suite with mandatory named regression cases.

Any suite failure fails the gate. Missing, skipped or xfailed mandatory cases
also fail it, independently of the overall test count.
"""

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


def _git(*args: str) -> subprocess.CompletedProcess[str]:
    """
    Run git in the checkout, returning its result rather than raising.

    Example:
        >>> _git("rev-parse", "--is-inside-work-tree").stdout.strip()  # doctest: +SKIP
        'true'


    :param args: The git arguments.
    :return: The finished process, with its output as text.
    """
    return subprocess.run(
        ["git", *args], cwd=ROOT, text=True, capture_output=True, check=False
    )


def resolve_baseline(ref: str, fallback_branch: str | None) -> str:
    """
    Use the baseline commit if this checkout has it, else the merge base with a branch.

    A push's previous head is gone after a force-push, so it cannot be compared with;
    the commit this branch shares with the default branch is the next trusted baseline.

    Example:
        >>> resolve_baseline("0" * 40, "origin/main")  # doctest: +SKIP
        Baseline 0000000000000000000000000000000000000000 is not in this checkout ...


    :param ref: The full SHA of the commit to compare with.
    :param fallback_branch: The branch whose merge base to use instead, or None.
    :return: The SHA to compare with.
    :raises ValueError: If ``ref`` is not a full SHA, or neither commit is available.
    """
    if not re.fullmatch(r"[0-9a-f]{40}", ref):
        raise ValueError("Baseline must be a full commit SHA")
    if _git("cat-file", "-e", f"{ref}^{{commit}}").returncode == 0:
        return ref
    if fallback_branch:
        merge_base = _git("merge-base", "HEAD", fallback_branch)
        if merge_base.returncode == 0 and merge_base.stdout.strip():
            base = merge_base.stdout.strip()
            print(
                f"Baseline {ref} is not in this checkout (force-pushed away?); "
                f"comparing with the merge base with {fallback_branch}, {base}."
            )
            return base
    raise ValueError(
        f"Baseline commit {ref} is not in this checkout, and no merge base was found"
    )


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
        "--fallback-branch",
        help="Compare with the merge base with this branch when the baseline commit is gone",
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
        previous = baseline_at(
            resolve_baseline(args.baseline_ref, args.fallback_branch)
        )
        if previous is not None:
            check_baseline(document, previous)
    artifacts = args.artifacts.resolve()
    if artifacts == ROOT or ROOT in artifacts.parents:
        raise ValueError("Keep ratchet artifacts outside the checkout")
    artifacts.mkdir(parents=True, exist_ok=True)
    gate = RegressionGate(required)
    # Tests already isolate preferences; scratch stays on the Linux filesystem.
    with tempfile.TemporaryDirectory(prefix="liuxin-db-ratchet-") as scratch:
        os.chdir(ROOT)
        status = pytest.main(
            [
                "tests/databases",
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
