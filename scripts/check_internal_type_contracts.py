#!/usr/bin/env python3
"""Require type checkers to accept valid internal calls and reject known mistakes.

The fixture is checked as source and never executed. Expected diagnostics are
matched by file, line, and rule, so an unrelated checker failure cannot make a
negative example pass. Unmarked examples must remain free of diagnostics.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import tomllib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE = REPO_ROOT / "tests" / "typing" / "internal_contracts.py"
_EXPECTED_ERROR = re.compile(r"# expect-error: (\w+) ([\w-]+)$")


@dataclass(frozen=True)
class Diagnostic:
    """One checker diagnostic with a stable source location and rule name."""

    path: Path
    line: int
    code: str
    message: str


def _expected_errors(source: str, checker: str) -> dict[int, str]:
    """Read the per-checker expectations without executing fixture code."""

    expected = {}
    for line, text in enumerate(source.splitlines(), start=1):
        match = _EXPECTED_ERROR.search(text)
        if match is not None:
            expected[line] = match.group(1 if checker == "basedpyright" else 2)
    if not expected:
        raise ValueError("The internal contract fixture has no expected errors.")
    return expected


def _diagnostic_failures(
    expected: Mapping[int, str],
    diagnostics: Iterable[Diagnostic],
    fixture: Path,
) -> list[str]:
    """Reject missing expected errors and diagnostics on otherwise valid code."""

    fixture = fixture.resolve()
    wanted = {(fixture, line, code) for line, code in expected.items()}
    observed = {
        (item.path.resolve(), item.line, item.code): item for item in diagnostics
    }
    failures = [
        f"{path.name}:{line}: expected {code}, but the checker accepted this mistake"
        for path, line, code in sorted(wanted - observed.keys())
    ]
    for key in sorted(observed.keys() - wanted):
        item = observed[key]
        failures.append(
            f"{item.path}:{item.line}: unexpected {item.code}: {item.message}"
        )
    return failures


def _parse_diagnostics(checker: str, output: str) -> tuple[Diagnostic, ...]:
    """Normalize the supported checkers' JSON error and warning formats."""

    if checker == "basedpyright":
        rows = json.loads(output)["generalDiagnostics"]
        return tuple(
            Diagnostic(
                Path(row["file"]),
                row["range"]["start"]["line"] + 1,
                row.get("rule", "unknown"),
                row["message"],
            )
            for row in rows
            if row["severity"] in {"error", "warning"}
        )
    return tuple(
        Diagnostic(
            REPO_ROOT / row["file"],
            row["line"],
            row["code"],
            row["message"],
        )
        for line in output.splitlines()
        if line.strip()
        for row in [json.loads(line)]
        if row["severity"] in {"error", "warning"}
    )


def _checker_command(checker: str) -> list[str]:
    """Keep mypy's configured source targets active alongside the fixture."""

    executable = REPO_ROOT / ".venv" / "bin" / checker
    if not executable.is_file():
        raise FileNotFoundError(f"Missing prepared checker: {executable}")
    if checker == "basedpyright":
        return [str(executable), "--outputjson", str(FIXTURE)]
    with (REPO_ROOT / "pyproject.toml").open("rb") as stream:
        targets = tomllib.load(stream)["tool"]["mypy"]["files"]
    # Explicit CLI paths replace mypy's configured file list. Retaining that
    # list prevents follow_imports=skip from turning the tested contracts into
    # Any and gives CoreCommand/CoreQuery their actual, distinct definitions.
    return [str(executable), "--output", "json", *targets, str(FIXTURE)]


def main(argv: list[str] | None = None) -> int:
    """Check the positive examples and every annotated negative example."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checker", choices=("basedpyright", "mypy"), required=True)
    args = parser.parse_args(argv)
    expected = _expected_errors(FIXTURE.read_text(encoding="utf-8"), args.checker)
    completed = subprocess.run(
        _checker_command(args.checker),
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=180,
    )
    if completed.returncode not in {0, 1}:
        print(completed.stderr or completed.stdout)
        return 1
    diagnostics = _parse_diagnostics(args.checker, completed.stdout)
    failures = _diagnostic_failures(expected, diagnostics, FIXTURE)
    if failures:
        print("\n".join(failures))
        return 1
    print(
        f"Internal contracts ({args.checker}): valid calls accepted; "
        f"{len(expected)} invalid examples rejected."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
