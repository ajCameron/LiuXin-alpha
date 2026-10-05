#!/usr/bin/env python3
"""
Validate internal static-type contracts.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise check internal type contracts through a consuming regression::

        python -m pytest -q tests/scripts/test_ci_workflow_contracts.py
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
_EXPECTED_ERROR = re.compile(r"# expect-error: ([\w-]+) ([\w-]+)$")


@dataclass(frozen=True)
class Diagnostic:
    """
    One checker diagnostic with a stable source location and rule name.

    Example:
        Exercise Diagnostic through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py
    """

    path: Path
    line: int
    code: str
    message: str


def _expected_errors(source: str, checker: str) -> dict[int, str]:
    """
    Read exact-line expectations; a dash leaves that checker unmarked here.

    Example:
        Exercise  expected errors through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param source: Value supplied for source under the utility contract.
    :param checker: Value supplied for checker under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    expected = {}
    for line, text in enumerate(source.splitlines(), start=1):
        match = _EXPECTED_ERROR.search(text)
        if match is not None:
            code = match.group(1 if checker == "basedpyright" else 2)
            if code != "-":
                expected[line] = code
    if not expected:
        raise ValueError("The internal contract fixture has no expected errors.")
    return expected


def _diagnostic_failures(
    expected: Mapping[int, str],
    diagnostics: Iterable[Diagnostic],
    fixture: Path,
) -> list[str]:
    """
    Reject missing expected errors and diagnostics on otherwise valid code.

    Example:
        Exercise  diagnostic failures through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param expected: Value supplied for expected under the utility contract.
    :param diagnostics: Value supplied for diagnostics under the utility contract.
    :param fixture: Value supplied for fixture under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

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
    """
    Normalize the supported checkers' JSON error and warning formats.

    Example:
        Exercise  parse diagnostics through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param checker: Value supplied for checker under the utility contract.
    :param output: Value supplied for output under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

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
    """
    Keep mypy's configured source targets active alongside the fixture.

    Example:
        Exercise  checker command through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param checker: Value supplied for checker under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

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
    """
    Check the positive examples and every annotated negative example.

    Example:
        Exercise main through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param argv: Value supplied for argv under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

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
