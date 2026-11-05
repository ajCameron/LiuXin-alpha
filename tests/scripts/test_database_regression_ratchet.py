"""Keep the regression gate resistant to omissions, skips and count substitutions."""

from types import SimpleNamespace

import pytest

from scripts.run_database_regression_ratchet import (
    RegressionGate,
    check_baseline,
    required_cases,
)

CASE = "tests/databases/test_example.py::test_invariant[SQLite]"


def manifest(cases):
    return {"version": 1, "invariants": {"one-owner": cases}}


def test_baseline_accepts_additions_but_not_equal_count_substitution():
    previous = manifest([CASE])
    new = CASE.replace("SQLite", "PostgreSQL")
    check_baseline(manifest([CASE, new]), previous)
    with pytest.raises(ValueError, match="decreased"):
        check_baseline(manifest([new]), previous)


@pytest.mark.parametrize(
    "cases",
    [
        [],
        ["outside.py::test_no"],
        ["tests/databases/../test_escape.py::test_no"],
        [CASE, CASE],
    ],
)
def test_manifest_rejects_empty_duplicate_and_escaping_cases(cases):
    with pytest.raises(ValueError):
        required_cases(manifest(cases))


def report(phase, outcome="passed", xfail=False):
    item = SimpleNamespace(nodeid=CASE, when=phase, outcome=outcome)
    if xfail:
        item.wasxfail = "known issue"
    return item


@pytest.mark.parametrize("phase", ["setup", "call", "teardown"])
@pytest.mark.parametrize(
    "outcome,xfail",
    [("skipped", False), ("failed", False), ("skipped", True), ("passed", True)],
)
def test_every_nonpass_or_expected_failure_fails_the_gate(phase, outcome, xfail):
    gate = RegressionGate({CASE})
    gate.pytest_collection_modifyitems([SimpleNamespace(nodeid=CASE)])
    for step in ["setup", "call", "teardown"]:
        gate.pytest_runtest_logreport(
            report(
                step,
                outcome if step == phase else "passed",
                xfail if step == phase else False,
            )
        )
    session = SimpleNamespace(exitstatus=0)
    gate.pytest_sessionfinish(session, 0)
    assert session.exitstatus == pytest.ExitCode.TESTS_FAILED
    assert gate.failures()


def test_missing_deselected_and_collect_only_cases_cannot_pass():
    gate = RegressionGate({CASE})
    assert gate.failures()
    gate.pytest_collection_modifyitems([SimpleNamespace(nodeid=CASE)])
    assert gate.failures()
    for phase in ["setup", "call", "teardown"]:
        gate.pytest_runtest_logreport(report(phase))
    assert not gate.failures()
    session = SimpleNamespace(exitstatus=0)
    gate.pytest_sessionfinish(session, 0)
    assert session.exitstatus == 0


@pytest.mark.parametrize(
    "body,expected",
    [
        ("assert True", 0),
        ('pytest.skip("optional backend")', 1),
        ('pytest.xfail("known issue")', 1),
    ],
)
def test_real_pytest_session_cannot_turn_skips_or_xfails_into_green(
    tmp_path, body, expected
):
    import os
    import subprocess
    import sys
    from pathlib import Path

    source = tmp_path / "tests" / "databases" / "test_probe.py"
    source.parent.mkdir(parents=True)
    source.write_text(f"import pytest\ndef test_required():\n    {body}\n")
    root = Path(__file__).resolve().parents[2]
    code = """
import pytest
from scripts.run_database_regression_ratchet import RegressionGate
case = 'tests/databases/test_probe.py::test_required'
gate = RegressionGate({case})
raise SystemExit(pytest.main(['-q', 'tests/databases/test_probe.py'], plugins=[gate]))
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        env={
            **os.environ,
            "PYTHONPATH": str(root),
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
            "PYTEST_ADDOPTS": "",
        },
        text=True,
        capture_output=True,
    )
    assert result.returncode == expected, result.stdout + result.stderr
